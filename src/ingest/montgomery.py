"""Montgomery County, MD -- Socrata/SODA bronze ingest.

    python -m src.ingest.montgomery --since 2026-01-01

Three datasets at three grains (ids in config/sources.toml):

    bhju-22kf  Incidents      ~125,005 rows   one row per crash
    mmzv-x632  Drivers        ~219,644 rows   one row per driver
    n7fk-dce5  Non-Motorists    ~7,498 rows   one row per non-motorist

Bronze layout, one directory per run:

    data/bronze/montgomery/{dataset_id}/{load_ts}/page_00001.json      raw bytes
    data/bronze/montgomery/{dataset_id}/{load_ts}/page_00001.parquet   parsed

Directories are keyed by dataset *id*, not by the friendly name, because the id
is the identity Socrata guarantees; a dataset republished under a new id must
not silently blend into the old partition tree.


Why keyset pagination on (:updated_at, :id)
-------------------------------------------
`$offset` is wrong here and the assignment says it is checked. Offset is
positional: it means "skip the first N rows of the result set as it exists at
the moment this request is served". The table is written concurrently. A row
inserted before your cursor position between page 3 and page 4 shifts every
later row one place forward, so one row is never returned; a delete shifts the
other way and one row is returned twice. Neither shows up as an error. Keyset
pagination asks instead for "the rows after this specific row in this specific
total order", which is stable no matter what happens to the rows behind it.

The order must be total, which is why it is a pair. `:updated_at` alone is not
unique -- and on this dataset it is spectacularly non-unique: a bulk reload gave
125,005 of the incident rows the identical stamp 2024-06-12T20:28:27.326. A
cursor on `:updated_at` alone either loops on that one value forever (with `>=`)
or skips 124k rows in a single step (with `>`). `:id` breaks the tie.

    $order=:updated_at,:id
    $where=:updated_at > 'T' OR (:updated_at = 'T' AND :id > 'ID')

NOTE on the predicate form. The tuple spelling `(:updated_at, :id) > ('T','ID')`
is the natural way to write this, and this Socrata instance rejects it:

    query.compiler.malformed ... at line 1 character 42: Expected one of `)',
    `OR', `AND', `IS', ... but got `,'

Its SoQL compiler has no row-value constructor. The disjunction above is the
identical predicate expanded by hand, and it was verified against the server's
own `$order` collation -- a keyset walk and an `$offset` walk over the same
slice return the same rows in the same sequence. That matters more than it
looks: `:id` values are opaque strings like `row-cs5z_bqgp-b3nq` that do *not*
sort in ASCII order, but Socrata's `>` and its ORDER BY agree with each other,
which is the only property keyset pagination actually needs.


Why the watermark is on :updated_at and not on crash_date_time
--------------------------------------------------------------
A watermark on the crash's own date field answers "what has happened since I
last ran" and that is the wrong question. Records are edited after the fact:
a report filed today can amend a crash from 2019, and a crash from last week
can arrive in the dataset a fortnight late. A crash-date watermark has already
passed those dates, so it never asks for them again and the amendment is lost
forever -- silently, because the row count still goes up every day from new
crashes. `:updated_at` is Socrata's own mutation stamp: any row the county
touches, for any reason, at any crash date, sorts to the top of the next run.
That is what makes "picks up records that appeared for dates already loaded"
true rather than aspirational.
"""

from __future__ import annotations

import argparse
import json
import logging
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterable

from .. import config
from .http import HttpClient
from .watermark import (
    WatermarkStore,
    new_load_ts,
    new_partition,
    row_sha256,
    write_bytes,
    write_rows_parquet,
)

log = logging.getLogger("ingest.montgomery")

SOURCE = "montgomery"

# Socrata's own system columns. `$select=*` does not include them, and they are
# the entire basis of the cursor, so they are requested explicitly.
SYSTEM_COLUMNS = (":id", ":updated_at", ":created_at", ":version")

DEFAULT_PAGE_SIZE = 5000

# Client-side pacing. Socrata throttles anonymous traffic per IP on a shared
# rolling budget; an app token raises that ceiling but does not remove it, and
# the assignment asks for throttling to be handled either way. So we pace in
# both modes -- just less when a token is present -- and still honour a 429's
# Retry-After through HttpClient.
PACE_WITH_TOKEN = 0.25
PACE_ANONYMOUS = 1.0


def dataset_map() -> dict[str, str]:
    """Friendly name -> Socrata 4x4 id, from config/sources.toml."""
    moco = config.sources()["montgomery"]
    return {
        name: moco[name]
        for name in ("incidents", "drivers", "non_motorists")
        if name in moco
    }


def resolve_dataset(token: str) -> tuple[str, str]:
    """Accept either a friendly name or a raw 4x4 id on the CLI."""
    names = dataset_map()
    if token in names:
        return token, names[token]
    for name, ident in names.items():
        if ident == token:
            return name, ident
    raise SystemExit(
        f"unknown dataset {token!r}; expected one of "
        f"{', '.join(sorted(names) + sorted(names.values()))}"
    )


def normalise_since(since: str) -> str:
    """`--since 2026-01-01` -> a Socrata floating-timestamp literal.

    Socrata compares `:updated_at` as a floating timestamp with millisecond
    precision. A bare date is accepted but padding it here keeps the cursor
    written to the watermark store in exactly the same shape as the one read
    back off a row, which makes the stored cursor directly comparable.
    """
    text = since.strip().rstrip("Z")
    try:
        parsed = datetime.fromisoformat(text)
    except ValueError:
        raise SystemExit(f"--since must be ISO-8601, got {since!r}")
    if parsed.tzinfo is not None:
        parsed = parsed.astimezone(timezone.utc).replace(tzinfo=None)
    return parsed.strftime("%Y-%m-%dT%H:%M:%S.") + f"{parsed.microsecond // 1000:03d}"


def soql_literal(value: str) -> str:
    """Single-quoted SoQL string literal with quote doubling.

    `:id` values are server-generated and quote-free today, but a cursor is
    interpolated into a query string and a value that can end a literal early
    is a correctness bug waiting for the day Socrata changes its id alphabet.
    """
    return "'" + value.replace("'", "''") + "'"


def keyset_where(cursor: dict[str, Any]) -> str | None:
    """The keyset predicate for a cursor, or None to read from the beginning.

    Three shapes:
      {}                                     -> no predicate, full table
      {updated_at, inclusive: true}          -> :updated_at >= T   (the --since floor)
      {updated_at, id}                       -> strict keyset after (T, ID)
    """
    updated_at = cursor.get("updated_at")
    if not updated_at:
        return None
    stamp = soql_literal(updated_at)
    row_id = cursor.get("id")
    if not row_id:
        op = ">=" if cursor.get("inclusive") else ">"
        return f":updated_at {op} {stamp}"
    return (
        f":updated_at > {stamp} OR "
        f"(:updated_at = {stamp} AND :id > {soql_literal(row_id)})"
    )


def cursor_from_row(row: dict[str, Any]) -> dict[str, Any]:
    """The cursor that resumes immediately after `row`.

    `:updated_at` comes back with a trailing Z (`...326Z`) but must go back in
    without one -- the filter is against a floating timestamp. Stripping it here
    means the stored cursor round-trips through a query unchanged.
    """
    return {
        "updated_at": str(row[":updated_at"]).rstrip("Z"),
        "id": str(row[":id"]),
    }


def ingest_dataset(
    name: str,
    dataset_id: str,
    *,
    store: WatermarkStore,
    client: HttpClient,
    since: str | None = None,
    page_size: int = DEFAULT_PAGE_SIZE,
    max_pages: int | None = None,
    load_ts: str | None = None,
    validate_contract: bool = False,
) -> dict[str, Any]:
    """Keyset-paginate one dataset into bronze. Returns a run summary."""
    base = config.sources()["montgomery"]["base"].rstrip("/")
    url = f"{base}/{dataset_id}.json"
    # Allocate the directory first and take load_ts from it, so the partition
    # name and every recorded load_ts are the same string by construction.
    partition = new_partition(SOURCE, dataset_id, load_ts or new_load_ts())
    load_ts = partition.name

    state = store.get(SOURCE, dataset_id)
    if state:
        cursor = state["cursor"]
        start = "resume"
        if since:
            log.warning(
                "%s: --since ignored, watermark already at %s "
                "(use --reset to re-backfill)", name, json.dumps(cursor)
            )
    elif since:
        # First run with a floor: inclusive, so `--since 2026-01-01` means
        # "everything touched on or after midnight on the 1st", not "after".
        cursor = {"updated_at": normalise_since(since), "inclusive": True}
        start = f"since {cursor['updated_at']}"
    else:
        cursor = {}
        start = "full backfill"

    log.info("%s (%s): %s -> %s", name, dataset_id, start, partition)

    page_no = 0
    total_rows = 0
    first_cursor = dict(cursor)

    while max_pages is None or page_no < max_pages:
        params: list[tuple[str, Any]] = [
            ("$select", "*, " + ", ".join(SYSTEM_COLUMNS)),
            ("$order", ":updated_at,:id"),
            ("$limit", page_size),
        ]
        where = keyset_where(cursor)
        if where:
            params.append(("$where", where))

        response = client.get(url, params=params)
        raw = response.content  # untouched response body -- bronze gets this

        page_no += 1
        raw_path = partition / f"page_{page_no:05d}.json"
        raw_meta = write_bytes(raw_path, raw)

        rows = json.loads(raw)  # parsing happens only after the bytes are down
        if not rows:
            # An empty page means the cursor has reached the end of the table.
            # The empty page is still kept: it is the durable proof of where the
            # run stopped and what it asked for.
            store.record_artifact(
                SOURCE, dataset_id, load_ts, raw_path, kind="raw",
                sha256=raw_meta["sha256"], size=raw_meta["bytes"], row_count=0,
                source_url=response.url,
            )
            log.info("%s: page %d empty, end of stream", name, page_no)
            break

        parquet_path = partition / f"page_{page_no:05d}.parquet"
        parsed = [
            row | {
                "_bronze_source": SOURCE,
                "_bronze_dataset": dataset_id,
                "_bronze_load_ts": load_ts,
                "_bronze_page": f"{page_no:05d}",
                "_bronze_raw_path": str(raw_path.name),
                "_bronze_row_sha256": row_sha256(row),
            }
            for row in rows
        ]
        pq_meta = write_rows_parquet(
            parquet_path, parsed,
            contract_table=f"montgomery.{dataset_id}" if validate_contract else None,
        )

        store.record_artifact(
            SOURCE, dataset_id, load_ts, raw_path, kind="raw",
            sha256=raw_meta["sha256"], size=raw_meta["bytes"], row_count=len(rows),
            source_url=response.url,
        )
        store.record_artifact(
            SOURCE, dataset_id, load_ts, parquet_path, kind="parsed",
            size=pq_meta["bytes"], row_count=len(rows),
        )

        cursor = cursor_from_row(rows[-1])
        total_rows += len(rows)

        # WATERMARK ADVANCE -- AFTER the durable write, never before.
        # Both files above are fsynced and recorded in bronze_manifest by the
        # time we get here. If this process dies one line earlier, the next run
        # re-requests this page and writes it again under a new load_ts: a
        # duplicate raw page, which append-only bronze is built to absorb and
        # silver de-duplicates on (:id, :version). If we advanced first and died
        # here, the next run would resume past rows that were never written and
        # they would be gone for good, with no error anywhere to notice. See
        # WatermarkStore.advance.
        store.advance(
            SOURCE, dataset_id, cursor,
            rows_in_batch=len(rows), load_ts=load_ts,
            note=f"page {page_no}",
        )

        log.info(
            "%s: page %d rows=%d total=%d cursor=%s/%s",
            name, page_no, len(rows), total_rows,
            cursor["updated_at"], cursor["id"],
        )

        if len(rows) < page_size:
            # Short page almost certainly means end of stream, but "almost" is
            # not a guarantee under concurrent writes, so we loop once more and
            # let the empty page above be the terminator.
            continue

    if max_pages is not None and page_no >= max_pages:
        log.warning(
            "%s: stopped at --max-pages %d; watermark is durable, re-run to continue",
            name, max_pages,
        )

    return {
        "dataset": name,
        "dataset_id": dataset_id,
        "load_ts": load_ts,
        "partition": str(partition),
        "pages": page_no,
        "rows": total_rows,
        "cursor_before": first_cursor,
        "cursor_after": cursor,
    }


def build_client(app_token: str | None = None, pace: float | None = None) -> HttpClient:
    token = app_token if app_token is not None else config.key("socrata_app_token")
    headers = {"Accept": "application/json"}
    if token:
        headers["X-App-Token"] = token
    if pace is None:
        pace = PACE_WITH_TOKEN if token else PACE_ANONYMOUS
    log.info("socrata app token %s; pacing %.2fs between requests",
             "present" if token else "absent (anonymous throttle applies)", pace)
    return HttpClient(headers=headers, min_interval=pace, timeout=120.0)


def main(argv: Iterable[str] | None = None) -> int:
    ap = argparse.ArgumentParser(
        prog="python -m src.ingest.montgomery",
        description="Keyset-paginated Socrata ingest into the bronze layer.",
    )
    ap.add_argument("--since", help="ISO date/timestamp floor for :updated_at on a "
                                    "first run; ignored once a watermark exists")
    ap.add_argument("--dataset", default="all",
                    help="incidents | drivers | non_motorists | a 4x4 id | all")
    ap.add_argument("--page-size", type=int, default=DEFAULT_PAGE_SIZE)
    ap.add_argument("--max-pages", type=int, default=None,
                    help="stop after N pages per dataset (the watermark is "
                         "durable, so a later run resumes exactly where this "
                         "one stopped)")
    ap.add_argument("--reset", action="store_true",
                    help="forget the watermark and re-backfill; bronze is "
                         "append-only, so nothing on disk is destroyed")
    ap.add_argument("--pace", type=float, default=None,
                    help="seconds between requests (default: 0.25 with an app "
                         "token, 1.0 anonymous)")
    ap.add_argument("--db", default=None, help="watermark store path")
    ap.add_argument("-v", "--verbose", action="store_true")
    args = ap.parse_args(list(argv) if argv is not None else None)

    logging.basicConfig(
        level=logging.DEBUG if args.verbose else logging.INFO,
        format="%(asctime)s %(levelname)-7s %(name)s %(message)s",
    )

    targets = (
        list(dataset_map().items())
        if args.dataset == "all"
        else [resolve_dataset(args.dataset)]
    )

    store = WatermarkStore(args.db) if args.db else WatermarkStore()
    if args.reset:
        for _, dataset_id in targets:
            store.reset(SOURCE, dataset_id)
        log.info("watermarks reset for %s", ", ".join(d for _, d in targets))

    load_ts = new_load_ts()  # one partition stamp for the whole run
    summaries = []
    with build_client(pace=args.pace) as client:
        for name, dataset_id in targets:
            summaries.append(
                ingest_dataset(
                    name, dataset_id,
                    store=store, client=client, since=args.since,
                    page_size=args.page_size, max_pages=args.max_pages,
                    validate_contract=True,
                    load_ts=load_ts,
                )
            )
        stats = dict(client.stats)

    print(json.dumps({"load_ts": load_ts, "http": stats, "datasets": summaries}, indent=2))
    return 0


if __name__ == "__main__":
    sys.exit(main())
