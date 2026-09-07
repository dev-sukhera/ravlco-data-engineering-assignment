"""NHTSA FARS -- annual bulk ZIP ingest with restatement detection.

    python -m src.ingest.fars                      # years from sources.toml
    python -m src.ingest.fars --check-only         # HEAD only: what has moved?
    python -m src.ingest.fars --years 2023

    https://static.nhtsa.gov/nhtsa/downloads/FARS/{year}/National/FARS{year}NationalCSV.zip

Bronze layout:

    data/bronze/fars/{year}/{load_ts}/FARS{year}NationalCSV.zip   raw, verbatim
    data/bronze/fars/{year}/{load_ts}/accident.parquet            parsed members
    data/bronze/fars/{year}/{load_ts}/person.parquet
    ...

Years come from `[fars].years` in config/sources.toml (2019-2024). The full
1975-2024 range is a deliberate scope cut: the older files use different schemas
and code dictionaries, and nothing downstream in this pipeline reaches back
past 2019.


The problem this module exists to solve
---------------------------------------
FARS files are revised in place. The URL does not change, the year in the
filename does not change, and NHTSA publishes no changelog. The 2023 national
file currently carries `Last-Modified: Wed, 01 Apr 2026 19:46:54 GMT` --
confirmed live -- which is nearly two and a half years after the year it
describes. Fatality records get reclassified, coordinates get corrected, and
late-adjudicated cases get added, all under the same URL.

So "have I already downloaded 2023?" is the wrong question. The right one is
"is the 2023 file still the same bytes it was when I loaded it?", and the only
honest answer is a content hash:

    HEAD -> Last-Modified / ETag       cheap, and a strong hint, but only a hint
    GET  -> SHA-256 of the body        the actual authority

Last-Modified alone is not enough in either direction. It can move on a
re-upload of identical content (so trusting it re-materialises a year for
nothing), and a CDN can serve a stale one (so trusting it misses a real
revision). It is therefore used only to decide whether a download is worth
attempting; the hash decides whether anything is written.

A changed hash triggers a **full refresh of that year**: a complete new
`{year}/{load_ts}/` partition holding the whole year again. Not an append --
appending an amended file to its own prior version double-counts every
unrevised row in it. Not a skip -- a skip is the failure mode this module is
built to prevent. Prior partitions stay on disk untouched, because bronze is
append-only and versioned by load_ts for FARS exactly as it is for the other
two sources, and silver needs version N-1 present to diff version N against it.


What this module deliberately does not do
-----------------------------------------
No sentinel handling. FARS encodes unknown coordinates as 77.7777 / 88.8888 /
99.9999 and unknown codes as 7/8/9-fill (`AGE` 998/999, `HOUR` 99). Every CSV is
read with `dtype=str` and NA detection switched off, so `"999"`, `"NA"` and `""`
all land in bronze as exactly the characters NHTSA wrote. Turning those into
nulls is silver's job (src/transform/fars.py) and doing it here would destroy
the evidence that the four known-defect tests need to fail on bronze.
"""

from __future__ import annotations

import argparse
import csv
import io
import json
import logging
import sys
import zipfile
from pathlib import Path
from typing import Any, Iterable

import pyarrow as pa
import pyarrow.parquet as pq

from .. import config
from .http import HttpClient
from .watermark import (
    WatermarkStore,
    bronze_partition,
    durable_replace,
    new_load_ts,
)

log = logging.getLogger("ingest.fars")

SOURCE = "fars"

# FARS CSVs are Windows-authored. cp1252 covers the smart quotes and degree
# signs that appear in free-text fields and never raises on a byte sequence,
# so it is the fallback rather than a second guess.
ENCODINGS = ("utf-8-sig", "cp1252")


def years() -> list[int]:
    return [int(y) for y in config.sources()["fars"]["years"]]


def zip_url(year: int) -> str:
    return config.sources()["fars"]["bulk_pattern"].format(year=year)


def probe(client: HttpClient, year: int) -> dict[str, Any]:
    """HEAD the year's file. Cheap enough to run on every year, every day."""
    response = client.head(zip_url(year))
    return {
        "year": year,
        "url": zip_url(year),
        "last_modified": response.headers.get("Last-Modified"),
        "etag": response.headers.get("ETag"),
        "content_length": response.headers.get("Content-Length"),
    }


def _decode(raw: bytes) -> tuple[str, str]:
    for encoding in ENCODINGS:
        try:
            return raw.decode(encoding), encoding
        except UnicodeDecodeError:
            continue
    return raw.decode("cp1252", errors="replace"), "cp1252/replace"


def write_member_parquet(
    dest: Path, raw: bytes, *, extra: dict[str, str]
) -> dict[str, Any]:
    """One CSV member of the ZIP -> one all-string parquet file.

    Every column is read as text and NA detection is off. `999`, `NA`, `NULL`
    and the empty string are all preserved as written -- see the module
    docstring on why bronze must not interpret sentinels.
    """
    text, encoding = _decode(raw)
    reader = csv.reader(io.StringIO(text, newline=""))
    try:
        header = next(reader)
    except StopIteration:
        header = []
    header = [h.strip() for h in header]
    columns: list[list[str]] = [[] for _ in header]
    rows = 0
    for record in reader:
        if not record:
            continue
        rows += 1
        for idx in range(len(header)):
            columns[idx].append(record[idx] if idx < len(record) else "")

    data = {name: pa.array(values, type=pa.string())
            for name, values in zip(header, columns)}
    for key, value in extra.items():
        data[key] = pa.array([value] * rows, type=pa.string())
    table = pa.table(data) if data else pa.table({"_bronze_empty": pa.array([], pa.string())})

    dest.parent.mkdir(parents=True, exist_ok=True)
    tmp = dest.with_name(dest.name + ".part")
    pq.write_table(table, tmp, compression="zstd")
    durable_replace(tmp, dest)
    return {"path": dest, "rows": rows, "columns": len(header),
            "encoding": encoding, "bytes": dest.stat().st_size}


def ingest_year(
    year: int,
    *,
    store: WatermarkStore,
    client: HttpClient,
    force: bool = False,
) -> dict[str, Any]:
    """Load one FARS year if -- and only if -- its bytes have changed."""
    dataset = str(year)
    state = store.get(SOURCE, dataset)
    cursor = state["cursor"] if state else {}
    head = probe(client, year)

    unchanged_header = (
        not force
        and cursor.get("sha256")
        and cursor.get("last_modified") == head["last_modified"]
        and cursor.get("etag") == head["etag"]
    )
    if unchanged_header:
        log.info("%d: Last-Modified/ETag unchanged (%s) -- no download",
                 year, head["last_modified"])
        return {"year": year, "action": "skipped_unchanged_header",
                "last_modified": head["last_modified"],
                "sha256": cursor.get("sha256"),
                "current_load_ts": cursor.get("current_load_ts")}

    # Header moved (or we have never seen this year). Download to a staging
    # path outside any partition: until the hash is known we do not know
    # whether this download deserves a partition at all.
    staging = (
        bronze_partition(SOURCE, dataset, "_staging").parent
        / "_staging" / f"FARS{year}NationalCSV.zip"
    )
    log.info("%d: downloading (%s bytes, last-modified %s)",
             year, head["content_length"], head["last_modified"])
    got = client.download(head["url"], staging)

    if not force and cursor.get("sha256") == got["sha256"]:
        # The header moved but the content did not: a re-upload of identical
        # bytes, which happens. Record the new header so tomorrow's HEAD check
        # short-circuits again, and write no partition.
        staging.unlink(missing_ok=True)
        log.info("%d: header moved but sha256 unchanged (%s) -- no refresh",
                 year, got["sha256"][:16])
        store.advance(
            SOURCE, dataset,
            cursor | {"last_modified": head["last_modified"], "etag": head["etag"]},
            rows_in_batch=0, load_ts=cursor.get("current_load_ts"),
            note="header moved, content identical",
        )
        return {"year": year, "action": "unchanged_content",
                "sha256": got["sha256"], "last_modified": head["last_modified"],
                "current_load_ts": cursor.get("current_load_ts")}

    reason = "restatement" if cursor.get("sha256") else "initial"
    if reason == "restatement":
        log.warning(
            "%d: RESTATED -- sha256 %s -> %s. Full-refreshing the year into a "
            "new partition; %d prior partition(s) retained.",
            year, str(cursor.get("sha256"))[:16], got["sha256"][:16],
            len(store.load_partitions(SOURCE, dataset)),
        )

    load_ts = new_load_ts()
    partition = bronze_partition(SOURCE, dataset, load_ts)
    partition.mkdir(parents=True, exist_ok=True)
    zip_path = partition / staging.name
    durable_replace(staging, zip_path)

    store.record_artifact(
        SOURCE, dataset, load_ts, zip_path, kind="raw",
        sha256=got["sha256"], size=got["bytes"],
        content_last_modified=head["last_modified"], etag=head["etag"],
        source_url=head["url"],
    )

    members: list[dict[str, Any]] = []
    with zipfile.ZipFile(zip_path) as zf:
        for info in sorted(zf.infolist(), key=lambda i: i.filename.lower()):
            if info.is_dir() or not info.filename.lower().endswith(".csv"):
                continue
            stem = Path(info.filename).stem.lower()
            member_path = partition / f"{stem}.parquet"
            meta = write_member_parquet(
                member_path, zf.read(info),
                extra={
                    "_bronze_source": SOURCE,
                    "_bronze_dataset": dataset,
                    "_bronze_load_ts": load_ts,
                    "_bronze_member": info.filename,
                    "_bronze_zip_sha256": got["sha256"],
                },
            )
            store.record_artifact(
                SOURCE, dataset, load_ts, member_path, kind="parsed",
                size=meta["bytes"], row_count=meta["rows"],
            )
            members.append({"member": info.filename, "rows": meta["rows"],
                            "columns": meta["columns"], "encoding": meta["encoding"]})
            log.info("%d: %-28s %8d rows x %3d cols (%s)",
                     year, info.filename, meta["rows"], meta["columns"], meta["encoding"])

    # WATERMARK ADVANCE -- AFTER the durable write, never before.
    # The ZIP and every parquet member are fsynced into the partition and
    # recorded in bronze_manifest by this point. Recording the new sha256 before
    # the files landed would make the next run believe this year is current and
    # skip it forever: the restatement would be lost permanently and silently,
    # since nothing downstream can tell a missing revision from a year that was
    # never revised. Advancing after can at worst re-download ~34MB. See
    # WatermarkStore.advance.
    store.advance(
        SOURCE, dataset,
        {
            "year": year,
            "sha256": got["sha256"],
            "last_modified": head["last_modified"],
            "etag": head["etag"],
            "bytes": got["bytes"],
            "current_load_ts": load_ts,
            "refresh_reason": reason,
            "members": len(members),
        },
        rows_in_batch=sum(m["rows"] for m in members),
        load_ts=load_ts,
        note=f"full refresh ({reason})",
    )

    return {
        "year": year,
        "action": "full_refresh",
        "reason": reason,
        "load_ts": load_ts,
        "partition": str(partition),
        "sha256": got["sha256"],
        "last_modified": head["last_modified"],
        "bytes": got["bytes"],
        "members": members,
        "prior_partitions": store.load_partitions(SOURCE, dataset)[:-1],
    }


def check_only(client: HttpClient, store: WatermarkStore, target: Iterable[int]) -> list[dict]:
    """HEAD every year and report what a real run would do. No downloads."""
    report = []
    for year in target:
        head = probe(client, year)
        cursor = store.cursor(SOURCE, str(year))
        if not cursor.get("sha256"):
            verdict = "never loaded"
        elif cursor.get("last_modified") != head["last_modified"]:
            verdict = (f"header moved: {cursor.get('last_modified')} -> "
                       f"{head['last_modified']} (would download and hash)")
        else:
            verdict = "unchanged"
        report.append(head | {"verdict": verdict, "known_sha256": cursor.get("sha256")})
        log.info("%d: %s", year, verdict)
    return report


def main(argv: Iterable[str] | None = None) -> int:
    ap = argparse.ArgumentParser(
        prog="python -m src.ingest.fars",
        description="FARS annual bulk ingest with hash-based restatement detection.",
    )
    ap.add_argument("--years", type=int, nargs="+", default=None,
                    help=f"default: {years()} from config/sources.toml")
    ap.add_argument("--check-only", action="store_true",
                    help="HEAD each year and report what would change; no downloads")
    ap.add_argument("--force", action="store_true",
                    help="download and re-materialise even if the hash is unchanged")
    ap.add_argument("--reset", action="store_true",
                    help="forget the recorded hashes; bronze partitions are kept")
    ap.add_argument("--pace", type=float, default=0.5)
    ap.add_argument("--db", default=None)
    ap.add_argument("-v", "--verbose", action="store_true")
    args = ap.parse_args(list(argv) if argv is not None else None)

    logging.basicConfig(
        level=logging.DEBUG if args.verbose else logging.INFO,
        format="%(asctime)s %(levelname)-7s %(name)s %(message)s",
    )

    target = args.years or years()
    store = WatermarkStore(args.db) if args.db else WatermarkStore()
    if args.reset:
        for year in target:
            store.reset(SOURCE, str(year))
        log.info("reset hashes for %s", target)

    with HttpClient(min_interval=args.pace, timeout=300.0) as client:
        if args.check_only:
            report = check_only(client, store, target)
        else:
            report = [ingest_year(y, store=store, client=client, force=args.force)
                      for y in target]
        stats = dict(client.stats)

    print(json.dumps({"http": stats, "years": report}, indent=2, default=str))
    return 0


if __name__ == "__main__":
    sys.exit(main())
