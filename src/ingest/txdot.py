"""TxDOT CRIS -- ArcGIS FeatureServer bronze ingest.

    python -m src.ingest.txdot                 # bounded slice (default)
    python -m src.ingest.txdot --full          # the whole 3.09M-row sweep

The layer is published as "Bicycle Involved Crashes" and its own service
description string is `txdot_ph_2.automated_sw.cris_crash`. `where=1=1` returns
3,088,450 features against ~14,535 with `bicyclist_involved_fl=1`: it is the
full statewide CRIS crash table under a bicyclist label. Whether it *should* be
used is a memo question, not an ingestion question. This module's job is to get
it right if it is used at all.

Bronze layout:

    data/bronze/txdot/cris_crash/{load_ts}/page_00001.json.gz    raw bytes
    data/bronze/txdot/cris_crash/{load_ts}/page_00001.parquet    parsed
    data/bronze/txdot/cris_crash/{load_ts}/oid_snapshot_*.json.gz

Pages are gzipped because a 2,000-feature page is ~8.6MB of JSON and a full
sweep is ~13GB uncompressed. The sha256 recorded in `bronze_manifest` is always
over the *uncompressed* bytes, so byte-for-byte fidelity stays checkable:
`gunzip -c page_00001.json.gz | shasum -a 256`.


Pagination: OBJECTID keyset, not resultOffset
---------------------------------------------
    where=ESRI_OID > {last} AND ESRI_OID <= {ceiling}
    orderByFields=ESRI_OID
    resultRecordCount=2000

Two failure modes are being avoided at once.

`resultOffset` is positional against a table that is being written to. Skip the
first N rows of a result set that has since gained or lost a row behind your
cursor and you silently miss or repeat rows -- the assignment says this is
checked specifically. Keyset asks for "everything after this OID" which is
stable regardless of what happens behind the cursor.

Unindexed WHERE predicates time out server-side. `crash_date` here is
`esriFieldTypeString` (verified in the layer descriptor), so a date range
filter is a string comparison across 3.09M rows with no index behind it. The
OID is the only reliably indexed predicate on this service, which makes it the
only one that can drive 1,545 sequential requests without the service starting
to return 500s. The field is named ESRI_OID rather than OBJECTID on this layer,
so it is read from the layer descriptor's `objectIdField` rather than hardcoded.

Measured live 2026-09-07: min OID 1, max OID 3,088,450, count 3,088,450 -- the
OID space is contiguous and one-to-one with rows today. The code does not assume
that; gaps just mean shorter pages.


Restatement, and why a sweep restarts rather than advancing forever
------------------------------------------------------------------
`amend_supp_fl` means TxDOT amends reports in place. There is no server-side
mutation timestamp to watermark on -- unlike Socrata's `:updated_at`, this
service exposes nothing that says "this row changed" -- and the date fields that
might stand in for one are unindexed strings. So the only correct mechanism is a
full OID sweep plus a local row-hash diff: every feature gets a `_bronze_row_sha256`
over its canonical attributes+geometry, bronze keeps every version under its own
load_ts, and silver diffs hash-by-natural-key to find what actually changed.

Which is why the OID cursor is *within* a sweep, not across sweeps. It exists so
that a run interrupted at page 900 of 1,545 resumes at page 901 instead of
restarting -- that is the durable-watermark payoff. When a sweep completes, the
next run starts a new sweep from the floor under a new load_ts, because a
restatement can land on any OID, including ones already passed.

Note also that ESRI_OID is service-assigned, not a TxDOT key. If the layer is
rebuilt the OIDs shift underneath us. That is survivable precisely because the
cursor's lifetime is one sweep: the natural key silver diffs on is `crash_id`.

Bronze does NOT set `outSR`. Geometry arrives in the service's native
wkid 102603 / latestWkid 3081 (NAD83 Texas Centric Albers Equal Area) and is
stored that way; asking the server to reproject would be a transformation, and
transformations belong in silver where the CRS choice is documented and testable.


Scope
-----
A full sweep is 1,545 pages at ~10.7s and ~8.6MB each: ~4.6 hours and ~13GB of
raw JSON, measured against the live service. The default run is therefore a
bounded slice -- `--max-pages 50`, the first 100,000 OIDs -- which exercises the
identical pagination path and generalises unchanged; `--full` removes the bound
and `--oid-min/--oid-max` place it anywhere in the OID space. The bound is
recorded in the watermark cursor, so a slice can never be mistaken later for a
complete sweep. See DECISIONS.md.

CRSS is deliberately not touched here or anywhere else in the pipeline: it is a
probability sample with PSU/stratum design variables and cannot be unioned with
this table or with FARS.
"""

from __future__ import annotations

import argparse
import json
import logging
import sys
from typing import Any, Iterable

from .. import config
from .http import HttpClient
from .watermark import (
    WatermarkStore,
    bronze_partition,
    new_load_ts,
    new_partition,
    row_sha256,
    write_bytes,
    write_rows_parquet,
)

log = logging.getLogger("ingest.txdot")

SOURCE = "txdot"

# Named for the service description string (txdot_ph_2.automated_sw.cris_crash)
# rather than for the layer's misleading "Bicycle Involved Crashes" title.
DATASET = "cris_crash"

DEFAULT_MAX_PAGES = 50          # 100k OIDs at the service's 2,000 maxRecordCount
OID_SNAPSHOT_CHUNK = 100_000    # returnIdsOnly window; see snapshot_oids()
PACE_SECONDS = 0.5


def layer_url() -> str:
    return config.sources()["txdot"]["feature_server"].rstrip("/")


def fetch_layer_metadata(client: HttpClient) -> dict[str, Any]:
    """The layer descriptor: OID field name, maxRecordCount, field types."""
    meta = client.get_json(layer_url(), params={"f": "json"})
    if "error" in meta:
        raise RuntimeError(f"layer descriptor error: {meta['error']}")
    return meta


def query(client: HttpClient, params: dict[str, Any]):
    """One /query call. Returns the raw Response -- bronze wants the bytes."""
    return client.get(f"{layer_url()}/query", params={"f": "json", **params})


def oid_extent(client: HttpClient, oid_field: str) -> dict[str, int]:
    """Universe size and OID bounds, in two cheap server-side aggregates.

    Doing this before the sweep gives an expected row count to check the sweep
    against, which is the only way to notice that a page silently came back
    short. `returnIdsOnly` over the whole layer is not viable for this -- the
    unbounded call runs past 240s and truncates mid-array (verified) -- hence
    the chunked snapshot in snapshot_oids().
    """
    count = query(client, {"where": "1=1", "returnCountOnly": "true"}).json()
    stats = query(client, {
        "where": "1=1",
        "outStatistics": json.dumps([
            {"statisticType": "min", "onStatisticField": oid_field,
             "outStatisticFieldName": "mn"},
            {"statisticType": "max", "onStatisticField": oid_field,
             "outStatisticFieldName": "mx"},
        ]),
    }).json()
    attrs = stats["features"][0]["attributes"]
    return {
        "count": int(count["count"]),
        "min_oid": int(attrs["mn"]),
        "max_oid": int(attrs["mx"]),
    }


def snapshot_oids(
    client: HttpClient,
    oid_field: str,
    *,
    floor: int,
    ceiling: int,
    store: WatermarkStore,
    load_ts: str,
    chunk: int = OID_SNAPSHOT_CHUNK,
) -> list[int]:
    """Snapshot the OID universe for the slice being swept, in chunks.

    `returnIdsOnly=true` is ~8 bytes per row instead of ~4KB, so this pins down
    exactly which rows existed when the sweep started, for a fraction of the
    cost of the sweep itself. The sweep is then checked against it: any OID in
    the snapshot that no page returned is a hole, and holes are what silent
    pagination bugs look like.

    Chunked because the unbounded call does not survive: `where=1=1&
    returnIdsOnly=true` over all 3.09M ids exceeded a 240-second timeout and
    came back as a truncated JSON array.
    """
    oids: list[int] = []
    lo = floor
    part = 0
    while lo < ceiling:
        hi = min(lo + chunk, ceiling)
        response = query(client, {
            "where": f"{oid_field} > {lo} AND {oid_field} <= {hi}",
            "returnIdsOnly": "true",
        })
        part += 1
        path = bronze_partition(SOURCE, DATASET, load_ts) / f"oid_snapshot_{part:05d}.json.gz"
        meta = write_bytes(path, response.content, compress=True)
        payload = json.loads(response.content)
        got = payload.get("objectIds") or []
        store.record_artifact(
            SOURCE, DATASET, load_ts, path, kind="raw",
            sha256=meta["sha256"], size=meta["bytes"], row_count=len(got),
            source_url=response.url,
        )
        oids.extend(int(o) for o in got)
        log.info("oid snapshot %d..%d -> %d ids", lo, hi, len(got))
        lo = hi
    return oids


def sweep(
    *,
    store: WatermarkStore,
    client: HttpClient,
    oid_min: int | None = None,
    oid_max: int | None = None,
    page_size: int | None = None,
    max_pages: int | None = DEFAULT_MAX_PAGES,
    snapshot: bool = True,
) -> dict[str, Any]:
    """Run (or resume) one OID-keyset sweep into bronze."""
    meta = fetch_layer_metadata(client)
    oid_field = meta.get("objectIdField") or "OBJECTID"
    page_size = page_size or int(meta.get("maxRecordCount") or 2000)

    extent = oid_extent(client, oid_field)
    floor = extent["min_oid"] - 1 if oid_min is None else oid_min
    ceiling = extent["max_oid"] if oid_max is None else oid_max

    prior = store.get(SOURCE, DATASET)
    cursor = prior["cursor"] if prior else {}
    resuming = bool(cursor) and cursor.get("status") == "in_progress"

    if resuming:
        # A resume continues the sweep that was interrupted, on that sweep's
        # own OID range. Honouring a different --oid-min/--oid-max here would
        # produce a partition whose pages came from two different ranges and
        # whose completeness could not be reasoned about. Use --reset to
        # abandon the in-progress sweep and start a new one on a new range.
        if (oid_min is not None and oid_min != cursor["oid_floor"]) or (
            oid_max is not None and oid_max != cursor["oid_ceiling"]
        ):
            log.warning(
                "ignoring --oid-min/--oid-max: resuming sweep %s on its own "
                "range (%d, %d]. Use --reset to start a new sweep.",
                cursor["sweep_id"], cursor["oid_floor"], cursor["oid_ceiling"],
            )
        load_ts = cursor["sweep_id"]
        floor = cursor["oid_floor"]
        ceiling = cursor["oid_ceiling"]
        page_size = int(cursor.get("page_size") or page_size)
        last_oid = cursor["last_objectid"]
        page_no = int(cursor.get("pages", 0))
        rows_total = int(cursor.get("rows", 0))
        log.info(
            "resuming sweep %s at OID %d (page %d, %d rows already in bronze)",
            load_ts, last_oid, page_no, rows_total,
        )
    else:
        load_ts = new_partition(SOURCE, DATASET, new_load_ts()).name
        last_oid = floor
        page_no = 0
        rows_total = 0
        log.info("new sweep %s over OIDs (%d, %d]", load_ts, floor, ceiling)

    bounded = max_pages is not None or oid_min is not None or oid_max is not None
    if resuming:
        # Sticky: a partition that was ever bounded is not a complete sweep of
        # the universe, and resuming it under a different bound does not make
        # it one. Silver must never mistake a slice for a census.
        bounded = bool(cursor.get("bounded")) or bounded
    partition = bronze_partition(SOURCE, DATASET, load_ts)

    snapshot_oids_list: list[int] = []
    if snapshot and not resuming:
        # Bound the snapshot to what this run will actually sweep, so a bounded
        # slice does not pay for a full-universe snapshot it cannot check.
        snap_ceiling = min(ceiling, floor + page_size * max_pages) if max_pages else ceiling
        snapshot_oids_list = snapshot_oids(
            client, oid_field, floor=floor, ceiling=snap_ceiling,
            store=store, load_ts=load_ts,
        )

    seen_oids: set[int] = set()
    pages_this_run = 0

    def sweep_cursor(*, complete: bool) -> dict[str, Any]:
        return {
            "sweep_id": load_ts,
            "status": "complete" if complete else "in_progress",
            "last_objectid": last_oid,
            "oid_floor": floor,
            "oid_ceiling": ceiling,
            "oid_field": oid_field,
            "page_size": page_size,
            "pages": page_no,
            "rows": rows_total,
            "bounded": bounded,
            "max_pages": max_pages,
            "universe_count": extent["count"],
            "universe_max_oid": extent["max_oid"],
        }

    while max_pages is None or pages_this_run < max_pages:
        response = query(client, {
            "where": f"{oid_field} > {last_oid} AND {oid_field} <= {ceiling}",
            "orderByFields": oid_field,
            "resultRecordCount": page_size,
            "outFields": "*",
            "returnGeometry": "true",
        })
        raw = response.content  # verbatim response body -- bronze gets this

        page_no += 1
        pages_this_run += 1
        raw_path = partition / f"page_{page_no:05d}.json.gz"
        raw_meta = write_bytes(raw_path, raw, compress=True)

        payload = json.loads(raw)  # parse only after the bytes are down
        if "error" in payload:
            raise RuntimeError(f"query error on page {page_no}: {payload['error']}")
        features = payload.get("features") or []

        if not features:
            store.record_artifact(
                SOURCE, DATASET, load_ts, raw_path, kind="raw",
                sha256=raw_meta["sha256"], size=raw_meta["bytes"], row_count=0,
                source_url=response.url,
            )
            # WATERMARK ADVANCE -- AFTER the durable write, never before.
            # An empty page is the end of the OID range. Closing the sweep here
            # is what lets the *next* run start a fresh sweep under a new
            # load_ts; without it the cursor would stay in_progress forever and
            # the full re-sweep that detects amend_supp_fl restatements would
            # never happen. See WatermarkStore.advance.
            store.advance(
                SOURCE, DATASET, sweep_cursor(complete=True),
                rows_in_batch=0, load_ts=load_ts, note=f"page {page_no} empty",
            )
            log.info("page %d empty -- OID space exhausted at %d", page_no, last_oid)
            break

        rows = []
        for feature in features:
            attrs = dict(feature.get("attributes") or {})
            geometry = feature.get("geometry")
            rows.append(
                attrs | {
                    # Geometry is flattened but NOT reprojected -- native
                    # wkid 102603 / 3081. Silver decides the CRS.
                    "_geometry_x": None if geometry is None else geometry.get("x"),
                    "_geometry_y": None if geometry is None else geometry.get("y"),
                    "_geometry_wkid": payload.get("spatialReference", {}).get("latestWkid"),
                    "_bronze_source": SOURCE,
                    "_bronze_dataset": DATASET,
                    "_bronze_load_ts": load_ts,
                    "_bronze_page": f"{page_no:05d}",
                    "_bronze_raw_path": raw_path.name,
                    # The restatement diff key. Covers attributes and geometry,
                    # so an amended report changes its hash even if only a
                    # coordinate moved.
                    "_bronze_row_sha256": row_sha256(
                        {**attrs, "_geom": geometry}
                    ),
                }
            )

        parquet_path = partition / f"page_{page_no:05d}.parquet"
        pq_meta = write_rows_parquet(parquet_path, rows)

        store.record_artifact(
            SOURCE, DATASET, load_ts, raw_path, kind="raw",
            sha256=raw_meta["sha256"], size=raw_meta["bytes"], row_count=len(rows),
            source_url=response.url,
        )
        store.record_artifact(
            SOURCE, DATASET, load_ts, parquet_path, kind="parsed",
            size=pq_meta["bytes"], row_count=len(rows),
        )

        page_oids = [int(a[oid_field]) for a in (f["attributes"] for f in features)
                     if a.get(oid_field) is not None]
        seen_oids.update(page_oids)
        last_oid = max(page_oids) if page_oids else last_oid
        rows_total += len(rows)

        # ArcGIS sets exceededTransferLimit iff more records match beyond this
        # page, so it -- not a short page -- is the authoritative end signal: a
        # page can be exactly resultRecordCount rows long and still be the last.
        # Only when the service omits the flag entirely do we fall back to page
        # length, and reaching the ceiling ends the slice either way.
        limit_flag = payload.get("exceededTransferLimit")
        exhausted = (
            (len(rows) < page_size) if limit_flag is None else (not limit_flag)
        ) or last_oid >= ceiling
        # WATERMARK ADVANCE -- AFTER the durable write, never before.
        # Both artifacts are fsynced and in bronze_manifest by now. Advancing
        # first and dying here would resume the sweep past OIDs whose page was
        # never written: those rows would be absent from this sweep's partition
        # with nothing anywhere reporting a problem. Advancing after can at
        # worst re-fetch one page into the same partition, overwriting an
        # identical file. See WatermarkStore.advance.
        store.advance(
            SOURCE, DATASET, sweep_cursor(complete=exhausted),
            rows_in_batch=len(rows), load_ts=load_ts, note=f"page {page_no}",
        )

        log.info(
            "page %d rows=%d last_oid=%d total=%d%s",
            page_no, len(rows), last_oid, rows_total,
            " (final page)" if exhausted else "",
        )
        if exhausted:
            break

    complete = store.cursor(SOURCE, DATASET).get("status") == "complete"
    if not complete:
        log.warning(
            "sweep %s left IN_PROGRESS at OID %d -- bounded run. Re-run to "
            "continue from exactly here; the partition and page numbering "
            "carry on in place.", load_ts, last_oid,
        )

    missing = sorted(set(snapshot_oids_list) - seen_oids) if snapshot_oids_list else []
    if snapshot_oids_list:
        # Completeness check against the pre-sweep OID snapshot. Only meaningful
        # for the range the sweep actually covered.
        covered = [o for o in snapshot_oids_list if floor < o <= last_oid]
        holes = sorted(set(covered) - seen_oids)
        log.info(
            "snapshot check: %d OIDs snapshotted, %d covered by this run, %d holes",
            len(snapshot_oids_list), len(covered), len(holes),
        )
        if holes:
            log.error("PAGINATION HOLES at OIDs %s%s", holes[:20],
                      " ..." if len(holes) > 20 else "")
        missing = holes

    return {
        "source": SOURCE,
        "dataset": DATASET,
        "load_ts": load_ts,
        "partition": str(partition),
        "oid_field": oid_field,
        "page_size": page_size,
        "pages_this_run": pages_this_run,
        "pages_total": page_no,
        "rows_total": rows_total,
        "last_objectid": last_oid,
        "oid_range": [floor, ceiling],
        "bounded": bounded,
        "sweep_complete": complete,
        "universe": extent,
        "snapshot_holes": missing[:100],
    }


def main(argv: Iterable[str] | None = None) -> int:
    ap = argparse.ArgumentParser(
        prog="python -m src.ingest.txdot",
        description="OBJECTID-keyset ingest of the TxDOT CRIS FeatureServer.",
    )
    ap.add_argument("--full", action="store_true",
                    help="remove the page bound and sweep all ~3.09M rows "
                         "(~1,545 pages, ~4.6h, ~13GB raw before gzip)")
    ap.add_argument("--max-pages", type=int, default=DEFAULT_MAX_PAGES,
                    help=f"pages per run (default {DEFAULT_MAX_PAGES}); the "
                         "sweep resumes from its cursor on the next run")
    ap.add_argument("--oid-min", type=int, default=None,
                    help="exclusive OID floor for the slice")
    ap.add_argument("--oid-max", type=int, default=None,
                    help="inclusive OID ceiling for the slice")
    ap.add_argument("--page-size", type=int, default=None,
                    help="default: the layer's own maxRecordCount (2000)")
    ap.add_argument("--no-oid-snapshot", action="store_true",
                    help="skip the returnIdsOnly universe snapshot")
    ap.add_argument("--reset", action="store_true",
                    help="abandon an in-progress sweep and start a new one")
    ap.add_argument("--pace", type=float, default=PACE_SECONDS)
    ap.add_argument("--db", default=None)
    ap.add_argument("-v", "--verbose", action="store_true")
    args = ap.parse_args(list(argv) if argv is not None else None)

    logging.basicConfig(
        level=logging.DEBUG if args.verbose else logging.INFO,
        format="%(asctime)s %(levelname)-7s %(name)s %(message)s",
    )

    store = WatermarkStore(args.db) if args.db else WatermarkStore()
    if args.reset:
        store.reset(SOURCE, DATASET)
        log.info("watermark reset; bronze partitions left intact")

    with HttpClient(min_interval=args.pace, timeout=180.0) as client:
        summary = sweep(
            store=store, client=client,
            oid_min=args.oid_min, oid_max=args.oid_max,
            page_size=args.page_size,
            max_pages=None if args.full else args.max_pages,
            snapshot=not args.no_oid_snapshot,
        )
        summary["http"] = dict(client.stats)

    print(json.dumps(summary, indent=2))
    return 0


if __name__ == "__main__":
    sys.exit(main())
