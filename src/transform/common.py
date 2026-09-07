"""Shared silver infrastructure: partition discovery, SCD2, deterministic writes.

Every silver table in this pipeline is built the same way, and this module is
that way. The three source transforms differ only in their conformed-column SQL
and in which SCD2 mode they ask for.


Engine: DuckDB, in-process
--------------------------
Bronze is parquet on local disk at ~10^5-10^6 rows per table. DuckDB reads it
with `read_parquet(..., union_by_name=true)` -- which is not a convenience here
but a requirement, because Socrata omits null keys so the column set genuinely
differs between pages, and FARS's accident file has 96 columns in 2019 and 85
in 2024. Typing, dedupe, versioning and aggregation are all one SQL statement
per table with no data crossing into Python.

Python does exactly two things: the substance grammar (a parser, unit-testable,
materialised into a lookup table and joined -- see `dictionaries.py`) and the
parquet write. Nothing else round-trips.

Spark is the escape hatch for national multi-year scale -- all 50 states,
1975-2024, ~10^8 rows -- and it is not used because at three states and 10^5-10^6
rows the entire silver build finishes in seconds on one core, and a cluster
would add scheduling latency, a serialisation boundary and an operational
surface to a workload that fits in L3 cache.


Determinism
-----------
Silver must be a pure function of its bronze inputs: two builds over the same
bronze must produce byte-identical parquet. Everything below serves that.

  * `valid_from` is a SOURCE stamp, never `now()`. Montgomery has `:updated_at`;
    TxDOT and FARS publish no mutation timestamp, so the bronze `_bronze_load_ts`
    that first carried a given row hash stands in. Both are properties of the
    input, so a rebuild reproduces them.
  * Wall-clock time appears in exactly one place, `_build_manifest.json`, which
    is excluded from the byte-identity check by construction (it is a JSON file,
    not a parquet table).
  * Every write is `ORDER BY` a key the writer PROVES is unique. This is the
    part that actually bites: a non-total ORDER BY is not an error in SQL, it is
    just a tie, and the tie is broken by whichever thread finished first.
    Measured on this data: `ORDER BY :id` over a Drivers relation with 12,368
    duplicate ids produced three different sha256s at threads=1/2/8, under BOTH
    the DuckDB COPY writer and the pyarrow writer. With a total order, both
    writers produced one hash at all three thread counts. `write_parquet()`
    below therefore asserts uniqueness of the sort key rather than trusting it.
  * Column order is fixed by the contract, not by SELECT *.

Writer: pyarrow, not DuckDB `COPY`. Once the sort key is total both are stable,
so this is not a correctness fix -- it buys three things instead. It pins
`row_group_size` and compression explicitly rather than inheriting DuckDB's
internal row-group heuristic (a version-dependent implementation detail, not a
contract). It writes through Phase 1's `durable_replace`, so silver gets the
same fsync-then-rename durability bronze has. And it materialises the arrow
table once, which is the same object the contract validator inspected -- there
is no second execution of the query between validation and write.


Versioning: bronze snapshot partitions + hash-diff SCD2
-------------------------------------------------------
One mechanism covers TxDOT amendments and FARS reissues, because they are the
same event: a stable natural key whose attributes changed between two bronze
partitions. `row_hash` is computed over the CONFORMED attribute columns only --
never over `_bronze_*` metadata and never over the raw serialisation -- so a
re-download that reorders JSON keys, or a new `_bronze_load_ts`, does not
manufacture a version.

Two modes, because the sources differ in what a partition MEANS:

  DELTA (Montgomery). A partition is whatever the keyset walk returned since the
  last cursor. Absence from a partition means nothing, so deletions are NOT
  observable -- if the county deletes a report, no incremental read will ever
  tell us. Saying so is the honest answer; inferring deletion from absence here
  would delete most of the table on every run.

  SNAPSHOT (TxDOT, FARS). A partition is a complete re-read of a declared scope:
  a FARS year, or a TxDOT OID range. Absence from a *later* snapshot of the
  same scope is therefore evidence of deletion, and the version is closed with
  `deleted_in_load_ts` set. TxDOT gates this on the ingest cursor's sticky
  `bounded` flag and on the OID range actually swept, because a partition that
  covered OIDs 1-100,000 is not evidence about OID 2,000,000.
"""

from __future__ import annotations

import hashlib
import json
import logging
import os
import re
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterable, Sequence

import duckdb
import pyarrow as pa
import pyarrow.parquet as pq
from pyproj import Geod

from ..config import BRONZE_DIR, SILVER_DIR, envelope
from ..ingest.watermark import WatermarkStore, durable_replace

log = logging.getLogger(__name__)

# WGS84 ellipsoid. Used only for the out-of-envelope distance report and the
# TxDOT coordinate-pair discrepancy: both are DISTANCES, so they are computed
# geodesically on the ellipsoid rather than in any projected CRS. That sidesteps
# the projection question entirely -- there is no zone to pick, no scale factor
# to apologise for, and it is correct for a Maryland pair and a Texas pair with
# the same call. EPSG:3857 would be wrong by 1/cos(lat) (~29% at 39N); even
# EPSG:26985 would need a different CRS for the Texas numbers.
GEOD = Geod(ellps="WGS84")

# ~120k rows per row group. Parquet's default is 1M, which for these tables
# would be one row group per file and no row-group pruning at all; 120k keeps
# 2-4 groups on the largest silver table while staying well above the point
# where per-group metadata starts to matter. Phase 4's bbox covering column
# prunes on these boundaries.
ROW_GROUP_SIZE = 122_880

GEO_OK = "OK"
GEO_OUT_OF_ENVELOPE = "OUT_OF_ENVELOPE"
GEO_SENTINEL = "SENTINEL"
GEO_MISSING = "MISSING"
GEO_QUALITY_VALUES = (GEO_OK, GEO_OUT_OF_ENVELOPE, GEO_SENTINEL, GEO_MISSING)

SOURCE_SYSTEMS = {
    "montgomery": "MONTGOMERY_MD",
    "txdot": "TXDOT_CRIS",
    "fars": "NHTSA_FARS",
}

_IDENT = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")


def quote_ident(name: str) -> str:
    """Double-quote an identifier. Socrata columns are literally `:id`."""
    return '"' + name.replace('"', '""') + '"'


def _safe_table(name: str) -> str:
    if not _IDENT.match(name):
        raise ValueError(f"unsafe SQL identifier: {name!r}")
    return name


# ---------------------------------------------------------------------------
# partition discovery
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class Partition:
    """One bronze partition: a source/dataset/load_ts directory on disk."""

    source: str
    dataset: str
    load_ts: str
    path: Path
    in_manifest: bool
    cursor: dict[str, Any] = field(default_factory=dict)

    def glob(self, pattern: str = "*.parquet") -> str:
        return str(self.path / pattern)


def discover_partitions(
    source: str,
    dataset: str,
    *,
    bronze_root: Path | None = None,
    store: WatermarkStore | None = None,
) -> list[Partition]:
    """Every bronze partition for a dataset, oldest first.

    The union of two sources of truth, deliberately:

      * the watermark store's `bronze_manifest` (authoritative about what was
        written durably, and the only place the ingest cursor lives), and
      * the on-disk tree (authoritative about what is actually readable now).

    Neither alone is right. A partition on disk but not in the manifest is a
    crash between write and record, or a tree copied from elsewhere -- it holds
    real rows and dropping it would silently lose data. A partition in the
    manifest but not on disk has been archived or pruned; it is skipped with a
    warning rather than failing the build, because bronze retention is an
    operational decision silver does not get to veto.

    Nothing is hardcoded. Adding a FARS year or a second TxDOT sweep changes
    what this returns without any code change, which is the requirement.
    """
    root = Path(bronze_root) if bronze_root is not None else BRONZE_DIR
    dataset_dir = root / source / dataset

    on_disk = {
        p.name: p
        for p in sorted(dataset_dir.iterdir())
        if p.is_dir() and not p.name.startswith("_")
    } if dataset_dir.is_dir() else {}

    in_manifest: set[str] = set()
    cursor: dict[str, Any] = {}
    if store is not None:
        try:
            in_manifest = set(store.load_partitions(source, dataset))
            cursor = store.cursor(source, dataset)
        except Exception as exc:  # a missing/locked store must not fail a rebuild
            log.warning("watermark store unavailable for %s/%s: %s", source, dataset, exc)

    for missing in sorted(in_manifest - set(on_disk)):
        log.warning(
            "bronze partition %s/%s/%s is in the manifest but not on disk -- skipping",
            source, dataset, missing,
        )

    out = [
        Partition(
            source=source,
            dataset=dataset,
            load_ts=load_ts,
            path=path,
            in_manifest=load_ts in in_manifest,
            cursor=cursor,
        )
        for load_ts, path in sorted(on_disk.items())
    ]
    for p in out:
        if not p.in_manifest and store is not None:
            log.warning(
                "bronze partition %s/%s/%s is on disk but not in the manifest -- "
                "reading it anyway (see discover_partitions docstring)",
                source, dataset, p.load_ts,
            )
    return out


def discover_datasets(source: str, *, bronze_root: Path | None = None) -> list[str]:
    """Dataset directories under one source. FARS's are years."""
    root = (Path(bronze_root) if bronze_root is not None else BRONZE_DIR) / source
    if not root.is_dir():
        return []
    return sorted(p.name for p in root.iterdir() if p.is_dir() and not p.name.startswith("_"))


def bronze_view(
    con: duckdb.DuckDBPyConnection,
    name: str,
    partitions: Sequence[Partition],
    *,
    pattern: str = "*.parquet",
) -> str:
    """Create a view over every partition's parquet and return its name.

    `union_by_name=true` is mandatory, not optional: Socrata omits keys whose
    value is null so page N and page N+1 genuinely have different column sets,
    and FARS's accident file lost 11 columns between 2019 and 2024.
    """
    _safe_table(name)
    globs = [p.glob(pattern) for p in partitions]
    if not globs:
        raise FileNotFoundError(f"no bronze partitions for view {name}")
    files = ", ".join("'" + g.replace("'", "''") + "'" for g in globs)
    con.execute(
        f"CREATE OR REPLACE VIEW {name} AS "
        f"SELECT * FROM read_parquet([{files}], union_by_name=true, filename=false)"
    )
    return name


def connect(threads: int | None = None) -> duckdb.DuckDBPyConnection:
    """An in-process DuckDB tuned for this build.

    `preserve_insertion_order` stays on (the default): every write is ordered
    explicitly and the writer asserts the order is total, so turning it off
    would buy a little memory in exchange for the one property the build cannot
    lose.
    """
    con = duckdb.connect()
    if threads is not None:
        con.execute(f"SET threads={int(threads)}")
    con.execute("SET preserve_insertion_order=true")
    return con


# ---------------------------------------------------------------------------
# SCD2
# ---------------------------------------------------------------------------

MODE_DELTA = "delta"
MODE_SNAPSHOT = "snapshot"

# Columns every history table carries, in this order, after the natural key and
# the conformed attributes. Fixed here so the contract, the writer and the
# reader cannot drift apart.
SCD2_COLUMNS = (
    "natural_key",
    "row_hash",
    "valid_from",
    "valid_to",
    "is_current",
    "version_no",
    "deleted_in_load_ts",
    "_bronze_load_ts",
    "_bronze_row_sha256",
    "_bronze_raw_path",
)


def scd2_sql(
    *,
    source_relation: str,
    natural_key: Sequence[str],
    attributes: Sequence[str],
    valid_from_expr: str,
    load_ts_expr: str = "_bronze_load_ts",
    row_sha_expr: str = "_bronze_row_sha256",
    raw_path_expr: str = "_bronze_raw_path",
    mode: str = MODE_DELTA,
    snapshot_scope: str | None = None,
    dedupe_order: str | None = None,
) -> str:
    """SQL that turns versioned bronze rows into an SCD2 history table.

    `source_relation` must already expose the conformed attribute columns; this
    function does no typing. Returns one row per (natural key, version).

    natural_key      conformed columns forming the key, e.g. ["crash_id"]
    attributes       conformed columns that a change to constitutes a new version
    valid_from_expr  SQL for the version's start stamp. Montgomery: the source's
                     own `:updated_at`. TxDOT/FARS: the load_ts that first
                     carried this hash -- they publish no mutation stamp, so the
                     earliest moment we can honestly say the value held is when
                     we first saw it.
    mode             MODE_DELTA (no deletion inference) or MODE_SNAPSHOT
    snapshot_scope   SQL expression naming the scope a snapshot is complete for
                     (FARS: the year; TxDOT: a constant per swept OID range).
                     Required for MODE_SNAPSHOT.
    dedupe_order     extra ORDER BY applied when one (key, valid_from) appears
                     more than once. Defaults to preferring the newest bronze
                     partition, then the lexically greatest row hash so the
                     choice is deterministic rather than merely arbitrary.

    The four stages below are separate CTEs on purpose: each one is a query a
    reviewer can run on its own to see what it removed.
    """
    if mode not in (MODE_DELTA, MODE_SNAPSHOT):
        raise ValueError(f"unknown scd2 mode {mode!r}")
    if mode == MODE_SNAPSHOT and not snapshot_scope:
        raise ValueError("MODE_SNAPSHOT requires snapshot_scope")

    key_cols = ", ".join(quote_ident(k) for k in natural_key)
    key_expr = " || '|' || ".join(f"coalesce(CAST({quote_ident(k)} AS VARCHAR), '')"
                                  for k in natural_key)
    attr_cols = ", ".join(quote_ident(a) for a in attributes)
    # row_hash over the CONFORMED attributes only. Not over _bronze_* metadata
    # (a new load_ts is not a change) and not over the raw payload (a key
    # reorder is not a change). Null is hashed as a distinguishable marker so
    # NULL and the literal string 'NULL' do not collide.
    hash_expr = "md5(" + " || '\\u001f' || ".join(
        f"coalesce(CAST({quote_ident(a)} AS VARCHAR), '\\u0000')" for a in attributes
    ) + ")"

    dedupe = dedupe_order or f"{load_ts_expr} DESC, {row_sha_expr} DESC"

    if mode == MODE_SNAPSHOT:
        # A key is deleted when it is absent from a snapshot of its own scope
        # that is NEWER than the last snapshot it appeared in. Scoping matters:
        # a TxDOT sweep of OIDs 1-100,000 says nothing about OID 2,000,000, and
        # the FARS 2023 file says nothing about 2024.
        deletion_cte = f"""
        , scopes AS (
            SELECT DISTINCT {snapshot_scope} AS scope, {load_ts_expr} AS load_ts
            FROM {source_relation}
        )
        , key_scope AS (
            SELECT nk, scope, MAX(load_ts) AS last_seen_load_ts
            FROM (SELECT {key_expr} AS nk, {snapshot_scope} AS scope,
                         {load_ts_expr} AS load_ts FROM {source_relation})
            GROUP BY 1, 2
        )
        , deletions AS (
            SELECT k.nk, MIN(s.load_ts) AS deleted_in_load_ts
            FROM key_scope k
            JOIN scopes s ON s.scope = k.scope AND s.load_ts > k.last_seen_load_ts
            GROUP BY 1
        )
        """
        deletion_join = "LEFT JOIN deletions d ON d.nk = v.natural_key"
        deleted_expr = "d.deleted_in_load_ts"
    else:
        deletion_cte = ""
        deletion_join = ""
        deleted_expr = "CAST(NULL AS VARCHAR)"

    return f"""
    WITH tagged AS (
        SELECT
            {key_expr}                                  AS natural_key,
            {key_cols},
            {attr_cols},
            {hash_expr}                                 AS row_hash,
            CAST({valid_from_expr} AS VARCHAR)          AS valid_from,
            {load_ts_expr}                              AS _bronze_load_ts,
            {row_sha_expr}                              AS _bronze_row_sha256,
            {raw_path_expr}                             AS _bronze_raw_path
        FROM {source_relation}
    )
    -- 1. one row per (key, valid_from). The same record can arrive in two
    --    overlapping bronze partitions with identical content; keep the newest
    --    partition's copy so lineage points at the freshest raw file.
    , deduped AS (
        SELECT * FROM (
            SELECT *, row_number() OVER (
                PARTITION BY natural_key, valid_from ORDER BY {dedupe}
            ) AS rn
            FROM tagged
        ) WHERE rn = 1
    )
    -- 2. collapse consecutive versions whose conformed attributes are identical.
    --    This is what stops a re-ingest from manufacturing a version: the hash
    --    is over the attributes, so a new _bronze_load_ts alone changes nothing.
    , runs AS (
        SELECT *, lag(row_hash) OVER (
            PARTITION BY natural_key ORDER BY valid_from, _bronze_load_ts
        ) AS prev_hash
        FROM deduped
    )
    , changes AS (
        SELECT * EXCLUDE (prev_hash, rn)
        FROM runs
        WHERE prev_hash IS NULL OR prev_hash <> row_hash
    )
    -- 3. close each version at its successor's start.
    , versioned AS (
        SELECT
            *,
            lead(valid_from) OVER (
                PARTITION BY natural_key ORDER BY valid_from, _bronze_load_ts
            ) AS valid_to,
            row_number() OVER (
                PARTITION BY natural_key ORDER BY valid_from, _bronze_load_ts
            ) AS version_no
        FROM changes
    )
    {deletion_cte}
    -- 4. a version is current when nothing succeeds it AND the key was not
    --    absent from a later snapshot of its own scope.
    SELECT
        v.natural_key,
        {", ".join("v." + quote_ident(k) for k in natural_key)},
        {", ".join("v." + quote_ident(a) for a in attributes)},
        v.row_hash,
        v.valid_from,
        v.valid_to,
        (v.valid_to IS NULL AND {deleted_expr} IS NULL) AS is_current,
        CAST(v.version_no AS INTEGER) AS version_no,
        {deleted_expr} AS deleted_in_load_ts,
        v._bronze_load_ts,
        v._bronze_row_sha256,
        v._bronze_raw_path
    FROM versioned v
    {deletion_join}
    """


# ---------------------------------------------------------------------------
# deterministic writes
# ---------------------------------------------------------------------------


class NonTotalOrder(ValueError):
    """The requested sort key does not uniquely order the rows.

    Raised rather than warned. A non-total ORDER BY silently produces different
    bytes on different runs -- measured, see the module docstring -- and a
    pipeline whose whole idempotency claim rests on byte-identity cannot ship a
    table it merely hopes is ordered.
    """


def write_parquet(
    con: duckdb.DuckDBPyConnection,
    relation: str,
    dest: Path,
    *,
    columns: Sequence[str],
    order_by: Sequence[str],
    row_group_size: int = ROW_GROUP_SIZE,
) -> dict[str, Any]:
    """Materialise `relation` to `dest` deterministically. Returns its manifest row.

    `columns` fixes column order (the contract's order, not SELECT *'s).
    `order_by` must be a total order; this asserts it rather than assuming it.

    Writes to `dest.part` then `durable_replace` -- the same fsync-then-rename
    Phase 1 uses for bronze, so a crash mid-write can never leave a half-written
    parquet where a complete one should be.
    """
    col_sql = ", ".join(quote_ident(c) for c in columns)
    order_sql = ", ".join(quote_ident(c) for c in order_by)

    dup = con.execute(
        f"SELECT COUNT(*) FROM (SELECT {order_sql} FROM {relation} "
        f"GROUP BY {order_sql} HAVING COUNT(*) > 1)"
    ).fetchone()[0]
    if dup:
        raise NonTotalOrder(
            f"{dest.name}: ORDER BY ({', '.join(order_by)}) leaves {dup} tied group(s). "
            "The parquet bytes would depend on thread scheduling. Extend the sort key."
        )

    table = con.execute(
        f"SELECT {col_sql} FROM {relation} ORDER BY {order_sql}"
    ).to_arrow_table()

    dest.parent.mkdir(parents=True, exist_ok=True)
    tmp = dest.with_name(dest.name + ".part")
    pq.write_table(
        table,
        tmp,
        compression="zstd",
        row_group_size=row_group_size,
        # No file-level statistics timestamp, no writer-supplied created_by
        # nondeterminism beyond the pyarrow version string, which is a property
        # of the environment and therefore stable within one.
        store_schema=True,
    )
    with open(tmp, "rb") as fh:
        os.fsync(fh.fileno())
    durable_replace(tmp, dest)

    digest = hashlib.sha256(dest.read_bytes()).hexdigest()
    return {
        "path": str(dest),
        "rows": table.num_rows,
        "columns": len(columns),
        "bytes": dest.stat().st_size,
        "sha256": digest,
        "row_groups": pq.ParquetFile(dest).num_row_groups,
    }


# ---------------------------------------------------------------------------
# envelope + geodesy
# ---------------------------------------------------------------------------


def envelope_sql(source: str, lat: str, lon: str) -> str:
    """SQL predicate: is this coordinate inside the source's envelope?

    Degree comparison on EPSG:4326, no projection -- see config/geo.toml for why
    the envelope is a strict superset of the jurisdiction polygon.
    """
    env = envelope(source)
    return (
        f"({lat} IS NOT NULL AND {lon} IS NOT NULL "
        f"AND {lat} BETWEEN {env['min_lat']} AND {env['max_lat']} "
        f"AND {lon} BETWEEN {env['min_lon']} AND {env['max_lon']})"
    )


def geodesic_distance_m(lat1: float, lon1: float, lat2: float, lon2: float) -> float:
    """Metres between two WGS84 points, on the ellipsoid.

    Used for the out-of-envelope distance report and the TxDOT coordinate-pair
    discrepancy. Geodesic rather than projected because these pairs span
    Maryland and Texas and no single projected CRS is honest for both --
    and emphatically not EPSG:3857, whose scale error at 39N is ~1/cos(39) = 1.29.
    """
    _, _, dist = GEOD.inv(lon1, lat1, lon2, lat2)
    return abs(dist)


def envelope_centroid(source: str) -> tuple[float, float]:
    env = envelope(source)
    return (
        (env["min_lat"] + env["max_lat"]) / 2.0,
        (env["min_lon"] + env["max_lon"]) / 2.0,
    )


def distance_from_envelope_m(source: str, lat: float, lon: float) -> float:
    """Geodesic distance from a point to the nearest edge of the envelope.

    Zero inside. Outside, the shortest distance to the box -- computed by
    clamping the point onto the box and measuring to the clamped point, which is
    the correct nearest-point on a lat/lon rectangle and is what makes "114 rows
    outside, the furthest 180 km away" a sentence with a defined meaning.
    """
    env = envelope(source)
    clat = min(max(lat, env["min_lat"]), env["max_lat"])
    clon = min(max(lon, env["min_lon"]), env["max_lon"])
    if clat == lat and clon == lon:
        return 0.0
    return geodesic_distance_m(lat, lon, clat, clon)


# ---------------------------------------------------------------------------
# build manifest
# ---------------------------------------------------------------------------


@dataclass
class BuildManifest:
    """What went in, what came out, and the hashes of both.

    The ONLY artefact in silver that carries wall-clock time. That is why the
    byte-identity check is defined over the parquet files and not over the whole
    directory: `built_at` differs between two runs by construction, and pinning
    it would be lying about when the build happened.
    """

    silver_root: Path
    bronze_root: Path
    sources: list[str]
    built_at: str = field(default_factory=lambda: datetime.now(timezone.utc).isoformat())
    inputs: list[dict[str, Any]] = field(default_factory=list)
    outputs: dict[str, dict[str, Any]] = field(default_factory=dict)
    stats: dict[str, Any] = field(default_factory=dict)
    warnings: list[str] = field(default_factory=list)

    def add_input(self, partition: Partition, *, rows: int | None = None) -> None:
        self.inputs.append(
            {
                "source": partition.source,
                "dataset": partition.dataset,
                "load_ts": partition.load_ts,
                "path": str(partition.path),
                "in_manifest": partition.in_manifest,
                "rows": rows,
            }
        )

    def add_output(self, table: str, info: dict[str, Any]) -> None:
        self.outputs[table] = info

    def add_stat(self, key: str, value: Any) -> None:
        self.stats[key] = value

    def warn(self, message: str) -> None:
        log.warning("%s", message)
        self.warnings.append(message)

    def write(self, path: Path | None = None) -> Path:
        dest = path or (self.silver_root / "_build_manifest.json")
        dest.parent.mkdir(parents=True, exist_ok=True)
        payload = {
            "built_at": self.built_at,
            "bronze_root": str(self.bronze_root),
            "silver_root": str(self.silver_root),
            "sources": self.sources,
            "inputs": sorted(
                self.inputs, key=lambda r: (r["source"], r["dataset"], r["load_ts"])
            ),
            "outputs": dict(sorted(self.outputs.items())),
            "stats": dict(sorted(self.stats.items())),
            "warnings": self.warnings,
        }
        tmp = dest.with_name(dest.name + ".part")
        tmp.write_text(json.dumps(payload, indent=2, sort_keys=False, default=str) + "\n")
        durable_replace(tmp, dest)
        return dest


@dataclass
class BuildContext:
    """Everything a source transform needs, and nothing it should reach around."""

    con: duckdb.DuckDBPyConnection
    bronze_root: Path
    silver_root: Path
    manifest: BuildManifest
    store: WatermarkStore | None = None
    allow_unmapped: bool = False
    validate: bool = True

    def partitions(self, source: str, dataset: str) -> list[Partition]:
        return discover_partitions(
            source, dataset, bronze_root=self.bronze_root, store=self.store
        )

    def dest(self, source: str, table: str) -> Path:
        return self.silver_root / source / f"{table}.parquet"


def silver_root(root: Path | str | None = None) -> Path:
    return Path(root) if root is not None else SILVER_DIR


def table_path(root: Path, source: str, table: str) -> Path:
    return Path(root) / source / f"{table}.parquet"


def read_silver(con: duckdb.DuckDBPyConnection, root: Path, source: str, table: str) -> str:
    """Register a silver parquet as a view and return the view name."""
    view = f"{source}_{table}".replace("-", "_")
    _safe_table(view)
    path = str(table_path(root, source, table)).replace("'", "''")
    con.execute(f"CREATE OR REPLACE VIEW {view} AS SELECT * FROM read_parquet('{path}')")
    return view


def hash_files(paths: Iterable[Path]) -> dict[str, str]:
    """sha256 per file, for the byte-identity check."""
    return {str(p): hashlib.sha256(p.read_bytes()).hexdigest() for p in sorted(paths)}
