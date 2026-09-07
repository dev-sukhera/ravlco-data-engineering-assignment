"""Durable watermark store + the bronze durability primitives it guards.

Storage: a DuckDB database at `data/bronze/_watermarks.duckdb`. DuckDB because
it is already the analytical engine for the rest of the pipeline (one dependency,
one file, ACID transactions, and the store is queryable from the same SQL the
silver layer uses). Three tables:

  watermarks         one row per (source, dataset): the current cursor.
  watermark_history  append-only log of every advance -- lets you answer "when
                     did this dataset last move, and by how much" without
                     instrumenting the pipeline separately.
  bronze_manifest    one row per durably-written bronze artifact: path, sha256,
                     bytes, row count, and the upstream Last-Modified/ETag.
                     This is where FARS keeps the per-year hash it diffs against.

The cursor is source-shaped JSON, because the three sources have genuinely
different cursors and flattening them into shared columns would be a lie:

  montgomery  {"updated_at": "...", "id": "row-...", "inclusive": false}
  txdot       {"last_objectid": N, "oid_floor": N, "oid_ceiling": N, ...}
  fars        {"sha256": "...", "last_modified": "...", "current_load_ts": "..."}

Why the write helpers live here rather than in a separate bronze module: the
ordering guarantee in `advance()` is only worth anything if the write that
precedes it actually reached the disk. Keeping `write_bytes` / `write_rows_parquet`
next to `advance()` keeps both halves of that contract in one file where they can
be read together.
"""

from __future__ import annotations

import gzip
import hashlib
import json
import os
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterable, Sequence

import duckdb
import pyarrow as pa
import pyarrow.parquet as pq

from ..config import BRONZE_DIR

DEFAULT_DB_PATH = BRONZE_DIR / "_watermarks.duckdb"

LOAD_TS_FORMAT = "%Y%m%dT%H%M%SZ"


def utc_now() -> datetime:
    return datetime.now(timezone.utc)


def _db_now() -> datetime:
    """Naive-UTC stamp for DuckDB columns.

    Every timestamp in this store is UTC; the columns are TIMESTAMP rather than
    TIMESTAMPTZ because DuckDB's tz-aware -> Python conversion pulls in pytz,
    which is not in requirements.txt and buys nothing when there is exactly one
    timezone in play.
    """
    return utc_now().replace(tzinfo=None)


def new_load_ts() -> str:
    """The partition stamp for one ingest run.

    Second resolution, UTC, lexically sortable, filesystem safe. One value per
    run per dataset -- every artifact a run writes shares it, so "the newest
    partition" is `max(load_ts)` and never a directory mtime.
    """
    return utc_now().strftime(LOAD_TS_FORMAT)


def bronze_partition(source: str, dataset: str, load_ts: str, root: Path | None = None) -> Path:
    """data/bronze/{source}/{dataset}/{load_ts}/

    Bronze is append-only and versioned by load_ts for *every* source, FARS
    included. A reload never overwrites a previous partition, so silver can
    always diff version N against version N-1.
    """
    return (root or BRONZE_DIR) / source / dataset / load_ts


# ---------------------------------------------------------------------------
# durable writes
# ---------------------------------------------------------------------------


def durable_replace(tmp: Path, dest: Path) -> None:
    """Rename tmp -> dest and fsync the containing directory.

    Without the directory fsync the rename itself can be lost in a crash even
    though the file contents survived, which would leave a watermark pointing
    at a partition that does not exist.
    """
    tmp.replace(dest)
    dir_fd = os.open(dest.parent, os.O_RDONLY)
    try:
        os.fsync(dir_fd)
    finally:
        os.close(dir_fd)


def write_bytes(dest: Path, payload: bytes, *, compress: bool = False) -> dict[str, Any]:
    """Write raw response bytes to bronze, verbatim, and fsync them.

    `payload` is the untouched response body: no decode, no reserialise, no
    key reordering. When `compress` is set the file is gzipped (TxDOT pages are
    ~8.6MB of JSON each), but the recorded sha256 is always over the
    *uncompressed* bytes -- so "byte-for-byte identical to what the server sent"
    stays provable with a `gunzip -c | sha256sum`.
    """
    dest.parent.mkdir(parents=True, exist_ok=True)
    tmp = dest.with_name(dest.name + ".part")
    body = gzip.compress(payload, mtime=0) if compress else payload
    with tmp.open("wb") as fh:
        fh.write(body)
        fh.flush()
        os.fsync(fh.fileno())
    durable_replace(tmp, dest)
    return {
        "path": dest,
        "sha256": hashlib.sha256(payload).hexdigest(),
        "bytes": len(payload),
        "stored_bytes": len(body),
        "compressed": compress,
    }


def read_bytes(path: Path) -> bytes:
    """Inverse of `write_bytes` -- transparently un-gzips a .gz page."""
    if path.suffix == ".gz":
        return gzip.decompress(path.read_bytes())
    return path.read_bytes()


def _scalar(value: Any) -> str | None:
    """Bronze coercion: everything becomes a string, nothing becomes a null.

    Socrata and ArcGIS both return nested objects (`geolocation`, `geometry`)
    and mixed scalar types in the same column across pages. Bronze stores the
    JSON encoding of anything non-scalar and the raw string of anything scalar.
    Typing, sentinel handling and dictionary normalisation are silver's job --
    a bronze layer that casts is a bronze layer that has already lost data.
    """
    if value is None:
        return None
    if isinstance(value, str):
        return value
    if isinstance(value, bool):
        return "true" if value else "false"
    if isinstance(value, (int, float)):
        return repr(value) if isinstance(value, float) else str(value)
    return json.dumps(value, sort_keys=True, separators=(",", ":"))


def row_sha256(row: dict[str, Any]) -> str:
    """Canonical hash of one source row, used for restatement diffing.

    Sorted keys and null-omission mean the hash is stable across pages that
    serialise the same record with different key order or omit different empty
    fields -- both of which Socrata does. TxDOT amendments (`amend_supp_fl`)
    and FARS reissues are detected by this hash changing for a stable key.
    """
    canonical = {k: _scalar(v) for k, v in row.items() if v is not None}
    return hashlib.sha256(
        json.dumps(canonical, sort_keys=True, separators=(",", ":")).encode()
    ).hexdigest()


def write_rows_parquet(
    dest: Path,
    rows: Sequence[dict[str, Any]],
    *,
    extra: dict[str, Any] | None = None,
) -> dict[str, Any]:
    """Write the parsed form of a page alongside its raw bytes.

    All columns are strings, by design (see `_scalar`). Column sets differ
    between pages -- Socrata omits keys whose value is null -- so downstream
    reads must union by name; DuckDB's `read_parquet(..., union_by_name=true)`
    does this and the silver layer relies on it.
    """
    dest.parent.mkdir(parents=True, exist_ok=True)
    columns: list[str] = []
    seen: set[str] = set()
    for row in rows:
        for k in row:
            if k not in seen:
                seen.add(k)
                columns.append(k)
    for k in (extra or {}):
        if k not in seen:
            seen.add(k)
            columns.append(k)

    data = {
        col: pa.array(
            [_scalar((extra or {}).get(col, row.get(col))) for row in rows],
            type=pa.string(),
        )
        for col in columns
    }
    table = pa.table(data) if columns else pa.table({"_bronze_empty": pa.array([], pa.string())})

    tmp = dest.with_name(dest.name + ".part")
    pq.write_table(table, tmp, compression="zstd")
    durable_replace(tmp, dest)
    return {"path": dest, "bytes": dest.stat().st_size, "row_count": len(rows)}


# ---------------------------------------------------------------------------
# the store
# ---------------------------------------------------------------------------

_SCHEMA = """
CREATE TABLE IF NOT EXISTS watermarks (
    source        VARCHAR NOT NULL,
    dataset       VARCHAR NOT NULL,
    cursor        VARCHAR NOT NULL,
    rows_loaded   BIGINT  NOT NULL DEFAULT 0,
    batches       BIGINT  NOT NULL DEFAULT 0,
    last_load_ts  VARCHAR,
    note          VARCHAR,
    updated_at    TIMESTAMP NOT NULL,
    PRIMARY KEY (source, dataset)
);

CREATE SEQUENCE IF NOT EXISTS watermark_history_seq;

CREATE TABLE IF NOT EXISTS watermark_history (
    seq           BIGINT DEFAULT nextval('watermark_history_seq'),
    source        VARCHAR NOT NULL,
    dataset       VARCHAR NOT NULL,
    cursor        VARCHAR NOT NULL,
    rows_in_batch BIGINT,
    load_ts       VARCHAR,
    note          VARCHAR,
    advanced_at   TIMESTAMP NOT NULL
);

CREATE TABLE IF NOT EXISTS bronze_manifest (
    source                VARCHAR NOT NULL,
    dataset               VARCHAR NOT NULL,
    load_ts               VARCHAR NOT NULL,
    artifact              VARCHAR NOT NULL,
    kind                  VARCHAR NOT NULL,
    sha256                VARCHAR,
    bytes                 BIGINT,
    row_count             BIGINT,
    content_last_modified VARCHAR,
    etag                  VARCHAR,
    source_url            VARCHAR,
    recorded_at           TIMESTAMP NOT NULL,
    PRIMARY KEY (source, dataset, load_ts, artifact)
);
"""


class WatermarkStore:
    """Durable cursor storage. One connection per operation, deliberately.

    DuckDB takes an exclusive file lock for the lifetime of a connection. Ingest
    touches the store a few dozen times per run and the pipeline is meant to run
    three sources in parallel under an orchestrator, so holding the lock for the
    whole run would serialise them for no reason. Connect, commit, close --
    the lock is held for milliseconds and a concurrent holder is waited out.
    """

    def __init__(self, db_path: Path | str = DEFAULT_DB_PATH, *, lock_timeout: float = 30.0):
        self.db_path = Path(db_path)
        self.lock_timeout = lock_timeout
        self.db_path.parent.mkdir(parents=True, exist_ok=True)
        with self._connect() as con:
            con.execute(_SCHEMA)

    def _connect(self):
        deadline = time.monotonic() + self.lock_timeout
        while True:
            try:
                return duckdb.connect(str(self.db_path))
            except duckdb.IOException as exc:
                if "lock" not in str(exc).lower() or time.monotonic() > deadline:
                    raise
                time.sleep(0.25)

    # -- read ------------------------------------------------------------

    def get(self, source: str, dataset: str) -> dict[str, Any] | None:
        """Current state for a dataset, or None if it has never been loaded."""
        with self._connect() as con:
            row = con.execute(
                "SELECT cursor, rows_loaded, batches, last_load_ts, note, updated_at "
                "FROM watermarks WHERE source = ? AND dataset = ?",
                [source, dataset],
            ).fetchone()
        if row is None:
            return None
        return {
            "cursor": json.loads(row[0]),
            "rows_loaded": row[1],
            "batches": row[2],
            "last_load_ts": row[3],
            "note": row[4],
            "updated_at": row[5],
        }

    def cursor(self, source: str, dataset: str, default: dict | None = None) -> dict[str, Any]:
        state = self.get(source, dataset)
        return state["cursor"] if state else dict(default or {})

    def history(self, source: str, dataset: str | None = None, limit: int = 50) -> list[dict]:
        sql = "SELECT seq, source, dataset, cursor, rows_in_batch, load_ts, note, advanced_at " \
              "FROM watermark_history WHERE source = ?"
        params: list[Any] = [source]
        if dataset:
            sql += " AND dataset = ?"
            params.append(dataset)
        sql += " ORDER BY seq DESC LIMIT ?"
        params.append(limit)
        with self._connect() as con:
            rows = con.execute(sql, params).fetchall()
        keys = ["seq", "source", "dataset", "cursor", "rows_in_batch", "load_ts", "note", "advanced_at"]
        return [dict(zip(keys, r)) | {"cursor": json.loads(r[3])} for r in rows]

    def artifacts(
        self, source: str, dataset: str | None = None, *, load_ts: str | None = None
    ) -> list[dict[str, Any]]:
        sql = ("SELECT source, dataset, load_ts, artifact, kind, sha256, bytes, row_count, "
               "content_last_modified, etag, source_url, recorded_at "
               "FROM bronze_manifest WHERE source = ?")
        params: list[Any] = [source]
        if dataset:
            sql += " AND dataset = ?"
            params.append(dataset)
        if load_ts:
            sql += " AND load_ts = ?"
            params.append(load_ts)
        sql += " ORDER BY load_ts, artifact"
        with self._connect() as con:
            rows = con.execute(sql, params).fetchall()
        keys = ["source", "dataset", "load_ts", "artifact", "kind", "sha256", "bytes",
                "row_count", "content_last_modified", "etag", "source_url", "recorded_at"]
        return [dict(zip(keys, r)) for r in rows]

    def load_partitions(self, source: str, dataset: str) -> list[str]:
        """Every load_ts ever written for a dataset, oldest first."""
        with self._connect() as con:
            rows = con.execute(
                "SELECT DISTINCT load_ts FROM bronze_manifest "
                "WHERE source = ? AND dataset = ? ORDER BY load_ts",
                [source, dataset],
            ).fetchall()
        return [r[0] for r in rows]

    # -- write -----------------------------------------------------------

    def record_artifact(
        self,
        source: str,
        dataset: str,
        load_ts: str,
        path: Path,
        *,
        kind: str,
        sha256: str | None = None,
        size: int | None = None,
        row_count: int | None = None,
        content_last_modified: str | None = None,
        etag: str | None = None,
        source_url: str | None = None,
    ) -> None:
        """Record one durably-written bronze artifact.

        Called after the file is fsynced and before the watermark advances, so
        the manifest is the evidence that the advance was legitimate. Paths are
        stored relative to the bronze root: the manifest survives the tree being
        moved or archived elsewhere.
        """
        try:
            artifact = str(path.relative_to(BRONZE_DIR))
        except ValueError:
            artifact = str(path)
        with self._connect() as con:
            con.execute(
                "INSERT OR REPLACE INTO bronze_manifest VALUES (?,?,?,?,?,?,?,?,?,?,?,?)",
                [source, dataset, load_ts, artifact, kind, sha256, size, row_count,
                 content_last_modified, etag, source_url, _db_now()],
            )

    def advance(
        self,
        source: str,
        dataset: str,
        cursor: dict[str, Any],
        *,
        rows_in_batch: int = 0,
        load_ts: str | None = None,
        note: str | None = None,
    ) -> None:
        """Move the watermark forward. CALL THIS ONLY AFTER A DURABLE WRITE.

        Ordering contract, and the reason every caller has a comment at its call
        site: the page's raw bytes and parsed parquet must be fsynced to bronze
        and recorded in `bronze_manifest` *before* this runs.

        Advance-then-write loses data silently. If the process dies between the
        advance and the write -- SIGKILL, OOM, a 500 on the next page, a laptop
        lid -- the next run resumes from a cursor covering rows that were never
        persisted, and skips them forever. Nothing downstream errors; the rows
        are simply not there, and you find out months later from a row count
        that never looked wrong enough to investigate.

        Write-then-advance fails the other way: a crash in the gap replays the
        last page on the next run. That produces a duplicate raw page under a
        new load_ts, which is exactly what an append-only, load_ts-versioned
        bronze layer is built to absorb, and which silver de-duplicates on the
        natural key. Cheap, visible, recoverable. Choose that failure.
        """
        payload = json.dumps(cursor, sort_keys=True, separators=(",", ":"))
        now = _db_now()
        with self._connect() as con:
            con.execute("BEGIN TRANSACTION")
            con.execute(
                """
                INSERT INTO watermarks
                    (source, dataset, cursor, rows_loaded, batches, last_load_ts, note, updated_at)
                VALUES (?,?,?,?,1,?,?,?)
                ON CONFLICT (source, dataset) DO UPDATE SET
                    cursor       = excluded.cursor,
                    rows_loaded  = watermarks.rows_loaded + excluded.rows_loaded,
                    batches      = watermarks.batches + 1,
                    last_load_ts = excluded.last_load_ts,
                    note         = excluded.note,
                    updated_at   = excluded.updated_at
                """,
                [source, dataset, payload, rows_in_batch, load_ts, note, now],
            )
            con.execute(
                "INSERT INTO watermark_history "
                "(source, dataset, cursor, rows_in_batch, load_ts, note, advanced_at) "
                "VALUES (?,?,?,?,?,?,?)",
                [source, dataset, payload, rows_in_batch, load_ts, note, now],
            )
            con.execute("COMMIT")

    def reset(self, source: str, dataset: str | None = None) -> None:
        """Forget the cursor so the next run is a full backfill.

        Does not delete bronze. Bronze is append-only; a reset re-reads the
        source into a new load_ts partition and leaves the old ones intact.
        """
        with self._connect() as con:
            if dataset:
                con.execute(
                    "DELETE FROM watermarks WHERE source = ? AND dataset = ?", [source, dataset]
                )
            else:
                con.execute("DELETE FROM watermarks WHERE source = ?", [source])

    def summary(self, source: str | None = None) -> list[dict[str, Any]]:
        sql = ("SELECT source, dataset, cursor, rows_loaded, batches, last_load_ts, updated_at "
               "FROM watermarks")
        params: list[Any] = []
        if source:
            sql += " WHERE source = ?"
            params.append(source)
        sql += " ORDER BY source, dataset"
        with self._connect() as con:
            rows = con.execute(sql, params).fetchall()
        keys = ["source", "dataset", "cursor", "rows_loaded", "batches", "last_load_ts", "updated_at"]
        return [dict(zip(keys, r)) | {"cursor": json.loads(r[2])} for r in rows]


def _cli(argv: Iterable[str] | None = None) -> int:
    """`python -m src.ingest.watermark` -- inspect the store."""
    import argparse

    ap = argparse.ArgumentParser(description="Inspect the bronze watermark store.")
    ap.add_argument("--db", default=str(DEFAULT_DB_PATH))
    ap.add_argument("--source", help="filter to one source")
    ap.add_argument("--history", action="store_true", help="show recent advances")
    ap.add_argument("--artifacts", action="store_true", help="show the bronze manifest")
    ap.add_argument("--reset", nargs="+", metavar=("SOURCE", "DATASET"),
                    help="forget a cursor: --reset montgomery bhju-22kf")
    args = ap.parse_args(list(argv) if argv is not None else None)

    store = WatermarkStore(args.db)
    if args.reset:
        store.reset(args.reset[0], args.reset[1] if len(args.reset) > 1 else None)
        print(f"reset {' '.join(args.reset)}")
        return 0
    if args.history:
        for row in store.history(args.source or "montgomery"):
            print(json.dumps(row, default=str))
        return 0
    if args.artifacts:
        for row in store.artifacts(args.source or "montgomery"):
            print(json.dumps(row, default=str))
        return 0
    rows = store.summary(args.source)
    if not rows:
        print("(no watermarks yet)")
    for row in rows:
        print(f"{row['source']:<12} {row['dataset']:<16} rows={row['rows_loaded']:<9} "
              f"batches={row['batches']:<5} last_load_ts={row['last_load_ts']} "
              f"cursor={json.dumps(row['cursor'])}")
    return 0


if __name__ == "__main__":
    raise SystemExit(_cli())
