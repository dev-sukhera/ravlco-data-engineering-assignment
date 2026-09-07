"""Ingest idempotency and late-arrival tests (Phase 1, bronze).

These run against a real local HTTP server implementing just enough SoQL to
answer the queries src/ingest/montgomery.py actually sends. That is deliberate:
monkeypatching the client would test the code's opinion of the protocol rather
than the code path that talks to one, and the whole point of the keyset design
is what happens when the table changes underneath a paginated read -- which you
cannot stage without a server you can mutate mid-run.

The four properties under test:

  1. Re-running does not duplicate rows.
  2. A record that is updated after a load is picked up on the next run, even
     though its crash date was already covered -- and the same fixture proves a
     crash-date watermark would have missed it.
  3. A row inserted *behind* the cursor between pages does not displace
     anything: keyset does not skip, where $offset would have.
  4. The watermark never points past what is durably in bronze.
"""

from __future__ import annotations

import http.server
import json
import re
import threading
from pathlib import Path

import duckdb
import pyarrow as pa
import pytest

from src.ingest import fars, montgomery
from src.ingest.http import HttpClient
from src.ingest.watermark import WatermarkStore

DATASET = "test-data"


class FakeSocrata:
    """A mutable in-memory table served over SoQL-ish HTTP.

    Supports exactly the query shape montgomery.py emits: $select, $order,
    $limit and the two $where forms the cursor produces. Anything else raises,
    so a change in the emitted query cannot silently pass these tests.
    """

    WHERE_KEYSET = re.compile(
        r"^:updated_at > '(?P<ts>[^']*)' OR "
        r"\(:updated_at = '(?P=ts)' AND :id > '(?P<id>[^']*)'\)$"
    )
    WHERE_FLOOR = re.compile(r"^:updated_at (?P<op>>=|>) '(?P<ts>[^']*)'$")

    def __init__(self, rows: list[dict]):
        self.rows = list(rows)
        self.requests: list[dict] = []
        handler = self._handler()
        self.server = http.server.ThreadingHTTPServer(("127.0.0.1", 0), handler)
        threading.Thread(target=self.server.serve_forever, daemon=True).start()

    @property
    def base(self) -> str:
        return f"http://127.0.0.1:{self.server.server_address[1]}"

    def upsert(self, row: dict) -> None:
        """Insert or update a row, exactly as the county would."""
        self.rows = [r for r in self.rows if r[":id"] != row[":id"]] + [row]

    def select(self, where: str | None, limit: int) -> list[dict]:
        ordered = sorted(self.rows, key=lambda r: (r[":updated_at"], r[":id"]))
        if where:
            keyset = self.WHERE_KEYSET.match(where)
            floor = self.WHERE_FLOOR.match(where)
            if keyset:
                ts, row_id = keyset["ts"], keyset["id"]
                ordered = [r for r in ordered
                           if (r[":updated_at"], r[":id"]) > (ts, row_id)]
            elif floor:
                ts, op = floor["ts"], floor["op"]
                ordered = [r for r in ordered
                           if (r[":updated_at"] >= ts if op == ">=" else r[":updated_at"] > ts)]
            else:
                raise AssertionError(f"unrecognised $where emitted: {where!r}")
        return ordered[:limit]

    def _handler(self):
        fake = self

        class Handler(http.server.BaseHTTPRequestHandler):
            protocol_version = "HTTP/1.1"

            def log_message(self, *args):
                pass

            def do_GET(self):
                from urllib.parse import parse_qs, urlparse

                query = parse_qs(urlparse(self.path).query)
                where = query.get("$where", [None])[0]
                limit = int(query.get("$limit", ["1000"])[0])
                order = query.get("$order", [None])[0]
                assert order == ":updated_at,:id", f"bad $order: {order!r}"
                fake.requests.append({"where": where, "limit": limit})
                body = json.dumps(fake.select(where, limit)).encode()
                self.send_response(200)
                self.send_header("Content-Type", "application/json")
                self.send_header("Content-Length", str(len(body)))
                self.end_headers()
                self.wfile.write(body)

        return Handler

    def close(self):
        self.server.shutdown()


def make_row(n: int, *, updated_at: str, crash_date: str) -> dict:
    return {
        ":id": f"row-{n:05d}",
        ":updated_at": updated_at,
        ":created_at": updated_at,
        ":version": "1",
        "report_number": f"MCP{n:07d}",
        "crash_date_time": crash_date,
        "latitude": "39.1",
        "longitude": "-77.1",
    }


@pytest.fixture
def fake(monkeypatch):
    """A fake dataset of 25 rows, all crashes dated 2026-01-15."""
    rows = [
        make_row(n, updated_at=f"2026-02-01T00:00:{n:02d}.000",
                 crash_date="2026-01-15T08:00:00.000")
        for n in range(25)
    ]
    server = FakeSocrata(rows)
    monkeypatch.setitem(
        montgomery.config.sources()["montgomery"], "base", server.base
    )
    yield server
    server.close()


@pytest.fixture
def bronze(tmp_path, monkeypatch):
    """An isolated bronze root and watermark store per test."""
    root = tmp_path / "bronze"
    monkeypatch.setattr("src.ingest.watermark.BRONZE_DIR", root)
    monkeypatch.setattr("src.config.BRONZE_DIR", root)
    return root


def run_ingest(store: WatermarkStore, *, since: str | None = None, page_size: int = 10):
    with HttpClient(min_interval=0.0, timeout=10.0) as client:
        return montgomery.ingest_dataset(
            "test", DATASET, store=store, client=client,
            since=since, page_size=page_size,
        )


def bronze_rows(root: Path) -> duckdb.DuckDBPyRelation:
    pattern = str(root / "montgomery" / DATASET / "*" / "page_*.parquet")
    return duckdb.connect().execute(
        f"SELECT * FROM read_parquet('{pattern}', union_by_name=true)"
    ).df()


def test_rerun_does_not_duplicate_rows(fake, bronze, tmp_path):
    """Property 1: a second run over an unchanged table adds nothing."""
    store = WatermarkStore(tmp_path / "wm.duckdb")

    first = run_ingest(store)
    assert first["rows"] == 25

    second = run_ingest(store)
    assert second["rows"] == 0, "re-run pulled rows from an unchanged table"

    df = bronze_rows(bronze)
    assert len(df) == 25
    assert df[":id"].nunique() == 25, "duplicate :id in bronze after a re-run"


def test_late_update_for_an_already_loaded_date_is_picked_up(fake, bronze, tmp_path):
    """Property 2, and the trap the assignment is testing.

    Row 7 is amended a week after the first load. Its crash date is 2026-01-15,
    which the first run already covered end to end. An :updated_at watermark
    asks for it again; a crash_date_time watermark never would, because it has
    long since moved past 2026-01-15.
    """
    store = WatermarkStore(tmp_path / "wm.duckdb")
    run_ingest(store)

    amended = make_row(7, updated_at="2026-02-08T12:00:00.000",
                       crash_date="2026-01-15T08:00:00.000")
    amended["report_number"] = "MCP0000007-AMENDED"
    fake.upsert(amended)

    second = run_ingest(store)
    assert second["rows"] == 1, "late update to an already-loaded date was missed"

    df = bronze_rows(bronze)
    versions = df[df[":id"] == "row-00007"]
    assert len(versions) == 2, "bronze should hold both versions, append-only"
    assert "MCP0000007-AMENDED" in set(versions["report_number"])

    # The counterfactual, stated as an assertion rather than a comment: the
    # amended row's crash date is at or before the max crash date already
    # loaded, so a crash-date watermark would have filtered it out.
    max_crash_date_loaded = df["crash_date_time"].max()
    assert amended["crash_date_time"] <= max_crash_date_loaded


def test_keyset_does_not_skip_a_row_inserted_behind_the_cursor(fake, bronze, tmp_path):
    """Property 3: the specific failure $offset has and keyset does not.

    A row is inserted mid-run at a position *before* the cursor. Under $offset
    every subsequent row shifts one place forward and exactly one row is never
    returned. Under keyset the cursor names a row, not a position, so nothing
    moves.
    """
    store = WatermarkStore(tmp_path / "wm.duckdb")

    # Page 1 only, so the cursor sits mid-table with the table still open.
    with HttpClient(min_interval=0.0, timeout=10.0) as client:
        montgomery.ingest_dataset(
            "test", DATASET, store=store, client=client, page_size=10, max_pages=1,
        )
    cursor = store.cursor(montgomery.SOURCE, DATASET)
    assert cursor["id"] == "row-00009"

    # Insert behind the cursor: sorts between rows 3 and 4, already returned.
    fake.upsert(make_row(999, updated_at="2026-02-01T00:00:03.500",
                         crash_date="2026-01-15T08:00:00.000"))

    with HttpClient(min_interval=0.0, timeout=10.0) as client:
        montgomery.ingest_dataset(
            "test", DATASET, store=store, client=client, page_size=10,
        )

    df = bronze_rows(bronze)
    ids = set(df[":id"])
    original = {f"row-{n:05d}" for n in range(25)}
    missing = original - ids
    assert not missing, f"keyset skipped rows after an insert behind the cursor: {missing}"
    # The inserted row is genuinely behind the cursor, so this run does not see
    # it -- that is correct, not a miss. It carries an older :updated_at than
    # the watermark; the next full backfill or a real edit brings it in.
    assert "row-00999" not in ids


def test_watermark_never_leads_the_durable_write(fake, bronze, tmp_path):
    """Property 4: the cursor points at data that is on disk, not ahead of it.

    Simulates the crash-mid-run case by bounding the run, then asserts the
    cursor names the last row of the last page actually written, and that a
    resume re-reads from exactly there.
    """
    store = WatermarkStore(tmp_path / "wm.duckdb")
    with HttpClient(min_interval=0.0, timeout=10.0) as client:
        montgomery.ingest_dataset(
            "test", DATASET, store=store, client=client, page_size=10, max_pages=2,
        )

    df = bronze_rows(bronze)
    cursor = store.cursor(montgomery.SOURCE, DATASET)
    assert len(df) == 20
    assert cursor["id"] == max(df[":id"]), "watermark is ahead of what bronze holds"

    # Every artifact the watermark implies is on disk and in the manifest.
    artifacts = store.artifacts(montgomery.SOURCE, DATASET)
    assert artifacts, "no manifest rows recorded"
    for artifact in artifacts:
        assert (bronze / artifact["artifact"]).exists()

    resumed = run_ingest(store)
    assert resumed["rows"] == 5, "resume did not continue from the durable cursor"
    assert len(bronze_rows(bronze)) == 25


# ---------------------------------------------------------------------------
# FARS: full-refresh-under-restatement
# ---------------------------------------------------------------------------


class FakeNhtsa:
    """Serves one annual ZIP whose contents and headers can be changed."""

    def __init__(self, csv_body: bytes):
        self.requests: list[str] = []
        self.set_body(csv_body, last_modified="Wed, 01 Apr 2026 19:46:54 GMT")
        handler = self._handler()
        self.server = http.server.ThreadingHTTPServer(("127.0.0.1", 0), handler)
        threading.Thread(target=self.server.serve_forever, daemon=True).start()

    @property
    def pattern(self) -> str:
        port = self.server.server_address[1]
        return f"http://127.0.0.1:{port}/FARS{{year}}NationalCSV.zip"

    def set_body(self, csv_body: bytes, *, last_modified: str) -> None:
        import io
        import zipfile

        buffer = io.BytesIO()
        with zipfile.ZipFile(buffer, "w") as zf:
            zf.writestr("accident.csv", csv_body)
        self.body = buffer.getvalue()
        self.last_modified = last_modified
        self.etag = f'"{hash(self.body) & 0xFFFFFFFF:08x}"'

    def _handler(self):
        fake = self

        class Handler(http.server.BaseHTTPRequestHandler):
            protocol_version = "HTTP/1.1"

            def log_message(self, *args):
                pass

            def _headers(self):
                self.send_response(200)
                self.send_header("Content-Length", str(len(fake.body)))
                self.send_header("Last-Modified", fake.last_modified)
                self.send_header("ETag", fake.etag)
                self.end_headers()

            def do_HEAD(self):
                fake.requests.append("HEAD")
                self._headers()

            def do_GET(self):
                fake.requests.append("GET")
                self._headers()
                self.wfile.write(fake.body)

        return Handler

    def close(self):
        self.server.shutdown()


# Sentinels exactly as FARS writes them: unknown coordinates and 9-fill codes.
CSV_V1 = b"ST_CASE,LATITUDE,LONGITUD,AGE,HOUR\n1,39.1,-77.1,34,14\n2,77.7777,99.9999,999,99\n"
CSV_V2 = b"ST_CASE,LATITUDE,LONGITUD,AGE,HOUR\n1,39.2,-77.2,34,14\n2,77.7777,99.9999,999,99\n3,38.9,-77.0,22,8\n"


@pytest.fixture
def nhtsa(monkeypatch):
    server = FakeNhtsa(CSV_V1)
    monkeypatch.setitem(
        fars.config.sources()["fars"], "bulk_pattern", server.pattern
    )
    yield server
    server.close()


def run_fars(store, client_kwargs=None):
    with HttpClient(min_interval=0.0, timeout=10.0, **(client_kwargs or {})) as client:
        return fars.ingest_year(2023, store=store, client=client)


def test_fars_unchanged_headers_skip_the_download(nhtsa, bronze, tmp_path):
    store = WatermarkStore(tmp_path / "wm.duckdb")
    first = run_fars(store)
    assert first["action"] == "full_refresh"
    assert first["reason"] == "initial"

    nhtsa.requests.clear()
    second = run_fars(store)
    assert second["action"] == "skipped_unchanged_header"
    assert "GET" not in nhtsa.requests, "downloaded a year whose headers had not moved"


def test_fars_changed_hash_full_refreshes_the_year(nhtsa, bronze, tmp_path):
    """A revision must produce a NEW complete partition, not an append.

    Appending an amended file to its own prior version double-counts every
    unrevised row in it -- here, ST_CASE 2, which is unchanged between versions.
    """
    store = WatermarkStore(tmp_path / "wm.duckdb")
    first = run_fars(store)

    nhtsa.set_body(CSV_V2, last_modified="Thu, 02 Apr 2026 10:00:00 GMT")
    second = run_fars(store)

    assert second["action"] == "full_refresh"
    assert second["reason"] == "restatement"
    assert second["sha256"] != first["sha256"]
    assert second["load_ts"] != first["load_ts"]

    # The prior partition is still on disk, untouched: silver needs version N-1
    # to diff version N against.
    assert Path(first["partition"]).exists()
    assert (Path(first["partition"]) / "accident.parquet").exists()
    partitions = store.load_partitions("fars", "2023")
    assert len(partitions) == 2

    # Each partition holds one complete version of the year -- not a union.
    old = duckdb.connect().execute(
        f"SELECT * FROM read_parquet('{Path(first['partition'])}/accident.parquet')"
    ).df()
    new = duckdb.connect().execute(
        f"SELECT * FROM read_parquet('{Path(second['partition'])}/accident.parquet')"
    ).df()
    assert len(old) == 2 and len(new) == 3
    assert set(old["ST_CASE"]) == {"1", "2"}
    assert set(new["ST_CASE"]) == {"1", "2", "3"}

    # The cursor names the current version, so downstream reads the right one.
    cursor = store.cursor("fars", "2023")
    assert cursor["current_load_ts"] == second["load_ts"]
    assert cursor["refresh_reason"] == "restatement"


def test_fars_header_moved_but_content_identical_does_not_refresh(nhtsa, bronze, tmp_path):
    """A re-upload of identical bytes must not re-materialise the year.

    Last-Modified moves; the hash does not. Trusting the header alone would
    create a duplicate partition for no reason, and silver would then diff a
    version against an identical copy of itself.
    """
    store = WatermarkStore(tmp_path / "wm.duckdb")
    first = run_fars(store)

    nhtsa.last_modified = "Fri, 03 Apr 2026 11:00:00 GMT"
    second = run_fars(store)

    assert second["action"] == "unchanged_content"
    assert second["sha256"] == first["sha256"]
    assert len(store.load_partitions("fars", "2023")) == 1

    # The new header is recorded, so tomorrow's HEAD short-circuits again.
    assert store.cursor("fars", "2023")["last_modified"] == "Fri, 03 Apr 2026 11:00:00 GMT"
    nhtsa.requests.clear()
    assert run_fars(store)["action"] == "skipped_unchanged_header"
    assert "GET" not in nhtsa.requests


def test_fars_bronze_preserves_sentinels_verbatim(nhtsa, bronze, tmp_path):
    """Bronze must not interpret 77.7777 / 999 / 99. That is silver's job, and
    the known-defect tests have to be able to fail on bronze."""
    store = WatermarkStore(tmp_path / "wm.duckdb")
    result = run_fars(store)
    df = duckdb.connect().execute(
        f"SELECT * FROM read_parquet('{Path(result['partition'])}/accident.parquet')"
    ).df()

    sentinel = df[df["ST_CASE"] == "2"].iloc[0]
    assert sentinel["LATITUDE"] == "77.7777"
    assert sentinel["LONGITUD"] == "99.9999"
    assert sentinel["AGE"] == "999"
    assert sentinel["HOUR"] == "99"
    # The stored parquet column is genuinely string-typed: nothing in the
    # bronze path parsed 77.7777 into a float, which is what would have made
    # it indistinguishable from a real coordinate.
    import pyarrow.parquet as pq

    schema = pq.read_schema(Path(result["partition"]) / "accident.parquet")
    assert all(schema.field(name).type == pa.string() for name in schema.names)
