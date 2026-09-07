"""Fixtures for the silver tests.

Two corpora, one code path:

  * the committed extracts under `tests/fixtures/bronze/` -- a few hundred real
    bronze rows per table, in the exact bronze format, sampled to exhibit every
    defect (see tests/fixtures/make_extracts.py). This is the default, so the
    suite runs on a fresh clone with no `data/` present.
  * the full local bronze at `data/bronze/`, when `CRASH_TEST_FULL_BRONZE=1`.
    Same fixtures, same assertions, 1.5M rows instead of 3,195.

Every assertion in this suite is written as a helper that takes a DuckDB
connection and a relation name, so the bronze test and its silver twin run the
IDENTICAL check against the two layers. That is the point the scaffold's
docstring makes: a test that only passes proves nothing about whether the
transform did anything, and two tests that merely look similar prove nothing
about whether they check the same thing.

The bronze fixtures are session-scoped read-only views; the silver fixtures
build once per session into a tmp directory, because a full build over the real
bronze takes ~25s and doing it per test would make the suite unusable.
"""

from __future__ import annotations

import json
import os
import shutil
from pathlib import Path

import duckdb
import pytest

from src.ingest.watermark import WatermarkStore
from src.transform import build as build_module
from src.transform import common as c

REPO = Path(__file__).resolve().parents[1]
FIXTURE_BRONZE = REPO / "tests" / "fixtures" / "bronze"
FULL_BRONZE = REPO / "data" / "bronze"

FULL_ENV = "CRASH_TEST_FULL_BRONZE"


def using_full_bronze() -> bool:
    return os.environ.get(FULL_ENV, "") not in ("", "0", "false", "no")


@pytest.fixture(scope="session")
def bronze_root() -> Path:
    """Which bronze the whole session reads.

    Defaults to the committed extracts so no test depends on data/ being
    present. CRASH_TEST_FULL_BRONZE=1 points every fixture at the real corpus
    without changing a single assertion.
    """
    if using_full_bronze():
        if not FULL_BRONZE.exists():
            pytest.skip(f"{FULL_ENV} is set but {FULL_BRONZE} does not exist")
        return FULL_BRONZE
    if not FIXTURE_BRONZE.exists():
        pytest.skip(
            f"{FIXTURE_BRONZE} is missing -- run "
            "`python -m tests.fixtures.make_extracts` against a local bronze"
        )
    return FIXTURE_BRONZE


@pytest.fixture(scope="session")
def fixture_manifest() -> dict:
    """What make_extracts.py recorded, including the synthetic key ids.

    The restatement tests need to know WHICH crash was amended and which
    ST_CASE was removed. Reading it from the manifest rather than hardcoding it
    means regenerating the extracts cannot silently make those tests vacuous:
    they would fail on a missing key rather than pass on an absent one.
    """
    path = FIXTURE_BRONZE / "MANIFEST.json"
    if not path.exists():
        pytest.skip("fixture manifest missing")
    return json.loads(path.read_text())


@pytest.fixture(scope="session")
def bronze_con(bronze_root: Path):
    """A DuckDB connection with one view per bronze dataset."""
    con = c.connect()
    for name, source, dataset in (
        ("bronze_incidents", "montgomery", "bhju-22kf"),
        ("bronze_drivers", "montgomery", "mmzv-x632"),
        ("bronze_non_motorists", "montgomery", "n7fk-dce5"),
        ("bronze_txdot", "txdot", "cris_crash"),
    ):
        parts = c.discover_partitions(source, dataset, bronze_root=bronze_root)
        if parts:
            c.bronze_view(con, name, parts)
    years = c.discover_datasets("fars", bronze_root=bronze_root)
    fars_parts = [
        p for y in years
        for p in c.discover_partitions("fars", y, bronze_root=bronze_root)
    ]
    if fars_parts:
        c.bronze_view(con, "bronze_fars_accident", fars_parts, pattern="accident.parquet")
        c.bronze_view(con, "bronze_fars_person", fars_parts, pattern="person.parquet")
    yield con
    con.close()


# The scaffold's four defect tests take these by name. They are the connection
# plus the relation, so a helper can be handed the pair and not care which layer
# it is looking at.
@pytest.fixture(scope="session")
def bronze_incidents(bronze_con):
    return bronze_con, "bronze_incidents"


@pytest.fixture(scope="session")
def bronze_drivers(bronze_con):
    return bronze_con, "bronze_drivers"


@pytest.fixture(scope="session")
def bronze_non_motorists(bronze_con):
    return bronze_con, "bronze_non_motorists"


@pytest.fixture(scope="session")
def bronze_txdot(bronze_con):
    return bronze_con, "bronze_txdot"


@pytest.fixture(scope="session")
def bronze_fars_accident(bronze_con):
    return bronze_con, "bronze_fars_accident"


# ---------------------------------------------------------------------------
# silver
# ---------------------------------------------------------------------------


def make_watermark_store(db_path: Path, bronze_root: Path) -> WatermarkStore:
    """A watermark store describing the fixture partitions.

    The fixture tree has no `_watermarks.duckdb` -- it is a set of parquet files
    in a git repo. Without a store, `txdot.build` cannot learn which OID range
    each partition swept and correctly downgrades to delta mode, which would
    make the deletion-detection path untested.

    So the store is reconstructed here from the fixture tree, writing the same
    cursor shape `src/ingest/txdot.py` writes: `oid_floor`, `oid_ceiling`,
    `last_objectid`, sticky `bounded`. Both TxDOT partitions declare the SAME
    swept range, which is what makes them comparable snapshots -- and is exactly
    the condition `_snapshot_scope` checks.
    """
    store = WatermarkStore(db_path)
    for source, dataset in (
        ("montgomery", "bhju-22kf"), ("montgomery", "mmzv-x632"),
        ("montgomery", "n7fk-dce5"), ("txdot", "cris_crash"),
    ):
        parts = c.discover_partitions(source, dataset, bronze_root=bronze_root)
        for p in parts:
            for f in sorted(p.path.glob("*.parquet")):
                store.record_artifact(source, dataset, p.load_ts, f, kind="parquet")
            if source == "txdot":
                oids = duckdb.connect().execute(
                    f"""SELECT MIN(TRY_CAST("ESRI_OID" AS BIGINT)),
                               MAX(TRY_CAST("ESRI_OID" AS BIGINT))
                        FROM read_parquet('{p.path}/*.parquet', union_by_name=true)"""
                ).fetchone()
                store.advance(
                    source, dataset,
                    {"oid_floor": 0, "oid_ceiling": int(oids[1]),
                     "last_objectid": int(oids[1]), "bounded": True,
                     "status": "complete", "sweep_id": p.load_ts},
                    load_ts=p.load_ts,
                )
    for year in c.discover_datasets("fars", bronze_root=bronze_root):
        for p in c.discover_partitions("fars", year, bronze_root=bronze_root):
            for f in sorted(p.path.glob("*.parquet")):
                store.record_artifact("fars", year, p.load_ts, f, kind="parquet")
    return store


def build_silver_into(dest: Path, bronze_root: Path, **kwargs) -> dict:
    """Run the real build CLI path into `dest`. No mocks anywhere in this suite."""
    store = make_watermark_store(dest / "_watermarks.duckdb", bronze_root)
    return build_module.build_silver(
        bronze_root=bronze_root,
        silver_root=dest,
        store=store,
        small_corpus=not using_full_bronze(),
        **kwargs,
    )


@pytest.fixture(scope="session")
def silver_root(tmp_path_factory, bronze_root: Path) -> Path:
    """Silver, built once per session from `bronze_root` by the real build."""
    dest = tmp_path_factory.mktemp("silver")
    build_silver_into(dest, bronze_root)
    return dest


@pytest.fixture(scope="session")
def silver_con(silver_root: Path):
    """A connection with one view per silver table, named `silver_<table>`."""
    con = c.connect()
    for path in sorted(silver_root.rglob("*.parquet")):
        rel = path.relative_to(silver_root)
        name = "silver_" + "_".join(rel.with_suffix("").parts).replace("-", "_")
        p = str(path).replace("'", "''")
        con.execute(f"CREATE OR REPLACE VIEW {name} AS SELECT * FROM read_parquet('{p}')")
    yield con
    con.close()


@pytest.fixture(scope="session")
def silver_incidents(silver_con):
    return silver_con, "silver_montgomery_crash_current"


@pytest.fixture(scope="session")
def silver_drivers(silver_con):
    return silver_con, "silver_montgomery_driver_current"


@pytest.fixture(scope="session")
def silver_non_motorists(silver_con):
    return silver_con, "silver_montgomery_non_motorist_current"


@pytest.fixture(scope="session")
def silver_txdot(silver_con):
    return silver_con, "silver_txdot_crash_current"


@pytest.fixture(scope="session")
def silver_crash_fact(silver_con):
    """The unified crash grain -- what the scaffold's grain test asks for."""
    return silver_con, "silver_crash_current"


# ---------------------------------------------------------------------------
# the restatement runner
# ---------------------------------------------------------------------------


class PipelineRunner:
    """Rebuild silver into a fresh directory and hash what came out.

    Defined at the SILVER level, deliberately. The assignment's idempotency
    requirement is about the pipeline's output being a pure function of its
    input; bronze is append-only and already tested for that in Phase 1. What is
    untested until here is whether adding one amended row to bronze produces
    exactly one new version and leaves every other byte alone.
    """

    def __init__(self, bronze_root: Path, workdir: Path):
        self.bronze_root = bronze_root
        self.workdir = workdir
        self._n = 0

    def run(self, bronze_root: Path | None = None) -> dict[str, str]:
        """Build silver from scratch and return {relative path: sha256}."""
        self._n += 1
        dest = self.workdir / f"silver_{self._n:02d}"
        build_silver_into(dest, bronze_root or self.bronze_root)
        return {
            str(p.relative_to(dest)): h
            for p, h in (
                (p, __import__("hashlib").sha256(p.read_bytes()).hexdigest())
                for p in sorted(dest.rglob("*.parquet"))
            )
        }

    def clone_bronze(self, name: str) -> Path:
        """A writable copy of the bronze tree, for adding a partition to."""
        dest = self.workdir / name
        if dest.exists():
            shutil.rmtree(dest)
        shutil.copytree(self.bronze_root, dest,
                        ignore=shutil.ignore_patterns("*.duckdb"))
        return dest


@pytest.fixture
def pipeline_runner(bronze_root: Path, tmp_path: Path) -> PipelineRunner:
    return PipelineRunner(bronze_root, tmp_path)
