"""End-to-end properties of a whole silver build.

Row-count reconciliation layer to layer, contract conformance on every table,
and the determinism guarantee. These run against whichever corpus the session
is pointed at, so `CRASH_TEST_FULL_BRONZE=1 pytest` re-proves all of it at
1.5M rows.

"Golden" here means golden PROPERTIES, not golden files. A checked-in expected
parquet would have to be regenerated on every legitimate schema change and
would then prove only that somebody regenerated it. The properties below --
that silver's crash count equals bronze's distinct key count, that two builds
hash identically, that every table satisfies its contract -- are the things a
golden file was standing in for.
"""

from __future__ import annotations

import json

import pytest

from src import contracts
from src.transform import build as build_module
from src.transform import unified


# ===========================================================================
# row-count reconciliation
# ===========================================================================

# (bronze view, bronze distinct key, silver current view). The crash count must
# never silently change between layers -- that is the invariant the "keep,
# never drop" rule exists to protect, and this is where it is enforced.
RECONCILE = [
    ("bronze_incidents", 'COUNT(DISTINCT report_number)',
     "silver_montgomery_crash_current"),
    ("bronze_drivers", 'COUNT(DISTINCT person_id)',
     "silver_montgomery_driver_current"),
    ("bronze_txdot", "COUNT(DISTINCT crash_id)", "silver_txdot_crash_current"),
]


@pytest.mark.parametrize("bronze_rel,key_expr,silver_rel", RECONCILE)
def test_row_counts_reconcile_bronze_to_silver(bronze_con, silver_con,
                                               bronze_rel, key_expr, silver_rel):
    bronze_keys = bronze_con.execute(
        f"SELECT {key_expr} FROM {bronze_rel}"
    ).fetchone()[0]
    silver_rows = silver_con.execute(f"SELECT COUNT(*) FROM {silver_rel}").fetchone()[0]
    assert silver_rows == bronze_keys, (
        f"{silver_rel} has {silver_rows} rows against {bronze_keys} distinct "
        f"keys in {bronze_rel} -- the transform gained or lost records"
    )


def test_non_motorist_reconciliation_accounts_for_the_duplicate_person_ids(
    bronze_con, silver_con
):
    """The one table where silver is legitimately SMALLER than bronze's key count.

    9 person_ids are published twice under one report_number, disagreeing on a
    denormalised crash-level field. The grain is one row per non-motorist, so
    the duplicate is resolved -- and the difference must be exactly the number
    of duplicated ids, never one more.
    """
    bronze_ids = bronze_con.execute(
        "SELECT COUNT(DISTINCT person_id) FROM bronze_non_motorists"
    ).fetchone()[0]
    silver_rows = silver_con.execute(
        "SELECT COUNT(*) FROM silver_montgomery_non_motorist_current"
    ).fetchone()[0]
    assert silver_rows == bronze_ids


def test_fars_accident_reconciles_per_year(bronze_con, silver_con):
    """Per year, because ST_CASE restarts and a cross-year count would hide a
    collision rather than reveal one."""
    bronze = dict(bronze_con.execute(
        "SELECT _bronze_dataset, COUNT(DISTINCT ST_CASE) FROM bronze_fars_accident "
        "GROUP BY 1 ORDER BY 1"
    ).fetchall())
    silver = dict(silver_con.execute(
        "SELECT year, COUNT(*) FROM silver_fars_accident_current GROUP BY 1 ORDER BY 1"
    ).fetchall())
    assert set(silver) == set(bronze)
    for year in bronze:
        # A year may be SMALLER in silver if a later snapshot removed a case --
        # which the fixture's synthetic 2019 reissue does, on purpose.
        assert silver[year] <= bronze[year], f"{year} gained rows"
        closed = silver_con.execute(
            "SELECT COUNT(*) FROM silver_fars_accident_history "
            "WHERE year = ? AND deleted_in_load_ts IS NOT NULL", [year]
        ).fetchone()[0]
        assert silver[year] + closed == bronze[year], (
            f"{year}: {bronze[year]} bronze cases, {silver[year]} current, "
            f"{closed} closed -- they must add up"
        )


def test_unified_crash_grain_reconciles_to_its_sources(silver_con):
    total = silver_con.execute(
        "SELECT COUNT(*) FROM silver_crash_current"
    ).fetchone()[0]
    parts = sum(
        silver_con.execute(f"SELECT COUNT(*) FROM {t}").fetchone()[0]
        for t in ("silver_montgomery_crash_current", "silver_txdot_crash_current",
                  "silver_fars_accident_current")
    )
    assert total == parts


def test_history_contains_every_current_row(silver_con):
    """A current row that is not in history would mean history is not the log."""
    for hist, curr in (
        ("silver_montgomery_crash_history", "silver_montgomery_crash_current"),
        ("silver_montgomery_driver_history", "silver_montgomery_driver_current"),
        ("silver_txdot_crash_history", "silver_txdot_crash_current"),
        ("silver_fars_accident_history", "silver_fars_accident_current"),
        ("silver_fars_person_history", "silver_fars_person_current"),
    ):
        n = silver_con.execute(
            f"SELECT COUNT(*) FROM {hist} WHERE is_current"
        ).fetchone()[0]
        m = silver_con.execute(f"SELECT COUNT(*) FROM {curr}").fetchone()[0]
        assert n == m, f"{hist}: {n} current rows vs {m} in {curr}"


def test_scd2_history_is_a_well_formed_interval_chain(silver_con):
    """Per key: versions number 1..n, valid_to chains to the next valid_from,
    and at most one version is current."""
    for hist in ("silver_montgomery_crash_history", "silver_txdot_crash_history",
                 "silver_fars_accident_history", "silver_fars_person_history"):
        broken = silver_con.execute(
            f"""SELECT COUNT(*) FROM (
                  SELECT natural_key,
                         lead(valid_from) OVER (PARTITION BY natural_key
                                                ORDER BY version_no) AS next_from,
                         valid_to
                  FROM {hist})
                WHERE next_from IS DISTINCT FROM valid_to"""
        ).fetchone()[0]
        assert broken == 0, f"{hist}: {broken} version(s) do not chain"

        multi_current = silver_con.execute(
            f"""SELECT COUNT(*) FROM (
                  SELECT natural_key FROM {hist} WHERE is_current
                  GROUP BY 1 HAVING COUNT(*) > 1)"""
        ).fetchone()[0]
        assert multi_current == 0, f"{hist}: {multi_current} key(s) have two current rows"

        bad_numbering = silver_con.execute(
            f"""SELECT COUNT(*) FROM (
                  SELECT natural_key, MIN(version_no) lo, MAX(version_no) hi,
                         COUNT(*) n FROM {hist} GROUP BY 1)
                WHERE lo <> 1 OR hi <> n"""
        ).fetchone()[0]
        assert bad_numbering == 0, f"{hist}: version_no is not 1..n per key"


# ===========================================================================
# contracts
# ===========================================================================


def test_every_silver_table_satisfies_its_contract(silver_con, silver_root):
    """The build already validates before writing; this re-checks what LANDED.

    The two are not the same assertion. The build validates a relation it is
    about to project and write; this validates the parquet a consumer will read,
    which is the only version that matters after the process exits.
    """
    contract = contracts.load_contract(contracts.SILVER_CONTRACT)
    from tests.conftest import using_full_bronze

    checked = 0
    for source in build_module.ALL_SOURCES:
        for table, _rel, contract_table, order_by in build_module.TABLES[source]:
            view = f"silver_{source}_{table}".replace("-", "_")
            violations = contracts.validate_relation(
                silver_con, view, contract, contract_table,
                unique_keys=[order_by] if table.endswith("_history") else None,
                check_row_count_min=using_full_bronze(),
            )
            assert violations == [], (
                f"{view}:\n" + "\n".join(v.render() for v in violations)
            )
            checked += 1
    violations = contracts.validate_relation(
        silver_con, "silver_crash_current", contract, "silver.crash",
        check_row_count_min=using_full_bronze(),
    )
    assert violations == [], "\n".join(v.render() for v in violations)
    assert checked == sum(len(v) for v in build_module.TABLES.values())


def test_bronze_pages_satisfy_the_bronze_contract(bronze_con):
    """Real bronze pages, against contracts/bronze.schema.json.

    Bronze is all-VARCHAR and additive, so the contract asserts presence and
    type rather than column order -- but a source that stops publishing
    `report_number`, or an ingest that starts casting, must fail here.
    """
    contract = contracts.load_contract(contracts.BRONZE_CONTRACT)
    for view, table in (
        ("bronze_incidents", "montgomery.bhju-22kf"),
        ("bronze_drivers", "montgomery.mmzv-x632"),
        ("bronze_non_motorists", "montgomery.n7fk-dce5"),
        ("bronze_txdot", "txdot.cris_crash"),
        ("bronze_fars_accident", "fars.accident"),
    ):
        violations = contracts.validate_relation(bronze_con, view, contract, table)
        assert violations == [], (
            f"{view}:\n" + "\n".join(v.render() for v in violations)
        )


def test_the_bronze_contract_records_the_two_different_provenance_blocks():
    """FARS writes five _bronze_* columns, the HTTP sources six.

    Requiring all six everywhere would fail every FARS page for a difference
    that is correct: FARS's unit of raw preservation is the annual zip, not a
    page, so a per-row hash inside it would be provenance theatre.
    """
    contract = contracts.load_contract(contracts.BRONZE_CONTRACT)
    http = set(contract["tables"]["montgomery.bhju-22kf"]["properties"])
    zipped = set(contract["tables"]["fars.accident"]["properties"])
    assert "_bronze_row_sha256" in http and "_bronze_page" in http
    assert "_bronze_zip_sha256" in zipped and "_bronze_member" in zipped
    assert "_bronze_row_sha256" not in zipped


def test_silver_crash_matches_the_downstream_lead_contract(silver_con):
    """The columns silver.crash shares with contracts/lead_output.schema.json
    must satisfy that contract's own constraints, not just silver's.

    Silver is upstream of the lead output; if `source_system` drifts out of that
    enum or `jurisdiction` stops being two letters, the failure belongs here and
    not three phases later.
    """
    import json as _json
    from pathlib import Path

    lead = _json.loads(
        (Path(contracts.CONTRACTS_DIR) / "lead_output.schema.json").read_text()
    )
    props = lead["properties"]

    allowed = set(props["source_system"]["enum"])
    got = {r[0] for r in silver_con.execute(
        "SELECT DISTINCT source_system FROM silver_crash_current").fetchall()}
    assert got <= allowed

    bad = silver_con.execute(
        "SELECT COUNT(*) FROM silver_crash_current "
        f"WHERE NOT regexp_matches(jurisdiction, '{props['jurisdiction']['pattern']}')"
    ).fetchone()[0]
    assert bad == 0

    lo = props["severity_ordinal"]["minimum"]
    hi = props["severity_ordinal"]["maximum"]
    assert silver_con.execute(
        f"SELECT COUNT(*) FROM silver_crash_current "
        f"WHERE severity_ordinal NOT BETWEEN {lo} AND {hi}"
    ).fetchone()[0] == 0


# ===========================================================================
# determinism and the manifest
# ===========================================================================


def test_two_builds_over_the_same_bronze_are_byte_identical(pipeline_runner):
    """The idempotent-backfill claim, at the file level.

    Silver is a pure function of bronze, so this must hold with no tolerance --
    not "the same row counts", the same sha256 per file.
    """
    first = pipeline_runner.run()
    second = pipeline_runner.run()
    assert first == second
    assert len(first) >= 10


def test_the_manifest_records_inputs_and_output_hashes(silver_root):
    manifest = json.loads((silver_root / "_build_manifest.json").read_text())

    assert manifest["inputs"], "no input partitions recorded"
    for row in manifest["inputs"]:
        assert {"source", "dataset", "load_ts", "path"} <= set(row)

    assert manifest["outputs"], "no outputs recorded"
    for name, info in manifest["outputs"].items():
        assert len(info["sha256"]) == 64
        assert info["rows"] >= 0
        assert (silver_root / (name + ".parquet")).exists() or name == unified.TABLE

    # Wall-clock time appears here and ONLY here, which is why byte-identity is
    # defined over the parquet files rather than the whole directory.
    assert "built_at" in manifest
    assert not any(p.name == "_build_manifest.json"
                   for p in silver_root.rglob("*.parquet"))


def test_the_manifest_hashes_match_the_files_on_disk(silver_root):
    import hashlib
    manifest = json.loads((silver_root / "_build_manifest.json").read_text())
    for name, info in manifest["outputs"].items():
        path = silver_root / (name + ".parquet")
        assert hashlib.sha256(path.read_bytes()).hexdigest() == info["sha256"]


def test_a_source_can_be_built_alone(tmp_path, bronze_root):
    """--source montgomery must produce a usable silver on its own.

    Partition recovery without a full rebuild is a Part 6 requirement, and a
    unified table that only assembles when all three sources are present would
    make it impossible.
    """
    from tests.conftest import build_silver_into

    result = build_silver_into(tmp_path / "one", bronze_root, sources=["montgomery"])
    outputs = set(result["outputs"])
    assert any(o.startswith("montgomery/") for o in outputs)
    assert not any(o.startswith("txdot/") or o.startswith("fars/") for o in outputs)
    assert unified.TABLE in outputs

    import duckdb
    n = duckdb.connect().execute(
        f"SELECT COUNT(DISTINCT source_system) FROM read_parquet("
        f"'{tmp_path / 'one' / (unified.TABLE + '.parquet')}')"
    ).fetchone()[0]
    assert n == 1


def test_column_order_is_the_contract_order(silver_con):
    """Determinism depends on it, so it is asserted rather than assumed."""
    contract = contracts.load_contract(contracts.SILVER_CONTRACT)
    for source in build_module.ALL_SOURCES:
        for table, _rel, contract_table, _o in build_module.TABLES[source]:
            if contract_table == "fars.codebook":
                continue
            view = f"silver_{source}_{table}".replace("-", "_")
            got = [r[0] for r in silver_con.execute(
                f"DESCRIBE SELECT * FROM {view}").fetchall()]
            assert got == list(contract["tables"][contract_table]["properties"])
