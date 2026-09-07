"""Gold layer: grain, integrity, reconciliation against silver, idempotency.

Every test here runs against a gold built by the real `build_gold()` from the
session's silver (itself built from the committed bronze extract, or from the
full corpus under CRASH_TEST_FULL_BRONZE=1). Nothing is mocked; the assertions
are the ones the assignment's Part 2 makes in prose.
"""

from __future__ import annotations

import hashlib
import shutil

import duckdb
import pytest

from src import config, contracts
from src.transform import conformed, model
from tests.conftest import using_full_bronze


def _one(con, sql, *params):
    return con.execute(sql, list(params)).fetchone()[0]


def _juris_sql() -> str:
    return ", ".join(f"'{j}'" for j in config.model()["scope"]["jurisdictions"])


# ===========================================================================
# 1. grain
# ===========================================================================


def test_fact_crash_is_one_row_per_resolved_crash(gold_con):
    assert _one(gold_con, "SELECT COUNT(*) FROM gold_fact_crash") > 0
    dup_sk = _one(gold_con, "SELECT COUNT(*) FROM (SELECT crash_sk FROM gold_fact_crash "
                            "GROUP BY 1 HAVING COUNT(*) > 1)")
    dup_uid = _one(gold_con, "SELECT COUNT(*) FROM (SELECT primary_crash_uid FROM gold_fact_crash "
                             "GROUP BY 1 HAVING COUNT(*) > 1)")
    assert dup_sk == 0 and dup_uid == 0


def test_bridge_covers_every_scoped_silver_crash_exactly_once(gold_con):
    """The silver -> gold reconciliation: no crash lost, no crash doubled."""
    scoped = _one(gold_con, f"SELECT COUNT(*) FROM silver_crash_current "
                            f"WHERE jurisdiction IN ({_juris_sql()})")
    rows = _one(gold_con, "SELECT COUNT(*) FROM gold_bridge_crash_source")
    distinct = _one(gold_con, "SELECT COUNT(DISTINCT crash_uid) FROM gold_bridge_crash_source")
    assert rows == distinct == scoped, (scoped, rows, distinct)
    missing = gold_con.execute(f"""
        SELECT crash_uid FROM silver_crash_current
        WHERE jurisdiction IN ({_juris_sql()})
          AND crash_uid NOT IN (SELECT crash_uid FROM gold_bridge_crash_source) LIMIT 3
    """).fetchall()
    assert not missing


def test_fact_crash_rows_equal_bridge_primaries(gold_con):
    primaries = _one(gold_con, "SELECT COUNT(*) FROM gold_bridge_crash_source WHERE is_primary")
    facts = _one(gold_con, "SELECT COUNT(*) FROM gold_fact_crash")
    assert primaries == facts
    # ... and every non-primary bridge row is a matched FARS record whose
    # primary carries a Montgomery or TxDOT key: FARS never wins precedence.
    bad = _one(gold_con, """
        SELECT COUNT(*) FROM gold_bridge_crash_source b JOIN gold_fact_crash f USING (crash_sk)
        WHERE NOT b.is_primary
          AND (b.source_system <> 'NHTSA_FARS' OR f.primary_source_system = 'NHTSA_FARS')""")
    assert bad == 0


def test_source_count_and_flags_agree_with_the_bridge(gold_con):
    assert _one(gold_con, """
        SELECT COUNT(*) FROM gold_fact_crash f
        JOIN (SELECT crash_sk, COUNT(*) n, bool_or(source_system='NHTSA_FARS') fars,
                     bool_or(source_system='MONTGOMERY_MD') moco,
                     bool_or(source_system='TXDOT_CRIS') tx
              FROM gold_bridge_crash_source GROUP BY 1) b USING (crash_sk)
        WHERE f.source_count <> b.n OR f.in_fars <> b.fars
           OR f.in_montgomery <> b.moco OR f.in_txdot <> b.tx""") == 0


def test_driver_grain_fans_out_from_crash_grain(gold_con):
    """The Phase 2 negative control, restated at gold: drivers are more
    numerous than crashes, so a crash count taken off fact_driver overcounts."""
    drivers = _one(gold_con, "SELECT COUNT(*) FROM gold_fact_driver WHERE source_system='MONTGOMERY_MD'")
    crashes_with_drivers = _one(gold_con, "SELECT COUNT(DISTINCT crash_sk) FROM gold_fact_driver "
                                          "WHERE source_system='MONTGOMERY_MD'")
    moco_crashes = _one(gold_con, "SELECT COUNT(*) FROM gold_fact_crash WHERE in_montgomery")
    assert drivers > crashes_with_drivers, "no fan-out in the fixture?"
    assert crashes_with_drivers <= moco_crashes
    if using_full_bronze():
        assert 1.6 < drivers / moco_crashes < 2.0, drivers / moco_crashes


# ===========================================================================
# 2. integrity
# ===========================================================================


def _phase3_tables(contract) -> dict[str, str]:
    """{contract table: relation} for the tables `build_gold` actually writes.

    The gold contract also carries Phase 4's `crash_geo` and `dim_block_group`,
    which are built by `python -m src.geo.build` from reference data this
    fixture deliberately does not have. Scoping by `model.COLUMNS` keeps these
    two tests asserting exactly what they always asserted -- every table this
    build produces -- rather than failing on a table it does not.
    """
    return {f"gold.{t}": f"gold_{t}" for t in model.COLUMNS
            if f"gold.{t}" in contract["tables"]}


def test_every_foreign_key_resolves_with_no_nulls(gold_con):
    contract = contracts.load_contract(model.GOLD_CONTRACT)
    checked = 0
    for table in _phase3_tables(contract):
        spec = contract["tables"][table]
        rel = "gold_" + table.split(".", 1)[1]
        for fk in spec["x-table-constraints"]["foreign_keys"]:
            assert "orphans_allowed_when" not in fk, f"gold admits no orphan licence: {table} {fk}"
            child = fk["columns"][0]
            parent_rel = "gold_" + fk["references"]["table"].split(".", 1)[1]
            parent = fk["references"]["columns"][0]
            orphans = _one(gold_con, f"SELECT COUNT(*) FROM {rel} ch WHERE NOT EXISTS "
                                     f"(SELECT 1 FROM {parent_rel} p WHERE p.{parent} = ch.{child})")
            assert orphans == 0, f"{table}.{child} -> {parent_rel}.{parent}: {orphans} orphans"
            if fk["columns"][0].endswith("_sk"):
                nulls = _one(gold_con, f"SELECT COUNT(*) FROM {rel} WHERE {child} IS NULL")
                assert nulls == 0, f"{table}.{child} has NULLs"
            checked += 1
    assert checked >= 15


@pytest.mark.parametrize("dim,sk", [
    ("gold_dim_time", "time_sk"), ("gold_dim_geography", "geography_sk"),
    ("gold_dim_road_class", "road_class_sk"), ("gold_dim_weather_condition", "weather_condition_sk"),
    ("gold_dim_non_motorist_type", "non_motorist_type_sk"),
])
def test_dimensions_carry_an_unknown_member(gold_con, dim, sk):
    assert _one(gold_con, f"SELECT COUNT(*) FROM {dim} WHERE {sk} = -1") == 1


def test_dim_date_covers_every_fact_date(gold_con):
    for fact in ("gold_fact_crash", "gold_fact_driver", "gold_fact_non_motorist"):
        assert _one(gold_con, f"SELECT COUNT(*) FROM {fact} f WHERE date_sk NOT IN "
                              f"(SELECT date_sk FROM gold_dim_date)") == 0
    assert _one(gold_con, "SELECT COUNT(*) FROM gold_dim_date d WHERE date_sk <> "
                          "CAST(strftime(d.date, '%Y%m%d') AS INTEGER)") == 0
    # Contiguous: one row per day from min to max, no gaps.
    lo, hi, n = gold_con.execute("SELECT MIN(date), MAX(date), COUNT(*) FROM gold_dim_date").fetchone()
    assert (hi - lo).days + 1 == n


def test_unknown_hour_lands_on_the_unknown_time_member_not_a_null(gold_con):
    """FARS HOUR/MINUTE = 99 -> silver crash_datetime_local NULL -> gold time_sk -1
    with the DATE still populated. The row is not lost and the key is not NULL."""
    assert _one(gold_con, "SELECT COUNT(*) FROM gold_fact_crash WHERE crash_datetime_local IS NULL "
                          "AND (time_sk <> -1 OR date_sk IS NULL)") == 0
    assert _one(gold_con, "SELECT COUNT(*) FROM gold_fact_crash WHERE crash_datetime_local IS NOT NULL "
                          "AND time_sk <> hour(crash_datetime_local) * 100 + minute(crash_datetime_local)") == 0
    in_scope_unknown = _one(gold_con, f"SELECT COUNT(*) FROM silver_crash_current WHERE "
                                      f"crash_datetime_local IS NULL AND jurisdiction IN ({_juris_sql()})")
    assert _one(gold_con, "SELECT COUNT(*) FROM gold_fact_crash WHERE time_sk = -1") >= (
        1 if in_scope_unknown else 0)


def test_time_sk_is_a_wall_clock_minute(gold_con):
    assert _one(gold_con, "SELECT COUNT(*) FROM gold_dim_time") == 24 * 60 + 1
    assert _one(gold_con, "SELECT COUNT(*) FROM gold_dim_time WHERE time_sk >= 0 "
                          "AND time_sk <> hour * 100 + minute") == 0


def test_geography_resolves_to_a_county_wherever_the_source_names_one(gold_con):
    """Montgomery is county 24031 by construction; TxDOT and FARS carry a county
    FIPS. A row falls back to the STATE member only when its county is absent,
    and to UNKNOWN never (every in-scope jurisdiction has a state member)."""
    assert _one(gold_con, "SELECT COUNT(*) FROM gold_fact_crash WHERE geography_sk = -1") == 0
    assert _one(gold_con, """
        SELECT COUNT(*) FROM gold_fact_crash f JOIN gold_dim_geography g USING (geography_sk)
        WHERE f.primary_source_system = 'MONTGOMERY_MD' AND g.county_geoid <> '24031'""") == 0
    assert _one(gold_con, """
        SELECT COUNT(*) FROM gold_fact_crash f JOIN gold_dim_geography g USING (geography_sk)
        WHERE g.jurisdiction <> f.jurisdiction""") == 0


# ===========================================================================
# 3. severity and counts
# ===========================================================================


def test_resolved_severity_is_the_max_over_sources_and_grain_flags_disagreement(gold_con):
    below = _one(gold_con, """
        SELECT COUNT(*) FROM gold_fact_crash f
        JOIN gold_bridge_crash_source b USING (crash_sk)
        JOIN silver_crash_current s USING (crash_uid)
        WHERE s.severity_ordinal > f.severity_ordinal""")
    assert below == 0, "a linked source is more severe than the resolved crash"
    assert _one(gold_con, "SELECT COUNT(*) FROM gold_fact_crash WHERE "
                          "severity_ordinal < severity_ordinal_primary") == 0
    assert _one(gold_con, """
        SELECT COUNT(*) FROM gold_fact_crash
        WHERE (severity_grain = 'RESOLVED_MAX') <> (severity_ordinal <> severity_ordinal_primary)""") == 0
    assert _one(gold_con, "SELECT COUNT(*) FROM gold_fact_crash WHERE severity_ordinal NOT BETWEEN 0 AND 5") == 0


def test_party_counts_reconcile_with_silver(gold_con):
    """Montgomery counts on fact_crash come from the party facts; they must
    equal Phase 2's per-crash row counts exactly. FARS driver/non-motorist
    counts must equal the PER_TYP split of the person file."""
    assert _one(gold_con, """
        SELECT COUNT(*) FROM gold_fact_crash f
        JOIN silver_montgomery_crash_current m ON f.primary_crash_uid = 'MONTGOMERY_MD:' || m.report_number
        WHERE f.driver_count <> coalesce(m.driver_row_count, 0)
           OR f.non_motorist_count <> coalesce(m.non_motorist_row_count, 0)""") == 0
    assert _one(gold_con, """
        SELECT COUNT(*) FROM gold_fact_crash f
        WHERE f.driver_count <> (SELECT COUNT(*) FROM gold_fact_driver d WHERE d.crash_sk = f.crash_sk
                                 AND d.source_system = f.primary_source_system)
          AND f.count_source = 'PARTY_ROWS'""") == 0
    # 785-style driverless crashes are rows with driver_count = 0, not missing rows.
    driverless_silver = _one(gold_con, "SELECT COUNT(*) FROM silver_montgomery_crash_current "
                                       "WHERE NOT has_driver_rows")
    driverless_gold = _one(gold_con, "SELECT COUNT(*) FROM gold_fact_crash "
                                     "WHERE primary_source_system='MONTGOMERY_MD' AND driver_count = 0")
    assert driverless_gold == driverless_silver
    # TxDOT: no party rows, counts from CRIS, count_source says so.
    assert _one(gold_con, "SELECT COUNT(*) FROM gold_fact_crash WHERE primary_source_system='TXDOT_CRIS' "
                          "AND (count_source <> 'CRIS_COUNTS' OR driver_count IS NOT NULL)") == 0
    assert _one(gold_con, "SELECT COUNT(*) FROM gold_fact_driver WHERE source_system = 'TXDOT_CRIS'") == 0


def test_fars_fatal_counts_agree_between_person_rows_and_the_accident_file(gold_con):
    """FARS publishes FATALS on the accident row; gold derives fatal_count from
    person INJ_SEV. Phase 2 measured zero accidents lacking a K person; assert
    the same here (a revised synthetic row in the fixture is the exception and
    is excluded by name)."""
    mismatched = gold_con.execute("""
        SELECT primary_crash_uid, fatal_count, fars_fatal_count FROM gold_fact_crash
        WHERE primary_source_system = 'NHTSA_FARS' AND fatal_count <> fars_fatal_count
    """).fetchall()
    # The fixture's synthetic reissue adds 1 to one accident's FATALS without
    # adding a person: that single row is the known, deliberate mismatch.
    allowed = 0 if using_full_bronze() else 1
    assert len(mismatched) <= allowed, mismatched


def test_fars_passengers_are_counted_out_of_party_scope_not_dropped(gold_con, gold_manifest):
    out = gold_manifest["stats"].get("fars_persons_out_of_party_scope", {})
    in_gold = _one(gold_con, "SELECT COUNT(*) FROM gold_fact_driver WHERE source_system='NHTSA_FARS'") + \
              _one(gold_con, "SELECT COUNT(*) FROM gold_fact_non_motorist WHERE source_system='NHTSA_FARS'")
    in_scope_persons = _one(gold_con, """
        SELECT COUNT(*) FROM silver_fars_person_current p
        WHERE 'NHTSA_FARS:' || p.year || '-' || p.st_case IN (SELECT crash_uid FROM gold_bridge_crash_source)""")
    assert in_gold + sum(int(v) for v in out.values()) == in_scope_persons
    assert set(map(int, out)) <= {2, 3, 4, 9, 10}


# ===========================================================================
# 4. crosswalks
# ===========================================================================


@pytest.mark.parametrize("vocab,system,column,table,expr", [
    ("road_class", "MONTGOMERY_MD", "route_type", "silver_montgomery_crash_current",
     "coalesce(route_type, '__NULL__')"),
    ("weather_condition", "MONTGOMERY_MD", "weather", "silver_montgomery_crash_current",
     "coalesce(weather, '__NULL__')"),
    ("non_motorist_type", "MONTGOMERY_MD", "pedestrian_type", "silver_montgomery_non_motorist_current",
     "coalesce(pedestrian_type, '__NULL__')"),
    ("road_class", "TXDOT_CRIS", "road_cls_id", "silver_txdot_crash_current",
     "coalesce(CAST(road_cls_id AS VARCHAR), '__NULL__')"),
    ("weather_condition", "TXDOT_CRIS", "wthr_cond_id", "silver_txdot_crash_current",
     "coalesce(CAST(wthr_cond_id AS VARCHAR), '__NULL__')"),
    ("road_class", "NHTSA_FARS", "func_sys", "silver_fars_accident_current",
     "coalesce(CAST(func_sys AS VARCHAR), '__NULL__')"),
    ("weather_condition", "NHTSA_FARS", "weather", "silver_fars_accident_current",
     "coalesce(CAST(weather AS VARCHAR), '__NULL__')"),
])
def test_every_distinct_silver_value_has_a_crosswalk_row(gold_con, vocab, system, column, table, expr):
    conformed.register(gold_con, vocab)
    conformed.assert_all_mapped(gold_con, vocab, table=table, source_system=system,
                                source_column=column, value_expr=expr)


def test_an_unmapped_value_raises_naming_it():
    con = duckdb.connect()
    conformed.register(con, "weather_condition")
    con.execute("CREATE TABLE t AS SELECT 'Volcanic Ash' AS weather UNION ALL SELECT 'Clear'")
    with pytest.raises(conformed.UnmappedValue, match="Volcanic Ash"):
        conformed.assert_all_mapped(con, "weather_condition", table="t", source_system="MONTGOMERY_MD",
                                    source_column="weather", value_expr="weather")


def test_crosswalk_csv_rejects_a_code_outside_the_vocabulary(tmp_path):
    p = tmp_path / "bad.csv"
    p.write_text("source_system,source_column,source_value,conformed_code,notes\n"
                 "MONTGOMERY_MD,weather,Clear,SUNNY,\n")
    conformed.load.cache_clear()
    with pytest.raises(ValueError, match="SUNNY"):
        conformed.load("weather_condition", str(p))
    conformed.load.cache_clear()


def test_txdot_codes_are_unknown_not_guessed(gold_con):
    """No CRIS lookup could be cited, so every TxDOT road class and weather row
    must sit on UNKNOWN. Decoding one without a citation is the defect."""
    for col, dim, sk in (("road_class_sk", "gold_dim_road_class", "road_class_sk"),
                         ("weather_condition_sk", "gold_dim_weather_condition", "weather_condition_sk")):
        assert _one(gold_con, f"SELECT COUNT(*) FROM gold_fact_crash WHERE primary_source_system='TXDOT_CRIS' "
                              f"AND {col} <> -1") == 0
    assert _one(gold_con, "SELECT COUNT(*) FROM gold_map_road_class_source WHERE source_system='TXDOT_CRIS' "
                          "AND source_value <> '__NULL__' AND NOT is_lossy") == 0


def test_lossy_rows_are_flagged_from_their_notes(gold_con):
    assert _one(gold_con, "SELECT COUNT(*) FROM gold_map_road_class_source WHERE is_lossy") >= 5
    assert _one(gold_con, "SELECT COUNT(*) FROM gold_map_road_class_source WHERE "
                          "source_value = 'Interstate (State)' AND is_lossy") == 0


# ===========================================================================
# 5. scope, keys, contract
# ===========================================================================


def test_scope_exclusion_is_declared_and_counted(gold_con, gold_manifest):
    juris = set(config.model()["scope"]["jurisdictions"])
    assert _one(gold_con, f"SELECT COUNT(*) FROM gold_fact_crash WHERE jurisdiction NOT IN ({_juris_sql()})") == 0
    excluded = _one(gold_con, f"SELECT COUNT(*) FROM silver_crash_current WHERE jurisdiction NOT IN ({_juris_sql()})")
    assert gold_manifest["stats"]["scope"]["excluded_total"] == excluded
    assert set(gold_manifest["stats"]["scope"]["jurisdictions"]) == juris
    for _src, j, _n in gold_manifest["stats"]["scope"]["excluded_by_source_jurisdiction"]:
        assert j not in juris


def test_crash_sk_is_the_documented_hash_of_the_primary_uid(gold_con):
    rows = gold_con.execute("SELECT primary_crash_uid, crash_sk FROM gold_fact_crash "
                            "ORDER BY primary_crash_uid LIMIT 50").fetchall()
    for uid, sk in rows:
        assert sk == int(hashlib.sha256(uid.encode()).hexdigest()[:15], 16), uid
    # Pinned constant: guards against anyone quietly changing the hash function,
    # which would renumber every key downstream of gold.
    assert _one(gold_con, "SELECT " + model.SK.format(expr="'MONTGOMERY_MD:PINNED'")) == \
        int(hashlib.sha256(b"MONTGOMERY_MD:PINNED").hexdigest()[:15], 16) == 397006722033535768


def test_party_keys_are_unique_per_source_party(gold_con):
    for fact, sk in (("gold_fact_driver", "driver_sk"), ("gold_fact_non_motorist", "non_motorist_sk")):
        assert _one(gold_con, f"SELECT COUNT(*) FROM (SELECT {sk} FROM {fact} GROUP BY 1 HAVING COUNT(*) > 1)") == 0
        assert _one(gold_con, f"SELECT COUNT(*) FROM (SELECT source_system, party_natural_key FROM {fact} "
                              f"GROUP BY 1, 2 HAVING COUNT(*) > 1)") == 0


def test_gold_tables_pass_their_contract(gold_con):
    contract = contracts.load_contract(model.GOLD_CONTRACT)
    resolve = _phase3_tables(contract)
    violations = []
    for table, rel in resolve.items():
        violations += contracts.validate_relation(gold_con, rel, contract, table,
                                                  check_row_count_min=using_full_bronze())
        violations += contracts.validate_foreign_keys(gold_con, contract, table, rel, resolve)
    assert not violations, "\n".join(v.render() for v in violations)


def test_contract_names_the_table_and_column_of_a_foreign_key_violation():
    con = duckdb.connect()
    contract = contracts.load_contract(model.GOLD_CONTRACT)
    con.execute("CREATE TABLE dim_severity AS SELECT * FROM (VALUES (0),(1),(2),(3),(4),(5)) t(severity_sk)")
    con.execute("CREATE TABLE bad AS SELECT 7 AS severity_sk, 'x' AS other")
    v = contracts.validate_foreign_keys(
        con, contract, "gold.fact_driver", "bad", {"gold.dim_severity": "dim_severity"})
    assert len(v) == 1
    text = v[0].render()
    assert "severity_sk" in text and "gold.dim_severity" in text and "fact_driver" in text


def test_gold_manifest_records_silver_inputs_and_resolution(gold_manifest):
    assert gold_manifest["inputs"], "silver hashes missing"
    assert all(len(h) == 64 for h in gold_manifest["inputs"].values())
    er = gold_manifest["stats"]["entity_resolution"]
    assert er["totals"]["matched"] >= 0 and "by_jurisdiction_year" in er
    assert gold_manifest["config"]["model"]["resolution"]["tier_a_max_distance_m"] == 250


# ===========================================================================
# 6. idempotency and restatement at gold
# ===========================================================================


def test_gold_is_byte_identical_across_two_builds(gold_runner):
    first = gold_runner.run()
    second = gold_runner.run()
    assert first, "no gold parquet written"
    assert first == second, {k: (first[k], second[k]) for k in first if first[k] != second[k]}


def test_txdot_amendment_changes_one_fact_row_and_never_its_key(gold_runner, fixture_manifest, bronze_root):
    if using_full_bronze():
        pytest.skip("needs the fixture's synthetic amended TxDOT partition")
    amended = fixture_manifest["synthetic"]["txdot_amended_crash_id"]
    _p1, p2 = fixture_manifest["partitions"]["txdot"]

    one = gold_runner.clone_bronze("bronze_before")
    shutil.rmtree(one / "txdot" / "cris_crash" / p2)
    before = gold_runner.run(one)
    gold_before = gold_runner.last_gold
    after = gold_runner.run(bronze_root)
    gold_after = gold_runner.last_gold

    # Only fact_crash may change. Dimensions, the bridge (no key moved), and
    # the party facts (TxDOT has none) are byte-identical.
    changed = {k for k in before if before[k] != after.get(k)}
    assert changed == {"fact_crash.parquet"}, changed

    con = duckdb.connect()
    uid = f"TXDOT_CRIS:{amended}"
    # `_silver_build_sha` is the hash of the silver TABLE a row came from, so
    # every TxDOT row's provenance pointer legitimately moves; excluding it,
    # exactly one row differs.
    diff = con.execute(f"""
        SELECT primary_crash_uid FROM (
          SELECT * EXCLUDE (_silver_build_sha) FROM read_parquet('{gold_after}/fact_crash.parquet')
          EXCEPT SELECT * EXCLUDE (_silver_build_sha) FROM read_parquet('{gold_before}/fact_crash.parquet'))""").fetchall()
    assert diff == [(uid,)], diff
    sha_moved = con.execute(f"""
        SELECT a.primary_source_system, COUNT(*) FILTER (WHERE a._silver_build_sha <> b._silver_build_sha)
        FROM read_parquet('{gold_after}/fact_crash.parquet') a
        JOIN read_parquet('{gold_before}/fact_crash.parquet') b USING (crash_sk)
        GROUP BY 1 ORDER BY 1""").fetchall()
    assert all(n == 0 for src, n in sha_moved if src != "TXDOT_CRIS"), sha_moved
    assert all(n > 0 for src, n in sha_moved if src == "TXDOT_CRIS"), sha_moved
    b, a = [con.execute(f"""SELECT crash_sk, is_amended, silver_version_no, _silver_build_sha
                            FROM read_parquet('{g}/fact_crash.parquet')
                            WHERE primary_crash_uid = '{uid}'""").fetchone()
            for g in (gold_before, gold_after)]
    assert b[0] == a[0], "the surrogate key moved on an amendment"
    assert (b[1], b[2]) == (False, 1) and (a[1], a[2]) == (True, 2)
    assert b[3] != a[3], "the silver provenance hash did not follow the restated table"


def test_removing_a_matched_fars_record_leaves_the_local_key_and_drops_the_link(
    gold_runner, fixture_manifest, bronze_root
):
    """A FARS reissue that drops an accident matched to a Montgomery crash: the
    Montgomery crash_sk stays, in_fars flips to false, the bridge loses exactly
    that FARS row, and the Montgomery-side severity reverts to its own value."""
    if using_full_bronze():
        pytest.skip("synthesises a third FARS partition on top of the fixture tree")

    baseline = gold_runner.run(bronze_root)
    g0 = gold_runner.last_gold
    con = duckdb.connect()
    pair = con.execute(f"""
        SELECT b.crash_uid, b.crash_sk, f.primary_crash_uid
        FROM read_parquet('{g0}/bridge_crash_source.parquet') b
        JOIN read_parquet('{g0}/fact_crash.parquet') f USING (crash_sk)
        WHERE NOT b.is_primary AND b.source_system = 'NHTSA_FARS'
          AND f.primary_source_system = 'MONTGOMERY_MD' AND b.crash_uid LIKE 'NHTSA_FARS:2019-%'
        ORDER BY b.crash_uid LIMIT 1""").fetchone()
    assert pair, "the fixture must contain at least one real FARS ∩ Montgomery 2019 pair"
    fars_uid, crash_sk, moco_uid = pair
    st_case = fars_uid.split("-", 1)[1]

    # Third 2019 partition: the newest snapshot minus that ST_CASE.
    tree = gold_runner.clone_bronze("bronze_reissue")
    parts = sorted(p.name for p in (tree / "fars" / "2019").iterdir())
    newest = tree / "fars" / "2019" / parts[-1]
    p3 = tree / "fars" / "2019" / "20260103T000000000Z"
    p3.mkdir()
    for member in ("accident", "vehicle", "person"):
        con.execute(f"""COPY (SELECT * REPLACE ('20260103T000000000Z' AS _bronze_load_ts)
                             FROM read_parquet('{newest / member}.parquet')
                             WHERE ST_CASE <> '{st_case}' ORDER BY ST_CASE)
                        TO '{p3 / member}.parquet' (FORMAT PARQUET, COMPRESSION ZSTD)""")

    after = gold_runner.run(tree)
    g1 = gold_runner.last_gold
    row0, row1 = [con.execute(f"""SELECT crash_sk, in_fars, source_count, severity_ordinal,
                                         severity_ordinal_primary, severity_grain
                                  FROM read_parquet('{g}/fact_crash.parquet')
                                  WHERE primary_crash_uid = '{moco_uid}'""").fetchone()
                  for g in (g0, g1)]
    assert row0[0] == row1[0] == crash_sk, "the Montgomery key moved"
    assert row0[1:3] == (True, 2) and row1[1:3] == (False, 1)
    assert row1[3] == row1[4], "severity should revert to the primary's own value"
    assert row1[5] != "RESOLVED_MAX"
    assert con.execute(f"SELECT COUNT(*) FROM read_parquet('{g1}/bridge_crash_source.parquet') "
                       f"WHERE crash_uid = '{fars_uid}'").fetchone()[0] == 0
    # The FARS record's own key never existed as a fact row in either build.
    for g in (g0, g1):
        assert con.execute(f"SELECT COUNT(*) FROM read_parquet('{g}/fact_crash.parquet') "
                           f"WHERE primary_crash_uid = '{fars_uid}'").fetchone()[0] == 0
    # Everything unrelated to that crash is unchanged: the dims are byte-identical.
    for k in baseline:
        if k.startswith("dim_") or k.startswith("map_"):
            assert baseline[k] == after[k], k
