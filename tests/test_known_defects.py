"""The known defects: each fails on bronze and passes on silver.

Every invariant below lives in ONE helper that takes a DuckDB connection and a
relation name. The bronze test and its `test_silver_*` twin call the same
helper against the two layers, so the pair cannot drift apart into two tests
that merely look similar. A test that only passes proves nothing about whether
the transform did anything; a pair of tests that check different things proves
less than that.

The four scaffold tests keep their signatures and are `xfail(strict=True)`: an
unexpected pass FAILS the suite, because a defect that is not present in the
fixture is a broken fixture, not a fixed defect.

Both corpora run the identical assertions -- the committed extracts by default,
the full local bronze under CRASH_TEST_FULL_BRONZE=1. Assertions are therefore
written as properties ("no single date classifies every row"), not as magic
counts, except where a count is the property (the fixture manifest's synthetic
keys). The measured full-corpus numbers are in the build report and in
`python -m src.transform.report`.
"""

from __future__ import annotations

import pytest

from src.config import envelope
from src.transform import common as c
from src.transform.dictionaries import parse_substance, parse_substance_list

pytest_plugins = ()

MOCO_ENV = envelope("montgomery")


# ===========================================================================
# shared assertion helpers
# ===========================================================================


def coordinates_outside_envelope(con, relation: str, *, lat: str, lon: str,
                                 use_assignment_bbox: bool = False) -> int:
    """Rows whose coordinate falls outside the Montgomery envelope.

    `lat`/`lon` are SQL expressions so bronze (VARCHAR needing TRY_CAST) and
    silver (already DOUBLE) can be handed the same helper.
    """
    if use_assignment_bbox:
        pred = (f"{lat} BETWEEN {MOCO_ENV['assignment_min_lat']} "
                f"AND {MOCO_ENV['assignment_max_lat']} "
                f"AND {lon} BETWEEN {MOCO_ENV['assignment_min_lon']} "
                f"AND {MOCO_ENV['assignment_max_lon']}")
    else:
        pred = c.envelope_sql("montgomery", lat, lon)
    return con.execute(
        f"SELECT COUNT(*) FROM {relation} WHERE {lat} IS NOT NULL "
        f"AND {lon} IS NOT NULL AND NOT ({pred})"
    ).fetchone()[0]


def null_or_zero_coordinates(con, relation: str, *, lat: str, lon: str) -> int:
    """The check that does NOT catch the defect. Measured to prove it doesn't."""
    return con.execute(
        f"SELECT COUNT(*) FROM {relation} "
        f"WHERE {lat} IS NULL OR {lon} IS NULL OR {lat} = 0 OR {lon} = 0"
    ).fetchone()[0]


def unnormalised_substance_values(con, relation: str, column: str) -> list[str]:
    """Distinct raw values that a single normalised vocabulary would not accept.

    "Normalised" means: one value, one meaning, one spelling of each concept.
    A column carrying two dictionary generations fails this by construction --
    the same fact is spelled `NONE DETECTED` and
    `Not Suspect of Alcohol Use, Not Suspect of Drug Use`.
    """
    values = [
        r[0] for r in con.execute(
            f"SELECT DISTINCT {c.quote_ident(column)} FROM {relation}"
        ).fetchall()
    ]
    schemes = {parse_substance(v).scheme for v in values if v is not None}
    return sorted(v for v in values if v is not None) if len(
        schemes - {"NULL"}
    ) > 1 else []


def substance_scheme_by_crash_date(con, relation: str, *, date_expr: str,
                                   scheme_expr: str) -> list[tuple]:
    """(date, new_count, old_count) for every date carrying both generations."""
    return con.execute(
        f"""SELECT {date_expr} AS d,
                   SUM(CASE WHEN {scheme_expr} = 'NEW' THEN 1 ELSE 0 END) AS n_new,
                   SUM(CASE WHEN {scheme_expr} = 'OLD' THEN 1 ELSE 0 END) AS n_old
            FROM {relation} GROUP BY 1 HAVING n_new > 0 AND n_old > 0 ORDER BY 1"""
    ).fetchall()


def min_rows_misclassified_by_any_cutover_date(
    con, relation: str, *, key: str, date_expr: str, scheme_expr: str
) -> int:
    """The best any hardcoded cutover date could do, in rows misclassified.

    This is the assertion the scaffold's docstring is really asking for. A
    cutover date D classifies a row as new-generation iff its crash date >= D.
    Minimised over every candidate date in the data. Zero means some date works;
    non-zero means no date does, and the transform must not use one.
    """
    return con.execute(
        f"""WITH v AS (
              SELECT {key} AS k, {date_expr} AS d, {scheme_expr} AS scheme
              FROM {relation} WHERE {scheme_expr} IN ('NEW', 'OLD')
            ), cand AS (SELECT DISTINCT d FROM v)
            SELECT coalesce(MIN(wrong), 0) FROM (
              SELECT cand.d,
                     SUM(CASE WHEN (v.d >= cand.d) <> (v.scheme = 'NEW')
                              THEN 1 ELSE 0 END) AS wrong
              FROM cand CROSS JOIN v GROUP BY 1)"""
    ).fetchone()[0]


def anti_join_counts(con, left: str, right: str, column: str = "report_number") -> tuple[int, int]:
    """(left keys with no right row, right keys with no left row)."""
    q = f"""SELECT COUNT(*) FROM (SELECT DISTINCT {column} FROM {{a}}) a
            WHERE NOT EXISTS (SELECT 1 FROM {{b}} b WHERE b.{column} = a.{column})"""
    return (
        con.execute(q.format(a=left, b=right)).fetchone()[0],
        con.execute(q.format(a=right, b=left)).fetchone()[0],
    )


def rows_per_crash(con, relation: str, column: str = "report_number") -> float:
    return con.execute(
        f"SELECT COUNT(*)::DOUBLE / COUNT(DISTINCT {column}) FROM {relation}"
    ).fetchone()[0]


def duplicate_key_count(con, relation: str, key: list[str]) -> int:
    cols = ", ".join(c.quote_ident(k) for k in key)
    return con.execute(
        f"SELECT COUNT(*) FROM (SELECT {cols} FROM {relation} "
        f"GROUP BY {cols} HAVING COUNT(*) > 1)"
    ).fetchone()[0]


def sentinel_coordinate_rows(con, relation: str, *, lat: str, lon: str) -> int:
    """Rows whose coordinate is a FARS sentinel, matched numerically.

    Numeric, not string: NHTSA writes 77.7777 in 2019 and 77.77770000 in 2024,
    so a string match finds a fraction of them.
    """
    terms = " OR ".join(
        f"abs({lat} - {s}) < 1e-4" for s in (77.7777, 88.8888, 99.9999)
    ) + " OR " + " OR ".join(
        f"abs({lon} - {s}) < 1e-4" for s in (777.7777, 888.8888, 999.9999)
    )
    return con.execute(
        f"SELECT COUNT(*) FROM {relation} WHERE {terms}"
    ).fetchone()[0]


def impossible_coordinate_rows(con, relation: str, *, lat: str, lon: str) -> int:
    """Rows outside the coordinate system's own valid range."""
    return con.execute(
        f"SELECT COUNT(*) FROM {relation} WHERE {lat} IS NOT NULL "
        f"AND ({lat} NOT BETWEEN -90 AND 90 OR {lon} NOT BETWEEN -180 AND 180)"
    ).fetchone()[0]


BRONZE_LAT = "TRY_CAST(latitude AS DOUBLE)"
BRONZE_LON = "TRY_CAST(longitude AS DOUBLE)"

# Bronze has no normalised scheme column, so the test derives one the same way a
# naive implementation would: uppercase-only means old generation, an embedded
# ", " means new. That is deliberately the NAIVE classifier -- the point of the
# bronze half of each pair is to show what the raw data looks like to someone
# who has not written the grammar yet.
BRONZE_SCHEME = ("CASE WHEN driver_substance_abuse ~ '^[A-Z0-9/ ]+$' THEN 'OLD' "
                 "WHEN driver_substance_abuse LIKE '%, %' THEN 'NEW' END")
SILVER_SCHEME = ("CASE WHEN substance_scheme = 'OLD_SINGLE' THEN 'OLD' "
                 "WHEN substance_scheme = 'NEW_PAIR' THEN 'NEW' END")


# ===========================================================================
# 1. coordinates
# ===========================================================================


@pytest.mark.xfail(strict=True, reason="must fail on bronze")
def test_coordinates_within_montgomery_envelope(bronze_incidents):
    """bhju-22kf has zero null and zero zero-valued coordinates, and still
    contains records well outside the county. A null check does not catch this.

    Montgomery County envelope is roughly lat 38.9-39.36, lon -77.54 to -76.87.
    """
    con, rel = bronze_incidents
    # The check that passes and proves nothing.
    assert null_or_zero_coordinates(con, rel, lat=BRONZE_LAT, lon=BRONZE_LON) == 0
    # The check that matters. This is the assertion that must fail on bronze.
    assert coordinates_outside_envelope(
        con, rel, lat=BRONZE_LAT, lon=BRONZE_LON) == 0


def test_silver_coordinates_within_montgomery_envelope(silver_incidents):
    """Silver's canonical coordinate is inside the envelope or NULL -- and the
    defective rows are still there, flagged, with their raw values intact."""
    con, rel = silver_incidents
    assert coordinates_outside_envelope(
        con, rel, lat="latitude", lon="longitude") == 0

    flagged, raw_kept, dist = con.execute(
        f"""SELECT COUNT(*),
                   SUM(CASE WHEN lat_raw IS NOT NULL AND lon_raw IS NOT NULL
                            THEN 1 ELSE 0 END),
                   MIN(distance_from_envelope_m)
            FROM {rel} WHERE geo_quality = 'OUT_OF_ENVELOPE'"""
    ).fetchone()
    assert flagged > 0, (
        "no OUT_OF_ENVELOPE rows in silver -- either the transform dropped them "
        "(it must not) or the fixture no longer contains any"
    )
    # Kept, not dropped: every flagged row still carries its published values.
    assert raw_kept == flagged
    assert dist > 0, "an out-of-envelope row must be a positive distance outside"
    # And the canonical coordinate is NULL for exactly those rows.
    assert con.execute(
        f"SELECT COUNT(*) FROM {rel} WHERE geo_quality = 'OUT_OF_ENVELOPE' "
        "AND latitude IS NOT NULL"
    ).fetchone()[0] == 0


def test_silver_crash_count_is_unchanged_by_the_coordinate_check(
    bronze_incidents, silver_incidents
):
    """The defect handling must not change the crash universe.

    This is the guard that makes "keep, don't drop" enforceable rather than
    aspirational: silver's crash count equals bronze's DISTINCT report_number
    count exactly, including every out-of-envelope row.
    """
    bcon, brel = bronze_incidents
    scon, srel = silver_incidents
    bronze_crashes = bcon.execute(
        f"SELECT COUNT(DISTINCT report_number) FROM {brel}"
    ).fetchone()[0]
    silver_crashes = scon.execute(f"SELECT COUNT(*) FROM {srel}").fetchone()[0]
    assert silver_crashes == bronze_crashes


# ===========================================================================
# 2. two dictionary generations
# ===========================================================================


@pytest.mark.xfail(strict=True, reason="must fail on bronze")
def test_substance_abuse_dictionary_normalised(bronze_drivers):
    """driver_substance_abuse mixes an old uppercase single-value scheme with a
    newer comma-joined pair scheme, and carries at least three distinct
    spellings of null across the two.
    """
    con, rel = bronze_drivers
    unnormalised = unnormalised_substance_values(con, rel, "driver_substance_abuse")
    assert unnormalised == [], (
        f"{len(unnormalised)} raw values spanning two dictionary generations"
    )


def test_silver_substance_abuse_dictionary_normalised(silver_drivers):
    """Silver replaces the raw string with one vocabulary, and keeps the four
    spellings of null apart instead of flattening them."""
    con, rel = silver_drivers
    statuses = {
        r[0] for r in con.execute(
            f"SELECT DISTINCT alcohol_status FROM {rel} "
            f"UNION SELECT DISTINCT drug_status FROM {rel}"
        ).fetchall()
    }
    assert statuses <= {"NOT_SUSPECTED", "SUSPECTED", "UNKNOWN", "NOT_APPLICABLE"}
    assert "UNMAPPED" not in statuses

    assert con.execute(
        f"SELECT COUNT(*) FROM {rel} WHERE substance_scheme = 'UNMAPPED'"
    ).fetchone()[0] == 0

    # Both generations are present and both resolved -- the fixture would be
    # broken if only one were.
    schemes = dict(con.execute(
        f"SELECT substance_scheme, COUNT(*) FROM {rel} GROUP BY 1"
    ).fetchall())
    assert schemes.get("OLD_SINGLE", 0) > 0 and schemes.get("NEW_PAIR", 0) > 0

    # The spellings of null stay distinct. 'N/A' means "no driver to test"
    # (parked, driverless); 'UNKNOWN' means "a driver, not tested". Collapsing
    # them is the lossy move.
    na = con.execute(
        f"SELECT COUNT(*) FROM {rel} WHERE alcohol_status = 'NOT_APPLICABLE'"
    ).fetchone()[0]
    unk = con.execute(
        f"SELECT COUNT(*) FROM {rel} WHERE alcohol_status = 'UNKNOWN'"
    ).fetchone()[0]
    assert na > 0 and unk > 0, "the two null spellings must remain distinguishable"


# ===========================================================================
# 3. the cutover overlaps
# ===========================================================================


@pytest.mark.xfail(strict=True, reason="must fail on bronze")
def test_dictionary_cutover_overlap_handled(bronze_drivers):
    """The two encodings coexist for several days. A hardcoded cutover date is
    wrong. Find the overlap window and prove your handling covers it.
    """
    con, rel = bronze_drivers
    # The substantive assertion: THERE EXISTS a single date that classifies
    # every row's generation correctly. False on bronze, and that is the defect.
    wrong = min_rows_misclassified_by_any_cutover_date(
        con, rel, key='":id"',
        date_expr="substr(crash_date_time, 1, 10)",
        scheme_expr=BRONZE_SCHEME,
    )
    assert wrong == 0, (
        f"the best possible hardcoded cutover date still misclassifies {wrong} "
        "driver row(s) -- the generations overlap"
    )


def test_silver_dictionary_cutover_overlap_handled(silver_drivers, bronze_drivers):
    """Silver resolves every row in the overlap window through the grammar, so
    the window needs no special case at all."""
    bcon, brel = bronze_drivers
    scon, srel = silver_drivers

    window = substance_scheme_by_crash_date(
        bcon, brel, date_expr="substr(crash_date_time, 1, 10)",
        scheme_expr=BRONZE_SCHEME,
    )
    assert window, "the fixture must contain at least one overlapping date"
    lo, hi = window[0][0], window[-1][0]

    total, resolved = scon.execute(
        f"""SELECT COUNT(*),
                   SUM(CASE WHEN substance_scheme IN ('OLD_SINGLE', 'NEW_PAIR')
                            THEN 1 ELSE 0 END)
            FROM {srel} WHERE crash_date BETWEEN DATE '{lo}' AND DATE '{hi}'"""
    ).fetchone()
    assert total > 0
    assert resolved == total, (
        f"{total - resolved} row(s) in the overlap window {lo}..{hi} did not "
        "resolve through the grammar"
    )

    # And both generations really are present inside the window, so the test is
    # not passing because the window happens to be homogeneous.
    in_window = dict(scon.execute(
        f"""SELECT substance_scheme, COUNT(*) FROM {srel}
            WHERE crash_date BETWEEN DATE '{lo}' AND DATE '{hi}' GROUP BY 1"""
    ).fetchall())
    assert in_window.get("OLD_SINGLE", 0) > 0
    assert in_window.get("NEW_PAIR", 0) > 0


# ===========================================================================
# 4. the crash universe
# ===========================================================================


@pytest.mark.xfail(strict=True, reason="must fail on bronze")
def test_incidents_drivers_report_number_reconciliation(bronze_incidents, bronze_drivers):
    """The two tables do not agree on the set of report_numbers. An inner join
    silently drops crashes. Quantify with an anti-join in both directions.
    """
    con, inc = bronze_incidents
    _, drv = bronze_drivers
    inc_only, drv_only = anti_join_counts(con, inc, drv)
    assert (inc_only, drv_only) == (0, 0), (
        f"{inc_only} incident report_number(s) have no driver row; "
        f"{drv_only} driver report_number(s) have no incident row"
    )


def test_silver_incidents_drivers_report_number_reconciliation(
    silver_incidents, silver_drivers, silver_con
):
    """Silver keeps every crash and flags the ones with no party rows, so an
    inner join is never needed and the disagreement is a queryable column."""
    con, inc = silver_incidents
    _, drv = silver_drivers

    inc_only, drv_only = anti_join_counts(con, inc, drv)
    # The direction that holds is enforced: every driver's crash exists.
    assert drv_only == 0
    # The direction that does not is a FLAG, not a dropped row.
    assert inc_only > 0, "the fixture must contain crashes with no driver row"
    flagged = con.execute(
        f"SELECT COUNT(*) FROM {inc} WHERE NOT has_driver_rows"
    ).fetchone()[0]
    assert flagged == inc_only, (
        "has_driver_rows must mark exactly the crashes the anti-join finds"
    )
    # Some are explained by a non-motorist-only crash; the rest have no party
    # row at all, which is the finding.
    explained = con.execute(
        f"SELECT COUNT(*) FROM {inc} WHERE NOT has_driver_rows "
        "AND has_non_motorist_rows"
    ).fetchone()[0]
    assert 0 <= explained <= flagged


# ===========================================================================
# 5. grain
# ===========================================================================


def test_crash_fact_grain_is_one_row_per_crash(silver_crash_fact, silver_con):
    """Drivers is one row per driver but carries denormalised crash-level
    attributes. Aggregating off it overcounts multi-vehicle crashes.
    """
    con, rel = silver_crash_fact

    # One row per crash_uid, and one per (source_system, source_record_id).
    assert duplicate_key_count(con, rel, ["crash_uid"]) == 0
    assert duplicate_key_count(con, rel, ["source_system", "source_record_id"]) == 0

    # Per source, the natural key is unique too.
    for source, natural in (
        ("MONTGOMERY_MD", "silver_montgomery_crash_current"),
        ("TXDOT_CRIS", "silver_txdot_crash_current"),
        ("NHTSA_FARS", "silver_fars_accident_current"),
    ):
        n_fact = con.execute(
            f"SELECT COUNT(*) FROM {rel} WHERE source_system = '{source}'"
        ).fetchone()[0]
        n_src = con.execute(f"SELECT COUNT(*) FROM {natural}").fetchone()[0]
        assert n_fact == n_src, f"{source}: {n_fact} fact rows vs {n_src} source rows"

    # The negative control: counting crashes off the Drivers table gives a
    # materially larger number, which is the defect this grain exists to avoid.
    moco_crashes = con.execute(
        f"SELECT COUNT(*) FROM {rel} WHERE source_system = 'MONTGOMERY_MD'"
    ).fetchone()[0]
    driver_rows = con.execute(
        "SELECT COUNT(*) FROM silver_montgomery_driver_current"
    ).fetchone()[0]
    covered = con.execute(
        "SELECT COUNT(DISTINCT report_number) FROM silver_montgomery_driver_current"
    ).fetchone()[0]
    ratio = driver_rows / covered
    assert ratio > 1.5, (
        f"expected driver rows to fan out over crashes (~1.8x); got {ratio:.3f}"
    )
    assert driver_rows > moco_crashes


def test_silver_party_tables_are_unique_on_their_own_key(silver_drivers,
                                                         silver_non_motorists):
    """The real key, enforced -- which is the other half of the fan-out defect."""
    con, drv = silver_drivers
    _, nmo = silver_non_motorists
    assert duplicate_key_count(con, drv, ["person_id"]) == 0
    assert duplicate_key_count(con, nmo, ["person_id"]) == 0


def test_silver_crash_level_flags_are_not_read_off_a_driver_row(silver_incidents,
                                                                silver_con):
    """Crash-level substance flags must equal the aggregate over drivers.

    Reading them off any single driver row would disagree the moment a
    multi-driver crash has two different values, which the fixture contains.
    """
    con, inc = silver_incidents
    mismatches = con.execute(
        f"""WITH agg AS (
              SELECT report_number,
                     BOOL_OR(alcohol_status = 'SUSPECTED') AS alc,
                     BOOL_OR(drug_status = 'SUSPECTED')    AS drg
              FROM silver_montgomery_driver_current GROUP BY 1)
            SELECT COUNT(*) FROM {inc} i JOIN agg USING (report_number)
            WHERE i.any_driver_alcohol_suspected <> agg.alc
               OR i.any_driver_drug_suspected <> agg.drg"""
    ).fetchone()[0]
    assert mismatches == 0


# ===========================================================================
# 6. FARS sentinels
# ===========================================================================


def test_fars_sentinel_coordinates_excluded(silver_crash_fact, silver_con,
                                            bronze_fars_accident):
    """FARS encodes unknown coordinates as 77.7777 / 88.8888 / 99.9999."""
    bcon, brel = bronze_fars_accident
    scon, rel = silver_crash_fact

    # The defect is present in bronze, in both decimal formats.
    bronze_sentinels = sentinel_coordinate_rows(
        bcon, brel, lat="TRY_CAST(LATITUDE AS DOUBLE)",
        lon="TRY_CAST(LONGITUD AS DOUBLE)")
    assert bronze_sentinels > 0, "the fixture must contain sentinel coordinates"

    formats = {
        r[0] for r in bcon.execute(
            f"""SELECT DISTINCT LATITUDE FROM {brel}
                WHERE abs(TRY_CAST(LATITUDE AS DOUBLE) - 77.7777) < 1e-4
                   OR abs(TRY_CAST(LATITUDE AS DOUBLE) - 88.8888) < 1e-4
                   OR abs(TRY_CAST(LATITUDE AS DOUBLE) - 99.9999) < 1e-4"""
        ).fetchall()
    }
    assert len(formats) > 1, (
        f"expected more than one decimal spelling of the sentinels, got {formats}"
    )
    # And a string match would miss most of them -- which is why the transform
    # compares numerically.
    string_matched = bcon.execute(
        f"""SELECT COUNT(*) FROM {brel}
            WHERE LATITUDE IN ('77.7777', '88.8888', '99.9999')"""
    ).fetchone()[0]
    assert string_matched < bronze_sentinels

    # Silver carries none of them as coordinates.
    assert sentinel_coordinate_rows(scon, rel, lat="latitude", lon="longitude") == 0
    assert impossible_coordinate_rows(scon, rel, lat="latitude", lon="longitude") == 0

    # No crash placed in the Arctic. 71.4N is Point Barrow; nothing outside
    # Alaska belongs above 72, and nothing anywhere does.
    max_lat, max_lat_non_ak = scon.execute(
        f"""SELECT MAX(latitude),
                   MAX(latitude) FILTER (WHERE jurisdiction <> 'AK') FROM {rel}"""
    ).fetchone()
    assert max_lat is None or max_lat <= 72
    assert max_lat_non_ak is None or max_lat_non_ak <= 72

    # The rows themselves are kept, flagged SENTINEL.
    flagged = scon.execute(
        "SELECT COUNT(*) FROM silver_fars_accident_current "
        "WHERE geo_quality = 'SENTINEL'"
    ).fetchone()[0]
    assert flagged > 0
    assert scon.execute(
        "SELECT COUNT(*) FROM silver_fars_accident_current "
        "WHERE geo_quality = 'SENTINEL' AND latitude IS NOT NULL"
    ).fetchone()[0] == 0


def test_fars_unknown_hour_nulls_the_timestamp_not_the_date(silver_con):
    """HOUR/MINUTE=99 must NULL the timestamp and leave the date populated.

    Collapsing an unknown hour to midnight would invent a rush of crashes at
    00:00 that never happened.
    """
    n = silver_con.execute(
        "SELECT COUNT(*) FROM silver_fars_accident_current "
        "WHERE crash_datetime_local IS NULL AND crash_date IS NOT NULL"
    ).fetchone()[0]
    assert n > 0, "the fixture must contain HOUR=99 accidents"
    assert silver_con.execute(
        "SELECT COUNT(*) FROM silver_fars_accident_current "
        "WHERE crash_hour IS NULL AND crash_datetime_local IS NOT NULL"
    ).fetchone()[0] == 0


# ===========================================================================
# 7. idempotency under restatement
# ===========================================================================


def test_pipeline_is_idempotent_under_restatement(pipeline_runner, fixture_manifest,
                                                  bronze_root):
    """Re-running over a date range that includes an amended TxDOT report must
    produce byte-identical output.
    """
    from tests.conftest import using_full_bronze

    first = pipeline_runner.run()
    second = pipeline_runner.run()
    assert first == second, (
        "two builds over identical bronze produced different bytes: "
        + str({k: (first[k], second[k]) for k in first if first[k] != second[k]})
    )
    assert first, "the build produced no parquet files"

    if using_full_bronze():
        pytest.skip(
            "the restatement half needs the fixture's synthetic amended row; "
            "the full corpus has only one TxDOT sweep"
        )

    # The fixture's second TxDOT partition IS the restatement: the same OID
    # range re-swept with one crash amended. Build with only the first partition
    # present, then with both, and compare.
    amended = fixture_manifest["synthetic"]["txdot_amended_crash_id"]
    p1, p2 = fixture_manifest["partitions"]["txdot"]

    one = pipeline_runner.clone_bronze("bronze_before")
    import shutil as _shutil
    _shutil.rmtree(one / "txdot" / "cris_crash" / p2)
    before = pipeline_runner.run(one)
    after = pipeline_runner.run(bronze_root)

    # Exactly one new history version for the amended crash, and none for
    # anything else.
    import duckdb as _duckdb
    con = _duckdb.connect()
    versions = con.execute(
        f"""SELECT natural_key, COUNT(*) FROM read_parquet(
              '{pipeline_runner.workdir}/silver_04/txdot/crash_history.parquet')
            GROUP BY 1 HAVING COUNT(*) > 1"""
    ).fetchall()
    assert versions == [(amended, 2)], (
        f"expected exactly one crash with two versions ({amended}); got {versions}"
    )

    # Every silver file belonging to another SOURCE is byte-identical: a TxDOT
    # amendment must not perturb Montgomery or FARS at all.
    #
    # crash_current.parquet is deliberately excluded from that set and checked
    # separately. It is the unified grain, so it CONTAINS the amended TxDOT
    # crash and is supposed to change -- asserting it identical would be
    # asserting that the amendment never reached the crash fact.
    other_sources = {
        k for k in before
        if not k.startswith("txdot/") and k != "crash_current.parquet"
    }
    differing = {k for k in other_sources if before.get(k) != after.get(k)}
    assert not differing, (
        f"a TxDOT amendment changed another source's silver files: {sorted(differing)}"
    )
    assert before["crash_current.parquet"] != after["crash_current.parquet"], (
        "the unified crash grain did not change -- the amendment never reached it"
    )

    # And it changed in exactly one row, in exactly the two columns an
    # amendment touches.
    uid = f"TXDOT_CRIS:{amended}"
    diff = con.execute(
        f"""SELECT COUNT(*) FROM (
              SELECT * FROM read_parquet(
                '{pipeline_runner.workdir}/silver_04/crash_current.parquet')
              EXCEPT
              SELECT * FROM read_parquet(
                '{pipeline_runner.workdir}/silver_03/crash_current.parquet'))"""
    ).fetchall()[0][0]
    assert diff == 1, f"expected exactly one changed unified row, got {diff}"
    row = con.execute(
        f"""SELECT is_amended, version_no FROM read_parquet(
              '{pipeline_runner.workdir}/silver_04/crash_current.parquet')
            WHERE crash_uid = '{uid}'"""
    ).fetchone()
    assert row == (True, 2)


def test_fars_reissue_versions_the_revised_case_and_closes_the_removed_one(
    silver_con, fixture_manifest, bronze_root
):
    """A FARS year re-downloaded with one record revised and one removed.

    Revised -> a second version, still current. Removed -> the existing version
    closed with deleted_in_load_ts set and is_current false. Everything else
    unchanged, because the row hash is over the conformed attributes.
    """
    from tests.conftest import using_full_bronze
    if using_full_bronze():
        pytest.skip("the full corpus has one partition per FARS year")

    revised = fixture_manifest["synthetic"]["fars_2019_revised_st_case"]
    removed = fixture_manifest["synthetic"]["fars_2019_removed_st_case"]

    rev = silver_con.execute(
        "SELECT version_no, is_current, deleted_in_load_ts FROM "
        "silver_fars_accident_history WHERE year = '2019' AND st_case = ? "
        "ORDER BY version_no", [revised]
    ).fetchall()
    assert [r[0] for r in rev] == [1, 2], f"expected two versions, got {rev}"
    assert rev[0][1] is False and rev[1][1] is True
    assert all(r[2] is None for r in rev)

    rem = silver_con.execute(
        "SELECT version_no, is_current, deleted_in_load_ts FROM "
        "silver_fars_accident_history WHERE year = '2019' AND st_case = ?",
        [removed]
    ).fetchall()
    assert len(rem) == 1, f"a removed record must not gain a version: {rem}"
    assert rem[0][1] is False, "a removed record must not be current"
    assert rem[0][2] is not None, "deleted_in_load_ts must name the closing snapshot"

    # And it is absent from the current slice, without having been deleted from
    # history -- the record of its existence survives.
    assert silver_con.execute(
        "SELECT COUNT(*) FROM silver_fars_accident_current "
        "WHERE year = '2019' AND st_case = ?", [removed]
    ).fetchone()[0] == 0

    # 2024 was NOT re-downloaded, so nothing in it gained a version.
    assert silver_con.execute(
        "SELECT COUNT(*) FROM silver_fars_accident_history "
        "WHERE year = '2024' AND version_no > 1"
    ).fetchone()[0] == 0
