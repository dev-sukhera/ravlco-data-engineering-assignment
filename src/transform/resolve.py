"""Cross-source entity resolution: which FARS fatality is which local crash.

    python -m src.transform.resolve --report
    python -m src.transform.resolve --report --silver-root /tmp/s --json

The assignment asks for resolution "or a proof that the universes are disjoint
enough to scope instead". The universes are NOT disjoint, and the overlap is
exactly one shape: FARS is a national fatality census, so every fatal crash in
Montgomery County or Texas that FARS recorded also appears -- as a fatal crash --
in the county or state feed. Montgomery and TxDOT cannot overlap with each other
(one is Maryland, one is Texas; the jurisdiction column makes that a zero-row
assertion, see `disjointness()`). So resolution is scoped to FARS ∩ {Montgomery,
TxDOT}, and this module is the measurement that scoping rests on.


How a match is decided
----------------------
Blocking: same jurisdiction, same county, crash dates at most one day apart.
County is the only geography every source publishes -- Montgomery is county 031
by construction, TxDOT and FARS carry county FIPS -- and it shrinks the pair
space from 10^5 x 10^5 to a few dozen per FARS record. One day rather than zero
because a 23:50 crash CAN be dated differently in two sources; measured on the
local corpus no accepted match actually straddled midnight (0 of 1,157), which
is itself worth knowing -- both feeds date the crash the same way.

Scoring is two numbers, no weights to defend: geodesic distance in metres and
wall-clock difference in minutes. Both sides publish naive LOCAL time, so the
difference is honest without timezone work (Phase 4 localises; the pair is in
one county so both clocks are the same zone). Distance is computed on the WGS84
ellipsoid via pyproj.Geod, registered as a DuckDB scalar function -- the same
`common.GEOD` Phase 2 used for the envelope report. Not projected, and not
EPSG:3857: a Maryland pair and a Texas pair need two different projected CRSs,
and 3857's scale error at 39N is 29%, which at a 250 m threshold is the
difference between a match and a miss.

Tiers, thresholds in config/model.toml, measured before they were chosen:
  A  both sides have OK coordinates and a time; within 250 m and 30 min.
  B  same, within 1,000 m and 120 min. Admitted and flagged.
  C  one side has no usable coordinate (FARS sentinel, quarantined envelope,
     TxDOT MISSING): county + date + time within 30 min. No location evidence,
     so the time window is the tight one.
Anything else is not a match, and the reason it is not is recorded.

Assignment is one-to-one and deterministic: rank every FARS record's candidates
by (tier, distance, time, crash_uid) and every local record's candidates the
same way; accept the pairs that are each other's best; remove them; repeat until
nothing new is accepted. The `crash_uid` tie-break is what makes two builds
produce the same bridge byte for byte -- without it a tie would be resolved by
scan order, which is thread scheduling.

The fatal block. FARS only contains fatal crashes, so the local candidate should
be fatal too -- but "fatal" has to be read from BOTH local signals, because they
disagree: Montgomery's `acrs_report_type = 'Fatal Crash'` and its max-over-
parties `severity_ordinal = 5` differ on 31 crashes (Phase 3 report). The block
takes the union. Separately, this module measures how many FARS fatalities have
a tier-A-quality neighbour the local source did NOT call fatal: FARS counts a
death within 30 days of the crash, the local feeds record at-scene severity, so
such pairs are real -- and they are reported, not admitted, unless
`admit_non_fatal_candidates` is switched on. Measured 2026-09-08: 2 FARS
records in Maryland and 22 in Texas have a tier-A-quality neighbour the local
feed did not call fatal; 21 of the 24 are otherwise unmatched.
"""

from __future__ import annotations

import argparse
import json
import logging
import sys
import time
from pathlib import Path
from typing import Any

import duckdb
from duckdb import sqltypes as T

from .. import config
from . import common as c

log = logging.getLogger("transform.resolve")

TIER_A, TIER_B, TIER_C = "A", "B", "C"
METHOD_FOR_TIER = {
    TIER_A: "FATAL_DATE_GEO_TIME_A",
    TIER_B: "FATAL_DATE_GEO_TIME_B",
    TIER_C: "FATAL_DATE_COUNTY_TIME",
}
MAX_PASSES = 20

# The relation names model.py registers before calling `run`. Fixed here so the
# SQL below is readable; a caller with different names passes a mapping.
DEFAULT_VIEWS = {
    "crash": "scoped_crash",          # silver.crash filtered to gold scope
    "montgomery": "moco_crash",
    "txdot": "txd_crash",
    "fars": "fars_accident",
}

REASONS_FARS = (
    "OUTSIDE_LOCAL_COVERAGE",     # no local source for this jurisdiction (FL)
    "NO_LOCAL_ROWS_IN_COUNTY",    # a local source exists but has no crash in this county
                                  # (Montgomery is one county; the TxDOT slice is partial)
    "NO_CANDIDATE_ON_DATE",       # local rows in the county, none within +-1 day
    "NO_FATAL_CANDIDATE_ON_DATE", # candidates exist, none recorded as fatal
    "CANDIDATE_TOO_FAR",          # fatal candidates exist, none inside a tier
    "NO_TIME_FOR_TIER_C",         # no coordinates AND no time on one side
    "CANDIDATE_TAKEN",            # an in-tier candidate was assigned elsewhere
)
REASONS_LOCAL = (
    "OUTSIDE_FARS_COVERAGE",      # crash_date outside the loaded FARS years
    "NO_FARS_ON_DATE",
    "FARS_TOO_FAR",
    "NO_TIME_FOR_TIER_C",
    "FARS_TAKEN",
)


def register_udfs(con: duckdb.DuckDBPyConnection) -> None:
    """`geodesic_m(lat1, lon1, lat2, lon2)` -- metres on the WGS84 ellipsoid.

    A Python scalar UDF is slow per row, and that is fine here: blocking leaves
    ~10^4 candidate pairs, not 10^10. It buys exactness (pyproj.Geod, not a
    spherical haversine) and reuse of the one geodesy routine the pipeline has.
    CRS: input EPSG:4326, output metres on the ellipsoid. No projection.
    """
    def geodesic_m(lat1, lon1, lat2, lon2):
        if lat1 is None or lon1 is None or lat2 is None or lon2 is None:
            return None
        return c.geodesic_distance_m(lat1, lon1, lat2, lon2)

    try:
        con.create_function("geodesic_m", geodesic_m, [T.DOUBLE] * 4, T.DOUBLE,
                            null_handling="special")
    except duckdb.Error as exc:  # already registered on this connection
        if "already" not in str(exc).lower():
            raise


def thresholds(cfg: dict[str, Any] | None = None) -> dict[str, Any]:
    cfg = cfg if cfg is not None else config.model()
    r = cfg["resolution"]
    return {
        "max_date_delta_days": int(r["max_date_delta_days"]),
        "tier_a_max_distance_m": float(r["tier_a_max_distance_m"]),
        "tier_a_max_time_min": float(r["tier_a_max_time_min"]),
        "tier_b_max_distance_m": float(r["tier_b_max_distance_m"]),
        "tier_b_max_time_min": float(r["tier_b_max_time_min"]),
        "tier_c_max_time_min": float(r["tier_c_max_time_min"]),
        "admit_non_fatal_candidates": bool(r["admit_non_fatal_candidates"]),
    }


# ---------------------------------------------------------------------------
# the two sides
# ---------------------------------------------------------------------------


def _sides_sql(views: dict[str, str], present: set[str]) -> tuple[str, str]:
    """SQL for `er_fars` and `er_local`, over whichever sources are present."""
    fars = f"""
        SELECT s.crash_uid, s.jurisdiction, a.county_fips,
               s.crash_date, s.crash_datetime_local,
               s.latitude, s.longitude,
               s.geo_quality = 'OK' AS geo_ok
        FROM {views['crash']} s
        JOIN {views['fars']} a
          ON a.year || '-' || a.st_case = s.source_record_id AND a.is_current
        WHERE s.source_system = 'NHTSA_FARS'
    """
    locals_: list[str] = []
    if "montgomery" in present:
        locals_.append(f"""
        SELECT s.crash_uid, s.source_system, s.jurisdiction,
               '031' AS county_fips,
               s.crash_date, s.crash_datetime_local, s.latitude, s.longitude,
               s.geo_quality = 'OK' AS geo_ok,
               -- BOTH fatal signals, because they disagree on 31 crashes.
               (m.acrs_report_type = 'Fatal Crash' OR s.severity_ordinal = 5)
                   AS is_fatal_candidate,
               CASE WHEN m.acrs_report_type = 'Fatal Crash' AND s.severity_ordinal = 5
                        THEN 'REPORT_TYPE_AND_PARTY_MAX'
                    WHEN m.acrs_report_type = 'Fatal Crash' THEN 'REPORT_TYPE_ONLY'
                    WHEN s.severity_ordinal = 5 THEN 'PARTY_MAX_ONLY'
               END AS fatal_signal
        FROM {views['crash']} s
        JOIN {views['montgomery']} m
          ON m.report_number = s.source_record_id AND m.is_current
        WHERE s.source_system = 'MONTGOMERY_MD'
        """)
    if "txdot" in present:
        locals_.append(f"""
        SELECT s.crash_uid, s.source_system, s.jurisdiction,
               t.county_fips,
               s.crash_date, s.crash_datetime_local, s.latitude, s.longitude,
               s.geo_quality = 'OK' AS geo_ok,
               (coalesce(t.crash_fatal_fl, false) OR s.severity_ordinal = 5
                    OR coalesce(t.death_cnt, 0) > 0) AS is_fatal_candidate,
               CASE WHEN coalesce(t.crash_fatal_fl, false) AND s.severity_ordinal = 5
                        THEN 'FATAL_FLAG_AND_SEVERITY'
                    WHEN coalesce(t.crash_fatal_fl, false) THEN 'FATAL_FLAG_ONLY'
                    WHEN s.severity_ordinal = 5 THEN 'SEVERITY_ONLY'
                    WHEN coalesce(t.death_cnt, 0) > 0 THEN 'DEATH_COUNT_ONLY'
               END AS fatal_signal
        FROM {views['crash']} s
        JOIN {views['txdot']} t
          ON t.crash_id = s.source_record_id AND t.is_current
        WHERE s.source_system = 'TXDOT_CRIS'
        """)
    if not locals_:
        locals_.append("""
        SELECT NULL::VARCHAR crash_uid, NULL::VARCHAR source_system,
               NULL::VARCHAR jurisdiction, NULL::VARCHAR county_fips,
               NULL::DATE crash_date, NULL::TIMESTAMP crash_datetime_local,
               NULL::DOUBLE latitude, NULL::DOUBLE longitude, NULL::BOOLEAN geo_ok,
               NULL::BOOLEAN is_fatal_candidate, NULL::VARCHAR fatal_signal
        WHERE false""")
    return fars, "\nUNION ALL BY NAME\n".join(f"({q})" for q in locals_)


# ---------------------------------------------------------------------------
# run
# ---------------------------------------------------------------------------


def run(con: duckdb.DuckDBPyConnection, *, views: dict[str, str] | None = None,
        present: set[str] | None = None, cfg: dict[str, Any] | None = None) -> dict[str, Any]:
    """Build `er_fars`, `er_local`, `er_pairs`, `er_matches`, `er_unmatched_fars`,
    `er_unmatched_local` on `con` and return the census as a dict."""
    views = {**DEFAULT_VIEWS, **(views or {})}
    present = present if present is not None else {"montgomery", "txdot", "fars"}
    th = thresholds(cfg)
    register_udfs(con)
    t0 = time.perf_counter()

    def lap(step: str) -> None:
        nonlocal t0
        log.debug("resolve: %-22s %6.2fs", step, time.perf_counter() - t0)
        t0 = time.perf_counter()

    fars_sql, local_sql = _sides_sql(views, present)
    con.execute(f"CREATE OR REPLACE TABLE er_fars AS {fars_sql}")
    con.execute(f"CREATE OR REPLACE TABLE er_local AS {local_sql}")
    lap("sides")

    # Every blocked pair, scored. `dd` is signed days (local minus FARS) so the
    # midnight-straddle direction is visible in the report.
    con.execute(f"""
        CREATE OR REPLACE TABLE er_pairs AS
        WITH b AS (
            SELECT f.crash_uid AS fars_uid, l.crash_uid AS local_uid, l.source_system,
                   f.jurisdiction, f.county_fips,
                   date_diff('day', f.crash_date, l.crash_date) AS dd,
                   CASE WHEN f.crash_datetime_local IS NOT NULL
                         AND l.crash_datetime_local IS NOT NULL
                        THEN abs(date_diff('minute', f.crash_datetime_local,
                                           l.crash_datetime_local)) END AS dt_min,
                   f.latitude AS flat, f.longitude AS flon, l.latitude AS llat, l.longitude AS llon,
                   f.geo_ok AS fars_geo_ok, l.geo_ok AS local_geo_ok,
                   -- Equirectangular approximation, metres, EPSG:4326 degrees in.
                   -- Only a pre-filter: see dist_m below.
                   111320.0 * sqrt(pow(l.latitude - f.latitude, 2)
                                 + pow((l.longitude - f.longitude) * cos(radians(f.latitude)), 2))
                       AS approx_m,
                   l.is_fatal_candidate, l.fatal_signal
            FROM er_fars f
            JOIN er_local l
              ON l.jurisdiction = f.jurisdiction
             AND l.county_fips = f.county_fips
             AND abs(date_diff('day', f.crash_date, l.crash_date)) <= {th['max_date_delta_days']}
        ),
        p AS (
            SELECT fars_uid, local_uid, source_system, jurisdiction, county_fips, dd, dt_min,
                   -- CRS: both sides EPSG:4326. Exact distance on the WGS84
                   -- ellipsoid (pyproj.Geod) wherever a match is possible; for
                   -- pairs more than 5 km apart by the approximation (error
                   -- < 0.5% at this scale) the approximate number is kept,
                   -- because no tier admits it and the Python UDF is the only
                   -- slow step in the build.
                   CASE WHEN fars_geo_ok AND local_geo_ok THEN
                     CASE WHEN approx_m <= 5000 THEN geodesic_m(flat, flon, llat, llon)
                          ELSE approx_m END
                   END AS dist_m,
                   fars_geo_ok, local_geo_ok, is_fatal_candidate, fatal_signal
            FROM b
        )
        SELECT *,
               CASE
                 WHEN dist_m IS NOT NULL AND dt_min IS NOT NULL
                      AND dist_m <= {th['tier_a_max_distance_m']}
                      AND dt_min <= {th['tier_a_max_time_min']} THEN 'A'
                 WHEN dist_m IS NOT NULL AND dt_min IS NOT NULL
                      AND dist_m <= {th['tier_b_max_distance_m']}
                      AND dt_min <= {th['tier_b_max_time_min']} THEN 'B'
                 WHEN dist_m IS NULL AND dt_min IS NOT NULL
                      AND dt_min <= {th['tier_c_max_time_min']} THEN 'C'
               END AS tier
        FROM p
    """)
    lap("pairs")

    # Candidates admitted to assignment. The fatal block is applied here, not
    # in er_pairs, so the non-fatal pairs stay measurable.
    fatal_pred = "TRUE" if th["admit_non_fatal_candidates"] else "is_fatal_candidate"
    con.execute(f"""
        CREATE OR REPLACE TABLE er_open AS
        SELECT fars_uid, local_uid, tier, dist_m, dt_min
        FROM er_pairs WHERE tier IS NOT NULL AND {fatal_pred}
    """)
    con.execute("""
        CREATE OR REPLACE TABLE er_matches (
            fars_uid VARCHAR, local_uid VARCHAR, tier VARCHAR,
            dist_m DOUBLE, dt_min BIGINT, pass_no INTEGER)
    """)

    # Mutual-best assignment, repeated. The ORDER BY is a TOTAL order
    # (crash_uid last) so the ranking -- and therefore the bridge -- is a pure
    # function of the data.
    passes = 0
    for pass_no in range(1, MAX_PASSES + 1):
        n = con.execute(f"""
            INSERT INTO er_matches
            WITH bf AS (
                SELECT fars_uid, local_uid FROM (
                    SELECT *, row_number() OVER (PARTITION BY fars_uid
                        ORDER BY tier, coalesce(dist_m, 1e12), coalesce(dt_min, 1e12), local_uid) rn
                    FROM er_open) WHERE rn = 1),
            bl AS (
                SELECT fars_uid, local_uid FROM (
                    SELECT *, row_number() OVER (PARTITION BY local_uid
                        ORDER BY tier, coalesce(dist_m, 1e12), coalesce(dt_min, 1e12), fars_uid) rn
                    FROM er_open) WHERE rn = 1)
            SELECT o.fars_uid, o.local_uid, o.tier, o.dist_m, o.dt_min, {pass_no}
            FROM er_open o
            JOIN bf USING (fars_uid, local_uid)
            JOIN bl USING (fars_uid, local_uid)
        """).fetchone()
        accepted = con.execute(
            f"SELECT COUNT(*) FROM er_matches WHERE pass_no = {pass_no}").fetchone()[0]
        passes = pass_no
        if accepted == 0:
            break
        con.execute("""
            DELETE FROM er_open
            WHERE fars_uid IN (SELECT fars_uid FROM er_matches)
               OR local_uid IN (SELECT local_uid FROM er_matches)
        """)

    lap(f"assignment ({passes} passes)")

    # Why each unmatched record on either side is unmatched. One reason per
    # record, chosen as the MOST specific true statement.
    local_present = bool(present & {"montgomery", "txdot"})
    # FARS is published and reissued by calendar YEAR, so coverage is a year
    # range: a Montgomery crash in 2016 is not "unmatched", it predates the
    # loaded FARS years.
    fars_years = con.execute(
        "SELECT MIN(year(crash_date)), MAX(year(crash_date)) FROM er_fars").fetchone()
    con.execute(f"""
        CREATE OR REPLACE TABLE er_unmatched_fars AS
        SELECT f.crash_uid AS fars_uid, f.jurisdiction, year(f.crash_date) AS year,
               CASE
                 WHEN NOT EXISTS (SELECT 1 FROM er_local l WHERE l.jurisdiction = f.jurisdiction)
                      THEN 'OUTSIDE_LOCAL_COVERAGE'
                 WHEN NOT EXISTS (SELECT 1 FROM er_local l WHERE l.jurisdiction = f.jurisdiction
                                  AND l.county_fips = f.county_fips)
                      THEN 'NO_LOCAL_ROWS_IN_COUNTY'
                 WHEN NOT EXISTS (SELECT 1 FROM er_pairs p WHERE p.fars_uid = f.crash_uid)
                      THEN 'NO_CANDIDATE_ON_DATE'
                 WHEN NOT EXISTS (SELECT 1 FROM er_pairs p WHERE p.fars_uid = f.crash_uid
                                  AND ({fatal_pred}))
                      THEN 'NO_FATAL_CANDIDATE_ON_DATE'
                 WHEN EXISTS (SELECT 1 FROM er_pairs p WHERE p.fars_uid = f.crash_uid
                              AND ({fatal_pred}) AND p.tier IS NOT NULL)
                      THEN 'CANDIDATE_TAKEN'
                 WHEN NOT EXISTS (SELECT 1 FROM er_pairs p WHERE p.fars_uid = f.crash_uid
                                  AND ({fatal_pred}) AND p.dt_min IS NOT NULL)
                      AND NOT f.geo_ok
                      THEN 'NO_TIME_FOR_TIER_C'
                 ELSE 'CANDIDATE_TOO_FAR'
               END AS reason,
               (SELECT MIN(dist_m) FROM er_pairs p WHERE p.fars_uid = f.crash_uid
                AND ({fatal_pred})) AS nearest_fatal_m,
               (SELECT MIN(dist_m) FROM er_pairs p WHERE p.fars_uid = f.crash_uid
                AND NOT p.is_fatal_candidate) AS nearest_nonfatal_m
        FROM er_fars f
        WHERE NOT EXISTS (SELECT 1 FROM er_matches m WHERE m.fars_uid = f.crash_uid)
    """)
    con.execute(f"""
        CREATE OR REPLACE TABLE er_unmatched_local AS
        SELECT l.crash_uid AS local_uid, l.source_system, l.jurisdiction,
               year(l.crash_date) AS year, l.fatal_signal,
               CASE
                 WHEN {('FALSE' if fars_years[0] is None else
                        f"year(l.crash_date) NOT BETWEEN {fars_years[0]} AND {fars_years[1]}")}
                      THEN 'OUTSIDE_FARS_COVERAGE'
                 WHEN NOT EXISTS (SELECT 1 FROM er_pairs p WHERE p.local_uid = l.crash_uid)
                      THEN 'NO_FARS_ON_DATE'
                 WHEN EXISTS (SELECT 1 FROM er_pairs p WHERE p.local_uid = l.crash_uid
                              AND p.tier IS NOT NULL)
                      THEN 'FARS_TAKEN'
                 WHEN NOT l.geo_ok AND NOT EXISTS (SELECT 1 FROM er_pairs p
                              WHERE p.local_uid = l.crash_uid AND p.dt_min IS NOT NULL)
                      THEN 'NO_TIME_FOR_TIER_C'
                 ELSE 'FARS_TOO_FAR'
               END AS reason,
               (SELECT MIN(dist_m) FROM er_pairs p WHERE p.local_uid = l.crash_uid) AS nearest_fars_m
        FROM er_local l
        WHERE l.is_fatal_candidate
          AND NOT EXISTS (SELECT 1 FROM er_matches m WHERE m.local_uid = l.crash_uid)
    """)

    lap("unmatched reasons")
    out = census(con, th, passes=passes, fars_years=fars_years, local_present=local_present)
    lap("census")
    return out


# ---------------------------------------------------------------------------
# census
# ---------------------------------------------------------------------------


def _rows(con, sql: str) -> list[dict[str, Any]]:
    cur = con.execute(sql)
    cols = [d[0] for d in cur.description]
    return [dict(zip(cols, r)) for r in cur.fetchall()]


def census(con, th: dict[str, Any], *, passes: int, fars_years, local_present: bool) -> dict[str, Any]:
    out: dict[str, Any] = {"thresholds": th, "assignment_passes": passes,
                           "fars_coverage_years": [fars_years[0], fars_years[1]]}
    out["totals"] = _rows(con, """
        SELECT (SELECT COUNT(*) FROM er_fars) AS fars_in_scope,
               (SELECT COUNT(*) FROM er_local) AS local_crashes,
               (SELECT COUNT(*) FROM er_local WHERE is_fatal_candidate) AS local_fatal_candidates,
               (SELECT COUNT(*) FROM er_pairs) AS blocked_pairs,
               (SELECT COUNT(*) FROM er_matches) AS matched,
               (SELECT COUNT(*) FROM er_unmatched_fars) AS unmatched_fars,
               (SELECT COUNT(*) FROM er_unmatched_local) AS unmatched_local_fatal
    """)[0]
    out["by_jurisdiction_year"] = _rows(con, """
        WITH f AS (SELECT f.jurisdiction, year(f.crash_date) y, COUNT(*) n,
                          COUNT(*) FILTER (WHERE EXISTS (SELECT 1 FROM er_local l
                              WHERE l.jurisdiction = f.jurisdiction
                                AND l.county_fips = f.county_fips)) n_covered
                   FROM er_fars f GROUP BY 1,2),
             l AS (SELECT jurisdiction, year(crash_date) y,
                          COUNT(*) FILTER (WHERE is_fatal_candidate) n FROM er_local GROUP BY 1,2),
             m AS (SELECT p.jurisdiction, year(f.crash_date) y,
                          COUNT(*) FILTER (WHERE m.tier='A') a,
                          COUNT(*) FILTER (WHERE m.tier='B') b,
                          COUNT(*) FILTER (WHERE m.tier='C') c
                   FROM er_matches m JOIN er_pairs p USING (fars_uid, local_uid)
                   JOIN er_fars f ON f.crash_uid = m.fars_uid GROUP BY 1,2)
        SELECT coalesce(f.jurisdiction, l.jurisdiction) AS jurisdiction,
               coalesce(f.y, l.y) AS year,
               coalesce(f.n, 0) AS fars, coalesce(f.n_covered, 0) AS fars_in_covered_counties,
               coalesce(l.n, 0) AS local_fatal,
               coalesce(m.a, 0) AS tier_a, coalesce(m.b, 0) AS tier_b, coalesce(m.c, 0) AS tier_c,
               coalesce(m.a, 0) + coalesce(m.b, 0) + coalesce(m.c, 0) AS matched
        FROM f FULL OUTER JOIN l ON l.jurisdiction = f.jurisdiction AND l.y = f.y
        LEFT JOIN m ON m.jurisdiction = coalesce(f.jurisdiction, l.jurisdiction)
                   AND m.y = coalesce(f.y, l.y)
        ORDER BY 1, 2
    """)
    out["unmatched_fars_by_reason"] = _rows(con, """
        SELECT jurisdiction, reason, COUNT(*) n FROM er_unmatched_fars
        GROUP BY 1,2 ORDER BY 1,3 DESC""")
    out["unmatched_local_by_reason"] = _rows(con, """
        SELECT source_system, reason, COUNT(*) n FROM er_unmatched_local
        GROUP BY 1,2 ORDER BY 1,3 DESC""")
    out["matched_by_fatal_signal"] = _rows(con, """
        SELECT p.source_system, p.fatal_signal, COUNT(*) n
        FROM er_matches m JOIN er_pairs p USING (fars_uid, local_uid)
        GROUP BY 1,2 ORDER BY 1,3 DESC""")
    out["match_distance_distribution"] = _rows(con, """
        SELECT tier,
               COUNT(*) n,
               round(quantile_cont(dist_m, 0.5), 1) AS dist_p50,
               round(quantile_cont(dist_m, 0.9), 1) AS dist_p90,
               round(quantile_cont(dist_m, 0.95), 1) AS dist_p95,
               round(MAX(dist_m), 1) AS dist_max,
               quantile_cont(dt_min, 0.5) AS dt_p50,
               quantile_cont(dt_min, 0.95) AS dt_p95,
               MAX(dt_min) AS dt_max
        FROM er_matches GROUP BY 1 ORDER BY 1""")
    out["midnight_straddle_matches"] = con.execute("""
        SELECT COUNT(*) FROM er_matches m JOIN er_pairs p USING (fars_uid, local_uid)
        WHERE p.dd <> 0""").fetchone()[0]
    # The 30-day-death probe: FARS records whose NEAREST tier-A-quality
    # neighbour is a crash the local source did not record as fatal.
    out["nonfatal_tier_a_pairs"] = _rows(con, f"""
        SELECT source_system, COUNT(DISTINCT fars_uid) AS fars_records,
               COUNT(*) AS pairs
        FROM er_pairs
        WHERE NOT is_fatal_candidate AND tier = 'A'
        GROUP BY 1 ORDER BY 1""")
    out["nonfatal_tier_a_unmatched_fars"] = con.execute("""
        SELECT COUNT(DISTINCT p.fars_uid) FROM er_pairs p
        JOIN er_unmatched_fars u ON u.fars_uid = p.fars_uid
        WHERE NOT p.is_fatal_candidate AND p.tier = 'A'""").fetchone()[0]
    out["tier_b_pairs"] = _rows(con, """
        SELECT m.fars_uid, m.local_uid, round(m.dist_m, 1) dist_m, m.dt_min
        FROM er_matches m WHERE tier = 'B' ORDER BY 1""")
    out["disjointness"] = disjointness(con)
    return out


def disjointness(con) -> dict[str, Any]:
    """Montgomery ∩ TxDOT: provably empty because they share no jurisdiction.

    Not a proof by assertion -- a proof by the column. If a Montgomery row ever
    carried jurisdiction 'TX' this returns a non-zero overlap and the test fails.
    """
    # Aggregate FIRST: a row-level self-join on jurisdiction is 125k x 125k
    # inside Maryland alone (measured: 74 s for a question whose answer is a
    # set intersection of three two-letter codes).
    rows = con.execute("""
        WITH sj AS (SELECT DISTINCT source_system, jurisdiction FROM er_local)
        SELECT a.source_system, b.source_system, a.jurisdiction
        FROM sj a JOIN sj b
          ON a.jurisdiction = b.jurisdiction AND a.source_system < b.source_system
        ORDER BY 1, 2, 3
    """).fetchall()
    juris = dict(con.execute("""
        SELECT source_system, list_sort(list(DISTINCT jurisdiction))
        FROM er_local GROUP BY 1""").fetchall())
    return {"local_source_pairs_sharing_a_jurisdiction": [list(r) for r in rows],
            "jurisdictions_by_source": {k: list(v) for k, v in juris.items()}}


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------


def _print_report(stats: dict[str, Any]) -> None:
    t = stats["totals"]
    print("Entity resolution -- FARS ∩ {Montgomery, TxDOT}")
    print(f"  thresholds: {stats['thresholds']}")
    print(f"  FARS in scope {t['fars_in_scope']}, local crashes {t['local_crashes']} "
          f"({t['local_fatal_candidates']} fatal candidates), blocked pairs {t['blocked_pairs']}")
    print(f"  matched {t['matched']} in {stats['assignment_passes']} pass(es); "
          f"unmatched FARS {t['unmatched_fars']}, unmatched local fatal {t['unmatched_local_fatal']}")
    print(f"  midnight-straddle matches: {stats['midnight_straddle_matches']}")
    print("\n  jurisdiction  year   FARS  FARS_in_covered_counties  local_fatal  A    B    C   matched")
    for r in stats["by_jurisdiction_year"]:
        print(f"  {r['jurisdiction']:<12} {r['year']:>5} {r['fars']:>6} {r['fars_in_covered_counties']:>25} "
              f"{r['local_fatal']:>12} "
              f"{r['tier_a']:>4} {r['tier_b']:>4} {r['tier_c']:>4} {r['matched']:>9}")
    print("\n  unmatched FARS by reason")
    for r in stats["unmatched_fars_by_reason"]:
        print(f"    {r['jurisdiction']}  {r['reason']:<28} {r['n']}")
    print("  unmatched local fatal by reason")
    for r in stats["unmatched_local_by_reason"]:
        print(f"    {r['source_system']:<14} {r['reason']:<24} {r['n']}")
    print("  matched, by which local fatal signal fired")
    for r in stats["matched_by_fatal_signal"]:
        print(f"    {r['source_system']:<14} {r['fatal_signal']:<28} {r['n']}")
    print("  match distance / time distribution by tier")
    for r in stats["match_distance_distribution"]:
        print(f"    {r}")
    print(f"  non-fatal local crash within tier-A thresholds of a FARS fatality "
          f"(30-day-death probe): {stats['nonfatal_tier_a_pairs']}; "
          f"of which the FARS record is otherwise unmatched: "
          f"{stats['nonfatal_tier_a_unmatched_fars']}")
    print(f"  tier-B pairs ({len(stats['tier_b_pairs'])}):")
    for r in stats["tier_b_pairs"][:50]:
        print(f"    {r}")
    print(f"  disjointness: {stats['disjointness']}")


def _cli(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(prog="python -m src.transform.resolve")
    ap.add_argument("--report", action="store_true", help="print the match census")
    ap.add_argument("--silver-root", type=Path, default=None)
    ap.add_argument("--json", action="store_true")
    ap.add_argument("-v", "--verbose", action="store_true")
    args = ap.parse_args(argv)
    logging.basicConfig(level=logging.DEBUG if args.verbose else logging.INFO,
                        format="%(levelname)s %(name)s %(message)s")

    from . import model  # local import: model imports resolve
    con = c.connect()
    present = model.register_silver(con, c.silver_root(args.silver_root))
    model.scope_crashes(con)
    stats = run(con, present=present)
    if args.json:
        print(json.dumps(stats, indent=2, default=str))
    else:
        _print_report(stats)
    return 0


if __name__ == "__main__":
    sys.exit(_cli())
