"""Entity resolution: unit cases on tiny relations, determinism, and the real
FARS ∩ Montgomery pairs in the committed extract.

The unit cases build the exact relations `resolve.run()` reads -- `scoped_crash`,
`moco_crash`, `txd_crash`, `fars_accident` with only the columns it touches --
so the SQL under test is the production SQL, not a re-implementation.
"""

from __future__ import annotations

import json
import random

import duckdb
import pytest

from src.transform import common as c
from src.transform import resolve

CFG = {"resolution": {
    "max_date_delta_days": 1,
    "tier_a_max_distance_m": 250, "tier_a_max_time_min": 30,
    "tier_b_max_distance_m": 1000, "tier_b_max_time_min": 120,
    "tier_c_max_time_min": 30,
    "admit_non_fatal_candidates": False,
}}

# One degree of latitude ~ 111,320 m; 0.0009 deg ~ 100 m.
LAT, LON = 39.05, -77.10


class World:
    """Tiny in-memory silver with a builder API."""

    def __init__(self):
        self.con = duckdb.connect()
        self.rows: list[tuple] = []
        self.moco: list[tuple] = []
        self.txd: list[tuple] = []
        self.fars: list[tuple] = []

    def fars_crash(self, uid, *, date, time, lat=LAT, lon=LON, juris="MD", county="031",
                   geo="OK"):
        year, st_case = uid.split("-")
        self.rows.append((f"NHTSA_FARS:{uid}", "NHTSA_FARS", uid, juris, date, time,
                          lat, lon, geo, 5))
        self.fars.append((year, st_case, county))

    def moco_crash(self, rn, *, date, time, lat=LAT, lon=LON, fatal=True, ordinal=None, geo="OK"):
        ordinal = ordinal if ordinal is not None else (5 if fatal else 3)
        self.rows.append((f"MONTGOMERY_MD:{rn}", "MONTGOMERY_MD", rn, "MD", date, time,
                          lat, lon, geo, ordinal))
        self.moco.append((rn, "Fatal Crash" if fatal else "Injury Crash"))

    def txd_crash(self, cid, *, date, time, lat=29.7, lon=-95.4, county="201", fatal=True, geo="OK"):
        self.rows.append((f"TXDOT_CRIS:{cid}", "TXDOT_CRIS", cid, "TX", date, time,
                          lat, lon, geo, 5 if fatal else 2))
        self.txd.append((cid, county, fatal, 1 if fatal else 0))

    def run(self, *, shuffle_seed=None, cfg=CFG):
        rows = list(self.rows)
        if shuffle_seed is not None:
            random.Random(shuffle_seed).shuffle(rows)
        con = self.con
        con.execute("""CREATE OR REPLACE TABLE scoped_crash (
            crash_uid VARCHAR, source_system VARCHAR, source_record_id VARCHAR, jurisdiction VARCHAR,
            crash_date DATE, crash_datetime_local TIMESTAMP, latitude DOUBLE, longitude DOUBLE,
            geo_quality VARCHAR, severity_ordinal INTEGER)""")
        if rows:
            con.executemany("INSERT INTO scoped_crash VALUES (?,?,?,?,?,?,?,?,?,?)", rows)
        con.execute("CREATE OR REPLACE TABLE moco_crash (report_number VARCHAR, acrs_report_type VARCHAR, "
                    "is_current BOOLEAN)")
        if self.moco:
            con.executemany("INSERT INTO moco_crash VALUES (?,?,true)", self.moco)
        con.execute("CREATE OR REPLACE TABLE txd_crash (crash_id VARCHAR, county_fips VARCHAR, "
                    "crash_fatal_fl BOOLEAN, death_cnt INTEGER, is_current BOOLEAN)")
        if self.txd:
            con.executemany("INSERT INTO txd_crash VALUES (?,?,?,?,true)", self.txd)
        con.execute("CREATE OR REPLACE TABLE fars_accident (year VARCHAR, st_case VARCHAR, "
                    "county_fips VARCHAR, is_current BOOLEAN)")
        if self.fars:
            con.executemany("INSERT INTO fars_accident VALUES (?,?,?,true)", self.fars)
        return resolve.run(con, present={"montgomery", "txdot", "fars"}, cfg=cfg)

    def matches(self):
        return sorted(self.con.execute(
            "SELECT fars_uid, local_uid, tier FROM er_matches").fetchall())

    def unmatched_fars(self):
        return dict(self.con.execute("SELECT fars_uid, reason FROM er_unmatched_fars").fetchall())

    def unmatched_local(self):
        return dict(self.con.execute("SELECT local_uid, reason FROM er_unmatched_local").fetchall())


# ===========================================================================
# unit cases
# ===========================================================================


def test_close_in_space_and_time_is_a_tier_a_match():
    w = World()
    w.fars_crash("2019-240001", date="2019-03-01", time="2019-03-01 14:00")
    w.moco_crash("MCP1", date="2019-03-01", time="2019-03-01 14:10", lat=LAT + 0.0018)   # ~200 m
    w.run()
    assert w.matches() == [("NHTSA_FARS:2019-240001", "MONTGOMERY_MD:MCP1", "A")]
    dist = w.con.execute("SELECT dist_m FROM er_matches").fetchone()[0]
    assert 150 < dist < 250   # geodesic, WGS84 -- not a degree-based approximation


def test_five_kilometres_away_is_not_a_match_and_says_why():
    w = World()
    w.fars_crash("2019-240001", date="2019-03-01", time="2019-03-01 14:00")
    w.moco_crash("MCP1", date="2019-03-01", time="2019-03-01 14:00", lat=LAT + 0.045)   # ~5 km
    w.run()
    assert w.matches() == []
    assert w.unmatched_fars() == {"NHTSA_FARS:2019-240001": "CANDIDATE_TOO_FAR"}
    assert w.unmatched_local() == {"MONTGOMERY_MD:MCP1": "FARS_TOO_FAR"}


def test_two_candidates_the_closer_one_wins_and_the_other_is_reported_taken():
    w = World()
    w.fars_crash("2019-240001", date="2019-03-01", time="2019-03-01 14:00")
    w.moco_crash("NEAR", date="2019-03-01", time="2019-03-01 14:00", lat=LAT + 0.0005)   # ~55 m
    w.moco_crash("FAR", date="2019-03-01", time="2019-03-01 14:00", lat=LAT + 0.0015)    # ~165 m
    w.run()
    assert w.matches() == [("NHTSA_FARS:2019-240001", "MONTGOMERY_MD:NEAR", "A")]
    assert w.unmatched_local() == {"MONTGOMERY_MD:FAR": "FARS_TAKEN"}


def test_two_fars_records_one_local_crash_reports_candidate_taken():
    w = World()
    w.fars_crash("2019-240001", date="2019-03-01", time="2019-03-01 14:00")
    w.fars_crash("2019-240002", date="2019-03-01", time="2019-03-01 14:05", lat=LAT + 0.0003)
    w.moco_crash("ONLY", date="2019-03-01", time="2019-03-01 14:00")
    w.run()
    assert w.matches() == [("NHTSA_FARS:2019-240001", "MONTGOMERY_MD:ONLY", "A")]
    assert w.unmatched_fars() == {"NHTSA_FARS:2019-240002": "CANDIDATE_TAKEN"}


def test_sentinel_coordinates_fall_back_to_county_date_time_tier_c():
    w = World()
    w.fars_crash("2019-240001", date="2019-03-01", time="2019-03-01 14:00", lat=None, lon=None,
                 geo="SENTINEL")
    w.moco_crash("MCP1", date="2019-03-01", time="2019-03-01 14:20")
    w.moco_crash("MCP2", date="2019-03-01", time="2019-03-01 20:00")   # same day, wrong time
    w.run()
    assert w.matches() == [("NHTSA_FARS:2019-240001", "MONTGOMERY_MD:MCP1", "C")]
    assert w.unmatched_local() == {"MONTGOMERY_MD:MCP2": "FARS_TOO_FAR"}


def test_no_coordinates_and_no_time_cannot_match():
    w = World()
    w.fars_crash("2019-240001", date="2019-03-01", time=None, lat=None, lon=None, geo="SENTINEL")
    w.moco_crash("MCP1", date="2019-03-01", time="2019-03-01 14:20")
    w.run()
    assert w.matches() == []
    assert w.unmatched_fars() == {"NHTSA_FARS:2019-240001": "NO_TIME_FOR_TIER_C"}


def test_midnight_straddle_matches_across_the_date_boundary():
    w = World()
    w.fars_crash("2019-240001", date="2019-03-01", time="2019-03-01 23:50")
    w.moco_crash("MCP1", date="2019-03-02", time="2019-03-02 00:10")
    stats = w.run()
    assert w.matches() == [("NHTSA_FARS:2019-240001", "MONTGOMERY_MD:MCP1", "A")]
    assert stats["midnight_straddle_matches"] == 1


def test_second_day_apart_is_outside_the_block():
    w = World()
    w.fars_crash("2019-240001", date="2019-03-01", time="2019-03-01 12:00")
    w.moco_crash("MCP1", date="2019-03-03", time="2019-03-03 12:00")
    w.run()
    assert w.matches() == []
    assert w.unmatched_fars() == {"NHTSA_FARS:2019-240001": "NO_CANDIDATE_ON_DATE"}
    assert w.unmatched_local() == {"MONTGOMERY_MD:MCP1": "NO_FARS_ON_DATE"}


def test_fatal_block_reads_both_montgomery_signals():
    """Report type says Fatal Crash but the party max is 4: still a candidate."""
    w = World()
    w.fars_crash("2019-240001", date="2019-03-01", time="2019-03-01 14:00")
    w.moco_crash("MCP1", date="2019-03-01", time="2019-03-01 14:00", fatal=True, ordinal=4)
    w.run()
    assert w.matches() == [("NHTSA_FARS:2019-240001", "MONTGOMERY_MD:MCP1", "A")]
    signal = w.con.execute("SELECT fatal_signal FROM er_pairs").fetchone()[0]
    assert signal == "REPORT_TYPE_ONLY"


def test_non_fatal_neighbour_is_reported_not_admitted_unless_configured():
    w = World()
    w.fars_crash("2019-240001", date="2019-03-01", time="2019-03-01 14:00")
    w.moco_crash("INJ", date="2019-03-01", time="2019-03-01 14:00", fatal=False)   # died later?
    stats = w.run()
    assert w.matches() == []
    assert w.unmatched_fars() == {"NHTSA_FARS:2019-240001": "NO_FATAL_CANDIDATE_ON_DATE"}
    assert stats["nonfatal_tier_a_pairs"] == [
        {"source_system": "MONTGOMERY_MD", "fars_records": 1, "pairs": 1}]
    assert stats["nonfatal_tier_a_unmatched_fars"] == 1

    admit = {"resolution": {**CFG["resolution"], "admit_non_fatal_candidates": True}}
    w.run(cfg=admit)
    assert w.matches() == [("NHTSA_FARS:2019-240001", "MONTGOMERY_MD:INJ", "A")]


def test_txdot_side_blocks_on_county_fips():
    w = World()
    w.fars_crash("2020-480001", date="2020-06-01", time="2020-06-01 09:00", lat=29.7, lon=-95.4,
                 juris="TX", county="201")
    w.txd_crash("SAME_COUNTY", date="2020-06-01", time="2020-06-01 09:00", county="201")
    w.txd_crash("OTHER_COUNTY", date="2020-06-01", time="2020-06-01 09:00", county="113")
    w.run()
    assert w.matches() == [("NHTSA_FARS:2020-480001", "TXDOT_CRIS:SAME_COUNTY", "A")]
    assert w.unmatched_local() == {"TXDOT_CRIS:OTHER_COUNTY": "NO_FARS_ON_DATE"}


def test_montgomery_and_txdot_never_share_a_jurisdiction():
    w = World()
    w.moco_crash("MCP1", date="2019-03-01", time="2019-03-01 14:00")
    w.txd_crash("T1", date="2019-03-01", time="2019-03-01 14:00")
    w.fars_crash("2019-240001", date="2019-03-01", time="2019-03-01 14:00")
    stats = w.run()
    assert stats["disjointness"]["local_source_pairs_sharing_a_jurisdiction"] == []
    assert stats["disjointness"]["jurisdictions_by_source"] == {
        "MONTGOMERY_MD": ["MD"], "TXDOT_CRIS": ["TX"]}


def test_local_crash_outside_fars_years_is_labelled_not_unmatched_by_distance():
    w = World()
    w.fars_crash("2019-240001", date="2019-03-01", time="2019-03-01 14:00")
    w.moco_crash("MCP1", date="2019-03-01", time="2019-03-01 14:00")
    w.moco_crash("OLD", date="2016-05-05", time="2016-05-05 14:00")
    w.run()
    assert w.unmatched_local() == {"MONTGOMERY_MD:OLD": "OUTSIDE_FARS_COVERAGE"}


# ===========================================================================
# determinism
# ===========================================================================


def test_assignment_is_deterministic_and_independent_of_row_order():
    def world():
        w = World()
        rng = random.Random(7)
        for i in range(40):
            d = f"2019-03-{1 + i % 9:02d}"
            lat = LAT + rng.uniform(-0.05, 0.05)
            lon = LON + rng.uniform(-0.05, 0.05)
            w.fars_crash(f"2019-24{i:04d}", date=d, time=f"{d} 1{i % 10}:00", lat=lat, lon=lon)
            # a real twin, a decoy 400 m off, and a tie-ish twin at the same spot
            w.moco_crash(f"R{i}", date=d, time=f"{d} 1{i % 10}:05", lat=lat + 0.0002, lon=lon)
            w.moco_crash(f"D{i}", date=d, time=f"{d} 1{i % 10}:05", lat=lat + 0.0036, lon=lon)
            if i % 5 == 0:
                w.moco_crash(f"T{i}", date=d, time=f"{d} 1{i % 10}:05", lat=lat + 0.0002, lon=lon)
        return w

    w1, w2, w3 = world(), world(), world()
    w1.run(); w2.run(shuffle_seed=1); w3.run(shuffle_seed=99)
    assert w1.matches() == w2.matches() == w3.matches()
    assert len(w1.matches()) == 40
    # Exact ties (same distance, same time) are broken on crash_uid, so the
    # lexically smaller report number wins every time -- R before T.
    tied = [m for m in w1.matches() if m[1].endswith(("R0", "R5", "R10", "R15", "R20", "R25", "R30", "R35"))]
    assert len(tied) == 8


# ===========================================================================
# the real pairs in the extract
# ===========================================================================


def test_known_fars_montgomery_pairs_resolve_under_the_montgomery_key(gold_con, fixture_manifest):
    """Re-derive the expected 2019 pairs independently of resolve.py (same day
    or adjacent, geodesic <= 250 m, |dt| <= 30 min, Montgomery fatal-typed) and
    check the bridge agrees exactly."""
    from tests.conftest import using_full_bronze
    if using_full_bronze():
        pytest.skip("the extract's known_pairs describe the fixture, not the corpus")
    kp = fixture_manifest.get("known_pairs")
    assert kp, "regenerate the extract: MANIFEST.json has no known_pairs"

    fars = gold_con.execute("""
        SELECT a.st_case, a.crash_date, a.crash_datetime_local, a.latitude, a.longitude
        FROM silver_fars_accident_current a WHERE a.year='2019' AND a.state_fips='24' AND a.county_fips='031'
    """).fetchall()
    moco = gold_con.execute("""
        SELECT report_number, crash_date, crash_datetime_local, latitude, longitude
        FROM silver_montgomery_crash_current
        WHERE (acrs_report_type='Fatal Crash' OR severity_ordinal=5) AND year(crash_date)=2019
    """).fetchall()
    assert {r[0] for r in fars} == set(kp["fars_2019_md_county_031_st_cases"])
    assert set(kp["montgomery_fatal_2019_report_numbers"]) <= {r[0] for r in moco}

    expected = set()
    for st, fd, ft, flat, flon in fars:
        for rn, md, mt, mlat, mlon in moco:
            if abs((fd - md).days) > 1 or None in (ft, mt, flat, mlat):
                continue
            if abs((ft - mt).total_seconds()) <= 1800 and \
                    c.geodesic_distance_m(flat, flon, mlat, mlon) <= 250:
                expected.add((f"NHTSA_FARS:2019-{st}", f"MONTGOMERY_MD:{rn}"))
    assert len(expected) >= 20, f"only {len(expected)} independently derivable pairs in the extract"

    got = set(gold_con.execute("""
        SELECT b.crash_uid, f.primary_crash_uid
        FROM gold_bridge_crash_source b JOIN gold_fact_crash f USING (crash_sk)
        WHERE NOT b.is_primary AND b.crash_uid LIKE 'NHTSA_FARS:2019-24%'
          AND f.primary_source_system = 'MONTGOMERY_MD'""").fetchall())
    # The independent derivation is many-to-many; the bridge is one-to-one. So
    # every bridge pair must be in the expected set, and every expected FARS
    # record with exactly one candidate must be in the bridge.
    assert got <= expected, got - expected
    singles = {f for f in {e[0] for e in expected}
               if sum(1 for e in expected if e[0] == f) == 1}
    assert singles <= {g[0] for g in got}, singles - {g[0] for g in got}
    assert len(got) >= 20


def test_resolve_report_cli_runs(silver_root, capsys):
    resolve._cli(["--report", "--silver-root", str(silver_root)])
    out = capsys.readouterr().out
    assert "Entity resolution" in out and "unmatched FARS by reason" in out
    resolve._cli(["--report", "--silver-root", str(silver_root), "--json"])
    payload = json.loads(capsys.readouterr().out)
    assert "by_jurisdiction_year" in payload
