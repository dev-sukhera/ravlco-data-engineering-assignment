"""Unit tests for the pieces that are pure functions or single queries.

These need no bronze at all -- the grammar and the crosswalk are functions over
strings, the contract validator is a function over a DuckDB relation, and the
drift detector is a function over a list. Keeping them here means a change to
the parser fails in milliseconds rather than after a 25-second build.
"""

from __future__ import annotations

import duckdb
import pytest

from src import contracts
from src.transform import common as c
from src.transform import fars as fars_transform
from src.transform import txdot as txdot_transform
from src.transform.dictionaries import (
    NOT_APPLICABLE,
    NOT_SUSPECTED,
    SCHEME_NEW,
    SCHEME_NULL,
    SCHEME_OLD,
    SCHEME_UNMAPPED,
    SUSPECTED,
    UNKNOWN,
    ObservedValue,
    detect_drift,
    parse_substance,
    parse_substance_list,
    vocabulary_asof,
)
from src.transform.severity_crosswalk import (
    UnmappedSeverity,
    to_ordinal,
)


# ===========================================================================
# the substance grammar
# ===========================================================================

# Every distinct value observed in mmzv-x632.driver_substance_abuse on
# 2026-09-07, all 21 of them, with the mapping decided in dictionaries.py.
# (raw, scheme, alcohol, drug, detail)
OLD_GENERATION = [
    ("NONE DETECTED",              SCHEME_OLD, NOT_SUSPECTED,  NOT_SUSPECTED,  "NONE_DETECTED"),
    ("ALCOHOL PRESENT",            SCHEME_OLD, SUSPECTED,      NOT_SUSPECTED,  "ALCOHOL_PRESENT"),
    ("ALCOHOL CONTRIBUTED",        SCHEME_OLD, SUSPECTED,      NOT_SUSPECTED,  "ALCOHOL_CONTRIBUTED"),
    ("ILLEGAL DRUG PRESENT",       SCHEME_OLD, NOT_SUSPECTED,  SUSPECTED,      "ILLEGAL_DRUG_PRESENT"),
    ("ILLEGAL DRUG CONTRIBUTED",   SCHEME_OLD, NOT_SUSPECTED,  SUSPECTED,      "ILLEGAL_DRUG_CONTRIBUTED"),
    ("MEDICATION PRESENT",         SCHEME_OLD, NOT_SUSPECTED,  SUSPECTED,      "MEDICATION_PRESENT"),
    ("MEDICATION CONTRIBUTED",     SCHEME_OLD, NOT_SUSPECTED,  SUSPECTED,      "MEDICATION_CONTRIBUTED"),
    ("COMBINED SUBSTANCE PRESENT", SCHEME_OLD, SUSPECTED,      SUSPECTED,      "COMBINED_SUBSTANCE_PRESENT"),
    ("COMBINATION CONTRIBUTED",    SCHEME_OLD, SUSPECTED,      SUSPECTED,      "COMBINATION_CONTRIBUTED"),
    ("OTHER",                      SCHEME_OLD, UNKNOWN,        SUSPECTED,      "OTHER"),
    ("UNKNOWN",                    SCHEME_OLD, UNKNOWN,        UNKNOWN,        None),
    ("N/A",                        SCHEME_OLD, NOT_APPLICABLE, NOT_APPLICABLE, None),
]

NEW_GENERATION = [
    ("Not Suspect of Alcohol Use, Not Suspect of Drug Use", SCHEME_NEW, NOT_SUSPECTED, NOT_SUSPECTED),
    ("Suspect of Alcohol Use, Not Suspect of Drug Use",     SCHEME_NEW, SUSPECTED,     NOT_SUSPECTED),
    ("Not Suspect of Alcohol Use, Suspect of Drug Use",     SCHEME_NEW, NOT_SUSPECTED, SUSPECTED),
    ("Suspect of Alcohol Use, Suspect of Drug Use",         SCHEME_NEW, SUSPECTED,     SUSPECTED),
    ("Suspect of Alcohol Use, Unknown",                     SCHEME_NEW, SUSPECTED,     UNKNOWN),
    ("Not Suspect of Alcohol Use, Unknown",                 SCHEME_NEW, NOT_SUSPECTED, UNKNOWN),
    ("Unknown, Not Suspect of Drug Use",                    SCHEME_NEW, UNKNOWN,       NOT_SUSPECTED),
    ("Unknown, Suspect of Drug Use",                        SCHEME_NEW, UNKNOWN,       SUSPECTED),
    ("Unknown, Unknown",                                    SCHEME_NEW, UNKNOWN,       UNKNOWN),
]


@pytest.mark.parametrize("raw,scheme,alcohol,drug,detail", OLD_GENERATION)
def test_old_generation_values_parse(raw, scheme, alcohol, drug, detail):
    s = parse_substance(raw)
    assert (s.scheme, s.alcohol_status, s.drug_status, s.substance_detail) == (
        scheme, alcohol, drug, detail
    )


@pytest.mark.parametrize("raw,scheme,alcohol,drug", NEW_GENERATION)
def test_new_generation_values_parse(raw, scheme, alcohol, drug):
    s = parse_substance(raw)
    assert (s.scheme, s.alcohol_status, s.drug_status) == (scheme, alcohol, drug)


def test_all_21_observed_values_are_covered():
    """The full observed vocabulary, so a mapping cannot be quietly dropped."""
    assert len(OLD_GENERATION) + len(NEW_GENERATION) == 21


def test_the_four_spellings_of_null_stay_distinct():
    """SQL NULL, 'N/A', 'UNKNOWN' and 'Unknown, Unknown' are NOT the same fact.

    'N/A' means there was no driver to test -- a parked or driverless vehicle.
    'UNKNOWN' means there was a driver and the officer did not record a result.
    Flattening all four to NULL is the lossy move the assignment is testing for.
    """
    assert parse_substance(None).scheme == SCHEME_NULL
    assert parse_substance("").scheme == SCHEME_NULL
    assert parse_substance("N/A").alcohol_status == NOT_APPLICABLE
    assert parse_substance("UNKNOWN").alcohol_status == UNKNOWN
    assert parse_substance("Unknown, Unknown").alcohol_status == UNKNOWN
    # And the two UNKNOWNs are still distinguishable by generation.
    assert parse_substance("UNKNOWN").scheme == SCHEME_OLD
    assert parse_substance("Unknown, Unknown").scheme == SCHEME_NEW


def test_present_and_contributed_both_mean_suspected_but_keep_the_distinction():
    """The causation claim survives in substance_detail, not in the status.

    An officer who records CONTRIBUTED has necessarily also detected, so both
    map to SUSPECTED for the harmonised flag -- but 'alcohol contributed' has to
    stay recoverable, and substance_detail is the only place it can live without
    making the flag incomparable with the new generation.
    """
    present = parse_substance("ALCOHOL PRESENT")
    contributed = parse_substance("ALCOHOL CONTRIBUTED")
    assert present.alcohol_status == contributed.alcohol_status == SUSPECTED
    assert present.substance_detail != contributed.substance_detail


def test_naive_comma_split_would_invent_a_party():
    """The embedded comma is the defect. One driver, two comma-separated tokens."""
    raw = "Not Suspect of Alcohol Use, Not Suspect of Drug Use"
    assert len(raw.split(",")) == 2          # what a naive split sees
    assert len(parse_substance_list(raw)) == 1  # what is actually there


@pytest.mark.parametrize("raw,n_parties", [
    # Real crash-level concatenations from bhju-22kf.
    ("NONE DETECTED", 1),
    ("N/A, NONE DETECTED", 2),
    ("NONE DETECTED, UNKNOWN", 2),
    ("ALCOHOL PRESENT, NONE DETECTED", 2),
    ("Not Suspect of Alcohol Use, Not Suspect of Drug Use", 1),
    ("Not Suspect of Alcohol Use, Not Suspect of Drug Use, "
     "Not Suspect of Alcohol Use, Not Suspect of Drug Use", 2),
    ("Not Suspect of Alcohol Use, Not Suspect of Drug Use, Unknown, Unknown", 2),
    ("Unknown, Unknown, Unknown, Unknown", 2),
    ("Not Suspect of Alcohol Use, Not Suspect of Drug Use, "
     "Not Suspect of Alcohol Use, Not Suspect of Drug Use, "
     "Not Suspect of Alcohol Use, Not Suspect of Drug Use", 3),
    ("Not Suspect of Alcohol Use, Not Suspect of Drug Use, "
     "Suspect of Alcohol Use, Unknown", 2),
    # Mixed generations in one crash -- the case the tokeniser exists for.
    ("NONE DETECTED, Not Suspect of Alcohol Use, Not Suspect of Drug Use", 2),
    ("Unknown, Unknown, UNKNOWN", 2),
    (None, 0),
    ("", 0),
])
def test_crash_level_concatenations_recover_the_right_party_count(raw, n_parties):
    parties = parse_substance_list(raw)
    assert len(parties) == n_parties
    assert all(not p.is_unmapped for p in parties)


def test_ambiguity_unknown_unknown_is_one_new_party_not_two_old_ones():
    """The one case that looks ambiguous, and is not.

    'Unknown, Unknown' could in principle be one new-generation party or two old
    ones -- except the old generation spells it 'UNKNOWN' in caps. The
    vocabularies are disjoint, so the greedy consume never has to guess.
    """
    assert len(parse_substance_list("Unknown, Unknown")) == 1
    assert len(parse_substance_list("UNKNOWN, UNKNOWN")) == 2


def test_unmapped_token_is_reported_not_guessed():
    """An unrecognised value must reach the drift detector, not be absorbed."""
    s = parse_substance("SOMETHING NOBODY HAS SEEN")
    assert s.scheme == SCHEME_UNMAPPED
    assert s.is_unmapped
    # And one bad party in a crash string does not destroy the rest.
    parties = parse_substance_list("NONE DETECTED, WIDGET, ALCOHOL PRESENT")
    assert [p.is_unmapped for p in parties] == [False, True, False]


def test_a_new_generation_alcohol_token_without_its_drug_half_is_unmapped():
    """A truncated pair is a defect, not a party with a guessed drug status."""
    parties = parse_substance_list("Suspect of Alcohol Use")
    assert len(parties) == 1 and parties[0].is_unmapped


def test_any_suspected_is_true_for_either_side():
    assert parse_substance("Suspect of Alcohol Use, Not Suspect of Drug Use").any_suspected
    assert parse_substance("Not Suspect of Alcohol Use, Suspect of Drug Use").any_suspected
    assert not parse_substance("NONE DETECTED").any_suspected


# ===========================================================================
# the drift detector
# ===========================================================================


def _obs(value, n=1, first=None, last=None):
    return ObservedValue(value, n, first, last, None, None)


def test_drift_detector_is_silent_on_the_accepted_vocabulary():
    values = [_obs(raw, 10) for raw, *_ in OLD_GENERATION + NEW_GENERATION]
    report = detect_drift("mmzv-x632", "driver_substance_abuse", values)
    assert not report.drifted
    assert report.rows_checked == 210


def test_drift_detector_fires_on_a_new_token_with_evidence():
    values = [
        _obs("NONE DETECTED", 100, "2023-01-01T00:00:00.000", "2023-12-27T23:00:00.000"),
        _obs("SOMETHING NEW", 3, "2026-02-01T10:00:00.000", "2026-03-01T10:00:00.000"),
    ]
    report = detect_drift("mmzv-x632", "driver_substance_abuse", values)
    assert report.drifted
    assert [t.value for t in report.unmapped] == ["SOMETHING NEW"]
    assert report.unmapped_rows == 3
    assert report.unmapped[0].first_crash_date_time == "2026-02-01T10:00:00.000"
    assert "SOMETHING NEW" in report.render()


def test_drift_detector_never_treats_null_as_drift():
    report = detect_drift(
        "mmzv-x632", "driver_substance_abuse", [_obs(None, 5), _obs("", 2)]
    )
    assert not report.drifted


def test_drift_detector_replays_the_pre_cutover_vocabulary_and_fires():
    """The Part 6 demonstration, as a unit test.

    Rebuild the vocabulary from values first seen on or before 2023-12-27, then
    replay everything against it. The new generation must be named. This is the
    same code path `python -m src.transform.drift --vocabulary-asof` runs.
    """
    values = [
        _obs("NONE DETECTED", 100, "2015-01-01T00:00:00.000", "2024-01-03T00:00:00.000"),
        _obs("N/A", 20, "2015-01-01T00:00:00.000", "2024-01-03T00:00:00.000"),
        _obs("Not Suspect of Alcohol Use, Not Suspect of Drug Use", 50,
             "2023-12-28T12:59:00.000", "2026-09-02T14:54:00.000"),
        _obs("Unknown, Unknown", 7, "2023-12-28T12:59:00.000", "2026-09-02T12:30:00.000"),
    ]
    accepted = vocabulary_asof(
        "mmzv-x632", "driver_substance_abuse", values, "2023-12-27"
    )
    assert set(accepted) == {"NONE DETECTED", "N/A"}

    report = detect_drift(
        "mmzv-x632", "driver_substance_abuse", values, accepted=accepted
    )
    assert report.drifted
    assert {t.value for t in report.unmapped} == {
        "Not Suspect of Alcohol Use, Not Suspect of Drug Use", "Unknown, Unknown"
    }
    assert all(t.first_crash_date_time.startswith("2023-12-28")
               for t in report.unmapped)


def test_drift_detector_is_generic_over_columns_not_special_cased():
    """The same call catches the injury_severity re-casing.

    A detector special-cased to driver_substance_abuse would have caught one
    generation change and slept through the other in the same week.
    """
    values = [
        _obs("NO APPARENT INJURY", 100, "2015-01-01T00:00:00.000", "2024-01-03T00:00:00.000"),
        _obs("No Apparent Injury", 46, "2023-12-28T12:59:00.000", "2026-09-02T14:54:00.000"),
    ]
    accepted = vocabulary_asof("mmzv-x632", "injury_severity", values, "2023-12-27")
    report = detect_drift("mmzv-x632", "injury_severity", values, accepted=accepted)
    assert report.drifted
    assert [t.value for t in report.unmapped] == ["No Apparent Injury"]


def test_drift_detector_tokenises_the_crash_level_column():
    """A never-before-seen CONCATENATION is not drift; a new TOKEN is.

    Otherwise every new multi-driver combination would look like a dictionary
    change, and the alert would be worthless within a week.
    """
    novel_combination = _obs(
        "ALCOHOL PRESENT, MEDICATION CONTRIBUTED, Unknown, Unknown", 1)
    assert not detect_drift(
        "bhju-22kf", "driver_substance_abuse", [novel_combination]).drifted

    novel_token = _obs("NONE DETECTED, BRAND NEW CODE", 1)
    report = detect_drift("bhju-22kf", "driver_substance_abuse", [novel_token])
    assert report.drifted
    assert [t.value for t in report.unmapped] == ["BRAND NEW CODE"]


# ===========================================================================
# the severity crosswalk
# ===========================================================================


@pytest.mark.parametrize("system,column,value,expected", [
    ("MONTGOMERY_MD", "injury_severity", "FATAL INJURY", 5),
    ("MONTGOMERY_MD", "injury_severity", "Fatal Injury", 5),
    ("MONTGOMERY_MD", "injury_severity", "NO APPARENT INJURY", 1),
    ("MONTGOMERY_MD", "injury_severity", "No Apparent Injury", 1),
    ("MONTGOMERY_MD", "injury_severity", None, 0),
    ("TXDOT_CRIS", "crash_sev_id", "4", 5),
    ("TXDOT_CRIS", "crash_sev_id", "1", 4),
    ("TXDOT_CRIS", "crash_sev_id", "2", 3),
    ("TXDOT_CRIS", "crash_sev_id", "3", 2),
    ("TXDOT_CRIS", "crash_sev_id", "5", 1),
    ("TXDOT_CRIS", "crash_sev_id", "0", 0),
    ("TXDOT_CRIS", "crash_sev_id", "95", 0),
    ("NHTSA_FARS", "INJ_SEV", "4", 5),
    ("NHTSA_FARS", "INJ_SEV", "0", 1),
])
def test_severity_crosswalk_maps(system, column, value, expected):
    assert to_ordinal(system, column, value) == expected


def test_fars_injured_severity_unknown_maps_to_0_not_2():
    """INJ_SEV 5 is 'injured, severity unknown'. It is NOT 'possible injury'.

    The person is known to be injured and the degree is not recorded. No other
    source has a code for that, and collapsing it into C would invent a severity
    the record does not assert.
    """
    assert to_ordinal("NHTSA_FARS", "INJ_SEV", "5") == 0
    assert to_ordinal("NHTSA_FARS", "INJ_SEV", "1") == 2  # the real C


def test_fars_died_prior_to_crash_maps_to_0_not_5():
    """INJ_SEV 6 is a death that the crash did not cause."""
    assert to_ordinal("NHTSA_FARS", "INJ_SEV", "6") == 0
    assert to_ordinal("NHTSA_FARS", "INJ_SEV", "4") == 5


def test_txdot_severity_scale_runs_backwards_from_the_ordinal():
    """5 is the LEAST severe CRIS code and 4 the most.

    Ordering by the raw id sorts the scale backwards, which is why the crosswalk
    exists and why no code compares crash_sev_id directly.
    """
    assert to_ordinal("TXDOT_CRIS", "crash_sev_id", "5") < to_ordinal(
        "TXDOT_CRIS", "crash_sev_id", "4")


def test_unmapped_severity_raises_rather_than_defaulting_to_zero():
    """A new code is drift. Silently mapping it to 0 hides a dictionary change."""
    with pytest.raises(UnmappedSeverity):
        to_ordinal("TXDOT_CRIS", "crash_sev_id", "77")
    with pytest.raises(UnmappedSeverity):
        to_ordinal("MONTGOMERY_MD", "injury_severity", "CATASTROPHIC")


def test_every_crosswalk_ordinal_is_in_range():
    from src.transform.severity_crosswalk import mappings
    for m in mappings():
        assert 0 <= m.severity_ordinal <= 5
        assert m.notes.strip(), f"{m.source_system}.{m.source_value} has no note"


# ===========================================================================
# the TxDOT crash_sev_id consistency query
# ===========================================================================


def test_txdot_crash_sev_id_consistency_query(silver_txdot):
    """crash_sev_id's meaning, verified from the injury counts in the same row.

    The CRIS guide's code table is not machine-readable from the published PDF,
    so this is the evidence the crosswalk rests on. Each code's injury-count
    profile must be the one the crosswalk claims -- and 4 must be the fatal one,
    not 5.
    """
    con, rel = silver_txdot
    rows = {
        r["crash_sev_id"]: r
        for r in txdot_transform.verify_crash_sev_consistency(con, rel)
    }

    if 4 in rows:
        r = rows[4]
        assert r["with_fatal"] == r["n"], "every crash_sev_id=4 must have a death"
    if 1 in rows:
        r = rows[1]
        assert r["with_serious"] == r["n"]
        assert r["with_fatal"] == 0, "crash_sev_id=1 must not be fatal"
    if 2 in rows:
        r = rows[2]
        assert r["with_minor"] == r["n"]
        assert r["with_serious"] == 0 and r["with_fatal"] == 0
    if 3 in rows:
        r = rows[3]
        assert r["with_possible"] == r["n"]
        assert r["with_minor"] == 0 and r["with_fatal"] == 0
    if 5 in rows:
        r = rows[5]
        assert r["with_none"] == r["n"]
        assert (r["with_fatal"] == r["with_serious"] == r["with_minor"]
                == r["with_possible"] == 0), (
            "crash_sev_id=5 is NOT INJURED -- if any injury count is set here "
            "the scale has been read backwards"
        )
    # The undocumented code, if the fixture caught it, is unknown -- never a
    # severity.
    if 95 in rows:
        assert rows[95]["with_fatal"] == 0
        assert to_ordinal("TXDOT_CRIS", "crash_sev_id", "95") == 0


# ===========================================================================
# FARS sentinel rules
# ===========================================================================


def test_sentinel_predicate_matches_both_decimal_formats():
    """The whole reason the match is numeric.

    NHTSA writes the same sentinel as 77.7777 in 2019 and 77.77770000 in 2024.
    """
    con = duckdb.connect()
    pred = fars_transform.sentinel_predicate("accident", "LATITUDE", "v")
    rows = con.execute(
        f"""SELECT v, {pred} AS is_sentinel FROM (VALUES
              ('77.7777'), ('77.77770000'), ('88.8888'), ('88.88880000'),
              ('99.9999'), ('99.99990000'), ('39.1234'), ('-77.2')
            ) t(v)"""
    ).fetchall()
    got = dict(rows)
    assert all(got[v] for v in ("77.7777", "77.77770000", "88.8888",
                                "88.88880000", "99.9999", "99.99990000"))
    assert not got["39.1234"] and not got["-77.2"]


def test_a_column_with_no_sentinel_rule_is_never_all_sentinel():
    """No rule must mean 'nothing is a sentinel', never 'everything is'."""
    pred = fars_transform.sentinel_predicate("accident", "LGT_COND", "v")
    assert pred == "FALSE"


def test_nine_is_not_blanket_treated_as_unknown():
    """9 is a substantive code in LGT_COND, HARM_EV and MAN_COLL.

    A blanket 7/8/9 rule would corrupt all three, which is why the sentinel
    list is per column.
    """
    sentinels = fars_transform.load_sentinels()
    guarded = {(s.table, s.column) for s in sentinels}
    for col in ("LGT_COND", "HARM_EV", "MAN_COLL", "WEATHER"):
        assert ("accident", col) not in guarded


def test_state_fips_maps_to_two_letter_jurisdiction():
    """The output contract requires ^[A-Z]{2}$."""
    m = fars_transform.STATE_FIPS_TO_USPS
    assert m["24"] == "MD" and m["48"] == "TX" and m["12"] == "FL"
    # FARS writes STATE unpadded, so both spellings must resolve.
    assert m["1"] == m["01"] == "AL"
    assert all(len(v) == 2 and v.isupper() for v in m.values())


# ===========================================================================
# the contract validator
# ===========================================================================


@pytest.fixture
def tiny_contract():
    return {
        "tables": {
            "t": {
                "type": "object",
                "additionalProperties": False,
                "required": ["id", "ordinal"],
                "properties": {
                    "id": {"type": "string"},
                    "ordinal": {"type": "integer", "minimum": 0, "maximum": 5},
                    "quality": {"enum": ["OK", "BAD", None], "type": ["string", "null"]},
                    "lat": {"type": ["number", "null"], "minimum": -90, "maximum": 90},
                },
                "x-table-constraints": {
                    "unique_keys": [["id"]],
                    "foreign_keys": [],
                    "row_count_min": 1,
                },
            },
            "parent": {
                "type": "object", "additionalProperties": False,
                "required": ["id"], "properties": {"id": {"type": "string"}},
                "x-table-constraints": {"unique_keys": [["id"]]},
            },
            "child": {
                "type": "object", "additionalProperties": False,
                "required": ["id", "parent_id", "orphan_ok"],
                "properties": {
                    "id": {"type": "string"},
                    "parent_id": {"type": "string"},
                    "orphan_ok": {"type": "boolean"},
                },
                "x-table-constraints": {
                    "unique_keys": [["id"]],
                    "foreign_keys": [{
                        "columns": ["parent_id"],
                        "references": {"table": "parent", "columns": ["id"]},
                        "orphans_allowed_when": "orphan_ok",
                    }],
                },
            },
        }
    }


def _rel(con, name, sql):
    con.execute(f"CREATE OR REPLACE TABLE {name} AS {sql}")
    return name


def test_contract_accepts_a_conforming_relation(tiny_contract):
    con = duckdb.connect()
    _rel(con, "t", """SELECT * FROM (VALUES
           ('a', 3, 'OK', 39.1), ('b', 0, NULL, NULL)) v(id, ordinal, quality, lat)""")
    assert contracts.validate_relation(con, "t", tiny_contract, "t") == []


def test_contract_catches_a_range_violation(tiny_contract):
    con = duckdb.connect()
    _rel(con, "t", """SELECT * FROM (VALUES
           ('a', 9, 'OK', 39.1)) v(id, ordinal, quality, lat)""")
    v = contracts.validate_relation(con, "t", tiny_contract, "t")
    assert [x.kind for x in v] == ["range"]
    assert v[0].count == 1
    assert v[0].examples == [["a"]]


def test_contract_catches_an_enum_violation(tiny_contract):
    con = duckdb.connect()
    _rel(con, "t", """SELECT * FROM (VALUES
           ('a', 1, 'WEIRD', 39.1)) v(id, ordinal, quality, lat)""")
    kinds = {x.kind for x in contracts.validate_relation(con, "t", tiny_contract, "t")}
    assert kinds == {"enum"}


def test_contract_catches_a_null_in_a_required_column(tiny_contract):
    con = duckdb.connect()
    _rel(con, "t", """SELECT * FROM (VALUES
           ('a', 1, 'OK', 39.1), (NULL, 2, 'OK', 39.2)) v(id, ordinal, quality, lat)""")
    kinds = [x.kind for x in contracts.validate_relation(con, "t", tiny_contract, "t")]
    assert "null" in kinds


def test_contract_catches_a_wrong_type(tiny_contract):
    """A column that was never cast is the failure this catches."""
    con = duckdb.connect()
    _rel(con, "t", """SELECT * FROM (VALUES
           ('a', '3', 'OK', 39.1)) v(id, ordinal, quality, lat)""")
    v = contracts.validate_relation(con, "t", tiny_contract, "t")
    assert any(x.kind == "type" and "ordinal" in x.detail for x in v)


def test_contract_catches_a_duplicate_key(tiny_contract):
    con = duckdb.connect()
    _rel(con, "t", """SELECT * FROM (VALUES
           ('a', 1, 'OK', 39.1), ('a', 2, 'OK', 39.2)) v(id, ordinal, quality, lat)""")
    v = contracts.validate_relation(con, "t", tiny_contract, "t")
    assert [x.kind for x in v] == ["unique-key"]
    assert v[0].count == 1


def test_contract_catches_column_reordering(tiny_contract):
    """Column order is part of the contract because it is part of determinism."""
    con = duckdb.connect()
    _rel(con, "t", """SELECT ordinal, id, quality, lat FROM (VALUES
           ('a', 1, 'OK', 39.1)) v(id, ordinal, quality, lat)""")
    kinds = [x.kind for x in contracts.validate_relation(con, "t", tiny_contract, "t")]
    assert "column-order" in kinds


def test_contract_catches_an_unexpected_column(tiny_contract):
    con = duckdb.connect()
    _rel(con, "t", """SELECT *, 1 AS surprise FROM (VALUES
           ('a', 1, 'OK', 39.1)) v(id, ordinal, quality, lat)""")
    kinds = [x.kind for x in contracts.validate_relation(con, "t", tiny_contract, "t")]
    assert "unexpected-columns" in kinds


def test_contract_row_count_floor_can_be_skipped_for_small_corpora(tiny_contract):
    tiny_contract["tables"]["t"]["x-table-constraints"]["row_count_min"] = 1000
    con = duckdb.connect()
    _rel(con, "t", """SELECT * FROM (VALUES
           ('a', 1, 'OK', 39.1)) v(id, ordinal, quality, lat)""")
    assert any(x.kind == "row-count" for x in
               contracts.validate_relation(con, "t", tiny_contract, "t"))
    assert contracts.validate_relation(
        con, "t", tiny_contract, "t", check_row_count_min=False) == []


def test_contract_foreign_key_and_the_orphans_allowed_escape(tiny_contract):
    """orphans_allowed_when is not a loophole -- it names the measured exception."""
    con = duckdb.connect()
    _rel(con, "parent", "SELECT * FROM (VALUES ('p1')) v(id)")
    _rel(con, "child", """SELECT * FROM (VALUES
           ('c1', 'p1', false), ('c2', 'MISSING', true)
         ) v(id, parent_id, orphan_ok)""")
    resolve = {"parent": "parent"}
    # The orphan is licensed by its flag, so the contract is satisfied.
    assert contracts.validate_foreign_keys(
        con, tiny_contract, "child", "child", resolve) == []

    # Take the flag away and it is a violation again.
    _rel(con, "child", """SELECT * FROM (VALUES
           ('c1', 'p1', false), ('c2', 'MISSING', false)
         ) v(id, parent_id, orphan_ok)""")
    v = contracts.validate_foreign_keys(
        con, tiny_contract, "child", "child", resolve)
    assert [x.kind for x in v] == ["foreign-key"]
    assert v[0].count == 1


def test_contract_raise_for_collects_every_violation(tiny_contract):
    """A validator that stops at the first error makes fixing a schema a game."""
    con = duckdb.connect()
    _rel(con, "t", """SELECT * FROM (VALUES
           ('a', 9, 'WEIRD', 999.0), ('a', 8, 'ALSO WEIRD', 998.0)
         ) v(id, ordinal, quality, lat)""")
    v = contracts.validate_relation(con, "t", tiny_contract, "t")
    kinds = {x.kind for x in v}
    # ordinal > 5, quality outside the enum, lat > 90, and a duplicate id --
    # four independent findings, all reported from one pass.
    assert kinds == {"range", "enum", "unique-key"}
    assert len(v) == 4
    with pytest.raises(contracts.ContractViolation) as exc:
        contracts.raise_for(v, context="test")
    assert "4 contract violation(s)" in str(exc.value)
    assert "ordinal > 5" in str(exc.value) and "lat > 90" in str(exc.value)


def test_contract_raises_for_a_table_with_no_entry(tiny_contract):
    """A table nobody has written a contract for has not been reviewed."""
    con = duckdb.connect()
    with pytest.raises(KeyError):
        contracts.validate_relation(con, "t", tiny_contract, "nonexistent")


def test_the_real_contracts_load_and_cover_every_built_table():
    """The committed contracts must actually describe what build.py writes."""
    from src.transform import build as build_module
    from src.transform import unified

    silver = contracts.load_contract(contracts.SILVER_CONTRACT)
    expected = {ct for source in build_module.ALL_SOURCES
                for _t, _r, ct, _o in build_module.TABLES[source]}
    expected.add("silver.crash")
    assert expected <= set(silver["tables"])

    bronze = contracts.load_contract(contracts.BRONZE_CONTRACT)
    assert {"montgomery.bhju-22kf", "montgomery.mmzv-x632", "montgomery.n7fk-dce5",
            "txdot.cris_crash", "fars.accident", "fars.vehicle",
            "fars.person"} <= set(bronze["tables"])
    assert unified.COLUMNS == list(silver["tables"]["silver.crash"]["properties"])


# ===========================================================================
# the deterministic writer
# ===========================================================================


def test_writer_refuses_a_non_total_sort_key(tmp_path):
    """The finding that shaped the writer, as a guard.

    ORDER BY a non-unique key produced three different sha256s at threads=1/2/8
    under both the DuckDB COPY writer and pyarrow, because the tie is broken by
    whichever thread finished first. write_parquet() asserts the order is total
    rather than hoping.
    """
    con = c.connect()
    con.execute("CREATE TABLE t AS SELECT * FROM (VALUES ('a', 1), ('a', 2)) v(k, n)")
    with pytest.raises(c.NonTotalOrder):
        c.write_parquet(con, "t", tmp_path / "t.parquet",
                        columns=["k", "n"], order_by=["k"])
    # With a total order it writes.
    info = c.write_parquet(con, "t", tmp_path / "t.parquet",
                           columns=["k", "n"], order_by=["k", "n"])
    assert info["rows"] == 2 and (tmp_path / "t.parquet").exists()


def test_writer_is_byte_stable_across_thread_counts(tmp_path):
    import hashlib
    hashes = set()
    for threads in (1, 4):
        con = c.connect(threads=threads)
        con.execute(
            "CREATE TABLE t AS SELECT i AS k, i * 2 AS n, 'x' || i AS s "
            "FROM range(5000) r(i)"
        )
        dest = tmp_path / f"t_{threads}.parquet"
        c.write_parquet(con, "t", dest, columns=["k", "n", "s"], order_by=["k"])
        hashes.add(hashlib.sha256(dest.read_bytes()).hexdigest())
        con.close()
    assert len(hashes) == 1


def test_envelope_distance_is_zero_inside_and_positive_outside():
    assert c.distance_from_envelope_m("montgomery", 39.1, -77.1) == 0.0
    d = c.distance_from_envelope_m("montgomery", 39.72, -79.486)
    assert 150_000 < d < 250_000, f"expected ~170 km, got {d/1000:.1f} km"


def test_geodesic_distance_is_not_web_mercator():
    """A sanity check on the CRS choice.

    One degree of latitude is ~111 km everywhere. If this were computed in
    EPSG:3857 the answer at 39N would be off by ~1/cos(39) = 1.29.
    """
    d = c.geodesic_distance_m(39.0, -77.0, 40.0, -77.0)
    assert 110_000 < d < 112_000
