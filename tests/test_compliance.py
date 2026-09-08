"""Phase 6: the eligibility engine, the vault, the lineage and the fixture golden.

Same rules as the rest of the suite: no mocks, no network, real code paths.
Every engine test is a minimal record dict evaluated by the real
`EligibilityEngine` against the real `src/compliance/rules.yaml` and
`config/blackout_windows.csv`, so a rule that stops being in the file stops
passing a test.

`tests/test_engine.py` is already taken -- it holds the silver grammar and
crosswalk unit tests -- so the engine tests live here.

Three fixtures do the heavy lifting:

  `engine`             the production ruleset at the frozen as_of
  `record`             a factory for a minimal, otherwise-clean record
  `compliance_root`    a full build into tmp_path, gated on the local gold

The build-backed tests skip rather than fail when `data/gold/` is absent, the
same way the Phase 4 and 5 geo tests skip on missing reference data: a fresh
clone must be able to run the suite, and a skip that names what is missing is
more useful than a green run that asserted nothing.
"""

from __future__ import annotations

import csv
import hashlib
import json
import shutil
from datetime import date, datetime, time, timedelta, timezone
from pathlib import Path

import pandas as pd
import pytest

from src import config, contracts
from src.compliance import build as build_mod
from src.compliance import consent as consent_mod
from src.compliance import fl_incompleteness as fl_mod
from src.compliance import leads as leads_mod
from src.compliance import lineage as lineage_mod
from src.compliance import retention as retention_mod
from src.compliance import ruleset as ruleset_mod
from src.compliance import vault as vault_mod
from src.compliance import window as window_mod
from src.compliance.engine import EligibilityEngine
from src.compliance.gates import channel as channel_gate
from src.compliance.reason_codes import IdentityProvenance, MonitoringLabel, ReasonCode

REPO = Path(__file__).resolve().parents[1]
AS_OF = date(2026, 9, 1)
GOLDEN = REPO / "tests" / "fixtures" / "compliance" / "fixture_dispositions.csv"
SELLER = "CRASH_TO_CONTACT_DEMO_SELLER"


# ---------------------------------------------------------------------------
# fixtures
# ---------------------------------------------------------------------------


@pytest.fixture(scope="session")
def engine() -> EligibilityEngine:
    return EligibilityEngine(as_of=AS_OF)


@pytest.fixture
def record():
    """A minimal record that fails nothing, so a test can break ONE thing.

    Consent is present and complete, the scrub is fresh, the RND said No, the
    line type is a landline (the least restrictive row, so a test that changes
    it is testing the routing and not the consent requirement), and the
    geography is clean. Everything a test asserts is therefore attributable to
    the field that test changed.
    """
    def make(**overrides):
        provenance = consent_mod.synthesise_provenance(
            party_id="T001", party_token="pt_" + "0" * 32,
            obtained_on=date(2026, 1, 1), timezone_name="America/New_York",
            seller=SELLER,
        )
        base = {
            "lead_id": "LD_" + "0" * 16,
            "party_token": "pt_" + "0" * 32,
            "jurisdiction": "MD",
            "identity_provenance": IdentityProvenance.SYNTHETIC_FIXTURE.value,
            "record_types": ["crash_report", "written_solicitation",
                             "telephone_solicitation"],
            "incident_date": date(2026, 1, 1),
            "report_filing_date": date(2026, 1, 2),
            "npa": "301",
            "line_type": "landline",
            "line_type_asof": None,
            "rnd_response": "NO",
            "on_national_dnc": False,
            "dnc_scrub_age_days": 2,
            "consent_on_file": True,
            "consent_revoked": False,
            "consent_provenance": provenance,
            "revocations": [],
            "internal_dnc_listed": False,
            "ebr": [],
            "seller": SELLER,
            "tz_iana": "America/New_York",
            "tz_source": "COORDINATE",
            "envelope_status": "OK",
            "snap_status": "SNAPPED",
            "snap_distance_m": 5.0,
        }
        base.update(overrides)
        return base
    return make


@pytest.fixture(scope="session")
def local_gold() -> Path:
    root = config.GOLD_DIR
    if not (root / "fact_crash.parquet").exists():
        pytest.skip(f"no local gold at {root} -- run `python -m src.transform.build`")
    return root


@pytest.fixture(scope="session")
def compliance_root(tmp_path_factory, local_gold: Path) -> dict:
    """One real build into tmp_path. No mocks; the CLI's own entry point."""
    dest = tmp_path_factory.mktemp("compliance")
    result = build_mod.build_compliance(
        gold_root=local_gold,
        out_root=dest / "out",
        vault_dir=dest / "vault",
        sample_path=dest / "sample_leads.csv",
        schema_check_path=dest / "sample_leads.schema_check.json",
        as_of=AS_OF,
        crash_only_limit=200,
        small_corpus=True,
    )
    result["_dest"] = dest
    return result


@pytest.fixture(scope="session")
def lead_rows(compliance_root) -> pd.DataFrame:
    return pd.read_parquet(Path(compliance_root["out_root"]) / "leads.parquet")


# ---------------------------------------------------------------------------
# 1. time windows
# ---------------------------------------------------------------------------


def test_texas_31_day_window_blocks_and_dates_exclusively(engine, record):
    """TX, incident 10 days ago -> BLOCKED_UNTIL incident + 31 + 1."""
    incident = AS_OF - timedelta(days=10)
    decision = engine.evaluate(record(
        jurisdiction="TX", incident_date=incident,
        report_filing_date=incident + timedelta(days=1),
        npa="512", tz_iana="America/Chicago",
        snap_status=leads_mod.SNAP_NOT_IN_SCOPE, snap_distance_m=None,
    ))
    assert decision.status == "BLOCKED_UNTIL"
    assert ReasonCode.TX_SOLICITATION_31D in decision.reason_codes
    # Exclusive: contactable strictly AFTER incident + 31 days.
    assert decision.blocked_until_date == incident + timedelta(days=32)


def test_florida_data_gate_binds_when_the_bar_rule_has_lapsed(engine, record):
    """Filed 40 days ago, incident 45 -> the 60-day gate alone; the 30-day one is gone.

    ASSIGNMENT.md 5a: "A candidate who implements only the bar rule has
    implemented the wrong constraint." This is the assertion that catches it.
    """
    filed = AS_OF - timedelta(days=40)
    decision = engine.evaluate(record(
        jurisdiction="FL", incident_date=AS_OF - timedelta(days=45),
        report_filing_date=filed, npa="407",
        snap_status=leads_mod.SNAP_NOT_IN_SCOPE, snap_distance_m=None,
    ))
    assert decision.status == "BLOCKED_UNTIL"
    assert ReasonCode.FL_CRASH_REPORT_60D in decision.reason_codes
    assert ReasonCode.FL_SOLICITATION_30D not in decision.reason_codes
    assert decision.blocked_until_date == filed + timedelta(days=61)


def test_florida_emits_both_codes_and_takes_the_maximum(engine, record):
    filed = AS_OF - timedelta(days=5)
    incident = AS_OF - timedelta(days=6)
    decision = engine.evaluate(record(
        jurisdiction="FL", incident_date=incident, report_filing_date=filed,
        npa="407", snap_status=leads_mod.SNAP_NOT_IN_SCOPE, snap_distance_m=None,
    ))
    assert ReasonCode.FL_CRASH_REPORT_60D in decision.reason_codes
    assert ReasonCode.FL_SOLICITATION_30D in decision.reason_codes
    assert decision.blocked_until_date == max(
        filed + timedelta(days=61), incident + timedelta(days=31)
    ) == filed + timedelta(days=61)


def test_missing_anchor_date_closes_the_gate_with_no_publishable_date(engine, record):
    decision = engine.evaluate(record(
        jurisdiction="FL", incident_date=AS_OF - timedelta(days=5),
        report_filing_date=None, npa="407",
        snap_status=leads_mod.SNAP_NOT_IN_SCOPE, snap_distance_m=None,
    ))
    assert decision.status == "INELIGIBLE"
    assert ReasonCode.ANCHOR_DATE_MISSING in decision.reason_codes
    # No date, precisely because inventing an anchor is how a waiting period
    # gets skipped by a null.
    assert decision.blocked_until_date is None


def test_maryland_has_no_time_gate_but_the_row_names_its_carrier(engine, record):
    """MD, consented, fresh DNC, wireless -- the disposition the analysis concludes."""
    decision = engine.evaluate(record(line_type="wireless"))
    assert decision.status == "ELIGIBLE"
    assert ReasonCode.ELIGIBLE_CONSENTED in decision.reason_codes
    # No Maryland time window exists; the constraint is a channel bar.
    md_rows = [r for r in engine.blackout if r.jurisdiction == "MD"]
    assert md_rows and all(not r.has_window for r in md_rows)
    assert ReasonCode.MD_MVA_TELEPHONE_SOLICITATION_BAR.value in md_rows[0].notes
    assert ReasonCode.LIVE_SOLICITATION_PROHIBITED.value in md_rows[0].notes


def test_maryland_cold_contact_is_barred_on_both_grounds(engine, record):
    """The conclusion the business will not like, asserted rather than argued.

    Remove the consumer-direct consent -- which is what "cold contact" means --
    and the Maryland record fails the 4-320 channel bar AND the Rule 19-307.3
    live-solicitation bar, with no time window anywhere in sight.
    """
    decision = engine.evaluate(record(
        identity_provenance=IdentityProvenance.PUBLIC_CRASH_REPORT.value,
        consent_on_file=False, consent_provenance=None, line_type="wireless",
    ))
    assert decision.status == "INELIGIBLE"
    assert ReasonCode.MD_MVA_TELEPHONE_SOLICITATION_BAR in decision.reason_codes
    assert ReasonCode.LIVE_SOLICITATION_PROHIBITED in decision.reason_codes
    assert ReasonCode.CONSENT_ABSENT in decision.reason_codes
    assert decision.blocked_until_date is None


def test_aviation_wildcard_row_reaches_only_a_declared_aviation_record(engine, record):
    road = engine.evaluate(record(incident_date=AS_OF - timedelta(days=2)))
    assert ReasonCode.AVIATION_45D not in road.reason_codes
    air = engine.evaluate(record(
        incident_date=AS_OF - timedelta(days=2),
        record_types=["aviation_accident"],
    ))
    assert ReasonCode.AVIATION_45D in air.reason_codes


# ---------------------------------------------------------------------------
# 2. provenance and source gates
# ---------------------------------------------------------------------------


@pytest.mark.parametrize("provenance", [None, IdentityProvenance.UNKNOWN.value])
def test_unknown_provenance_is_always_ineligible(engine, record, provenance):
    decision = engine.evaluate(record(identity_provenance=provenance))
    assert decision.status == "INELIGIBLE"
    assert ReasonCode.PROVENANCE_UNKNOWN in decision.reason_codes


@pytest.mark.parametrize("jurisdiction,expected", [
    ("MD", ReasonCode.MD_MVA_TELEPHONE_SOLICITATION_BAR),
    ("TX", ReasonCode.TX_REDACTED_NO_CONTACT_PII),
    ("FL", ReasonCode.FL_CRASH_REPORT_CONFIDENTIAL),
])
def test_public_crash_report_with_no_contact_block(engine, jurisdiction, expected):
    """A real gold row: no phone, no consent, no line type. The production case."""
    decision = engine.evaluate_crash_only({
        "lead_id": "CO_1", "jurisdiction": jurisdiction,
        "incident_date": date(2024, 1, 1), "report_filing_date": date(2024, 1, 2),
        "tz_iana": "America/New_York", "tz_source": "COORDINATE",
        "envelope_status": "OK", "snap_status": "NOT_ATTEMPTED",
    })
    assert decision.status == "INELIGIBLE"
    assert expected in decision.reason_codes
    assert ReasonCode.CONSENT_ABSENT in decision.reason_codes


def test_motor_vehicle_record_provenance_fails_the_dppa_everywhere(engine, record):
    for jurisdiction in ("MD", "TX", "FL"):
        decision = engine.evaluate(record(
            jurisdiction=jurisdiction,
            identity_provenance=IdentityProvenance.MOTOR_VEHICLE_RECORD.value,
            snap_status=leads_mod.SNAP_NOT_IN_SCOPE, snap_distance_m=None,
            npa="301" if jurisdiction == "MD" else "512",
            tz_iana="America/New_York",
        ))
        assert ReasonCode.DPPA_NO_PERMISSIBLE_USE in decision.reason_codes
        assert decision.status == "INELIGIBLE"


# ---------------------------------------------------------------------------
# 3. the channel stack
# ---------------------------------------------------------------------------


@pytest.mark.parametrize("response,expected", [
    ("NO", None),
    ("YES", ReasonCode.RND_REASSIGNED),
    ("NO_DATA", ReasonCode.RND_NO_DATA_NO_SAFE_HARBOR),
    (None, ReasonCode.RND_RESPONSE_UNRESOLVED),
])
def test_rnd_has_three_states_and_a_silence(engine, record, response, expected):
    """The safe harbour attaches ONLY to "No". NO_DATA is not a green light."""
    decision = engine.evaluate(record(rnd_response=response))
    rnd_codes = {ReasonCode.RND_REASSIGNED, ReasonCode.RND_NO_DATA_NO_SAFE_HARBOR,
                 ReasonCode.RND_RESPONSE_UNRESOLVED}
    present = rnd_codes & set(decision.reason_codes)
    assert present == (set() if expected is None else {expected})
    if expected is not None:
        assert decision.status == "INELIGIBLE"


@pytest.mark.parametrize("age,stale", [(30, False), (31, False), (32, True), (45, True)])
def test_dnc_scrub_staleness_is_a_31_day_sla_not_a_listing_check(engine, record,
                                                                 age, stale):
    decision = engine.evaluate(record(on_national_dnc=False, dnc_scrub_age_days=age))
    assert (ReasonCode.DNC_SCRUB_STALE in decision.reason_codes) is stale
    assert ReasonCode.DNC_LISTED not in decision.reason_codes


def test_a_missing_scrub_date_fails_closed(engine, record):
    decision = engine.evaluate(record(dnc_scrub_age_days=None))
    assert ReasonCode.DNC_SCRUB_STALE in decision.reason_codes


def test_listed_on_the_national_registry_is_a_bar(engine, record):
    decision = engine.evaluate(record(on_national_dnc=True))
    assert decision.status == "INELIGIBLE"
    assert ReasonCode.DNC_LISTED in decision.reason_codes


def test_internal_dnc_fires_when_the_token_is_suppressed(engine, record):
    clean = engine.evaluate(record(internal_dnc_listed=False))
    assert ReasonCode.INTERNAL_DNC not in clean.reason_codes
    suppressed = engine.evaluate(record(internal_dnc_listed=True))
    assert suppressed.status == "INELIGIBLE"
    assert ReasonCode.INTERNAL_DNC in suppressed.reason_codes


@pytest.mark.parametrize("kind,days,fires", [
    ("TRANSACTION", 30, True), ("TRANSACTION", 600, False),
    ("INQUIRY", 30, True), ("INQUIRY", 120, False),
])
def test_established_business_relationship_windows(engine, record, kind, days, fires):
    """No fixture row has an EBR, so ELIGIBLE_EBR never fires there. It can here."""
    decision = engine.evaluate(record(
        ebr=[{"kind": kind, "occurred_at": AS_OF - timedelta(days=days)}]
    ))
    assert (ReasonCode.ELIGIBLE_EBR in decision.reason_codes) is fires


def test_voip_and_unknown_route_to_the_most_restrictive_row(engine):
    """The routing OUTCOME, asserted directly rather than inferred from a status."""
    rules = engine.rules
    wireless = channel_gate.routing_outcome(rules, "wireless")
    landline = channel_gate.routing_outcome(rules, "landline")
    assert wireless != landline
    for line_type in ("voip", "unknown", None, "satellite-nobody-anticipated"):
        outcome = channel_gate.routing_outcome(rules, line_type)
        assert outcome == wireless, line_type
        assert outcome != landline, line_type
        assert outcome["tier"] == rules.line_type_routing["most_restrictive_tier"]


def test_voip_is_recorded_and_unknown_holds(engine, record):
    voip = engine.evaluate(record(line_type="voip"))
    assert ReasonCode.LINE_TYPE_VOIP_RESTRICTED in voip.reason_codes
    # Recorded, not blocking: the consent requirement it carries is enforced by
    # the consent gate, and this record has valid consent.
    assert voip.status == "ELIGIBLE"
    unknown = engine.evaluate(record(line_type="unknown"))
    assert ReasonCode.LINE_TYPE_UNRESOLVED in unknown.reason_codes
    assert unknown.status == "INELIGIBLE"


def test_line_type_staleness_fires_only_when_an_asof_is_present(engine, record):
    absent = engine.evaluate(record(line_type_asof=None))
    assert ReasonCode.LINE_TYPE_STALE not in absent.reason_codes
    fresh = engine.evaluate(record(line_type_asof=AS_OF - timedelta(days=10)))
    assert ReasonCode.LINE_TYPE_STALE not in fresh.reason_codes
    stale = engine.evaluate(record(line_type_asof=AS_OF - timedelta(days=60)))
    assert ReasonCode.LINE_TYPE_STALE in stale.reason_codes
    assert stale.status == "INELIGIBLE"


# ---------------------------------------------------------------------------
# 4. consent
# ---------------------------------------------------------------------------


def test_revocation_trumps_a_consent_on_file(engine, record):
    decision = engine.evaluate(record(consent_on_file=True, consent_revoked=True))
    assert decision.status == "INELIGIBLE"
    assert ReasonCode.CONSENT_REVOKED in decision.reason_codes
    assert ReasonCode.ELIGIBLE_CONSENTED not in decision.reason_codes


def test_revoke_all_blocks_every_sellers_campaign(engine, record):
    """FCC DA 26-12's revoke-all, honoured now rather than on 2027-01-31."""
    token = "pt_" + "0" * 32
    per_seller = consent_mod.make_revocation(
        party_token=token, seller="SOME_OTHER_SELLER",
        received_at=datetime(2026, 6, 1, tzinfo=timezone.utc),
        honour_business_days=10, scope=consent_mod.SCOPE_SELLER,
    )
    scoped = engine.evaluate(record(revocations=[per_seller]))
    assert ReasonCode.CONSENT_REVOKED not in scoped.reason_codes

    revoke_all = consent_mod.make_revocation(
        party_token=token, seller="SOME_OTHER_SELLER",
        received_at=datetime(2026, 6, 1, tzinfo=timezone.utc),
        honour_business_days=10, scope=consent_mod.SCOPE_ALL,
    )
    cascaded = engine.evaluate(record(revocations=[revoke_all]))
    assert ReasonCode.CONSENT_REVOKED in cascaded.reason_codes
    assert cascaded.status == "INELIGIBLE"


def test_revocation_honour_by_is_ten_business_days(engine):
    revocation = consent_mod.make_revocation(
        party_token="pt_x", seller=SELLER,
        received_at=datetime(2026, 8, 21, 12, 0, tzinfo=timezone.utc),  # a Friday
        honour_business_days=int(
            engine.rules.consent_spec["revocation"]["honour_business_days"]),
    )
    assert revocation.honour_by == date(2026, 9, 4)
    assert revocation.honour_by.weekday() < 5


def test_incomplete_consent_provenance_is_unverifiable_consent(engine, record):
    """The burden of proving consent is on the caller."""
    required = list(engine.rules.consent_spec["required_provenance_fields"])
    complete = record()["consent_provenance"]
    assert complete.is_complete(required)

    broken = complete.model_copy(update={"disclosure_hash": None})
    assert broken.missing(required) == ["disclosure_hash"]
    decision = engine.evaluate(record(consent_provenance=broken))
    assert decision.status == "INELIGIBLE"
    assert ReasonCode.CONSENT_UNVERIFIABLE in decision.reason_codes
    assert ReasonCode.ELIGIBLE_CONSENTED not in decision.reason_codes


def test_a_consent_boolean_with_no_provenance_record_is_not_consent(engine, record):
    decision = engine.evaluate(record(consent_on_file=True, consent_provenance=None))
    assert ReasonCode.CONSENT_UNVERIFIABLE in decision.reason_codes


def test_absent_consent(engine, record):
    decision = engine.evaluate(record(consent_on_file=False, consent_provenance=None))
    assert ReasonCode.CONSENT_ABSENT in decision.reason_codes
    assert decision.status == "INELIGIBLE"


# ---------------------------------------------------------------------------
# 5. effective dating -- ASSIGNMENT.md 5d
# ---------------------------------------------------------------------------


def test_bradford_moves_a_texas_decision_with_no_code_change(record):
    """The same record, the same ruleset, two dates, two answers.

    47 C.F.R. 64.1200(a)(2)'s written-consent requirement bound nationally
    until Bradford (5th Cir., 25 Feb 2026); after it, the Fifth Circuit is
    carved out. Texas is the Fifth Circuit. Nothing in `src/` knows that.
    """
    provenance = consent_mod.synthesise_provenance(
        party_id="T009", party_token="pt_" + "1" * 32,
        obtained_on=date(2025, 1, 1), timezone_name="America/Chicago",
        seller=SELLER, consent_form=consent_mod.FORM_ORAL,
    )
    payload = dict(
        jurisdiction="TX", identity_provenance=IdentityProvenance.CONSUMER_DIRECT.value,
        line_type="wireless", npa="512", tz_iana="America/Chicago",
        incident_date=date(2024, 1, 1), report_filing_date=date(2024, 1, 2),
        consent_provenance=provenance,
        snap_status=leads_mod.SNAP_NOT_IN_SCOPE, snap_distance_m=None,
    )
    before = EligibilityEngine(as_of=date(2026, 2, 1)).evaluate(record(**payload))
    after = EligibilityEngine(as_of=date(2026, 9, 1)).evaluate(record(**payload))
    assert ReasonCode.CONSENT_FORM_NOT_WRITTEN in before.reason_codes
    assert ReasonCode.CONSENT_FORM_NOT_WRITTEN not in after.reason_codes
    assert before.status == "INELIGIBLE" and after.status == "ELIGIBLE"


def test_written_consent_still_binds_outside_the_fifth_circuit(record):
    """Bradford carved out one circuit, not the rule. Maryland is the Fourth."""
    provenance = consent_mod.synthesise_provenance(
        party_id="T010", party_token="pt_" + "2" * 32, obtained_on=date(2025, 1, 1),
        timezone_name="America/New_York", seller=SELLER,
        consent_form=consent_mod.FORM_ORAL,
    )
    decision = EligibilityEngine(as_of=date(2026, 9, 1)).evaluate(record(
        jurisdiction="MD", identity_provenance=IdentityProvenance.CONSUMER_DIRECT.value,
        line_type="wireless", consent_provenance=provenance,
    ))
    assert ReasonCode.CONSENT_FORM_NOT_WRITTEN in decision.reason_codes


def test_the_vacated_one_to_one_rule_never_fires_at_any_date(record):
    """IMC v. FCC. effective_from is AFTER effective_to, so it is never in force."""
    rules = ruleset_mod.load_ruleset()
    rule = next(r for r in rules.consent_rules if r.id == "CONSENT_ONE_TO_ONE")
    assert rule.raw["status"] == "VACATED"
    assert rule.raw["contractual"] is True
    assert rule.effective_from > rule.effective_to
    for as_of in (date(2024, 1, 1), date(2025, 1, 25), date(2025, 6, 1),
                  date(2026, 9, 1), date(2030, 1, 1)):
        assert not rule.in_force(as_of)
        decision = EligibilityEngine(as_of=as_of).evaluate(record())
        assert ReasonCode.CONSENT_NOT_ONE_TO_ONE not in decision.reason_codes


def test_maryland_calling_hours_did_not_exist_before_2024(engine):
    """The Stop the Spam Calls Act took effect 2024-01-01; before that, federal."""
    rules = engine.rules
    before = window_mod.resolve(
        ruleset=rules, jurisdiction="MD", coordinate_zone="America/New_York",
        npa="301", as_of=date(2023, 6, 1))
    after = window_mod.resolve(
        ruleset=rules, jurisdiction="MD", coordinate_zone="America/New_York",
        npa="301", as_of=date(2026, 9, 1))
    assert before.latest == "21:00"
    assert after.latest == "20:00"


# ---------------------------------------------------------------------------
# 6. severity ordering and the parallel citation array
# ---------------------------------------------------------------------------


def test_three_failing_gates_give_three_codes_in_declared_severity_order(engine,
                                                                        record):
    decision = engine.evaluate(record(
        identity_provenance=IdentityProvenance.UNKNOWN.value,
        on_national_dnc=True,
        consent_on_file=False, consent_provenance=None,
    ))
    order = engine.rules.severity_order
    positions = [order.index(c) for c in decision.reason_codes]
    assert positions == sorted(positions)
    assert {ReasonCode.PROVENANCE_UNKNOWN, ReasonCode.CONSENT_ABSENT,
            ReasonCode.DNC_LISTED} <= set(decision.reason_codes)
    assert len(decision.legal_basis) == len(decision.reason_codes)
    assert all(b.strip() for b in decision.legal_basis)


def test_a_record_that_fails_nothing_and_proves_nothing_is_still_ineligible(engine,
                                                                           record):
    """The assignment's default disposition, emitted as a positive statement."""
    decision = engine.evaluate(record(
        consent_on_file=False, consent_provenance=None,
        identity_provenance=IdentityProvenance.CONSUMER_DIRECT.value,
        jurisdiction="OH", npa=None, tz_iana="America/New_York",
        snap_status=leads_mod.SNAP_NOT_IN_SCOPE, snap_distance_m=None,
    ))
    assert decision.status == "INELIGIBLE"
    assert decision.reason_codes  # never empty
    assert len(decision.legal_basis) == len(decision.reason_codes)


def test_a_non_curable_failure_beats_a_time_window(engine, record):
    """Both codes are emitted; only the status collapses to INELIGIBLE."""
    incident = AS_OF - timedelta(days=5)
    decision = engine.evaluate(record(
        jurisdiction="TX", incident_date=incident,
        report_filing_date=incident, npa="512", tz_iana="America/Chicago",
        on_national_dnc=True,
        snap_status=leads_mod.SNAP_NOT_IN_SCOPE, snap_distance_m=None,
    ))
    assert decision.status == "INELIGIBLE"
    assert decision.blocked_until_date is None
    assert ReasonCode.DNC_LISTED in decision.reason_codes
    assert ReasonCode.TX_SOLICITATION_31D in decision.reason_codes


def test_every_emittable_code_has_an_order_and_a_disposition(engine):
    order = set(engine.rules.severity_order)
    for code in ReasonCode:
        assert code.value in order, code
        assert engine.rules.disposition(code.value)


# ---------------------------------------------------------------------------
# 7. the calling window
# ---------------------------------------------------------------------------


def test_p007_el_paso_resolves_to_mountain_from_the_coordinate(engine):
    """The Texas default is Central. The coordinate says Denver, and it wins."""
    from src.geo import tz as tz_mod

    zone = tz_mod.ZoneFinder().zone_at(31.8479, -106.5348)
    assert zone == "America/Denver"
    assert config.geo()["tz"]["jurisdiction_default"]["TX"] == "America/Chicago"
    window = window_mod.resolve(
        ruleset=engine.rules, jurisdiction="TX", coordinate_zone=zone,
        npa="915", as_of=AS_OF)
    assert window.zone == "America/Denver"
    assert window.npa_zones == ("America/Denver",)     # the fallback agrees
    assert window.basis.startswith("coords:")
    assert (window.earliest, window.latest) == ("09:00", "21:00")


def test_p008_pensacola_resolves_to_central_and_the_npa_set_narrows_it(engine):
    """Both naive methods agree on the wrong answer here; the coordinate does not."""
    from src.geo import tz as tz_mod

    zone = tz_mod.ZoneFinder().zone_at(30.4213, -87.2169)
    assert zone == "America/Chicago"
    assert config.geo()["tz"]["jurisdiction_default"]["FL"] == "America/New_York"
    table = window_mod.default_npa_table()
    assert table["850"].split and len(table["850"].zones) == 2
    window = window_mod.resolve(
        ruleset=engine.rules, jurisdiction="FL", coordinate_zone=zone,
        npa="850", as_of=AS_OF)
    assert window.zone == "America/Chicago"
    assert window.intersected and window.narrowed_minutes == 60
    assert (window.earliest, window.latest) == ("08:00", "19:00")
    assert window.basis.startswith("coords:America/Chicago ∩ npa:850")


def test_a_disagreeing_npa_narrows_the_window_below_either_source(engine):
    eastern = window_mod.resolve(
        ruleset=engine.rules, jurisdiction="FL", coordinate_zone="America/New_York",
        npa="407", as_of=AS_OF)
    disagreeing = window_mod.resolve(
        ruleset=engine.rules, jurisdiction="FL", coordinate_zone="America/New_York",
        npa="415", as_of=AS_OF)      # Pacific area code, Eastern coordinate
    def span(w):
        return window_mod._minutes(w.latest) - window_mod._minutes(w.earliest)
    assert span(disagreeing) < span(eastern)
    assert disagreeing.intersected
    assert "America/Los_Angeles" in disagreeing.basis


def test_florida_is_eight_to_eight_and_texas_opens_at_nine(engine):
    fl = window_mod.resolve(ruleset=engine.rules, jurisdiction="FL",
                            coordinate_zone="America/New_York", npa="407", as_of=AS_OF)
    assert (fl.earliest, fl.latest) == ("08:00", "20:00")
    tx = window_mod.resolve(ruleset=engine.rules, jurisdiction="TX",
                            coordinate_zone="America/Chicago", npa="512", as_of=AS_OF)
    assert (tx.earliest, tx.latest) == ("09:00", "21:00")
    # Tex. Bus. & Com. Code 301.051: noon on a Sunday.
    sunday = window_mod.resolve(
        ruleset=engine.rules, jurisdiction="TX", coordinate_zone="America/Chicago",
        npa="512", as_of=AS_OF, dial_at_local=datetime(2026, 9, 6, 13, 0))
    assert sunday.day == "SUNDAY" and sunday.earliest == "12:00"


def test_dialling_outside_the_window_fires_the_code(engine, record):
    inside = engine.evaluate(record(dial_at_local=datetime(2026, 9, 1, 10, 30)))
    assert ReasonCode.OUTSIDE_CALLING_WINDOW not in inside.reason_codes
    outside = engine.evaluate(record(dial_at_local=datetime(2026, 9, 1, 21, 30)))
    assert ReasonCode.OUTSIDE_CALLING_WINDOW in outside.reason_codes
    assert outside.status == "INELIGIBLE"


def test_the_area_code_is_parsed_from_e164_not_sliced(engine):
    assert window_mod.npa_from_e164("+13015550101") == "301"
    assert window_mod.npa_from_e164("+19155550107") == "915"
    # Not a NANP country code -- there is no area code to find, so None.
    assert window_mod.npa_from_e164("+442071234567") is None
    assert window_mod.npa_from_e164("3015550101") is None      # not E.164
    assert window_mod.npa_from_e164(None) is None


def test_an_npa_absent_from_the_table_is_a_third_answer(engine):
    window = window_mod.resolve(
        ruleset=engine.rules, jurisdiction="MD",
        coordinate_zone="America/New_York", npa="999", as_of=AS_OF)
    assert not window.intersected
    assert "not in config/npa_timezone.csv" in window.basis
    assert window.zone == "America/New_York"


# ---------------------------------------------------------------------------
# 8. data quality
# ---------------------------------------------------------------------------


def test_out_of_envelope_and_snap_rejection_are_different_codes(engine, record):
    envelope = engine.evaluate(record(envelope_status="OUT_OF_ENVELOPE"))
    assert ReasonCode.COORDINATE_OUT_OF_ENVELOPE in envelope.reason_codes

    snap = engine.evaluate(record(snap_status="REJECTED_DISTANCE",
                                  snap_distance_m=144.4))
    assert ReasonCode.SNAP_DISTANCE_EXCEEDED in snap.reason_codes
    # A coordinate that resolved a zone from the polygon set has a sound
    # calling window; only its road-level attributes are unsound. Two
    # different failures, two different codes.
    assert ReasonCode.GEOCODE_TIER_INSUFFICIENT not in snap.reason_codes


@pytest.mark.parametrize("status", ["NOT_ATTEMPTED", "NOT_IN_SCOPE", "NO_GEOMETRY",
                                    "UNAVAILABLE", "SNAPPED"])
def test_an_out_of_scope_or_unavailable_snap_is_not_a_reason_code(engine, record,
                                                                  status):
    decision = engine.evaluate(record(snap_status=status, snap_distance_m=None))
    assert ReasonCode.SNAP_DISTANCE_EXCEEDED not in decision.reason_codes


@pytest.mark.parametrize("source", ["COUNTY_FALLBACK", "JURISDICTION_DEFAULT",
                                    "UNRESOLVED"])
def test_a_non_coordinate_timezone_is_an_insufficient_geocode_tier(engine, record,
                                                                   source):
    decision = engine.evaluate(record(tz_source=source))
    assert ReasonCode.GEOCODE_TIER_INSUFFICIENT in decision.reason_codes
    assert decision.status == "INELIGIBLE"


def test_no_timezone_means_no_window(engine, record):
    decision = engine.evaluate(record(tz_iana=None, tz_source="UNRESOLVED", npa=None))
    assert ReasonCode.TIMEZONE_UNRESOLVED in decision.reason_codes


# ---------------------------------------------------------------------------
# 9. the ruleset loader -- version, content hash, null windows
# ---------------------------------------------------------------------------


def _copy_ruleset(tmp_path: Path) -> Path:
    dest = tmp_path / "rules.yaml"
    shutil.copy2(config.compliance_path("ruleset_path"), dest)
    return dest


def _rehash(path: Path) -> None:
    """Recompute and stamp `content_sha256_prefix` after a deliberate edit."""
    import yaml

    doc = yaml.safe_load(path.read_text())
    digest = ruleset_mod.content_sha256(doc)
    text = path.read_text()
    old = f"content_sha256_prefix: {doc['content_sha256_prefix']}"
    path.write_text(text.replace(old, f"content_sha256_prefix: {digest[:16]}", 1))


def test_the_loader_refuses_a_version_the_build_did_not_ask_for(tmp_path):
    path = _copy_ruleset(tmp_path)
    with pytest.raises(ruleset_mod.RulesetError, match="expects"):
        ruleset_mod.load_ruleset(path, expect_version="9.9.9")


def test_the_loader_refuses_a_ruleset_that_disagrees_with_the_config(tmp_path):
    """The stale-config guard: a duplicated number is only a guard if
    disagreeing with the authority is an error rather than a preference."""
    path = _copy_ruleset(tmp_path)
    path.write_text(path.read_text().replace("max_age_days: 31", "max_age_days: 45", 1))
    path.write_text(path.read_text().replace("version: 1.0.0", "version: 1.1.0", 1))
    _rehash(path)
    with pytest.raises(ruleset_mod.RulesetError, match="DNC scrub ages out"):
        ruleset_mod.load_ruleset(path, expect_version="1.1.0")


def test_the_loader_refuses_a_rule_change_with_no_version_bump(tmp_path):
    path = _copy_ruleset(tmp_path)
    path.write_text(path.read_text().replace("max_age_days: 31", "max_age_days: 45", 1))
    with pytest.raises(ruleset_mod.RulesetError, match="content hash"):
        ruleset_mod.load_ruleset(path)


def test_the_loader_refuses_a_version_bump_with_no_rule_change(tmp_path):
    path = _copy_ruleset(tmp_path)
    path.write_text(path.read_text().replace("version: 1.0.0", "version: 1.1.0", 1))
    with pytest.raises(ruleset_mod.RulesetError, match="content hash"):
        ruleset_mod.load_ruleset(path, expect_version="1.1.0")


def test_a_null_window_row_must_name_the_rule_that_carries_it(tmp_path):
    dest = tmp_path / "blackout.csv"
    dest.write_text(
        "jurisdiction,record_type,days_from,anchor_field,legal_basis,notes\n"
        'ZZ,telephone_solicitation,,,"Some statute","no window here"\n'
    )
    with pytest.raises(ruleset_mod.RulesetError, match="reason code"):
        ruleset_mod.load_blackout(dest, known_codes=[c.value for c in ReasonCode])
    # Naming the carrier makes the same row legitimate.
    dest.write_text(
        "jurisdiction,record_type,days_from,anchor_field,legal_basis,notes\n"
        'ZZ,telephone_solicitation,,,"Some statute",'
        '"channel bar - LIVE_SOLICITATION_PROHIBITED carries it"\n'
    )
    rows = ruleset_mod.load_blackout(dest, known_codes=[c.value for c in ReasonCode])
    assert len(rows) == 1 and not rows[0].has_window


def test_a_window_row_with_no_anchor_is_refused(tmp_path):
    dest = tmp_path / "blackout.csv"
    dest.write_text(
        "jurisdiction,record_type,days_from,anchor_field,legal_basis,notes\n"
        'ZZ,written_solicitation,30,,"Some statute",\n'
    )
    with pytest.raises(ruleset_mod.RulesetError, match="anchor_field"):
        ruleset_mod.load_blackout(dest, known_codes=[c.value for c in ReasonCode])


def test_a_window_row_with_no_citation_is_refused(tmp_path):
    dest = tmp_path / "blackout.csv"
    dest.write_text(
        "jurisdiction,record_type,days_from,anchor_field,legal_basis,notes\n"
        "ZZ,written_solicitation,30,incident_date,,\n"
    )
    with pytest.raises(ruleset_mod.RulesetError, match="legal_basis"):
        ruleset_mod.load_blackout(dest, known_codes=[c.value for c in ReasonCode])


# ---------------------------------------------------------------------------
# 10. THE LIVE-DEFENCE DEMO -- "Ohio just enacted a 45-day window"
# ---------------------------------------------------------------------------


def _hash_tree(root: Path, *, skip: tuple[str, ...] = ()) -> dict[str, str]:
    return {
        str(p.relative_to(root)): hashlib.sha256(p.read_bytes()).hexdigest()
        for p in sorted(root.rglob("*"))
        if p.is_file() and "__pycache__" not in p.parts and p.name not in skip
    }


def test_ohio_45_day_window_blocks_with_no_file_under_src_changed(tmp_path, record):
    """ASSIGNMENT.md Part 9, verbatim, as a test.

    Append one row to a copy of the blackout table, bump the ruleset version,
    and a jurisdiction the engine has never heard of blocks correctly -- with
    the citation from the CSV's own column and a code derived from the row.
    The assertion that matters is the second one: `src/compliance/` is
    byte-identical before and after.
    """
    src_before = _hash_tree(REPO / "src" / "compliance")

    blackout = tmp_path / "blackout_windows.csv"
    shutil.copy2(config.compliance_path("blackout_path"), blackout)
    with blackout.open("a", encoding="utf-8", newline="") as fh:
        fh.write('OH,written_solicitation,45,incident_date,'
                 '"Ohio R. Prof. Cond. 7.3(b)(3)",\n')

    rules = _copy_ruleset(tmp_path)
    rules.write_text(rules.read_text().replace("version: 1.0.0", "version: 1.1.0", 1))
    _rehash(rules)

    engine = EligibilityEngine(ruleset_path=rules, ruleset_version="1.1.0",
                               blackout_path=blackout, as_of=AS_OF)
    incident = AS_OF - timedelta(days=10)
    decision = engine.evaluate(record(
        jurisdiction="OH", incident_date=incident,
        report_filing_date=incident, npa=None, tz_iana="America/New_York",
        snap_status=leads_mod.SNAP_NOT_IN_SCOPE, snap_distance_m=None,
    ))
    assert decision.status == "BLOCKED_UNTIL"
    assert decision.blocked_until_date == incident + timedelta(days=46)
    assert "OH_WRITTEN_SOLICITATION_45D" in decision.reason_codes
    basis = decision.legal_basis[decision.reason_codes.index(
        "OH_WRITTEN_SOLICITATION_45D")]
    assert "Ohio R. Prof. Cond. 7.3(b)(3)" in basis
    assert decision.ruleset_version == "1.1.0"

    assert _hash_tree(REPO / "src" / "compliance") == src_before, (
        "adding a jurisdiction changed a file under src/compliance/"
    )


def test_adding_ohio_does_not_move_any_existing_decision(tmp_path, record):
    """The locality claim: a new row touches only the records it reaches."""
    payloads = [
        dict(jurisdiction="MD", line_type="wireless"),
        dict(jurisdiction="TX", npa="512", tz_iana="America/Chicago",
             incident_date=date(2026, 1, 1), report_filing_date=date(2026, 1, 2),
             snap_status=leads_mod.SNAP_NOT_IN_SCOPE, snap_distance_m=None),
        dict(jurisdiction="FL", npa="407", incident_date=date(2026, 1, 1),
             report_filing_date=date(2026, 1, 2),
             snap_status=leads_mod.SNAP_NOT_IN_SCOPE, snap_distance_m=None),
    ]
    before = [EligibilityEngine(as_of=AS_OF).evaluate(record(**p)) for p in payloads]

    blackout = tmp_path / "blackout_windows.csv"
    shutil.copy2(config.compliance_path("blackout_path"), blackout)
    with blackout.open("a", encoding="utf-8", newline="") as fh:
        fh.write('OH,written_solicitation,45,incident_date,'
                 '"Ohio R. Prof. Cond. 7.3(b)(3)",\n')
    rules = _copy_ruleset(tmp_path)
    rules.write_text(rules.read_text().replace("version: 1.0.0", "version: 1.1.0", 1))
    _rehash(rules)
    engine = EligibilityEngine(ruleset_path=rules, ruleset_version="1.1.0",
                               blackout_path=blackout, as_of=AS_OF)
    after = [engine.evaluate(record(**p)) for p in payloads]

    for b, a in zip(before, after):
        assert (b.status, b.blocked_until_date, b.reason_codes) == \
               (a.status, a.blocked_until_date, a.reason_codes)
        # The ids DO move, because the blackout table's hash is in every id.
        # That is correct: the ruleset that produced the decision changed.
        assert b.decision_lineage_id != a.decision_lineage_id


# ---------------------------------------------------------------------------
# 11. lineage
# ---------------------------------------------------------------------------


def test_the_same_record_twice_is_one_lineage_row(engine, record, tmp_path):
    store = lineage_mod.LineageStore.open(tmp_path / "lineage.parquet")
    payload = record()
    first = engine.evaluate(payload)
    second = engine.evaluate(payload)
    assert first.decision_lineage_id == second.decision_lineage_id
    store.add(first.lineage_row)
    store.add(second.lineage_row)
    assert store.total == 1 and store.appended == 1 and store.reused == 1


def test_a_changed_input_makes_a_new_id_and_leaves_the_old_row_alone(engine, record,
                                                                     tmp_path):
    store = lineage_mod.LineageStore.open(tmp_path / "lineage.parquet")
    original = engine.evaluate(record())
    store.add(original.lineage_row)
    changed = engine.evaluate(record(on_national_dnc=True))
    store.add(changed.lineage_row)
    assert original.decision_lineage_id != changed.decision_lineage_id
    assert store.total == 2
    frame = store.frame()
    kept = frame[frame.decision_lineage_id == original.decision_lineage_id].iloc[0]
    assert kept.eligibility_status == original.status


def test_writing_a_different_payload_under_an_existing_id_raises(engine, record,
                                                                 tmp_path):
    store = lineage_mod.LineageStore.open(tmp_path / "lineage.parquet")
    decision = engine.evaluate(record())
    store.add(decision.lineage_row)
    tampered = dict(decision.lineage_row)
    tampered["eligibility_status"] = "ELIGIBLE" \
        if decision.status != "ELIGIBLE" else "INELIGIBLE"
    with pytest.raises(lineage_mod.LineageConflict, match="already exists"):
        store.add(tampered)


def test_the_lineage_snapshot_carries_no_direct_identifiers(engine, record):
    decision = engine.evaluate(record())
    snapshot = json.loads(decision.lineage_row["input_snapshot_json"])
    for forbidden in lineage_mod.FORBIDDEN_KEYS:
        assert forbidden not in snapshot
    assert snapshot["party_token"].startswith("pt_")


def test_a_changed_rule_gives_every_decision_a_new_id(tmp_path, record):
    rules = _copy_ruleset(tmp_path)
    # The EBR window, deliberately: it is a real legal parameter that is NOT
    # mirrored in config/compliance.toml, so the cross-check does not fire and
    # what is being tested here is the lineage id rather than the guard. The
    # guard has its own test below.
    rules.write_text(rules.read_text().replace("        months: 18",
                                               "        months: 24", 1))
    rules.write_text(rules.read_text().replace("version: 1.0.0", "version: 1.1.0", 1))
    _rehash(rules)
    base = EligibilityEngine(as_of=AS_OF).evaluate(record())
    edited = EligibilityEngine(ruleset_path=rules, ruleset_version="1.1.0",
                               as_of=AS_OF).evaluate(record())
    assert base.decision_lineage_id != edited.decision_lineage_id
    assert edited.ruleset_version == "1.1.0"


# ---------------------------------------------------------------------------
# 12. the vault
# ---------------------------------------------------------------------------


def test_tokens_are_stable_with_a_key_and_differ_without_it(tmp_path):
    one = vault_mod.Vault(root=tmp_path / "a", as_of=AS_OF, key=b"key-one")
    two = vault_mod.Vault(root=tmp_path / "b", as_of=AS_OF, key=b"key-one")
    other = vault_mod.Vault(root=tmp_path / "c", as_of=AS_OF, key=b"key-two")
    assert one.party_token("P001") == two.party_token("P001")
    assert one.party_token("P001") != other.party_token("P001")
    assert one.party_token("P001") != one.party_token("P002")
    assert one.party_token(None) is None
    assert one.party_token("P001").startswith("pt_")
    assert one.phone_token("+13015550101").startswith("ph_")


def test_the_analytic_view_carries_no_direct_identifier(tmp_path):
    vault = vault_mod.Vault(root=tmp_path / "vault", as_of=AS_OF, build_sha="test")
    vault_mod.load_fixture_into_vault(
        vault, REPO / "fixtures" / "synthetic_parties.csv",
        actor="test", purpose="unit test")
    view = vault.analytic_view(actor="test", purpose="unit test")
    for column in vault_mod.DIRECT_IDENTIFIERS:
        assert column not in view.columns
    assert "zip5" in view.columns          # 18 U.S.C. 2725(3), the carve-out
    assert set(view.columns) <= set(vault_mod.ANALYTIC_COLUMNS)


def test_every_vault_read_appends_an_access_log_row(tmp_path):
    vault = vault_mod.Vault(root=tmp_path / "vault", as_of=AS_OF, build_sha="test")
    vault_mod.load_fixture_into_vault(
        vault, REPO / "fixtures" / "synthetic_parties.csv",
        actor="loader", purpose="load")
    before = vault.access_count
    vault.analytic_view(actor="analyst", purpose="eligibility evaluation")
    vault.read(vault_mod.PARTIES, actor="analyst", purpose="second look")
    assert vault.access_count == before + 2
    frame = vault.access_frame()
    assert set(frame["actor"]) >= {"analyst"}
    assert all(frame["purpose"].str.len() > 0)
    # 18 U.S.C. 2721(c) wants who, what and when -- frozen, so two identical
    # builds write identical audit partitions.
    assert set(frame["read_at"]) == {
        datetime.combine(AS_OF, time.min, tzinfo=timezone.utc)}
    path = vault.flush_access_log()
    assert path.exists() and "test" in path.name


def test_the_vault_refuses_a_non_total_sort_key(tmp_path):
    vault = vault_mod.Vault(root=tmp_path / "vault", as_of=AS_OF)
    frame = pd.DataFrame({"party_token": ["a", "a"], "x": [1, 2]})
    with pytest.raises(vault_mod.VaultError, match="not total"):
        vault.write("t", frame, order_by=["party_token"])


def test_an_append_only_table_never_rewrites_a_stored_row(tmp_path):
    vault = vault_mod.Vault(root=tmp_path / "vault", as_of=AS_OF)
    vault.append("t", [{"party_token": "a", "x": 1}], order_by=["party_token"])
    vault.append("t", [{"party_token": "a", "x": 2},
                       {"party_token": "b", "x": 3}], order_by=["party_token"])
    stored = pd.read_parquet(vault.path("t")).set_index("party_token")["x"].to_dict()
    assert stored == {"a": 1, "b": 3}


# ---------------------------------------------------------------------------
# 13. the Florida incompleteness monitor
# ---------------------------------------------------------------------------


def test_rows_filed_inside_the_trailing_sixty_days_are_labelled(engine):
    monitor = fl_mod.Monitor.from_rules(engine.rules)
    assert monitor.trailing_days == 60
    frame = pd.DataFrame({
        "jurisdiction": ["FL", "FL", "FL", "MD"],
        "report_filing_date": ["2026-08-20", "2026-05-01", None, "2026-08-20"],
    })
    labelled = monitor.label_rows(frame, AS_OF)
    labels = list(labelled[fl_mod.LABEL_COLUMN])
    assert labels[0] == MonitoringLabel.FL_STRUCTURALLY_INCOMPLETE_WINDOW.value
    assert labels[1] is None                       # outside the window
    assert labels[2] == MonitoringLabel.NOT_APPLICABLE.value   # the FARS case
    assert labels[3] is None                       # not Florida
    assert labelled.loc[2, fl_mod.REASON_COLUMN]   # the reason is recorded


def test_a_trailing_florida_aggregate_is_refused_unless_annotated(engine):
    monitor = fl_mod.Monitor.from_rules(engine.rules)
    frame = pd.DataFrame({"jurisdiction": ["FL"], "report_filing_date": ["2026-08-20"]})
    with pytest.raises(fl_mod.IncompleteWindowRefused, match="316.066"):
        monitor.trailing_aggregate(frame, as_of=AS_OF, aggregate_days=30)
    annotated = monitor.trailing_aggregate(frame, as_of=AS_OF, aggregate_days=30,
                                           annotate=True)
    assert annotated["overlap_days"] == 30
    assert annotated["warning"]


def test_the_monitoring_label_is_not_a_reason_code():
    """A data-quality observation must not leak into a legal exclusion count."""
    codes = {c.value for c in ReasonCode}
    for label in MonitoringLabel:
        assert label.value not in codes


# ---------------------------------------------------------------------------
# 14. retention
# ---------------------------------------------------------------------------


def test_retention_reports_and_deletes_nothing(engine):
    frame = pd.DataFrame({
        "party_token": ["a", "b"],
        "received_at": ["2015-01-01T00:00:00+00:00", "2026-01-01T00:00:00+00:00"],
    })
    findings = retention_mod.report(engine.rules, {"revocations": frame}, as_of=AS_OF)
    revocations = next(f for f in findings if f.record_class == "revocations")
    assert revocations.ttl_years == 5
    assert revocations.rows_total == 2 and revocations.rows_expired == 1
    assert revocations.legal_basis
    payload = retention_mod.as_manifest(findings)
    assert payload["deletes_nothing"] is True
    # The file is untouched: reporting is not deleting.
    assert len(frame) == 2


# ---------------------------------------------------------------------------
# 15. the fixture golden -- all 40 rows, end to end
# ---------------------------------------------------------------------------


def _golden() -> dict[str, dict[str, str]]:
    with GOLDEN.open(newline="", encoding="utf-8") as fh:
        return {row["party_id"]: row for row in csv.DictReader(fh)}


def test_all_forty_fixture_rows_match_the_committed_golden(lead_rows):
    """(party_id -> status, blocked_until_date, reason_codes), end to end.

    Rows whose disposition depends on the road snap are compared only when the
    snap actually ran. `snap_status = UNAVAILABLE` (no OSM extract on this
    machine) is NOT a reason code, so on a box without the 214 MB Maryland PBF
    those rows land ELIGIBLE and the golden's snap-dependent rows are skipped
    with that stated rather than silently passing.
    """
    golden = _golden()
    assert len(lead_rows) == 40 == len(golden)
    snap_ran = "UNAVAILABLE" not in set(lead_rows["snap_status"])
    skipped: list[str] = []
    for row in lead_rows.to_dict("records"):
        label = row["party_id_label"]
        expected = golden[label]
        if expected["requires_snap"] == "true" and not snap_ran:
            skipped.append(label)
            continue
        assert row["eligibility_status"] == expected["status"], label
        actual_until = "" if row["blocked_until_date"] is None \
            else str(row["blocked_until_date"])
        assert actual_until == expected["blocked_until_date"], label
        assert "|".join(row["reason_codes"]) == expected["reason_codes"], label
    if skipped:
        pytest.skip(f"road snap unavailable; {len(skipped)} snap-dependent rows "
                    f"not compared: {skipped}")


@pytest.mark.parametrize("label,expected_code", [
    ("P013", ReasonCode.RND_NO_DATA_NO_SAFE_HARBOR),   # "No Data" is not a green light
    ("P014", ReasonCode.RND_REASSIGNED),                # only "No" is a safe harbour
    ("P016", ReasonCode.DNC_SCRUB_STALE),               # 45-day scrub
    ("P003", ReasonCode.CONSENT_REVOKED),
    ("P018", ReasonCode.CONSENT_REVOKED),
    ("P005", ReasonCode.LINE_TYPE_VOIP_RESTRICTED),
    ("P029", ReasonCode.LINE_TYPE_VOIP_RESTRICTED),
    ("P006", ReasonCode.LINE_TYPE_UNRESOLVED),
    ("P019", ReasonCode.COORDINATE_OUT_OF_ENVELOPE),
])
def test_the_named_fixture_rows_carry_the_codes_they_exist_to_prove(lead_rows, label,
                                                                    expected_code):
    row = lead_rows[lead_rows.party_id_label == label].iloc[0]
    codes = set(row["reason_codes"])
    if expected_code is None:
        return
    assert expected_code.value in codes, (label, sorted(codes))
    assert row["eligibility_status"] != "ELIGIBLE" or \
        engine_disposition_is_note(expected_code)


def engine_disposition_is_note(code) -> bool:
    return ruleset_mod.default_ruleset().disposition(code.value) == ruleset_mod.NOTE


def test_p014_reassigned_number_loses_the_safe_harbour(lead_rows):
    """P014's RND response is "YES" and it must not be contactable.

    This row is why `rules.yaml` quotes `rnd_response: ["YES"]`. YAML 1.1
    parses a bare YES as the boolean true, so the unquoted version silently
    compared `True` against the string "YES", the rule never matched, and
    every reassigned number in the corpus passed the gate as though the
    database had answered "No". The test that found it is
    `test_rnd_has_three_states_and_a_silence`; this one holds the line on the
    fixture row it was hiding.
    """
    row = lead_rows[lead_rows.party_id_label == "P014"].iloc[0]
    assert row["contact"]["rnd_response"] == "YES"
    assert ReasonCode.RND_REASSIGNED.value in set(row["reason_codes"])
    assert row["eligibility_status"] == "INELIGIBLE"


def test_the_two_trap_rows_resolve_from_coordinates(lead_rows):
    p007 = lead_rows[lead_rows.party_id_label == "P007"].iloc[0]
    assert p007["geo"]["iana_timezone"] == "America/Denver"
    assert p007["contact"]["calling_window_local"]["basis"].startswith("coords:")
    assert p007["contact"]["calling_window_local"]["earliest"] == "09:00"

    p008 = lead_rows[lead_rows.party_id_label == "P008"].iloc[0]
    assert p008["geo"]["iana_timezone"] == "America/Chicago"
    basis = p008["contact"]["calling_window_local"]["basis"]
    assert basis.startswith("coords:America/Chicago ∩ npa:850")
    assert p008["contact"]["calling_window_local"]["latest"] == "19:00"


def test_texas_and_florida_rows_are_not_penalised_for_an_out_of_scope_snap(lead_rows):
    out_of_scope = lead_rows[lead_rows.jurisdiction.isin(["TX", "FL"])]
    assert len(out_of_scope) > 0
    assert set(out_of_scope["snap_status"]) == {leads_mod.SNAP_NOT_IN_SCOPE}
    for row in out_of_scope.to_dict("records"):
        assert ReasonCode.SNAP_DISTANCE_EXCEEDED.value not in set(row["reason_codes"])


# ---------------------------------------------------------------------------
# 16. the build: contracts, PII, determinism, the crash-only answer
# ---------------------------------------------------------------------------


def test_every_lead_row_validates_against_the_output_contract(lead_rows,
                                                              compliance_root):
    sample = build_mod.read_sample_csv(
        Path(compliance_root["_dest"]) / "sample_leads.csv")
    assert len(sample) == 40
    results = build_mod.validate_lead_rows(sample)
    assert all(r["valid"] for r in results), [r for r in results if not r["valid"]]


def test_a_row_claiming_eligible_with_no_affirmative_code_fails_the_contract(tmp_path):
    """Named table, named column -- the contract has to say what broke."""
    import pyarrow as pa

    from src.transform import common as c

    bad = {name: None for name in build_mod.LEADS_SCHEMA.names}
    bad.update({
        "lead_id": "LD_" + "0" * 16, "source_system": "OTHER",
        "source_record_id": "x", "ingested_at": datetime(2026, 9, 1, tzinfo=timezone.utc),
        "incident_date": date(2026, 1, 1), "jurisdiction": "MD",
        "eligibility_status": "ELIGIBLE",
        "reason_codes": ["DNC_LISTED"], "legal_basis": ["16 C.F.R. 310.4"],
        "decision_lineage_id": "a" * 64,
        "evaluated_at": datetime(2026, 9, 1, tzinfo=timezone.utc),
        "ruleset_version": "1.0.0", "party_token": "pt_" + "0" * 32,
        "match_method": "NO_MATCH",
        "identity_provenance": "SYNTHETIC_FIXTURE",
        "_compliance_build_sha": "b" * 64, "ruleset_sha256": "c" * 64,
        "blackout_sha256": "d" * 64,
        "geo": {}, "contact": {}, "consent": None,
    })
    table = pa.Table.from_pylist([bad], schema=build_mod.LEADS_SCHEMA)
    con = c.connect()
    try:
        con.register("v_bad", table)
        violations = contracts.validate_relation(
            con, "v_bad", contracts.load_contract(contracts.COMPLIANCE_CONTRACT),
            "compliance.leads", check_row_count_min=False)
    finally:
        con.close()
    kinds = [v.kind for v in violations]
    assert "row-rule:eligible_requires_affirmative_code" in kinds
    offender = next(v for v in violations
                    if v.kind == "row-rule:eligible_requires_affirmative_code")
    assert offender.table == "compliance.leads"
    assert offender.count == 1


def test_no_fixture_identifier_reaches_gold_or_the_committed_output(compliance_root):
    """The boundary, asserted over bytes rather than over intentions."""
    with (REPO / "fixtures" / "synthetic_parties.csv").open(newline="") as fh:
        rows = list(csv.DictReader(fh))
    needles = set()
    for row in rows:
        needles |= {row["full_name"], row["street_address"], row["phone_e164"],
                    row["phone_e164"].lstrip("+1")}
    needles = {n.encode() for n in needles if n}

    roots = [Path(compliance_root["out_root"]), REPO / "output"]
    scanned = 0
    for root in roots:
        for path in root.rglob("*"):
            if not path.is_file():
                continue
            scanned += 1
            blob = path.read_bytes()
            hits = [n.decode() for n in needles if n in blob]
            assert not hits, f"{path} contains {hits}"
    assert scanned > 0


def test_two_builds_are_byte_identical_and_append_no_lineage(local_gold, tmp_path):
    def run(n: int):
        return build_mod.build_compliance(
            gold_root=local_gold, out_root=tmp_path / "out",
            vault_dir=tmp_path / "vault",
            sample_path=tmp_path / f"sample_{n}.csv",
            schema_check_path=tmp_path / f"check_{n}.json",
            as_of=AS_OF, crash_only_limit=50, small_corpus=True,
        )
    # The manifest is excluded BY CONSTRUCTION, not by convenience: it is the
    # one artefact that carries wall-clock time, exactly as Phases 2-5 do, and
    # its every other value is asserted equal below.
    manifest_name = "_compliance_manifest.json"
    first = run(1)
    hashes_one = _hash_tree(tmp_path / "out", skip=(manifest_name,))
    second = run(2)
    hashes_two = _hash_tree(tmp_path / "out", skip=(manifest_name,))

    assert hashes_one == hashes_two
    assert set(hashes_one) >= {"leads.parquet", "decision_lineage.parquet",
                               "exclusion_by_code.parquet"}
    assert second["stats"]["lineage"]["appended"] == 0
    assert second["stats"]["lineage"]["reused"] == first["stats"]["lineage"]["rows_total"]
    assert (tmp_path / "sample_1.csv").read_bytes() == \
           (tmp_path / "sample_2.csv").read_bytes()
    assert first["compliance_build_sha"] == second["compliance_build_sha"]

    # Identical manifest values EXCEPT built_at, and the output hashes it
    # records, which name the sample paths the two runs were given.
    assert first["built_at"] != second["built_at"]
    def comparable(result):
        stats = {k: v for k, v in result["stats"].items() if k != "lineage"}
        return {k: (stats if k == "stats" else v)
                for k, v in result.items() if k not in ("built_at", "outputs")}
    one, two = comparable(first), comparable(second)
    assert json.dumps(one, sort_keys=True, default=str) == \
           json.dumps(two, sort_keys=True, default=str)
    for table in ("leads", "decision_lineage", "exclusion_by_code",
                  "crash_only_decisions"):
        assert first["outputs"][table]["sha256"] == second["outputs"][table]["sha256"]


def test_the_crash_only_run_is_the_honest_answer(compliance_root):
    """No identity layer, no eligible record -- in any jurisdiction."""
    stats = compliance_root["stats"]["crash_only"]
    assert stats["attempted"]
    assert stats["eligible_total"] == 0
    assert set(stats["by_jurisdiction_status"]) == {"MD", "TX", "FL"}
    for jurisdiction, by_status in stats["by_jurisdiction_status"].items():
        assert by_status.get("ELIGIBLE", 0) == 0, jurisdiction


def test_the_exclusion_table_covers_both_runs(compliance_root):
    frame = pd.read_parquet(
        Path(compliance_root["out_root"]) / "exclusion_by_code.parquet")
    assert set(frame["run"]) == {build_mod.RUN_FIXTURE, build_mod.RUN_CRASH_ONLY}
    assert set(frame["disposition"]) <= {
        "BAR", "HOLD_UNTIL_DATE", "HOLD_UNTIL_REFRESH", "NOTE", "AFFIRMATIVE"}
    assert (frame["records"] > 0).all()
    fixture_total = frame[frame.run == build_mod.RUN_FIXTURE]["records"].sum()
    assert fixture_total >= 40


def test_the_manifest_records_what_the_memo_has_to_quote(compliance_root):
    stats = compliance_root["stats"]
    inputs = compliance_root["inputs"]
    assert inputs["ruleset"]["version"] == "1.0.0"
    assert len(inputs["ruleset"]["sha256"]) == 64
    assert len(inputs["blackout"]["sha256"]) == 64
    assert inputs["fixture"]["sha256"]
    assert stats["fixture"]["by_status"]
    assert stats["fixture"]["by_reason_code"]
    assert set(stats["traps"]) == {"P007", "P008"}
    assert stats["crash_match"]["attempted"]
    assert stats["vault"]["access_rows"] > 0
    assert stats["retention"]["deletes_nothing"] is True
    assert stats["fl_incompleteness"]["trailing_days"] == 60
    # The fixture carries no line-type resolution date. Named, not hidden.
    assert stats["fixture"]["line_type_asof_missing"] == 40


def test_the_score_hook_receives_only_actionable_records(lead_rows):
    rows = [
        {**row, "geo": dict(row["geo"]), "contact": dict(row["contact"])}
        for row in lead_rows.to_dict("records")
    ]
    handed = leads_mod.score_inputs(rows)
    statuses = {r["eligibility_status"] for r in handed}
    assert statuses <= {"ELIGIBLE", "BLOCKED_UNTIL"}
    assert "INELIGIBLE" not in statuses
    assert all(r["calling_window_local"] is not None for r in handed)


# ---------------------------------------------------------------------------
# 17. the greps the assignment names by hand
# ---------------------------------------------------------------------------


def _sources(root: Path):
    return [p for p in root.rglob("*.py") if "__pycache__" not in p.parts]


def test_no_web_mercator_anywhere_in_the_compliance_layer():
    """3857 is for tiles. Never for distance, buffer or area."""
    for path in _sources(REPO / "src" / "compliance"):
        text = path.read_text(encoding="utf-8")
        for lineno, line in enumerate(text.splitlines(), 1):
            assert "3857" not in line, f"{path}:{lineno}: {line.strip()}"


def test_no_area_code_is_taken_by_slicing_a_phone_number():
    """A pipeline that computes a calling window from SUBSTR(phone,1,3) fails 5c."""
    import re

    banned = re.compile(r"phone\s*\[\s*:?\s*3\s*\]|SUBSTR\s*\(\s*phone", re.IGNORECASE)
    # No exemption for comments or docstrings, deliberately. A reviewer runs
    # this grep by hand and it does not read prose, so even a docstring
    # WARNING against the pattern would produce a hit and cost a moment of
    # doubt. src/compliance/window.py paraphrases the assignment's sentence
    # rather than quoting the literal, and says why.
    for path in _sources(REPO / "src"):
        for lineno, line in enumerate(path.read_text(encoding="utf-8").splitlines(), 1):
            assert not banned.search(line), f"{path}:{lineno}: {line.strip()}"


def test_no_jurisdiction_is_named_in_a_compliance_branch():
    """A state named in code is a state that needs a deploy.

    The law lives in `rules.yaml` and `blackout_windows.csv`. Code may MENTION
    a jurisdiction in a comment or a docstring -- that is how a reader learns
    why a rule exists -- but never in an executable line.
    """
    import ast
    import re

    named = re.compile(r"""["'](MD|TX|FL|OH)["']""")
    offenders: list[str] = []
    for path in _sources(REPO / "src" / "compliance"):
        tree = ast.parse(path.read_text(encoding="utf-8"))
        docstrings = set()
        for node in ast.walk(tree):
            if isinstance(node, (ast.Module, ast.ClassDef, ast.FunctionDef,
                                 ast.AsyncFunctionDef)):
                doc = ast.get_docstring(node, clean=False)
                if doc:
                    docstrings.add(doc)
        for node in ast.walk(tree):
            if isinstance(node, ast.Constant) and isinstance(node.value, str):
                if node.value in docstrings or "\n" in node.value:
                    continue
                if named.match(f'"{node.value}"'):
                    offenders.append(f"{path.name}:{node.lineno}: {node.value!r}")
    assert not offenders, offenders


def test_the_committed_sample_is_forty_contract_valid_rows():
    sample = build_mod.read_sample_csv(REPO / "output" / "sample_leads.csv")
    assert len(sample) == 40
    results = build_mod.validate_lead_rows(sample)
    assert all(r["valid"] for r in results), [r for r in results if not r["valid"]]
    check = json.loads((REPO / "output" / "sample_leads.schema_check.json").read_text())
    assert check["rows"] == 40 and check["invalid"] == 0
    for row in sample:
        assert row["reason_codes"], row["lead_id"]
        assert len(row["legal_basis"]) == len(row["reason_codes"])
        if row["eligibility_status"] == "ELIGIBLE":
            assert any(c.startswith("ELIGIBLE_") for c in row["reason_codes"])


def test_config_and_the_ruleset_agree_on_every_duplicated_number(engine):
    cfg = config.compliance()
    doc = engine.rules.doc
    assert doc["window_arithmetic"] == cfg["window_arithmetic"]
    dnc = next(r["params"]["max_age_days"] for r in doc["channel_gates"]["dnc"]
               if "max_age_days" in r["params"])
    assert int(dnc) == int(cfg["dnc_max_age_days"])
    assert int(doc["consent"]["revocation"]["honour_business_days"]) == \
        int(cfg["revocation_honour_business_days"])
    assert doc["version"] == cfg["ruleset_version"]
