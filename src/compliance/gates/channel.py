"""Contact-channel gates: DNC, internal DNC, EBR, RND, line type, window, consent.

ASSIGNMENT.md 5c, in one module because these gates share one subject -- the
telephone number -- and because the routing decision for a line type feeds the
consent requirement, which feeds the affirmative basis. Splitting them would
mean passing derived state between files for no gain.

Four things here are the ones this section is actually testing:

  **DNC is a freshness SLA, not a load.** `DNC_SCRUB_STALE` fires when the
  scrub is older than 31 days EVEN IF THE NUMBER IS NOT LISTED. What the 31
  days buys is the 16 C.F.R. 310.4(b)(3)(iv) safe harbour, and an aged scrub
  loses it whatever the last answer was. A missing scrub date is stale too:
  `missing_is_stale` in `rules.yaml`, failing closed.

  **The RND has three recorded states and a fourth for silence.** The
  64.1200(m) safe harbour attaches ONLY to "No". "No Data" is not a green
  light -- it is the database saying it cannot answer -- and "we never asked"
  is a different fact again. Three codes, one silence, no collapsing.

  **`voip` and `unknown` route to the MOST restrictive row.** The routing
  table is data (`rules.yaml` `channel_gates.line_type_routing`) so a test can
  assert the routing OUTCOME directly rather than inferring it from a status:
  the voip row's tier and consent requirements must equal the wireless row's,
  and must not equal the landline row's.

  **Consent is evidence, not a boolean.** `consent_on_file = true` with an
  incomplete provenance record is `CONSENT_UNVERIFIABLE`, because the burden
  of proving consent is on the caller. Revocation trumps a consent on file.
"""

from __future__ import annotations

from datetime import date, datetime, time
from typing import Any

from .. import window as window_mod
from ..consent import ConsentProvenance, is_revoked
from ..ruleset import Ruleset
from . import Finding, finding_from_rule

# 30.44 days: the mean Gregorian month. Used only to turn an EBR window
# expressed in months into a day count. An 18-month EBR boundary is not a
# date anyone litigates to the day, and a calendar-month walk would make the
# answer depend on which month the transaction fell in.
DAYS_PER_MONTH = 30.44


# ---------------------------------------------------------------------------
# line type routing -- consulted before the gates, because consent depends on it
# ---------------------------------------------------------------------------


def routing_row(rules: Ruleset, line_type: Any) -> dict[str, Any]:
    """The routing table row for a line type. Never returns the permissive one.

    An unrecognised value falls through to the `null` row, which carries the
    most restrictive treatment. That is the direction ASSIGNMENT.md 5c names:
    "Route voip and unknown to the most restrictive treatment, never the most
    permissive." A line type nobody anticipated is exactly the case where the
    default has to fail closed.
    """
    table = rules.line_type_routing
    wanted = None if line_type is None else str(line_type)
    fallback: dict[str, Any] | None = None
    for row in table["rows"]:
        if row.get("line_type") is None:
            fallback = row
        if row.get("line_type") == wanted:
            return row
    if fallback is None:  # pragma: no cover - the ruleset always declares one
        raise KeyError("line_type_routing has no null row to fall back to")
    return fallback


def routing_outcome(rules: Ruleset, line_type: Any) -> dict[str, Any]:
    """The comparable part of a routing row -- what a test asserts equality on."""
    row = routing_row(rules, line_type)
    return {
        "tier": row["tier"],
        "requires_prior_express_consent": row["requires_prior_express_consent"],
        "written_consent_required_for_autodial": row[
            "written_consent_required_for_autodial"],
        "manual_dial_permitted": row["manual_dial_permitted"],
    }


# ---------------------------------------------------------------------------
# consent
# ---------------------------------------------------------------------------


def evaluate_consent(
    record: dict[str, Any], rules: Ruleset, as_of: date
) -> tuple[list[Finding], bool]:
    """Consent findings, and whether a valid unrevoked consent exists.

    Returns the flag as well as the findings because `consent_valid` is an
    INPUT to two other families -- the fixture's source gate and every
    live-solicitation rule -- and computing it twice in two places is how the
    two would eventually disagree.
    """
    spec = rules.consent_spec
    required = list(spec["required_provenance_fields"])
    provenance: ConsentProvenance | None = record.get("consent_provenance")

    local = dict(record)
    local["consent_provenance_complete"] = (
        provenance.is_complete(required) if provenance is not None else False
    )
    local["consent_form"] = (
        provenance.consent_form if provenance is not None else record.get("consent_form")
    )
    local["circuit"] = rules.circuit(record.get("jurisdiction"))

    findings: list[Finding] = []
    for rule in rules.in_force(rules.consent_rules, as_of):
        if not rule.matches(local):
            continue
        if not _circuit_applies(rule, local.get("circuit")):
            continue
        if rule.params.get("accepted_consent_form") is not None:
            accepted = [str(f) for f in rule.params["accepted_consent_form"]]
            if str(local.get("consent_form")) in accepted:
                continue
        if rule.params.get("max_sellers_named") is not None:
            named = provenance.sellers_named if provenance else []
            if len(named) <= int(rule.params["max_sellers_named"]):
                continue
        findings.append(finding_from_rule(
            rule,
            detail=_consent_detail(rule, local, provenance, required),
        ))

    valid = not findings
    local["consent_valid"] = valid
    if valid:
        for rule in rules.in_force(rules.consent_affirmative, as_of):
            if rule.matches(local):
                findings.append(finding_from_rule(
                    rule,
                    detail=(
                        f"provenance_kind="
                        f"{provenance.provenance_kind if provenance else 'none'} "
                        f"form={local.get('consent_form')}"
                    ),
                ))
    return findings, valid


def _circuit_applies(rule, circuit: str | None) -> bool:
    """A circuit-scoped rule does not bind in an excluded circuit.

    This is ASSIGNMENT.md 5d(2) as three lines of data: `scope: circuit` plus
    `excluded_circuits: ["5th"]` is *Bradford* absorbed without a rewrite.
    """
    if str(rule.raw.get("scope", "national")) != "circuit":
        return True
    excluded = [str(c) for c in (rule.raw.get("excluded_circuits") or [])]
    return str(circuit) not in excluded


def _consent_detail(rule, local, provenance, required) -> str:
    if rule.reason_code == "CONSENT_UNVERIFIABLE":
        missing = provenance.missing(required) if provenance else list(required)
        return f"provenance missing {missing}"
    if rule.reason_code == "CONSENT_FORM_NOT_WRITTEN":
        return (f"consent_form={local.get('consent_form')} in circuit "
                f"{local.get('circuit')}")
    return (f"consent_on_file={local.get('consent_on_file')} "
            f"consent_revoked={local.get('consent_revoked')}")


# ---------------------------------------------------------------------------
# the rest of the channel stack
# ---------------------------------------------------------------------------


def evaluate(record: dict[str, Any], rules: Ruleset, as_of: date) -> list[Finding]:
    out: list[Finding] = []
    out += _dnc(record, rules, as_of)
    out += _internal_dnc(record, rules, as_of)
    out += _rnd(record, rules, as_of)
    out += _line_type(record, rules, as_of)
    out += _calling_window(record, rules, as_of)
    out += _ebr(record, rules, as_of)
    return out


def _dnc(record, rules: Ruleset, as_of: date) -> list[Finding]:
    out: list[Finding] = []
    for rule in rules.in_force(rules.dnc, as_of):
        max_age = rule.params.get("max_age_days")
        if max_age is None:
            if rule.matches(record):
                out.append(finding_from_rule(
                    rule, detail=f"on_national_dnc={record.get('on_national_dnc')}"))
            continue
        age = record.get("dnc_scrub_age_days")
        missing_is_stale = bool(rule.params.get("missing_is_stale", True))
        if age is None:
            if missing_is_stale:
                out.append(finding_from_rule(
                    rule, detail="no dnc_scrub_asof recorded (missing_is_stale)"))
            continue
        if int(age) > int(max_age):
            out.append(finding_from_rule(
                rule,
                detail=(f"scrub is {int(age)} days old, limit {int(max_age)}; "
                        f"fires whether or not the number is listed"),
            ))
    return out


def _internal_dnc(record, rules: Ruleset, as_of: date) -> list[Finding]:
    if not record.get("internal_dnc_listed"):
        return []
    return [
        finding_from_rule(rule, detail="party_token present in vault.internal_dnc")
        for rule in rules.in_force(rules.internal_dnc, as_of)
    ]


def _rnd(record, rules: Ruleset, as_of: date) -> list[Finding]:
    return [
        finding_from_rule(rule, detail=f"rnd_response={record.get('rnd_response')!r}")
        for rule in rules.in_force(rules.rnd, as_of)
        if rule.matches(record)
    ]


def _line_type(record, rules: Ruleset, as_of: date) -> list[Finding]:
    out: list[Finding] = []
    row = routing_row(rules, record.get("line_type"))
    code = row.get("reason_code")
    if code:
        out.append(Finding(
            reason_code=str(code),
            legal_basis=" ".join(str(row["legal_basis"]).split()),
            detail=(f"line_type={record.get('line_type')!r} routed to tier "
                    f"{row['tier']} (most restrictive is "
                    f"{rules.line_type_routing['most_restrictive_tier']})"),
            rule_id=f"LINE_TYPE_ROUTING:{row.get('line_type')}",
            params={k: row[k] for k in
                    ("tier", "requires_prior_express_consent",
                     "written_consent_required_for_autodial", "manual_dial_permitted")},
        ))
    for rule in rules.in_force(rules.line_type_freshness, as_of):
        if not rule.matches(record):
            continue
        age = record.get("line_type_age_days")
        if age is not None and int(age) > int(rule.params["max_age_days"]):
            out.append(finding_from_rule(
                rule,
                detail=(f"line type resolved {int(age)} days ago, limit "
                        f"{rule.params['max_age_days']}"),
            ))
    return out


def _calling_window(record, rules: Ruleset, as_of: date) -> list[Finding]:
    """Only fires when a dial time is supplied. Otherwise the window is EMITTED.

    A lead file is produced hours before anyone picks up a handset, so on the
    fixture the honest output is the permitted window, not a judgement about a
    call that has not happened. The gate exists for the moment it would
    matter, and the test dials at 21:30 local to prove it.
    """
    dial_at: datetime | None = record.get("dial_at_local")
    window: window_mod.CallingWindow | None = record.get("calling_window")
    if dial_at is None or window is None or window.zone is None:
        return []
    if window.contains(dial_at.time()):
        return []
    return [
        finding_from_rule(
            rule,
            detail=(f"dial_at_local {dial_at.time().strftime('%H:%M')} in "
                    f"{window.zone} is outside {window.earliest}-{window.latest}"),
        )
        for rule in rules.in_force(rules.outside_calling_window, as_of)
        if rule.matches(record)
    ]


def _ebr(record, rules: Ruleset, as_of: date) -> list[Finding]:
    """The established-business-relationship exemption, if one is in window."""
    events = record.get("ebr") or []
    out: list[Finding] = []
    for rule in rules.in_force(rules.ebr, as_of):
        kind = str(rule.params["kind"])
        limit_days = float(rule.params["months"]) * DAYS_PER_MONTH
        for event in events:
            if str(event.get("kind")) != kind:
                continue
            occurred = _as_date(event.get("occurred_at"))
            if occurred is None or occurred > as_of:
                continue
            age = (as_of - occurred).days
            if age <= limit_days:
                out.append(finding_from_rule(
                    rule,
                    detail=(f"{kind} {occurred.isoformat()} is {age} days old, "
                            f"within {rule.params['months']} months"),
                ))
                break
    return out


def _as_date(value: Any) -> date | None:
    if value is None:
        return None
    if hasattr(value, "date") and not isinstance(value, date):
        return value.date()
    if isinstance(value, date):
        return value
    try:
        return date.fromisoformat(str(value)[:10])
    except ValueError:
        return None
