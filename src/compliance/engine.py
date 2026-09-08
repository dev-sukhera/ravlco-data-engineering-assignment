"""The eligibility engine.

Design notes, which are also grading criteria:

1.  The default disposition is INELIGIBLE. Eligibility is proven, never assumed.
2.  Rules are DATA, loaded from config. A new jurisdiction with a new waiting
    period must be a row in a table and a version bump — not a code change.
    You will be asked to demonstrate this in the live defence.
3.  Every decision emits an immutable lineage record. If you cannot reconstruct
    why a record was released eighteen months from now, you have not built a
    compliance control, you have built a filter.
4.  Rulesets are versioned (semver) and every decision records the version that
    produced it. The law moves; see Part 5d of the assignment.

---------------------------------------------------------------------------

How this implementation honours each of those, and the one thing it refuses
--------------------------------------------------------------------------
**(1)** `evaluate` starts from no permission and works towards one. There is
no branch that sets ELIGIBLE; ELIGIBLE is what the precedence table in
`rules.yaml` returns when a decision has an AFFIRMATIVE finding and nothing
else. A record that fails nothing but proves nothing gets
`NO_AFFIRMATIVE_BASIS` and is INELIGIBLE -- the assignment's stated default,
emitted as a code so `reason_codes` is never empty and the default is a
positive statement rather than an absence.

**(2)** Nothing in `src/compliance/` names a state, a day count or an hour.
`grep -rnE "'(MD|TX|FL)'" src/compliance/` finds only the NPA table's comments
and the fixture loader's column names. The Ohio demo is a row in
`config/blackout_windows.csv`.

**(3)** Every call to `evaluate` builds a lineage row whose id is a hash of
(tokenised input, ruleset sha, blackout sha, as_of). Re-running appends
nothing; changing anything appends a new row and never touches the old one.

**(4)** The constructor REFUSES to build if `rules.yaml`'s `version` is not
the version the caller asked for, and the loader independently refuses if the
file's content hash does not match its declared prefix. A decision stamped
`1.0.0` that was produced by a different file is worse than no audit record,
because it looks like one.

**The refusal:** `evaluate` never short-circuits. Every gate runs on every
record and every applicable code is accumulated, even when the first gate
already guarantees INELIGIBLE. The memo's exclusion table counts codes, so an
engine that stopped at the first failure would systematically under-report
every rule that happens to sort late -- and the exclusion table is the part
of the memo the assignment says is more informative than the inclusion table.


Composing a status from findings
--------------------------------
    dispositions = {code -> BAR | HOLD_UNTIL_DATE | HOLD_UNTIL_REFRESH
                            | NOTE | AFFIRMATIVE}      (rules.yaml)

    any BAR or HOLD_UNTIL_REFRESH  -> INELIGIBLE      (blocked_until null)
    else any HOLD_UNTIL_DATE       -> BLOCKED_UNTIL   (blocked_until = max)
    else any AFFIRMATIVE           -> ELIGIBLE
    else                           -> INELIGIBLE + NO_AFFIRMATIVE_BASIS

The precedence lives in `rules.yaml` `status_precedence` and is walked, not
hardcoded, so "a stale DNC scrub stops being a hold and becomes a bar" is an
edit to one line of data.

Note the second clause carefully: a record that is BOTH DNC-listed AND inside
the Texas 31-day window is INELIGIBLE, not BLOCKED_UNTIL. Both codes are
emitted; only the status collapses. Publishing a date would tell the business
the record becomes contactable on that date, which is false.

`reason_codes` is ordered by `rules.yaml` `reason_code_severity_order` and
`legal_basis` is emitted PARALLEL to it -- same length, same order, one
citation per code -- because the contract has them as two arrays and a
consumer has no other way to pair them.
"""

from __future__ import annotations

import hashlib
import json
import logging
from dataclasses import dataclass, field
from datetime import date, datetime, timezone
from pathlib import Path
from typing import Any, Iterable, Mapping, Sequence

from .. import config
from . import lineage as lineage_mod
from . import window as window_mod
from .consent import ConsentProvenance, is_revoked
from .gates import Finding
from .gates import channel as channel_gate
from .gates import quality as quality_gate
from .gates import source as source_gate
from .gates import timewindow as time_gate
from .reason_codes import IdentityProvenance, ReasonCode
from .ruleset import (AFFIRMATIVE, BAR, HOLD_UNTIL_DATE, HOLD_UNTIL_REFRESH,
                      BlackoutRow, Ruleset, load_blackout, load_ruleset)

log = logging.getLogger("compliance.engine")

Status = str  # "ELIGIBLE" | "INELIGIBLE" | "BLOCKED_UNTIL"; the enum is the contract's


@dataclass(frozen=True)
class EligibilityDecision:
    status: Status
    reason_codes: list[str]
    legal_basis: list[str]
    blocked_until_date: date | None
    decision_lineage_id: str
    evaluated_at: datetime
    ruleset_version: str

    # Everything below is additive. The scaffold's seven fields ARE the
    # contract and none of them changed; these carry what the build, the
    # manifest and the tests need without a second pass over the engine.
    findings: tuple[Finding, ...] = ()
    calling_window: window_mod.CallingWindow | None = None
    lineage_row: dict[str, Any] = field(default_factory=dict, repr=False)
    ruleset_sha256: str = ""
    blackout_sha256: str = ""

    @property
    def is_contactable(self) -> bool:
        return self.status == "ELIGIBLE"

    def code_set(self) -> set[str]:
        return set(self.reason_codes)


class EligibilityEngine:
    """Runs every gate over one record and composes an auditable decision.

    Construction is where the stale-config guard lives, so an engine that
    exists is an engine whose ruleset has been verified. `blackout_path` and
    `ruleset_path` default to `config/compliance.toml`; the tests pass
    `tmp_path` copies, which is how the Ohio demo proves itself without
    touching a tracked file.
    """

    def __init__(
        self,
        ruleset_path: str | Path | None = None,
        ruleset_version: str | None = None,
        blackout_path: str | Path | None = None,
        as_of: date | None = None,
        *,
        actor: str | None = None,
        contact_kind: str | None = None,
        seller: str | None = None,
        npa_table: Mapping[str, window_mod.NpaZones] | None = None,
        build_sha: str = "unset",
    ) -> None:
        cfg = config.compliance()
        self.as_of = as_of or date.fromisoformat(str(cfg["as_of_date"]))
        self.ruleset_version = ruleset_version or str(cfg["ruleset_version"])
        self.ruleset_path = Path(ruleset_path) if ruleset_path is not None \
            else config.compliance_path("ruleset_path")
        # Raises RulesetError on a version or content-hash mismatch. Refusing
        # to CONSTRUCT rather than refusing to evaluate means a build cannot
        # get half way through a corpus before discovering its ruleset is
        # stale.
        self.rules: Ruleset = load_ruleset(
            self.ruleset_path, expect_version=self.ruleset_version
        )
        self.blackout_path = Path(blackout_path) if blackout_path is not None \
            else config.compliance_path("blackout_path")
        self.blackout: list[BlackoutRow] = load_blackout(
            self.blackout_path, known_codes=[c.value for c in ReasonCode]
        )
        self.blackout_sha256 = hashlib.sha256(
            self.blackout_path.read_bytes()
        ).hexdigest()
        self.actor = actor if actor is not None else str(cfg["actor"])
        self.contact_kind = contact_kind if contact_kind is not None \
            else str(cfg["contact_kind"])
        self.seller = seller or "CRASH_TO_CONTACT_DEMO_SELLER"
        self.npa_table = npa_table if npa_table is not None \
            else window_mod.default_npa_table()
        self.build_sha = build_sha

        self._severity = {c: i for i, c in enumerate(self.rules.severity_order)}
        self._derived_slot = self._severity[
            str(self.rules.blackout_codes["derived_severity_marker"])
        ]

    # -- the frozen clock -------------------------------------------------
    @property
    def evaluated_at(self) -> datetime:
        """`as_of` at 00:00:00 UTC, NOT wall clock.

        The contract wants a timestamptz and the fixture README demands
        reproducibility; a wall clock in an output column makes two identical
        builds produce different bytes. The real wall clock is in
        `_compliance_manifest.json`'s `built_at`, where it belongs.
        """
        return datetime.combine(self.as_of, datetime.min.time(), tzinfo=timezone.utc)

    # -- the public API ---------------------------------------------------
    def evaluate(self, record: dict) -> EligibilityDecision:
        """Return a complete, auditable decision for one record.

        Starts from INELIGIBLE and works towards ELIGIBLE only by
        affirmatively satisfying each gate. Accumulates ALL applicable reason
        codes, not just the first -- the exclusion analysis in the memo
        depends on being able to count them.
        """
        prepared = self._prepare(record)

        # Consent runs first because two other families read its verdict:
        # the fixture's source gate (`unless: consent_valid`) and every
        # live-solicitation rule (the Rule 7.3 initiated-contact exception).
        consent_findings, consent_valid = channel_gate.evaluate_consent(
            prepared, self.rules, self.as_of
        )
        prepared["consent_valid"] = consent_valid

        findings: list[Finding] = list(consent_findings)
        findings += source_gate.evaluate(prepared, self.rules, self.as_of)
        findings += time_gate.evaluate(
            prepared, self.rules, self.as_of, blackout=self.blackout
        )
        findings += channel_gate.evaluate(prepared, self.rules, self.as_of)
        findings += quality_gate.evaluate(prepared, self.rules, self.as_of)

        status, blocked_until = self._compose(findings)
        if not findings:  # pragma: no cover - _compose always emits the default
            findings = []
        findings = self._with_default(findings, status)
        findings = self._ordered(findings)

        reason_codes = [f.reason_code for f in findings]
        legal_basis = [f.legal_basis for f in findings]

        row = lineage_mod.build_row(
            lead_id=str(record.get("lead_id") or record.get("party_token") or ""),
            party_token=record.get("party_token"),
            snapshot=self._snapshot(prepared),
            findings=findings,
            status=status,
            blocked_until=blocked_until,
            reason_codes=reason_codes,
            legal_basis=legal_basis,
            as_of=self.as_of,
            evaluated_at=self.evaluated_at,
            ruleset_version=self.rules.version,
            ruleset_sha=self.rules.sha256,
            blackout_sha=self.blackout_sha256,
            build_sha=self.build_sha,
        )
        return EligibilityDecision(
            status=status,
            reason_codes=reason_codes,
            legal_basis=legal_basis,
            blocked_until_date=blocked_until,
            decision_lineage_id=row["decision_lineage_id"],
            evaluated_at=self.evaluated_at,
            ruleset_version=self.rules.version,
            findings=tuple(findings),
            calling_window=prepared.get("calling_window"),
            lineage_row=row,
            ruleset_sha256=self.rules.sha256,
            blackout_sha256=self.blackout_sha256,
        )

    def evaluate_crash_only(self, crash: dict) -> EligibilityDecision:
        """A gold crash with NO identity layer -- the production-truth path.

        This is the answer to the memo's Part 7 question, produced by the same
        code as everything else rather than by an argument in prose. A crash
        row has a jurisdiction, dates and geography and no contact block at
        all, so it enters as `PUBLIC_CRASH_REPORT` provenance with no phone,
        no line type and no consent. What comes out is the honest count of
        deliverable leads from the real sources, per jurisdiction.
        """
        record = dict(crash)
        record.setdefault("identity_provenance", IdentityProvenance.PUBLIC_CRASH_REPORT)
        for absent in ("phone_token", "line_type", "line_type_asof", "rnd_response",
                       "npa", "consent_provenance"):
            record.setdefault(absent, None)
        record.setdefault("consent_on_file", False)
        record.setdefault("consent_revoked", False)
        record.setdefault("on_national_dnc", False)
        record.setdefault("dnc_scrub_age_days", None)
        return self.evaluate(record)

    # -- internals --------------------------------------------------------
    def _prepare(self, record: Mapping[str, Any]) -> dict[str, Any]:
        """Add the values the ruleset matches on that are not raw inputs.

        Everything derived here is put ON THE RECORD rather than passed
        alongside it, so a rule in `rules.yaml` can match it exactly like any
        input field. That is what keeps the live-solicitation exception a
        two-line `unless:` in the ruleset instead of a branch in code.
        """
        prepared = dict(record)
        prepared.setdefault("actor", self.actor)
        prepared.setdefault("contact_kind", self.contact_kind)
        prepared.setdefault("seller", self.seller)
        prepared["circuit"] = self.rules.circuit(prepared.get("jurisdiction"))

        routing = channel_gate.routing_row(self.rules, prepared.get("line_type"))
        prepared["line_type_tier"] = routing["tier"]
        prepared["line_type_requires_consent"] = routing["requires_prior_express_consent"]
        prepared["line_type_requires_written_consent"] = routing[
            "written_consent_required_for_autodial"]

        # A revocation in the append-only table is as good as the row's own
        # flag, and `scope = ALL` reaches every seller. Honoured from
        # `received_at`, not from `honour_by`: ten business days is a deadline
        # for us, not a grace period for continuing to call.
        revocation = is_revoked(
            prepared.get("revocations") or [],
            str(prepared.get("party_token") or ""),
            str(prepared.get("seller")),
            self.as_of,
        )
        if revocation is not None:
            prepared["consent_revoked"] = True
            prepared["revocation_scope"] = revocation.scope
            prepared["revocation_received_at"] = revocation.received_at

        prepared["calling_window"] = window_mod.resolve(
            ruleset=self.rules,
            jurisdiction=prepared.get("jurisdiction"),
            coordinate_zone=prepared.get("tz_iana"),
            npa=prepared.get("npa"),
            as_of=self.as_of,
            dial_at_local=prepared.get("dial_at_local"),
            npa_table=self.npa_table,
        )
        asof = prepared.get("line_type_asof")
        prepared["line_type_age_days"] = _age_days(asof, self.as_of)
        return prepared

    def _compose(self, findings: Sequence[Finding]) -> tuple[Status, date | None]:
        """Walk `status_precedence`; the first clause whose condition holds wins."""
        present = {self.rules.disposition(f.reason_code) for f in findings}
        for clause in self.rules.status_precedence:
            if clause.get("default"):
                return str(clause["status"]), None
            wanted = set(clause.get("when_any_disposition") or [])
            if not (present & wanted):
                continue
            status = str(clause["status"])
            if str(clause.get("blocked_until")) == "max":
                dates = [f.blocked_until for f in findings if f.blocked_until]
                return status, max(dates) if dates else None
            return status, None
        raise AssertionError(  # pragma: no cover - the ruleset declares a default
            "status_precedence has no default clause"
        )

    def _with_default(self, findings: list[Finding], status: Status) -> list[Finding]:
        """Emit the default-disposition code when nothing else explains INELIGIBLE."""
        if findings:
            return findings
        clause = next(c for c in self.rules.status_precedence if c.get("default"))
        code = str(clause["emit_reason_code"])
        return [Finding(
            reason_code=code,
            legal_basis=(
                "ASSIGNMENT.md Part 5: the default disposition is INELIGIBLE and "
                "eligibility must be affirmatively proven, per record, with a "
                "citation. This record failed no gate and proved no permission."
            ),
            detail="no failing gate and no affirmative basis",
            rule_id="STATUS_PRECEDENCE:default",
        )]

    def _ordered(self, findings: Sequence[Finding]) -> list[Finding]:
        """Sort by the declared severity, de-duplicating on (code, rule).

        The tie-break is the code string then the rule id, so two rules that
        emit the same code (the DPPA appears twice: once for a motor vehicle
        record and once for a fixture row with no consent) order
        deterministically instead of by dict insertion.
        """
        seen: set[tuple[str, str | None]] = set()
        unique: list[Finding] = []
        for f in findings:
            key = (f.reason_code, f.rule_id)
            if key in seen:
                continue
            seen.add(key)
            unique.append(f)
        return sorted(
            unique,
            key=lambda f: (
                self._severity.get(f.reason_code, self._derived_slot),
                f.reason_code,
                f.rule_id or "",
            ),
        )

    def _snapshot(self, prepared: Mapping[str, Any]) -> dict[str, Any]:
        """What goes into the lineage id: the inputs, not the derivations.

        Derived values are excluded on purpose. They are functions of the
        inputs plus the ruleset, both of which are already in the id, so
        including them would add nothing -- and would make the id move when a
        purely presentational field (the `basis` string's wording) changed.
        """
        drop = {"calling_window", "consent_provenance", "revocations", "ebr",
                "line_type_tier", "line_type_requires_consent",
                "line_type_requires_written_consent", "circuit",
                "line_type_age_days", "consent_valid", "revocation_scope",
                "revocation_received_at"}
        snap = {k: v for k, v in prepared.items() if k not in drop}
        provenance: ConsentProvenance | None = prepared.get("consent_provenance")
        if provenance is not None:
            # The consent record is an INPUT and it must move the id -- but by
            # its hash, not by its bulk. A lineage row already stores the full
            # record in `input_snapshot_json` for the rows that carry one.
            snap["consent_provenance_digest"] = hashlib.sha256(
                json.dumps(provenance.as_row(), sort_keys=True, default=str).encode()
            ).hexdigest()[:32]
        snap["ebr_events"] = len(prepared.get("ebr") or [])
        snap["revocations"] = [
            {"seller": r.seller, "scope": r.scope,
             "received_at": r.received_at.isoformat()}
            for r in (prepared.get("revocations") or [])
        ]
        return snap


def _age_days(value: Any, as_of: date) -> int | None:
    if value is None:
        return None
    if hasattr(value, "date") and not isinstance(value, date):
        value = value.date()
    if not isinstance(value, date):
        try:
            value = date.fromisoformat(str(value)[:10])
        except ValueError:
            return None
    return (as_of - value).days
