"""Source-eligibility and live-solicitation gates -- ASSIGNMENT.md 5b and 7.

Both families are pure `rules.yaml` lookups: match the rule's `when`/`unless`
against the record, and emit its reason code and citation. There is no `if
jurisdiction == "TX"` anywhere in this file, and there must not be -- the
live-defence question is a data change, and a state named in code is a state
that needs a deploy.

The two families are together because they answer the same question from
opposite ends. A source gate asks *may we hold this person's contact details
at all* (the DPPA, TX 550.065(f), MD 4-320, FL 316.066). A live-solicitation
gate asks *may this actor place this kind of contact* (Rule 7.3(b) and its
analogues, and Tex. Penal Code 38.12(a)(2), which is a felony reaching the
individual caller). A record can pass one and fail the other, and the memo's
answer for a jurisdiction is usually that it fails both.
"""

from __future__ import annotations

from datetime import date
from typing import Any

from ..ruleset import Ruleset
from . import Finding, finding_from_rule


def evaluate(record: dict[str, Any], rules: Ruleset, as_of: date) -> list[Finding]:
    out: list[Finding] = []
    for rule in rules.in_force(rules.source_gates, as_of):
        if rule.matches(record):
            out.append(finding_from_rule(
                rule,
                detail=(
                    f"identity_provenance={record.get('identity_provenance')} "
                    f"jurisdiction={record.get('jurisdiction')}"
                ),
            ))
    for rule in rules.in_force(rules.live_solicitation, as_of):
        if rule.matches(record):
            out.append(finding_from_rule(
                rule,
                detail=(
                    f"actor={record.get('actor')} "
                    f"contact_kind={record.get('contact_kind')} "
                    f"consent_valid={record.get('consent_valid')}"
                ),
            ))
    return out
