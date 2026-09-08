"""Gate families. Each one is a pure function over a record and the ruleset.

    evaluate(record: dict, rules: Ruleset, as_of: date) -> list[Finding]

Pure in the sense that matters here: no I/O, no clock, no global state, and no
knowledge of what any other gate found. That is what makes "run EVERY gate,
never short-circuit" cheap to implement and impossible to get wrong by
accident -- there is no early return to add, because there is nothing to
return early from. The engine calls all of them, concatenates the findings and
composes a status afterwards.

Everything a gate needs beyond `rules` and `as_of` is on the record dict,
including a handful of values the engine DERIVES before the gates run
(`consent_valid`, `consent_provenance_complete`,
`line_type_requires_written_consent`, `circuit`). Those are on the record
rather than passed as a context object so that a rule in `rules.yaml` can
match on them exactly like any input field -- which is what lets the
live-solicitation exception be a two-line `unless: {consent_valid: [true]}`
in the ruleset instead of a branch in code.

A `Finding` is deliberately not a decision. It says what failed, under which
rule, with which citation, and -- for a time gate -- the date it clears. What
that MEANS for the record's status is the engine's job, using the disposition
map and the precedence in `rules.yaml`, because the same failure can mean
different things under different versions of the law.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import date
from typing import Any

from ..ruleset import Rule


@dataclass(frozen=True)
class Finding:
    """One failure or one affirmative basis, with its citation.

    `blocked_until` is non-null only for a time gate that HAS a computable
    end. A refresh hold (a stale DNC scrub, an unresolved line type) leaves it
    None on purpose: publishing a date for something that is not cleared by
    the passage of time would tell the business the record becomes contactable
    on that date, which is false.
    """

    reason_code: str
    legal_basis: str
    blocked_until: date | None = None
    detail: str = ""
    rule_id: str | None = None
    params: dict[str, Any] = field(default_factory=dict)

    def as_lineage(self) -> dict[str, Any]:
        """The shape stored in `decision_lineage.findings`.

        Params are recorded AS EVALUATED, not as a pointer to the rule: the
        whole reason a lineage record exists is that eighteen months from now
        `rules.yaml` will say something else.
        """
        return {
            "reason_code": self.reason_code,
            "legal_basis": self.legal_basis,
            "blocked_until": self.blocked_until.isoformat() if self.blocked_until else None,
            "detail": self.detail,
            "rule_id": self.rule_id,
            "params": dict(sorted(self.params.items())),
        }


def finding_from_rule(rule: Rule, *, detail: str = "",
                      blocked_until: date | None = None) -> Finding:
    """Build a finding from a matched rule, carrying its citation and params."""
    return Finding(
        reason_code=str(rule.reason_code),
        legal_basis=rule.legal_basis,
        blocked_until=blocked_until,
        detail=detail,
        rule_id=rule.id,
        params=dict(rule.params),
    )
