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
"""

from __future__ import annotations
from dataclasses import dataclass
from datetime import date, datetime
from typing import Literal

from .reason_codes import ReasonCode

Status = Literal["ELIGIBLE", "INELIGIBLE", "BLOCKED_UNTIL"]


@dataclass(frozen=True)
class EligibilityDecision:
    status: Status
    reason_codes: list[ReasonCode]
    legal_basis: list[str]
    blocked_until_date: date | None
    decision_lineage_id: str
    evaluated_at: datetime
    ruleset_version: str


class EligibilityEngine:
    def __init__(self, ruleset_path: str, ruleset_version: str) -> None:
        self.ruleset_path = ruleset_path
        self.ruleset_version = ruleset_version

    def evaluate(self, record: dict) -> EligibilityDecision:
        """Return a complete, auditable decision for one record.

        Implement me. Start from INELIGIBLE and work towards ELIGIBLE only by
        affirmatively satisfying each gate. Accumulate ALL applicable reason
        codes, not just the first — the exclusion analysis in the memo depends
        on being able to count them.
        """
        raise NotImplementedError(
            "Implement the eligibility engine. See Part 5 of the assignment."
        )
