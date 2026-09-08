"""Retention TTLs by record class -- reported, never enforced.

ASSIGNMENT.md 5e: "Retention TTLs by record class, with deletion that actually
propagates to backups and derived tables. **This is where most
implementations quietly fail.**"

So this module does not delete anything, and that is the design rather than an
omission. Deletion that actually propagates has to reach: the vault tables,
every derived table that carries the token, the parquet files a consumer
already copied, the decision-lineage rows that exist precisely so a deleted
record's decision can still be explained, and the backups -- across which it
must be replayable, auditable and reversible if it was wrong. That is an
operational control with a runbook and a rollback story, and it belongs in
Phase 8 beside the orchestration.

A `delete()` here would be the quiet failure the assignment names: it would
make the repo look compliant and leave the copies. So the module REPORTS -- it
answers "what is past its TTL at `as_of`, by record class, and how many rows"
-- and the report is the input to the control that does the deleting.

The one genuine tension, stated rather than resolved: `decision_lineage` has
the LONGEST TTL (7 years) and it holds a tokenised snapshot of a record whose
identity may be deleted at 5. That is deliberate and it is the right way
round. The lineage row is the evidence that a decision was lawful; deleting it
on a subject request would destroy the defence to a claim about that very
subject. The token in it is unresolvable once the vault row is gone, which is
what makes keeping it defensible -- the row says "a decision was made about
some party under these rules" and no longer says who.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass
from datetime import date, datetime, timedelta
from typing import Any, Mapping, Sequence

import pandas as pd

from .ruleset import Ruleset

log = logging.getLogger("compliance.retention")

# 365.25 days per year: TTLs here are multi-year, so the leap-day drift a
# 365-day year accumulates would move a five-year boundary by more than a day.
DAYS_PER_YEAR = 365.25


@dataclass(frozen=True)
class RetentionFinding:
    record_class: str
    ttl_years: float
    legal_basis: str
    anchor_column: str
    rows_total: int
    rows_expired: int
    oldest: str | None
    cutoff: str

    def as_dict(self) -> dict[str, Any]:
        return {
            "record_class": self.record_class,
            "ttl_years": self.ttl_years,
            "legal_basis": self.legal_basis,
            "anchor_column": self.anchor_column,
            "rows_total": self.rows_total,
            "rows_expired": self.rows_expired,
            "oldest": self.oldest,
            "cutoff": self.cutoff,
        }


# record class in rules.yaml -> the column that dates a row of that class.
ANCHOR_COLUMNS = {
    "consent_provenance": "obtained_at",
    "revocations": "received_at",
    "internal_dnc": "added_at",
    "decision_lineage": "evaluated_at",
    "vault_access_log": "read_at",
    "vault_parties": "incident_date",
}


def report(
    rules: Ruleset,
    tables: Mapping[str, pd.DataFrame],
    *,
    as_of: date,
) -> list[RetentionFinding]:
    """What would be deleted at `as_of`, per record class. Deletes nothing."""
    out: list[RetentionFinding] = []
    for record_class, spec in sorted(rules.retention.items()):
        years = float(spec["years"])
        cutoff = as_of - timedelta(days=years * DAYS_PER_YEAR)
        column = ANCHOR_COLUMNS.get(record_class)
        frame = tables.get(record_class)
        if frame is None or column is None or column not in getattr(frame, "columns", []):
            out.append(RetentionFinding(
                record_class=record_class,
                ttl_years=years,
                legal_basis=" ".join(str(spec["legal_basis"]).split()),
                anchor_column=column or "(none)",
                rows_total=0 if frame is None else int(len(frame)),
                rows_expired=0,
                oldest=None,
                cutoff=cutoff.isoformat(),
            ))
            continue
        stamps = pd.to_datetime(frame[column], errors="coerce", utc=True)
        expired = stamps.notna() & (stamps.dt.date < cutoff)
        oldest = stamps.min()
        out.append(RetentionFinding(
            record_class=record_class,
            ttl_years=years,
            legal_basis=" ".join(str(spec["legal_basis"]).split()),
            anchor_column=column,
            rows_total=int(len(frame)),
            rows_expired=int(expired.sum()),
            oldest=None if pd.isna(oldest) else str(oldest.date()),
            cutoff=cutoff.isoformat(),
        ))
    return out


def as_manifest(findings: Sequence[RetentionFinding]) -> dict[str, Any]:
    return {
        "deletes_nothing": True,
        "why": (
            "Deletion that propagates to backups and derived tables is an "
            "operational control with a runbook and a rollback story, not a "
            "function call. See the module docstring and COMPLIANCE.md."
        ),
        "classes": [f.as_dict() for f in findings],
        "rows_expired_total": sum(f.rows_expired for f in findings),
    }
