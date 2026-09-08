"""Blackout windows: `(jurisdiction, record_type) -> earliest_contact_date`.

ASSIGNMENT.md 5a: "Implement as a data-driven table, **not as hardcoded
branches**." `config/blackout_windows.csv` is that table and this module is
the only thing that reads it. A jurisdiction is a row; a code is derived from
the row when `rules.yaml` does not name one.


The arithmetic, and why exclusive
---------------------------------
    earliest_contact_date = anchor + days + 1 day
    contactable           <=> as_of >= earliest_contact_date
    blocked_until_date    = earliest_contact_date otherwise

The statutes do not pin down the boundary day, and the two readings differ by
exactly one day. Exclusive is chosen because a 31-day window that opens on day
31 has waited thirty days, and because the downside is asymmetric:
Tex. Penal Code 38.12 is a criminal statute and Fla. Stat. 316.066(3)(d) is a
third-degree felony, so the reading that cannot UNDER-count is the one to take.
The choice is `window_arithmetic` in both `rules.yaml` and
`config/compliance.toml`, and the loader refuses to run if they disagree.


The Florida interaction, which is the point of the section
----------------------------------------------------------
Florida has TWO rows and BOTH are evaluated. Fla. Stat. 316.066(2) runs 60
days from the FILING date; R. Reg. Fla. Bar 4-7.18(b)(1)(A) runs 30 days from
the INCIDENT date. They anchor on different dates and have different lengths,
so neither subsumes the other -- and the data gate is longer, so it binds.
Every row that binds emits its own code and `blocked_until` is the MAXIMUM, so
a record 40 days past filing and 45 days past the incident shows
FL_CRASH_REPORT_60D alone: the bar rule has genuinely lapsed and saying
otherwise would inflate the exclusion table with a constraint that no longer
exists. "A candidate who implements only the bar rule has implemented the
wrong constraint."


A missing anchor
----------------
A row whose anchor field is null cannot be shown to have elapsed. The gate
stays CLOSED with `ANCHOR_DATE_MISSING` and **no** `blocked_until`: there is
no date to publish, and defaulting the anchor to the incident date -- or to
today -- is how a waiting period gets skipped by a null.
"""

from __future__ import annotations

from datetime import date, timedelta
from typing import Any, Sequence

from ..ruleset import BlackoutRow, Ruleset
from . import Finding


def applicable_rows(
    record: dict[str, Any], blackout: Sequence[BlackoutRow], rules: Ruleset
) -> list[BlackoutRow]:
    """Which blackout rows reach this record.

    A row matches on `jurisdiction`. The wildcard jurisdiction (`US`) matches
    only where the record DECLARES the row's `record_type` in `record_types`
    -- without that, every crash in the corpus would inherit the aviation
    window, which is the failure mode a wildcard row exists to test.
    """
    wildcard = str(rules.blackout_codes["wildcard_jurisdiction"])
    declared = {str(t) for t in (record.get("record_types") or [])}
    jurisdiction = record.get("jurisdiction")
    out: list[BlackoutRow] = []
    for row in blackout:
        if row.jurisdiction == jurisdiction:
            out.append(row)
        elif row.jurisdiction == wildcard and row.record_type in declared:
            out.append(row)
    return out


def reason_code_for(row: BlackoutRow, rules: Ruleset) -> str:
    """The code for a blackout row: named in `rules.yaml`, or derived from it.

    The derived branch is the live-defence answer. Appending an Ohio row to
    the CSV produces `OH_WRITTEN_SOLICITATION_45D` with the CSV's own citation
    and no edit to `rules.yaml` and no file under `src/` touched.
    """
    spec = rules.blackout_codes
    key = f"{row.jurisdiction}/{row.record_type}"
    named = spec["map"].get(key)
    if named:
        return str(named)
    return str(spec["derived_code_template"]).format(
        jurisdiction=row.jurisdiction.upper(),
        record_type=row.record_type.upper(),
        days=row.days_from,
    )


def earliest_contact_date(anchor: date, days: int, *, exclusive: bool = True) -> date:
    """`anchor + days`, plus one more day when the arithmetic is exclusive."""
    return anchor + timedelta(days=days + (1 if exclusive else 0))


def evaluate(
    record: dict[str, Any],
    rules: Ruleset,
    as_of: date,
    *,
    blackout: Sequence[BlackoutRow],
) -> list[Finding]:
    exclusive = str(rules.doc["window_arithmetic"]).lower() == "exclusive"
    spec = rules.blackout_codes
    out: list[Finding] = []

    for row in applicable_rows(record, blackout, rules):
        if not row.has_window:
            # A declared no-window row (Maryland). The loader has already
            # verified it names the rule that carries the constraint, and that
            # rule fires on its own; emitting a second code here would
            # double-count Maryland in the exclusion table.
            continue
        code = reason_code_for(row, rules)
        anchor = _as_date(record.get(str(row.anchor_field)))
        if anchor is None:
            out.append(Finding(
                reason_code=str(spec["anchor_missing_reason_code"]),
                legal_basis=(
                    f"{row.legal_basis}. "
                    + " ".join(str(spec["anchor_missing_legal_basis"]).split())
                ),
                blocked_until=None,
                detail=(
                    f"{row.jurisdiction}/{row.record_type} anchors on "
                    f"{row.anchor_field}, which is null on this record"
                ),
                rule_id=f"BLACKOUT:{row.jurisdiction}/{row.record_type}",
                params={"days_from": row.days_from, "anchor_field": row.anchor_field},
            ))
            continue
        opens = earliest_contact_date(anchor, int(row.days_from), exclusive=exclusive)
        if as_of >= opens:
            continue                      # the window has lapsed; no finding
        out.append(Finding(
            reason_code=code,
            legal_basis=row.legal_basis,
            blocked_until=opens,
            detail=(
                f"{row.anchor_field}={anchor.isoformat()} + {row.days_from}d "
                f"({'exclusive' if exclusive else 'inclusive'}) "
                f"-> earliest_contact_date {opens.isoformat()}; as_of {as_of.isoformat()}"
            ),
            rule_id=f"BLACKOUT:{row.jurisdiction}/{row.record_type}",
            params={"days_from": row.days_from, "anchor_field": row.anchor_field,
                    "window_arithmetic": "exclusive" if exclusive else "inclusive"},
        ))
    return out


def _as_date(value: Any) -> date | None:
    if value is None or value == "":
        return None
    if isinstance(value, date) and not hasattr(value, "hour"):
        return value
    if hasattr(value, "date"):
        return value.date()
    try:
        return date.fromisoformat(str(value)[:10])
    except ValueError:
        return None
