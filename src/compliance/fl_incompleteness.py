"""The Florida second-order effect: a statute that looks like an outage.

ASSIGNMENT.md 5a, after the blackout table:

    "Note also the second-order effect: because of 316.066(2), **the most
     recent 60 days of any Florida public feed is structurally incomplete.**
     If you compute a trailing-30-day Florida trend, you will read a statute
     as an outage. Detect this and label it."

That is a MONITORING requirement, not an eligibility one, and this module is
built as one. Nothing here produces a reason code or touches a decision: the
label lives in `reason_codes.MonitoringLabel`, in a different enum from
`ReasonCode`, precisely so a data-quality observation cannot leak into an
exclusion count that the memo reports as a legal outcome. A record is not less
contactable because the feed around it is thin.

What it does instead is refuse a number. `trailing_aggregate` will not return
a Florida trailing-window count whose window overlaps the trailing 60 days
unless the caller explicitly asks for an annotated one -- because the failure
mode being guarded against is not a wrong answer, it is a RIGHT-LOOKING
answer. A trailing-30-day Florida crash count computed on 2026-09-01 is a
number, it is plottable, and it is roughly the count of crashes whose reports
the state is currently withholding. Refusing is the only response that cannot
be mistaken for a finding.


There is no Florida crash feed in scope
---------------------------------------
The three assigned sources are Montgomery County, TxDOT and FARS; Florida
appears in the blackout rules and in FARS fatalities and nowhere else. So this
runs against the two Florida populations that exist:

  * the Florida FIXTURE rows, which have a `report_filing_date` and can
    therefore be labelled, and
  * FARS Florida, which has NO filing date at all -- FARS publishes a crash
    date and a year and nothing about when a state report was filed. Those
    rows are labelled `NOT_APPLICABLE` **with the reason recorded**, not
    silently treated as complete. "We cannot test this" and "this passed" are
    different answers and only one of them is true.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass
from datetime import date, timedelta
from typing import Any, Iterable, Sequence

import pandas as pd

from .reason_codes import MonitoringLabel
from .ruleset import Ruleset

log = logging.getLogger("compliance.fl_incompleteness")

LABEL_COLUMN = "incompleteness_label"
REASON_COLUMN = "incompleteness_reason"


class IncompleteWindowRefused(RuntimeError):
    """A trailing aggregate would overlap the structurally incomplete window."""


@dataclass(frozen=True)
class Monitor:
    """The 316.066(2) monitor, configured from `rules.yaml`."""

    jurisdiction: str
    trailing_days: int
    legal_basis: str
    label: str

    @classmethod
    def from_rules(cls, rules: Ruleset) -> "Monitor":
        spec = rules.monitoring["fl_structural_incompleteness"]
        return cls(
            jurisdiction=str(spec["jurisdiction"]),
            trailing_days=int(spec["trailing_days"]),
            legal_basis=" ".join(str(spec["legal_basis"]).split()),
            label=str(spec["label"]),
        )

    def window_start(self, as_of: date) -> date:
        """The first day of the structurally incomplete window."""
        return as_of - timedelta(days=self.trailing_days)

    def label_rows(self, frame: pd.DataFrame, as_of: date) -> pd.DataFrame:
        """Add `incompleteness_label` and its reason. Never drops a row.

        Rows outside the monitored jurisdiction get no label. Rows inside it
        with no `report_filing_date` get NOT_APPLICABLE and the reason, which
        is the FARS case: there is no filing date in FARS to measure against,
        and pretending the test passed would be worse than saying so.
        """
        out = frame.copy()
        out[LABEL_COLUMN] = None
        out[REASON_COLUMN] = None
        if out.empty:
            return out

        in_scope = out["jurisdiction"].astype(str) == self.jurisdiction
        filed = pd.to_datetime(out.get("report_filing_date"), errors="coerce")
        start = pd.Timestamp(self.window_start(as_of))
        end = pd.Timestamp(as_of)

        no_date = in_scope & filed.isna()
        out.loc[no_date, LABEL_COLUMN] = MonitoringLabel.NOT_APPLICABLE.value
        out.loc[no_date, REASON_COLUMN] = (
            "no report_filing_date on this feed, so the trailing-window test "
            "cannot be evaluated for the row"
        )

        inside = in_scope & filed.notna() & (filed >= start) & (filed <= end)
        out.loc[inside, LABEL_COLUMN] = self.label
        out.loc[inside, REASON_COLUMN] = (
            f"report_filing_date is within the trailing {self.trailing_days} days "
            f"of {as_of.isoformat()}; {self.legal_basis}"
        )
        return out

    def overlaps(self, *, as_of: date, aggregate_days: int) -> bool:
        """Does a trailing-N-day aggregate ending at `as_of` touch the window?

        Any trailing window ending at `as_of` starts inside the incomplete
        window whenever N is at or below the statutory 60, so this is true for
        every aggregate the business actually wants. Written as a comparison
        rather than as `N <= 60` so a caller asking for a 400-day window that
        ENDS today still gets the warning: the last 60 days of it are thin,
        which matters less but is still true.
        """
        return self.window_start(as_of) < as_of and aggregate_days > 0

    def trailing_aggregate(
        self,
        frame: pd.DataFrame,
        *,
        as_of: date,
        aggregate_days: int,
        annotate: bool = False,
    ) -> dict[str, Any]:
        """A trailing-window count -- refused, or annotated on request.

        `annotate=False` (the default) RAISES. That is the point: a caller who
        wants this number has to say, in code, that they know it is not a
        trend. `annotate=True` returns the count together with how much of the
        window is structurally incomplete and how many rows carried the label,
        so a chart built from it cannot be built without the caveat.
        """
        start = as_of - timedelta(days=aggregate_days)
        labelled = self.label_rows(frame, as_of)
        in_scope = labelled["jurisdiction"].astype(str) == self.jurisdiction
        filed = pd.to_datetime(labelled.get("report_filing_date"), errors="coerce")
        window = in_scope & filed.notna() & (filed > pd.Timestamp(start)) \
            & (filed <= pd.Timestamp(as_of))

        overlap_days = min(aggregate_days, self.trailing_days)
        detail = {
            "jurisdiction": self.jurisdiction,
            "as_of": as_of.isoformat(),
            "aggregate_days": aggregate_days,
            "window_start": start.isoformat(),
            "structurally_incomplete_from": self.window_start(as_of).isoformat(),
            "overlap_days": overlap_days,
            "overlap_fraction": round(overlap_days / aggregate_days, 4)
            if aggregate_days else 0.0,
            "rows_in_window": int(window.sum()),
            "rows_labelled_incomplete": int(
                (labelled[LABEL_COLUMN] == self.label).sum()
            ),
            "rows_not_applicable": int(
                (labelled[LABEL_COLUMN] == MonitoringLabel.NOT_APPLICABLE.value).sum()
            ),
            "legal_basis": self.legal_basis,
        }
        if self.overlaps(as_of=as_of, aggregate_days=aggregate_days) and not annotate:
            raise IncompleteWindowRefused(
                f"a trailing {aggregate_days}-day {self.jurisdiction} aggregate ending "
                f"{as_of.isoformat()} overlaps the structurally incomplete window by "
                f"{overlap_days} day(s). {self.legal_basis} "
                "Pass annotate=True to get the number WITH its caveat; there is no "
                "way to get it without one."
            )
        detail["annotated"] = True
        detail["warning"] = (
            f"{overlap_days} of {aggregate_days} days in this window are structurally "
            f"incomplete: {self.legal_basis}"
        )
        return detail
