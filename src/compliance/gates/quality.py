"""Data-quality gates: Phase 4's measured defects promoted to eligibility codes.

Every rule here is a `rules.yaml` row matched against a field that Phase 4
already computed and stored. Nothing is re-derived, and nothing is corrected:
the Phase 4 decision to QUARANTINE rather than drop carries straight through,
so P019's 100-km-out coordinate produces a dispositioned row with a code and a
count, not a silently missing lead.

The one judgement in this file is what does NOT fire.

  * `snap_status` values `NOT_ATTEMPTED`, `NOT_IN_SCOPE`, `NO_GEOMETRY` and
    `UNAVAILABLE` are not failures. Texas and Florida get no road snap because
    `config/geo.toml [snap] source_systems` scopes it to Montgomery; letting
    an out-of-scope enrichment turn into a reason code would put every TX and
    FL row in the exclusion table for a decision about a 683 MB download.
  * `GEOCODE_TIER_INSUFFICIENT` and `SNAP_DISTANCE_EXCEEDED` are different
    codes for different failures and do not both fire on the same row. See
    the `DQ_SNAP_DISTANCE_EXCEEDED` note in `rules.yaml` for the argument.
  * `tz_gap_adjusted` / `tz_ambiguous` -- Phase 4's DST flags -- are recorded
    on the lead but gate nothing. A calling window is a function of the ZONE,
    not of the crash instant, so an hour of uncertainty about when a crash
    happened does not make the party's permitted call hours uncertain. They
    would matter for a dial-time computation and the flags are carried so a
    later rule can use them.
"""

from __future__ import annotations

from datetime import date
from typing import Any

from ..ruleset import Ruleset
from . import Finding, finding_from_rule


def evaluate(record: dict[str, Any], rules: Ruleset, as_of: date) -> list[Finding]:
    out: list[Finding] = []
    for rule in rules.in_force(rules.data_quality, as_of):
        if not rule.matches(record):
            continue
        out.append(finding_from_rule(rule, detail=_detail(rule, record)))
    return out


def _detail(rule, record: dict[str, Any]) -> str:
    code = str(rule.reason_code)
    if code == "SNAP_DISTANCE_EXCEEDED":
        distance = record.get("snap_distance_m")
        return (f"snap_status={record.get('snap_status')} "
                f"snap_distance_m={distance if distance is None else round(float(distance), 1)}")
    if code == "GEOCODE_TIER_INSUFFICIENT":
        return f"tz_source={record.get('tz_source')} tz_iana={record.get('tz_iana')}"
    if code == "COORDINATE_OUT_OF_ENVELOPE":
        return (f"envelope_status={record.get('envelope_status')} "
                f"distance_from_envelope_m="
                f"{record.get('distance_from_envelope_m')}")
    return f"tz_iana={record.get('tz_iana')} tz_source={record.get('tz_source')}"
