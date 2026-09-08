"""The calling window: derived from GEOGRAPHY, with the phone as a second opinion.

ASSIGNMENT.md 5c is unusually specific about this one, and it is the only
section that names a way to fail outright:

    "Derive the window from the lead's address where known, fall back to
     NPA-NXX, and take the intersection when they disagree. [...] A pipeline
     that computes a calling window from SUBSTR(phone,1,3) fails this section."

So the order is fixed and it is never negotiable:

  1. **The coordinate.** `src/geo/tz.py` resolves an IANA zone from the
     party's WGS84 point against the Timezone Boundary Builder polygons. This
     is the primary and it wins.
  2. **The NPA, as a SET.** `config/npa_timezone.csv` maps an area code to
     every zone it can reach -- a set, because 850 is Pensacola (Central) AND
     Tallahassee (Eastern), and a single-valued NPA table is simply wrong for
     part of its own territory. The area code is parsed from the E.164 number
     with the country code stripped, never by slicing the first three
     characters of a string that begins with "+1".
  3. **The intersection.** Every zone either source considers possible is
     honoured: each zone's permitted window is re-expressed in the coordinate
     zone's clock and the intervals are INTERSECTED. Disagreement therefore
     NARROWS the window, which is the only safe direction. Agreement leaves it
     unchanged, which is why the two trap rows look boring in the output and
     interesting in `basis`.
  4. **The stricter of federal and state hours**, from `rules.yaml`. Both are
     citations, both are data.

`basis` on every emitted window states the whole derivation, coordinate first.
It is the audit trail for the one number in this pipeline that a regulator
would ask about, and it is written to be read by someone who is not going to
open the code.


The two trap rows
-----------------
`fixtures/README.md`: "Two records in particular exist to catch a specific
mistake; you will find them if your timezone derivation is correct and miss
them if it is not."

  P007  El Paso, TX      31.8479, -106.5348   NPA 915
        Coordinate -> America/Denver. The Texas jurisdiction default is
        America/Chicago, so a state-derived window is an hour wrong, in the
        direction that places a call an hour after the permitted end.
  P008  Pensacola, FL    30.4213,  -87.2169   NPA 850
        Coordinate -> America/Chicago. The Florida default is
        America/New_York and MOST of area code 850 is Eastern, so BOTH naive
        methods agree on the wrong answer. The NPA set is
        {America/New_York, America/Chicago} and the intersection narrows the
        Central-clock window by an hour at the top.


Day-of-week
-----------
Texas is the reason this exists: Tex. Bus. & Com. Code 301.051 permits calls
after 9 a.m. on a weekday or Saturday but only after 12 noon on a Sunday. The
window is therefore computed for a DAY -- `dial_at_local`'s date when one is
supplied, otherwise the run's frozen `as_of`. A lead file states the window
for the day it was produced for; a dial-time check states it for the day of
the call. Which one was used is in `basis`.
"""

from __future__ import annotations

import csv
import functools
import logging
import re
from dataclasses import dataclass
from datetime import date, datetime, time
from pathlib import Path
from typing import Any, Iterable, Mapping, Sequence
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError

from .. import config

log = logging.getLogger("compliance.window")

# Weekday index (Monday=0, as datetime.weekday()) -> the name rules.yaml uses.
WEEKDAY_NAMES = (
    "MONDAY", "TUESDAY", "WEDNESDAY", "THURSDAY", "FRIDAY", "SATURDAY", "SUNDAY",
)

# NANP: an area code is [2-9][0-8][0-9]. Enforced so a malformed number yields
# "no fallback" rather than a plausible-looking wrong one.
_NANP_NPA = re.compile(r"^[2-9][0-8][0-9]$")
_E164 = re.compile(r"^\+(\d{7,15})$")

# The NANP country code. In its own constant because stripping it is the whole
# point of `npa_from_e164`: the failure this module exists to avoid is reading
# an area code off the front of a string that starts with a country code.
NANP_COUNTRY_CODE = "1"


class WindowError(ValueError):
    pass


# ---------------------------------------------------------------------------
# the NPA fallback table
# ---------------------------------------------------------------------------


def npa_from_e164(number: str | None) -> str | None:
    """The NANP area code of an E.164 number, or None.

    Parsed, not sliced. `+13015550101` is a country code (`1`) followed by a
    ten-digit national number whose first three digits are the area code; the
    string's first three characters are `+13`, which is not an area code and
    is not Maryland. The function refuses a non-NANP country code outright
    rather than guessing, because there is no NPA to find in one.
    """
    if not number:
        return None
    m = _E164.match(str(number).strip())
    if not m:
        return None
    digits = m.group(1)
    if not digits.startswith(NANP_COUNTRY_CODE):
        return None
    national = digits[len(NANP_COUNTRY_CODE):]
    if len(national) != 10:
        return None
    npa = national[:3]
    return npa if _NANP_NPA.match(npa) else None


@dataclass(frozen=True)
class NpaZones:
    """One row of `config/npa_timezone.csv`."""

    npa: str
    zones: tuple[str, ...]
    split: bool
    notes: str


def load_npa_table(path: str | Path | None = None) -> dict[str, NpaZones]:
    """Parse the NPA -> zone-set table. Comment lines start with `#`."""
    p = Path(path) if path is not None else config.compliance_path("npa_timezone_path")
    if not p.exists():
        raise WindowError(f"no NPA timezone table at {p}")
    lines = [ln for ln in p.read_text(encoding="utf-8").splitlines()
             if not ln.lstrip().startswith("#")]
    out: dict[str, NpaZones] = {}
    for row in csv.DictReader(lines):
        npa = (row.get("npa") or "").strip()
        if not npa:
            continue
        zones = tuple(z.strip() for z in (row.get("zones") or "").split(";") if z.strip())
        if not zones:
            raise WindowError(f"{p}: NPA {npa} has no zones")
        split = str(row.get("split", "")).strip().lower() == "true"
        if split != (len(zones) > 1):
            raise WindowError(
                f"{p}: NPA {npa} declares split={split} but lists {len(zones)} zone(s). "
                "The flag is what a reader trusts; it cannot disagree with the data."
            )
        out[npa] = NpaZones(npa, zones, split, (row.get("notes") or "").strip())
    return out


@functools.cache
def default_npa_table() -> dict[str, NpaZones]:
    return load_npa_table()


# ---------------------------------------------------------------------------
# the window
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class CallingWindow:
    """The contract's `contact.calling_window_local`, plus its derivation.

    `earliest` / `latest` are HH:MM in `zone`, which is always the COORDINATE
    zone when there is one. `basis` is the sentence a reviewer reads.
    """

    earliest: str
    latest: str
    basis: str
    zone: str | None
    coordinate_zone: str | None
    npa: str | None
    npa_zones: tuple[str, ...]
    intersected: bool
    narrowed_minutes: int
    day: str
    state_rule_id: str | None

    def as_contract(self) -> dict[str, Any] | None:
        if self.zone is None:
            return None
        return {"earliest": self.earliest, "latest": self.latest, "basis": self.basis}

    def contains(self, local: time) -> bool:
        return _minutes(self.earliest) <= _to_minutes(local) <= _minutes(self.latest)


def _minutes(hhmm: str) -> int:
    h, m = hhmm.split(":")
    return int(h) * 60 + int(m)


def _to_minutes(t: time) -> int:
    return t.hour * 60 + t.minute


def _hhmm(minutes: int) -> str:
    # Clamped to a real clock. An intersection can in principle push an edge
    # past midnight (a three-hour zone spread against a thirteen-hour window
    # cannot, but the arithmetic does not know that); the contract wants HH:MM
    # and a "25:00" would validate against the pattern while meaning nothing.
    minutes = max(0, min(23 * 60 + 59, minutes))
    return f"{minutes // 60:02d}:{minutes % 60:02d}"


def _offset_minutes(zone: str, when: datetime) -> int:
    try:
        tz = ZoneInfo(zone)
    except (ZoneInfoNotFoundError, ValueError) as exc:
        raise WindowError(f"unknown IANA zone {zone!r}") from exc
    off = when.replace(tzinfo=tz).utcoffset()
    if off is None:  # pragma: no cover - zoneinfo always answers
        raise WindowError(f"no UTC offset for {zone!r} at {when}")
    return int(off.total_seconds() // 60)


def _state_rule(ruleset, jurisdiction: str | None, as_of: date) -> dict[str, Any] | None:
    """The in-force state hours rule for a jurisdiction, or None.

    Effective-dated like everything else: Maryland's 8 p.m. curfew is the Stop
    the Spam Calls Act of 2023 and did not exist before 2024-01-01, so
    replaying an `as_of` in 2023 correctly gets the federal window.
    """
    for entry in ruleset.calling_window.get("states", []):
        if entry.get("jurisdiction") != jurisdiction:
            continue
        start = entry.get("effective_from")
        end = entry.get("effective_to")
        if start is not None and as_of < _d(start):
            continue
        if end is not None and as_of > _d(end):
            continue
        return entry
    return None


def _d(value: Any) -> date:
    if isinstance(value, datetime):
        return value.date()
    if isinstance(value, date):
        return value
    return date.fromisoformat(str(value))


def _hours_for_day(entry: Mapping[str, Any], day: str) -> tuple[str, str]:
    """A rule's hours for one weekday, honouring `day_overrides`."""
    for override in entry.get("day_overrides") or []:
        if day in [str(d).upper() for d in override.get("days", [])]:
            return str(override["earliest"]), str(override["latest"])
    return str(entry["earliest"]), str(entry["latest"])


def resolve(
    *,
    ruleset,
    jurisdiction: str | None,
    coordinate_zone: str | None,
    npa: str | None,
    as_of: date,
    dial_at_local: datetime | None = None,
    npa_table: Mapping[str, NpaZones] | None = None,
) -> CallingWindow:
    """The permitted window for one record, with its derivation in `basis`.

    Returns a window whose `zone` is None only when NEITHER the coordinate nor
    the area code yields a zone. That case is a `TIMEZONE_UNRESOLVED` finding
    in the data-quality gate, not a silently-defaulted window.
    """
    table = npa_table if npa_table is not None else default_npa_table()
    npa_entry = table.get(npa or "")
    npa_zones = npa_entry.zones if npa_entry else ()

    # The reference clock. The COORDINATE zone whenever there is one -- the
    # window is expressed where the party actually is, and the NPA only ever
    # narrows it.
    reference = coordinate_zone or (npa_zones[0] if npa_zones else None)
    day_date = dial_at_local.date() if dial_at_local is not None else as_of
    day = WEEKDAY_NAMES[day_date.weekday()]

    federal = ruleset.calling_window["federal"]
    fed_early, fed_late = _hours_for_day(federal, day)
    state = _state_rule(ruleset, jurisdiction, as_of)
    if state is not None:
        st_early, st_late = _hours_for_day(state, day)
        # "Stricter of" is max on the opening and min on the closing bound.
        early = max(_minutes(fed_early), _minutes(st_early))
        late = min(_minutes(fed_late), _minutes(st_late))
        rule_note = (
            f"federal {fed_early}-{fed_late} [{_short(federal['legal_basis'])}] "
            f"& {jurisdiction} {st_early}-{st_late} [{_short(state['legal_basis'])}] "
            f"-> stricter {_hhmm(early)}-{_hhmm(late)}"
        )
        state_rule_id = str(state.get("id"))
    else:
        early, late = _minutes(fed_early), _minutes(fed_late)
        rule_note = (
            f"federal {fed_early}-{fed_late} [{_short(federal['legal_basis'])}]; "
            f"no stricter {jurisdiction} rule in force at {as_of}"
        )
        state_rule_id = None

    if reference is None:
        return CallingWindow(
            earliest=_hhmm(early), latest=_hhmm(late),
            basis=f"coords:none npa:{npa or 'none'}->none; no zone; {rule_note}",
            zone=None, coordinate_zone=None, npa=npa, npa_zones=(),
            intersected=False, narrowed_minutes=0, day=day, state_rule_id=state_rule_id,
        )

    # Every zone either source considers possible, de-duplicated but ordered
    # coordinate-first so the basis reads in the order the assignment demands.
    candidates: list[str] = []
    for zone in ([coordinate_zone] if coordinate_zone else []) + list(npa_zones):
        if zone and zone not in candidates:
            candidates.append(zone)

    # Offsets are taken at NOON on the day in question: it is inside the
    # permitted window in every zone, and it is never the DST transition hour,
    # so no candidate zone's offset is ambiguous or nonexistent at that
    # instant. Phase 4 made the same call for a different reason.
    noon = datetime.combine(day_date, time(12, 0))
    ref_offset = _offset_minutes(reference, noon)

    shifted_early, shifted_late = early, late
    for zone in candidates:
        delta = ref_offset - _offset_minutes(zone, noon)
        shifted_early = max(shifted_early, early + delta)
        shifted_late = min(shifted_late, late + delta)

    intersected = len(candidates) > 1
    narrowed = (late - early) - (shifted_late - shifted_early)
    if shifted_late < shifted_early:
        # Unreachable across NANP zones (a 3-hour spread against a 12-hour
        # window), but recorded rather than silently repaired if it ever is.
        log.warning("empty calling-window intersection over %s", candidates)
        shifted_late = shifted_early

    if npa_zones:
        npa_render = f"npa:{npa}->{{{', '.join(npa_zones)}}}"
    elif npa:
        # A third answer, distinct from "agrees" and from "disagrees": the
        # fallback has nothing to say, so the coordinate stands alone and the
        # basis says which of the three happened.
        npa_render = f"npa:{npa}->none (not in config/npa_timezone.csv)"
    else:
        npa_render = "npa:none"
    agreement = "=" if (not intersected) else "∩"
    basis = (
        f"coords:{coordinate_zone or 'none'} {agreement} {npa_render}; "
        f"{rule_note}; expressed in {reference} on {day} ({day_date}) "
        f"-> {_hhmm(shifted_early)}-{_hhmm(shifted_late)}"
        + (f"; intersection narrowed the window by {narrowed} min" if narrowed else "")
    )
    return CallingWindow(
        earliest=_hhmm(shifted_early), latest=_hhmm(shifted_late), basis=basis,
        zone=reference, coordinate_zone=coordinate_zone, npa=npa,
        npa_zones=tuple(npa_zones), intersected=intersected,
        narrowed_minutes=max(0, narrowed), day=day, state_rule_id=state_rule_id,
    )


def _short(basis: Any, limit: int = 64) -> str:
    """The first clause of a citation, for the one-line `basis` string.

    The FULL citation is on the decision's `legal_basis` array; this is the
    breadcrumb that tells a reader which rule to go and look at.
    """
    text = " ".join(str(basis).split())
    text = text.split(":")[0].split(" (")[0].split("(the")[0].strip().rstrip(".")
    return text if len(text) <= limit else text[: limit - 1] + "…"
