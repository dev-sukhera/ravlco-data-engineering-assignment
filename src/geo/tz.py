"""Coordinate-derived IANA timezone, and the naive local clock localised to UTC.

This is a compliance control, not a geography nicety. Part 5's calling windows
are computed from the timestamp this module produces; an hour of error here is a
call placed outside a permitted window, which is a TCPA problem and not a
rounding problem. Everything below is written for that stake.


Three things, in order
----------------------
1. **Zone from the coordinate.** `timezonefinder` 8.x, offline, on the
   Timezone Boundary Builder polygons. Not from the state (Texas is Central
   *and* Mountain; Florida is Eastern *and* Central), not from an area code,
   not from the county name. `tz_source = 'COORDINATE'`.

2. **A fallback with visible provenance for rows that have no coordinate.**
   The county's zone, derived FROM THE DATA: the modal coordinate-derived zone
   among the rows in that county that do have coordinates, together with the
   share that mode holds. A county whose share is below 1.0 is genuinely split
   (Gulf County, FL is the textbook case) and its fallback is marked
   low-confidence. `tz_source = 'COUNTY_FALLBACK'`. With no county at all, the
   jurisdiction default from `config/geo.toml [tz.jurisdiction_default]`:
   `tz_source = 'JURISDICTION_DEFAULT'`. Phase 6 turns every non-`COORDINATE`
   source into `GEOCODE_TIER_INSUFFICIENT`; this module's only job is to make
   which-one-happened unambiguous.

3. **Localise, do not convert.** All three feeds publish a naive LOCAL wall
   clock. `crash_datetime_local` is that clock, stored as a naive TIMESTAMP by
   Phase 2 precisely so nobody could mistake it for UTC. Attaching UTC to it
   and "converting" would shift every Maryland crash by four or five hours in
   the wrong direction -- the error the assignment warns about twice.


DST: the policy, and why the flag matters more than the policy
--------------------------------------------------------------
Twice a year a local wall clock is not a function of an instant.

  * **Spring forward.** 2024-03-10 02:30 in `America/New_York` never happened.
    Policy: shift forward by the size of the gap (02:30 -> 03:30 EDT ->
    07:30Z) and set `tz_gap_adjusted`.
  * **Fall back.** 2024-11-03 01:30 in `America/New_York` happened twice.
    Policy: `fold = 0`, the first occurrence, DST still in effect (-04:00 ->
    05:30Z) and set `tz_ambiguous`.

Neither policy is *correct*, and that is the point. The source published a wall
clock that is either impossible or ambiguous; no rule recovers the instant it
meant, and a pipeline that silently picks one has manufactured certainty. What
makes the row safe downstream is the FLAG: an eligibility rule can refuse to
compute a calling window from a timestamp known to be an hour uncertain, and it
can only do that if the uncertainty survived into the table. Both policies are
config (`[tz.dst]`); both flags are columns.

Detection is explicit -- `utcoffset()` under `fold=0` and `fold=1` are compared
directly -- rather than delegated to a library's normalisation, because the
whole value of the flag is that it was *decided*, not inherited. `zoneinfo`
(CPython stdlib, system tzdata) is the implementation; no third-party tz
database is introduced beyond `timezonefinder`'s polygons.


Measured on the local corpus, 2026-09-08 (`python -m src.geo.build --json`):
the counts per zone, per `tz_source`, and the per-year gap/ambiguous row counts
are in `ai docs/implementation/phase4-geo-report.md`. They are reproduced by
that one command.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from functools import lru_cache
from typing import Any, Iterable, Mapping, Sequence
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError

from .. import config

log = logging.getLogger("geo.tz")

TZ_SOURCE_COORDINATE = "COORDINATE"
TZ_SOURCE_COUNTY = "COUNTY_FALLBACK"
TZ_SOURCE_JURISDICTION = "JURISDICTION_DEFAULT"
TZ_SOURCE_UNRESOLVED = "UNRESOLVED"
TZ_SOURCES = (
    TZ_SOURCE_COORDINATE,
    TZ_SOURCE_COUNTY,
    TZ_SOURCE_JURISDICTION,
    TZ_SOURCE_UNRESOLVED,
)

TIME_OK = "OK"
TIME_UNKNOWN = "TIME_UNKNOWN"
TIME_STATUS_VALUES = (TIME_OK, TIME_UNKNOWN)

UTC = timezone.utc


# ---------------------------------------------------------------------------
# zone from coordinate
# ---------------------------------------------------------------------------


class ZoneFinder:
    """`timezonefinder` behind one call, loaded once.

    `in_memory=True` reads the whole polygon set up front: ~50 MB of RAM to
    turn a 268k-row lookup from disk-seek-bound into CPU-bound. It is a pure
    function of (lat, lon) and of the shipped Timezone Boundary Builder
    vintage, so it is deterministic and the vintage is recorded in the geo
    manifest as `timezonefinder_version`.
    """

    def __init__(self, finder: Any | None = None) -> None:
        if finder is None:
            from timezonefinder import TimezoneFinder

            finder = TimezoneFinder(in_memory=True)
        self._finder = finder

    def zone_at(self, lat: float | None, lon: float | None) -> str | None:
        """IANA zone for a WGS84 coordinate, or None if it is in no polygon.

        No projection: `timezonefinder` tests a lon/lat point against lon/lat
        polygons, which is a topological question. Reprojecting first would be
        pure loss.
        """
        if lat is None or lon is None:
            return None
        try:
            return self._finder.timezone_at(lng=float(lon), lat=float(lat))
        except (ValueError, TypeError):
            return None

    def zones_for(
        self, coords: Iterable[tuple[float | None, float | None]]
    ) -> list[str | None]:
        return [self.zone_at(lat, lon) for lat, lon in coords]


# ---------------------------------------------------------------------------
# localisation
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class Localised:
    """One naive local stamp resolved to an instant, with its uncertainty."""

    utc: datetime | None
    offset_minutes: int | None
    gap_adjusted: bool
    ambiguous: bool
    time_status: str

    @property
    def ok(self) -> bool:
        return self.time_status == TIME_OK


NO_TIME = Localised(None, None, False, False, TIME_UNKNOWN)


@lru_cache(maxsize=512)
def _zone(name: str) -> ZoneInfo:
    return ZoneInfo(name)


def localise(naive: datetime | None, zone_name: str | None) -> Localised:
    """Localise a naive LOCAL wall clock to UTC. Never converts.

    Returns `NO_TIME` when there is no stamp or no zone -- an unresolvable row
    keeps its `crash_date` and gets `time_status = 'TIME_UNKNOWN'` with a NULL
    UTC, because collapsing an unknown hour to midnight would invent a crash
    time (Phase 2 makes the same call for FARS `HOUR = 99`).

    The gap/ambiguity test is the explicit one: attach the zone under `fold=0`
    and `fold=1` and compare the two UTC offsets.

      * equal          -> an ordinary instant.
      * fold0 < fold1  -> the wall clock is in the spring-forward GAP. The
                          offset *before* the transition is the smaller one, so
                          a smaller fold-0 offset means the clock jumped over
                          this time. Shift forward by (fold1 - fold0).
      * fold0 > fold1  -> the wall clock is AMBIGUOUS (fall-back). fold=0 is
                          the first occurrence, DST still in effect.

    A round-trip check backs up the gap branch rather than replacing it: after
    shifting, `utc -> local` must reproduce the shifted wall clock.
    """
    if naive is None or zone_name is None:
        return NO_TIME
    if naive.tzinfo is not None:
        raise ValueError(
            f"localise() takes a NAIVE local stamp; got {naive!r} with tzinfo. "
            "A tz-aware value here means somebody already converted."
        )
    try:
        zone = _zone(zone_name)
    except (ZoneInfoNotFoundError, ValueError):
        log.warning("unknown IANA zone %r", zone_name)
        return NO_TIME

    off0 = naive.replace(tzinfo=zone, fold=0).utcoffset()
    off1 = naive.replace(tzinfo=zone, fold=1).utcoffset()
    if off0 is None or off1 is None:  # pragma: no cover - zoneinfo always answers
        return NO_TIME

    gap_adjusted = False
    ambiguous = False
    wall = naive
    if off0 < off1:
        # Nonexistent local time. Policy: shift forward by the gap.
        gap_adjusted = True
        wall = naive + (off1 - off0)
    elif off0 > off1:
        # Repeated local time. Policy: fold=0, the earlier (DST) offset.
        ambiguous = True

    aware = wall.replace(tzinfo=zone, fold=config.geo()["tz"]["dst"]["ambiguous_fold"])
    offset = aware.utcoffset()
    assert offset is not None
    utc = aware.astimezone(UTC)

    if gap_adjusted:
        # The shift must land on a real instant; if it did not, the zone's
        # transition is not the shape we assumed and we would rather know.
        back = utc.astimezone(zone).replace(tzinfo=None)
        if back != wall:  # pragma: no cover - defensive
            log.warning("gap shift for %s in %s did not round-trip (%s)",
                        naive, zone_name, back)

    return Localised(
        utc=utc.replace(tzinfo=UTC),
        offset_minutes=int(offset.total_seconds() // 60),
        gap_adjusted=gap_adjusted,
        ambiguous=ambiguous,
        time_status=TIME_OK,
    )


def utc_offset_minutes(naive: datetime, zone_name: str) -> int:
    """Convenience for tests and for Phase 7's sun-time work."""
    return localise(naive, zone_name).offset_minutes or 0


# ---------------------------------------------------------------------------
# fallbacks
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class CountyZone:
    """A county's zone as the data itself reports it."""

    county_geoid: str
    tz_iana: str
    n_coordinate_rows: int
    modal_rows: int
    split_tz: bool

    @property
    def share(self) -> float:
        return self.modal_rows / self.n_coordinate_rows if self.n_coordinate_rows else 0.0


def county_zone_table(
    pairs: Iterable[tuple[str | None, str | None]]
) -> dict[str, CountyZone]:
    """Modal coordinate-derived zone per county, from `(county_geoid, tz_iana)`.

    Derived from the corpus rather than from a lookup table on purpose: a
    shipped county->zone table is one more thing to keep current, and the
    crashes we actually hold are the evidence we actually have. The cost is
    that a county with no geocoded row gets no entry -- which is correct, and
    is why `JURISDICTION_DEFAULT` exists.

    Ties are broken by zone name so the table is deterministic; a tie is by
    definition a split county and is flagged as one.
    """
    counts: dict[str, dict[str, int]] = {}
    for county, zone in pairs:
        if not county or not zone:
            continue
        counts.setdefault(str(county), {}).setdefault(zone, 0)
        counts[str(county)][zone] += 1

    out: dict[str, CountyZone] = {}
    for county, by_zone in counts.items():
        total = sum(by_zone.values())
        # -count first, then the name: deterministic, and a tie is a split.
        best, best_n = sorted(by_zone.items(), key=lambda kv: (-kv[1], kv[0]))[0]
        out[county] = CountyZone(
            county_geoid=county,
            tz_iana=best,
            n_coordinate_rows=total,
            modal_rows=best_n,
            split_tz=best_n < total,
        )
    return out


def jurisdiction_default(jurisdiction: str | None) -> str | None:
    if not jurisdiction:
        return None
    return config.geo()["tz"]["jurisdiction_default"].get(jurisdiction)


def resolve_zone(
    *,
    coordinate_zone: str | None,
    county_geoid: str | None,
    jurisdiction: str | None,
    county_zones: Mapping[str, CountyZone],
) -> tuple[str | None, str, bool]:
    """(`tz_iana`, `tz_source`, `low_confidence`) for one row.

    The precedence is the whole point and it never varies: the coordinate wins
    if there is one, then the county the data says it is in, then the
    jurisdiction. `low_confidence` is True for any fallback whose county is
    split, and for every `JURISDICTION_DEFAULT`.
    """
    if coordinate_zone:
        return coordinate_zone, TZ_SOURCE_COORDINATE, False
    if county_geoid:
        cz = county_zones.get(str(county_geoid))
        if cz is not None:
            return cz.tz_iana, TZ_SOURCE_COUNTY, cz.split_tz
    default = jurisdiction_default(jurisdiction)
    if default:
        return default, TZ_SOURCE_JURISDICTION, True
    return None, TZ_SOURCE_UNRESOLVED, True


# ---------------------------------------------------------------------------
# reporting
# ---------------------------------------------------------------------------


def dst_census(
    rows: Sequence[tuple[Any, Localised]]
) -> dict[str, Any]:
    """Gap and ambiguous row counts per year -- the memo's off-by-one evidence.

    These are the rows a naive pipeline gets wrong by exactly one hour. There
    are a handful each March and November and they are the reason the flags
    exist, so the number is reported rather than merely available.
    """
    gap: dict[int, int] = {}
    amb: dict[int, int] = {}
    for year, loc in rows:
        if loc.gap_adjusted:
            gap[year] = gap.get(year, 0) + 1
        if loc.ambiguous:
            amb[year] = amb.get(year, 0) + 1
    return {
        "gap_adjusted_by_year": dict(sorted(gap.items())),
        "ambiguous_by_year": dict(sorted(amb.items())),
        "gap_adjusted_total": sum(gap.values()),
        "ambiguous_total": sum(amb.values()),
    }


def timezonefinder_version() -> str:
    try:
        import timezonefinder

        return str(getattr(timezonefinder, "__version__", "unknown"))
    except ImportError:  # pragma: no cover
        return "unavailable"
