"""ERA5 reanalysis from Open-Meteo, joined on the UTC hour.

Why reanalysis and not station observations
-------------------------------------------
ERA5 is a physical model re-run over assimilated observations on a ~9-25 km
grid. It is spatially smooth, temporally complete and **never missing** -- every
hour of every cell since 1940 has a value. NOAA GHCNh is the opposite trade: a
real thermometer at a real point, accurate where it exists and frequently absent
where it does not, with station moves and instrument changes in the record.

For a crash pipeline the choice is not close. A missing weather value is not a
missing feature, it is a systematically missing feature -- rural crashes are
further from stations than urban ones, so a station join silently biases every
downstream rate by population density. Reanalysis trades point accuracy for
completeness, and completeness is what makes the column safe to compare across
269,000 crashes. What it costs is honesty about resolution: an ERA5 cell is
larger than a thunderstorm, so "it was raining in this 9 km box in this hour" is
a weaker claim than the officer's "it was raining here". The build measures the
disagreement between the two rather than asserting either is right (see
`precipitation_agreement`).

(ASSIGNMENT.md notes that NOAA's ISD was superseded and relocated to S3 in
mid-2026, so any tutorial pointing at `ncei.noaa.gov/data/global-hourly/` is
stale. Nothing here points at it.)


Why the join key is UTC, and why that means tz runs first
---------------------------------------------------------
Open-Meteo is asked for `timezone=UTC`, so its hourly index IS a UTC instant.
The crash side has a UTC instant only after `src/geo/tz.py` has localised the
naive wall clock. Joining on local time would be wrong twice a year by exactly
one hour, in opposite directions, on the two days each year with the most
weather-related crashes -- and it would be wrong invisibly. Stage order is a
correctness constraint here, not a convenience.


Request budget
--------------
Not one request per crash: 124,900 Montgomery crashes against a 600/min,
10,000/day budget is a five-hour job to learn ~30 cells' worth of weather.
Instead the crash locations are bucketed to **H3 r5** cells (~247 km2, ~8.5 km
edge -- just coarser than ERA5's grid, so one request per cell is at most one
request per grid box) and one request is made per (cell, calendar year), because
Open-Meteo weights a long hourly range as multiple calls and a year-sized
request stays a single one. Every request is counted in the manifest against
both published limits.

Raw responses are cached byte-for-byte under `data/reference/open_meteo/`. A
re-run with a warm cache makes **zero** network calls; the test asserts that by
pointing the cache at a populated tmp dir and handing in a client that raises on
any request.

Scope, and what the unbounded version costs
-------------------------------------------
Montgomery County from 2024-01-01, per `config/geo.toml [weather]`. The whole
corpus (three jurisdictions, 2015-2026, ~1,900 r5 cells x 12 years) is ~23,000
requests: three days against the 10,000/day cap, or one day at the documented
commercial tier. That is a scheduled batch with a resume cursor, not a stage in
an interactive build -- the report says how it would be run.
"""

from __future__ import annotations

import json
import logging
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Iterable, Mapping, Sequence

import pandas as pd

from .. import config
from ..ingest.http import HttpClient
from ..ingest.watermark import durable_replace
from . import h3_index

log = logging.getLogger("geo.weather")

WEATHER_JOINED = "JOINED"
WEATHER_NOT_IN_SCOPE = "NOT_IN_SCOPE"
WEATHER_NO_GEOMETRY = "NO_GEOMETRY"
WEATHER_NO_TIME = "NO_TIME"
WEATHER_NO_DATA = "NO_DATA"
WEATHER_STATUS_VALUES = (
    WEATHER_JOINED, WEATHER_NOT_IN_SCOPE, WEATHER_NO_GEOMETRY,
    WEATHER_NO_TIME, WEATHER_NO_DATA,
)

# Open-Meteo's default units for the requested variables, spelled out because
# they end up in the column names and a silent unit change upstream would
# otherwise be invisible. Asserted against `hourly_units` in every response.
EXPECTED_UNITS = {
    "temperature_2m": "°C",
    "precipitation": "mm",
    "rain": "mm",
    "snowfall": "cm",
    "weather_code": "wmo code",
    "wind_speed_10m": "km/h",
}

# Suffix per variable, so `era5_snowfall_cm` says its unit in its name.
COLUMN_SUFFIX = {
    "temperature_2m": "_c",
    "precipitation": "_mm",
    "rain": "_mm",
    "snowfall": "_cm",
    "weather_code": "",
    "wind_speed_10m": "_kmh",
}


def era5_columns() -> list[str]:
    hourly = config.geo()["weather"]["hourly"]
    return [f"era5_{v}{COLUMN_SUFFIX[v]}" for v in hourly]


class RequestBudget:
    """Counts calls against Open-Meteo's published 600/min and 10,000/day.

    A counter rather than a limiter: the client already paces at <= 5 req/s, so
    the budget's job is to make the number VISIBLE in the manifest. "We made 47
    requests against a 10,000/day cap" is the sentence that turns a design
    choice into evidence.
    """

    def __init__(self) -> None:
        cfg = config.geo()["weather"]
        self.per_minute = int(cfg["max_requests_per_minute"])
        self.per_day = int(cfg["max_requests_per_day"])
        self.requests = 0
        self.cache_hits = 0
        self.generation_ms: list[float] = []

    def spend(self, n: int = 1) -> None:
        self.requests += n
        if self.requests > self.per_day:
            raise RuntimeError(
                f"Open-Meteo daily budget exhausted ({self.requests} > {self.per_day}). "
                "Narrow config/geo.toml [weather] or schedule the backfill."
            )

    def as_dict(self) -> dict[str, Any]:
        gen = self.generation_ms
        return {
            "requests": self.requests,
            "cache_hits": self.cache_hits,
            "limit_per_minute": self.per_minute,
            "limit_per_day": self.per_day,
            "fraction_of_daily_limit": round(self.requests / self.per_day, 6),
            "generationtime_ms_total": round(sum(gen), 3) if gen else None,
        }


# ---------------------------------------------------------------------------
# cache + fetch
# ---------------------------------------------------------------------------


def cache_path(root: Path, cell: str, year: int) -> Path:
    return Path(root) / "open_meteo" / str(year) / f"{cell}.json"


def _request_url(lat: float, lon: float, start: date, end: date) -> tuple[str, dict[str, Any]]:
    cfg = config.geo()["weather"]
    return config.sources()["weather"]["open_meteo"], {
        "latitude": round(float(lat), 4),
        "longitude": round(float(lon), 4),
        "start_date": start.isoformat(),
        "end_date": end.isoformat(),
        "hourly": ",".join(cfg["hourly"]),
        "timezone": cfg["timezone"],
    }


def fetch_cell_year(
    root: Path,
    cell: str,
    year: int,
    *,
    start: date,
    end: date,
    budget: RequestBudget,
    client: HttpClient | None = None,
    offline: bool = False,
) -> dict[str, Any] | None:
    """One (cell, year) response, from cache or from the archive API.

    The cache check comes first and returns before a client is even touched,
    which is what makes "warm cache => zero network calls" a structural
    property rather than a hope.
    """
    dest = cache_path(root, cell, year)
    if dest.exists():
        budget.cache_hits += 1
        return json.loads(dest.read_text())
    if offline:
        raise FileNotFoundError(
            f"{dest} is not cached and --offline was requested.\n"
            f"  run without --offline, or with --skip-weather to leave every row "
            f"weather_status = '{WEATHER_NOT_IN_SCOPE}'"
        )

    lat, lon = h3_index.h3.cell_to_latlng(cell)
    url, params = _request_url(lat, lon, start, end)
    if client is None:
        client = HttpClient(min_interval=float(config.geo()["weather"]["min_interval_s"]))
    budget.spend()
    response = client.get(url, params=params)
    # Raw bytes to disk before anything parses them -- the same rule bronze
    # follows, for the same reason: the cached artefact must be the server's
    # answer, not our reading of it.
    dest.parent.mkdir(parents=True, exist_ok=True)
    tmp = dest.with_name(dest.name + ".part")
    tmp.write_bytes(response.content)
    durable_replace(tmp, dest)
    payload = json.loads(response.content)
    if "generationtime_ms" in payload:
        budget.generation_ms.append(float(payload["generationtime_ms"]))
    return payload


def response_to_frame(cell: str, payload: Mapping[str, Any]) -> pd.DataFrame:
    """One Open-Meteo response to a tidy hourly frame, units checked.

    `era5_grid_lat` / `era5_grid_lon` are the coordinates the SERVER returned,
    which are the grid-cell centre and differ from the ones we asked for. That
    difference is the resolution of the product made visible, so it is stored
    per row rather than thrown away.
    """
    hourly = payload.get("hourly") or {}
    units = payload.get("hourly_units") or {}
    times = hourly.get("time") or []
    if not times:
        return pd.DataFrame()

    for var, expected in EXPECTED_UNITS.items():
        if var in units and units[var] != expected:
            log.warning("Open-Meteo returned %s in %s, expected %s -- column names "
                        "encode the unit and are now wrong", var, units[var], expected)

    # timezone=UTC was requested, so the index is already UTC instants; the
    # localisation is asserted rather than assumed.
    tzname = str(payload.get("timezone", ""))
    if tzname not in ("UTC", "GMT"):
        raise ValueError(f"Open-Meteo returned timezone={tzname!r}, expected UTC")

    df = pd.DataFrame({"era5_hour_utc": pd.to_datetime(times, utc=True)})
    df["h3_r5"] = cell
    for var in config.geo()["weather"]["hourly"]:
        col = f"era5_{var}{COLUMN_SUFFIX[var]}"
        values = hourly.get(var)
        df[col] = pd.Series(values, dtype="float64") if values is not None else pd.NA
    df["era5_grid_lat"] = float(payload.get("latitude", float("nan")))
    df["era5_grid_lon"] = float(payload.get("longitude", float("nan")))
    return df


# ---------------------------------------------------------------------------
# the stage
# ---------------------------------------------------------------------------


def in_scope(df: pd.DataFrame) -> pd.Series:
    """Which crash rows the configured slice covers.

    Scope is three conditions, each of which produces a DIFFERENT status, so a
    NULL weather column always has a stated reason: the source system is in
    scope, the row has a geometry, and it has a UTC instant to key on.
    """
    cfg = config.geo()["weather"]
    start = cfg["start_date"]
    start = start if isinstance(start, date) else date.fromisoformat(str(start))
    return (
        df["primary_source_system"].isin(list(cfg["source_systems"]))
        & (pd.to_datetime(df["crash_date"]).dt.date >= start)
    )


def status_for(df: pd.DataFrame, scope: pd.Series) -> pd.Series:
    status = pd.Series(WEATHER_NOT_IN_SCOPE, index=df.index, dtype="object")
    status[scope & df["h3_r5"].isna()] = WEATHER_NO_GEOMETRY
    status[scope & df["h3_r5"].notna() & df["crash_datetime_utc"].isna()] = WEATHER_NO_TIME
    return status


def build(
    df: pd.DataFrame,
    *,
    reference_root: Path,
    client: HttpClient | None = None,
    offline: bool = False,
    enabled: bool = True,
) -> tuple[pd.DataFrame, dict[str, Any]]:
    """Attach ERA5 columns to `df`, keyed on (H3 r5 cell, UTC hour).

    `df` must already carry `crash_datetime_utc` (from `tz.py`), `latitude`,
    `longitude`, `crash_date` and `primary_source_system`. Returns the ERA5
    columns indexed like `df`, plus the stats the manifest records.
    """
    budget = RequestBudget()
    cfg = config.geo()["weather"]
    out = pd.DataFrame(index=df.index)
    out["h3_r5"] = [
        h3_index.cell(lat, lon, int(config.geo()["h3"]["weather_resolution"]))
        for lat, lon in zip(df["latitude"], df["longitude"])
    ]
    scope = in_scope(df) if enabled else pd.Series(False, index=df.index)
    work = pd.concat([df, out["h3_r5"]], axis=1)
    out["weather_status"] = status_for(work, scope)
    out["era5_hour_utc"] = pd.Series(pd.NaT, index=df.index, dtype="datetime64[us, UTC]")
    for col in (*era5_columns(), "era5_grid_lat", "era5_grid_lon"):
        out[col] = pd.Series(float("nan"), index=df.index, dtype="float64")

    joinable = scope & out["h3_r5"].notna() & df["crash_datetime_utc"].notna()
    if not enabled or not joinable.any():
        stats = {
            "enabled": enabled,
            "in_scope_rows": int(scope.sum()),
            "joinable_rows": int(joinable.sum()),
            "cells": 0, "cell_years": 0, "joined": 0,
            "budget": budget.as_dict(),
            "slice": {"source_systems": list(cfg["source_systems"]),
                      "start_date": str(cfg["start_date"])},
        }
        return out.drop(columns=["h3_r5"]), stats

    keys = pd.DataFrame({
        "h3_r5": out.loc[joinable, "h3_r5"],
        # The join key is the UTC hour floor: ERA5 publishes hourly means/
        # accumulations stamped at the hour they begin.
        "era5_hour_utc": pd.to_datetime(
            df.loc[joinable, "crash_datetime_utc"], utc=True
        ).dt.floor("h"),
    })
    keys["year"] = keys["era5_hour_utc"].dt.year

    frames: list[pd.DataFrame] = []
    cell_years = sorted({(c, int(y)) for c, y in zip(keys["h3_r5"], keys["year"])})
    for cell, year in cell_years:
        payload = fetch_cell_year(
            reference_root, cell, year,
            start=date(year, 1, 1), end=_year_end(year),
            budget=budget, client=client, offline=offline,
        )
        if payload:
            frames.append(response_to_frame(cell, payload))

    if not frames:
        out.loc[joinable, "weather_status"] = WEATHER_NO_DATA
        return out.drop(columns=["h3_r5"]), {
            "enabled": enabled, "in_scope_rows": int(scope.sum()),
            "joinable_rows": int(joinable.sum()), "cells": 0,
            "cell_years": len(cell_years), "joined": 0, "budget": budget.as_dict(),
        }

    era5 = pd.concat(frames, ignore_index=True).drop_duplicates(
        subset=["h3_r5", "era5_hour_utc"], keep="first"
    )
    merged = keys.merge(era5, on=["h3_r5", "era5_hour_utc"], how="left")
    merged.index = keys.index

    value_cols = ["era5_hour_utc", *era5_columns(), "era5_grid_lat", "era5_grid_lon"]
    hit = merged["era5_grid_lat"].notna()
    for col in value_cols:
        out.loc[merged.index[hit], col] = merged.loc[hit, col].to_numpy()
    out.loc[merged.index[hit], "weather_status"] = WEATHER_JOINED
    out.loc[merged.index[~hit], "weather_status"] = WEATHER_NO_DATA

    stats = {
        "enabled": enabled,
        "in_scope_rows": int(scope.sum()),
        "joinable_rows": int(joinable.sum()),
        "cells": len({c for c, _ in cell_years}),
        "cell_years": len(cell_years),
        "joined": int(hit.sum()),
        "unjoined": int((~hit).sum()),
        "era5_hours": int(len(era5)),
        "budget": budget.as_dict(),
        "slice": {"source_systems": list(cfg["source_systems"]),
                  "start_date": str(cfg["start_date"]),
                  "bucket": f"h3_r{config.geo()['h3']['weather_resolution']}"},
        "status_counts": {k: int(v) for k, v in
                          out["weather_status"].value_counts().sort_index().items()},
    }
    return out.drop(columns=["h3_r5"]), stats


def _year_end(year: int) -> date:
    """31 December, or yesterday for the current year.

    The ERA5 archive lags real time by about five days; asking for a future
    date returns nulls that would look like missing weather rather than like
    weather that has not happened yet.
    """
    today = datetime.now(timezone.utc).date()
    end = date(year, 12, 31)
    lag = today - timedelta(days=6)
    return min(end, lag) if year >= today.year else end


# ---------------------------------------------------------------------------
# the analytic sentence the memo wants
# ---------------------------------------------------------------------------


def precipitation_agreement(
    df: pd.DataFrame,
    *,
    officer_col: str = "officer_is_precipitation",
    era5_col: str = "era5_precipitation_mm",
    status_col: str = "weather_status",
) -> dict[str, Any]:
    """Officer-reported precipitation vs ERA5 `> 0`, on the joined rows.

    A confusion matrix, not an accuracy: the two are not measuring the same
    thing and neither is ground truth. The officer records conditions at a
    point at the moment of the crash and frequently records nothing at all;
    ERA5 records the mean over a ~9-25 km box over a whole hour and is never
    missing. Disagreement is mostly the resolution gap -- 0.1 mm smeared over
    an hour and a county is not rain a driver would notice -- and it is exactly
    why the ERA5 column is a context feature rather than a causal one.
    """
    joined = df[(df[status_col] == WEATHER_JOINED) & df[era5_col].notna()]
    if joined.empty:
        return {"joined_rows": 0}
    era5_wet = joined[era5_col] > 0
    officer = joined[officer_col]
    known = officer.notna()
    both = joined[known]
    o = officer[known].astype(bool)
    e = era5_wet[known]
    return {
        "joined_rows": int(len(joined)),
        "officer_known_rows": int(known.sum()),
        "officer_wet": int(o.sum()),
        "era5_wet": int(e.sum()),
        "both_wet": int((o & e).sum()),
        "both_dry": int((~o & ~e).sum()),
        "officer_wet_era5_dry": int((o & ~e).sum()),
        "officer_dry_era5_wet": int((~o & e).sum()),
        "agreement_rate": round(float(((o & e) | (~o & ~e)).mean()), 6) if len(both) else None,
        "era5_wet_rate": round(float(e.mean()), 6) if len(both) else None,
        "officer_wet_rate": round(float(o.mean()), 6) if len(both) else None,
    }
