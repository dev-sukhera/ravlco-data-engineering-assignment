"""Pure score features with field-level provenance and explicit availability.

No function in this module sees identity, channel, consent, or neighbourhood
attributes.  A missing crash fact contributes zero and remains visible as
UNAVAILABLE; it is never replaced with a group average.
"""

from __future__ import annotations

import math
from dataclasses import dataclass
from datetime import date, datetime
from typing import Any, Mapping

OK = "OK"
UNAVAILABLE = "UNAVAILABLE"


@dataclass(frozen=True)
class Feature:
    name: str
    value: Any
    source_field: str
    source_table: str
    transform: str
    status: str
    contribution: float

    def component(self, precision: int) -> dict[str, Any]:
        return {
            "contribution": round(float(self.contribution), precision),
            "source": f"{self.source_table}.{self.source_field}",
            "status": self.status,
            "transform": self.transform,
            "value": self.value,
        }


def severity(row: Mapping[str, Any], cfg: Mapping[str, Any]) -> Feature:
    raw = _present(row.get("severity_ordinal"))
    table, field = "gold.fact_crash", "severity_ordinal"
    if raw is None:
        return _missing("severity", field, table, "configured ordinal points table")
    ordinal = int(raw)
    points = cfg["severity_points"]
    if str(ordinal) not in points:
        raise ValueError(f"severity ordinal {ordinal} has no configured points")
    return Feature("severity", ordinal, field, table,
                   "configured ordinal points table", OK, float(points[str(ordinal)]))


def recency(row: Mapping[str, Any], cfg: Mapping[str, Any]) -> Feature:
    raw = _present(row.get("incident_date"))
    if raw is None:
        return _missing("recency", "incident_date", "leads",
                        "exponential decay from frozen as_of")
    incident = _date(raw)
    as_of = _date(cfg["as_of_date"])
    age = max(0, (as_of - incident).days)
    half_life = float(cfg["recency_half_life_days"])
    contribution = float(cfg["recency_max_points"]) * math.pow(0.5, age / half_life)
    return Feature("recency", age, "incident_date", "leads",
                   f"days old; exponential half-life {half_life:g} days",
                   OK, contribution)


def vulnerable_road_user(row: Mapping[str, Any], cfg: Mapping[str, Any]) -> Feature:
    ped, bike = _present(row.get("pedestrian_involved")), _present(row.get("bicyclist_involved"))
    source = "pedestrian_involved,bicyclist_involved"
    if ped is None and bike is None:
        return _missing("vulnerable_road_user", source, "gold.fact_crash", "boolean OR")
    value = bool(ped) or bool(bike)
    return Feature("vulnerable_road_user", value, source, "gold.fact_crash",
                   "boolean OR", OK, float(cfg["vulnerable_road_user_points"]) if value else 0.0)


def hit_run(row: Mapping[str, Any], cfg: Mapping[str, Any]) -> Feature:
    raw = _present(row.get("hit_run"))
    if raw is None:
        return _missing("hit_run", "hit_run", "gold.fact_crash", "boolean flag")
    value = bool(raw)
    return Feature("hit_run", value, "hit_run", "gold.fact_crash", "boolean flag",
                   OK, float(cfg["hit_run_points"]) if value else 0.0)


def road_context(row: Mapping[str, Any], cfg: Mapping[str, Any]) -> Feature:
    fhwa = _present(row.get("fhwa_class"))
    speed = _present(row.get("osm_maxspeed_mph"))
    snapped = row.get("snap_status") == "SNAPPED"
    if fhwa is not None:
        key = str(int(fhwa))
        points = float(cfg["road_context_points"].get(key, 0.0))
        return Feature("road_context", int(fhwa), "fhwa_class", "gold.dim_road_class",
                       "configured FHWA class points table", OK, points)
    if speed is not None and snapped:
        threshold = 45.0
        points = float(cfg["road_context_points"]["high_speed"]) if float(speed) >= threshold else 0.0
        return Feature("road_context", float(speed), "osm_maxspeed_mph", "gold.crash_geo",
                       "SNAPPED OSM speed >=45 mph fallback", OK, points)
    return _missing("road_context", "fhwa_class", "gold.dim_road_class",
                    "FHWA points; SNAPPED OSM speed fallback")


def adverse_weather(row: Mapping[str, Any], cfg: Mapping[str, Any]) -> Feature:
    coded = _present(row.get("is_adverse"))
    precip = _present(row.get("era5_precipitation_mm"))
    if coded is not None:
        value = bool(coded)
        return Feature("adverse_weather", value, "is_adverse",
                       "gold.dim_weather_condition", "boolean flag", OK,
                       float(cfg["adverse_weather_points"]) if value else 0.0)
    if precip is not None and row.get("weather_status") == "JOINED":
        threshold = float(cfg["era5_precipitation_threshold_mm"])
        value = float(precip) >= threshold
        return Feature("adverse_weather", float(precip), "era5_precipitation_mm",
                       "gold.crash_geo", f"JOINED precipitation >= {threshold:g} mm fallback",
                       OK, float(cfg["adverse_weather_points"]) if value else 0.0)
    return _missing("adverse_weather", "is_adverse", "gold.dim_weather_condition",
                    "adverse flag; JOINED ERA5 precipitation fallback")


FEATURES = {
    "severity": severity,
    "recency": recency,
    "vulnerable_road_user": vulnerable_road_user,
    "hit_run": hit_run,
    "road_context": road_context,
    "adverse_weather": adverse_weather,
}


def _missing(name: str, field: str, table: str, transform: str) -> Feature:
    return Feature(name, None, field, table, transform, UNAVAILABLE, 0.0)


def _present(value: Any) -> Any:
    if value is None:
        return None
    try:
        if value != value:
            return None
    except (TypeError, ValueError):
        pass
    return value


def _date(value: Any) -> date:
    if isinstance(value, datetime):
        return value.date()
    if isinstance(value, date):
        return value
    return date.fromisoformat(str(value)[:10])
