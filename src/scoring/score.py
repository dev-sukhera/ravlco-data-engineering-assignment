"""The pure additive score applied only after compliance admission."""

from __future__ import annotations

import hashlib
import json
import math
from dataclasses import dataclass
from datetime import date
from typing import Any, Mapping, Sequence

from .. import config
from . import features

SCORABLE = frozenset({"ELIGIBLE", "BLOCKED_UNTIL"})
CLAIM = ("Among records the compliance layer says may be contacted, order by the "
         "likely seriousness of the underlying crash and the freshness of the "
         "record, using only facts about the crash itself.")


@dataclass(frozen=True)
class Score:
    priority_score: float
    score_components: dict[str, Any]


def scoring_config(cfg: Mapping[str, Any] | None = None, *, as_of: date | str | None = None) -> dict[str, Any]:
    raw = dict((cfg or config.scoring())["scoring"] if "scoring" in (cfg or config.scoring()) else (cfg or {}))
    raw["severity_points"] = dict(raw["severity_points"])
    raw["road_context_points"] = dict(raw["road_context_points"])
    if as_of is not None:
        raw["as_of_date"] = as_of.isoformat() if isinstance(as_of, date) else str(as_of)
    names = list(raw.get("features", []))
    unknown = sorted(set(names) - set(features.FEATURES))
    if unknown:
        raise ValueError(f"configured scoring feature(s) have no implementation: {unknown}")
    numeric = ["rounding_precision", "recency_half_life_days", "recency_max_points",
               "vulnerable_road_user_points", "hit_run_points", "adverse_weather_points",
               "era5_precipitation_threshold_mm", "top_fraction", "folds"]
    values = [raw[k] for k in numeric]
    values += list(raw["severity_points"].values())
    values += list(raw["road_context_points"].values())
    if any(isinstance(v, bool) or not isinstance(v, (int, float)) or not math.isfinite(float(v)) for v in values):
        raise ValueError("all scoring weights and numeric parameters must be finite numbers")
    if float(raw["recency_half_life_days"]) <= 0:
        raise ValueError("recency_half_life_days must be positive")
    return raw


def config_sha256(cfg: Mapping[str, Any]) -> str:
    return hashlib.sha256(json.dumps(cfg, sort_keys=True, default=str).encode()).hexdigest()


def score_lead(inputs: Mapping[str, Any], *, as_of: date | str | None = None,
               cfg: Mapping[str, Any] | None = None, include_severity: bool = True) -> Score:
    settings = scoring_config(cfg, as_of=as_of)
    precision = int(settings["rounding_precision"])
    terms: dict[str, Any] = {}
    for name in settings["features"]:
        feature = features.FEATURES[name](inputs, settings)
        component = feature.component(precision)
        if name == "severity" and not include_severity:
            component["contribution"] = 0.0
            component["status"] = "EXCLUDED_BACKTEST"
        terms[name] = component
    available = sum(1 for item in terms.values() if item["status"] == features.OK)
    total = round(math.fsum(float(item["contribution"]) for item in terms.values()), precision)
    terms["coverage"] = {"available": available, "of": len(terms)}
    terms["ruleset"] = {"as_of": settings["as_of_date"],
                        "scoring_config_sha256": config_sha256(settings)}
    return Score(total, {key: terms[key] for key in sorted(terms)})


def score_leads(rows: Sequence[Mapping[str, Any]], *, as_of: date | str | None = None,
                cfg: Mapping[str, Any] | None = None) -> list[dict[str, Any]]:
    out: list[dict[str, Any]] = []
    for raw in rows:
        row = dict(raw)
        if row.get("eligibility_status") in SCORABLE:
            scored = score_lead(row, as_of=as_of, cfg=cfg)
            row["priority_score"] = scored.priority_score
            row["score_components"] = scored.score_components
        else:
            row["priority_score"] = None
            row["score_components"] = None
        out.append(row)
    return sorted(out, key=_rank_key)


def _rank_key(row: Mapping[str, Any]) -> tuple[Any, ...]:
    score = row.get("priority_score")
    incident = str(row.get("incident_date") or "")
    return (score is None, -(float(score) if score is not None else 0.0),
            tuple(-ord(ch) for ch in incident), str(row.get("lead_id") or ""))
