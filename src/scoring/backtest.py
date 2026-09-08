"""Reproducible temporal and spatial evaluation of the context-only score.

Severity is the outcome, so its score term is forced to zero here. County and
H3 are used only to assign evaluation folds; neither enters a contribution.
"""

from __future__ import annotations

import hashlib
import math
from pathlib import Path
from typing import Any, Mapping

import duckdb
import pandas as pd

from .score import config_sha256, score_lead, scoring_config


def load_crash_context(gold_root: Path | str, *, con=None) -> pd.DataFrame:
    gold = Path(gold_root)
    required = [gold / f"{name}.parquet" for name in
                ("fact_crash", "crash_geo", "dim_road_class", "dim_weather_condition")]
    missing = [str(path) for path in required if not path.exists()]
    if missing:
        raise FileNotFoundError("missing scoring input(s): " + ", ".join(missing))
    owned = con is None
    con = con or duckdb.connect()
    q = lambda path: str(path).replace("'", "''")
    try:
        return con.execute(f"""
            SELECT f.crash_sk, f.primary_source_system AS source_system,
                   f.jurisdiction, f.crash_date,
                   year(f.crash_date)::INTEGER AS year, f.severity_ordinal,
                   f.pedestrian_involved, f.bicyclist_involved, f.hit_run,
                   r.fhwa_class, w.is_adverse, g.snap_status,
                   g.osm_maxspeed_mph, g.weather_status,
                   g.era5_precipitation_mm,
                   g.pip_county_geoid AS county_block,
                   g.h3_r7 AS h3_r7_block
            FROM read_parquet('{q(required[0])}') f
            LEFT JOIN read_parquet('{q(required[1])}') g USING (crash_sk)
            LEFT JOIN read_parquet('{q(required[2])}') r USING (road_class_sk)
            LEFT JOIN read_parquet('{q(required[3])}') w USING (weather_condition_sk)
            WHERE f.severity_ordinal IS NOT NULL
            ORDER BY f.crash_sk
        """).df()
    finally:
        if owned:
            con.close()


def score_context(frame: pd.DataFrame, *, cfg: Mapping[str, Any] | None = None) -> pd.DataFrame:
    settings = scoring_config(cfg)
    threshold = int(settings["severe_threshold"])
    rows = []
    for raw in frame.to_dict("records"):
        result = score_lead(raw, cfg=settings, include_severity=False)
        ped = raw.get("pedestrian_involved")
        bike = raw.get("bicyclist_involved")
        vru = (False if pd.isna(ped) else bool(ped)) or (False if pd.isna(bike) else bool(bike))
        rows.append({**raw, "severe": int(raw["severity_ordinal"]) >= threshold,
                     "context_score": result.priority_score,
                     "vru_baseline_score": int(vru),
                     "score_components": result.score_components,
                     "_scoring_build_sha": config_sha256(settings)})
    return pd.DataFrame(rows)


def deterministic_fold(value: Any, folds: int, *, salt: str = "") -> int:
    key = "MISSING" if value is None or pd.isna(value) else str(value)
    digest = hashlib.sha256(f"{salt}|{key}".encode()).digest()
    return int.from_bytes(digest[:8], "big") % folds


def evaluate(scored: pd.DataFrame, *, cfg: Mapping[str, Any] | None = None
             ) -> tuple[pd.DataFrame, pd.DataFrame]:
    settings = scoring_config(cfg)
    holdout = int(settings["holdout_year"])
    later = int(settings["later_holdout_start_year"])
    folds_n = int(settings["folds"])
    seed = int(settings["random_seed"])
    periods = {str(holdout): scored[scored["year"] == holdout],
               f"{later}_plus": scored[scored["year"] >= later]}
    fold_rows, metric_rows = [], []
    for period, universe in periods.items():
        if universe.empty:
            continue
        for strategy, column in (("blocked_county", "county_block"),
                                 ("blocked_h3_r7", "h3_r7_block"),
                                 ("random_control", "crash_sk")):
            salt = f"random:{seed}" if strategy == "random_control" else strategy
            assigned = universe.assign(
                fold=[deterministic_fold(v, folds_n, salt=salt) for v in universe[column]])
            groups = [("ALL", assigned)] + sorted(
                ((str(k), g) for k, g in assigned.groupby("jurisdiction")),
                key=lambda item: item[0])
            for label, group in groups:
                summaries = []
                for fold in range(folds_n):
                    row = _metrics(group[group["fold"] == fold],
                                   float(settings["top_fraction"]))
                    fold_rows.append({"holdout": period, "train_year_max": holdout - 1,
                                      "strategy": strategy, "blocking_key": column,
                                      "jurisdiction": label, "fold": fold, **row})
                    if row["records"]:
                        summaries.append(row)
                metric_rows.append({"holdout": period, "train_year_max": holdout - 1,
                                    "strategy": strategy, "blocking_key": column,
                                    "jurisdiction": label, **_combine(summaries)})
    return pd.DataFrame(metric_rows), pd.DataFrame(fold_rows)


def _metrics(frame: pd.DataFrame, fraction: float) -> dict[str, Any]:
    n = len(frame)
    if not n:
        return {"records": 0, "severe_records": 0, "prevalence": None,
                "precision_at_top": None, "lift_over_random": None,
                "vru_precision_at_top": None, "lift_over_vru": None}
    k = max(1, int(math.ceil(n * fraction)))
    order = frame.sort_values(["context_score", "crash_date", "crash_sk"],
                              ascending=[False, False, True], kind="mergesort")
    vru = frame.sort_values(["vru_baseline_score", "crash_date", "crash_sk"],
                            ascending=[False, False, True], kind="mergesort")
    prevalence = float(frame["severe"].mean())
    precision = float(order.head(k)["severe"].mean())
    vru_precision = float(vru.head(k)["severe"].mean())
    return {"records": n, "severe_records": int(frame["severe"].sum()),
            "prevalence": round(prevalence, 6), "precision_at_top": round(precision, 6),
            "lift_over_random": round(precision / prevalence, 6) if prevalence else None,
            "vru_precision_at_top": round(vru_precision, 6),
            "lift_over_vru": round(precision / vru_precision, 6) if vru_precision else None}


def _combine(rows: list[dict[str, Any]]) -> dict[str, Any]:
    if not rows:
        return {"records": 0, "severe_records": 0, "prevalence": None,
                "precision_at_top": None, "lift_over_random": None,
                "vru_precision_at_top": None, "lift_over_vru": None}
    records = sum(r["records"] for r in rows)
    severe = sum(r["severe_records"] for r in rows)
    weighted = lambda key: sum((r[key] or 0.0) * r["records"] for r in rows) / records
    prevalence, precision = severe / records, weighted("precision_at_top")
    vru_precision = weighted("vru_precision_at_top")
    return {"records": records, "severe_records": severe,
            "prevalence": round(prevalence, 6), "precision_at_top": round(precision, 6),
            "lift_over_random": round(precision / prevalence, 6) if prevalence else None,
            "vru_precision_at_top": round(vru_precision, 6),
            "lift_over_vru": round(precision / vru_precision, 6) if vru_precision else None}


def run_backtest(gold_root: Path | str, *, cfg: Mapping[str, Any] | None = None
                 ) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    context = score_context(load_crash_context(gold_root), cfg=cfg)
    metrics, folds = evaluate(context, cfg=cfg)
    return metrics, folds, context
