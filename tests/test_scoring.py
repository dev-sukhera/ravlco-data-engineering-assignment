"""Phase 7 scoring behaviour: pure, decomposable, gated, and reproducible."""

from __future__ import annotations

import hashlib
import json
import math
from datetime import date
from pathlib import Path

import pytest

from src import config
from src.scoring import backtest
from src.scoring import build as build_mod
from src.scoring.score import CLAIM, score_lead, score_leads, scoring_config

AS_OF = date(2026, 9, 1)


def row(**overrides):
    base = {"lead_id": "LD_a", "eligibility_status": "ELIGIBLE",
            "incident_date": "2026-08-01", "severity_ordinal": 2,
            "pedestrian_involved": False, "bicyclist_involved": False,
            "hit_run": False, "fhwa_class": 4, "is_adverse": False,
            "snap_status": None, "osm_maxspeed_mph": None,
            "weather_status": None, "era5_precipitation_mm": None}
    base.update(overrides)
    return base


def test_documented_claim_is_the_executable_boundary():
    scoring_doc = (Path(__file__).resolve().parents[1] / "SCORING.md").read_text()
    assert CLAIM in scoring_doc
    assert "only facts about the crash itself" in CLAIM


def test_score_decomposes_and_every_feature_has_provenance():
    result = score_lead(row(), as_of=AS_OF)
    terms = [value for value in result.score_components.values()
             if "contribution" in value]
    assert result.priority_score == round(math.fsum(x["contribution"] for x in terms), 2)
    assert all({"source", "status", "contribution"} <= set(x) for x in terms)


def test_status_gate_scores_only_admitted_records():
    scored = score_leads([row(), row(lead_id="LD_b", eligibility_status="BLOCKED_UNTIL"),
                          row(lead_id="LD_c", eligibility_status="INELIGIBLE")])
    by_id = {item["lead_id"]: item for item in scored}
    assert by_id["LD_a"]["priority_score"] is not None
    assert by_id["LD_b"]["priority_score"] is not None
    assert by_id["LD_c"]["priority_score"] is None
    assert by_id["LD_c"]["score_components"] is None


def test_severity_and_recency_are_monotone():
    assert score_lead(row(severity_ordinal=3)).priority_score >= score_lead(
        row(severity_ordinal=2)).priority_score
    assert score_lead(row(severity_ordinal=1)).priority_score > score_lead(
        row(severity_ordinal=0)).priority_score
    assert score_lead(row(incident_date="2026-08-31")).priority_score > score_lead(
        row(incident_date="2026-01-01")).priority_score


def test_missing_crash_context_is_finite_and_explicit():
    result = score_lead(row(severity_ordinal=None, pedestrian_involved=None,
                            bicyclist_involved=None, hit_run=None, fhwa_class=None,
                            is_adverse=None))
    assert math.isfinite(result.priority_score)
    assert result.score_components["coverage"] == {"available": 1, "of": 6}
    assert result.score_components["severity"]["status"] == "UNAVAILABLE"
    assert result.score_components["severity"]["contribution"] == 0


def test_as_of_is_an_input_not_wall_clock():
    old = score_lead(row(), as_of="2026-09-01")
    new = score_lead(row(), as_of="2026-10-01")
    assert old.score_components["recency"]["contribution"] > new.score_components["recency"]["contribution"]


def test_equal_scores_use_documented_tie_break():
    ranked = score_leads([
        row(lead_id="LD_b", incident_date="2026-08-01"),
        row(lead_id="LD_c", incident_date="2026-08-02"),
        row(lead_id="LD_a", incident_date="2026-08-01"),
    ], as_of=AS_OF)
    # Recency makes the newer record first; exact ties then use lead_id.
    assert [item["lead_id"] for item in ranked] == ["LD_c", "LD_a", "LD_b"]


def test_denylist_is_absent_from_feature_and_score_implementations():
    root = Path(__file__).resolve().parents[1]
    core = "\n".join((root / "src" / "scoring" / name).read_text().lower()
                     for name in ("features.py", "score.py", "build.py"))
    deny = config.scoring()["scoring"]["denylist"]
    assert "dim_block_group" not in core
    for term in ("b19013", "b01003", "median_income", "tenure",
                 "vehicle_availability", "zip5"):
        assert term not in core
    assert set(config.scoring()["scoring"]["features"]).isdisjoint(deny["columns"])
    assert "3857" not in core


def test_config_rejects_unknown_feature_and_nonfinite_weight():
    raw = json.loads(json.dumps(config.scoring()))
    raw["scoring"]["features"].append("mystery")
    with pytest.raises(ValueError, match="no implementation"):
        scoring_config(raw)
    raw = json.loads(json.dumps(config.scoring()))
    raw["scoring"]["hit_run_points"] = float("inf")
    with pytest.raises(ValueError, match="finite"):
        scoring_config(raw)


def test_spatial_fold_is_a_function_of_block_key():
    assert backtest.deterministic_fold("24031", 5) == backtest.deterministic_fold("24031", 5)
    assert 0 <= backtest.deterministic_fold(None, 5) < 5


@pytest.fixture(scope="module")
def local_gold():
    root = config.GOLD_DIR
    if not (root / "fact_crash.parquet").exists():
        pytest.skip(f"no local gold at {root} -- run `python -m src.transform.build`")
    return root


def test_backtest_has_temporal_blocked_and_random_results(local_gold, tmp_path):
    result = build_mod.build_scoring(gold_root=local_gold, out_root=tmp_path / "score")
    assert result["row_counts"]["crash_context_scores"] > 0
    import pandas as pd
    metrics = pd.read_parquet(tmp_path / "score" / "backtest_metrics.parquet")
    assert {"blocked_county", "blocked_h3_r7", "random_control"} <= set(metrics["strategy"])
    assert (metrics["train_year_max"] < metrics["holdout"].str[:4].astype(int)).all()


def test_two_backtest_table_runs_are_byte_identical(local_gold, tmp_path):
    out = tmp_path / "score"
    build_mod.build_scoring(gold_root=local_gold, out_root=out)
    first = {p.name: hashlib.sha256(p.read_bytes()).hexdigest()
             for p in out.glob("*.parquet")}
    build_mod.build_scoring(gold_root=local_gold, out_root=out)
    second = {p.name: hashlib.sha256(p.read_bytes()).hexdigest()
              for p in out.glob("*.parquet")}
    assert first == second
