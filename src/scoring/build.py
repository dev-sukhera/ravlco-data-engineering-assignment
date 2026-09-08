"""Build and validate the reproducible scoring backtest.

Run with ``python -m src.scoring.build``. Importing this module performs no I/O.
"""

from __future__ import annotations

import argparse
import hashlib
import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import duckdb

from .. import config, contracts
from ..ingest.watermark import durable_replace
from ..transform import common
from . import backtest

CONTRACT = contracts.SCORING_CONTRACT
TABLES = {
    "backtest_metrics": ("scoring.backtest_metrics",
                         ["holdout", "strategy", "blocking_key", "jurisdiction"]),
    "backtest_folds": ("scoring.backtest_folds",
                       ["holdout", "strategy", "blocking_key", "jurisdiction", "fold"]),
    "crash_context_scores": ("scoring.crash_context_scores", ["crash_sk"]),
}


def _sha(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def build_scoring(*, gold_root: Path | str | None = None,
                  out_root: Path | str | None = None,
                  validate: bool = True, small_corpus: bool = False) -> dict[str, Any]:
    gold = Path(gold_root) if gold_root is not None else config.GOLD_DIR
    out = Path(out_root) if out_root is not None else gold / "scoring"
    settings = config.scoring()
    metrics, folds, scores = backtest.run_backtest(gold, cfg=settings)
    score_columns = ["crash_sk", "source_system", "jurisdiction", "crash_date", "year",
                     "severity_ordinal", "severe", "context_score", "vru_baseline_score",
                     "county_block", "h3_r7_block", "score_components",
                     "_scoring_build_sha"]
    scores = scores[score_columns].copy()
    scores["crash_date"] = scores["crash_date"].map(
        lambda value: value.date() if hasattr(value, "date") else value)
    scores["score_components"] = scores["score_components"].map(
        lambda value: json.dumps(value, sort_keys=True, separators=(",", ":")))
    tables = {"backtest_metrics": metrics, "backtest_folds": folds,
              "crash_context_scores": scores}
    con = duckdb.connect()
    try:
        for name, frame in tables.items():
            con.register(f"v_{name}", frame)
        if validate:
            contract = contracts.load_contract(CONTRACT)
            violations = []
            for name in tables:
                violations += contracts.validate_relation(
                    con, f"v_{name}", contract, TABLES[name][0],
                    check_row_count_min=not small_corpus)
            contracts.raise_for(violations, context="scoring.schema.json")
        out.mkdir(parents=True, exist_ok=True)
        outputs = {}
        for name, frame in tables.items():
            outputs[name] = common.write_parquet(
                con, f"v_{name}", out / f"{name}.parquet",
                columns=list(frame.columns), order_by=TABLES[name][1])
    finally:
        con.close()
    inputs = {}
    for name in ("fact_crash", "crash_geo", "dim_road_class", "dim_weather_condition"):
        path = gold / f"{name}.parquet"
        inputs[name] = {"path": str(path), "sha256": _sha(path)}
    scoring_path = config.CONFIG_DIR / "scoring.toml"
    inputs["scoring_config"] = {"path": str(scoring_path), "sha256": _sha(scoring_path)}
    manifest = {"built_at": datetime.now(timezone.utc).isoformat(),
                "gold_root": str(gold), "out_root": str(out), "inputs": inputs,
                "parameters": settings["scoring"], "outputs": outputs,
                "row_counts": {name: len(frame) for name, frame in tables.items()}}
    dest = out / "_scoring_manifest.json"
    tmp = dest.with_name(dest.name + ".part")
    tmp.write_text(json.dumps(manifest, indent=2, default=str) + "\n")
    durable_replace(tmp, dest)
    return manifest


def _cli(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(prog="python -m src.scoring.build")
    parser.add_argument("--gold-root", type=Path)
    parser.add_argument("--out-root", type=Path)
    parser.add_argument("--no-validate", action="store_true")
    parser.add_argument("--small-corpus", action="store_true")
    args = parser.parse_args(argv)
    result = build_scoring(gold_root=args.gold_root, out_root=args.out_root,
                           validate=not args.no_validate,
                           small_corpus=args.small_corpus)
    for name, count in result["row_counts"].items():
        print(f"{name}: {count} rows")
    return 0


if __name__ == "__main__":
    raise SystemExit(_cli())
