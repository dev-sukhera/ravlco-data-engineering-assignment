"""Dagster asset graph for the independently runnable crash pipeline.

Retry semantics are intentionally asymmetric. Network assets have a bounded
exponential run retry in addition to the ingesters' tenacity HTTP retry: the
inner layer handles a request, the outer layer handles a lost process or an
exhausted request budget. Deterministic transforms do not retry because a
contract failure or deterministic exception needs intervention, not noise.
``decision_lineage`` is observable, never materialisable; its content-addressed
writer already makes a repeated partial build idempotent, so it has no retry.
"""

from __future__ import annotations

import hashlib
import json
from datetime import date
from pathlib import Path
from typing import Any, Iterable

import duckdb
import numpy as np
import pyarrow as pa
import pyarrow.parquet as pq
import shapely
from dagster import (
    AssetCheckResult,
    AssetCheckSeverity,
    Backoff,
    DailyPartitionsDefinition,
    DataVersion,
    Definitions,
    Field,
    MetadataValue,
    ObserveResult,
    RetryPolicy,
    StaticPartitionsDefinition,
    asset,
    asset_check,
    job,
    observable_source_asset,
    op,
)

from src import config, contracts
from src.analysis.build import TABLE_SPECS as ANALYSIS_TABLES, build_analysis
from src.compliance import build as compliance_build
from src.compliance.build import build_compliance
from src.geo.build import build_geo
from src.ingest import fars, montgomery, txdot
from src.scoring.build import TABLES as SCORING_TABLES, build_scoring
from src.transform import drift
from src.transform.build import build_silver
from src.transform.model import build_gold

from orchestration.backfill import backfill

_OPERABILITY = config.operability()
NETWORK_RETRY = RetryPolicy(
    max_retries=int(_OPERABILITY["network_max_retries"]),
    delay=float(_OPERABILITY["network_retry_delay_seconds"]),
    backoff=Backoff.EXPONENTIAL,
)
FARS_PARTITIONS = StaticPartitionsDefinition(
    [str(year) for year in config.sources()["fars"]["years"]]
)
MONTGOMERY_PARTITIONS = DailyPartitionsDefinition(
    start_date=str(_OPERABILITY["montgomery_partition_start"]), timezone="UTC"
)


def _json_violations(items: Iterable[contracts.Violation]) -> MetadataValue:
    return MetadataValue.json([{
        "table": v.table, "kind": v.kind, "detail": v.detail,
        "count": v.count, "examples": v.examples,
    } for v in items])


def _relation_check(path: Path, contract_path: Path, table: str) -> list[contracts.Violation]:
    if not path.exists():
        return [contracts.Violation(table, "missing-artifact", str(path))]
    con = duckdb.connect()
    try:
        con.read_parquet(str(path)).create_view("checked")
        return contracts.validate(
            con, "checked", table, contract_path=contract_path,
            check_row_count_min=False, raise_on_violation=False)
    finally:
        con.close()


def _tree_contract_check(root: Path, contract_path: Path,
                         mapping: dict[str, str]) -> AssetCheckResult:
    violations: list[contracts.Violation] = []
    contract = contracts.load_contract(contract_path)
    con = duckdb.connect()
    resolve: dict[str, str] = {}
    try:
        for index, (relative, table) in enumerate(mapping.items()):
            path = root / relative
            if not path.exists():
                violations.append(contracts.Violation(table, "missing-artifact", str(path)))
                continue
            relation = f"checked_{index}"
            con.read_parquet(str(path)).create_view(relation)
            resolve[table] = relation
        for table, relation in resolve.items():
            violations.extend(contracts.validate_relation(
                con, relation, contract, table, check_row_count_min=False))
            violations.extend(contracts.validate_foreign_keys(
                con, contract, table, relation, resolve))
    finally:
        con.close()
    return AssetCheckResult(
        passed=not violations,
        metadata={"tables_checked": len(resolve), "violations": _json_violations(violations)},
    )


def validate_geoparquet_tree(root: Path) -> list[dict[str, Any]]:
    """Return metadata/bbox violations for every GeoParquet file in a tree."""
    violations: list[dict[str, Any]] = []
    files = sorted(root.rglob("*.parquet")) if root.exists() else []
    if not files:
        return [{"path": str(root), "kind": "missing-artifact"}]
    for path in files:
        schema = pq.read_schema(path)
        raw = (schema.metadata or {}).get(b"geo")
        if raw is None:
            violations.append({"path": str(path), "kind": "missing-geo-metadata"})
            continue
        try:
            geo = json.loads(raw)
        except (ValueError, TypeError) as exc:
            violations.append({"path": str(path), "kind": "invalid-geo-json", "detail": str(exc)})
            continue
        if geo.get("version") != "1.1.0":
            violations.append({"path": str(path), "kind": "version", "actual": geo.get("version")})
        primary = geo.get("primary_column")
        column = geo.get("columns", {}).get(primary, {})
        covering = column.get("covering", {}).get("bbox", {})
        if set(covering) != {"xmin", "ymin", "xmax", "ymax"} or \
                any(value != ["bbox", key] for key, value in covering.items()):
            violations.append({"path": str(path), "kind": "bbox-covering", "actual": covering})
        crs = column.get("crs")
        crs_text = json.dumps(crs, sort_keys=True)
        if not isinstance(crs, dict) or "4326" not in crs_text:
            violations.append({"path": str(path), "kind": "crs", "actual": crs})
        if "bbox" not in schema.names or not pa.types.is_struct(schema.field("bbox").type):
            violations.append({"path": str(path), "kind": "bbox-column"})
            continue
        # Metadata can claim a covering while row values are wrong. Verify the
        # actual struct against WKB bounds; report counts, never coordinates.
        table = pq.read_table(path, columns=[primary, "bbox"])
        wkbs = np.asarray(table.column(primary).to_pylist(), dtype=object)
        actual_bounds = shapely.bounds(shapely.from_wkb(wkbs))
        boxes = table.column("bbox").to_pylist()
        bad_bounds = 0
        for bounds, box in zip(actual_bounds, boxes):
            if np.isnan(bounds).all():
                bad_bounds += int(box is not None and any(value is not None for value in box.values()))
                continue
            expected = np.asarray([box[k] for k in ("xmin", "ymin", "xmax", "ymax")]) \
                if box is not None else np.asarray([np.nan] * 4)
            bad_bounds += int(not np.allclose(bounds, expected, rtol=0, atol=1e-12))
        if bad_bounds:
            violations.append({"path": str(path), "kind": "bbox-does-not-bound-geometry",
                               "count": bad_bounds})
    return violations


def check_lineage_append_only(path: Path, state_path: Path, *, prefix_rows: int | None = None,
                              persist: bool = True) -> tuple[bool, dict[str, Any]]:
    """Compare row count and canonical first-N hash with the last observation."""
    if not path.exists():
        return False, {"violations": [{"kind": "missing-artifact", "path": str(path)}]}
    prefix_rows = prefix_rows or int(_OPERABILITY["lineage_prefix_rows"])
    table = pq.read_table(path).sort_by([("decision_lineage_id", "ascending")])
    first = table.slice(0, min(prefix_rows, table.num_rows))
    # Hash logical rows, not Arrow chunk layout: appending a parquet row can
    # change record-batch boundaries while leaving the established prefix
    # byte-for-byte equivalent at the data level.
    prefix_payload = json.dumps(first.to_pylist(), sort_keys=True,
                                separators=(",", ":"), default=str).encode()
    current = {"rows": table.num_rows,
               "prefix_rows": first.num_rows,
               "prefix_sha256": hashlib.sha256(prefix_payload).hexdigest()}
    previous = json.loads(state_path.read_text()) if state_path.exists() else None
    violations = []
    if previous and current["rows"] < previous["rows"]:
        violations.append({"kind": "row-count-decreased", "previous": previous["rows"],
                           "current": current["rows"]})
    comparable = previous and min(previous["prefix_rows"], current["prefix_rows"]) == previous["prefix_rows"]
    if comparable and current["prefix_sha256"] != previous["prefix_sha256"]:
        violations.append({"kind": "prefix-changed", "rows": previous["prefix_rows"]})
    if not violations and persist:
        state_path.parent.mkdir(parents=True, exist_ok=True)
        tmp = state_path.with_name(state_path.name + ".part")
        tmp.write_text(json.dumps(current, sort_keys=True) + "\n")
        tmp.replace(state_path)
    return not violations, {"current": current, "previous": previous, "violations": violations}


@asset(partitions_def=FARS_PARTITIONS, retry_policy=NETWORK_RETRY,
       description="Append-only FARS bronze ZIP snapshot for one configured year.")
def bronze_fars(context) -> dict[str, Any]:
    year = context.partition_key
    fars.main(["--years", year])
    return {"year": year}


@asset(partitions_def=MONTGOMERY_PARTITIONS, retry_policy=NETWORK_RETRY,
       description="Montgomery keyset ingest for one UTC load date; crash dates never drive the watermark.")
def bronze_montgomery(context) -> dict[str, Any]:
    load_date = context.partition_key
    montgomery.main(["--since", load_date])
    return {"load_date": load_date}


@asset(retry_policy=NETWORK_RETRY,
       description="One bounded TxDOT OID-keyset sweep; OID state, not a false date partition, is durable.")
def bronze_txdot() -> dict[str, Any]:
    txdot.main([])
    return {"partition_model": "OID keyset; unpartitioned Dagster asset"}


@asset(deps=[bronze_fars], description="Deterministic FARS SCD2 silver tables from all annual bronze snapshots.")
def silver_fars() -> dict[str, Any]:
    return build_silver(sources=["fars"])


@asset(deps=[bronze_montgomery], description="Deterministic Montgomery SCD2 silver tables with blocking vocabulary drift gate.")
def silver_montgomery() -> dict[str, Any]:
    return build_silver(sources=["montgomery"])


@asset(deps=[bronze_txdot], description="Deterministic TxDOT amendment-aware SCD2 silver table.")
def silver_txdot() -> dict[str, Any]:
    return build_silver(sources=["txdot"])


@asset(deps=[silver_fars, silver_montgomery, silver_txdot],
       description="Conformed star schema with exactly one fact_crash row per crash.")
def gold_model() -> dict[str, Any]:
    return build_gold()


@asset(deps=[gold_model], description="GeoParquet 1.1.0 crash enrichment, flat and jurisdiction/year hive copies.")
def crash_geo() -> dict[str, Any]:
    return build_geo()


@asset(deps=[crash_geo], description="Spatial statistics and hotspot artefacts over the geocoded crash corpus.")
def analysis() -> dict[str, Any]:
    return build_analysis()


@asset(deps=[analysis], description="Restricted tokenising vault boundary; direct identifiers never enter assets downstream.")
def vault() -> dict[str, Any]:
    return {"root": str(config.DATA_DIR / "vault")}


@asset(deps=[vault], description="Derived, non-identifying party geography and calling-zone enrichment boundary.")
def party_enrichment() -> dict[str, Any]:
    return {"builder": "build_compliance"}


@asset(deps=[party_enrichment, vault, crash_geo],
       description="Lawful lead decisions; calls the import-safe compliance builder once for all compliance artefacts.")
def leads() -> dict[str, Any]:
    return build_compliance()


@asset(deps=[leads], description="Crash-only compliance dispositions, intentionally separate from fixture leads.")
def crash_only_decisions() -> dict[str, Any]:
    return {"path": str(config.GOLD_DIR / "compliance" / "crash_only_decisions.parquet")}


@asset(deps=[leads, crash_only_decisions], description="Auditable exclusion counts by stable legal reason code.")
def exclusion_by_code() -> dict[str, Any]:
    return {"path": str(config.GOLD_DIR / "compliance" / "exclusion_by_code.parquet")}


@observable_source_asset(name="decision_lineage",
                         description="External append-only content-addressed decision lineage sink; never materialised by Dagster.")
def decision_lineage_observation() -> ObserveResult:
    path = config.GOLD_DIR / "compliance" / "decision_lineage.parquet"
    version = hashlib.sha256(path.read_bytes()).hexdigest() if path.exists() else "missing"
    return ObserveResult(data_version=DataVersion(version), metadata={"path": str(path)})


@asset(deps=[exclusion_by_code, crash_geo], description="Deterministic temporal/spatial blocked scoring backtest.")
def scoring_backtest() -> dict[str, Any]:
    return build_scoring()


@asset(deps=[leads, scoring_backtest], description="Contract-validated, PII-free sample CSV for downstream review.")
def sample_leads() -> dict[str, Any]:
    return {"path": str(config.REPO_ROOT / "output" / "sample_leads.csv")}


@asset_check(asset=bronze_fars, name="bronze_fars_contract", blocking=True)
def bronze_fars_contract() -> AssetCheckResult:
    violations = []
    for name in ("accident", "vehicle", "person"):
        files = sorted((config.BRONZE_DIR / "fars").glob(f"*/*/{name}.parquet"))
        if not files:
            violations.append(contracts.Violation(f"fars.{name}", "missing-artifact", name))
            continue
        con = duckdb.connect()
        try:
            con.read_parquet([str(p) for p in files], union_by_name=True).create_view("checked")
            violations += contracts.validate(con, "checked", f"fars.{name}",
                contract_path=contracts.BRONZE_CONTRACT, check_row_count_min=False,
                raise_on_violation=False)
        finally:
            con.close()
    return AssetCheckResult(passed=not violations, metadata={"violations": _json_violations(violations)})


def _bronze_moco_result() -> AssetCheckResult:
    violations = []
    for dataset in ("bhju-22kf", "mmzv-x632", "n7fk-dce5"):
        files = sorted((config.BRONZE_DIR / "montgomery" / dataset).glob("*/*.parquet"))
        if not files:
            violations.append(contracts.Violation(f"montgomery.{dataset}", "missing-artifact", dataset))
            continue
        con = duckdb.connect()
        try:
            con.read_parquet([str(p) for p in files], union_by_name=True).create_view("checked")
            violations += contracts.validate(con, "checked", f"montgomery.{dataset}",
                contract_path=contracts.BRONZE_CONTRACT, check_row_count_min=False,
                raise_on_violation=False)
        finally:
            con.close()
    return AssetCheckResult(passed=not violations, metadata={"violations": _json_violations(violations)})


@asset_check(asset=bronze_montgomery, name="bronze_montgomery_contract", blocking=True)
def bronze_montgomery_contract() -> AssetCheckResult:
    return _bronze_moco_result()


@asset_check(asset=bronze_txdot, name="bronze_txdot_contract", blocking=True)
def bronze_txdot_contract() -> AssetCheckResult:
    files = sorted((config.BRONZE_DIR / "txdot" / "cris_crash").glob("*/*.parquet"))
    violations = []
    if not files:
        violations.append(contracts.Violation("txdot.cris_crash", "missing-artifact", "cris_crash"))
    else:
        con = duckdb.connect()
        try:
            con.read_parquet([str(p) for p in files], union_by_name=True).create_view("checked")
            violations += contracts.validate(con, "checked", "txdot.cris_crash",
                contract_path=contracts.BRONZE_CONTRACT, check_row_count_min=False,
                raise_on_violation=False)
        finally:
            con.close()
    return AssetCheckResult(passed=not violations, metadata={"violations": _json_violations(violations)})


@asset_check(asset=silver_montgomery, name="montgomery_vocabulary_drift", blocking=True)
def montgomery_vocabulary_drift() -> AssetCheckResult:
    reports = []
    for column in ("driver_substance_abuse", "injury_severity"):
        values = drift.observe("mmzv-x632", column)
        reports.append(drift.detect_drift("mmzv-x632", column, values))
    bad = [report for report in reports if report.unmapped]
    return AssetCheckResult(
        passed=not bad,
        metadata={"reports": MetadataValue.json([{
            "dataset": r.dataset, "column": r.column,
            "rows_checked": r.rows_checked, "drifted": r.drifted,
            "unmapped": [token.value for token in r.unmapped],
            "rendered": r.render(),
        } for r in reports])},
    )


def _silver_result(source: str) -> AssetCheckResult:
    from src.transform.build import TABLES

    # Current slices are the boundary consumed downstream. History files share
    # their schema but intentionally are not unique on the current-slice key.
    mapping = {
        f"{source}/{name}.parquet": contract_name
        for name, _relation, contract_name, _order in TABLES[source]
        if name.endswith("_current") or name == "codebook"
    }
    return _tree_contract_check(config.SILVER_DIR, contracts.SILVER_CONTRACT, mapping)


@asset_check(asset=silver_fars, name="silver_fars_contract", blocking=True)
def silver_fars_contract() -> AssetCheckResult:
    return _silver_result("fars")


@asset_check(asset=silver_montgomery, name="silver_montgomery_contract", blocking=True)
def silver_montgomery_contract() -> AssetCheckResult:
    return _silver_result("montgomery")


@asset_check(asset=silver_txdot, name="silver_txdot_contract", blocking=True)
def silver_txdot_contract() -> AssetCheckResult:
    return _silver_result("txdot")


@asset_check(asset=gold_model, name="gold_contract_and_fact_grain", blocking=True)
def gold_contract_and_fact_grain() -> AssetCheckResult:
    mapping = {f"{name}.parquet": f"gold.{name}" for name in (
        "fact_crash", "bridge_crash_source", "fact_driver", "fact_non_motorist",
        "dim_date", "dim_time", "dim_geography", "dim_road_class",
        "dim_weather_condition", "dim_non_motorist_type", "dim_severity",
        "map_severity_source", "map_road_class_source", "map_weather_source",
        "map_non_motorist_type_source")}
    return _tree_contract_check(config.GOLD_DIR, contracts.CONTRACTS_DIR / "gold.schema.json", mapping)


@asset_check(asset=gold_model, name="silver_unified_input_contract", blocking=True)
def silver_unified_input_contract() -> AssetCheckResult:
    return _tree_contract_check(
        config.SILVER_DIR, contracts.SILVER_CONTRACT,
        {"crash_current.parquet": "silver.crash"})


@asset_check(asset=crash_geo, name="geoparquet_1_1_bbox", blocking=True)
def geoparquet_1_1_bbox() -> AssetCheckResult:
    failures = validate_geoparquet_tree(config.GOLD_DIR / "crash_geo")
    return AssetCheckResult(passed=not failures,
                            metadata={"violations": MetadataValue.json(failures)})


@asset_check(asset=crash_geo, name="crash_geo_contract", blocking=True)
def crash_geo_contract() -> AssetCheckResult:
    return _tree_contract_check(
        config.GOLD_DIR, contracts.CONTRACTS_DIR / "gold.schema.json",
        {"crash_geo.parquet": "gold.crash_geo",
         "dim_block_group.parquet": "gold.dim_block_group"})


@asset_check(asset=analysis, name="analysis_contract", blocking=True)
def analysis_contract() -> AssetCheckResult:
    mapping = {f"{name}.parquet": spec[0] for name, spec in ANALYSIS_TABLES.items()}
    return _tree_contract_check(config.GOLD_DIR / "analysis",
                                contracts.CONTRACTS_DIR / "analysis.schema.json", mapping)


@asset_check(asset=leads, name="compliance_contract", blocking=True)
def compliance_contract() -> AssetCheckResult:
    mapping = {f"{name}.parquet": spec[0] for name, spec in compliance_build.TABLE_SPECS.items()}
    return _tree_contract_check(config.GOLD_DIR / "compliance", contracts.COMPLIANCE_CONTRACT, mapping)


@asset_check(asset=sample_leads, name="lead_output_contract", blocking=True)
def lead_output_contract() -> AssetCheckResult:
    path = config.REPO_ROOT / "output" / "sample_leads.csv"
    if not path.exists():
        return AssetCheckResult(passed=False, metadata={"violations": MetadataValue.json([{"kind": "missing-artifact"}])})
    results = compliance_build.validate_lead_rows(compliance_build.read_sample_csv(path))
    failures = [row for row in results if not row["valid"]]
    return AssetCheckResult(passed=not failures,
                            metadata={"violations": MetadataValue.json(failures), "rows": len(results)})


@asset_check(asset=scoring_backtest, name="scoring_contract", blocking=True)
def scoring_contract() -> AssetCheckResult:
    mapping = {f"{name}.parquet": spec[0] for name, spec in SCORING_TABLES.items()}
    return _tree_contract_check(config.GOLD_DIR / "scoring", contracts.SCORING_CONTRACT, mapping)


@asset_check(asset=scoring_backtest, name="fl_trailing_window_completeness")
def fl_trailing_window_completeness() -> AssetCheckResult:
    path = config.GOLD_DIR / "compliance" / "_compliance_manifest.json"
    if not path.exists():
        return AssetCheckResult(passed=False, severity=AssetCheckSeverity.WARN,
                                metadata={"reason": "compliance manifest missing"})
    detail = json.loads(path.read_text())["stats"]["fl_incompleteness"]["fixture_leads"]
    overlaps = int(detail.get("overlap_days", 0)) > 0
    return AssetCheckResult(
        passed=not overlaps, severity=AssetCheckSeverity.WARN,
        metadata={"window": MetadataValue.json(detail),
                  "reason": "FL public-feed trailing window overlaps the statutory 60-day gap"})


@asset_check(asset=decision_lineage_observation, name="append_only_lineage", blocking=True)
def append_only_lineage() -> AssetCheckResult:
    root = config.GOLD_DIR / "compliance"
    passed, metadata = check_lineage_append_only(
        root / "decision_lineage.parquet", root / "_lineage_observation.json")
    return AssetCheckResult(passed=passed,
                            metadata={"observation": MetadataValue.json(metadata)})


@op(config_schema={
    "start": str,
    "end": str,
    "layer": Field(str, default_value="all"),
    "out_root": Field(str, is_required=False),
    "bronze_root": Field(str, is_required=False),
    "reference_root": Field(str, is_required=False),
    "small_corpus": Field(bool, default_value=False),
})
def run_range_backfill(context) -> None:
    """Dagster job entry point; deliberately shares ``backfill()`` with the CLI."""
    backfill(**context.op_config)


@job(description="Date-range backfill using orchestration.backfill.backfill.")
def range_backfill_job():
    run_range_backfill()


_ASSETS = [bronze_fars, bronze_montgomery, bronze_txdot, silver_fars,
           silver_montgomery, silver_txdot, gold_model, crash_geo, analysis,
           vault, party_enrichment, leads, crash_only_decisions,
           exclusion_by_code, scoring_backtest, sample_leads]
_CHECKS = [bronze_fars_contract, bronze_montgomery_contract, bronze_txdot_contract,
           silver_fars_contract, silver_montgomery_contract, silver_txdot_contract,
           montgomery_vocabulary_drift, gold_contract_and_fact_grain,
           silver_unified_input_contract,
           geoparquet_1_1_bbox, crash_geo_contract, analysis_contract, compliance_contract,
           lead_output_contract, scoring_contract, fl_trailing_window_completeness,
           append_only_lineage]

defs = Definitions(assets=[*_ASSETS, decision_lineage_observation],
                   asset_checks=_CHECKS, jobs=[range_backfill_job])
