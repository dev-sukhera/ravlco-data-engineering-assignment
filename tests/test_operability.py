"""Phase 8 operability behaviour; all tests are local and network-free."""

from __future__ import annotations

import hashlib
import json
import shutil
from pathlib import Path

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
from dagster import AssetKey, DailyPartitionsDefinition, StaticPartitionsDefinition

from orchestration.backfill import (
    backfill, hash_artifacts, prove, recover_fars_year, recover_geo_partition,
)
from orchestration.dagster_defs import (
    FARS_PARTITIONS,
    MONTGOMERY_PARTITIONS,
    NETWORK_RETRY,
    check_lineage_append_only,
    defs,
    range_backfill_job,
    validate_geoparquet_tree,
)
from src import config
from src.geo.build import write_geoparquet
from src.transform.dictionaries import ObservedValue, detect_drift, vocabulary

FIXTURE_BRONZE = Path(__file__).parent / "fixtures" / "bronze"


def test_definitions_are_described_acyclic_and_match_layer_order():
    graph = defs.resolve_asset_graph()
    expected = {
        "bronze_fars", "bronze_montgomery", "bronze_txdot", "silver_fars",
        "silver_montgomery", "silver_txdot", "gold_model", "crash_geo",
        "analysis", "vault", "party_enrichment", "leads",
        "crash_only_decisions", "exclusion_by_code", "decision_lineage",
        "scoring_backtest", "sample_leads",
    }
    assert {key.to_user_string() for key in graph.get_all_asset_keys()} == expected
    assert all(graph.get(key).description for key in graph.get_all_asset_keys())
    parents = lambda name: {
        key.to_user_string() for key in graph.get(AssetKey(name)).parent_keys}
    assert parents("gold_model") == {"silver_fars", "silver_montgomery", "silver_txdot"}
    assert {"crash_geo", "vault"} <= parents("leads")
    assert parents("exclusion_by_code") == {"leads", "crash_only_decisions"}
    assert "exclusion_by_code" in parents("scoring_backtest")
    # Dagster resolves a topological order while loading Definitions; asking
    # for it is also an explicit cycle assertion.
    assert len(graph.toposorted_asset_keys) == len(expected)


def test_natural_partition_definitions_and_network_retry():
    assert isinstance(FARS_PARTITIONS, StaticPartitionsDefinition)
    assert FARS_PARTITIONS.get_partition_keys() == [
        str(year) for year in config.sources()["fars"]["years"]]
    assert isinstance(MONTGOMERY_PARTITIONS, DailyPartitionsDefinition)
    assert NETWORK_RETRY.max_retries == config.operability()["network_max_retries"]


def test_backfill_dry_run_validates_without_touching_bronze(tmp_path):
    before = hash_artifacts(FIXTURE_BRONZE)
    result = backfill("2024-01-01", "2024-03-31", layer="gold", dry_run=True,
                      out_root=tmp_path, bronze_root=FIXTURE_BRONZE)
    assert result.layers_run == ("silver", "gold")
    assert not result.artifacts
    assert hash_artifacts(FIXTURE_BRONZE) == before


def test_fixture_silver_backfill_is_byte_identical():
    one, two, passed = prove(
        "2024-01-01", "2024-03-31", layer="silver",
        bronze_root=FIXTURE_BRONZE, small_corpus=True)
    assert passed
    assert one.artifacts.keys() == two.artifacts.keys()
    assert {key: item.sha256 for key, item in one.artifacts.items()} == {
        key: item.sha256 for key, item in two.artifacts.items()}


def test_fars_recovery_does_not_touch_other_source_bytes(tmp_path):
    result = backfill("2024-01-01", "2024-12-31", layer="silver",
                      bronze_root=FIXTURE_BRONZE, out_root=tmp_path,
                      small_corpus=True)
    silver = tmp_path / "silver"
    before_other = hash_artifacts(silver / "montgomery") | hash_artifacts(silver / "txdot")
    target = silver / "fars" / "accident_current.parquet"
    original = hashlib.sha256(target.read_bytes()).hexdigest()
    target.write_bytes(b"corrupt")
    assert hashlib.sha256(target.read_bytes()).hexdigest() != original
    recover_fars_year(2024, bronze_root=FIXTURE_BRONZE, silver_root=silver,
                      small_corpus=True)
    assert hashlib.sha256(target.read_bytes()).hexdigest() == original
    assert (hash_artifacts(silver / "montgomery") | hash_artifacts(silver / "txdot")) == before_other


def test_dagster_job_uses_the_same_backfill_bytes(tmp_path):
    direct_root, dagster_root = tmp_path / "direct", tmp_path / "dagster"
    direct = backfill("2024-01-01", "2024-03-31", layer="silver",
                      bronze_root=FIXTURE_BRONZE, out_root=direct_root,
                      small_corpus=True)
    execution = range_backfill_job.execute_in_process(run_config={"ops": {
        "run_range_backfill": {"config": {
            "start": "2024-01-01", "end": "2024-03-31", "layer": "silver",
            "out_root": str(dagster_root), "bronze_root": str(FIXTURE_BRONZE),
            "small_corpus": True,
        }}}})
    assert execution.success
    assert {key: value.sha256 for key, value in direct.artifacts.items()} == {
        key: value.sha256 for key, value in hash_artifacts(dagster_root).items()}


def test_geo_partition_recovery_is_local_when_full_data_is_available(tmp_path):
    source = config.GOLD_DIR
    target_rel = Path("crash_geo/jurisdiction=MD/year=2024/part-0.parquet")
    other_rel = Path("crash_geo/jurisdiction=MD/year=2023/part-0.parquet")
    if not (source / target_rel).exists():
        import pytest
        pytest.skip(f"{source / target_rel} is missing; full-data recovery test requires data/")
    gold = tmp_path / "gold"
    gold.mkdir()
    for path in source.glob("*.parquet"):
        shutil.copy2(path, gold / path.name)
    for relative in (target_rel, other_rel):
        (gold / relative).parent.mkdir(parents=True, exist_ok=True)
        shutil.copy2(source / relative, gold / relative)
    before_other = hashlib.sha256((gold / other_rel).read_bytes()).hexdigest()
    (gold / target_rel).write_bytes(b"corrupt")
    recovered = recover_geo_partition(
        "MD", 2024, gold_root=gold, reference_root=config.DATA_DIR / "reference")
    assert recovered.sha256 == hashlib.sha256((gold / target_rel).read_bytes()).hexdigest()
    assert hashlib.sha256((gold / other_rel).read_bytes()).hexdigest() == before_other


def test_historical_dictionary_cutover_fires_and_current_is_silent():
    old = [ObservedValue("NONE DETECTED", 4, "2023-12-20", "2023-12-27")]
    new = ObservedValue(
        "Not Suspect of Alcohol Use, Not Suspect of Drug Use", 3,
        "2023-12-28", "2024-01-03")
    accepted_old = ("NONE DETECTED",)
    report = detect_drift("mmzv-x632", "driver_substance_abuse", [*old, new],
                          accepted=accepted_old, tokenised=False)
    assert report.drifted
    assert "Not Suspect" in report.unmapped[0].value
    current = detect_drift(
        "mmzv-x632", "driver_substance_abuse", [*old, new],
        accepted=vocabulary("mmzv-x632", "driver_substance_abuse"), tokenised=True)
    assert not current.drifted


def test_geoparquet_1_1_bbox_metadata_and_row_bounds(tmp_path):
    import geopandas as gpd
    from shapely.geometry import Point

    frame = gpd.GeoDataFrame(
        {"crash_sk": [1, 2]},
        geometry=[Point(-77.1, 39.1), Point(-77.2, 39.2)], crs="EPSG:4326")
    path = tmp_path / "part.parquet"
    write_geoparquet(frame, path, row_group_size=1)
    assert validate_geoparquet_tree(tmp_path) == []
    table = pq.read_table(path, columns=["bbox", "geometry"])
    boxes = table.column("bbox").to_pylist()
    assert boxes[0] == {"xmin": -77.1, "ymin": 39.1, "xmax": -77.1, "ymax": 39.1}


def _lineage(path: Path, ids: list[str]) -> None:
    pq.write_table(pa.table({"decision_lineage_id": ids, "payload": ids}), path)


def test_lineage_append_only_observation_passes_append_and_fails_truncate(tmp_path):
    path, state = tmp_path / "lineage.parquet", tmp_path / "state.json"
    _lineage(path, ["a", "b"])
    assert check_lineage_append_only(path, state, prefix_rows=2)[0]
    _lineage(path, ["a", "b", "c"])
    assert check_lineage_append_only(path, state, prefix_rows=2)[0]
    _lineage(path, ["a"])
    passed, metadata = check_lineage_append_only(path, state, prefix_rows=2)
    assert not passed
    assert metadata["violations"][0]["kind"] == "row-count-decreased"


def test_orchestration_has_no_business_geometry_or_wall_clock():
    text = "\n".join(path.read_text() for path in Path("orchestration").glob("*.py"))
    assert "3857" not in text
    assert "date.today" not in text
    assert "datetime.now" not in text
