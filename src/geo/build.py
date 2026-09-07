"""Gold + reference data in, `crash_geo` and `dim_block_group` out, deterministically.

    python -m src.geo.build
    python -m src.geo.build --gold-root /tmp/g --reference-root /tmp/ref
    python -m src.geo.build --skip-snap --skip-weather
    python -m src.geo.build --offline          # fail loudly, never silently skip
    python -m src.geo.build --json

Stage order is a correctness constraint, not a preference
---------------------------------------------------------
    reference -> census PIP -> H3 -> timezone -> snap -> weather

`weather` keys on the UTC hour, which does not exist until `tz` has localised
the naive wall clock, which needs the coordinate. Running weather before tz
would join on local time and be wrong by exactly one hour on the two days a year
with the most weather-related crashes -- invisibly. The dependency is why the
stages are a sequence in one function rather than independent jobs.

Outputs
-------
  data/gold/crash_geo.parquet                              CANONICAL
  data/gold/crash_geo/jurisdiction=XX/year=YYYY/part-0.parquet
  data/gold/dim_block_group.parquet
  data/gold/_geo_manifest.json

Both `crash_geo` artefacts are GeoParquet 1.1.0 with a `bbox` covering column
and identical content. The flat file is canonical because that is what Phases
5-7 actually do: join on `crash_sk` and scan every row for a hotspot surface.
The partitioned copy exists for the read the flat file is bad at -- a bounding-
box filter -- and is sorted by `h3_r9` within each partition so the covering
bbox actually prunes. A random row order would give every row group a bbox the
size of the county and prune nothing; the report has the measurement.

`fact_crash` and `dim_geography` are not touched. Phase 3's parquet hashes and
its 182 tests must not move, and an enrichment that can be rebuilt from hashed
reference inputs does not belong inside the fact it enriches.

Determinism
-----------
Two runs over the same gold and a warm reference cache produce byte-identical
parquet. Total sort order is `crash_sk` for the flat file and `(h3_r9,
crash_sk)` within each partition -- H3 first so the bbox column is monotone,
`crash_sk` to make the order total. Wall-clock time appears only in
`_geo_manifest.json`. `_geo_build_sha` is a hash of the INPUTS (the fact_crash
sha256 plus every reference file's sha256 plus the geo config), never of the
build time, so it is stable across runs and moves exactly when an input moves.

Restatement, not SCD2
---------------------
A changed TIGER, PBF or ACS hash restates the affected columns: rebuild, and the
manifest records the old and the new hash. There is deliberately no SCD2 on
enrichment columns. They are DERIVED -- a pure function of (crash coordinate,
reference file) -- so their history is fully reconstructible by re-running
against the older reference bytes, whose hash the manifest already holds.
Versioning a derived value in a second history table would create a second thing
that can disagree with the first, which is the argument Phase 3 makes for
keeping crash history in silver.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import logging
import sys
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterable

import duckdb
import geopandas as gpd
import numpy as np
import pandas as pd
import pyarrow.parquet as pq
import shapely

from .. import config, contracts
from ..config import GOLD_DIR
from ..ingest.watermark import durable_replace
from ..transform import common as c
from . import census_join, envelope, h3_index, reference, snap as snap_mod, tz as tz_mod
from . import weather as weather_mod

log = logging.getLogger("geo.build")

GOLD_CONTRACT = contracts.CONTRACTS_DIR / "gold.schema.json"

CRASH_GEO = "crash_geo"
DIM_BLOCK_GROUP = "dim_block_group"

# Column order IS the contract. Grouped exactly as contracts/gold.schema.json
# lists them: identity, geometry/census, H3, time, snap, weather, lineage.
CRASH_GEO_COLUMNS = [
    "crash_sk", "jurisdiction", "primary_source_system", "crash_date",
    "geometry", "geo_quality", "pip_county_geoid", "county_agrees_with_source",
    "tract_geoid", "bg_geoid", "pip_status",
    "h3_r9", "h3_r8", "h3_r7",
    "crash_datetime_local", "tz_iana", "tz_source", "tz_low_confidence",
    "crash_datetime_utc", "utc_offset_minutes", "tz_gap_adjusted", "tz_ambiguous",
    "time_status",
    "snap_status", "osm_way_id", "snap_distance_m", "segment_length_m",
    "offset_m", "offset_frac", "osm_highway", "osm_maxspeed", "osm_maxspeed_mph",
    "osm_lanes", "osm_name", "osm_ref", "snap_crs_epsg",
    "weather_status", "era5_hour_utc", "era5_temperature_2m_c",
    "era5_precipitation_mm", "era5_rain_mm", "era5_snowfall_cm",
    "era5_weather_code", "era5_wind_speed_10m_kmh", "era5_grid_lat", "era5_grid_lon",
    "_geo_build_sha", "tiger_vintage", "osm_pbf_sha256", "tz_db_version",
]


# ---------------------------------------------------------------------------
# manifest
# ---------------------------------------------------------------------------


@dataclass
class GeoManifest:
    """Inputs by hash, per-stage counts, outputs by hash. Only wall clock here."""

    gold_root: Path
    reference_root: Path
    built_at: str = field(default_factory=lambda: datetime.now(timezone.utc).isoformat())
    inputs: dict[str, Any] = field(default_factory=dict)
    reference_files: dict[str, Any] = field(default_factory=dict)
    outputs: dict[str, dict[str, Any]] = field(default_factory=dict)
    stats: dict[str, Any] = field(default_factory=dict)
    warnings: list[str] = field(default_factory=list)

    def warn(self, message: str) -> None:
        log.warning("%s", message)
        self.warnings.append(message)

    def write(self) -> Path:
        dest = self.gold_root / "_geo_manifest.json"
        dest.parent.mkdir(parents=True, exist_ok=True)
        payload = {
            "built_at": self.built_at,
            "gold_root": str(self.gold_root),
            "reference_root": str(self.reference_root),
            "config": {"geo": _jsonable(config.geo())},
            "inputs": dict(sorted(self.inputs.items())),
            "reference_files": dict(sorted(self.reference_files.items())),
            "outputs": dict(sorted(self.outputs.items())),
            "stats": dict(sorted(self.stats.items())),
            "warnings": self.warnings,
        }
        tmp = dest.with_name(dest.name + ".part")
        tmp.write_text(json.dumps(payload, indent=2, default=str) + "\n")
        durable_replace(tmp, dest)
        return dest


def _jsonable(value: Any) -> Any:
    if isinstance(value, dict):
        return {k: _jsonable(v) for k, v in value.items()}
    if isinstance(value, (list, tuple)):
        return [_jsonable(v) for v in value]
    if isinstance(value, (str, int, float, bool)) or value is None:
        return value
    return str(value)


def build_sha(fact_sha: str, reference_hashes: dict[str, str]) -> str:
    """A hash of the INPUTS: gold's fact_crash plus every reference file.

    Not of the build time, and not of the output. Stable across runs (so
    byte-identity holds) and it moves exactly when something that could change
    an enrichment value changes -- which is what makes it a restatement marker
    rather than a decoration. The geo config is folded in too, because a
    changed snap threshold changes the answers just as surely as a new PBF.
    """
    h = hashlib.sha256()
    h.update(fact_sha.encode())
    for name, sha in sorted(reference_hashes.items()):
        h.update(f"{name}={sha}".encode())
    h.update(json.dumps(_jsonable(config.geo()), sort_keys=True, default=str).encode())
    return h.hexdigest()


# ---------------------------------------------------------------------------
# gold in
# ---------------------------------------------------------------------------

FACT_COLUMNS = [
    "crash_sk", "jurisdiction", "primary_source_system", "crash_date",
    "crash_datetime_local", "latitude", "longitude", "geo_quality",
    "geography_sk", "weather_condition_sk",
]


def read_fact_crash(con: duckdb.DuckDBPyConnection, gold_root: Path) -> pd.DataFrame:
    path = gold_root / "fact_crash.parquet"
    if not path.exists():
        raise FileNotFoundError(
            f"no gold fact_crash at {path} -- build it with `python -m src.transform.model`"
        )
    p = str(path).replace("'", "''")
    cols = ", ".join(c.quote_ident(x) for x in FACT_COLUMNS)
    # ORDER BY crash_sk here so every downstream frame shares one row order and
    # the final sort is a no-op rather than a re-shuffle.
    return con.execute(
        f"SELECT {cols} FROM read_parquet('{p}') ORDER BY crash_sk"
    ).df()


def gold_hashes(gold_root: Path) -> dict[str, str]:
    manifest = gold_root / "_build_manifest.json"
    if manifest.exists():
        payload = json.loads(manifest.read_text())
        return {k: v["sha256"] for k, v in payload.get("outputs", {}).items()}
    return {
        p.stem: hashlib.sha256(p.read_bytes()).hexdigest()
        for p in sorted(gold_root.glob("*.parquet"))
    }


# ---------------------------------------------------------------------------
# stages
# ---------------------------------------------------------------------------


def stage_census(
    points: gpd.GeoDataFrame,
    store: reference.ReferenceStore,
    manifest: GeoManifest,
) -> pd.DataFrame:
    """County refinement + block-group PIP + the tract prefix cross-check."""
    counties = census_join.load_counties(store)
    county_result, county_stats = envelope.refine(points, counties)
    manifest.stats["county_refinement"] = county_stats

    bgs = census_join.load_block_groups(store)
    bg_result, bg_stats = census_join.point_in_polygon(
        points, bgs, key="crash_sk", out_col="bg_geoid"
    )
    bg_result["tract_geoid"] = census_join.tract_from_bg(
        bg_result["bg_geoid"].astype("string")
    )
    manifest.stats["block_group_pip"] = bg_stats

    tracts = census_join.load_tracts(store)
    manifest.stats["tract_prefix_check"] = census_join.verify_tract_prefix(
        points, tracts, bg_result, key="crash_sk"
    )

    merged = county_result.merge(bg_result, on="crash_sk", how="left")
    frame = points[["crash_sk", "primary_source_system"]].merge(
        merged, on="crash_sk", how="left"
    )
    manifest.stats["bbox_vs_polygon"] = envelope.bbox_versus_polygon(frame)
    manifest.stats["pip_hit_rate_by_source"] = _hit_rate_by(frame)
    return merged


def _hit_rate_by(frame: pd.DataFrame) -> dict[str, Any]:
    out: dict[str, Any] = {}
    for source, part in frame.groupby("primary_source_system"):
        with_geom = part[part["pip_status"] != census_join.PIP_NO_GEOMETRY]
        matched = int((part["pip_status"] == census_join.PIP_MATCHED).sum())
        out[str(source)] = {
            "rows": int(len(part)),
            "with_geometry": int(len(with_geom)),
            "matched": matched,
            "no_polygon": int((part["pip_status"] == census_join.PIP_NO_POLYGON).sum()),
            "hit_rate": round(matched / len(with_geom), 6) if len(with_geom) else None,
        }
    return out


def stage_h3(points: gpd.GeoDataFrame, manifest: GeoManifest) -> pd.DataFrame:
    cells = h3_index.index_points(zip(points["latitude"], points["longitude"]))
    df = pd.DataFrame(cells, index=points.index)
    df.insert(0, "crash_sk", points["crash_sk"].to_numpy())

    # The invariant, asserted rather than assumed: r8 and r7 are parents of the
    # stored r9, so any rollup built on them is consistent by construction.
    finest, parents = h3_index.resolutions()
    have = df[df[f"h3_r{finest}"].notna()]
    for r in parents:
        bad = int((have[f"h3_r{finest}"].map(lambda x: h3_index.parent(x, r))
                   != have[f"h3_r{r}"]).sum())
        if bad:
            raise AssertionError(
                f"h3: {bad} rows where cell_to_parent(h3_r{finest}, {r}) != h3_r{r}"
            )
    manifest.stats["h3"] = {
        "store_resolution": finest,
        "parent_resolutions": parents,
        "indexed": int(len(have)),
        "null_cells": int(len(df) - len(have)),
        f"distinct_r{finest}": int(have[f"h3_r{finest}"].nunique()),
        **{f"distinct_r{r}": int(have[f"h3_r{r}"].nunique()) for r in parents},
        "parent_invariant_checked": True,
    }
    return df


def stage_tz(
    df: pd.DataFrame,
    census: pd.DataFrame,
    manifest: GeoManifest,
    finder: Any | None = None,
) -> pd.DataFrame:
    """Coordinate zone, county/jurisdiction fallback, and the UTC stamp."""
    zf = tz_mod.ZoneFinder(finder)
    coord_zone = pd.Series(
        zf.zones_for(zip(df["latitude"], df["longitude"])), index=df.index, dtype="object"
    )

    # The county whose zone a coordinate-less row inherits. The polygon answer
    # when there is one (it is the better geography); the source's own county
    # otherwise -- which is all a row with no coordinate has.
    pip_county = census.set_index("crash_sk")["pip_county_geoid"].reindex(
        df["crash_sk"].to_numpy()
    )
    pip_county.index = df.index
    county = pip_county.astype("object").where(
        pip_county.notna(), df["source_county_geoid"].astype("object")
    )

    county_zones = tz_mod.county_zone_table(zip(county, coord_zone))
    resolved = [
        tz_mod.resolve_zone(
            coordinate_zone=cz, county_geoid=cg, jurisdiction=j,
            county_zones=county_zones,
        )
        for cz, cg, j in zip(coord_zone, county, df["jurisdiction"])
    ]
    out = pd.DataFrame(
        resolved, index=df.index,
        columns=["tz_iana", "tz_source", "tz_low_confidence"],
    )
    out.insert(0, "crash_sk", df["crash_sk"].to_numpy())

    local = pd.to_datetime(df["crash_datetime_local"])
    localised = [
        tz_mod.localise(None if pd.isna(ts) else ts.to_pydatetime(), zone)
        for ts, zone in zip(local, out["tz_iana"])
    ]
    out["crash_datetime_utc"] = pd.Series(
        [l.utc for l in localised], index=df.index
    ).astype("datetime64[us, UTC]")
    out["utc_offset_minutes"] = pd.array(
        [l.offset_minutes for l in localised], dtype="Int32"
    )
    out["tz_gap_adjusted"] = [l.gap_adjusted for l in localised]
    out["tz_ambiguous"] = [l.ambiguous for l in localised]
    out["time_status"] = [l.time_status for l in localised]

    split = {k: v for k, v in county_zones.items() if v.split_tz}
    unexpected = _unexpected_zones(df["jurisdiction"], out["tz_iana"], out["tz_source"])
    manifest.stats["timezone"] = {
        "timezonefinder_version": tz_mod.timezonefinder_version(),
        "source_counts": {k: int(v) for k, v in
                          out["tz_source"].value_counts().sort_index().items()},
        "zone_counts": {str(k): int(v) for k, v in
                        out["tz_iana"].value_counts().sort_index().items()},
        "zone_by_jurisdiction": _cross(df["jurisdiction"], out["tz_iana"]),
        "zone_by_source_system": _cross(df["primary_source_system"], out["tz_iana"]),
        "counties_with_derived_zone": len(county_zones),
        "split_tz_counties": {
            k: {"modal_zone": v.tz_iana, "share": round(v.share, 6),
                "coordinate_rows": v.n_coordinate_rows}
            for k, v in sorted(split.items())
        },
        "low_confidence_rows": int(out["tz_low_confidence"].sum()),
        "time_status_counts": {k: int(v) for k, v in
                               out["time_status"].value_counts().sort_index().items()},
        "dst": tz_mod.dst_census(
            list(zip(pd.to_datetime(df["crash_date"]).dt.year, localised))
        ),
        "utc_offset_counts": {str(k): int(v) for k, v in
                              out["utc_offset_minutes"].value_counts().sort_index().items()},
        "unexpected_zones": unexpected,
    }
    for j, zones in sorted(unexpected.items()):
        manifest.warn(
            f"{sum(zones.values())} {j} row(s) resolved to a zone outside "
            f"config/geo.toml [tz.expected_zones]: {zones}. These are coordinate "
            f"errors surfaced by the zone lookup, not zone errors; the coordinate "
            f"is kept and counted, never corrected."
        )
    return out


def _unexpected_zones(
    jurisdiction: pd.Series, zone: pd.Series, source: pd.Series
) -> dict[str, dict[str, int]]:
    """Coordinate-derived zones a jurisdiction has no business producing.

    Maryland is entirely Eastern; Texas is Central and Mountain; Florida is
    Eastern and Central. A row outside that set has a coordinate that is not
    where the source says it is -- across the Rio Grande, or in the Gulf. The
    zone lookup is the cheapest detector for it we have, so the count is
    reported; the row is not touched.
    """
    expected = config.geo()["tz"]["expected_zones"]
    out: dict[str, dict[str, int]] = {}
    mask = source == tz_mod.TZ_SOURCE_COORDINATE
    for j, z in zip(jurisdiction[mask], zone[mask]):
        if z is None or j is None:
            continue
        if z not in expected.get(str(j), []):
            out.setdefault(str(j), {}).setdefault(str(z), 0)
            out[str(j)][str(z)] += 1
    return {k: dict(sorted(v.items())) for k, v in sorted(out.items())}


def _cross(left: pd.Series, right: pd.Series) -> dict[str, dict[str, int]]:
    tab = pd.crosstab(left, right)
    return {str(i): {str(col): int(v) for col, v in row.items() if v}
            for i, row in tab.iterrows()}


def stage_snap(
    points: gpd.GeoDataFrame,
    store: reference.ReferenceStore,
    manifest: GeoManifest,
    *,
    enabled: bool = True,
) -> pd.DataFrame:
    """Nearest OSM road for the in-scope source systems; everything else NOT_ATTEMPTED."""
    systems = list(config.geo()["snap"]["source_systems"])
    out = pd.DataFrame({"crash_sk": points["crash_sk"].to_numpy()}, index=points.index)
    for col in snap_mod.SNAP_COLUMNS:
        out[col] = pd.NA
    out["snap_status"] = snap_mod.SNAP_NOT_ATTEMPTED
    out.loc[points.geometry.isna().to_numpy(), "snap_status"] = snap_mod.SNAP_NO_GEOMETRY

    if not enabled:
        manifest.stats["snap"] = {"enabled": False, "source_systems": systems,
                                  "reason": "--skip-snap"}
        return _typed_snap(out)

    in_scope = points["primary_source_system"].isin(systems).to_numpy()
    subset = points[in_scope]
    if subset.empty:
        manifest.stats["snap"] = {"enabled": True, "source_systems": systems,
                                  "attempted": 0}
        return _typed_snap(out)

    roads = snap_mod.build_road_network(store)
    # One projected CRS per jurisdiction, from config/geo.toml [crs.snap].
    # Montgomery is entirely Maryland, so one call covers it; a multi-state
    # snap would loop here rather than pick a compromise CRS.
    jurisdictions = sorted(subset["jurisdiction"].dropna().unique())
    stats: dict[str, Any] = {"enabled": True, "source_systems": systems,
                             "by_jurisdiction": {}}
    for j in jurisdictions:
        part = subset[subset["jurisdiction"] == j]
        epsg = snap_mod.snap_crs_for(j)
        result, s = snap_mod.snap_points(part, roads, key="crash_sk", epsg=epsg)
        s["linear_reference_check"] = snap_mod.verify_linear_reference(
            part, roads, result, key="crash_sk", epsg=epsg
        )
        stats["by_jurisdiction"][j] = s
        result = result.set_index("crash_sk")
        idx = out.index[out["crash_sk"].isin(set(part["crash_sk"]))]
        take = result.reindex(out.loc[idx, "crash_sk"].to_numpy())
        for col in snap_mod.SNAP_COLUMNS:
            out.loc[idx, col] = take[col].to_numpy()

    typed = _typed_snap(out)
    stats["status_counts"] = {k: int(v) for k, v in
                              typed["snap_status"].value_counts().sort_index().items()}
    manifest.stats["snap"] = stats
    return typed


def _typed_snap(out: pd.DataFrame) -> pd.DataFrame:
    """Pin every snap column's dtype, including when the stage did not run.

    An all-NULL object column is typeless, and a typeless column reaches DuckDB
    as INTEGER and the contract as a type violation. Declaring the dtype here
    means `--skip-snap` produces a table with the same SCHEMA as a full build
    and only different values -- which is what makes the two comparable.
    """
    out["osm_way_id"] = pd.to_numeric(out["osm_way_id"], errors="coerce").astype("Int64")
    out["snap_crs_epsg"] = pd.to_numeric(out["snap_crs_epsg"], errors="coerce").astype("Int32")
    out["snap_status"] = out["snap_status"].astype("string")
    for col in ("snap_distance_m", "segment_length_m", "offset_m", "offset_frac",
                "osm_maxspeed_mph"):
        out[col] = pd.to_numeric(out[col], errors="coerce").astype("float64")
    for col in ("osm_highway", "osm_maxspeed", "osm_lanes", "osm_name", "osm_ref"):
        out[col] = out[col].astype("string")
    return out


# ---------------------------------------------------------------------------
# assembly + write
# ---------------------------------------------------------------------------


def assemble(
    fact: pd.DataFrame,
    points: gpd.GeoDataFrame,
    census: pd.DataFrame,
    h3: pd.DataFrame,
    times: pd.DataFrame,
    snapped: pd.DataFrame,
    weather: pd.DataFrame,
    *,
    geo_build_sha: str,
    osm_sha: str | None,
) -> gpd.GeoDataFrame:
    """One frame, contract column order, one row per `crash_sk`."""
    ref = config.geo()["reference"]
    df = pd.DataFrame({
        "crash_sk": fact["crash_sk"].to_numpy(),
        "jurisdiction": fact["jurisdiction"].to_numpy(),
        "primary_source_system": fact["primary_source_system"].to_numpy(),
        "crash_date": fact["crash_date"].to_numpy(),
        "geo_quality": fact["geo_quality"].to_numpy(),
        "crash_datetime_local": fact["crash_datetime_local"].to_numpy(),
    })
    for part in (census, h3, times, snapped, weather):
        cols = [x for x in part.columns if x != "crash_sk"]
        df = pd.concat([df, part[cols].reset_index(drop=True)], axis=1)

    df["_geo_build_sha"] = geo_build_sha
    df["tiger_vintage"] = str(ref["tiger_year"])
    df["osm_pbf_sha256"] = osm_sha
    df["tz_db_version"] = f"timezonefinder {tz_mod.timezonefinder_version()}"

    # Every text column gets pandas' StringDtype rather than object. An object
    # column that happens to hold only None is typeless, and typeless columns
    # land in DuckDB as INTEGER and in parquet as null -- so the schema would
    # depend on the DATA, which is exactly what a contract exists to prevent.
    for col in ("jurisdiction", "primary_source_system", "geo_quality",
                "pip_county_geoid", "bg_geoid", "tract_geoid", "pip_status",
                "h3_r9", "h3_r8", "h3_r7", "tz_iana", "tz_source", "time_status",
                "weather_status", "_geo_build_sha", "tiger_vintage",
                "osm_pbf_sha256", "tz_db_version"):
        df[col] = df[col].astype("string")
    # DATE, not TIMESTAMP: DuckDB's .df() widens a DATE to datetime64, and a
    # midnight timestamp standing in for a date is the kind of type drift that
    # only shows up when somebody compares it to a real timestamp.
    df["crash_date"] = pd.to_datetime(df["crash_date"]).dt.date
    df["crash_datetime_local"] = pd.to_datetime(
        df["crash_datetime_local"]).astype("datetime64[us]")
    df["county_agrees_with_source"] = df["county_agrees_with_source"].astype("boolean")
    for col in ("tz_gap_adjusted", "tz_ambiguous", "tz_low_confidence"):
        df[col] = df[col].astype(bool)
    df["utc_offset_minutes"] = df["utc_offset_minutes"].astype("Int32")

    gdf = gpd.GeoDataFrame(df, geometry=points.geometry.reset_index(drop=True),
                           crs=points.crs)
    gdf = gdf[CRASH_GEO_COLUMNS]
    # Total order: crash_sk. The flat file is the canonical artefact and this is
    # what makes its bytes reproducible.
    return gdf.sort_values("crash_sk", ignore_index=True)


def register(con: duckdb.DuckDBPyConnection, name: str, gdf: gpd.GeoDataFrame) -> str:
    """Register a frame for contract validation, geometry as WKB.

    Validation runs on exactly what will be written: GeoParquet stores geometry
    as WKB, so the validator sees WKB (a BLOB, which the contract types as a
    string). Validating a shapely-object column would be validating something
    that never reaches disk.
    """
    df = pd.DataFrame(gdf.drop(columns=["geometry"]) if "geometry" in gdf else gdf)
    if "geometry" in gdf:
        df.insert(
            list(gdf.columns).index("geometry"),
            "geometry",
            shapely.to_wkb(np.asarray(gdf.geometry.values)),
        )
        df = df[list(gdf.columns)]
    con.register(f"{name}__src", df)
    con.execute(f"CREATE OR REPLACE TABLE {name} AS SELECT * FROM {name}__src")
    con.unregister(f"{name}__src")
    return name


def write_geoparquet(
    gdf: gpd.GeoDataFrame, dest: Path, *, row_group_size: int
) -> dict[str, Any]:
    """GeoParquet 1.1.0 with the `bbox` covering column, written durably.

    `write_covering_bbox=True` adds a struct column of per-row xmin/ymin/xmax/
    ymax and declares it in the `geo` metadata as the primary column's covering.
    Parquet's own row-group statistics over that struct are what a reader prunes
    on, which is why the row order matters as much as the column does.
    """
    dest.parent.mkdir(parents=True, exist_ok=True)
    tmp = dest.with_name(dest.name + ".part")
    gdf.to_parquet(
        tmp,
        index=False,
        schema_version="1.1.0",
        write_covering_bbox=True,
        compression=config.geo()["geoparquet"]["compression"],
        row_group_size=row_group_size,
    )
    durable_replace(tmp, dest)
    pf = pq.ParquetFile(dest)
    return {
        "path": str(dest),
        "rows": int(pf.metadata.num_rows),
        "columns": len(gdf.columns),
        "bytes": dest.stat().st_size,
        "sha256": hashlib.sha256(dest.read_bytes()).hexdigest(),
        "row_groups": pf.num_row_groups,
        "row_group_size": row_group_size,
    }


def write_partitioned(gdf: gpd.GeoDataFrame, root: Path) -> dict[str, Any]:
    """Hive-partitioned GeoParquet: jurisdiction=XX/year=YYYY/part-0.parquet.

    Rows are sorted by `(h3_r9, crash_sk)` INSIDE each partition, which is the
    point of the exercise: H3 cells at one resolution are a space-filling curve,
    so consecutive rows are spatially adjacent and each row group's covering
    bbox is small. In a random order every row group's bbox is the whole
    partition and the covering column prunes nothing while still costing four
    doubles per row. Ungeocoded rows sort first (NULL h3) into their own row
    groups, so a spatial query skips them wholesale.

    `jurisdiction` stays a column as well as a directory: a partition file that
    is read on its own must still be a complete `crash_geo` row, and DuckDB's
    hive reader is told `hive_partitioning=0` in the contract test for exactly
    that reason.
    """
    if root.exists():
        for p in sorted(root.rglob("*.parquet")):
            p.unlink()
    cfg = config.geo()["geoparquet"]
    years = pd.to_datetime(gdf["crash_date"]).dt.year
    written: list[dict[str, Any]] = []
    for (j, year), part in gdf.groupby(
        [gdf["jurisdiction"], years], sort=True, dropna=False
    ):
        part = part.sort_values(["h3_r9", "crash_sk"], ignore_index=True,
                                na_position="first")
        dest = root / f"jurisdiction={j}" / f"year={int(year)}" / "part-0.parquet"
        written.append(write_geoparquet(part, dest,
                                        row_group_size=int(cfg["row_group_size"])))
    return {
        "root": str(root),
        "partitions": len(written),
        "rows": sum(w["rows"] for w in written),
        "bytes": sum(w["bytes"] for w in written),
        "row_groups": sum(w["row_groups"] for w in written),
        "row_group_size": int(cfg["row_group_size"]),
        # A directory of files has no single sha; the per-file hashes are the
        # thing the byte-identity test compares.
        "files": {str(Path(w["path"]).relative_to(root)): w["sha256"] for w in written},
    }


# ---------------------------------------------------------------------------
# the build
# ---------------------------------------------------------------------------


def build_geo(
    *,
    gold_root: Path | None = None,
    reference_root: Path | None = None,
    skip_snap: bool = False,
    skip_weather: bool = False,
    offline: bool = False,
    validate: bool = True,
    small_corpus: bool = False,
    threads: int | None = None,
    zone_finder: Any | None = None,
    http_client: Any | None = None,
) -> dict[str, Any]:
    gold = Path(gold_root) if gold_root else GOLD_DIR
    ref_root = reference.reference_root(reference_root)
    store = reference.ReferenceStore(ref_root, offline=offline)
    con = c.connect(threads=threads)
    manifest = GeoManifest(gold_root=gold, reference_root=ref_root)

    log.info("reading gold from %s", gold)
    fact = read_fact_crash(con, gold)
    manifest.inputs = gold_hashes(gold)
    fact["source_county_geoid"] = envelope.source_county_geoid(fact["geography_sk"])
    points = census_join.points_frame(fact)

    log.info("reference data")
    if not offline:
        reference.ensure_all_tiger(store)
        if not skip_snap:
            reference.ensure_osm(store)
    acs = reference.ensure_acs(store) if not offline else {
        "route": "api" if config.key("census_api_key") else "summary_file",
        "paths": ({f: reference.acs_api_relpath(f)
                   for f in config.geo()["reference"]["state_fips"]}
                  if config.key("census_api_key")
                  else {"all": reference.acs_summary_relpath()}),
    }
    manifest.reference_files = store.manifest
    osm_entry = store.entry(reference.osm_relpath("maryland")) or {}
    geo_sha = build_sha(manifest.inputs.get("fact_crash", ""), store.hashes())

    log.info("dim_block_group")
    dim_bg, bg_stats = census_join.build_dim_block_group(store, acs)
    manifest.stats["dim_block_group"] = bg_stats

    log.info("census point-in-polygon")
    census = stage_census(points, store, manifest)

    log.info("h3")
    h3 = stage_h3(points, manifest)

    log.info("timezone")
    times = stage_tz(fact, census, manifest, finder=zone_finder)

    log.info("road snapping")
    snapped = stage_snap(points, store, manifest, enabled=not skip_snap)

    log.info("weather")
    with_utc = fact.assign(crash_datetime_utc=times["crash_datetime_utc"].to_numpy())
    weather, weather_stats = weather_mod.build(
        with_utc, reference_root=ref_root, client=http_client,
        offline=offline, enabled=not skip_weather,
    )
    manifest.stats["weather"] = weather_stats

    log.info("assembling crash_geo")
    crash_geo = assemble(
        fact, points, census, h3, times, snapped, weather,
        geo_build_sha=geo_sha, osm_sha=osm_entry.get("sha256"),
    )
    manifest.stats["reconciliation"] = reconcile(fact, crash_geo)
    manifest.stats["weather_precipitation_agreement"] = _precip_agreement(
        con, gold, crash_geo
    )

    if validate:
        _validate(con, gold, crash_geo, dim_bg, small_corpus=small_corpus)

    cfg = config.geo()["geoparquet"]
    manifest.outputs[CRASH_GEO] = write_geoparquet(
        crash_geo, gold / f"{CRASH_GEO}.parquet",
        row_group_size=int(cfg["flat_row_group_size"]),
    )
    manifest.outputs[f"{CRASH_GEO}_partitioned"] = write_partitioned(
        crash_geo, gold / CRASH_GEO
    )
    register(con, "dim_block_group_out", dim_bg)
    manifest.outputs[DIM_BLOCK_GROUP] = c.write_parquet(
        con, "dim_block_group_out", gold / f"{DIM_BLOCK_GROUP}.parquet",
        columns=census_join.DIM_BLOCK_GROUP_COLUMNS, order_by=["bg_geoid"],
    )

    manifest.write()
    con.close()
    return {
        "outputs": manifest.outputs,
        "stats": manifest.stats,
        "warnings": manifest.warnings,
        "gold_root": str(gold),
        "reference_root": str(ref_root),
    }


def reconcile(fact: pd.DataFrame, crash_geo: gpd.GeoDataFrame) -> dict[str, Any]:
    """fact_crash <-> crash_geo row accounting. Every number here has a test."""
    missing = set(fact["crash_sk"]) - set(crash_geo["crash_sk"])
    extra = set(crash_geo["crash_sk"]) - set(fact["crash_sk"])
    ok = crash_geo["geo_quality"] == "OK"
    return {
        "fact_crash_rows": int(len(fact)),
        "crash_geo_rows": int(len(crash_geo)),
        "distinct_crash_sk": int(crash_geo["crash_sk"].nunique()),
        "missing_from_crash_geo": len(missing),
        "not_in_fact_crash": len(extra),
        "with_geometry": int(crash_geo["geometry"].notna().sum()),
        "geo_quality_ok": int(ok.sum()),
        "ok_rows_with_bg": int((ok & crash_geo["bg_geoid"].notna()).sum()),
        "ok_rows_with_h3": int((ok & crash_geo["h3_r9"].notna()).sum()),
        "rows_with_tz": int(crash_geo["tz_iana"].notna().sum()),
        "rows_with_utc": int(crash_geo["crash_datetime_utc"].notna().sum()),
        "rows_with_local_time": int(crash_geo["crash_datetime_local"].notna().sum()),
        "time_unknown": int((crash_geo["time_status"] == "TIME_UNKNOWN").sum()),
    }


def _precip_agreement(
    con: duckdb.DuckDBPyConnection, gold: Path, crash_geo: gpd.GeoDataFrame
) -> dict[str, Any]:
    """Officer-reported precipitation vs ERA5, joined through dim_weather_condition."""
    fact = gold / "fact_crash.parquet"
    dim = gold / "dim_weather_condition.parquet"
    if not (fact.exists() and dim.exists()):
        return {}
    officer = con.execute(f"""
        SELECT f.crash_sk, d.is_precipitation AS officer_is_precipitation
        FROM read_parquet('{str(fact).replace("'", "''")}') f
        JOIN read_parquet('{str(dim).replace("'", "''")}') d USING (weather_condition_sk)
    """).df()
    merged = crash_geo[["crash_sk", "weather_status", "era5_precipitation_mm"]].merge(
        officer, on="crash_sk", how="left"
    )
    return weather_mod.precipitation_agreement(merged)


def _validate(
    con: duckdb.DuckDBPyConnection,
    gold: Path,
    crash_geo: gpd.GeoDataFrame,
    dim_bg: pd.DataFrame,
    *,
    small_corpus: bool,
) -> None:
    """Both new tables against contracts/gold.schema.json, before the first write.

    Same discipline as Phases 2 and 3: a contract failure must leave the
    previous outputs exactly as they were. A half-replaced gold is worse than a
    stale one because the stale one is at least internally consistent.
    """
    contract = contracts.load_contract(GOLD_CONTRACT)
    register(con, "crash_geo_out", crash_geo)
    register(con, "dim_block_group_out", dim_bg)
    fact = gold / "fact_crash.parquet"
    if fact.exists():
        con.execute(
            "CREATE OR REPLACE VIEW fact_crash_ref AS SELECT * FROM read_parquet('%s')"
            % str(fact).replace("'", "''")
        )
    resolve = {
        "gold.crash_geo": "crash_geo_out",
        "gold.dim_block_group": "dim_block_group_out",
        "gold.fact_crash": "fact_crash_ref" if fact.exists() else None,
    }
    resolve = {k: v for k, v in resolve.items() if v}

    violations: list[contracts.Violation] = []
    for table, relation in (("gold.crash_geo", "crash_geo_out"),
                            ("gold.dim_block_group", "dim_block_group_out")):
        violations += contracts.validate_relation(
            con, relation, contract, table, check_row_count_min=not small_corpus
        )
        violations += contracts.validate_foreign_keys(
            con, contract, table, relation, resolve
        )
    contracts.raise_for(violations, context="gold.schema.json (geo)")


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------


def _cli(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(
        prog="python -m src.geo.build",
        description="Build the geospatial enrichment over gold.",
    )
    ap.add_argument("--gold-root", type=Path, default=None)
    ap.add_argument("--reference-root", type=Path, default=None)
    ap.add_argument("--skip-snap", action="store_true",
                    help="leave every row snap_status = NOT_ATTEMPTED")
    ap.add_argument("--skip-weather", action="store_true",
                    help="leave every row weather_status = NOT_IN_SCOPE")
    ap.add_argument("--offline", action="store_true",
                    help="never touch the network; fail naming the missing file")
    ap.add_argument("--no-validate", action="store_true")
    ap.add_argument("--small-corpus", action="store_true",
                    help="skip row_count_min floors (fixture-scale builds)")
    ap.add_argument("--threads", type=int, default=None)
    ap.add_argument("--json", action="store_true")
    ap.add_argument("-v", "--verbose", action="store_true")
    args = ap.parse_args(argv)
    logging.basicConfig(level=logging.DEBUG if args.verbose else logging.INFO,
                        format="%(levelname)s %(name)s %(message)s")

    result = build_geo(
        gold_root=args.gold_root, reference_root=args.reference_root,
        skip_snap=args.skip_snap, skip_weather=args.skip_weather,
        offline=args.offline, validate=not args.no_validate,
        small_corpus=args.small_corpus, threads=args.threads,
    )
    if args.json:
        print(json.dumps(result, indent=2, default=str))
    else:
        print(f"\ngeo -> {result['gold_root']}")
        for name, info in sorted(result["outputs"].items()):
            if "sha256" in info:
                print(f"  {name:<28} {info['rows']:>9} rows  "
                      f"{info['bytes'] / 1e6:>7.2f} MB  {info['row_groups']:>4} rg  "
                      f"{info['sha256'][:16]}")
            else:
                print(f"  {name:<28} {info['rows']:>9} rows  "
                      f"{info['bytes'] / 1e6:>7.2f} MB  {info['partitions']} partitions")
        rec = result["stats"]["reconciliation"]
        tz_stats = result["stats"]["timezone"]
        print(f"\n  fact_crash {rec['fact_crash_rows']} -> crash_geo "
              f"{rec['crash_geo_rows']} ({rec['with_geometry']} with geometry, "
              f"{rec['ok_rows_with_bg']} with a block group)")
        print(f"  tz_source: {tz_stats['source_counts']}")
        print(f"  DST: {tz_stats['dst']['gap_adjusted_total']} gap-adjusted, "
              f"{tz_stats['dst']['ambiguous_total']} ambiguous")
        for w in result["warnings"]:
            print(f"  WARNING: {w}")
    return 0


if __name__ == "__main__":
    sys.exit(_cli())
