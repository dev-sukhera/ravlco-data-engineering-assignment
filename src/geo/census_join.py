"""Point-in-polygon to TIGER tract and block group, and `dim_block_group`.

Two jobs.

**The join.** Every crash with a usable coordinate gets the 2025 TIGER block
group it falls in; the tract is `bg_geoid[:11]`, because a block-group GEOID is
by construction `STATE(2) COUNTY(3) TRACT(6) BG(1)` and the tract is a literal
prefix. A separate `dim_tract` would be a table whose only content is a
substring of another table's key. The build cross-checks the prefix against a
direct join to the TIGER TRACT layer on a sample; a disagreement would mean the
two TIGER layers are internally inconsistent, which is a finding rather than a
tuning problem, so it is measured rather than assumed.

**The dimension.** One row per block group in the three states with TIGER's
stored `ALAND`/`AWATER` in m2 and the ACS 2023 5-year population, so Phase 5 has
a denominator. `density_per_km2 = population / (ALAND / 1e6)` is ARITHMETIC ON A
STORED NUMBER, not a geometric operation -- TIGER already computed the area on
the authoritative geometry and recomputing it would introduce a projection
question for no gain. (If it were computed, it would be in EPSG:5070: equal-area
across three states is the whole point. Nothing here computes it.)


CRS
---
The join runs in **EPSG:4326** and involves no projection at all. Point-in-
polygon is a TOPOLOGICAL predicate: whether a point is inside a ring is
invariant under any continuous, non-self-intersecting map, so projecting first
buys nothing and costs a reprojection of 33,000 polygons. This is the one
spatial operation in the whole phase that is legitimately done in geographic
coordinates, and it is worth saying out loud because "always reproject" is the
rule people over-apply.

TIGER ships NAD83, EPSG:4269. At crash-coordinate precision NAD83 and WGS84
differ by about a metre -- far inside the geocoding error of all three feeds --
but the frames are still different, so the polygons are reprojected to 4326
explicitly rather than relabelled. The cost is milliseconds and the alternative
is a claim in a comment instead of a call in the code.


Boundary points
---------------
`within` drops a point that lies exactly on a shared edge; `intersects` matches
it twice. Neither is safe on its own. The rule (`config/geo.toml [pip]`) is
`intersects` -- never lose a real point -- followed by a deterministic
de-duplication on the smallest GEOID, with the number of rows that needed the
tie-break reported. A large count would mean the layers overlap, which TIGER's
do not; a small one is the handful of crashes reported at a centreline that is
itself the block-group boundary.


ACS
---
Population only: `B01003_001E`. Median household income (`B19013_001E`),
vehicles by tenure (`B25044`) and means of transport to work (`B08301`) are
deliberately NOT fetched. They are the protected-class proxies ASSIGNMENT.md
Part 4 warns about, and the strongest guarantee that a feature does not leak
into a lead score is that the pipeline never loaded it. Population is different
in kind: it is a DENOMINATOR used in aggregate to normalise a hotspot rate, and
it never becomes a per-record attribute of a lead.
"""

from __future__ import annotations

import io
import json
import logging
import zipfile
from pathlib import Path
from typing import Any, Iterable, Sequence

import geopandas as gpd
import pandas as pd

from .. import config
from . import reference

log = logging.getLogger("geo.census_join")

PIP_MATCHED = "MATCHED"
PIP_NO_POLYGON = "NO_POLYGON"
PIP_NO_GEOMETRY = "NO_GEOMETRY"
PIP_STATUS_VALUES = (PIP_MATCHED, PIP_NO_POLYGON, PIP_NO_GEOMETRY)

# Columns kept from TIGER. ALAND/AWATER are m2 as published; INTPTLAT/INTPTLON
# are the Census's own internal point (guaranteed inside the polygon), kept
# because a representative point that TIGER already computed beats one we
# derive.
BG_FIELDS = ["GEOID", "STATEFP", "COUNTYFP", "TRACTCE", "ALAND", "AWATER",
             "INTPTLAT", "INTPTLON"]
TRACT_FIELDS = ["GEOID", "STATEFP", "COUNTYFP", "ALAND", "AWATER"]
COUNTY_FIELDS = ["GEOID", "STATEFP", "COUNTYFP", "NAME", "NAMELSAD",
                 "ALAND", "AWATER", "INTPTLAT", "INTPTLON"]


# ---------------------------------------------------------------------------
# TIGER
# ---------------------------------------------------------------------------


def _read_zip(path: Path, fields: Sequence[str]) -> gpd.GeoDataFrame:
    """One TIGER zip to a GeoDataFrame in EPSG:4326.

    Read straight out of the zip (`/vsizip/`) so the cached raw bytes stay the
    only copy of the source; nothing is unpacked to disk where it could drift
    from the hash the manifest recorded.
    """
    gdf = gpd.read_file(f"/vsizip/{path}", columns=list(fields))
    src = gdf.crs
    # TIGER is NAD83 (EPSG:4269). Reproject to the storage CRS explicitly --
    # PIP would not care, but relabelling a frame instead of transforming it is
    # the kind of shortcut that is invisible until somebody joins against a
    # WGS84 dataset at sub-metre precision.
    gdf = gdf.to_crs(config.geo()["crs"]["storage"])
    log.debug("%s: %d features, %s -> %s", path.name, len(gdf), src, gdf.crs)
    return gdf


def _cached_layer(
    store: reference.ReferenceStore,
    name: str,
    build: Any,
) -> gpd.GeoDataFrame:
    """A parsed layer, cached as GeoParquet beside the raw zips.

    Derived from files whose hashes the manifest records, so it is safe to
    delete and reproducible; the cache exists because re-reading three state
    block-group shapefiles costs seconds on every run and the geo build is
    meant to be re-runnable while iterating.
    """
    dest = store.parsed(f"{name}.parquet")
    if dest.exists():
        return gpd.read_parquet(dest)
    gdf = build()
    gdf.to_parquet(dest, index=False)
    return gdf


def load_block_groups(
    store: reference.ReferenceStore, states: Iterable[str] | None = None
) -> gpd.GeoDataFrame:
    fips = list(states or config.geo()["reference"]["state_fips"])

    def build() -> gpd.GeoDataFrame:
        year = int(config.geo()["reference"]["tiger_year"])
        parts = [
            _read_zip(store.require(reference.tiger_relpath("BG", year=year, fips=f)),
                      BG_FIELDS)
            for f in fips
        ]
        gdf = gpd.GeoDataFrame(pd.concat(parts, ignore_index=True), crs=parts[0].crs)
        return gdf.sort_values("GEOID", ignore_index=True)

    return _cached_layer(store, "tiger_bg_" + "_".join(fips), build)


def load_tracts(
    store: reference.ReferenceStore, states: Iterable[str] | None = None
) -> gpd.GeoDataFrame:
    fips = list(states or config.geo()["reference"]["state_fips"])

    def build() -> gpd.GeoDataFrame:
        year = int(config.geo()["reference"]["tiger_year"])
        parts = [
            _read_zip(store.require(reference.tiger_relpath("TRACT", year=year, fips=f)),
                      TRACT_FIELDS)
            for f in fips
        ]
        gdf = gpd.GeoDataFrame(pd.concat(parts, ignore_index=True), crs=parts[0].crs)
        return gdf.sort_values("GEOID", ignore_index=True)

    return _cached_layer(store, "tiger_tract_" + "_".join(fips), build)


NATIONAL = "national"


def load_counties(
    store: reference.ReferenceStore, states: Iterable[str] | str | None = NATIONAL
) -> gpd.GeoDataFrame:
    """The TIGER COUNTY layer. National by default; pass FIPS codes to filter.

    National is the default because filtering to the three in-scope states
    makes the county PIP LIE: 79 Montgomery-reported crashes fall inside the
    padded envelope but in the District of Columbia, and against a three-state
    layer they come back `NO_POLYGON` -- indistinguishable from a point in the
    Atlantic. 3,235 polygons cost nothing in a spatial index, and the answer
    "this crash is in DC" is the answer the refinement exists to give.
    """
    fips = None if states == NATIONAL else list(states or [])
    name = "tiger_county_national" if fips is None else "tiger_county_" + "_".join(fips)

    def build() -> gpd.GeoDataFrame:
        year = int(config.geo()["reference"]["tiger_year"])
        gdf = _read_zip(store.require(reference.tiger_relpath("COUNTY", year=year)),
                        COUNTY_FIELDS)
        if fips is not None:
            gdf = gdf[gdf["STATEFP"].isin(fips)]
        return gdf.sort_values("GEOID", ignore_index=True)

    return _cached_layer(store, name, build)


# ---------------------------------------------------------------------------
# the join
# ---------------------------------------------------------------------------


def points_frame(
    df: pd.DataFrame, *, lat: str = "latitude", lon: str = "longitude"
) -> gpd.GeoDataFrame:
    """A GeoDataFrame of POINTs in EPSG:4326; NULL coordinates give NULL geometry.

    A row with no coordinate keeps its row. GeoParquet permits a null geometry
    and dropping the row here would break the one-row-per-`crash_sk` guarantee
    that makes `crash_geo` joinable to `fact_crash` without a left join.
    """
    geom = gpd.points_from_xy(df[lon], df[lat], crs=config.geo()["crs"]["storage"])
    out = gpd.GeoDataFrame(df.copy(), geometry=geom, crs=config.geo()["crs"]["storage"])
    missing = df[lat].isna() | df[lon].isna()
    out.loc[missing, out.geometry.name] = None
    return out


def point_in_polygon(
    points: gpd.GeoDataFrame,
    polygons: gpd.GeoDataFrame,
    *,
    key: str,
    geoid_col: str = "GEOID",
    out_col: str = "geoid",
) -> tuple[pd.DataFrame, dict[str, Any]]:
    """PIP `points` into `polygons`, one row per input key. Returns (frame, stats).

    Runs in EPSG:4326 with no projection: point-in-polygon is topological (see
    the module docstring). Both sides are asserted to be in the same CRS rather
    than silently aligned, because a silent `to_crs` inside a join is how a
    pipeline ends up doing spatial work in a frame nobody chose.
    """
    if points.crs != polygons.crs:
        raise ValueError(
            f"PIP needs one CRS on both sides: points {points.crs}, "
            f"polygons {polygons.crs}. Reproject deliberately, at the call site."
        )
    predicate = config.geo()["pip"]["predicate"]
    have = points[points.geometry.notna()]
    joined = gpd.sjoin(
        have[[key, have.geometry.name]],
        polygons[[geoid_col, polygons.geometry.name]],
        how="left",
        predicate=predicate,
    )

    # Boundary de-duplication. `intersects` can match a point on a shared edge
    # to both neighbours; the tie-break is the smallest GEOID, which is
    # arbitrary but DETERMINISTIC -- the same point gets the same answer on
    # every run and on every machine, which is what the byte-identity guarantee
    # needs. The count is reported so a large number would be visible.
    dupes = int(joined.duplicated(subset=[key]).sum())
    joined = (
        joined.sort_values([key, geoid_col], kind="stable")
        .drop_duplicates(subset=[key], keep="first")
    )

    out = points[[key]].merge(
        joined[[key, geoid_col]].rename(columns={geoid_col: out_col}),
        on=key, how="left",
    )
    out["_has_geometry"] = points.geometry.notna().to_numpy()
    out["pip_status"] = PIP_MATCHED
    out.loc[~out["_has_geometry"], "pip_status"] = PIP_NO_GEOMETRY
    out.loc[out["_has_geometry"] & out[out_col].isna(), "pip_status"] = PIP_NO_POLYGON
    out = out.drop(columns=["_has_geometry"])

    n_geom = int(points.geometry.notna().sum())
    matched = int((out["pip_status"] == PIP_MATCHED).sum())
    stats = {
        "predicate": predicate,
        "rows": int(len(out)),
        "with_geometry": n_geom,
        "matched": matched,
        "no_polygon": int((out["pip_status"] == PIP_NO_POLYGON).sum()),
        "no_geometry": int((out["pip_status"] == PIP_NO_GEOMETRY).sum()),
        "boundary_ties_broken": dupes,
        "hit_rate": round(matched / n_geom, 6) if n_geom else None,
    }
    return out, stats


def tract_from_bg(bg_geoid: pd.Series) -> pd.Series:
    """Tract GEOID = the first 11 characters of the block-group GEOID.

    STATE(2) + COUNTY(3) + TRACT(6). This is the Census's own construction, not
    a convention we are adopting; `verify_tract_prefix` proves it against the
    TIGER TRACT layer rather than trusting the docstring.
    """
    return bg_geoid.str.slice(0, 11)


def verify_tract_prefix(
    points: gpd.GeoDataFrame,
    tracts: gpd.GeoDataFrame,
    bg_result: pd.DataFrame,
    *,
    key: str,
    sample: int = 5000,
    seed: int = 20260908,
) -> dict[str, Any]:
    """Cross-check `bg_geoid[:11]` against a direct tract PIP on a sample.

    Should agree 100%. A disagreement means TIGER's BG and TRACT layers are not
    consistent with each other for that vintage -- a real finding about the
    reference data, not about this code -- so it is measured and reported with
    examples rather than asserted away.

    The sample is a fixed-seed draw so the number in the report is reproducible.
    """
    matched = bg_result[bg_result["pip_status"] == PIP_MATCHED]
    if matched.empty:
        return {"sampled": 0, "agree": 0, "disagree": 0, "examples": []}
    take = matched.sample(min(sample, len(matched)), random_state=seed)[key]
    subset = points[points[key].isin(set(take))]
    direct, _ = point_in_polygon(subset, tracts, key=key, out_col="tract_direct")
    merged = direct.merge(matched[[key, "bg_geoid"]], on=key, how="inner")
    merged["tract_prefix"] = tract_from_bg(merged["bg_geoid"])
    disagree = merged[
        merged["tract_direct"].notna()
        & (merged["tract_direct"] != merged["tract_prefix"])
    ]
    return {
        "sampled": int(len(merged)),
        "agree": int(len(merged) - len(disagree)),
        "disagree": int(len(disagree)),
        "examples": disagree.head(3)[[key, "tract_direct", "tract_prefix"]]
        .to_dict("records"),
    }


# ---------------------------------------------------------------------------
# ACS
# ---------------------------------------------------------------------------


def _clean_estimate(value: Any) -> int | None:
    """An ACS estimate, with every annotation value turned into NULL.

    -666666666 and friends are not numbers: they are "suppressed", "not
    applicable", "median in the open-ended interval". Reading one as a
    population is the classic silent ACS corruption, and it is silent precisely
    because -666,666,666 people looks like an outlier rather than like a bug.
    """
    if value is None:
        return None
    try:
        n = int(float(value))
    except (TypeError, ValueError):
        return None
    if n in reference.ACS_ANNOTATIONS or n < 0:
        return None
    return n


def read_acs_population(
    store: reference.ReferenceStore, acs: dict[str, Any]
) -> pd.DataFrame:
    """`bg_geoid -> population` from whichever ACS route `reference` used.

    Both routes produce the same estimates for the same vintage; which one ran
    is recorded in the manifest, because "we used the bulk file because no API
    key was configured" is a fact a reviewer reproducing this needs.
    """
    if acs["route"] == "api":
        return _read_acs_api(store, acs)
    return _read_acs_summary_file(store, acs)


def _read_acs_api(store: reference.ReferenceStore, acs: dict[str, Any]) -> pd.DataFrame:
    var = config.geo()["reference"]["acs_variable"]
    rows: list[tuple[str, int | None]] = []
    for rel in acs["paths"].values():
        payload = json.loads(store.require(rel).read_text())
        header, *body = payload
        idx = {name: i for i, name in enumerate(header)}
        for r in body:
            geoid = (r[idx["state"]] + r[idx["county"]] + r[idx["tract"]]
                     + r[idx["block group"]])
            rows.append((geoid, _clean_estimate(r[idx[var]])))
    return _acs_frame(rows)


def _read_acs_summary_file(
    store: reference.ReferenceStore, acs: dict[str, Any]
) -> pd.DataFrame:
    """The Census table-based Summary File: `GEO_ID|B01003_E001|B01003_M001`.

    Block-group rows carry the summary-level prefix `1500000US` followed by the
    12-digit GEOID, so the state filter is a string prefix and the whole
    national file is streamed once rather than parsed into memory.
    """
    path = store.require(acs["paths"]["all"])
    wanted = tuple(
        "1500000US" + f for f in config.geo()["reference"]["state_fips"]
    )
    rows: list[tuple[str, int | None]] = []
    with path.open(encoding="utf-8", errors="replace") as fh:
        header = fh.readline().rstrip("\n").split("|")
        est_col = next(i for i, c in enumerate(header) if c.endswith("E001"))
        for line in fh:
            if not line.startswith(wanted):
                continue
            parts = line.rstrip("\n").split("|")
            rows.append((parts[0].split("US", 1)[1], _clean_estimate(parts[est_col])))
    return _acs_frame(rows)


def _acs_frame(rows: list[tuple[str, int | None]]) -> pd.DataFrame:
    df = pd.DataFrame(rows, columns=["bg_geoid", "population"])
    df = df.drop_duplicates(subset=["bg_geoid"], keep="first")
    df["population"] = df["population"].astype("Int64")
    return df.sort_values("bg_geoid", ignore_index=True)


# ---------------------------------------------------------------------------
# dim_block_group
# ---------------------------------------------------------------------------

DIM_BLOCK_GROUP_COLUMNS = [
    "bg_geoid", "tract_geoid", "county_geoid", "state_fips", "county_fips",
    "aland_m2", "awater_m2", "population", "density_per_km2",
    "intpt_lat", "intpt_lon", "tiger_vintage", "acs_vintage", "acs_variable",
    "acs_route",
]


def build_dim_block_group(
    store: reference.ReferenceStore, acs: dict[str, Any]
) -> tuple[pd.DataFrame, dict[str, Any]]:
    """One row per 2025 TIGER block group in the three states.

    `density_per_km2` divides the ACS population by TIGER's STORED `ALAND`
    converted to km2. No geometry is touched: TIGER computed the land area on
    the authoritative polygon in the authoritative frame, and recomputing it
    here would mean choosing a projection (EPSG:5070, for the record) to get a
    slightly different answer to a question already answered. Water area is
    excluded from the denominator because nobody lives on it and a
    density-per-total-area for a coastal Florida block group is a rate whose
    denominator is mostly ocean.

    Zero-land block groups (there are some -- water-only BGs exist) get a NULL
    density rather than an infinity.
    """
    bgs = load_block_groups(store)
    pop = read_acs_population(store, acs)

    df = pd.DataFrame({
        "bg_geoid": bgs["GEOID"].astype(str),
        "state_fips": bgs["STATEFP"].astype(str),
        "county_fips": bgs["COUNTYFP"].astype(str),
        "aland_m2": bgs["ALAND"].astype("int64"),
        "awater_m2": bgs["AWATER"].astype("int64"),
        "intpt_lat": pd.to_numeric(bgs["INTPTLAT"], errors="coerce"),
        "intpt_lon": pd.to_numeric(bgs["INTPTLON"], errors="coerce"),
    })
    df["tract_geoid"] = tract_from_bg(df["bg_geoid"])
    df["county_geoid"] = df["state_fips"] + df["county_fips"]
    df = df.merge(pop, on="bg_geoid", how="left")

    land_km2 = df["aland_m2"] / 1e6
    df["density_per_km2"] = (
        df["population"].astype("Float64") / land_km2.where(land_km2 > 0)
    ).astype("float64").round(6)

    ref = config.geo()["reference"]
    df["tiger_vintage"] = str(ref["tiger_year"])
    df["acs_vintage"] = f"{ref['acs_year']} {ref['acs_dataset']}"
    df["acs_variable"] = str(ref["acs_variable"])
    df["acs_route"] = str(acs["route"])
    df = df[DIM_BLOCK_GROUP_COLUMNS].sort_values("bg_geoid", ignore_index=True)

    stats = {
        "block_groups": int(len(df)),
        "by_state": {k: int(v) for k, v in
                     df["state_fips"].value_counts().sort_index().items()},
        "population_null": int(df["population"].isna().sum()),
        "population_total": int(df["population"].fillna(0).sum()),
        "zero_land_block_groups": int((df["aland_m2"] == 0).sum()),
        "acs_route": acs["route"],
        "acs_rows_matched": int(df["population"].notna().sum()),
    }
    return df, stats
