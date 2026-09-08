"""The analysis frames every Phase 5 module shares.

One code path, so the three analyses agree on N, period and study area BY
CONSTRUCTION rather than by three modules independently reading the same
parquet and independently getting the filter right. If `lisa.py` and
`hotspots.py` disagreed about whether the 162 Prince George's crashes are in
scope, the "raw versus normalised" contrast they produce would be measuring
that disagreement rather than the normalisation.

What this module decides
------------------------
**The study area is a polygon answer.** `pip_county_geoid = '24031'` -- where
the coordinate actually fell -- not `jurisdiction = 'MD'`, which is where the
report was filed. Montgomery County police file crashes in Prince George's, the
District and Fairfax VA; those rows have no Montgomery population denominator
and would place "hot spots" outside the county. Every exclusion is COUNTED into
an `ExclusionLedger` and lands in the manifest, because "we analysed 72,675 of
125,005 rows" is only defensible if the other 52,330 are itemised.

**The cell universe is the filled county, not the cells that had crashes.**
A hot-spot test run only over cells with at least one crash cannot find a cold
spot and cannot see the zero that makes its neighbour hot -- it conditions on
the outcome. The county polygon is filled with r8 cells in `overlap` mode so
that every cell a Montgomery crash can land in is in the universe, and the
cells the fill adds with zero crashes are real observations of zero.

**Population is apportioned, and the apportionment is mass-preserving.** Block
groups and H3 hexagons do not nest -- neither is a refinement of the other --
so a cell's population is the area-weighted share of every block group it
intersects. The weights come from an intersection computed in **EPSG:5070**
(equal-area: the apportionment is a RATIO OF AREAS and only an equal-area
projection makes that ratio the number the Census would compute), and they are
normalised per block group so that summing a block group's allocations back up
returns its population exactly. `assert_mass_preserved` is called on every
build and is also a test.

The assumption underneath is that population is uniform within a block group.
It is not: a block group that is half Rock Creek Park has all its residents in
the other half, and this apportionment spreads them across the park. The
failure mode is a park cell with a plausible-looking denominator and therefore
an understated per-capita rate. That is why `min_population_for_rate` exists
and why the cells it flags are reported rather than deleted.

CRS discipline
--------------
- Storage and the crash points as read: **EPSG:4326**.
- The H3 fill and the neighbour graph: **no projection**. H3 is defined on the
  sphere and `polygon_to_cells` does its own thing internally; handing it
  projected metres would return cells in the Gulf of Guinea.
- Every area in the apportionment: **EPSG:5070**.
- The projected point set the KDE and ST-DBSCAN consume: **EPSG:26985**
  (NAD83 / Maryland, metres), the same zone Phase 4 snapped in.
- EPSG:3857 appears nowhere in this package. A test asserts it.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass, field
from datetime import date
from pathlib import Path
from typing import Any, Iterable, Sequence

import duckdb
import geopandas as gpd
import h3
import numpy as np
import pandas as pd
import shapely

from .. import config
from ..geo import census_join
from ..transform import common as c

log = logging.getLogger("analysis.frames")

# severity_ordinal is the KABCO scale carried on fact_crash (0 unknown, 1 O,
# 2 C, 3 B, 4 A, 5 K -- see gold.dim_severity). "Injury" is C and above, which
# is the MMUCC definition; 0 is "not reported", NOT "no injury", so it never
# counts as an injury and never counts as a non-injury either.
INJURY_MIN_ORDINAL = 2
FATAL_ORDINAL = 5


# ---------------------------------------------------------------------------
# the study area
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class StudyArea:
    """The bounds every frame in this phase is built inside.

    Frozen and passed explicitly rather than re-read from config in each
    module: the sensitivity run is the same code with a different period, and
    a module that reaches back into config cannot be given one.
    """

    county_geoid: str
    label: str
    source_systems: tuple[str, ...]
    period_start: date
    period_end: date
    resolution: int
    neighbor_k: int
    min_population_for_rate: int
    rate_per: int

    @property
    def period(self) -> str:
        return f"{self.period_start.isoformat()}:{self.period_end.isoformat()}"

    @property
    def name(self) -> str:
        return f"county={self.county_geoid}"


def _as_date(value: Any) -> date:
    if isinstance(value, date):
        return value
    return date.fromisoformat(str(value)[:10])


def study_area_from_config(
    *, period: str | None = None, sensitivity: bool = False
) -> StudyArea:
    """`config/geo.toml [analysis]` -> a StudyArea.

    `period` is the CLI's `--period 2019-01-01:2025-12-31` override; passing
    it is how the sensitivity run and the tests get a different window without
    a second config file.
    """
    cfg = config.geo()["analysis"]
    if period:
        start_s, _, end_s = period.partition(":")
        start, end = _as_date(start_s), _as_date(end_s)
    elif sensitivity:
        start = _as_date(cfg["sensitivity_period_start"])
        end = _as_date(cfg["sensitivity_period_end"])
    else:
        start, end = _as_date(cfg["period_start"]), _as_date(cfg["period_end"])
    if end < start:
        raise ValueError(f"period end {end} precedes start {start}")
    return StudyArea(
        county_geoid=str(cfg["study_area"]),
        label=str(cfg["study_area_label"]),
        source_systems=tuple(str(s) for s in cfg["source_systems"]),
        period_start=start,
        period_end=end,
        resolution=int(cfg["h3_resolution"]),
        neighbor_k=int(cfg["neighbor_k"]),
        min_population_for_rate=int(cfg["min_population_for_rate"]),
        rate_per=int(cfg["rate_per"]),
    )


# ---------------------------------------------------------------------------
# the crash corpus, with every drop counted
# ---------------------------------------------------------------------------


@dataclass
class ExclusionLedger:
    """Why each row that is not in the corpus is not in the corpus.

    Ordered, because the reasons are applied in order and a row is attributed
    to the FIRST one that excludes it -- otherwise the counts overlap and do
    not sum. `total_in` minus the sum of `reasons` is exactly `kept`, and
    `check()` asserts it.
    """

    total_in: int = 0
    kept: int = 0
    reasons: dict[str, int] = field(default_factory=dict)
    detail: dict[str, Any] = field(default_factory=dict)

    def drop(self, reason: str, n: int) -> None:
        if n:
            self.reasons[reason] = self.reasons.get(reason, 0) + int(n)

    def check(self) -> None:
        dropped = sum(self.reasons.values())
        if self.total_in - dropped != self.kept:
            raise AssertionError(
                f"exclusion ledger does not balance: {self.total_in} in, "
                f"{dropped} dropped, {self.kept} kept "
                f"(off by {self.total_in - dropped - self.kept})"
            )

    def as_dict(self) -> dict[str, Any]:
        return {
            "rows_considered": self.total_in,
            "rows_kept": self.kept,
            "excluded_total": sum(self.reasons.values()),
            "excluded_by_reason": dict(sorted(self.reasons.items())),
            **({"detail": self.detail} if self.detail else {}),
        }


CRASH_COLUMNS = [
    "crash_sk", "primary_source_system", "crash_date", "crash_datetime_local",
    "crash_datetime_utc", "pip_county_geoid", "bg_geoid", "h3_r8", "h3_r7",
    "longitude", "latitude", "severity_ordinal", "osm_name", "osm_highway",
    "osm_way_id", "offset_m",
]


def load_crashes(
    con: duckdb.DuckDBPyConnection, gold_root: Path, area: StudyArea
) -> tuple[pd.DataFrame, ExclusionLedger]:
    """The study corpus: one row per crash, plus the ledger of what was dropped.

    Reads `crash_geo` (Phase 4's enrichment) joined to `fact_crash` for
    `severity_ordinal`, which lives on the fact and deliberately was not copied
    into the enrichment. Coordinates come out of the WKB geometry via DuckDB
    spatial rather than from `fact_crash.latitude/longitude`, so that the point
    the analysis uses is the same point the point-in-polygon used -- a corpus
    whose PIP says Montgomery and whose coordinate says something else is a
    corpus with a silent join bug in it.

    No projection here. `ST_X`/`ST_Y` on EPSG:4326 WKB returns degrees, which
    is what H3 wants and what `projected_points` reprojects from.
    """
    cg = gold_root / "crash_geo.parquet"
    fc = gold_root / "fact_crash.parquet"
    for p in (cg, fc):
        if not p.exists():
            raise FileNotFoundError(
                f"no {p.name} at {p} -- build it with "
                "`python -m src.transform.model` then `python -m src.geo.build`"
            )
    cg_s, fc_s = str(cg).replace("'", "''"), str(fc).replace("'", "''")
    sources = ", ".join("'" + s.replace("'", "''") + "'" for s in area.source_systems)

    # crash_geo is GeoParquet 1.1.0, and DuckDB's spatial extension decodes
    # its `geo` metadata into a native GEOMETRY column -- but a plain parquet
    # reader (or a build written before the metadata existed) hands back the
    # raw WKB as a BLOB. Both are legitimate; which one arrives depends on the
    # extension being loaded, so the geometry accessor is chosen from the
    # column's actual type rather than assumed. Guessing wrong is not a subtle
    # failure -- it is a BinderException -- but it is one that would only fire
    # on someone else's machine.
    geom_type = con.execute(
        f"SELECT typeof(geometry) FROM read_parquet('{cg_s}') LIMIT 1"
    ).fetchone()[0]
    geom = "g.geometry" if str(geom_type).upper().startswith("GEOMETRY") \
        else "ST_GeomFromWKB(g.geometry)"

    frame = con.execute(f"""
        SELECT g.crash_sk, g.primary_source_system, g.crash_date,
               g.crash_datetime_local, g.crash_datetime_utc,
               g.pip_county_geoid, g.bg_geoid, g.h3_r8, g.h3_r7,
               ST_X({geom}) AS longitude,
               ST_Y({geom}) AS latitude,
               f.severity_ordinal,
               g.osm_name, g.osm_highway, g.osm_way_id, g.offset_m
        FROM read_parquet('{cg_s}') g
        JOIN read_parquet('{fc_s}') f USING (crash_sk)
        WHERE g.primary_source_system IN ({sources})
        ORDER BY g.crash_sk
    """).df()

    ledger = ExclusionLedger(total_in=int(len(frame)))

    # Order matters and is reported: a row is attributed to the first reason
    # that excludes it, so the counts sum to the total rather than overlapping.
    no_geom = frame["h3_r8"].isna() | frame["longitude"].isna()
    ledger.drop("no_usable_coordinate", int(no_geom.sum()))
    frame = frame[~no_geom]

    outside = frame["pip_county_geoid"].fillna("") != area.county_geoid
    outside_counts = (
        frame.loc[outside, "pip_county_geoid"].fillna("NO_POLYGON").value_counts()
    )
    ledger.drop("outside_study_area_county", int(outside.sum()))
    ledger.detail["outside_by_county_geoid"] = {
        str(k): int(v) for k, v in outside_counts.items()
    }
    frame = frame[~outside]

    # DuckDB's DATE arrives through `.df()` as datetime64[us], so the period
    # bounds are lifted to Timestamps rather than compared as `date` objects
    # (pandas refuses that comparison outright rather than coercing, which is
    # the good outcome -- it is why this is explicit).
    in_period = (
        frame["crash_date"] >= pd.Timestamp(area.period_start)
    ) & (frame["crash_date"] <= pd.Timestamp(area.period_end))
    ledger.drop("outside_period", int((~in_period).sum()))
    frame = frame[in_period]

    ledger.kept = int(len(frame))
    ledger.check()
    log.info(
        "study corpus: %d crashes in %s over %s (from %d source rows)",
        ledger.kept, area.name, area.period, ledger.total_in,
    )
    return frame.reset_index(drop=True), ledger


def crashes_by_year(frame: pd.DataFrame) -> pd.DataFrame:
    """Per-year counts -- the first figure, and the reason the period is what it is."""
    year = pd.Index(frame["crash_date"]).map(lambda d: d.year)
    out = (
        frame.assign(year=year)
        .groupby("year", as_index=False)
        .agg(
            n_crashes=("crash_sk", "size"),
            n_injury=("severity_ordinal",
                      lambda s: int((s >= INJURY_MIN_ORDINAL).sum())),
            n_fatal=("severity_ordinal", lambda s: int((s == FATAL_ORDINAL).sum())),
        )
    )
    return out.sort_values("year", ignore_index=True)


# ---------------------------------------------------------------------------
# the cell universe: fill the county, do not take the cells that had crashes
# ---------------------------------------------------------------------------


def county_polygon(
    counties: gpd.GeoDataFrame, county_geoid: str
) -> shapely.Geometry:
    """The study-area polygon, in EPSG:4326 as TIGER was loaded.

    No projection: this polygon is used for an H3 fill (spherical) and for a
    topological `contains` test. The only metric use of county geometry in this
    phase is the apportionment, which reprojects explicitly.
    """
    match = counties[counties["GEOID"] == county_geoid]
    if match.empty:
        raise KeyError(
            f"county {county_geoid} not in the TIGER COUNTY layer "
            f"({len(counties)} polygons) -- check config [analysis] study_area"
        )
    return match.geometry.iloc[0]


def fill_cells(
    polygon: shapely.Geometry, resolution: int
) -> tuple[list[str], list[str], dict[str, Any]]:
    """Every r8 cell that OVERLAPS the polygon, the wholly-inside subset, and stats.

    `contain='overlap'` rather than the default `'center'`, and the difference
    is not cosmetic. Centre-containment drops every cell whose centroid is
    outside the county but whose area is partly inside -- and a crash on the
    county line lands in exactly such a cell. Its cell would then be missing
    from the weights matrix, so the crash would either vanish from the corpus
    or appear as an unmodelled island. Overlap-containment makes the universe a
    superset of "cells a study-area crash can be in", which is the property the
    rest of the phase relies on.

    The cost is a fringe of cells that are mostly outside the county and are
    therefore under-counted for crashes but fully counted for population. They
    are flagged `is_edge_cell` and the report says what that does to the edge.

    Spherical, no CRS. h3 takes WGS84 degrees and projects internally.
    """
    shape = h3.geo_to_h3shape(polygon)
    cells = sorted(h3.polygon_to_cells_experimental(shape, resolution, contain="overlap"))
    interior = sorted(
        h3.polygon_to_cells_experimental(shape, resolution, contain="full")
    )
    stats = {
        "resolution": resolution,
        "cells_overlap": len(cells),
        "cells_fully_inside": len(interior),
        "cells_on_boundary": len(cells) - len(interior),
        "contain_mode": "overlap",
    }
    return cells, interior, stats


def cell_polygons(cells: Sequence[str]) -> gpd.GeoDataFrame:
    """Cell id -> its boundary polygon, EPSG:4326.

    `cell_to_boundary` returns (lat, lng) pairs; shapely wants (x, y) = (lng,
    lat). Getting that backwards produces a polygon in the Indian Ocean that
    intersects nothing, which is a silent zero rather than an error -- so the
    swap is explicit and the build asserts the resulting bounds are sane.
    """
    geoms = [
        shapely.Polygon([(lng, lat) for lat, lng in h3.cell_to_boundary(cell)])
        for cell in cells
    ]
    gdf = gpd.GeoDataFrame({"h3": list(cells)}, geometry=geoms, crs="EPSG:4326")
    minx, miny, maxx, maxy = gdf.total_bounds
    if not (-180 <= minx <= maxx <= 180 and -90 <= miny <= maxy <= 90):
        raise AssertionError(f"cell polygons have impossible bounds {gdf.total_bounds}")
    return gdf


# ---------------------------------------------------------------------------
# population apportionment: block groups do not nest in hexagons
# ---------------------------------------------------------------------------


def apportion_population(
    cells: Sequence[str],
    block_groups: gpd.GeoDataFrame,
    populations: pd.DataFrame,
    *,
    crs_epsg: int | None = None,
) -> tuple[pd.DataFrame, dict[str, Any]]:
    """Area-weight block-group population onto H3 cells.

    Returns one row per cell with `population` and `land_area_km2`, plus the
    allocation stats the mass-preservation test reads.

    Method. Intersect every (block group, cell) pair; the weight of cell *c*
    in block group *b* is

        w(b, c) = area(b n c) / SUM_c' area(b n c')

    normalised over the CELLS THIS BUILD HAS rather than over the block
    group's own polygon area. The two are the same number when the cell
    universe covers the block group -- which the overlap fill guarantees --
    and normalising by the observed denominator makes the allocation exactly
    mass-preserving even when a sliver of a boundary block group falls outside
    the fill, instead of quietly losing those people.

    CRS. Every area on both sides of that ratio is computed in **EPSG:5070**
    (NAD83 / CONUS Albers, equal-area). An equal-area projection is not
    optional here: the claim being made is that this cell holds this SHARE of
    that block group, and a conformal projection distorts area systematically
    with latitude, so the shares would not be the shares. (At Montgomery's
    ~30 km north-south extent 26985 would give nearly the same answer; 5070 is
    the projection ASSIGNMENT.md names for an area and it is the one whose
    correctness does not depend on the study area being small.)

    Land weighting. `land_area_km2` scales each intersection by the block
    group's TIGER `ALAND / (ALAND + AWATER)`. That is a scalar per block group,
    so it cancels out of `w(b, c)` entirely and affects only the reported area
    -- population weights are unchanged by it. It is applied anyway because
    `rate_per_km2` should be per square kilometre of land, and the Potomac is
    a third of some of these cells.
    """
    epsg = int(crs_epsg if crs_epsg is not None
               else config.geo()["analysis"]["crs"]["apportionment"])

    cells_gdf = cell_polygons(cells)
    # EPSG:5070 -- equal-area. Both frames, one call each, so the intersection
    # and both areas are in the same equal-area frame.
    cells_m = cells_gdf.to_crs(epsg=epsg)
    bg = block_groups.copy()
    bg["bg_geoid"] = bg["GEOID"].astype("string")
    bg_m = bg.to_crs(epsg=epsg)

    pop = populations.set_index("bg_geoid")
    bg_m = bg_m[bg_m["bg_geoid"].isin(pop.index)]

    inter = gpd.overlay(
        bg_m[["bg_geoid", "geometry"]], cells_m[["h3", "geometry"]],
        how="intersection", keep_geom_type=True,
    )
    if inter.empty:
        raise AssertionError(
            "no block group intersects any study cell -- the two frames are "
            "probably in different CRSs or the county filter is wrong"
        )
    inter["area_m2"] = inter.geometry.area  # EPSG:5070 metres squared

    denom = inter.groupby("bg_geoid")["area_m2"].transform("sum")
    inter["share"] = np.where(denom > 0, inter["area_m2"] / denom, 0.0)

    aland = pop["aland_m2"].astype("float64")
    awater = pop["awater_m2"].astype("float64")
    total_tiger = (aland + awater).replace(0.0, np.nan)
    land_share = (aland / total_tiger).fillna(1.0)

    inter["population"] = inter["share"] * inter["bg_geoid"].map(
        pop["population"].astype("float64")
    )
    # Land-weighted area. `land_share` is constant within a block group, so it
    # cancels out of `share` above and changes only this column.
    inter["land_area_km2"] = (
        inter["area_m2"] * inter["bg_geoid"].map(land_share) / 1e6
    )

    out = (
        inter.groupby("h3", as_index=False)
        .agg(population=("population", "sum"),
             land_area_km2=("land_area_km2", "sum"),
             n_block_groups=("bg_geoid", "nunique"))
        .rename(columns={"h3": "h3_cell"})
    )
    # Cells the fill produced that intersect no block group at all: entirely
    # outside the census geography (over the Potomac, or across a state line
    # in the boundary fringe). Zero population is the right answer and it must
    # be an explicit row, not a missing one.
    missing = sorted(set(cells) - set(out["h3_cell"]))
    if missing:
        out = pd.concat([out, pd.DataFrame({
            "h3_cell": missing, "population": 0.0,
            "land_area_km2": 0.0, "n_block_groups": 0,
        })], ignore_index=True)

    stats = {
        "crs_epsg": epsg,
        "cells": len(out),
        "block_groups": int(inter["bg_geoid"].nunique()),
        "intersection_pairs": int(len(inter)),
        "cells_with_no_block_group": len(missing),
        "population_allocated": float(out["population"].sum()),
        "population_source_total": float(
            pop.loc[sorted(inter["bg_geoid"].unique()), "population"].sum()
        ),
        "land_area_km2_total": float(out["land_area_km2"].sum()),
    }
    return out.sort_values("h3_cell", ignore_index=True), stats


def assert_mass_preserved(
    cells: Sequence[str],
    block_groups: gpd.GeoDataFrame,
    populations: pd.DataFrame,
    *,
    crs_epsg: int | None = None,
    tolerance: float = 1e-6,
) -> dict[str, Any]:
    """Every block group's population is fully allocated, to within rounding.

    A relative tolerance, not an absolute one: the assertion is that no PERSON
    is created or destroyed by the apportionment, and 57 million people at
    float64 leaves plenty of room under 1e-6.
    """
    allocated, stats = apportion_population(
        cells, block_groups, populations, crs_epsg=crs_epsg
    )
    total_in = stats["population_source_total"]
    total_out = stats["population_allocated"]
    rel = abs(total_out - total_in) / total_in if total_in else 0.0
    if rel > tolerance:
        raise AssertionError(
            f"apportionment lost mass: {total_in:.3f} people in the "
            f"{stats['block_groups']} intersecting block groups, "
            f"{total_out:.3f} allocated to {stats['cells']} cells "
            f"(relative error {rel:.3e} > {tolerance:.0e})"
        )
    return {**stats, "relative_error": rel}


# ---------------------------------------------------------------------------
# the cell statistics table
# ---------------------------------------------------------------------------

CELL_STATS_COLUMNS = [
    "h3_r8", "study_area", "period_start", "period_end",
    "n_crashes", "n_injury", "n_fatal",
    "population", "land_area_km2", "rate_per_1k_pop", "rate_per_km2",
    "rate_is_defined", "n_neighbors", "is_edge_cell", "is_land",
    "_analysis_build_sha", "_geo_build_sha",
]


def cell_stats(
    crashes: pd.DataFrame,
    cells: Sequence[str],
    allocated: pd.DataFrame,
    neighbors: dict[str, list[str]],
    area: StudyArea,
    *,
    interior_cells: Iterable[str] = (),
    analysis_build_sha: str = "",
    geo_build_sha: str = "",
) -> tuple[pd.DataFrame, dict[str, Any]]:
    """One row per cell in the filled county. Zero-crash cells included.

    `rate_is_defined` is the honest column. A cell whose apportioned
    population is below `min_population_for_rate` gets NULL for
    `rate_per_1k_pop`, never `inf` and never a rate over three people: an
    interchange with 40 crashes and 6 residents is not a per-capita hot spot,
    it is a place with no residents. Those cells stay in the raw-count analysis
    -- they are exactly what the raw-versus-normalised contrast is about -- and
    are dropped from the normalised one, with the count reported.

    `rate_per_km2` has no such floor: land area is never zero for a cell that
    intersects any block group, and a density over land is defined wherever
    there is land.
    """
    key = f"h3_r{area.resolution}"
    counts = (
        crashes.groupby(key)
        .agg(
            n_crashes=("crash_sk", "size"),
            n_injury=("severity_ordinal",
                      lambda s: int((s >= INJURY_MIN_ORDINAL).sum())),
            n_fatal=("severity_ordinal", lambda s: int((s == FATAL_ORDINAL).sum())),
        )
    )

    interior = set(interior_cells)
    df = pd.DataFrame({"h3_r8": list(cells)})
    df = df.join(counts.reindex(df["h3_r8"]).reset_index(drop=True))
    for col in ("n_crashes", "n_injury", "n_fatal"):
        df[col] = df[col].fillna(0).astype("int64")

    alloc = allocated.set_index("h3_cell")
    df["population"] = (
        df["h3_r8"].map(alloc["population"]).fillna(0.0).astype("float64")
    )
    df["land_area_km2"] = (
        df["h3_r8"].map(alloc["land_area_km2"]).fillna(0.0).astype("float64")
    )

    defined = df["population"] >= area.min_population_for_rate
    df["rate_is_defined"] = defined
    df["rate_per_1k_pop"] = np.where(
        defined, df["n_crashes"] / df["population"].where(defined, 1.0) * area.rate_per,
        np.nan,
    )
    has_land = df["land_area_km2"] > 0
    df["rate_per_km2"] = np.where(
        has_land, df["n_crashes"] / df["land_area_km2"].where(has_land, 1.0), np.nan
    )

    df["n_neighbors"] = df["h3_r8"].map(lambda cid: len(neighbors.get(cid, []))).astype("int64")
    # Two different edges, and they are not the same cell set. `is_edge_cell`
    # is geometric -- the cell is not wholly inside the county, so its crash
    # count is truncated by the county line while its population is not.
    # `n_neighbors < 6` is graph-theoretic -- the cell has fewer than the six
    # neighbours a hexagon has, which at the fill boundary is the same thing
    # and in the interior never happens.
    df["is_edge_cell"] = ~df["h3_r8"].isin(interior)
    df["is_land"] = has_land

    df["study_area"] = area.county_geoid
    df["period_start"] = area.period_start
    df["period_end"] = area.period_end
    df["_analysis_build_sha"] = analysis_build_sha
    df["_geo_build_sha"] = geo_build_sha

    df = df[CELL_STATS_COLUMNS].sort_values("h3_r8", ignore_index=True)

    unplaced = set(crashes[key].dropna()) - set(cells)
    stats = {
        "cells": int(len(df)),
        "cells_with_crashes": int((df["n_crashes"] > 0).sum()),
        "cells_zero_crashes": int((df["n_crashes"] == 0).sum()),
        "cells_edge": int(df["is_edge_cell"].sum()),
        "cells_rate_undefined": int((~df["rate_is_defined"]).sum()),
        "cells_rate_undefined_with_crashes": int(
            (~df["rate_is_defined"] & (df["n_crashes"] > 0)).sum()
        ),
        "crashes_in_cells": int(df["n_crashes"].sum()),
        "crashes_not_in_any_study_cell": len(unplaced),
        "population_total": float(df["population"].sum()),
        "min_population_for_rate": area.min_population_for_rate,
    }
    # The fill is a superset of the cells a study-area crash can be in, so
    # this is zero by construction. Asserted rather than assumed: a non-zero
    # here means the fill mode regressed to 'center' and the corpus and the
    # cell universe have silently stopped describing the same thing.
    if unplaced:
        raise AssertionError(
            f"{len(unplaced)} crash cell(s) are outside the filled county "
            f"universe, e.g. {sorted(unplaced)[:3]} -- the H3 fill mode is wrong"
        )
    return df, stats


# ---------------------------------------------------------------------------
# the projected point set
# ---------------------------------------------------------------------------


def projected_points(
    crashes: pd.DataFrame, *, crs_epsg: int | None = None
) -> gpd.GeoDataFrame:
    """Crash points in the local metric CRS, for KDE and ST-DBSCAN.

    **EPSG:26985** (NAD83 / Maryland, metres) by config. Every bandwidth, grid
    pitch and clustering epsilon downstream is a number of metres in THIS
    frame; 3857 would inflate each of them by 1/cos(39.1 deg) ~ 1.29, so a
    "500 m" bandwidth would be a 387 m one on the ground. Maryland is a single
    state-plane zone, so one code is correct county-wide -- which is why this
    function refuses to be handed a corpus outside the study area rather than
    silently projecting Texas into Maryland's zone.
    """
    epsg = int(crs_epsg if crs_epsg is not None
               else config.geo()["analysis"]["crs"]["local"])
    gdf = gpd.GeoDataFrame(
        crashes.copy(),
        geometry=gpd.points_from_xy(crashes["longitude"], crashes["latitude"]),
        crs="EPSG:4326",  # storage CRS: degrees, as read
    )
    # EPSG:4326 -> EPSG:26985. The only reprojection of the point set, and
    # everything metric downstream happens on its output.
    out = gdf.to_crs(epsg=epsg)
    out["x_m"] = out.geometry.x
    out["y_m"] = out.geometry.y
    return out


def connect(threads: int | None = None) -> duckdb.DuckDBPyConnection:
    """A DuckDB connection with the spatial extension, from the Phase 2 helper."""
    con = c.connect(threads)
    con.execute("INSTALL spatial; LOAD spatial;")
    return con


def load_block_group_population(
    con: duckdb.DuckDBPyConnection, gold_root: Path, county_geoid: str
) -> pd.DataFrame:
    """`dim_block_group` rows for one county: population and TIGER's areas.

    Population is `B01003_001E`, ACS 2023 5-year, and it is the ONLY ACS
    variable this phase touches. That is not a scope note, it is the design:
    income, tenure, vehicles and commute are the protected-class proxies
    ASSIGNMENT.md Part 4 warns about, and population is different in kind --
    it is a DENOMINATOR that turns a count into a rate in aggregate, and it
    never becomes an attribute of a person or a lead.
    """
    path = gold_root / "dim_block_group.parquet"
    if not path.exists():
        raise FileNotFoundError(f"no dim_block_group at {path}")
    p = str(path).replace("'", "''")
    return con.execute(f"""
        SELECT bg_geoid, county_geoid, population, aland_m2, awater_m2
        FROM read_parquet('{p}')
        WHERE county_geoid = '{county_geoid.replace("'", "''")}'
          AND population IS NOT NULL
        ORDER BY bg_geoid
    """).df()


def load_county_layer(store: Any) -> gpd.GeoDataFrame:
    """The national TIGER COUNTY layer, via Phase 4's cached loader."""
    return census_join.load_counties(store)


def load_block_group_layer(store: Any, state_fips: str) -> gpd.GeoDataFrame:
    """TIGER block groups for one state, via Phase 4's cached loader."""
    return census_join.load_block_groups(store, states=[state_fips])
