"""Nearest OSM road segment, snap distance as a quality attribute, linear referencing.

    crash point --(nearest segment, projected CRS)--> way
                --(line.project)-----------------> offset along that way

Two things make this enrichment rather than fiction.

**The distance is a column, not a filter.** `snap_distance_m` is stored for
every attempted row, including the rejected ones, so a consumer can see that a
crash was 118 m from the nearest carriageway and decide for itself. The
assignment's line -- "a crash snapped 400 m to a road is not enrichment, it is
fiction" -- is an outer bound, not a threshold; the threshold in
`config/geo.toml [snap] max_distance_m` is set from the MEASURED distribution
(p50/p90/p95/p99 are in the Phase 4 report) and is a config edit, not a code
change.

**The linear reference is what makes corridor analysis possible.** A point
snapped to a way is still a point; `offset_m` along that way, with the way's
length, is what lets Phase 5 ask "which 800 m of Georgia Avenue" instead of
"which cell". `offset_frac = offset_m / segment_length_m` is the same number
normalised, so segments of different lengths are comparable.


CRS: EPSG:26985 for Maryland, and why that is not optional
----------------------------------------------------------
Everything in this module is metric: a nearest-neighbour distance, a segment
length, a distance along a line. All three are computed in **NAD83 / Maryland
(EPSG:26985)**, a state-plane zone in metres whose scale error across Montgomery
County is under 1 part in 10,000 -- centimetres over the tens of metres that
matter here.

In EPSG:3857 the same operations would be wrong by 1/cos(latitude): at 39.1 deg N
that is a factor of 1.288, so a 50 m threshold would actually admit everything
within 38.8 m on the ground and reject real snaps between 38.8 m and 50 m. That
is not a rounding difference, it is a different answer about which crashes
belong to which road. The negative-control test in `tests/test_geo.py` measures
the factor rather than asserting it in prose.

Texas, if a county is ever snapped, uses the state-plane zone that county sits
in (El Paso -> EPSG:32139, Texas Central) from `config/geo.toml [crs.snap]`, not
a statewide CRS: state-plane is only honest within a few hundred km of its
central meridian, and one county is exactly the case it was designed for.
`snap_crs_epsg` is stored per row so the CRS a measurement was made in travels
with the measurement.


Reading the PBF without a new dependency
----------------------------------------
DuckDB's spatial extension ships `ST_ReadOSM`, which streams a `.osm.pbf` as
(kind, id, tags, refs, lat, lon). Ways with a `highway` tag are selected, their
node references unnested in order, joined to node coordinates and assembled with
`ST_MakeLine`. `pyrosm` is not installed and `osmnx` does not read PBF, so the
alternative was `pyosmium` -- a new dependency to do what the database already
does. The extracted network is cached as GeoParquet under `data/reference/`,
keyed by the PBF's sha256, so a new extract invalidates it by construction.

Footways, cycleways, paths and steps are excluded (`[snap] highway_values`).
Snapping a car crash to a footpath 8 m away is a worse answer than snapping it
to the carriageway 30 m away, and it would make the `maxspeed`/`lanes` null
rates an artefact of the filter instead of a fact about OSM tagging.
"""

from __future__ import annotations

import logging
import re
from pathlib import Path
from typing import Any, Iterable, Sequence

import duckdb
import geopandas as gpd
import numpy as np
import pandas as pd
import shapely

from .. import config
from ..transform import common as c
from . import reference

log = logging.getLogger("geo.snap")

SNAP_SNAPPED = "SNAPPED"
SNAP_REJECTED = "REJECTED_DISTANCE"
SNAP_NOT_ATTEMPTED = "NOT_ATTEMPTED"
SNAP_NO_GEOMETRY = "NO_GEOMETRY"
SNAP_STATUS_VALUES = (SNAP_SNAPPED, SNAP_REJECTED, SNAP_NOT_ATTEMPTED, SNAP_NO_GEOMETRY)

OSM_ATTRIBUTES = ["osm_highway", "osm_maxspeed", "osm_maxspeed_mph",
                  "osm_lanes", "osm_name", "osm_ref"]
SNAP_COLUMNS = [
    "snap_status", "osm_way_id", "snap_distance_m", "segment_length_m",
    "offset_m", "offset_frac", *OSM_ATTRIBUTES, "snap_crs_epsg",
]

# "35 mph", "35mph", "35" (OSM's implicit unit is km/h), "40 mph;30 mph".
_MAXSPEED = re.compile(r"^\s*(\d+(?:\.\d+)?)\s*(mph|km/?h)?\s*$", re.IGNORECASE)


# ---------------------------------------------------------------------------
# PBF -> road network
# ---------------------------------------------------------------------------


def clip_bbox(envelope_name: str = "montgomery") -> tuple[float, float, float, float]:
    """(min_lon, min_lat, max_lon, max_lat) for the OSM clip, in EPSG:4326.

    The jurisdiction envelope padded a further `[snap] clip_pad_deg`, so a
    crash near the county line can still snap to a road that continues into
    Howard or Frederick. Clipping to the county exactly would create a rim of
    artificially long snap distances that is an artefact of the clip.
    """
    env = config.envelope(envelope_name)
    pad = float(config.geo()["snap"]["clip_pad_deg"])
    return (env["min_lon"] - pad, env["min_lat"] - pad,
            env["max_lon"] + pad, env["max_lat"] + pad)


def road_network_path(store: reference.ReferenceStore, state: str,
                      bbox: tuple[float, float, float, float]) -> Path:
    """Cache path keyed by the PBF hash, so a new extract cannot be reused.

    "maryland-latest.osm.pbf" is not a version (see `reference.py`); the hash
    is. Putting it in the file name means a changed PBF produces a cache MISS
    rather than a stale hit, which is the restatement behaviour we want without
    any invalidation logic.
    """
    entry = store.entry(reference.osm_relpath(state)) or {}
    sha = str(entry.get("sha256", "nohash"))[:16]
    box = "_".join(f"{v:.2f}" for v in bbox)
    return store.parsed(f"osm_roads_{state}_{sha}_{box}.parquet")


def build_road_network(
    store: reference.ReferenceStore,
    *,
    state: str = "maryland",
    bbox: tuple[float, float, float, float] | None = None,
    con: duckdb.DuckDBPyConnection | None = None,
) -> gpd.GeoDataFrame:
    """Ways with a `highway` tag, clipped to `bbox`, as a cached GeoParquet.

    Two passes over the PBF rather than one: nodes are filtered to the bbox as
    they stream, which keeps the node table at a few hundred thousand rows
    instead of the ~10 million in the Maryland extract. The cost is reading the
    file twice; the alternative is holding every node in memory to discover
    that 95% of them are in Baltimore.
    """
    bbox = bbox or clip_bbox()
    dest = road_network_path(store, state, bbox)
    if dest.exists():
        return gpd.read_parquet(dest)

    pbf = store.require(reference.osm_relpath(state))
    own = con is None
    con = con or duckdb.connect()
    con.execute("INSTALL spatial; LOAD spatial;")
    path = str(pbf).replace("'", "''")
    min_lon, min_lat, max_lon, max_lat = bbox
    highways = ", ".join(f"'{h}'" for h in config.geo()["snap"]["highway_values"])

    log.info("reading ways from %s", pbf.name)
    con.execute(f"""
        CREATE OR REPLACE TABLE osm_way AS
        SELECT id, refs,
               map_extract(tags, 'highway')[1]  AS highway,
               map_extract(tags, 'maxspeed')[1] AS maxspeed,
               map_extract(tags, 'lanes')[1]    AS lanes,
               map_extract(tags, 'name')[1]     AS name,
               map_extract(tags, 'ref')[1]      AS ref
        FROM ST_ReadOSM('{path}')
        WHERE kind = 'way'
          AND map_extract(tags, 'highway')[1] IN ({highways})
          AND len(refs) >= 2
    """)
    log.info("reading nodes in the clip box")
    con.execute(f"""
        CREATE OR REPLACE TABLE osm_node AS
        SELECT id, lat, lon FROM ST_ReadOSM('{path}')
        WHERE kind = 'node'
          AND lat BETWEEN {min_lat} AND {max_lat}
          AND lon BETWEEN {min_lon} AND {max_lon}
    """)

    # Two parallel UNNESTs over lists of the same length zip elementwise in
    # DuckDB, which is how the node order (and therefore the direction of the
    # line, and therefore `offset_m`) is preserved.
    con.execute("""
        CREATE OR REPLACE TABLE way_node AS
        SELECT id AS way_id,
               unnest(refs) AS node_id,
               unnest(range(1, len(refs) + 1)) AS seq
        FROM osm_way
    """)
    # A way is kept only if EVERY one of its nodes is inside the clip box.
    # A partially-clipped way would be assembled from the vertices that
    # survived, which silently straightens the missing part -- and a straight
    # line where a curve was is a wrong `offset_m` for every crash on it.
    con.execute("""
        CREATE OR REPLACE TABLE road AS
        SELECT w.way_id,
               ST_MakeLine(list(ST_Point(n.lon, n.lat) ORDER BY w.seq)) AS geom,
               COUNT(*) AS n_nodes
        FROM way_node w
        JOIN osm_node n ON n.id = w.node_id
        GROUP BY w.way_id
        HAVING COUNT(*) >= 2
           AND COUNT(*) = (SELECT len(refs) FROM osm_way o WHERE o.id = w.way_id)
    """)
    rows = con.execute("""
        SELECT r.way_id AS osm_way_id, w.highway AS osm_highway,
               w.maxspeed AS osm_maxspeed, w.lanes AS osm_lanes,
               w.name AS osm_name, w.ref AS osm_ref,
               ST_AsWKB(r.geom) AS wkb
        FROM road r JOIN osm_way w ON w.id = r.way_id
        ORDER BY r.way_id
    """).fetch_df()
    if own:
        con.close()

    gdf = gpd.GeoDataFrame(
        rows.drop(columns=["wkb"]),
        # DuckDB hands back bytearray; shapely wants bytes.
        geometry=shapely.from_wkb([bytes(b) for b in rows["wkb"]]),
        # ST_Point(lon, lat) on OSM coordinates: EPSG:4326, the storage CRS.
        crs=config.geo()["crs"]["storage"],
    )
    gdf["osm_way_id"] = gdf["osm_way_id"].astype("int64")
    gdf["osm_maxspeed_mph"] = normalise_maxspeed(gdf["osm_maxspeed"])
    gdf = gdf.sort_values("osm_way_id", ignore_index=True)
    gdf.to_parquet(dest, index=False)
    log.info("road network: %d ways -> %s", len(gdf), dest.name)
    return gdf


def normalise_maxspeed(raw: pd.Series) -> pd.Series:
    """OSM `maxspeed` to mph, keeping the raw string in its own column.

    OSM's implicit unit is km/h; US tagging almost always writes "35 mph"
    explicitly. Anything that is not a bare number or a number with a unit
    (`walk`, `signals`, `RU:urban`, "40 mph;30 mph") becomes NULL rather than a
    guess -- the null rate is a reported fact about OSM tagging, and imputing
    it would be inventing a speed limit for a road nobody surveyed.
    """
    def one(value: Any) -> float | None:
        if not isinstance(value, str):
            return None
        m = _MAXSPEED.match(value)
        if not m:
            return None
        n = float(m.group(1))
        unit = (m.group(2) or "kmh").lower().replace("/", "")
        return round(n if unit == "mph" else n / 1.609344, 1)

    return pd.Series([one(v) for v in raw], index=raw.index, dtype="float64")


# ---------------------------------------------------------------------------
# the snap
# ---------------------------------------------------------------------------


def snap_crs_for(jurisdiction: str) -> int:
    crs = config.geo()["crs"]["snap"]
    if jurisdiction not in crs:
        raise KeyError(
            f"no projected CRS for jurisdiction {jurisdiction!r} in "
            f"config/geo.toml [crs.snap] (have: {sorted(crs)})"
        )
    return int(crs[jurisdiction])


def snap_points(
    points: gpd.GeoDataFrame,
    roads: gpd.GeoDataFrame,
    *,
    key: str = "crash_sk",
    epsg: int,
    max_distance_m: float | None = None,
    search_distance_m: float | None = None,
) -> tuple[pd.DataFrame, dict[str, Any]]:
    """Nearest road per point, with linear referencing. Returns (frame, stats).

    Candidates are searched to `search_distance_m` and only then judged against
    `max_distance_m`, so the distribution ABOVE the threshold is measurable
    rather than truncated: a rejected row still records that it was 118 m from
    a road, not merely that it failed.

    Ties -- two ways at the same distance, which happens at an intersection
    where both approaches are equidistant -- go to the lowest `osm_way_id`.
    Arbitrary but deterministic, which is what the byte-identity guarantee
    needs; `sjoin_nearest` alone returns all tied matches and their order
    depends on the index build.
    """
    cfg = config.geo()["snap"]
    max_distance_m = float(cfg["max_distance_m"] if max_distance_m is None else max_distance_m)
    search_distance_m = float(
        cfg["search_distance_m"] if search_distance_m is None else search_distance_m
    )

    out = pd.DataFrame(index=points.index)
    out[key] = points[key].to_numpy()
    out["snap_status"] = SNAP_NO_GEOMETRY
    out["osm_way_id"] = pd.Series(pd.NA, index=points.index, dtype="Int64")
    for col in ("snap_distance_m", "segment_length_m", "offset_m", "offset_frac"):
        out[col] = pd.Series(np.nan, index=points.index, dtype="float64")
    for col in OSM_ATTRIBUTES:
        out[col] = (
            pd.Series(np.nan, index=points.index, dtype="float64")
            if col.endswith("_mph")
            else pd.Series(pd.NA, index=points.index, dtype="object")
        )
    out["snap_crs_epsg"] = pd.Series(pd.NA, index=points.index, dtype="Int64")

    have = points[points.geometry.notna()]
    if have.empty or roads.empty:
        return out, {"attempted": 0, "snapped": 0, "rejected": 0,
                     "crs_epsg": epsg, "max_distance_m": max_distance_m}

    # THE metric operation of this module. Both sides go to EPSG:<epsg> -- a
    # projected, metre-based CRS for the jurisdiction -- because every number
    # produced below (nearest distance, segment length, offset along the line)
    # is a length on the ground. See the module docstring for why 3857 is not
    # an option.
    pts_p = have.to_crs(epsg=epsg)
    roads_p = roads.to_crs(epsg=epsg)

    joined = gpd.sjoin_nearest(
        pts_p[[key, pts_p.geometry.name]],
        roads_p[["osm_way_id", roads_p.geometry.name]],
        how="left",
        max_distance=search_distance_m,
        distance_col="snap_distance_m",
    )
    ties = int(joined.duplicated(subset=[key]).sum())
    joined = (
        joined.sort_values([key, "snap_distance_m", "osm_way_id"], kind="stable")
        .drop_duplicates(subset=[key], keep="first")
    )

    hit = joined["osm_way_id"].notna()
    matched = joined[hit].copy()
    matched["osm_way_id"] = matched["osm_way_id"].astype("int64")

    # Linear referencing, in the same projected CRS the distance came from.
    # `line.project(point)` is the distance along the line to the point's
    # nearest position on it -- metres, because the CRS is metres.
    lines = roads_p.set_index("osm_way_id").geometry
    geom = matched.set_index(key).geometry
    seg = lines.reindex(matched["osm_way_id"].to_numpy())
    seg.index = geom.index
    matched = matched.set_index(key)
    matched["segment_length_m"] = seg.length.to_numpy()
    matched["offset_m"] = shapely.line_locate_point(
        seg.to_numpy(), geom.to_numpy()
    )
    matched["offset_frac"] = np.where(
        matched["segment_length_m"] > 0,
        matched["offset_m"] / matched["segment_length_m"],
        np.nan,
    )

    attrs = roads.set_index("osm_way_id")[OSM_ATTRIBUTES]
    matched = matched.join(attrs, on="osm_way_id")

    accepted = matched["snap_distance_m"] <= max_distance_m
    matched["snap_status"] = np.where(accepted, SNAP_SNAPPED, SNAP_REJECTED)
    # Attributes are cleared on a rejected row. Keeping them would be the exact
    # failure the threshold exists to prevent: inheriting a road's speed limit
    # from 400 m away is worse than a NULL, because a NULL is visibly absent.
    for col in (*OSM_ATTRIBUTES, "segment_length_m", "offset_m", "offset_frac",
                "osm_way_id"):
        matched.loc[~accepted, col] = pd.NA

    by_key = matched.reindex(out[key].to_numpy())
    for col in ("snap_status", "osm_way_id", "snap_distance_m", "segment_length_m",
                "offset_m", "offset_frac", *OSM_ATTRIBUTES):
        out[col] = by_key[col].to_numpy()

    # A point that had a geometry but no candidate inside the search radius is
    # a distance rejection too -- it is further than `search_distance_m` from
    # every road -- but its distance is unknown rather than large, so the
    # column stays NULL and the count is reported separately.
    no_candidate = points.geometry.notna().to_numpy() & out["snap_status"].isna().to_numpy()
    out.loc[no_candidate, "snap_status"] = SNAP_REJECTED
    out.loc[points.geometry.isna().to_numpy(), "snap_status"] = SNAP_NO_GEOMETRY
    out["osm_way_id"] = out["osm_way_id"].astype("Int64")
    out["osm_maxspeed_mph"] = pd.to_numeric(out["osm_maxspeed_mph"], errors="coerce")
    out.loc[points.geometry.notna().to_numpy(), "snap_crs_epsg"] = epsg

    d = out.loc[out["snap_distance_m"].notna(), "snap_distance_m"]
    stats = {
        "crs_epsg": epsg,
        "max_distance_m": max_distance_m,
        "search_distance_m": search_distance_m,
        "roads": int(len(roads)),
        "attempted": int(points.geometry.notna().sum()),
        "with_candidate": int(hit.sum()),
        "no_candidate_within_search": int(no_candidate.sum()),
        "snapped": int((out["snap_status"] == SNAP_SNAPPED).sum()),
        "rejected": int((out["snap_status"] == SNAP_REJECTED).sum()),
        "ties_broken": ties,
        "distance_percentiles": _percentiles(d),
        "null_rates": null_rates(out),
    }
    return out, stats


def _percentiles(d: pd.Series) -> dict[str, float | None]:
    if d.empty:
        return {}
    q = d.quantile([0.5, 0.9, 0.95, 0.99, 1.0])
    return {
        "p50": round(float(q.loc[0.5]), 2),
        "p90": round(float(q.loc[0.9]), 2),
        "p95": round(float(q.loc[0.95]), 2),
        "p99": round(float(q.loc[0.99]), 2),
        "max": round(float(q.loc[1.0]), 2),
        "mean": round(float(d.mean()), 2),
        "n": int(len(d)),
    }


def null_rates(snapped: pd.DataFrame) -> dict[str, Any]:
    """`maxspeed` and `lanes` null rates, overall and by `highway` class.

    Reported, never imputed. OSM tags speed limits and lane counts densely on
    motorways and sparsely on residential streets, so an overall number hides
    the shape: the useful sentence is "94% of residential ways carry no
    maxspeed", and that is only visible per class.
    """
    df = snapped[snapped["snap_status"] == SNAP_SNAPPED]
    if df.empty:
        return {}
    overall = {
        "rows": int(len(df)),
        "maxspeed_null_rate": round(float(df["osm_maxspeed"].isna().mean()), 4),
        "lanes_null_rate": round(float(df["osm_lanes"].isna().mean()), 4),
        "maxspeed_unparseable": int(
            (df["osm_maxspeed"].notna() & df["osm_maxspeed_mph"].isna()).sum()
        ),
    }
    by_class = (
        df.groupby("osm_highway", dropna=False)
        .agg(rows=("osm_highway", "size"),
             maxspeed_null=("osm_maxspeed", lambda s: int(s.isna().sum())),
             lanes_null=("osm_lanes", lambda s: int(s.isna().sum())))
        .sort_values("rows", ascending=False)
    )
    return {
        "overall": overall,
        "by_highway_class": {
            str(k): {"rows": int(r["rows"]),
                     "maxspeed_null_rate": round(r["maxspeed_null"] / r["rows"], 4),
                     "lanes_null_rate": round(r["lanes_null"] / r["rows"], 4)}
            for k, r in by_class.iterrows()
        },
    }


def verify_linear_reference(
    points: gpd.GeoDataFrame,
    roads: gpd.GeoDataFrame,
    result: pd.DataFrame,
    *,
    key: str = "crash_sk",
    epsg: int,
    sample: int = 2000,
    seed: int = 20260908,
    tolerance_m: float = 1.0,
) -> dict[str, Any]:
    """Cross-check `offset_m` geodesically: interpolate back and measure.

    `line.interpolate(offset_m)` is the snapped position. Measured against the
    crash coordinate with `common.GEOD` on the WGS84 ellipsoid -- a DIFFERENT
    method in a DIFFERENT frame from the projected distance the snap produced
    -- it must agree with `snap_distance_m` to within the state-plane scale
    error. Two methods agreeing is evidence; one method repeated is not.
    """
    snapped = result[result["snap_status"] == SNAP_SNAPPED]
    if snapped.empty:
        return {"sampled": 0}
    take = snapped.sample(min(sample, len(snapped)), random_state=seed)
    lines = roads.set_index("osm_way_id").to_crs(epsg=epsg).geometry
    pts = points.set_index(key).geometry

    seg = lines.reindex(take["osm_way_id"].astype("int64").to_numpy()).to_numpy()
    on_line = shapely.line_interpolate_point(seg, take["offset_m"].to_numpy())
    back = gpd.GeoSeries(on_line, crs=epsg).to_crs(config.geo()["crs"]["storage"])
    crash = pts.reindex(take[key].to_numpy())

    geodesic = np.array([
        c.geodesic_distance_m(cy, cx, by, bx)
        for cx, cy, bx, by in zip(crash.x, crash.y, back.x, back.y)
    ])
    delta = np.abs(geodesic - take["snap_distance_m"].to_numpy())
    return {
        "sampled": int(len(take)),
        "max_abs_delta_m": round(float(np.nanmax(delta)), 4),
        "mean_abs_delta_m": round(float(np.nanmean(delta)), 4),
        "within_tolerance": int((delta <= tolerance_m).sum()),
        "tolerance_m": tolerance_m,
        "method": "shapely line_interpolate_point in EPSG:%d vs pyproj Geod on WGS84" % epsg,
    }
