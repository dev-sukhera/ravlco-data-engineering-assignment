"""ST-DBSCAN: clusters tight in space AND time, not a year-long smear.

ASSIGNMENT.md asks for "a recurring Friday-night corridor, not a year-long
smear", and the distinction is the whole method. Plain DBSCAN over (x, y) on
eleven years of crashes finds the road network -- every busy corridor is dense
in space, so every busy corridor is a cluster, and the answer is a map of
Montgomery County's arterials that could have been drawn without any data. The
question worth asking is whether a set of crashes is unusually concentrated in
space AND in time at once, because that is the pattern an operations team can
actually act on: a signal-timing change, a work zone, a seasonal sightline
problem.

Two radii, and why sklearn cannot do it directly
-------------------------------------------------
`sklearn.cluster.DBSCAN` takes ONE `eps` and one metric. Space and time have
no common unit -- there is no honest exchange rate between a metre and an hour
-- so squeezing t into the coordinate vector with some scale factor is a
choice of exchange rate disguised as a preprocessing step, and the clusters it
returns depend entirely on that hidden number.

The correct formulation takes two independent thresholds and calls two points
neighbours only when BOTH hold. That is expressible as a precomputed sparse
neighbourhood: build the spatial pairs within `eps_space_m` with a KD-tree,
drop the pairs whose time difference exceeds `eps_time_h`, and hand DBSCAN the
surviving graph with `metric='precomputed'` and `eps` at the graph's own
threshold. The result is real ST-DBSCAN semantics with sklearn's (well-tested)
density-connectivity, rather than a reimplementation of the cluster expansion.

The sparse matrix stores a small positive constant rather than a real distance,
because the pair either is or is not a neighbour -- there is no meaningful
scalar distance between two points in a space with two incommensurable axes.
`eps` is set above that constant so every stored pair is a neighbour and no
unstored pair is. Note the one subtlety of `metric='precomputed'` on a sparse
matrix: sklearn treats a MISSING entry as infinitely far, which is exactly the
semantics wanted, but it also means an explicitly stored ZERO would be dropped
by scipy's sparse format -- hence the constant is positive, not 0.

Time is UTC
-----------
`crash_datetime_utc` for the clustering, because a time DIFFERENCE across a DST
boundary is only correct on an absolute timescale -- two crashes an hour apart
across the spring-forward gap have local wall clocks two hours apart.
`crash_datetime_local` (Phase 4's naive wall clock) is used only for the
day-of-week and hour-of-day profile, where the local clock is the correct and
the only meaningful answer: "Friday night" is a local-time concept.

CRS
---
`x_m`, `y_m` come from `frames.projected_points`, i.e. **EPSG:26985** (NAD83 /
Maryland, metres). `eps_space_m` is a distance in that frame. In Web Mercator
at Montgomery's latitude the same number would be a 1/cos(39.1 deg) ~ 1.29x
larger circle on the ground, so a 300 m epsilon would silently become 233 m.
"""

from __future__ import annotations

import logging
from typing import Any

import numpy as np
import pandas as pd
import pyarrow as pa
from scipy.sparse import coo_matrix
from scipy.spatial import cKDTree
from sklearn.cluster import DBSCAN

log = logging.getLogger("analysis.st_dbscan")

# The constant stored for a qualifying pair. Positive, because scipy's sparse
# formats treat a stored zero as absent, and small, so `eps=NEIGHBOUR*1.5`
# admits every stored pair and nothing else.
NEIGHBOUR = 1.0
EPS = 1.5

DOW_NAMES = ["Monday", "Tuesday", "Wednesday", "Thursday", "Friday",
             "Saturday", "Sunday"]

CLUSTER_COLUMNS = [
    "cluster_id", "run", "n_crashes", "n_injury", "n_fatal",
    "span_days", "first_crash_date", "last_crash_date",
    "radius_m", "centroid_x_m", "centroid_y_m", "crs_epsg",
    "modal_dow", "modal_hour", "dow_concentration", "night_fraction",
    "place_name", "place_highway",
]

SPATIOTEMPORAL = "spatiotemporal"
SPACE_ONLY = "space_only"


# The schema, declared rather than inferred. An EMPTY pandas frame carries no
# usable type information into DuckDB -- an empty object column registers as
# INTEGER and an empty datetime64 column as TIMESTAMP -- so the contract's own
# type check would fail for a reason that has nothing to do with the data.
# `--skip-st-dbscan` and "the clustering found nothing" both produce an empty
# table, and both have to validate against the same contract as a full one.
CLUSTER_SCHEMA = pa.schema([
        ("cluster_id", pa.int64()),
        ("run", pa.string()),
        ("n_crashes", pa.int64()),
        ("n_injury", pa.int64()),
        ("n_fatal", pa.int64()),
        ("span_days", pa.float64()),
        ("first_crash_date", pa.date32()),
        ("last_crash_date", pa.date32()),
        ("radius_m", pa.float64()),
        ("centroid_x_m", pa.float64()),
        ("centroid_y_m", pa.float64()),
        ("crs_epsg", pa.int64()),
        ("modal_dow", pa.string()),
        ("modal_hour", pa.int64()),
        ("dow_concentration", pa.float64()),
        ("night_fraction", pa.float64()),
        ("place_name", pa.string()),
        ("place_highway", pa.string()),
])


def empty_clusters() -> pa.Table:
    """A correctly typed, zero-row cluster table. See `CLUSTER_SCHEMA`.

    Returns an ARROW table, not a pandas frame, because it is what gets
    registered with DuckDB and validated -- and pandas cannot carry the types
    through zero rows. `_empty_frame` is the pandas-shaped counterpart used
    while the two runs are still being assembled and concatenated.
    """
    return pa.table(
        {f.name: pa.array([], type=f.type) for f in CLUSTER_SCHEMA},
        schema=CLUSTER_SCHEMA,
    )


def _empty_frame() -> pd.DataFrame:
    """A zero-row pandas frame with the right columns, for concatenation.

    Deliberately separate from `empty_clusters`: an arrow table cannot go
    through `pd.concat`, and a pandas frame cannot carry types through zero
    rows into DuckDB. Each is used where its own property is the one that
    matters, and `run` converts at the boundary.
    """
    return pd.DataFrame({c: pd.Series(dtype="object") for c in CLUSTER_COLUMNS})


def neighbourhood(
    xy: np.ndarray,
    t_hours: np.ndarray,
    *,
    eps_space_m: float,
    eps_time_h: float | None,
) -> coo_matrix:
    """The sparse 0/1 neighbourhood: near in space AND (optionally) in time.

    `eps_time_h=None` is the space-only contrast run -- the same spatial
    epsilon with the time condition removed, which is the year-long smear the
    assignment names and the reason the spatiotemporal result is interesting.

    The KD-tree does the spatial pruning first because it is the selective
    condition: at 300 m over a county, the spatial pairs are a tiny fraction of
    the n^2 possibilities, and filtering those on time is linear in the pairs
    rather than quadratic in the points.
    """
    tree = cKDTree(xy)
    pairs = tree.query_pairs(r=eps_space_m, output_type="ndarray")
    if len(pairs) and eps_time_h is not None:
        dt = np.abs(t_hours[pairs[:, 0]] - t_hours[pairs[:, 1]])
        pairs = pairs[dt <= eps_time_h]

    n = len(xy)
    # Symmetric, with the diagonal: DBSCAN counts a point in its own
    # neighbourhood when deciding whether it is a core point, and an absent
    # diagonal would shift every min_samples decision by one.
    rows = np.concatenate([pairs[:, 0], pairs[:, 1], np.arange(n)])
    cols = np.concatenate([pairs[:, 1], pairs[:, 0], np.arange(n)])
    data = np.full(len(rows), NEIGHBOUR, dtype="float64")
    return coo_matrix((data, (rows, cols)), shape=(n, n)).tocsr()


def cluster(
    xy: np.ndarray,
    t_hours: np.ndarray,
    *,
    eps_space_m: float,
    eps_time_h: float | None,
    min_samples: int,
) -> np.ndarray:
    """Cluster labels per point; -1 is noise, as DBSCAN defines it."""
    graph = neighbourhood(
        xy, t_hours, eps_space_m=eps_space_m, eps_time_h=eps_time_h
    )
    db = DBSCAN(eps=EPS, min_samples=int(min_samples), metric="precomputed")
    return db.fit_predict(graph)


def summarise(
    points: pd.DataFrame,
    labels: np.ndarray,
    *,
    run: str,
    crs_epsg: int,
) -> pd.DataFrame:
    """One row per cluster: size, extent, span and its local-time profile.

    `span_days` is the discriminator between the two runs and the number the
    prose quotes: a spatiotemporal cluster is days wide, a space-only one is
    the whole study period.

    `dow_concentration` is the share of the cluster's crashes on its modal
    weekday. 1/7 = 0.14 is "no day-of-week pattern"; a corridor that is
    genuinely a Friday-night problem sits far above it. This is descriptive,
    not a test -- with a handful of crashes per cluster a high share is easy to
    get by chance, and the report says so rather than dressing it as a finding.
    """
    from .hotspots import place_names

    df = points.copy()
    df["cluster_id"] = labels
    df = df[df["cluster_id"] >= 0]
    if df.empty:
        return _empty_frame()

    local = pd.to_datetime(df["crash_datetime_local"], errors="coerce")
    df["_dow"] = local.dt.dayofweek
    df["_hour"] = local.dt.hour
    # "Night" as a plain clock rule (20:00-05:59), stated rather than implied.
    # Phase 4's report flags that the real answer is sunrise/sunset from the
    # coordinate and the date; that is Phase 7's job and this is a descriptive
    # column, not a feature.
    df["_night"] = df["_hour"].isin([20, 21, 22, 23, 0, 1, 2, 3, 4, 5])

    rows = []
    for cid, part in df.groupby("cluster_id", sort=True):
        cx, cy = part["x_m"].mean(), part["y_m"].mean()
        radius = float(
            np.sqrt((part["x_m"] - cx) ** 2 + (part["y_m"] - cy) ** 2).max()
        )
        dates = pd.to_datetime(part["crash_date"])
        dow = part["_dow"].dropna()
        hour = part["_hour"].dropna()
        rows.append({
            "cluster_id": int(cid),
            "run": run,
            "n_crashes": int(len(part)),
            "n_injury": int((part["severity_ordinal"] >= 2).sum()),
            "n_fatal": int((part["severity_ordinal"] == 5).sum()),
            "span_days": float((dates.max() - dates.min()).days),
            "first_crash_date": dates.min(),
            "last_crash_date": dates.max(),
            "radius_m": radius,
            "centroid_x_m": float(cx),
            "centroid_y_m": float(cy),
            "crs_epsg": int(crs_epsg),
            "modal_dow": DOW_NAMES[int(dow.mode().iloc[0])] if len(dow) else None,
            "modal_hour": int(hour.mode().iloc[0]) if len(hour) else None,
            "dow_concentration": (
                float((dow == dow.mode().iloc[0]).mean()) if len(dow) else None
            ),
            "night_fraction": float(part["_night"].mean()),
        })

    out = pd.DataFrame(rows)
    names = place_names(df.assign(h3_r8=df["h3_r8"]), cell_column="cluster_id")
    out = out.merge(names, on="cluster_id", how="left")
    for col in ("place_name", "place_highway"):
        # `string`, not object: an all-null object column (every crash
        # unsnapped, e.g. a --skip-snap geo build) registers as INTEGER in
        # DuckDB and fails the contract's type check spuriously.
        out[col] = (out[col] if col in out else pd.NA)
        out[col] = pd.Series(out[col], index=out.index, dtype="string")
    out["first_crash_date"] = pd.to_datetime(out["first_crash_date"]).dt.date
    out["last_crash_date"] = pd.to_datetime(out["last_crash_date"]).dt.date
    return out[CLUSTER_COLUMNS].sort_values(
        ["run", "cluster_id"], ignore_index=True
    )


def run(
    points: pd.DataFrame, cfg: dict[str, Any], *, crs_epsg: int
) -> tuple[pd.DataFrame, dict[str, Any]]:
    """Both runs -- with the time condition and without it -- and the contrast.

    The space-only run is not an ablation for completeness; it is the control
    that makes the spatiotemporal result mean anything. If the two produced
    clusters of similar temporal span, the time threshold would be doing no
    work and the report would have to say so.
    """
    xy = np.column_stack([
        points["x_m"].to_numpy(dtype="float64"),
        points["y_m"].to_numpy(dtype="float64"),
    ])
    utc = pd.to_datetime(points["crash_datetime_utc"], errors="coerce", utc=True)
    # Hours since the first crash in the corpus. An absolute timescale, so a
    # difference across a DST boundary is the real elapsed time.
    origin = utc.min()
    t_hours = ((utc - origin).dt.total_seconds() / 3600.0).to_numpy()

    usable = np.isfinite(t_hours) & np.isfinite(xy).all(axis=1)
    dropped = int((~usable).sum())
    xy, t_hours = xy[usable], t_hours[usable]
    usable_points = points[usable].reset_index(drop=True)

    eps_space = float(cfg["eps_space_m"])
    eps_time = float(cfg["eps_time_h"])
    min_samples = int(cfg["min_samples"])

    frames_out, stats = [], {}
    for name, time_eps in ((SPATIOTEMPORAL, eps_time), (SPACE_ONLY, None)):
        labels = cluster(
            xy, t_hours, eps_space_m=eps_space, eps_time_h=time_eps,
            min_samples=min_samples,
        )
        table = summarise(usable_points, labels, run=name, crs_epsg=crs_epsg)
        frames_out.append(table)
        spans = table["span_days"]
        stats[name] = {
            "eps_space_m": eps_space,
            "eps_time_h": time_eps,
            "min_samples": min_samples,
            "n_points": int(len(xy)),
            "n_clusters": int(len(table)),
            "n_clustered_points": int((labels >= 0).sum()),
            "n_noise_points": int((labels == -1).sum()),
            "median_span_days": float(spans.median()) if len(spans) else 0.0,
            "max_span_days": float(spans.max()) if len(spans) else 0.0,
            "median_cluster_size": float(table["n_crashes"].median()) if len(table) else 0.0,
            "largest_cluster_size": int(table["n_crashes"].max()) if len(table) else 0,
        }
        log.info(
            "ST-DBSCAN [%s]: %d clusters over %d points "
            "(%d clustered, %d noise), median span %.1f days",
            name, stats[name]["n_clusters"], len(xy),
            stats[name]["n_clustered_points"], stats[name]["n_noise_points"],
            stats[name]["median_span_days"],
        )

    stats["points_dropped_no_utc_or_xy"] = dropped
    stats["crs_epsg"] = crs_epsg
    st, so = stats[SPATIOTEMPORAL], stats[SPACE_ONLY]
    stats["span_ratio_space_only_to_spatiotemporal"] = (
        round(so["median_span_days"] / st["median_span_days"], 2)
        if st["median_span_days"] else None
    )
    out = pd.concat(frames_out, ignore_index=True)
    # Both runs found nothing: hand back the ARROW table, whose declared
    # schema survives zero rows. A zero-row pandas frame would register with
    # DuckDB as all-INTEGER and fail the contract's type check for a reason
    # that has nothing to do with the data.
    return (out if len(out) else empty_clusters()), stats
