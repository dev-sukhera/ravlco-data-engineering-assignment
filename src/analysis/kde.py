"""Kernel density: a continuous intensity surface, and the bandwidth argument.

ASSIGNMENT.md: "Bandwidth selection is the whole exercise; justify it." So this
module produces three surfaces and a defence, not one surface and a colour bar.

The three bandwidths
--------------------
1. **Rules of thumb** (Scott, Silverman). Closed-form, computed from n and the
   spread of the data, and both derived under an assumption this data violates
   flatly: that the underlying density is a single Gaussian blob. Crash
   locations are a road network -- a filamentary, multi-modal, sharply
   non-Gaussian thing. The rules are reported because they are the numbers a
   reader will compute to check, and because the size of their disagreement
   with the cross-validated answer is itself the argument for cross-validating.

2. **Cross-validated** (`sklearn.GridSearchCV` over log-spaced candidates,
   maximising held-out log-likelihood). This is the honest one: it asks which
   bandwidth best predicts crash locations the model has not seen, which is
   the question a bandwidth is actually answering.

3. **500 m, the practitioner control.** What gets picked off the shelf. Kept
   as a reference point so the report can say how far off-the-shelf lands.

Why the CV folds are spatially blocked
--------------------------------------
This is the trap ASSIGNMENT.md's last table row describes, and it bites KDE
exactly as hard as it bites a model. Crash locations are strongly spatially
autocorrelated: a randomly held-out point almost always has a training point a
few tens of metres away, on the same road, often at the same intersection. A
tiny bandwidth then scores brilliantly on held-out likelihood -- it has
effectively memorised the training points and the test point is sitting on one
-- so random-fold CV drives the bandwidth toward the grid pitch and produces a
surface of spikes at individual intersections that will not reproduce next
year.

Blocking by H3 r7 cell (~5.2 km2) breaks that: a whole neighbourhood is held
out at once, so a test point's nearest training point is a real distance away
and the score measures generalisation across space rather than interpolation
within it. `GroupKFold` on the r7 id does this in one line, and a test asserts
no r7 cell appears in two folds. The report quotes both answers -- the random-
fold bandwidth and the blocked one -- because the gap between them is the
measurement that justifies the paragraph.

CRS
---
Everything metric here is **EPSG:26985** (NAD83 / Maryland, metres): the point
coordinates, the grid pitch, all three bandwidths, and the integral. A
bandwidth is a distance, and a distance computed in Web Mercator at
Montgomery's latitude is wrong by 1/cos(39.1 deg) ~ 1.29 -- a "500 m" kernel
would be a 387 m one on the ground. The surface is written with `crs_epsg` on every row
and in the manifest, because a grid of bare (x, y) doubles with no frame is
not a spatial output.

The integral check
------------------
The surface is a probability density, so it integrates to 1 over the plane by
construction -- and therefore the Riemann sum over the grid is ~1 if and only
if the grid actually covers the support and the cell area is right. That makes
it a real check on the grid extent and the units at once rather than a
tautology: a grid that clips the county, or an area computed in degrees,
both fail it. The intensity surface (expected crashes per km2) is the density
times N, and both are written.

Two evaluators, and why there are two
-------------------------------------
`sklearn.neighbors.KernelDensity` selects the bandwidth. It evaluates a
pointwise log-likelihood at held-out points, which is exactly what
cross-validation needs and what a tree-based estimator is good at.

It does NOT draw the published surface, and that is a measured decision rather
than a preference. The surface is ~402,000 grid cells against 70,692 crashes;
`score_samples` over that took **over 25 minutes per bandwidth** on the real
corpus (much worse than a synthetic benchmark predicts, because crash points
lie along a road network spread across a 50 km county and the tree can barely
prune), and there are three distinct bandwidths to draw.

For a Gaussian kernel evaluated on a REGULAR GRID there is an exact and much
faster route: binning the points onto that same grid and convolving with a
Gaussian. Convolution is what a KDE *is* -- the estimate is the point measure
convolved with the kernel -- so `scipy.ndimage.gaussian_filter` on the 2-D
histogram is not an approximation of the method, it is the method, computed in
O(grid) instead of O(grid x points). The only error introduced is that each
crash is snapped to its 100 m bin centre, displacing it by at most 71 m under
a bandwidth of 500-2,947 m, plus the filter's truncation at 4 sigma.

That claim is not left as an assertion. `max_relative_error_vs_exact` evaluates
the sklearn estimator at a random sample of grid cells and compares, on every
build, and the number goes in the manifest. MEASURED 2026-09-08: the error
scales with the bin-to-bandwidth ratio, as it should -- 1.5e-03 at the
cross-validated 2,947 m, 3.7e-03 at Scott's 1,261 m, and 2.5e-02 at the 500 m
practitioner control, where a 100 m bin is a fifth of the kernel. The
recommended surface is the widest one, so the error on the number that gets
used is the smallest of the three; the 500 m surface is a comparison exhibit
and 2.5% is well inside what it is used to say. Runtime went from over 25
minutes per surface to about one second.

CRS
---
Everything metric here is **EPSG:26985** (NAD83 / Maryland, metres): the point
coordinates, the grid pitch, all three bandwidths, and the integral. A
bandwidth is a distance, and a distance computed in Web Mercator at
Montgomery's latitude is wrong by 1/cos(39.1 deg) ~ 1.29 -- a "500 m" kernel
would be a 387 m one on the ground. The surface is written with `crs_epsg` on every row
and in the manifest, because a grid of bare (x, y) doubles with no frame is
not a spatial output.
"""

from __future__ import annotations

import logging
from typing import Any, Sequence

import numpy as np
import pandas as pd
from scipy.ndimage import gaussian_filter
from sklearn.model_selection import GridSearchCV, GroupKFold, KFold
from sklearn.neighbors import KernelDensity

log = logging.getLogger("analysis.kde")

SURFACE_COLUMNS = [
    "bandwidth_m", "bandwidth_method", "x_m", "y_m", "density",
    "intensity_per_km2", "crs_epsg",
]


# ---------------------------------------------------------------------------
# rules of thumb
# ---------------------------------------------------------------------------


def scott_bandwidth(points: np.ndarray) -> float:
    """Scott's rule, in the units of `points` (metres here).

    n**(-1/(d+4)) times the per-dimension standard deviation, averaged over the
    two dimensions. d = 2 for a planar point pattern, so the exponent is -1/6.

    Derived for a Gaussian target density. Crash points are not Gaussian; the
    number is reported as a reference, not used as an answer.
    """
    n, d = points.shape
    factor = n ** (-1.0 / (d + 4))
    return float(factor * points.std(axis=0, ddof=1).mean())


def silverman_bandwidth(points: np.ndarray) -> float:
    """Silverman's rule of thumb, in the units of `points`.

    (n * (d + 2) / 4) ** (-1 / (d + 4)) times the mean per-dimension standard
    deviation. Same Gaussian assumption as Scott and a slightly smaller
    constant, so it always returns a slightly narrower kernel.
    """
    n, d = points.shape
    factor = (n * (d + 2) / 4.0) ** (-1.0 / (d + 4))
    return float(factor * points.std(axis=0, ddof=1).mean())


# ---------------------------------------------------------------------------
# cross-validation
# ---------------------------------------------------------------------------


def candidate_bandwidths(
    low: float, high: float, count: int
) -> np.ndarray:
    """Log-spaced candidates. Log, because bandwidth acts multiplicatively.

    The difference between 100 m and 200 m is a different surface; the
    difference between 1,900 m and 2,000 m is not. Linear spacing would spend
    most of its candidates in the range where the answer does not change.
    """
    return np.logspace(np.log10(low), np.log10(high), int(count))


def select_bandwidth(
    points: np.ndarray,
    groups: Sequence[str] | None,
    *,
    candidates: np.ndarray,
    folds: int = 5,
    seed: int = 0,
    blocked: bool = True,
) -> tuple[float, pd.DataFrame, dict[str, Any]]:
    """Grid-search CV over bandwidth, maximising held-out log-likelihood.

    Returns (best bandwidth, the score table, stats). The score table is the
    deliverable as much as the number is: a flat likelihood curve means the
    choice does not matter much and the report should say so, and a curve
    still rising at the edge of the range means the range was too narrow.

    `blocked=True` uses `GroupKFold` on `groups` (the H3 r7 cell) -- see the
    module docstring for why random folds are a leak here. `blocked=False`
    runs plain `KFold`, and exists so the report can quote the difference
    rather than assert it.
    """
    if blocked:
        if groups is None:
            raise ValueError("blocked CV needs a group label per point")
        n_groups = len(set(groups))
        if n_groups < folds:
            raise ValueError(
                f"blocked CV wants at least {folds} distinct r7 blocks, got "
                f"{n_groups} -- lower [analysis.kde] cv_folds or widen the corpus"
            )
        cv: Any = GroupKFold(n_splits=folds)
        fit_groups = np.asarray(groups)
    else:
        # Shuffled with the build seed so the "random folds" comparison is
        # itself reproducible.
        cv = KFold(n_splits=folds, shuffle=True, random_state=seed)
        fit_groups = None

    search = GridSearchCV(
        KernelDensity(kernel="gaussian"),
        {"bandwidth": list(candidates)},
        cv=cv,
        # KernelDensity.score is the total log-likelihood of the held-out fold.
        # Higher is better, which is what GridSearchCV maximises by default.
        n_jobs=1,  # deterministic ordering; the grid is 12 fits, not 12,000
        refit=False,
    )
    search.fit(points, groups=fit_groups)

    table = pd.DataFrame({
        "bandwidth_m": np.asarray(search.cv_results_["param_bandwidth"], dtype="float64"),
        "mean_log_likelihood": search.cv_results_["mean_test_score"],
        "std_log_likelihood": search.cv_results_["std_test_score"],
        "rank": search.cv_results_["rank_test_score"],
    }).sort_values("bandwidth_m", ignore_index=True)

    best = float(search.best_params_["bandwidth"])
    stats = {
        "method": "grouped_cv_r7_blocks" if blocked else "random_kfold",
        "folds": int(folds),
        "n_points": int(len(points)),
        "n_blocks": int(len(set(groups))) if groups is not None else None,
        "candidates": [float(b) for b in candidates],
        "best_bandwidth_m": best,
        "best_mean_log_likelihood": float(search.best_score_),
        "at_range_edge": bool(
            np.isclose(best, candidates.min()) or np.isclose(best, candidates.max())
        ),
    }
    if stats["at_range_edge"]:
        # A maximum on the boundary is not a maximum. Reported rather than
        # silently returned, because it means the candidate range excluded the
        # answer and the number below is a censored one.
        log.warning(
            "CV bandwidth %.0f m sits at the edge of the candidate range "
            "[%.0f, %.0f] -- the optimum is outside the searched range",
            best, candidates.min(), candidates.max(),
        )
    log.info(
        "bandwidth CV (%s, %d folds, n=%d): best %.0f m, log-likelihood %.2f",
        stats["method"], folds, len(points), best, search.best_score_,
    )
    return best, table, stats


def blocked_folds_are_disjoint(groups: Sequence[str], folds: int) -> bool:
    """No block appears in more than one fold. The property the test asserts."""
    g = np.asarray(groups)
    seen: dict[str, int] = {}
    for i, (_, test_idx) in enumerate(GroupKFold(n_splits=folds).split(g, groups=g)):
        for block in set(g[test_idx]):
            if seen.setdefault(block, i) != i:
                return False
    return True


# ---------------------------------------------------------------------------
# the surface
# ---------------------------------------------------------------------------


def make_grid(
    points: np.ndarray, *, pitch_m: float, pad_m: float
) -> tuple[np.ndarray, np.ndarray]:
    """A regular grid over the points' extent plus a pad, in EPSG:26985 metres.

    The pad matters for the integral check: a Gaussian kernel puts mass outside
    the convex hull of the data, and a grid clipped to the data's bounding box
    would integrate to noticeably less than 1 and look like a bug in the
    density rather than a bug in the grid. Three bandwidths of pad captures
    >99.7% of a Gaussian's mass.

    Cell centres, not cell corners: `density` is evaluated at the centre and
    multiplied by the full cell area, which is the midpoint rule.
    """
    minx, miny = points.min(axis=0) - pad_m
    maxx, maxy = points.max(axis=0) + pad_m
    xs = np.arange(minx, maxx + pitch_m, pitch_m)
    ys = np.arange(miny, maxy + pitch_m, pitch_m)
    return xs, ys


def surface(
    points: np.ndarray,
    *,
    bandwidth_m: float,
    xs: np.ndarray,
    ys: np.ndarray,
    crs_epsg: int,
    method: str,
    truncate: float = 4.0,
    verify_sample: int = 500,
    seed: int = 0,
) -> tuple[pd.DataFrame, dict[str, Any]]:
    """One KDE surface as a tidy (x_m, y_m, density) frame plus its diagnostics.

    Computed as a binned convolution: the crashes are histogrammed onto the
    output grid and the histogram is convolved with the Gaussian kernel. A
    kernel density estimate IS the point measure convolved with the kernel, so
    this is the definition evaluated efficiently, not a different estimator --
    see the module docstring for the cost measurement that forced it and for
    the two error terms it introduces (binning to the grid, and truncating the
    kernel at `truncate` sigma).

    `verify_sample` grid cells are then evaluated with `sklearn`'s exact
    tree-based estimator and compared, so the agreement is a number in the
    manifest rather than a claim in a comment. Set it to 0 to skip.

    `density` is a probability density per square metre (it integrates to 1);
    `intensity_per_km2` is that times N times 1e6, i.e. expected crashes per
    square kilometre over the study period, which is the number an operations
    reader can act on. Both are written because they answer different
    questions and only one has units anybody recognises.
    """
    pitch = float(xs[1] - xs[0])
    if not np.isclose(pitch, float(ys[1] - ys[0])):
        raise ValueError("the grid must be square for a symmetric Gaussian filter")

    # `xs`/`ys` are cell CENTRES, so the histogram edges sit half a pitch out
    # on each side. Getting this wrong shifts the whole surface by 50 m, which
    # is invisible on a map and wrong in the parquet.
    x_edges = np.append(xs - pitch / 2.0, xs[-1] + pitch / 2.0)
    y_edges = np.append(ys - pitch / 2.0, ys[-1] + pitch / 2.0)
    counts, _, _ = np.histogram2d(
        points[:, 1], points[:, 0], bins=[y_edges, x_edges]
    )
    n = len(points)
    binned = int(counts.sum())

    # sigma in GRID CELLS, which is the bandwidth in metres over the pitch in
    # metres -- both in EPSG:26985, so the ratio is dimensionless and correct.
    sigma = float(bandwidth_m) / pitch
    smoothed = gaussian_filter(
        counts, sigma=sigma, mode="constant", cval=0.0, truncate=truncate
    )
    # counts -> probability density per m2: divide by N and by the cell area.
    cell_area_m2 = pitch * pitch  # EPSG:26985 metres, so this is m2
    density = (smoothed / (n * cell_area_m2)).ravel()

    gx, gy = np.meshgrid(xs, ys, indexing="xy")
    coords = np.column_stack([gx.ravel(), gy.ravel()])
    integral = float(density.sum() * cell_area_m2)

    frame = pd.DataFrame({
        "bandwidth_m": float(bandwidth_m),
        "bandwidth_method": method,
        "x_m": coords[:, 0],
        "y_m": coords[:, 1],
        "density": density,
        # crashes per km2 = density (per m2) * N * 1e6 m2/km2
        "intensity_per_km2": density * n * 1e6,
        "crs_epsg": int(crs_epsg),
    })

    stats = {
        "bandwidth_m": float(bandwidth_m),
        "method": method,
        "estimator": "binned_gaussian_convolution",
        "truncate_sigma": float(truncate),
        "sigma_cells": sigma,
        "n_points": n,
        "points_inside_grid": binned,
        "points_outside_grid": n - binned,
        "grid_pitch_m": pitch,
        "grid_shape": [int(len(ys)), int(len(xs))],
        "grid_cells": int(len(frame)),
        "integral": integral,
        "integral_error": abs(integral - 1.0),
        "max_intensity_per_km2": float(frame["intensity_per_km2"].max()),
        "crs_epsg": int(crs_epsg),
    }
    if verify_sample:
        stats["max_relative_error_vs_exact"] = max_relative_error_vs_exact(
            points, coords, density, bandwidth_m=bandwidth_m,
            sample=verify_sample, seed=seed,
        )
    log.info(
        "KDE %s bandwidth=%.0f m: %d grid cells, integral %.4f, "
        "peak %.1f crashes/km2, max rel. error vs exact %.2e",
        method, bandwidth_m, len(frame), integral,
        stats["max_intensity_per_km2"],
        stats.get("max_relative_error_vs_exact", float("nan")),
    )
    return frame, stats


def max_relative_error_vs_exact(
    points: np.ndarray,
    coords: np.ndarray,
    density: np.ndarray,
    *,
    bandwidth_m: float,
    sample: int = 500,
    seed: int = 0,
) -> float:
    """How far the binned surface is from `sklearn`'s exact evaluation.

    Sampled rather than exhaustive, because the exhaustive comparison is the
    25-minute computation the binning exists to avoid. Cells are drawn from
    the ones that carry real mass -- comparing two numbers that are both
    ~1e-30 in an empty corner of the grid would report a huge relative error
    about nothing.
    """
    if not len(coords):
        return 0.0
    rng = np.random.default_rng(seed)
    # Restrict to cells above the median of the non-empty density, so the
    # comparison is made where the surface is actually claiming something.
    live = np.nonzero(density > np.median(density[density > 0]))[0]
    if not len(live):
        return 0.0
    idx = rng.choice(live, size=min(sample, len(live)), replace=False)
    exact = np.exp(
        KernelDensity(kernel="gaussian", bandwidth=float(bandwidth_m))
        .fit(points)
        .score_samples(coords[idx])
    )
    return float(np.max(np.abs(density[idx] - exact) / np.maximum(exact, 1e-300)))


def integrates_to_one(stats: dict[str, Any], tolerance: float = 0.02) -> bool:
    """The sanity check §4.4 asks for, as a boolean the build can assert."""
    return bool(stats["integral_error"] <= tolerance)
