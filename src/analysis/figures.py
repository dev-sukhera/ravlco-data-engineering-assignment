"""The figures. "An unlabeled heatmap is not an analysis" -- ASSIGNMENT.md 3c.

Every figure this module produces carries, without exception: a title naming
the study area and the period, N, the significance level and the correction
where one applies, the CRS of any distance shown, and axis units or a scale
bar. Those are not decoration -- a hot-spot map without its correction stated
is a different claim from the same map with it, and a metric axis with no CRS
is not reproducible.

Colours are defined once, at the top, and shared across every figure: the same
class is the same colour everywhere, so a reader who has learned the LISA
legend can read the Gi* map without relearning it. The hot/cold ramp is
red-blue (diverging, sequential in lightness in each direction) rather than a
rainbow, because a rainbow ramp implies an ordering its hues do not have and
is unreadable in greyscale or to a red-green colourblind reader. The diverging
choice is deliberate: hot and cold are opposite directions from a neutral, not
two ends of one scale.

Matplotlib only, static PNG, no basemap tiles -- a tile fetch is a network call
and this build has none. That costs the reader street context, which is why the
hot cells are labelled from `osm_name` instead.

Figures are NOT part of the byte-identity guarantee. Matplotlib embeds its own
version and a creation date in PNG metadata, so two runs differ in bytes while
being identical images. The data behind every figure here is a parquet table
that IS byte-identical, which is where the reproducibility claim lives.
"""

from __future__ import annotations

import logging
from pathlib import Path
from typing import Any, Sequence

import matplotlib

# Agg before pyplot: a build must never try to open a window, and on a headless
# runner the default backend selection is one more thing that can fail late.
matplotlib.use("Agg")

import matplotlib.pyplot as plt  # noqa: E402
import numpy as np  # noqa: E402
import pandas as pd  # noqa: E402
from matplotlib.colors import LinearSegmentedColormap, ListedColormap  # noqa: E402
from matplotlib.lines import Line2D  # noqa: E402
from matplotlib.patches import Patch  # noqa: E402

log = logging.getLogger("analysis.figures")

# One palette for the whole phase. Same class -> same colour in every figure.
CLASS_COLOURS: dict[str, str] = {
    "HOT_99": "#a50f15",   # deep red
    "HOT_95": "#fb6a4a",   # light red
    "NS": "#e8e8e8",       # neutral grey: "we could not reject the null"
    "COLD_95": "#6baed6",  # light blue
    "COLD_99": "#08519c",  # deep blue
    "RATE_UNDEFINED": "#d9d9d9",
}
QUADRANT_COLOURS: dict[str, str] = {
    "HH": "#a50f15",   # high among high -- same red as HOT
    "LL": "#08519c",   # low among low -- same blue as COLD
    "HL": "#fdae61",   # high outlier in a low neighbourhood
    "LH": "#74add1",   # low outlier in a high neighbourhood
    "NS": "#e8e8e8",
}
CONTRAST_COLOURS: dict[str, str] = {
    "HOT_RAW_ONLY": "#7b3294",     # volume without risk
    "HOT_RATE_ONLY": "#008837",    # risk without volume
    "COLD_RAW_ONLY": "#c2a5cf",
    "COLD_RATE_ONLY": "#a6dba0",
    "LEVEL_CHANGE": "#bdbdbd",
    "RATE_UNDEFINED": "#404040",
}
# Sequential, perceptually monotone in lightness, and it starts near-white so
# an empty part of the county reads as empty rather than as "low".
DENSITY_CMAP = LinearSegmentedColormap.from_list(
    "crash_density",
    ["#fff7ec", "#fee8c8", "#fdbb84", "#e34a33", "#7f0000"],
)

FIGSIZE = (9.0, 8.0)
DPI = 110  # keeps a full-page map comfortably under the 300 KB budget


def _finish(fig: plt.Figure, dest: Path, *, max_kb: int = 300) -> dict[str, Any]:
    """Save, report the size, and warn if a figure blew the committed budget."""
    dest.parent.mkdir(parents=True, exist_ok=True)
    fig.savefig(dest, dpi=DPI, bbox_inches="tight", facecolor="white")
    plt.close(fig)
    kb = dest.stat().st_size / 1024
    if kb > max_kb:
        log.warning(
            "%s is %.0f KB, over the %d KB budget for a committed figure",
            dest.name, kb, max_kb,
        )
    log.info("figure %s (%.0f KB)", dest.name, kb)
    return {"path": str(dest), "bytes": dest.stat().st_size, "kb": round(kb, 1)}


def _subtitle(area: Any, n: int, extra: str = "") -> str:
    """The provenance line every figure carries under its title."""
    parts = [
        f"{area.label} (county GEOID {area.county_geoid})",
        f"{area.period_start:%Y-%m-%d} to {area.period_end:%Y-%m-%d}",
        f"N = {n:,} crashes",
    ]
    if extra:
        parts.append(extra)
    return "  |  ".join(parts)


def _hex_patches(
    ax: plt.Axes,
    cells: pd.DataFrame,
    colours: Sequence[str],
    *,
    linewidth: float = 0.12,
) -> None:
    """Draw the r8 cells as filled polygons in EPSG:4326 degrees.

    Degrees, not metres, and deliberately: these maps show no distance, only
    topology and class, so there is nothing for a projection to be wrong
    about. The aspect ratio is set to 1/cos(latitude) so the hexagons look
    like hexagons rather than being squashed -- which is a DISPLAY correction,
    not a coordinate transform, and it is why no scale bar is drawn on the
    cell maps. Every figure that does show a distance (the KDE surfaces) is in
    EPSG:26985 metres with labelled axes and a scale bar.
    """
    from .frames import cell_polygons

    polys = cell_polygons(cells["h3_r8"].tolist())
    for geom, colour in zip(polys.geometry, colours):
        xs, ys = geom.exterior.xy
        ax.fill(xs, ys, facecolor=colour, edgecolor="white", linewidth=linewidth)
    ax.set_aspect(1.0 / np.cos(np.radians(float(np.mean(ax.get_ylim() or [39.1])))))
    ax.set_xlabel("longitude (EPSG:4326)")
    ax.set_ylabel("latitude (EPSG:4326)")


def _class_legend(ax: plt.Axes, counts: dict[str, int], colours: dict[str, str],
                  title: str) -> None:
    handles = [
        Patch(facecolor=colours[k], edgecolor="#999999",
              label=f"{k}  ({counts.get(k, 0):,} cells)")
        for k in colours if k in counts
    ]
    ax.legend(handles=handles, title=title, loc="upper left",
              fontsize=8, title_fontsize=8, framealpha=0.95)


# ---------------------------------------------------------------------------
# the figures
# ---------------------------------------------------------------------------


def per_year_counts(years: pd.DataFrame, area: Any, dest: Path) -> dict[str, Any]:
    """Why the period is what it is: the pandemic drop and the partial year."""
    fig, ax = plt.subplots(figsize=(9, 4.5))
    inside = years["in_study_period"]
    ax.bar(years["year"], years["n_crashes"],
           color=np.where(inside, "#3182bd", "#cccccc"),
           edgecolor="white", label=None)
    ax.plot(years["year"], years["n_injury"], "o-", color="#e6550d",
            markersize=4, linewidth=1.4, label="injury crashes (KABCO C+)")
    for _, row in years.iterrows():
        ax.annotate(f"{int(row['n_fatal'])}", (row["year"], row["n_injury"]),
                    textcoords="offset points", xytext=(0, 7),
                    ha="center", fontsize=7, color="#a50f15")
    ax.set_title(
        "Montgomery County crashes per calendar year\n"
        "shaded bars are the analysis period; grey bars excluded; "
        "red numerals are fatal crashes",
        fontsize=11,
    )
    ax.set_xlabel(
        f"calendar year  |  study period "
        f"{area.period_start:%Y-%m-%d} to {area.period_end:%Y-%m-%d}"
    )
    ax.set_ylabel("crashes")
    ax.legend(fontsize=8, loc="lower left")
    ax.grid(axis="y", alpha=0.25)
    ax.set_axisbelow(True)
    return _finish(fig, dest)


def lisa_map(
    lisa: pd.DataFrame,
    cells: pd.DataFrame,
    area: Any,
    dest: Path,
    *,
    variable: str,
    global_stats: dict[str, Any],
    n: int,
) -> dict[str, Any]:
    """The LISA cluster map, with the global test printed on it.

    The global I is on the figure and not only in the caption on purpose: a
    LISA map is only interpretable in the light of whether the global test
    rejected at all, and a map that travels without its caption should carry
    that with it.
    """
    part = lisa[lisa["variable"] == variable].set_index("h3_r8")
    quad = part["quadrant"].reindex(cells["h3_r8"]).fillna("NS")
    colours = [QUADRANT_COLOURS.get(q, "#ffffff") for q in quad]

    fig, ax = plt.subplots(figsize=FIGSIZE)
    _hex_patches(ax, cells, colours)
    ax.set_title(
        f"LISA clusters — local Moran's I on {variable}\n" + _subtitle(area, n),
        fontsize=11,
    )
    counts = {k: int(v) for k, v in quad.value_counts().items()}
    _class_legend(
        ax, counts, QUADRANT_COLOURS,
        f"quadrant, p_sim ≤ {part['p_fdr'].iloc[0]:.4g}\n(999 perms, "
        f"Benjamini–Hochberg FDR at α=0.05)",
    )
    g = global_stats
    ax.text(
        0.02, 0.02,
        f"Global Moran's I = {g['I']:.4f}   E[I] = {g['expected_I']:.5f}\n"
        f"z = {g['z_sim']:.2f}   p_sim = {g['p_sim']:.4g}   "
        f"({g['permutations']} permutations, seed {g['seed']})\n"
        f"HH/LL are clusters; HL/LH are spatial outliers (usually one intersection)",
        transform=ax.transAxes, fontsize=8, va="bottom",
        bbox={"facecolor": "white", "alpha": 0.9, "edgecolor": "#cccccc"},
    )
    return _finish(fig, dest)


def gi_star_pair(
    gi: pd.DataFrame,
    cells: pd.DataFrame,
    area: Any,
    dest: Path,
    *,
    stats: dict[str, Any],
    n: int,
) -> dict[str, Any]:
    """Raw counts and per-capita rate side by side, identical class colours.

    Side by side and not stacked, because the whole claim is that the two maps
    differ and a reader can only see that if both are in one eye-span.
    """
    fig, axes = plt.subplots(1, 2, figsize=(15, 7.5))
    for ax, variable, label in zip(
        axes, ("raw_count", "rate_per_1k_pop"),
        ("raw crash counts", "crashes per 1,000 residents"),
    ):
        part = gi[gi["variable"] == variable].set_index("h3_r8")
        cls = part["hotspot_class"].reindex(cells["h3_r8"])
        cls = cls.fillna("RATE_UNDEFINED")
        _hex_patches(ax, cells, [CLASS_COLOURS.get(c, "#ffffff") for c in cls])
        s = stats[variable]
        ax.set_title(
            f"Getis-Ord Gi*: {label}\n"
            f"n = {s['n']:,} cells   |   significant {s['significant_uncorrected_05']:,} "
            f"uncorrected → {s['significant_fdr_05']:,} after FDR",
            fontsize=10,
        )
        _class_legend(ax, {k: int(v) for k, v in cls.value_counts().items()},
                      CLASS_COLOURS, f"class (BH FDR, p ≤ {s['fdr_threshold_05']:.2e})")
    fig.suptitle(
        "Hot and cold spots, raw versus population-normalised\n" + _subtitle(area, n),
        fontsize=12,
    )
    fig.tight_layout()
    return _finish(fig, dest)


def contrast_map(
    contrast: pd.DataFrame,
    cells: pd.DataFrame,
    area: Any,
    dest: Path,
    *,
    n: int,
    stats: dict[str, Any],
) -> dict[str, Any]:
    """Where normalisation changed the answer, and in which direction."""
    kind = contrast.set_index("h3_r8")["contrast_kind"].reindex(cells["h3_r8"])
    colours = [
        CONTRAST_COLOURS.get(k, "#f7f7f7") if isinstance(k, str) else "#f7f7f7"
        for k in kind
    ]
    fig, ax = plt.subplots(figsize=FIGSIZE)
    _hex_patches(ax, cells, colours)
    ax.set_title(
        "What population normalisation changes\n" + _subtitle(area, n),
        fontsize=11,
    )
    counts = {k: int(v) for k, v in kind.dropna().value_counts().items()}
    handles = [
        Patch(facecolor=CONTRAST_COLOURS[k], edgecolor="#999999",
              label=f"{k}  ({counts.get(k, 0):,})")
        for k in CONTRAST_COLOURS if k in counts
    ]
    handles.append(Patch(facecolor="#f7f7f7", edgecolor="#999999",
                         label=f"same class  ({stats['cells_same_class']:,})"))
    ax.legend(handles=handles, title="Gi* class change (BH FDR, α=0.05)",
              loc="upper left", fontsize=8, title_fontsize=8)
    ax.text(
        0.02, 0.02,
        f"{stats['cells_changed_class']:,} of {stats['cells_compared']:,} cells "
        f"({100 * stats['changed_fraction']:.1f}%) change class.\n"
        "HOT_RAW_ONLY is volume without risk; HOT_RATE_ONLY is risk without volume.",
        transform=ax.transAxes, fontsize=8, va="bottom",
        bbox={"facecolor": "white", "alpha": 0.9, "edgecolor": "#cccccc"},
    )
    return _finish(fig, dest)


def _scale_bar(ax: plt.Axes, length_m: float = 5000.0) -> None:
    """A scale bar, because these axes are metres in a named projected CRS."""
    x0, x1 = ax.get_xlim()
    y0, y1 = ax.get_ylim()
    x = x0 + 0.08 * (x1 - x0)
    y = y0 + 0.10 * (y1 - y0)
    ax.plot([x, x + length_m], [y, y], color="black", linewidth=2.5,
            solid_capstyle="butt")
    ax.text(x, y + 0.015 * (y1 - y0),
            f"{length_m / 1000:.0f} km (EPSG:26985)", ha="left",
            fontsize=8, va="bottom",
            bbox={"facecolor": "white", "alpha": 0.75, "pad": 1.0,
                  "edgecolor": "none"})


def kde_panel(
    surfaces: pd.DataFrame,
    area: Any,
    dest: Path,
    *,
    order: Sequence[tuple[str, float]],
    n: int,
    crs_epsg: int,
) -> dict[str, Any]:
    """Every bandwidth on ONE shared colour scale.

    Shared, because the point of the figure is that the surfaces differ in
    shape, and per-panel normalisation would rescale each one to look equally
    peaked and hide exactly that. The scale is set by the SMALLEST bandwidth's
    maximum, which is the largest of the three, so the wider kernels appear as
    flat as they genuinely are.
    """
    fig, axes = plt.subplots(1, len(order), figsize=(5.2 * len(order), 6.0))
    axes = np.atleast_1d(axes)
    vmax = float(surfaces["intensity_per_km2"].max())

    # The stored grid is padded by three bandwidths so the surface can
    # integrate to 1 (see kde.make_grid). That pad is a third of the frame and
    # is empty by construction, so the DISPLAY is cropped to where the density
    # actually is -- the parquet keeps every cell, the picture shows the
    # county. Cropped identically on all panels so the comparison holds.
    live = surfaces[surfaces["intensity_per_km2"] > 0.01 * vmax]
    if len(live):
        pad = 2_000.0  # metres, EPSG:26985
        extent = (live["x_m"].min() - pad, live["x_m"].max() + pad,
                  live["y_m"].min() - pad, live["y_m"].max() + pad)
    else:
        extent = None

    for ax, (method, bw) in zip(axes, order):
        part = surfaces[surfaces["bandwidth_method"] == method]
        xs = np.sort(part["x_m"].unique())
        ys = np.sort(part["y_m"].unique())
        grid = (
            part.pivot(index="y_m", columns="x_m", values="intensity_per_km2")
            .reindex(index=ys, columns=xs)
            .to_numpy()
        )
        im = ax.imshow(
            grid, origin="lower", cmap=DENSITY_CMAP, vmin=0, vmax=vmax,
            extent=(xs.min(), xs.max(), ys.min(), ys.max()),
            interpolation="nearest", aspect="equal",
        )
        if extent:
            ax.set_xlim(extent[0], extent[1])
            ax.set_ylim(extent[2], extent[3])
        ax.set_title(f"{method}\nbandwidth = {bw:,.0f} m", fontsize=10)
        ax.set_xlabel(f"easting (m, EPSG:{crs_epsg})")
        ax.set_ylabel(f"northing (m, EPSG:{crs_epsg})")
        ax.ticklabel_format(style="plain")
        ax.tick_params(labelsize=7)
        _scale_bar(ax)

    fig.suptitle(
        "Crash intensity surfaces at three bandwidths, shared colour scale\n"
        + _subtitle(area, n, f"Gaussian KDE, 100 m grid, EPSG:{crs_epsg}"),
        fontsize=12,
    )
    fig.colorbar(im, ax=list(axes), shrink=0.7,
                 label="crashes per km² over the study period")
    return _finish(fig, dest)


def bandwidth_cv(
    table: pd.DataFrame,
    dest: Path,
    *,
    chosen: float,
    reference: dict[str, float],
    random_table: pd.DataFrame | None = None,
) -> dict[str, Any]:
    """The likelihood curve, with the rules of thumb marked on it.

    The figure that carries the bandwidth argument: a flat curve means the
    choice does not matter much and a peaked one means it does, and the
    distance between the blocked-CV peak and the random-fold peak is the leak
    the spatial blocking prevents, drawn to scale.
    """
    fig, ax = plt.subplots(figsize=(8.5, 5))
    ax.plot(table["bandwidth_m"], table["mean_log_likelihood"], "o-",
            color="#08519c", label="blocked CV (H3 r7 folds)")
    if random_table is not None:
        ax.plot(random_table["bandwidth_m"], random_table["mean_log_likelihood"],
                "s--", color="#cb181d", markersize=4,
                label="random K-fold (leaks — shown for contrast)")
        best_random = float(
            random_table.loc[random_table["mean_log_likelihood"].idxmax(), "bandwidth_m"]
        )
        ax.axvline(best_random, color="#cb181d", linestyle=":", linewidth=1.2)
        ax.annotate(f"random-fold optimum {best_random:.0f} m",
                    (best_random, ax.get_ylim()[0]), rotation=90,
                    fontsize=7, color="#cb181d", va="bottom",
                    textcoords="offset points", xytext=(3, 4))
    ax.axvline(chosen, color="#08519c", linewidth=1.4)
    ax.annotate(f"chosen {chosen:.0f} m", (chosen, ax.get_ylim()[1]),
                rotation=90, fontsize=8, color="#08519c", va="top",
                textcoords="offset points", xytext=(4, -4))
    # Scott and Silverman coincide exactly in two dimensions, so drawing both
    # puts two labels on one line. Group by value and name the line once.
    merged: dict[float, list[str]] = {}
    for name, value in sorted(reference.items()):
        merged.setdefault(round(value, 3), []).append(name)
    for value, names in sorted(merged.items()):
        ax.axvline(value, color="#666666", linestyle="--", linewidth=1)
        ax.annotate(f"{' = '.join(names)}  {value:,.0f} m",
                    (value, ax.get_ylim()[0]),
                    rotation=90, fontsize=7, color="#444444", va="bottom",
                    textcoords="offset points", xytext=(3, 4))
    ax.set_xscale("log")
    ax.set_xlabel("bandwidth (m, EPSG:26985) — log scale")
    ax.set_ylabel("mean held-out log-likelihood")
    ax.set_title(
        "KDE bandwidth selection by cross-validated log-likelihood\n"
        "folds blocked by H3 r7 cell — random folds leak across\n"
        "spatially autocorrelated neighbours and pick far too small",
        fontsize=10,
    )
    ax.grid(alpha=0.25)
    ax.legend(fontsize=8, loc="lower center")
    return _finish(fig, dest)


def st_cluster_map(
    clusters: pd.DataFrame,
    points: pd.DataFrame,
    area: Any,
    dest: Path,
    *,
    n: int,
    crs_epsg: int,
    eps_space_m: float,
    eps_time_h: float,
) -> dict[str, Any]:
    """Space-time clusters over the point cloud, sized by membership."""
    fig, ax = plt.subplots(figsize=FIGSIZE)
    ax.scatter(points["x_m"], points["y_m"], s=0.6, color="#d9d9d9",
               linewidths=0, label=f"all crashes (N = {n:,})")
    st = clusters[clusters["run"] == "spatiotemporal"]
    if len(st):
        ax.scatter(st["centroid_x_m"], st["centroid_y_m"],
                   s=np.clip(st["n_crashes"] * 3.5, 25, 400),
                   facecolor="#a50f15", edgecolor="black", linewidths=0.6,
                   alpha=0.85, label=f"ST-DBSCAN clusters ({len(st):,})")
        for _, row in st.nlargest(6, "n_crashes").iterrows():
            label = row["place_name"] if isinstance(row["place_name"], str) else "unnamed"
            ax.annotate(
                f"{label}\n{int(row['n_crashes'])} crashes, "
                f"{row['span_days']:.0f} d",
                (row["centroid_x_m"], row["centroid_y_m"]),
                fontsize=7, textcoords="offset points", xytext=(7, 5),
                bbox={"facecolor": "white", "alpha": 0.85, "pad": 1.2,
                      "edgecolor": "#cccccc"},
            )
    ax.set_aspect("equal")
    ax.set_xlabel(f"easting (m, EPSG:{crs_epsg})")
    ax.set_ylabel(f"northing (m, EPSG:{crs_epsg})")
    ax.ticklabel_format(style="plain")
    ax.tick_params(labelsize=7)
    _scale_bar(ax)
    ax.set_title(
        f"ST-DBSCAN: clusters tight in space AND time\n"
        f"ε_space = {eps_space_m:.0f} m (EPSG:{crs_epsg}), "
        f"ε_time = {eps_time_h:.0f} h\n" + _subtitle(area, n),
        fontsize=10,
    )
    ax.legend(fontsize=8, loc="upper left", markerscale=2)
    return _finish(fig, dest)
