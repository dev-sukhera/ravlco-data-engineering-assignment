"""Gold + crash_geo in, the spatial-analysis tables, figures and manifest out.

    python -m src.analysis.build
    python -m src.analysis.build --period 2015-01-01:2025-12-31   # sensitivity
    python -m src.analysis.build --skip-kde --skip-st-dbscan      # fast loop
    python -m src.analysis.build --json

Stage order, and which parts of it are a correctness constraint
---------------------------------------------------------------
    corpus -> cell universe -> apportionment -> cell stats
           -> weights -> {LISA, Gi*} -> contrast
           -> KDE -> ST-DBSCAN -> validate -> write -> figures

The first four are strictly ordered: the cell statistics cannot exist before
the universe they are computed over, and the universe is the FILLED county
rather than the cells that had crashes (see frames.py -- a hot-spot test over
only the non-zero cells conditions on the outcome). The weights matrix must be
built from that same universe, sorted, or the local statistics are computed on
a relation that is not adjacency.

LISA and Gi* are independent of each other and both depend on the cell stats;
the contrast depends on Gi*. KDE touches none of them -- it is a point-pattern
method and shares only the corpus, which is exactly why it goes through
`frames.load_crashes` rather than reading crash_geo itself.

**Validation happens before the first write**, as in Phases 2-4: a contract
failure must leave the previous outputs exactly as they were, because a
half-replaced output directory is worse than a stale one -- the stale one is
at least internally consistent.

Determinism
-----------
Two runs over unchanged inputs produce byte-identical parquet. Every
permutation test is seeded from `[analysis] seed`; the weights matrix is built
from a sorted id list with an explicit `id_order`; every table is written by
`common.write_parquet`, which refuses a non-total sort order rather than
producing bytes that depend on thread scheduling.

`_analysis_build_sha` is a hash of the INPUTS -- crash_geo's sha256,
dim_block_group's, fact_crash's, and the `[analysis]` config block -- never of
the build time. It moves exactly when something that could change an answer
changes. Wall-clock time appears only in `_analysis_manifest.json`, and the
figures are not part of the identity claim (matplotlib embeds a version
string); the parquet table behind each figure is.

What a one-row change to crash_geo does
---------------------------------------
It changes that cell's `cell_stats` row. In the LOCAL statistics it can only
change that cell and its k=1 neighbourhood, because a local statistic is a
function of the cell and its neighbours alone. The GLOBAL Moran's I and the
FDR threshold are functions of the whole map and do move -- and because the
FDR threshold moves, a cell far away can cross it. That is a property of
multiple-testing correction, not a bug, and the test asserts the local-input
claim rather than a whole-table one.
"""

from __future__ import annotations

import argparse
import dataclasses
import hashlib
import json
import logging
import sys
from dataclasses import dataclass, field
from datetime import date, datetime, timezone
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd
import pyarrow as pa

from .. import config, contracts
from ..config import GOLD_DIR, REPO_ROOT
from ..geo import reference as geo_reference
from ..ingest.watermark import durable_replace
from ..transform import common as c
from . import figures, frames, hotspots, kde as kde_mod, lisa as lisa_mod
from . import st_dbscan as st_mod
from . import weights as weights_mod

log = logging.getLogger("analysis.build")

ANALYSIS_CONTRACT = contracts.CONTRACTS_DIR / "analysis.schema.json"
DEFAULT_OUT_SUBDIR = "analysis"
DEFAULT_FIGURE_DIR = REPO_ROOT / "output" / "figures"

RAW = "raw_count"
RATE = "rate_per_1k_pop"

# table name -> (contract table, column list, total sort order)
TABLE_SPECS: dict[str, tuple[str, list[str], list[str]]] = {
    "cell_stats_h3_r8": (
        "analysis.cell_stats_h3_r8", frames.CELL_STATS_COLUMNS, ["h3_r8"],
    ),
    "lisa_h3_r8": (
        "analysis.lisa_h3_r8", lisa_mod.LISA_COLUMNS, ["h3_r8", "variable"],
    ),
    "gi_star_h3_r8": (
        "analysis.gi_star_h3_r8", hotspots.GI_COLUMNS, ["h3_r8", "variable"],
    ),
    "hotspot_contrast": (
        "analysis.hotspot_contrast", hotspots.CONTRAST_COLUMNS, ["h3_r8"],
    ),
    "kde_surface": (
        "analysis.kde_surface", kde_mod.SURFACE_COLUMNS,
        ["bandwidth_method", "x_m", "y_m"],
    ),
    "crash_year_counts": (
        "analysis.crash_year_counts",
        ["year", "n_crashes", "n_injury", "n_fatal", "in_study_period"],
        ["year"],
    ),
    "st_clusters": (
        "analysis.st_clusters", st_mod.CLUSTER_COLUMNS, ["run", "cluster_id"],
    ),
}


# ---------------------------------------------------------------------------
# manifest
# ---------------------------------------------------------------------------


@dataclass
class AnalysisManifest:
    """Inputs by hash, every statistic, outputs by hash. Only wall clock here.

    Same shape as `_geo_manifest.json`, and the same rule: every number that
    appears in ANALYSIS.md must be readable out of this file, so a reviewer can
    check a claim without re-running anything.
    """

    out_root: Path
    gold_root: Path
    built_at: str = field(default_factory=lambda: datetime.now(timezone.utc).isoformat())
    inputs: dict[str, Any] = field(default_factory=dict)
    outputs: dict[str, dict[str, Any]] = field(default_factory=dict)
    figures: dict[str, Any] = field(default_factory=dict)
    stats: dict[str, Any] = field(default_factory=dict)
    warnings: list[str] = field(default_factory=list)

    def warn(self, message: str) -> None:
        log.warning("%s", message)
        self.warnings.append(message)

    def write(self) -> Path:
        dest = self.out_root / "_analysis_manifest.json"
        dest.parent.mkdir(parents=True, exist_ok=True)
        payload = {
            "built_at": self.built_at,
            "gold_root": str(self.gold_root),
            "out_root": str(self.out_root),
            "config": {"analysis": _jsonable(config.geo()["analysis"])},
            "inputs": dict(sorted(self.inputs.items())),
            "outputs": dict(sorted(self.outputs.items())),
            "figures": dict(sorted(self.figures.items())),
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
    if isinstance(value, (np.integer,)):
        return int(value)
    if isinstance(value, (np.floating,)):
        return float(value)
    if isinstance(value, (np.bool_,)):
        return bool(value)
    if isinstance(value, (str, int, float, bool)) or value is None:
        return value
    return str(value)


def input_hashes(gold_root: Path) -> dict[str, str]:
    """sha256 of every gold input this phase reads. Named, not globbed."""
    out: dict[str, str] = {}
    for name in ("crash_geo", "dim_block_group", "fact_crash"):
        path = gold_root / f"{name}.parquet"
        if path.exists():
            out[name] = hashlib.sha256(path.read_bytes()).hexdigest()
    return out


def build_sha(hashes: dict[str, str], area: frames.StudyArea) -> str:
    """A hash of the INPUTS plus the analysis config and the effective period.

    Not of the build time and not of the output. The period is folded in
    explicitly (rather than only via the config block) because `--period`
    overrides config, and a sensitivity run that produced different numbers
    under the same lineage sha would make the sha a decoration.
    """
    h = hashlib.sha256()
    for name, sha in sorted(hashes.items()):
        h.update(f"{name}={sha}".encode())
    h.update(
        json.dumps(_jsonable(config.geo()["analysis"]), sort_keys=True,
                   default=str).encode()
    )
    h.update(f"period={area.period}|county={area.county_geoid}".encode())
    return h.hexdigest()


def geo_build_sha(con: Any, gold_root: Path) -> str:
    """The `_geo_build_sha` crash_geo was written with, for lineage.

    Read from the COLUMN, not from `_geo_manifest.json`. Phase 4 computes the
    sha and stamps it on every crash_geo row but does not put it in its own
    manifest, so the column is the only authoritative copy -- and it is also
    the right one: it is the value that actually travelled with the rows this
    build consumed, rather than the value some manifest says the last geo build
    produced. `DISTINCT` because a crash_geo written by two different geo
    builds would be a broken input and should be visible as one.
    """
    path = gold_root / "crash_geo.parquet"
    if not path.exists():
        return ""
    p = str(path).replace("'", "''")
    rows = con.execute(
        f"SELECT DISTINCT _geo_build_sha FROM read_parquet('{p}') "
        f"WHERE _geo_build_sha IS NOT NULL ORDER BY 1"
    ).fetchall()
    if len(rows) > 1:
        raise ValueError(
            f"crash_geo carries {len(rows)} distinct _geo_build_sha values "
            f"({[r[0][:12] for r in rows]}) -- it was assembled from more than "
            "one geo build and its lineage is not a single fact"
        )
    return str(rows[0][0]) if rows else ""


# ---------------------------------------------------------------------------
# the variables the statistics run on
# ---------------------------------------------------------------------------


def analysis_variables(
    cells: pd.DataFrame,
) -> tuple[dict[str, dict[str, float]], dict[str, list[str]]]:
    """`{variable: {cell: value}}` and the cell universe each one lives on.

    Two universes, and the difference is the honest handling of a cell with no
    denominator. `raw_count` runs over every cell in the filled county. The
    normalised rate runs only over cells where `rate_is_defined` -- filling the
    others with a zero rate would assert "no crash risk at the interchange",
    which is both false and exactly backwards, and dropping them silently would
    hide the cells the contrast is about.
    """
    raw_cells = cells["h3_r8"].tolist()
    rate_rows = cells[cells["rate_is_defined"]]
    values = {
        RAW: dict(zip(cells["h3_r8"], cells["n_crashes"].astype("float64"))),
        RATE: dict(zip(rate_rows["h3_r8"], rate_rows["rate_per_1k_pop"].astype("float64"))),
    }
    universes = {RAW: raw_cells, RATE: rate_rows["h3_r8"].tolist()}
    return values, universes


# ---------------------------------------------------------------------------
# the build
# ---------------------------------------------------------------------------


def build_analysis(
    *,
    gold_root: Path | str | None = None,
    out_root: Path | str | None = None,
    reference_root: Path | str | None = None,
    figure_dir: Path | str | None = None,
    period: str | None = None,
    sensitivity: bool = False,
    seed: int | None = None,
    permutations: int | None = None,
    skip_kde: bool = False,
    skip_st_dbscan: bool = False,
    skip_figures: bool = False,
    validate: bool = True,
    small_corpus: bool = False,
    threads: int | None = None,
) -> dict[str, Any]:
    """Run every Phase 5 analysis and write the tables, figures and manifest."""
    gold = Path(gold_root) if gold_root else GOLD_DIR
    out = Path(out_root) if out_root else (gold / DEFAULT_OUT_SUBDIR)
    figs = Path(figure_dir) if figure_dir else DEFAULT_FIGURE_DIR
    ref_root = Path(reference_root) if reference_root else geo_reference.reference_root()

    cfg = config.geo()["analysis"]
    area = frames.study_area_from_config(period=period, sensitivity=sensitivity)
    the_seed = int(seed if seed is not None else cfg["seed"])
    # Overridable so the test suite can run the whole pipeline end to end
    # without paying the production permutation budget. It is NOT a tuning
    # knob for a published result: below ~n/alpha the FDR correction cannot
    # reject anything (see config/geo.toml [analysis] permutations), and the
    # build warns when the run it just did was resolution-limited.
    permutations = int(permutations if permutations is not None
                       else cfg["permutations"])
    alpha = float(cfg["alpha"])
    use_fdr = bool(cfg["fdr"])

    manifest = AnalysisManifest(out_root=out, gold_root=gold)
    con = frames.connect(threads)
    store = geo_reference.ReferenceStore(ref_root, offline=True)

    hashes = input_hashes(gold)
    analysis_sha = build_sha(hashes, area)
    geo_sha = geo_build_sha(con, gold)
    manifest.inputs = {
        **{f"{k}_sha256": v for k, v in hashes.items()},
        "analysis_build_sha": analysis_sha,
        "geo_build_sha": geo_sha,
        "study_area": area.county_geoid,
        "study_area_label": area.label,
        "period": area.period,
        "seed": the_seed,
        "permutations": permutations,
        "alpha": alpha,
        "fdr": use_fdr,
    }

    # -- the corpus ------------------------------------------------------
    log.info("study corpus")
    crashes, ledger = frames.load_crashes(con, gold, area)
    manifest.stats["exclusions"] = ledger.as_dict()
    if crashes.empty:
        raise ValueError(
            f"no crashes in {area.name} over {area.period} -- check "
            "config/geo.toml [analysis] against the corpus that is actually built"
        )

    # The per-year table covers EVERY year the corpus has, not just the
    # analysis period: the pandemic drop and the partial final year are the
    # evidence for the period choice, so they have to be visible in it.
    all_years, _ = frames.load_crashes(
        con, gold,
        dataclasses.replace(area, period_start=date(1900, 1, 1),
                            period_end=date(2999, 12, 31)),
    )
    years = frames.crashes_by_year(all_years)
    years["in_study_period"] = (
        (years["year"] >= area.period_start.year) & (years["year"] <= area.period_end.year)
    )
    manifest.stats["crashes_by_year"] = years.to_dict("records")

    # -- the cell universe -----------------------------------------------
    log.info("filling the study-area polygon with r%d cells", area.resolution)
    counties = frames.load_county_layer(store)
    polygon = frames.county_polygon(counties, area.county_geoid)
    cell_ids, interior, fill_stats = frames.fill_cells(polygon, area.resolution)
    manifest.stats["cell_fill"] = fill_stats

    # -- population apportionment ----------------------------------------
    log.info("apportioning block-group population onto cells (EPSG:5070)")
    bg_layer = frames.load_block_group_layer(store, area.county_geoid[:2])
    bg_layer = bg_layer[bg_layer["GEOID"].str.startswith(area.county_geoid)]
    populations = frames.load_block_group_population(con, gold, area.county_geoid)
    allocated, apportion_stats = frames.apportion_population(
        cell_ids, bg_layer, populations
    )
    # Mass preservation is asserted on every build, not only in the test
    # suite: the whole normalised analysis is a division by this number.
    rel = abs(
        apportion_stats["population_allocated"]
        - apportion_stats["population_source_total"]
    ) / max(apportion_stats["population_source_total"], 1.0)
    if rel > 1e-6:
        raise AssertionError(
            f"apportionment is not mass-preserving (relative error {rel:.3e})"
        )
    apportion_stats["relative_error"] = rel
    manifest.stats["apportionment"] = apportion_stats

    # -- cell statistics --------------------------------------------------
    neighbors = weights_mod.neighbor_map(cell_ids, k=area.neighbor_k)
    cells, cell_stat_stats = frames.cell_stats(
        crashes, cell_ids, allocated, neighbors, area,
        interior_cells=interior,
        analysis_build_sha=analysis_sha, geo_build_sha=geo_sha,
    )
    manifest.stats["cell_stats"] = cell_stat_stats

    values, universes = analysis_variables(cells)

    # -- weights ----------------------------------------------------------
    log.info("spatial weights (k=%d hex ring)", area.neighbor_k)
    w_row: dict[str, Any] = {}
    w_bin: dict[str, Any] = {}
    weight_stats: dict[str, Any] = {}
    for name, universe in universes.items():
        # Row-standardised for Moran's I (the lag is a neighbourhood MEAN),
        # binary for Gi* (the statistic is a neighbourhood SUM). See
        # weights.py for why each is the only correct choice for its statistic.
        w_row[name], rstats = weights_mod.build(
            universe, k=area.neighbor_k, transform=weights_mod.ROW_STANDARDISED
        )
        w_bin[name], bstats = weights_mod.build(
            universe, k=area.neighbor_k, transform=weights_mod.BINARY
        )
        weight_stats[name] = {"row_standardised": rstats, "binary": bstats}
        if rstats["n_islands"]:
            manifest.warn(
                f"weights[{name}]: {rstats['n_islands']} island cell(s) with no "
                f"neighbour in the universe -- their local statistics are "
                f"undefined and esda gives them the island weight"
            )
    manifest.stats["weights"] = weight_stats

    # -- Moran's I / LISA --------------------------------------------------
    log.info("Moran's I and LISA")
    lisa_frames = []
    lisa_stats: dict[str, Any] = {}
    for name in sorted(values):
        table, s = lisa_mod.run(
            {name: values[name]}, w_row[name],
            permutations=permutations, seed=the_seed, alpha=alpha, use_fdr=use_fdr,
        )
        lisa_frames.append(table)
        lisa_stats.update(s)
    lisa_table = pd.concat(lisa_frames, ignore_index=True)
    manifest.stats["lisa"] = lisa_stats
    manifest.stats["clustering_exists"] = {
        name: lisa_mod.clusters_exist(lisa_stats, name, alpha) for name in lisa_stats
    }
    for name, exists in manifest.stats["clustering_exists"].items():
        if not exists:
            # The gate the prose has to pass. Recorded as a warning so it is
            # impossible to write "these are clusters" over a map whose global
            # test did not reject.
            manifest.warn(
                f"global Moran's I on {name} does not reject the null "
                f"(I={lisa_stats[name]['global']['I']:.4f}, "
                f"p_sim={lisa_stats[name]['global']['p_sim']:.4g}) -- the local "
                "map is a multiple-comparisons picture, not a set of clusters"
            )

    # -- Getis-Ord Gi* -----------------------------------------------------
    log.info("Getis-Ord Gi*")
    gi_table, gi_stats = hotspots.run(
        values, w_bin, permutations=permutations, seed=the_seed,
        alpha=alpha, use_fdr=use_fdr,
    )
    manifest.stats["gi_star"] = gi_stats
    # An empty corrected map means one of two very different things, and only
    # the diagnostic can tell them apart: the data had no hot spots, or the
    # permutation budget ran out of resolution before the correction could
    # reject anything. The second is a false negative and must never be
    # published as the first.
    for name, s_ in gi_stats.items():
        d = s_["correction_05"]
        if d["resolution_limited"]:
            manifest.warn(
                f"Gi*[{name}]: {permutations} permutations put the p-value "
                f"floor at {d['p_value_floor']:.2e}, above the rank-1 BH "
                f"critical value {d['bonferroni_threshold']:.2e} -- the "
                f"correction CANNOT reject an isolated cell at this budget. "
                f"Raise [analysis] permutations to at least "
                f"{d['permutations_required_for_rank_one']:,}."
            )
        elif d["bh_rejections"] == 0:
            manifest.warn(
                f"Gi*[{name}]: Benjamini-Hochberg rejects nothing at "
                f"alpha={alpha}; esda.fdr returned the Bonferroni bound "
                f"{d['bonferroni_threshold']:.2e}. There are no "
                f"FDR-significant cells for this variable."
            )

    contrast_table, contrast_stats = hotspots.contrast(
        gi_table, cells, crashes, raw_variable=RAW, rate_variable=RATE
    )
    manifest.stats["contrast"] = contrast_stats
    manifest.stats["top_hot_cells"] = {
        name: hotspots.top_cells(gi_table, cells, crashes, variable=name, n=10)
        .assign(gi_z=lambda d: d["gi_z"].round(3))
        [["h3_r8", "gi_z", "hotspot_class", "n_crashes", "n_injury", "n_fatal",
          "population", "rate_per_1k_pop", "place_name", "place_highway"]]
        .to_dict("records")
        for name in sorted(values)
    }

    # -- KDE ---------------------------------------------------------------
    tables: dict[str, pd.DataFrame] = {
        "cell_stats_h3_r8": cells,
        "lisa_h3_r8": lisa_table,
        "gi_star_h3_r8": gi_table,
        "hotspot_contrast": contrast_table,
        "crash_year_counts": years[
            ["year", "n_crashes", "n_injury", "n_fatal", "in_study_period"]
        ],
    }
    points = frames.projected_points(crashes)
    kde_bits: dict[str, Any] = {}
    if skip_kde:
        manifest.stats["kde"] = {"skipped": True}
    else:
        kde_bits = _run_kde(points, area, cfg, the_seed, manifest)
        tables["kde_surface"] = kde_bits["surfaces"]

    # -- ST-DBSCAN (the optional fourth) -----------------------------------
    if skip_st_dbscan:
        manifest.stats["st_dbscan"] = {"skipped": True}
        tables["st_clusters"] = st_mod.empty_clusters()
    else:
        st_table, st_stats = st_mod.run(points, cfg["st_dbscan"], crs_epsg=int(
            config.geo()["analysis"]["crs"]["local"]
        ))
        manifest.stats["st_dbscan"] = st_stats
        tables["st_clusters"] = st_table

    # -- validate, then write ---------------------------------------------
    if validate:
        _validate(con, tables, small_corpus=small_corpus)

    out.mkdir(parents=True, exist_ok=True)
    for name, frame in tables.items():
        contract_name, columns, order = TABLE_SPECS[name]
        _register(con, f"{name}_out", frame, columns)
        manifest.outputs[name] = c.write_parquet(
            con, f"{name}_out", out / f"{name}.parquet",
            columns=columns, order_by=order,
        )

    # -- figures -----------------------------------------------------------
    if not skip_figures:
        manifest.figures = _draw(
            figs, area, cells, lisa_table, gi_table, contrast_table,
            years, lisa_stats, gi_stats, contrast_stats, kde_bits, points,
            tables.get("st_clusters"), cfg, n=len(crashes),
        )

    manifest.write()
    con.close()
    return {
        "outputs": manifest.outputs,
        "figures": manifest.figures,
        "stats": manifest.stats,
        "warnings": manifest.warnings,
        "out_root": str(out),
        "gold_root": str(gold),
    }


def _run_kde(
    points: Any, area: frames.StudyArea, cfg: dict[str, Any], seed: int,
    manifest: AnalysisManifest,
) -> dict[str, Any]:
    """Bandwidth selection and the three surfaces. All metres in EPSG:26985."""
    kcfg = cfg["kde"]
    crs_epsg = int(cfg["crs"]["local"])
    xy = np.column_stack([points["x_m"].to_numpy(), points["y_m"].to_numpy()])
    blocks = points["h3_r7"].astype(str).to_numpy()

    candidates = kde_mod.candidate_bandwidths(
        float(kcfg["bandwidth_min_m"]), float(kcfg["bandwidth_max_m"]),
        int(kcfg["bandwidth_candidates"]),
    )
    # The CV sweep is quadratic in the corpus size and the selected bandwidth
    # is stable well below the full N; the final surfaces use every point. The
    # sample is drawn with the build seed, so it is reproducible.
    sample_n = min(int(kcfg["cv_sample_size"]), len(xy))
    rng = np.random.default_rng(seed)
    idx = np.sort(rng.choice(len(xy), size=sample_n, replace=False))
    cv_xy, cv_blocks = xy[idx], blocks[idx]

    best, cv_table, cv_stats = kde_mod.select_bandwidth(
        cv_xy, cv_blocks, candidates=candidates, folds=int(kcfg["cv_folds"]),
        seed=seed, blocked=True,
    )
    # The same sweep with random folds, for the contrast the report quotes.
    # Not used to choose anything -- it is the demonstration of the leak.
    random_best, random_table, random_stats = kde_mod.select_bandwidth(
        cv_xy, None, candidates=candidates, folds=int(kcfg["cv_folds"]),
        seed=seed, blocked=False,
    )

    scott = kde_mod.scott_bandwidth(xy)
    silverman = kde_mod.silverman_bandwidth(xy)
    practitioner = float(kcfg["practitioner_bandwidth_m"])

    chosen = {
        "cv_blocked": best,
        "scott": scott,
        "silverman": silverman,
        "practitioner": practitioner,
    }
    pitch = float(kcfg["grid_m"])
    xs, ys = kde_mod.make_grid(xy, pitch_m=pitch, pad_m=3.0 * max(chosen.values()))

    # One evaluation per DISTINCT bandwidth. Scott and Silverman coincide
    # exactly in two dimensions -- Silverman's (n(d+2)/4) factor is n at d=2,
    # collapsing it to Scott's -- so computing both would evaluate 400,000 grid
    # points against 70,692 crashes twice for identical numbers. The table
    # still carries a row block per METHOD, because "Scott and Silverman agree"
    # is something the reader should be able to see rather than be told.
    surfaces, surface_stats = [], {}
    computed: dict[float, pd.DataFrame] = {}
    computed_by: dict[float, str] = {}
    for method, bw in chosen.items():
        key = round(bw, 6)
        if key in computed:
            frame = computed[key].assign(bandwidth_method=method)
            surface_stats[method] = {**surface_stats[computed_by[key]],
                                     "method": method,
                                     "shared_with": computed_by[key]}
            surfaces.append(frame)
            continue
        frame, s = kde_mod.surface(
            xy, bandwidth_m=bw, xs=xs, ys=ys, crs_epsg=crs_epsg, method=method,
            truncate=float(kcfg["truncate_sigma"]), seed=seed,
        )
        computed[key] = frame
        computed_by[key] = method
        surfaces.append(frame)
        surface_stats[method] = s
        if not kde_mod.integrates_to_one(s):
            manifest.warn(
                f"KDE {method} (h={bw:.0f} m) integrates to {s['integral']:.4f}, "
                "not 1 -- the grid does not cover the kernel's support"
            )

    manifest.stats["kde"] = {
        "crs_epsg": crs_epsg,
        "grid_pitch_m": pitch,
        "n_points": int(len(xy)),
        "cv_sample_size": sample_n,
        "distinct_bandwidths_evaluated": len(computed),
        "bandwidths_m": chosen,
        "cv_blocked": cv_stats,
        "cv_random_folds": {**random_stats, "best_bandwidth_m": random_best},
        "cv_blocked_vs_random_ratio": round(best / random_best, 3) if random_best else None,
        "cv_table": cv_table.to_dict("records"),
        "cv_table_random_folds": random_table.to_dict("records"),
        "surfaces": surface_stats,
    }
    return {
        "surfaces": pd.concat(surfaces, ignore_index=True),
        "cv_table": cv_table,
        "cv_table_random": random_table,
        "chosen": chosen,
        "best": best,
        "crs_epsg": crs_epsg,
    }


def _draw(
    figs: Path, area: frames.StudyArea, cells: pd.DataFrame,
    lisa_table: pd.DataFrame, gi_table: pd.DataFrame, contrast_table: pd.DataFrame,
    years: pd.DataFrame, lisa_stats: dict[str, Any], gi_stats: dict[str, Any],
    contrast_stats: dict[str, Any], kde_bits: dict[str, Any], points: Any,
    st_clusters: pd.DataFrame | None, cfg: dict[str, Any], *, n: int,
) -> dict[str, Any]:
    """Every committed figure. Labels are asserted by construction in figures.py."""
    figs.mkdir(parents=True, exist_ok=True)
    out: dict[str, Any] = {}
    out["crashes_per_year"] = figures.per_year_counts(
        years, area, figs / "01_crashes_per_year.png"
    )
    for i, variable in enumerate((RATE, RAW), start=2):
        if variable not in lisa_stats:
            continue
        out[f"lisa_{variable}"] = figures.lisa_map(
            lisa_table, cells, area, figs / f"0{i}_lisa_{variable}.png",
            variable=variable, global_stats=lisa_stats[variable]["global"], n=n,
        )
    out["gi_star_pair"] = figures.gi_star_pair(
        gi_table, cells, area, figs / "04_gi_star_raw_vs_normalised.png",
        stats=gi_stats, n=n,
    )
    out["contrast"] = figures.contrast_map(
        contrast_table, cells, area, figs / "05_normalisation_contrast.png",
        n=n, stats=contrast_stats,
    )
    if kde_bits:
        order = [
            ("silverman", kde_bits["chosen"]["silverman"]),
            ("cv_blocked", kde_bits["chosen"]["cv_blocked"]),
            ("practitioner", kde_bits["chosen"]["practitioner"]),
        ]
        out["kde_panel"] = figures.kde_panel(
            kde_bits["surfaces"], area, figs / "06_kde_bandwidths.png",
            order=order, n=n, crs_epsg=kde_bits["crs_epsg"],
        )
        out["bandwidth_cv"] = figures.bandwidth_cv(
            kde_bits["cv_table"], figs / "07_bandwidth_selection.png",
            chosen=kde_bits["best"],
            reference={"Scott": kde_bits["chosen"]["scott"],
                       "Silverman": kde_bits["chosen"]["silverman"]},
            random_table=kde_bits.get("cv_table_random"),
        )
    if st_clusters is not None and len(st_clusters):
        out["st_dbscan"] = figures.st_cluster_map(
            st_clusters, points, area, figs / "08_st_dbscan_clusters.png",
            n=n, crs_epsg=int(cfg["crs"]["local"]),
            eps_space_m=float(cfg["st_dbscan"]["eps_space_m"]),
            eps_time_h=float(cfg["st_dbscan"]["eps_time_h"]),
        )
    return out


def _register(con: Any, name: str, frame: Any, columns: list[str]) -> None:
    """Register a pandas frame or a pyarrow table as `name`, in contract order.

    Both shapes appear because a legitimately EMPTY output (no contrast cells,
    ST-DBSCAN skipped) is carried as a pyarrow table with a declared schema --
    an empty pandas frame has no types for DuckDB to read and would fail the
    contract's type check for reasons unrelated to the data.
    """
    con.register(name, frame.select(columns) if isinstance(frame, pa.Table)
                 else frame[columns])


def _validate(con: Any, tables: dict[str, pd.DataFrame], *, small_corpus: bool) -> None:
    """Every table against `contracts/analysis.schema.json`, before any write.

    All violations across all tables are collected before raising, so a
    schema mistake is one fix-and-rerun rather than a game of whack-a-mole --
    the same discipline as Phases 2-4.
    """
    contract = contracts.load_contract(ANALYSIS_CONTRACT)
    resolve = {}
    violations = []
    for name, frame in tables.items():
        contract_name, columns, _ = TABLE_SPECS[name]
        _register(con, f"{name}_val", frame, columns)
        resolve[contract_name] = f"{name}_val"
    for name, frame in tables.items():
        contract_name, _, _ = TABLE_SPECS[name]
        violations += contracts.validate_relation(
            con, f"{name}_val", contract, contract_name,
            check_row_count_min=not small_corpus,
        )
        violations += contracts.validate_foreign_keys(
            con, contract, contract_name, f"{name}_val", resolve
        )
    contracts.raise_for(violations, context="analysis outputs")
    log.info("contracts: %d table(s) validated", len(tables))


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------


def _cli(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(
        prog="python -m src.analysis.build",
        description="Spatial analysis over gold + crash_geo: Moran's I / LISA, "
                    "Getis-Ord Gi* with an FDR correction, KDE, ST-DBSCAN.",
    )
    ap.add_argument("--gold-root", type=Path, default=None)
    ap.add_argument("--out-root", type=Path, default=None,
                    help="default: <gold-root>/analysis")
    ap.add_argument("--reference-root", type=Path, default=None)
    ap.add_argument("--figure-dir", type=Path, default=None,
                    help=f"default: {DEFAULT_FIGURE_DIR}")
    ap.add_argument("--study-area", default=None,
                    help="county GEOID; default from config [analysis] study_area")
    ap.add_argument("--period", default=None,
                    help="START:END, e.g. 2019-01-01:2025-12-31")
    ap.add_argument("--sensitivity", action="store_true",
                    help="use [analysis] sensitivity_period_* instead")
    ap.add_argument("--seed", type=int, default=None)
    ap.add_argument("--permutations", type=int, default=None,
                    help="override [analysis] permutations; the build warns if "
                         "the value is too small for the FDR correction to "
                         "reject anything")
    ap.add_argument("--skip-kde", action="store_true")
    ap.add_argument("--skip-st-dbscan", action="store_true")
    ap.add_argument("--skip-figures", action="store_true")
    ap.add_argument("--no-validate", action="store_true")
    ap.add_argument("--small-corpus", action="store_true",
                    help="skip row_count_min floors (fixture-scale builds)")
    ap.add_argument("--threads", type=int, default=None)
    ap.add_argument("--json", action="store_true")
    ap.add_argument("-v", "--verbose", action="store_true")
    args = ap.parse_args(argv)
    logging.basicConfig(level=logging.DEBUG if args.verbose else logging.INFO,
                        format="%(levelname)s %(name)s %(message)s")

    if args.study_area:
        # A CLI override of a config value is honoured by mutating the loaded
        # config, so the manifest's config snapshot shows what actually ran
        # rather than what the file says.
        config.geo()["analysis"]["study_area"] = args.study_area

    result = build_analysis(
        gold_root=args.gold_root, out_root=args.out_root,
        reference_root=args.reference_root, figure_dir=args.figure_dir,
        period=args.period, sensitivity=args.sensitivity, seed=args.seed,
        permutations=args.permutations,
        skip_kde=args.skip_kde, skip_st_dbscan=args.skip_st_dbscan,
        skip_figures=args.skip_figures, validate=not args.no_validate,
        small_corpus=args.small_corpus, threads=args.threads,
    )
    if args.json:
        print(json.dumps(_jsonable(result), indent=2, default=str))
    else:
        _print_summary(result)
    return 0


def _print_summary(result: dict[str, Any]) -> None:
    stats = result["stats"]
    print(f"\nanalysis -> {result['out_root']}")
    for name, info in sorted(result["outputs"].items()):
        print(f"  {name:<22} {info['rows']:>9} rows  "
              f"{info['bytes'] / 1e6:>6.2f} MB  {info['sha256'][:16]}")

    ex = stats["exclusions"]
    print(f"\n  corpus: {ex['rows_kept']:,} of {ex['rows_considered']:,} rows "
          f"({ex['excluded_total']:,} excluded)")
    for reason, n in ex["excluded_by_reason"].items():
        print(f"    {reason:<32} {n:>8,}")

    cs = stats["cell_stats"]
    print(f"\n  cells: {cs['cells']:,} filled  "
          f"({cs['cells_with_crashes']:,} with crashes, "
          f"{cs['cells_zero_crashes']:,} empty, {cs['cells_edge']:,} on the edge)")
    print(f"    rate undefined on {cs['cells_rate_undefined']:,} cells "
          f"({cs['cells_rate_undefined_with_crashes']:,} of them have crashes)")

    for name, s in sorted(stats.get("lisa", {}).items()):
        g = s["global"]
        print(f"\n  Moran's I [{name}]: I={g['I']:.4f} E[I]={g['expected_I']:.5f} "
              f"z={g['z_sim']:.2f} p_sim={g['p_sim']:.4g}")
        print(f"    LISA significant: {s['local']['significant_uncorrected']:,} "
              f"uncorrected -> {s['local']['significant_fdr']:,} after FDR  "
              f"{s['local']['quadrant_counts']}")

    for name, s in sorted(stats.get("gi_star", {}).items()):
        print(f"\n  Gi* [{name}]: n={s['n']:,}  significant "
              f"{s['significant_uncorrected_05']:,} -> {s['significant_fdr_05']:,} "
              f"after FDR (threshold {s['fdr_threshold_05']:.2e})")
        print(f"    classes {s['class_counts']}")

    ct = stats.get("contrast", {})
    if ct:
        print(f"\n  contrast: {ct['cells_changed_class']:,} of "
              f"{ct['cells_compared']:,} cells change class under normalisation")
        for kind, n in sorted(ct.get("by_kind", {}).items()):
            print(f"    {kind:<20} {n:>6,}")

    k = stats.get("kde", {})
    if k and not k.get("skipped"):
        print("\n  KDE bandwidths (m, EPSG:%d):" % k["crs_epsg"])
        for method, bw in sorted(k["bandwidths_m"].items()):
            s = k["surfaces"][method]
            print(f"    {method:<14} {bw:>8.0f}  integral {s['integral']:.4f}  "
                  f"peak {s['max_intensity_per_km2']:.1f} /km2")
        print(f"    blocked CV chose {k['cv_blocked']['best_bandwidth_m']:.0f} m; "
              f"random folds chose {k['cv_random_folds']['best_bandwidth_m']:.0f} m "
              f"(ratio {k['cv_blocked_vs_random_ratio']})")

    st = stats.get("st_dbscan", {})
    if st and not st.get("skipped"):
        print(f"\n  ST-DBSCAN: {st['spatiotemporal']['n_clusters']:,} clusters "
              f"(median span {st['spatiotemporal']['median_span_days']:.1f} d); "
              f"space-only {st['space_only']['n_clusters']:,} clusters "
              f"(median span {st['space_only']['median_span_days']:.1f} d)")

    for f in sorted(result.get("figures", {}).values(),
                    key=lambda x: x["path"]):
        print(f"  figure {Path(f['path']).name:<36} {f['kb']:>6.1f} KB")
    for w in result["warnings"]:
        print(f"  WARNING: {w}")


if __name__ == "__main__":
    sys.exit(_cli())
