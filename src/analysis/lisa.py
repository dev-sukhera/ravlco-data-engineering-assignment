"""Moran's I and LISA: does this cluster at all, before anyone says it clusters.

The order is the point. ASSIGNMENT.md's Moran's I row asks for "whether crash
rates cluster spatially at all, BEFORE you assert that they do", and the reason
that ordering matters is that every other technique in this phase assumes the
answer is yes. Gi* will happily return hot cells on white noise -- 5% of them,
by construction -- and KDE will happily draw a smooth surface over a Poisson
process. The global test is the one that can come back "no", and if it does,
the honest report says so and stops calling things clusters.

Global then local
-----------------
`esda.Moran` gives one number for the whole map: is a cell's value correlated
with the mean of its six neighbours? `esda.Moran_Local` decomposes that number
into one per cell, so a significant global I can be traced to the places
producing it. Running the local without the global is the error this module
exists to prevent: LISA on a map with no global autocorrelation returns a
scatter of "significant" cells that are the multiple-comparisons artefact, and
they look exactly like clusters on a choropleth.

Two variables, deliberately
---------------------------
`rate_per_1k_pop` is the primary: it asks whether crash RISK clusters.
`raw_count` is the contrast: it asks whether crash VOLUME clusters. Volume
clusters trivially wherever people are, so a strong I on raw counts is close to
uninformative; the interesting comparison is how much of it survives
normalisation. Both are run through the identical code path so the difference
between them is the variable and nothing else.

Inference
---------
999 conditional permutations, which is the standard test for a local statistic
whose analytic null is unreliable at small neighbourhood sizes. The finest
attainable pseudo p-value is 1/1000 = 0.001, reported as such rather than as
"p < 0.001".

**Seeding, and an esda gotcha worth writing down.** `esda.Moran_Local` takes
`seed=`; `esda.Moran` does NOT -- it draws from the global numpy RNG. So the
global test is bracketed by `np.random.seed(...)` and the local one is given
the seed directly. Verified: two `esda.Moran` calls with the same global seed
return identical `p_sim` and different seeds return different `p_sim`, so the
bracket is load-bearing and not decoration.

FDR
---
~1,900 cells tested at alpha = 0.05 is ~95 cells expected significant under a
true null. Benjamini-Hochberg (`esda.fdr`) returns the p-threshold that holds
the expected false-discovery PROPORTION at alpha; quadrants are classified only
where the corrected test passes. The uncorrected classification is kept beside
it as a column, so the correction's effect is a number a reader can check
rather than a sentence they have to believe.

Quadrants
---------
esda's `q` is 1 = HH, 2 = LH, 3 = LL, 4 = HL, computed on the standardised
value against its standardised lag. HH and LL are clusters (a high cell among
high neighbours; a low cell among low ones). LH and HL are SPATIAL OUTLIERS,
and they are the most operationally useful output of the whole method: a hot
cell in a cold neighbourhood is almost always one intersection, and it has a
name in `osm_name`.
"""

from __future__ import annotations

import logging
from typing import Any

import esda
import numpy as np
import pandas as pd
from libpysal.weights import W

from . import correction

log = logging.getLogger("analysis.lisa")

# esda's quadrant codes. Written out because `q == 2` in a filter three
# modules later is unreadable and one transposition away from wrong.
QUADRANTS = {1: "HH", 2: "LH", 3: "LL", 4: "HL"}
NOT_SIGNIFICANT = "NS"
QUADRANT_VALUES = ("HH", "LL", "HL", "LH", NOT_SIGNIFICANT)

LISA_COLUMNS = [
    "h3_r8", "variable", "local_i", "z_score", "p_sim", "p_fdr",
    "quadrant", "quadrant_uncorrected", "significant", "significant_uncorrected",
]


def global_moran(
    y: np.ndarray, w: W, *, permutations: int = 999, seed: int = 0
) -> dict[str, Any]:
    """Global Moran's I with both nulls reported.

    Two p-values, and they answer slightly different questions. `p_norm` is
    analytic: it assumes the values are normally distributed, which crash
    counts on hexagons are emphatically not (they are over-dispersed counts
    with a floor at zero). `p_sim` is the conditional permutation p-value,
    which assumes only exchangeability under the null and is the one to
    believe. Both are reported so the reader can see whether they agree; a
    large disagreement is itself a statement about the distribution.

    `w` must already carry its transform. Moran's I wants row-standardisation
    -- see weights.py -- and this function does not silently re-transform a
    matrix the caller built, because a W mutated in one place and used in
    another is how two statistics end up disagreeing for no visible reason.
    """
    # esda.Moran has no `seed` parameter and draws from the global numpy RNG.
    # This bracket is what makes the permutation p-value reproducible.
    np.random.seed(seed)
    mi = esda.Moran(y, w, permutations=permutations)
    return {
        "I": float(mi.I),
        "expected_I": float(mi.EI),
        "z_norm": float(mi.z_norm),
        "p_norm": float(mi.p_norm),
        "z_sim": float(mi.z_sim),
        "p_sim": float(mi.p_sim),
        "permutations": int(permutations),
        "seed": int(seed),
        "n": int(w.n),
        "transform": w.transform,
        # The finest p-value 999 permutations can produce. Quoted so a reader
        # meeting "p_sim = 0.001" knows it is a floor, not a measurement.
        "p_sim_floor": 1.0 / (permutations + 1),
    }


def local_moran(
    y: np.ndarray,
    w: W,
    *,
    permutations: int = 999,
    seed: int = 0,
    alpha: float = 0.05,
    use_fdr: bool = True,
) -> tuple[pd.DataFrame, dict[str, Any]]:
    """LISA per cell, FDR-corrected, with the uncorrected answer beside it.

    Returns a frame indexed by `w.id_order` -- the caller attaches the cell
    ids, because this function must not silently reorder anything.
    """
    # `alternative='two-sided'` is passed EXPLICITLY, and it is a correctness
    # choice rather than a warning suppression. esda's current default is
    # 'directed' -- a one-sided p-value in whichever direction the statistic
    # happened to fall -- and this function then classifies BOTH tails from
    # that one number (HH and LL are opposite directions). Applying a one-sided
    # p to a two-sided classification doubles the effective alpha, so every
    # "significant" cell here would be significant at 0.10 while the legend
    # said 0.05. It also pins the behaviour: esda has announced that this
    # default flips in its next major release, and a p-value that changes when
    # a dependency is upgraded is not a reproducible result.
    ml = esda.Moran_Local(
        y, w, permutations=permutations, seed=seed, alternative="two-sided"
    )
    # esda's two-sided p can exceed 1 by ~1e-3 for cells at z ~ 0; see
    # correction.clamp_pvalues. Clamped and counted, never silently passed on.
    p_sim, p_out_of_range = correction.clamp_pvalues(ml.p_sim)

    # See src/analysis/correction.py: esda.fdr's return value is ambiguous
    # between "BH rejected the single strongest cell" and "BH rejected
    # nothing", and the two look identical in an output table.
    diag = correction.describe(p_sim, alpha, permutations=permutations)
    threshold = diag["threshold"] if use_fdr else float(alpha)
    significant = p_sim <= threshold
    significant_raw = p_sim <= alpha

    quad = np.array([QUADRANTS.get(int(q), NOT_SIGNIFICANT) for q in ml.q])
    frame = pd.DataFrame({
        "local_i": np.asarray(ml.Is, dtype="float64"),
        "z_score": np.asarray(ml.z_sim, dtype="float64"),
        "p_sim": p_sim,
        "p_fdr": np.full(len(p_sim), threshold),
        "quadrant": np.where(significant, quad, NOT_SIGNIFICANT),
        "quadrant_uncorrected": np.where(significant_raw, quad, NOT_SIGNIFICANT),
        "significant": significant,
        "significant_uncorrected": significant_raw,
    })

    stats = {
        "n": int(w.n),
        "permutations": int(permutations),
        "seed": int(seed),
        "alpha": alpha,
        "fdr_applied": use_fdr,
        "fdr_threshold": threshold,
        "correction": diag,
        "p_sim_clamped_to_unit_interval": p_out_of_range,
        "significant_uncorrected": int(significant_raw.sum()),
        "significant_fdr": int(significant.sum()),
        "expected_false_positives_uncorrected": round(alpha * w.n, 1),
        "quadrant_counts": {
            k: int(v) for k, v in
            pd.Series(frame["quadrant"]).value_counts().sort_index().items()
        },
        "quadrant_counts_uncorrected": {
            k: int(v) for k, v in
            pd.Series(frame["quadrant_uncorrected"]).value_counts().sort_index().items()
        },
    }
    return frame, stats


def run(
    values: dict[str, dict[str, float]],
    weights: W,
    *,
    permutations: int = 999,
    seed: int = 0,
    alpha: float = 0.05,
    use_fdr: bool = True,
) -> tuple[pd.DataFrame, dict[str, Any]]:
    """Global + local Moran's I for every variable in `values`.

    `values` is `{variable_name: {cell: value}}`. Every variable runs over the
    SAME `weights` and the same `cells`, so the only difference between two
    variables' results is the variable.

    A variable whose cell set is smaller than the weights universe (the
    normalised rate, which is undefined on low-population cells) must be
    handed a SUBSET W built by the caller, not filled with zeros here: a zero
    rate on a cell that has no denominator is a fabricated observation, and it
    would drag every neighbouring local statistic toward "cold".
    """
    from . import weights as weights_mod

    frames: list[pd.DataFrame] = []
    stats: dict[str, Any] = {}
    for name in sorted(values):
        y = weights_mod.aligned(values[name], weights)
        if np.allclose(y, y[0]):
            # A constant field has zero variance; Moran's I is 0/0. This is a
            # real possibility on a tiny fixture corpus, and returning NaN
            # quietly would put NaN in a published table.
            raise ValueError(
                f"variable {name!r} is constant across all {len(y)} cells -- "
                "Moran's I is undefined on a field with no variance"
            )
        g = global_moran(y, weights, permutations=permutations, seed=seed)
        local, lstats = local_moran(
            y, weights, permutations=permutations, seed=seed,
            alpha=alpha, use_fdr=use_fdr,
        )
        local.insert(0, "variable", name)
        local.insert(0, "h3_r8", list(weights.id_order))
        frames.append(local)
        stats[name] = {"global": g, "local": lstats}
        log.info(
            "LISA %s: I=%.4f (E[I]=%.5f, z=%.2f, p_sim=%.4f), "
            "%d/%d cells significant after FDR (%d uncorrected)",
            name, g["I"], g["expected_I"], g["z_sim"], g["p_sim"],
            lstats["significant_fdr"], lstats["n"],
            lstats["significant_uncorrected"],
        )
    out = pd.concat(frames, ignore_index=True)
    return out[LISA_COLUMNS].sort_values(
        ["h3_r8", "variable"], ignore_index=True
    ), stats


def clusters_exist(stats: dict[str, Any], variable: str, alpha: float = 0.05) -> bool:
    """The gate the prose must pass before it uses the word "cluster".

    A single boolean, read off the GLOBAL permutation p-value and the sign of
    I. Positive and significant means the map is more clustered than chance;
    anything else means the local map is a multiple-comparisons picture and
    the write-up says so.
    """
    g = stats[variable]["global"]
    return bool(g["p_sim"] <= alpha and g["I"] > g["expected_I"])
