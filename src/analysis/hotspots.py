"""Getis-Ord Gi*: hot and cold cells, FDR-corrected, raw versus per capita.

What Gi* is, and why it is not LISA
-----------------------------------
Local Moran's I asks "is this cell SIMILAR to its neighbours" -- it finds
boundaries and outliers as readily as clusters, and a large negative I is as
significant as a large positive one. Gi* asks a different and more operational
question: "is the total amount of crash in this neighbourhood unusually large
compared to the map as a whole". That is why its output is signed and
interpretable directly as hot/cold, and why it is the statistic to hand to
someone who has to decide where to send an engineer. The two are run on the
same cells and the same weights here precisely so the reader can see where they
disagree.

`star=True` includes the cell in its own neighbourhood. Without it a single
intense cell surrounded by quiet ones scores LOW -- the statistic would be
measuring its neighbours only -- which is the opposite of what a hot-spot map
should say about an intersection with forty crashes at it.

Binary weights, and why
-----------------------
`transform='B'`. Gi* is a ratio of a neighbourhood SUM to the global sum; row-
standardising divides the numerator by the neighbour count and turns the
statistic into a comparison of means, which is a smoothness measure rather than
an intensity measure. See weights.py for the matching argument on the Moran
side, where the opposite choice is correct for the opposite reason.

The FDR correction is not optional here
---------------------------------------
Around 1,900 cells are tested. At alpha = 0.05 with a true null that is ~95
cells expected "significant" -- which is the same order as the number of hot
cells this map actually has. An uncorrected Gi* map of a county is therefore
not evidence; it is a map with a known number of lies on it whose locations are
unknown. `esda.fdr` returns the Benjamini-Hochberg threshold, and both the
corrected and uncorrected classifications are written as columns so the
correction's effect is a measurement in the output table rather than a claim in
the prose.

Bonferroni (alpha / n = 2.6e-5) is also computed and reported. It controls the
probability of ANY false positive rather than the expected proportion of them,
which is the wrong trade for a screening map: it asks "is there even one
mistake on this map", where the operational question is "what fraction of the
cells I send a crew to are wasted trips". Reported anyway, because "we chose BH
over Bonferroni" is a claim that needs its alternative's number beside it.

Both corrections are also what forces the permutation budget. A permutation
p-value cannot go below 1/(m+1), and BH rejects at rank 1 only when
1/(m+1) <= alpha/n -- so at n ~ 1,900 cells, fewer than ~38,800 permutations
makes an isolated hot cell UNREJECTABLE however extreme it is. config/geo.toml
carries the measurement; 999 permutations returns an empty corrected map and
it looks exactly like a null result.

The contrast is the assignment's actual question
------------------------------------------------
ASSIGNMENT.md: "Contrast against raw counts and show that population
normalization changes the answer." So `hotspot_contrast` is a first-class
output table, not a paragraph: every cell whose class differs between
`raw_count` and `rate_per_1k_pop`, with the majority snapped road name in the
cell so the prose can NAME the place. If on this data normalisation barely
changes the answer, that is a finding and the table is short and says so.
"""

from __future__ import annotations

import logging
from typing import Any

import esda
import numpy as np
import pandas as pd
import pyarrow as pa
from libpysal.weights import W

from . import correction

log = logging.getLogger("analysis.hotspots")

HOT_99 = "HOT_99"
HOT_95 = "HOT_95"
COLD_95 = "COLD_95"
COLD_99 = "COLD_99"
NOT_SIGNIFICANT = "NS"
HOTSPOT_CLASSES = (HOT_99, HOT_95, COLD_95, COLD_99, NOT_SIGNIFICANT)

GI_COLUMNS = [
    "h3_r8", "variable", "gi_z", "p_sim", "p_fdr", "p_bonferroni",
    "hotspot_class", "hotspot_class_uncorrected", "significant",
    "significant_uncorrected",
]

CONTRAST_COLUMNS = [
    "h3_r8", "class_raw_count", "class_rate_per_1k_pop", "contrast_kind",
    "n_crashes", "population", "rate_per_1k_pop",
    "gi_z_raw_count", "gi_z_rate_per_1k_pop", "place_name", "place_highway",
]

# Declared, not inferred, for the same reason as st_dbscan.CLUSTER_SCHEMA: an
# empty contrast table is a LEGITIMATE and interesting result ("normalisation
# changed nothing"), and it has to validate against the same contract as a
# populated one rather than failing a type check on empty object columns.
CONTRAST_SCHEMA = pa.schema([
    ("h3_r8", pa.string()),
    ("class_raw_count", pa.string()),
    ("class_rate_per_1k_pop", pa.string()),
    ("contrast_kind", pa.string()),
    ("n_crashes", pa.int64()),
    ("population", pa.float64()),
    ("rate_per_1k_pop", pa.float64()),
    ("gi_z_raw_count", pa.float64()),
    ("gi_z_rate_per_1k_pop", pa.float64()),
    ("place_name", pa.string()),
    ("place_highway", pa.string()),
])


def empty_contrast() -> pa.Table:
    """A correctly typed, zero-row contrast table. See `CONTRAST_SCHEMA`."""
    return pa.table(
        {f.name: pa.array([], type=f.type) for f in CONTRAST_SCHEMA},
        schema=CONTRAST_SCHEMA,
    )


def classify(
    z: np.ndarray, p: np.ndarray, *, threshold_95: float, threshold_99: float
) -> np.ndarray:
    """z-scores and p-values to {HOT_99, HOT_95, COLD_95, COLD_99, NS}.

    The two thresholds are passed in rather than being alpha and alpha/5,
    because after an FDR correction the 0.05-level and 0.01-level thresholds
    are two separate BH computations, not one number and a tenth of it.

    Sign comes from z, significance from p. A cell can only be HOT_99 if it
    clears the stricter threshold, so the classes nest: HOT_99 is a subset of
    what would be HOT_95.
    """
    out = np.full(len(z), NOT_SIGNIFICANT, dtype=object)
    hot = z > 0
    out[(p <= threshold_95) & hot] = HOT_95
    out[(p <= threshold_95) & ~hot] = COLD_95
    out[(p <= threshold_99) & hot] = HOT_99
    out[(p <= threshold_99) & ~hot] = COLD_99
    return out


def gi_star(
    y: np.ndarray,
    w: W,
    *,
    permutations: int = 999,
    seed: int = 0,
    alpha: float = 0.05,
    use_fdr: bool = True,
) -> tuple[pd.DataFrame, dict[str, Any]]:
    """Gi* for one variable, with the correction's effect as columns.

    `w` must be binary-transformed; this is asserted rather than silently
    fixed, because a caller who handed a row-standardised W to Gi* has a
    misunderstanding that a quiet re-transform would hide.
    """
    if str(w.transform).lower() != "b":
        raise ValueError(
            f"Gi* wants binary weights (transform='b'), got {w.transform!r}. "
            "Row-standardising turns the neighbourhood sum into a mean and the "
            "statistic stops being a hot-spot test -- see the module docstring."
        )
    # Two-sided, explicitly. The class column assigns HOT and COLD from this
    # one p-value, so a one-sided p (esda's current 'directed' default) would
    # make every reported 0.05 an effective 0.10. It also pins the behaviour
    # against esda's announced default change in its next major release. See
    # lisa.py for the same argument on the Moran side.
    g = esda.G_Local(y, w, star=True, permutations=permutations, seed=seed,
                     alternative="two-sided")
    z = np.asarray(g.Zs, dtype="float64")
    # esda's two-sided p can exceed 1 by ~1e-3 for cells at z ~ 0; see
    # correction.clamp_pvalues. Clamped and counted, never silently passed on.
    p, p_out_of_range = correction.clamp_pvalues(g.p_sim)

    n = len(p)
    # `esda.fdr` returns alpha/n both when BH rejects exactly the strongest
    # cell AND when it rejects nothing at all -- the same number meaning two
    # opposite things. `correction.describe` recomputes the BH decision so the
    # stats can say which happened. See src/analysis/correction.py.
    d95 = correction.describe(p, alpha, permutations=permutations)
    d99 = correction.describe(p, alpha / 5.0, permutations=permutations)
    if use_fdr:
        t95, t99 = d95["threshold"], d99["threshold"]
    else:
        t95, t99 = float(alpha), float(alpha / 5.0)
    bonferroni = alpha / n if n else float("nan")

    cls = classify(z, p, threshold_95=t95, threshold_99=t99)
    cls_raw = classify(z, p, threshold_95=alpha, threshold_99=alpha / 5.0)

    frame = pd.DataFrame({
        "gi_z": z,
        "p_sim": p,
        "p_fdr": np.full(n, t95),
        "p_bonferroni": np.full(n, bonferroni),
        "hotspot_class": cls,
        "hotspot_class_uncorrected": cls_raw,
        "significant": p <= t95,
        "significant_uncorrected": p <= alpha,
    })

    stats = {
        "n": n,
        "permutations": int(permutations),
        "seed": int(seed),
        "alpha": alpha,
        "fdr_applied": use_fdr,
        "fdr_threshold_05": t95,
        "fdr_threshold_01": t99,
        "bonferroni_threshold": bonferroni,
        "significant_uncorrected_05": int((p <= alpha).sum()),
        "significant_uncorrected_01": int((p <= alpha / 5.0).sum()),
        "significant_fdr_05": int((p <= t95).sum()),
        "significant_fdr_01": int((p <= t99).sum()),
        "significant_bonferroni": int((p <= bonferroni).sum()),
        "expected_false_positives_uncorrected_05": round(alpha * n, 1),
        "correction_05": d95,
        "correction_01": d99,
        "p_sim_clamped_to_unit_interval": p_out_of_range,
        "class_counts": {
            k: int(v) for k, v in pd.Series(cls).value_counts().sort_index().items()
        },
        "class_counts_uncorrected": {
            k: int(v) for k, v in pd.Series(cls_raw).value_counts().sort_index().items()
        },
        "z_min": float(z.min()) if n else None,
        "z_max": float(z.max()) if n else None,
    }
    return frame, stats


def run(
    values: dict[str, dict[str, float]],
    weights: dict[str, W],
    *,
    permutations: int = 999,
    seed: int = 0,
    alpha: float = 0.05,
    use_fdr: bool = True,
) -> tuple[pd.DataFrame, dict[str, Any]]:
    """Gi* for every variable, each against its own weights universe.

    `weights` is per-variable, not shared, and that is the one structural
    difference from `lisa.run`. The normalised rate is undefined on cells with
    no population, so it is computed over a SUBSET of the cells -- and a
    subset needs its own W, because a neighbour that has no row would
    contribute a zero the cell does not have. Filling those cells with a zero
    rate instead would say "nobody crashes at the interchange", which is the
    opposite of true.
    """
    from . import weights as weights_mod

    frames: list[pd.DataFrame] = []
    stats: dict[str, Any] = {}
    for name in sorted(values):
        w = weights[name]
        y = weights_mod.aligned(values[name], w)
        frame, s = gi_star(
            y, w, permutations=permutations, seed=seed, alpha=alpha, use_fdr=use_fdr
        )
        frame.insert(0, "variable", name)
        frame.insert(0, "h3_r8", list(w.id_order))
        frames.append(frame)
        stats[name] = s
        log.info(
            "Gi* %s: n=%d, significant %d -> %d after FDR (threshold %.2e), "
            "classes %s",
            name, s["n"], s["significant_uncorrected_05"],
            s["significant_fdr_05"], s["fdr_threshold_05"], s["class_counts"],
        )
    out = pd.concat(frames, ignore_index=True)
    return out[GI_COLUMNS].sort_values(
        ["h3_r8", "variable"], ignore_index=True
    ), stats


# ---------------------------------------------------------------------------
# the contrast: what normalisation changes
# ---------------------------------------------------------------------------


def place_names(crashes: pd.DataFrame, cell_column: str = "h3_r8") -> pd.DataFrame:
    """The majority snapped road name per cell, so the prose can name places.

    Phase 4 snapped 124,853 Montgomery crashes to an OSM way and kept
    `osm_name`. The modal name in a cell is a good label for it -- a cell on
    I-270 is mostly crashes on I-270 -- and it costs nothing because the
    column is already there. Ties break on the alphabetically first name so
    the label is deterministic.

    This is a LABEL, not a finding. It says which road most of the cell's
    crashes were snapped to; it does not claim the road caused them.
    """
    named = crashes[crashes["osm_name"].notna()]
    if named.empty:
        return pd.DataFrame(columns=[cell_column, "place_name", "place_highway"])

    counts = (
        named.groupby([cell_column, "osm_name", "osm_highway"], dropna=False)
        .size()
        .reset_index(name="n")
        .sort_values([cell_column, "n", "osm_name"], ascending=[True, False, True])
    )
    top = counts.drop_duplicates(subset=[cell_column], keep="first")
    return top.rename(
        columns={"osm_name": "place_name", "osm_highway": "place_highway"}
    )[[cell_column, "place_name", "place_highway"]]


def contrast(
    gi: pd.DataFrame,
    cells: pd.DataFrame,
    crashes: pd.DataFrame,
    *,
    raw_variable: str = "raw_count",
    rate_variable: str = "rate_per_1k_pop",
) -> tuple[pd.DataFrame, dict[str, Any]]:
    """Cells whose Gi* class differs between raw counts and the per-capita rate.

    `contrast_kind` names the direction, and the two interesting values are
    the ones the assignment is asking about:

      HOT_RAW_ONLY   volume without risk -- a downtown cell that is busy
                     because that is where the people are. Prioritising it
                     sends a crew to the densest place on the map, which is
                     where they already are.
      HOT_RATE_ONLY  risk without volume -- a cell with few residents and a
                     lot of crashes. These are the ones a raw-count map hides,
                     and they are usually a road rather than a neighbourhood.

    A cell present in the raw analysis but absent from the normalised one
    (population below the floor) is reported as `RATE_UNDEFINED` rather than
    dropped: "this cell has no denominator" is exactly the finding, and
    silently omitting it would make the contrast look smaller than it is.
    """
    raw = gi[gi["variable"] == raw_variable].set_index("h3_r8")
    rate = gi[gi["variable"] == rate_variable].set_index("h3_r8")
    base = cells.set_index("h3_r8")

    merged = pd.DataFrame({
        "class_raw_count": raw["hotspot_class"],
        "gi_z_raw_count": raw["gi_z"],
    })
    merged["class_rate_per_1k_pop"] = rate["hotspot_class"]
    merged["gi_z_rate_per_1k_pop"] = rate["gi_z"]
    merged["class_rate_per_1k_pop"] = merged["class_rate_per_1k_pop"].fillna(
        "RATE_UNDEFINED"
    )

    for col in ("n_crashes", "population", "rate_per_1k_pop"):
        merged[col] = base[col]

    def kind(row: pd.Series) -> str:
        r, n = row["class_raw_count"], row["class_rate_per_1k_pop"]
        if n == "RATE_UNDEFINED":
            return "RATE_UNDEFINED"
        hot_r, hot_n = r.startswith("HOT"), n.startswith("HOT")
        cold_r, cold_n = r.startswith("COLD"), n.startswith("COLD")
        if hot_r and not hot_n:
            return "HOT_RAW_ONLY"
        if hot_n and not hot_r:
            return "HOT_RATE_ONLY"
        if cold_r and not cold_n:
            return "COLD_RAW_ONLY"
        if cold_n and not cold_r:
            return "COLD_RATE_ONLY"
        return "LEVEL_CHANGE"  # same sign, different confidence band

    differs = merged["class_raw_count"] != merged["class_rate_per_1k_pop"]
    agreed = int((~differs).sum())
    if not differs.any():
        # Normalisation changed nothing. A real (and reportable) outcome, so it
        # gets a correctly typed empty table rather than an exception.
        return empty_contrast(), {
            "cells_compared": int(len(merged)),
            "cells_same_class": agreed,
            "cells_changed_class": 0,
            "changed_fraction": 0.0,
            "by_kind": {},
            "hot_under_both": int(
                (merged["class_raw_count"].str.startswith("HOT")
                 & merged["class_rate_per_1k_pop"].astype(str)
                 .str.startswith("HOT")).sum()
            ),
        }

    out = merged[differs].copy()
    out["contrast_kind"] = out.apply(kind, axis=1)
    out = out.reset_index().rename(columns={"index": "h3_r8"})

    names = place_names(crashes)
    out = out.merge(names, on="h3_r8", how="left")
    for col in ("place_name", "place_highway"):
        # Explicit `string` dtype, not object. When NO cell has a snapped road
        # -- which is the case whenever the geo build ran with --skip-snap --
        # an all-null object column registers with DuckDB as INTEGER and fails
        # the contract's type check for a reason that has nothing to do with
        # the data. pandas `string` survives being entirely null.
        out[col] = (out[col] if col in out else pd.NA)
        out[col] = pd.Series(out[col], index=out.index, dtype="string")

    out = out[CONTRAST_COLUMNS].sort_values(
        ["contrast_kind", "h3_r8"], ignore_index=True
    )

    stats = {
        "cells_compared": int(len(merged)),
        "cells_same_class": agreed,
        "cells_changed_class": int(len(out)),
        "changed_fraction": round(len(out) / len(merged), 6) if len(merged) else None,
        "by_kind": {
            str(k): int(v) for k, v in out["contrast_kind"].value_counts().items()
        } if len(out) else {},
        "hot_under_both": int(
            (merged["class_raw_count"].str.startswith("HOT")
             & merged["class_rate_per_1k_pop"].astype(str).str.startswith("HOT")).sum()
        ),
    }
    log.info(
        "contrast: %d/%d cells change class under normalisation (%s)",
        stats["cells_changed_class"], stats["cells_compared"], stats["by_kind"],
    )
    return out, stats


def top_cells(
    gi: pd.DataFrame,
    cells: pd.DataFrame,
    crashes: pd.DataFrame,
    *,
    variable: str,
    n: int = 10,
    hot: bool = True,
) -> pd.DataFrame:
    """The n most extreme cells for one variable, named. For the prose only."""
    part = gi[gi["variable"] == variable].merge(
        cells[["h3_r8", "n_crashes", "n_injury", "n_fatal", "population",
               "rate_per_1k_pop"]],
        on="h3_r8", how="left",
    )
    part = part.sort_values("gi_z", ascending=not hot).head(n)
    return part.merge(place_names(crashes), on="h3_r8", how="left")
