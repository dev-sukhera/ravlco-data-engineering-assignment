"""Multiple-testing correction, and what `esda.fdr` is actually telling you.

Both local statistics in this phase test every cell in the study area -- ~1,900
of them -- and then draw a map of the ones that came back significant. At
alpha = 0.05 with a true null, ~95 cells are expected to be "significant" by
chance, which is the same order as the number of genuinely hot cells. An
uncorrected local map of a county is therefore not evidence. It is a map with a
known number of lies on it whose locations are unknown.

`esda.fdr` is the correction the assignment names and the one this phase uses.
This module exists because its return value is ambiguous in a way that matters,
and the ambiguity is invisible in the output table.

The ambiguity
-------------
`esda.fdr(p, alpha)` returns a p-value CUT-OFF. Its implementation sorts the
p-values descending against the Benjamini-Hochberg critical values
`k * alpha / n`, and:

  * if some rank passes, it returns that rank's critical value;
  * if NO rank passes -- BH rejects nothing at all -- it returns `alpha / n`,
    the Bonferroni bound.

Those two paths can return THE SAME NUMBER. When only the single strongest cell
passes, the returned cut-off is `1 * alpha / n = alpha / n`; when nothing
passes, the returned cut-off is also `alpha / n`. In the first case one cell is
genuinely FDR-significant. In the second the correct reading is "the correction
rejected nothing", and any cell that happens to sit below `alpha / n` is being
admitted by a Bonferroni threshold that was never asked for.

This is not hypothetical. On this corpus at 999 permutations the per-capita
rate hit the second branch and returned 3.70e-05 = 0.05/1351, which looks
exactly like a very strict FDR result and is really the correction saying it
had no resolution to work with (see config/geo.toml [analysis] permutations for
the arithmetic). Publishing that as "no hot spots per capita" would have been a
false negative dressed as a finding.

So `describe` recomputes the BH decision independently and reports which branch
produced the number. The threshold still comes from `esda.fdr` -- one
implementation, the named one -- and this is a diagnostic beside it, not a
replacement for it.
"""

from __future__ import annotations

from typing import Any

import esda
import numpy as np


def clamp_pvalues(pvalues: np.ndarray) -> tuple[np.ndarray, int]:
    """Clamp to [0, 1] and report how many values needed it.

    `esda`'s two-sided conditional-randomisation p-value can come back very
    slightly ABOVE 1 -- 1.00122 was measured on this corpus at 99,999
    permutations. It is computed as twice the smaller tail with a continuity
    correction applied to both sides, and when the observed statistic sits
    almost exactly at the median of its null distribution the doubling
    overshoots. The affected cells are the ones with |z| ~ 0, i.e. the most
    thoroughly non-significant cells on the map, so clamping cannot change any
    classification -- but a p-value above 1 is not a p-value, it would fail the
    output contract's [0, 1] range check, and silently letting it through
    would be worse than either.

    Counted rather than quietly fixed: if this ever starts happening to a cell
    that is NOT at z ~ 0, the count is the thing that would show it.
    """
    p = np.asarray(pvalues, dtype="float64")
    out_of_range = int(((p > 1.0) | (p < 0.0)).sum())
    return np.clip(p, 0.0, 1.0), out_of_range


def bh_rejections(pvalues: np.ndarray, alpha: float) -> int:
    """How many hypotheses Benjamini-Hochberg actually rejects.

    The textbook step-up procedure: sort ascending, find the largest rank `k`
    with `p_(k) <= k * alpha / n`, and reject the `k` smallest. Zero means the
    correction rejected nothing, whatever cut-off `esda.fdr` returned.
    """
    p = np.sort(np.asarray(pvalues, dtype="float64"))
    n = len(p)
    if not n:
        return 0
    ranks = np.arange(1, n + 1)
    passing = np.nonzero(p <= ranks * alpha / n)[0]
    return int(passing[-1] + 1) if len(passing) else 0


def rank_one_permutations_required(n: int, alpha: float) -> int:
    """Permutations needed before BH can reject a single isolated cell.

    A permutation p-value's floor is `1 / (m + 1)`. BH rejects at rank 1 only
    when `p_(1) <= alpha / n`, so `m + 1 >= n / alpha`. Below this budget the
    most extreme cell on the map cannot be rejected however extreme it is, and
    the corrected map comes back empty for a reason that is entirely about the
    permutation count and not at all about the data.
    """
    return int(np.ceil(n / alpha)) - 1


def describe(
    pvalues: np.ndarray, alpha: float, *, permutations: int
) -> dict[str, Any]:
    """The threshold plus everything needed to read it honestly.

    `threshold` is `esda.fdr`'s answer and is what the build classifies with.
    Everything else is diagnosis: whether BH rejected anything at all, whether
    the permutation budget was the binding constraint, and what the Bonferroni
    alternative would have said.
    """
    p = np.asarray(pvalues, dtype="float64")
    n = len(p)
    threshold = float(esda.fdr(p, alpha)) if n else float(alpha)
    rejections = bh_rejections(p, alpha)
    bonferroni = alpha / n if n else float("nan")
    floor = 1.0 / (permutations + 1)
    required = rank_one_permutations_required(n, alpha)
    return {
        "alpha": alpha,
        "n": n,
        "threshold": threshold,
        "bh_rejections": rejections,
        # True when esda.fdr fell through to its Bonferroni branch: BH rejected
        # nothing and the returned number is alpha/n, not a BH critical value.
        # Any cell classified significant under it is being admitted by a
        # correction nobody chose.
        "fell_back_to_bonferroni": bool(rejections == 0 and n > 0),
        "bonferroni_threshold": bonferroni,
        "permutations": int(permutations),
        "p_value_floor": floor,
        "permutations_required_for_rank_one": required,
        # True when the permutation budget, not the evidence, is what stops the
        # correction rejecting. The single most important thing to know about
        # an empty corrected map.
        "resolution_limited": bool(floor > bonferroni),
        "cells_at_p_value_floor": int((p <= floor).sum()) if n else 0,
    }
