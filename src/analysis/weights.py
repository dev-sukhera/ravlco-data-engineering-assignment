"""The spatial weights matrix: who is a neighbour of whom, and how much.

Every statistic in this phase is a weighted comparison of a cell to its
neighbours, so W is the single object that decides what "near" means. Getting
it wrong does not raise -- it produces a plausible map of the wrong thing.

Why hexagons and not a distance band
------------------------------------
H3 r8 cells tile the plane with exactly six neighbours each, all at the same
centre-to-centre distance. That makes the neighbourhood UNIFORM: every interior
cell is compared against the same amount of surrounding area, so a high local
statistic is a property of the data and not of the cell's shape. A distance
band over irregular polygons (block groups, say) gives a downtown block group
forty neighbours and a rural one three, and the variance of the local statistic
then varies with geography -- which is the thing being tested for. Queen
contiguity on a square grid has the same problem in miniature: four rook
neighbours at distance d and four bishop neighbours at d*sqrt(2), weighted
equally.

No projection, ever
-------------------
Hex adjacency is TOPOLOGICAL. `h3.grid_disk` answers "which cells touch this
one" from the index itself; there is no distance computed, so there is no CRS
for a projection to be wrong about. This is the second place in the codebase
(after the point-in-polygon join) where the correct answer to "which CRS?" is
"none", and it is worth saying because "always reproject" is the rule people
over-apply. The apportionment two modules over is metric and does reproject,
to EPSG:5070.

Row-standardised for Moran's I, binary for Gi*
----------------------------------------------
This is not a preference, the two statistics want different objects.

`transform='r'` divides each row by its number of neighbours, so the spatial
lag is the MEAN of the neighbours. Moran's I is a correlation between a value
and its neighbourhood mean; without row-standardisation an edge cell with three
neighbours would have a systematically smaller lag than an interior cell with
six, and the statistic would partly measure the county boundary.

`transform='b'` leaves the weights at 1, so Gi* is a SUM over the neighbourhood
compared to the global sum. That is what makes Gi* a hot-spot statistic rather
than a smoothness statistic: it asks "is there an unusually large amount of
crash here", and an unusually large amount is a sum, not a mean. Row-
standardising it would divide out precisely the intensity being tested.
`G_Local(..., star=True)` includes the cell itself in its own neighbourhood,
which is what makes it able to flag a single isolated intense cell.

Determinism
-----------
`W` is built from a SORTED id list and an explicit `id_order`. libpysal will
otherwise take dict insertion order, which comes from h3-py's internal
iteration -- and a permutation test whose row order moves between runs produces
p-values that move between runs. The build asserts symmetry and reports
islands rather than letting libpysal warn about them into a log nobody reads.
"""

from __future__ import annotations

import logging
from typing import Any, Sequence

import numpy as np
from libpysal.weights import W

from ..geo import h3_index

log = logging.getLogger("analysis.weights")

ROW_STANDARDISED = "r"
BINARY = "b"


def neighbor_map(cells: Sequence[str], k: int = 1) -> dict[str, list[str]]:
    """{cell: its k-ring neighbours that are also in `cells`}, sorted.

    Delegates to `h3_index.disk_weights`, which already does the intersection
    and the sort. The intersection is the important part: the weights matrix is
    over the cells this build HAS, not over the cells H3 knows exist, and
    handing esda a neighbour that has no row is a KeyError at best and a
    silently dropped term at worst.
    """
    return h3_index.disk_weights(cells, k=k)


def build(
    cells: Sequence[str], *, k: int = 1, transform: str = ROW_STANDARDISED
) -> tuple[W, dict[str, Any]]:
    """A libpysal `W` over `cells`, plus the diagnostics that make it defensible.

    Raises on a duplicate id -- a repeated cell would give one location two
    rows and double its weight in every statistic -- and reports islands rather
    than raising on them, because an island is a real finding about the study
    area (a cell the fill produced across a water body, say) and the caller
    decides whether it invalidates the run.
    """
    ids = sorted(cells)
    if len(set(ids)) != len(ids):
        raise ValueError(
            f"duplicate cell ids in the weights universe "
            f"({len(ids)} given, {len(set(ids))} distinct)"
        )
    if transform.lower() not in (ROW_STANDARDISED, BINARY):
        raise ValueError(f"transform must be 'r' or 'b', got {transform!r}")

    neighbors = neighbor_map(ids, k=k)
    # id_order is passed explicitly. Without it libpysal takes dict order,
    # which is h3-py's iteration order, and every permutation result moves.
    w = W(neighbors, id_order=ids, silence_warnings=True)
    w.transform = transform

    degrees = np.array([len(neighbors[c]) for c in ids])
    components = n_components(neighbors)
    expected = 3 * k * (k + 1)  # the k-ring of a hexagon: 6, 18, 36, ...
    stats = {
        "n": int(w.n),
        "k": int(k),
        "transform": transform,
        "islands": list(w.islands),
        "n_islands": len(w.islands),
        "full_degree": expected,
        "cells_with_full_degree": int((degrees == expected).sum()),
        "min_degree": int(degrees.min()) if len(degrees) else 0,
        "max_degree": int(degrees.max()) if len(degrees) else 0,
        "mean_degree": float(degrees.mean()) if len(degrees) else 0.0,
        "symmetric": is_symmetric(neighbors),
        "id_order_sorted": ids == sorted(ids),
        # libpysal warns about a disconnected W and the warning is worth
        # keeping as a number: >1 component means part of the study area
        # cannot reach the rest through adjacency, so a permutation test
        # conditions on a graph that is really two graphs. At the county fill
        # this is normally a detached fringe of boundary cells across a river,
        # which is a fact about the county, not a defect -- but it is only a
        # fact once it has been counted.
        "n_components": components,
    }
    if not stats["symmetric"]:
        # Not recoverable by tuning. An asymmetric contiguity matrix means the
        # neighbour lists were built from different universes, and every
        # statistic below would be computed on a relation that is not "touches".
        raise AssertionError(
            "hex contiguity is not symmetric -- the neighbour map was built "
            "from more than one cell universe"
        )
    if w.islands:
        log.warning(
            "%d island cell(s) have no neighbour in the study universe: %s",
            len(w.islands), list(w.islands)[:5],
        )
    log.info(
        "weights: n=%d k=%d transform=%s, %d/%d cells at full degree %d, %d islands",
        w.n, k, transform, stats["cells_with_full_degree"], w.n, expected,
        len(w.islands),
    )
    return w, stats


def is_symmetric(neighbors: dict[str, list[str]]) -> bool:
    """`b in N(a)` iff `a in N(b)`, for every pair.

    Contiguity is a symmetric relation, so this must hold; it is checked rather
    than assumed because the intersection step in `neighbor_map` is exactly the
    kind of place a one-sided filter creeps in.
    """
    return all(
        a in neighbors.get(b, ()) for a, ns in neighbors.items() for b in ns
    )


def n_components(neighbors: dict[str, list[str]]) -> int:
    """How many connected pieces the adjacency graph has. 1 is the happy case."""
    seen: set[str] = set()
    count = 0
    for start in sorted(neighbors):
        if start in seen:
            continue
        count += 1
        stack = [start]
        seen.add(start)
        while stack:
            node = stack.pop()
            for nb in neighbors.get(node, ()):
                if nb not in seen:
                    seen.add(nb)
                    stack.append(nb)
    return count


def degrees(w: W) -> np.ndarray:
    """Neighbour count per cell, in `w.id_order`."""
    return np.array([len(w.neighbors[i]) for i in w.id_order])


def aligned(values: dict[str, float], w: W, *, fill: float = 0.0) -> np.ndarray:
    """A `{cell: value}` mapping as an array in `w.id_order`.

    The single place a y-vector is built, because the whole correctness of
    every statistic below rests on `y[i]` being the value for `w.id_order[i]`.
    A silent misalignment here produces a beautiful, entirely fictional map.
    """
    return np.array([float(values.get(i, fill)) for i in w.id_order], dtype="float64")
