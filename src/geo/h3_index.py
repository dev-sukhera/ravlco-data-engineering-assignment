"""H3 indexing: r9 stored, r8 and r7 derived, and the neighbourhood helper.

Why three resolutions and not one
---------------------------------
r8 (~0.74 km2, ~530 m edge) is the grain ASSIGNMENT.md names for aggregation.
r9 (~0.10 km2, ~200 m edge) is the finest DEFENSIBLE grain for these
coordinates: a Montgomery geocode is good to tens of metres, a TxDOT one
sometimes to a segment endpoint, so r10 (~65 m edge) would be indexing
positional error. r7 (~5.2 km2) is the rollup a county-level rate wants.

Only r9 is computed from the coordinate. r8 and r7 are `cell_to_parent` of it,
which is not a micro-optimisation: three independent `latlng_to_cell` calls can
disagree at a cell boundary (the r9 cell a point lands in and the r8 cell that
same point lands in need not be parent and child), and a rollup built on
inconsistent indexes silently double-counts. Deriving upward makes
`cell_to_parent(h3_r9, 8) == h3_r8` true BY CONSTRUCTION, and the build asserts
it anyway because a cheap invariant that is never checked is a comment.

No projection, ever
-------------------
H3 is defined on a sphere: `latlng_to_cell` takes WGS84 degrees and does its own
gnomonic projection onto an icosahedron face internally. There is no metric
operation here for a projected CRS to be wrong about, and passing it EPSG:3857
metres would not raise -- it would return cells off the coast of West Africa.
That is why this module takes lat/lon and nothing else.

API: h3-py 4.x only (`latlng_to_cell`, `cell_to_parent`, `grid_disk`,
`cell_area`). v3's `geo_to_h3` / `h3_to_parent` do not exist in 4.x; a test
asserts this module imports no v3 name.
"""

from __future__ import annotations

import logging
from typing import Iterable, Sequence

import h3

from .. import config

log = logging.getLogger("geo.h3_index")


def resolutions() -> tuple[int, list[int]]:
    """(stored finest resolution, parent resolutions) from config/geo.toml."""
    cfg = config.geo()["h3"]
    return int(cfg["store_resolution"]), [int(r) for r in cfg["parent_resolutions"]]


def _missing(value: float | None) -> bool:
    """None, pandas NA, or a float NaN -- all three arrive from a parquet read.

    Checked explicitly because `h3.latlng_to_cell(nan, nan, 9)` raises rather
    than returning null, and a bare `is None` misses the NaN that a DOUBLE
    column with nulls actually produces.
    """
    if value is None:
        return True
    try:
        return bool(value != value)  # NaN is the only value unequal to itself
    except (TypeError, ValueError):  # pragma: no cover - pd.NA raises here
        return True


def cell(lat: float | None, lon: float | None, resolution: int) -> str | None:
    """One H3 cell as a string, or None for a missing coordinate.

    Strings, not the 64-bit integers h3-py can also return: the cell id is an
    identifier that gets joined, partitioned on and pasted into a ticket, and a
    BIGINT that renders as -1234... in half the tools is a support burden with
    no storage win once parquet has dictionary-encoded the column.
    """
    if _missing(lat) or _missing(lon):
        return None
    try:
        return h3.latlng_to_cell(float(lat), float(lon), resolution)
    except (ValueError, TypeError) as exc:
        log.warning("h3 refused a coordinate at r%d: %s", resolution, exc)
        return None


def parent(cell_id: str | None, resolution: int) -> str | None:
    if not cell_id:
        return None
    return h3.cell_to_parent(cell_id, resolution)


def index_point(lat: float | None, lon: float | None) -> dict[str, str | None]:
    """`{'h3_r9': ..., 'h3_r8': ..., 'h3_r7': ...}` for one coordinate.

    A NULL coordinate yields NULL cells rather than a sentinel: there is no
    "unknown cell" member, because unlike a dimension key an H3 id has no room
    for one and inventing `0x0` would put every ungeocoded crash in the Gulf of
    Guinea.
    """
    finest, parents = resolutions()
    fine = cell(lat, lon, finest)
    out: dict[str, str | None] = {f"h3_r{finest}": fine}
    for r in parents:
        out[f"h3_r{r}"] = parent(fine, r)
    return out


def index_points(
    coords: Iterable[tuple[float | None, float | None]]
) -> list[dict[str, str | None]]:
    return [index_point(lat, lon) for lat, lon in coords]


def columns() -> list[str]:
    """The H3 column names, finest first -- the contract's order."""
    finest, parents = resolutions()
    return [f"h3_r{finest}"] + [f"h3_r{r}" for r in parents]


# ---------------------------------------------------------------------------
# what Phase 5 will call
# ---------------------------------------------------------------------------


def grid_disk(cell_id: str, k: int = 1) -> list[str]:
    """The cell plus every cell within `k` steps, sorted.

    Phase 5's neighbourhood smoothing and its spatial weights matrix both want
    this. Sorted because an unordered neighbour list makes a weights matrix
    whose row order depends on h3-py's internal iteration, and a Getis-Ord
    result that moves between runs is not a result.
    """
    return sorted(h3.grid_disk(cell_id, k))


def disk_weights(cells: Sequence[str], k: int = 1) -> dict[str, list[str]]:
    """{cell: neighbours within k that are also in `cells`}.

    The intersection matters: a weights matrix over the cells you HAVE is a
    different object from one over the cells that exist, and only the first is
    what `esda` should be handed.
    """
    present = set(cells)
    return {
        c: [n for n in grid_disk(c, k) if n in present and n != c]
        for c in sorted(present)
    }


def cell_area_km2(cell_id: str) -> float:
    """Exact spherical area of one cell. Used for reporting, not for rates.

    A per-km2 rate over crashes belongs in EPSG:5070 (equal-area, Phase 5), not
    on the H3 sphere -- but the cell's own area is a property of the cell and
    `h3.cell_area` is its authority.
    """
    return float(h3.cell_area(cell_id, unit="km^2"))
