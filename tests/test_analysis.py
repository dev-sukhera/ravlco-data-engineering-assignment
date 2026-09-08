"""Phase 5 tests: real statistics on real and synthetic fields, no mocks.

Three kinds of test here, and they are testing different things.

**Statistical correctness on synthetic fields.** A smooth gradient over a hex
patch must give a positive, significant Moran's I; a shuffled version of the
same values must not. A planted hot patch in Poisson noise must come back
HOT_99 after the FDR correction; pure noise must come back with at most the
false-discovery share. These are the tests that would catch a transposed
weights matrix or a sign error, and they cannot be written against real data
because real data has no known answer.

**Invariants on the real corpus.** Mass-preserving apportionment, the rate
floor, the fill covering every crash cell, no 3857 anywhere. These run against
the fixture-scale build by default and against the full corpus under
`CRASH_TEST_FULL_BRONZE=1`, exactly as Phases 1-4 do.

**Determinism.** Two builds into two tmp directories produce byte-identical
parquet, and the same seed produces the same p-values twice.

No mocks and no network anywhere, per the suite's standing rule. The synthetic
fields are built with `h3.grid_disk` on a real cell id, so even they exercise
the real indexing code.
"""

from __future__ import annotations

import hashlib
import json
import re
import shutil
import tokenize
from datetime import date
from pathlib import Path

import esda
import h3
import numpy as np
import pandas as pd
import pytest

from src import config, contracts
from src.analysis import (
    build as analysis_build,
    correction,
    frames,
    hotspots,
    kde as kde_mod,
    lisa as lisa_mod,
    st_dbscan,
    weights as weights_mod,
)

REPO = Path(__file__).resolve().parents[1]
ANALYSIS_SRC = REPO / "src" / "analysis"
SEED = 12345


# ---------------------------------------------------------------------------
# synthetic hex patches -- a known answer to test the statistics against
# ---------------------------------------------------------------------------


@pytest.fixture(scope="module")
def patch_cells() -> list[str]:
    """A filled r8 hex patch centred in Montgomery County: 127 cells, k=6."""
    centre = h3.latlng_to_cell(39.1, -77.2, 8)
    return sorted(h3.grid_disk(centre, 6))


def _gradient(cells: list[str]) -> np.ndarray:
    """A smooth west-to-east ramp: maximally spatially autocorrelated."""
    return np.array([h3.cell_to_latlng(c)[1] for c in cells])


class TestWeights:
    def test_interior_cells_have_exactly_six_neighbours(self, patch_cells):
        nb = weights_mod.neighbor_map(patch_cells, k=1)
        # The k=6 disk's outer ring is truncated; everything inside it is
        # interior and a hexagon has exactly six neighbours.
        centre = patch_cells[0]
        interior = set(h3.grid_disk(h3.latlng_to_cell(39.1, -77.2, 8), 5))
        assert interior, "the interior set must not be empty"
        assert all(len(nb[c]) == 6 for c in interior)
        assert centre in nb

    def test_neighbours_match_h3_grid_disk(self, patch_cells):
        nb = weights_mod.neighbor_map(patch_cells, k=1)
        target = h3.latlng_to_cell(39.1, -77.2, 8)
        expected = sorted(set(h3.grid_disk(target, 1)) - {target})
        assert nb[target] == expected

    def test_weights_are_symmetric_sorted_and_islandless(self, patch_cells):
        w, stats = weights_mod.build(patch_cells, k=1)
        assert stats["symmetric"] is True
        assert stats["id_order_sorted"] is True
        assert list(w.id_order) == sorted(patch_cells)
        assert w.islands == []
        assert stats["n_components"] == 1

    def test_k2_ring_has_eighteen_neighbours(self, patch_cells):
        nb = weights_mod.neighbor_map(patch_cells, k=2)
        centre = h3.latlng_to_cell(39.1, -77.2, 8)
        assert len(nb[centre]) == 18  # 3*k*(k+1) for k=2

    def test_duplicate_ids_raise(self, patch_cells):
        with pytest.raises(ValueError, match="duplicate cell ids"):
            weights_mod.build(patch_cells + patch_cells[:1])

    def test_aligned_follows_id_order_not_dict_order(self, patch_cells):
        w, _ = weights_mod.build(patch_cells)
        values = {c: float(i) for i, c in enumerate(reversed(patch_cells))}
        y = weights_mod.aligned(values, w)
        assert list(y) == [values[c] for c in w.id_order]

    def test_disconnected_universe_is_counted(self):
        """Two far-apart patches are two components, and that is reported."""
        a = sorted(h3.grid_disk(h3.latlng_to_cell(39.1, -77.2, 8), 2))
        b = sorted(h3.grid_disk(h3.latlng_to_cell(39.3, -76.9, 8), 2))
        _, stats = weights_mod.build(sorted(set(a) | set(b)))
        assert stats["n_components"] == 2


class TestMoran:
    def test_smooth_gradient_is_positively_autocorrelated(self, patch_cells):
        w, _ = weights_mod.build(patch_cells, transform=weights_mod.ROW_STANDARDISED)
        g = lisa_mod.global_moran(
            _gradient(patch_cells), w, permutations=999, seed=SEED
        )
        assert g["I"] > 0.5
        assert g["p_sim"] <= 0.01
        assert g["I"] > g["expected_I"]

    def test_shuffled_field_is_not_autocorrelated(self, patch_cells):
        """The same VALUES with the locations shuffled: the null, by construction.

        This is the control that makes the gradient test mean something. A
        weights matrix that was secretly the identity, or a statistic that
        keyed off the value distribution rather than its arrangement, would
        pass the gradient test and fail this one.
        """
        w, _ = weights_mod.build(patch_cells, transform=weights_mod.ROW_STANDARDISED)
        y = _gradient(patch_cells)
        shuffled = np.random.default_rng(SEED).permutation(y)
        g = lisa_mod.global_moran(shuffled, w, permutations=999, seed=SEED)
        assert g["p_sim"] > 0.05, f"a shuffled field should not cluster: {g}"
        assert abs(g["I"]) < 0.2

    def test_same_seed_gives_identical_results(self, patch_cells):
        w, _ = weights_mod.build(patch_cells, transform=weights_mod.ROW_STANDARDISED)
        y = _gradient(patch_cells)
        a = lisa_mod.global_moran(y, w, permutations=999, seed=SEED)
        b = lisa_mod.global_moran(y, w, permutations=999, seed=SEED)
        assert a == b

    def test_local_moran_is_seeded(self, patch_cells):
        w, _ = weights_mod.build(patch_cells, transform=weights_mod.ROW_STANDARDISED)
        y = _gradient(patch_cells)
        a, _ = lisa_mod.local_moran(y, w, permutations=999, seed=SEED)
        b, _ = lisa_mod.local_moran(y, w, permutations=999, seed=SEED)
        pd.testing.assert_frame_equal(a, b)

    def test_quadrants_only_where_significant(self, patch_cells):
        w, _ = weights_mod.build(patch_cells, transform=weights_mod.ROW_STANDARDISED)
        frame, _ = lisa_mod.local_moran(
            _gradient(patch_cells), w, permutations=999, seed=SEED
        )
        assert ((frame["quadrant"] == "NS") == ~frame["significant"]).all()
        assert set(frame["quadrant"]) <= set(lisa_mod.QUADRANT_VALUES)

    def test_constant_field_raises_rather_than_returning_nan(self, patch_cells):
        w, _ = weights_mod.build(patch_cells)
        values = {c: 1.0 for c in patch_cells}
        with pytest.raises(ValueError, match="constant"):
            lisa_mod.run({"flat": values}, w, permutations=99, seed=SEED)

    def test_clusters_exist_gate(self, patch_cells):
        w, _ = weights_mod.build(patch_cells, transform=weights_mod.ROW_STANDARDISED)
        values = dict(zip(patch_cells, _gradient(patch_cells)))
        _, stats = lisa_mod.run({"ramp": values}, w, permutations=999, seed=SEED)
        assert lisa_mod.clusters_exist(stats, "ramp") is True


class TestGiStar:
    """A planted cluster in noise, and the noise on its own."""

    @staticmethod
    def _noise(cells: list[str], seed: int = SEED) -> np.ndarray:
        return np.random.default_rng(seed).poisson(5.0, size=len(cells)).astype(float)

    def test_planted_patch_is_hot_after_fdr(self, patch_cells):
        w, _ = weights_mod.build(patch_cells, transform=weights_mod.BINARY)
        y = self._noise(patch_cells)
        centre = h3.latlng_to_cell(39.1, -77.2, 8)
        planted = set(h3.grid_disk(centre, 1))  # the 7-cell patch
        idx = [i for i, c in enumerate(w.id_order) if c in planted]
        assert len(idx) == 7
        y[idx] += 60.0

        frame, stats = hotspots.gi_star(y, w, permutations=99_999, seed=SEED)
        classes = frame.loc[idx, "hotspot_class"]
        assert classes.str.startswith("HOT").all(), (
            f"the planted patch should be hot after FDR, got {list(classes)}"
        )
        # The patch CENTRE is the strongest cell -- all six of its neighbours
        # are boosted. The patch's own outer ring has three unboosted
        # neighbours each, so HOT_95 there is the correct answer, not a
        # weakness: Gi* is a neighbourhood statistic and those cells genuinely
        # have a mixed neighbourhood.
        centre_row = frame.loc[w.id_order.index(centre)]
        assert centre_row["hotspot_class"] == hotspots.HOT_99
        assert stats["significant_fdr_05"] >= 7
        assert stats["correction_05"]["bh_rejections"] >= 7
        assert stats["correction_05"]["fell_back_to_bonferroni"] is False

    def test_pure_noise_stays_within_the_false_discovery_share(self, patch_cells):
        """Not zero -- a bound. An FDR procedure ADMITS false discoveries.

        Benjamini-Hochberg controls the expected proportion of false
        discoveries among the rejections at alpha, so on a true null the
        expected count is small but not zero. Asserting zero would be
        asserting something the method does not promise, and would be flaky.
        """
        w, _ = weights_mod.build(patch_cells, transform=weights_mod.BINARY)
        _, stats = hotspots.gi_star(
            self._noise(patch_cells), w, permutations=9999, seed=SEED, alpha=0.05
        )
        bound = max(3, int(0.05 * len(patch_cells)))
        assert stats["significant_fdr_05"] <= bound, stats

    def test_row_standardised_weights_are_refused(self, patch_cells):
        w, _ = weights_mod.build(patch_cells, transform=weights_mod.ROW_STANDARDISED)
        with pytest.raises(ValueError, match="binary weights"):
            hotspots.gi_star(self._noise(patch_cells), w, permutations=99, seed=SEED)

    def test_class_sign_agrees_with_z(self, patch_cells):
        w, _ = weights_mod.build(patch_cells, transform=weights_mod.BINARY)
        y = self._noise(patch_cells)
        y[:10] += 40.0
        frame, _ = hotspots.gi_star(y, w, permutations=9999, seed=SEED)
        hot = frame["hotspot_class"].str.startswith("HOT")
        cold = frame["hotspot_class"].str.startswith("COLD")
        assert (frame.loc[hot, "gi_z"] > 0).all()
        assert (frame.loc[cold, "gi_z"] < 0).all()


class TestFDR:
    def test_threshold_never_exceeds_alpha(self, patch_cells):
        rng = np.random.default_rng(SEED)
        for _ in range(5):
            p = rng.uniform(0, 1, size=500)
            assert esda.fdr(p, 0.05) <= 0.05

    def test_corrected_set_is_a_subset_of_the_uncorrected(self, patch_cells):
        w, _ = weights_mod.build(patch_cells, transform=weights_mod.BINARY)
        y = np.random.default_rng(SEED).poisson(5.0, size=len(patch_cells)).astype(float)
        y[:12] += 50.0
        frame, _ = hotspots.gi_star(y, w, permutations=9999, seed=SEED)
        corrected = set(frame.index[frame["significant"]])
        uncorrected = set(frame.index[frame["significant_uncorrected"]])
        assert corrected <= uncorrected

    def test_permutation_floor_can_make_bh_unable_to_reject(self):
        """The Phase 5 finding, pinned as a test.

        A permutation p-value cannot go below 1/(m+1). Benjamini-Hochberg
        rejects at rank k only when p_(k) <= k*alpha/n, so at rank 1 it needs
        1/(m+1) <= alpha/n. With m = 999 and n = 1942 that is 0.001 <= 2.6e-5,
        which is false -- so a lone genuinely extreme cell CANNOT be rejected,
        however extreme it is. This is why config/geo.toml sets 99,999
        permutations, and the test exists so that lowering it silently is not
        possible.
        """
        n, alpha = 1942, 0.05

        coarse = np.full(n, 0.5)
        coarse[0] = 1.0 / 1000  # the finest p 999 permutations allows
        fine = np.full(n, 0.5)
        fine[0] = 1.0 / 100_000  # what 99,999 permutations allows

        # `esda.fdr` returns alpha/n for BOTH -- the same number for "BH
        # rejected the strongest cell" and for "BH rejected nothing". That
        # ambiguity is the reason src/analysis/correction.py exists, and it is
        # why a build cannot read significance off the threshold alone.
        assert esda.fdr(coarse, alpha) == pytest.approx(alpha / n)
        assert esda.fdr(fine, alpha) == pytest.approx(alpha / n)

        # The DECISION differs, and only the recomputed BH step-up shows it.
        assert correction.bh_rejections(coarse, alpha) == 0
        assert correction.bh_rejections(fine, alpha) == 1

        d_coarse = correction.describe(coarse, alpha, permutations=999)
        d_fine = correction.describe(fine, alpha, permutations=99_999)
        assert d_coarse["resolution_limited"] is True
        assert d_coarse["fell_back_to_bonferroni"] is True
        assert d_fine["resolution_limited"] is False
        assert d_fine["fell_back_to_bonferroni"] is False
        assert d_coarse["permutations_required_for_rank_one"] == 38_839

    def test_configured_permutations_clear_the_bh_rank_one_bound(self):
        """`permutations` must be at least n/alpha for BH to be able to reject."""
        cfg = config.geo()["analysis"]
        m, alpha = int(cfg["permutations"]), float(cfg["alpha"])
        # ~1,942 cells fill Montgomery at r8; the bound is n/alpha.
        assert 1.0 / (m + 1) <= alpha / 1942, (
            f"{m} permutations put the p-value floor at {1 / (m + 1):.2e}, above "
            f"the rank-1 BH critical value {alpha / 1942:.2e} -- the FDR "
            "correction cannot reject a single isolated cell at this budget"
        )


# ---------------------------------------------------------------------------
# KDE
# ---------------------------------------------------------------------------


class TestKDE:
    @staticmethod
    def _points(n: int = 400, seed: int = SEED) -> np.ndarray:
        """Two Gaussian blobs, in metres. A known, non-degenerate density."""
        rng = np.random.default_rng(seed)
        a = rng.normal([340_000, 130_000], 800.0, size=(n // 2, 2))
        b = rng.normal([348_000, 138_000], 500.0, size=(n - n // 2, 2))
        return np.vstack([a, b])

    def test_scott_and_silverman_match_the_hand_computed_formulas(self):
        pts = self._points()
        n, d = pts.shape
        sd = pts.std(axis=0, ddof=1).mean()
        assert kde_mod.scott_bandwidth(pts) == pytest.approx(
            n ** (-1.0 / (d + 4)) * sd
        )
        assert kde_mod.silverman_bandwidth(pts) == pytest.approx(
            (n * (d + 2) / 4.0) ** (-1.0 / (d + 4)) * sd
        )
        # In TWO dimensions the two rules coincide exactly, and that is worth
        # pinning rather than glossing: Silverman's factor is
        # (n(d+2)/4)^(-1/(d+4)), and at d=2 the (d+2)/4 term is exactly 1, so
        # it collapses to Scott's n^(-1/(d+4)). They differ in 1-D and in 3-D
        # and not here. A reader who expects two different reference numbers in
        # the bandwidth table should see why there is one.
        assert kde_mod.silverman_bandwidth(pts) == pytest.approx(
            kde_mod.scott_bandwidth(pts)
        )

    def test_surface_integrates_to_one(self):
        pts = self._points()
        bw = 600.0
        xs, ys = kde_mod.make_grid(pts, pitch_m=100.0, pad_m=3 * bw)
        _, stats = kde_mod.surface(
            pts, bandwidth_m=bw, xs=xs, ys=ys, crs_epsg=26985, method="test"
        )
        assert stats["integral"] == pytest.approx(1.0, abs=0.02)
        assert kde_mod.integrates_to_one(stats)

    def test_a_clipped_grid_fails_the_integral_check(self):
        """The integral check is a real check on the grid, not a tautology."""
        pts = self._points()
        xs, ys = kde_mod.make_grid(pts, pitch_m=100.0, pad_m=0.0)
        # Clip hard, so the kernel's tails fall outside the grid entirely.
        xs, ys = xs[: len(xs) // 2], ys[: len(ys) // 2]
        _, stats = kde_mod.surface(
            pts, bandwidth_m=600.0, xs=xs, ys=ys, crs_epsg=26985, method="clipped"
        )
        assert not kde_mod.integrates_to_one(stats)

    def test_blocked_folds_never_share_a_block(self):
        rng = np.random.default_rng(SEED)
        groups = rng.choice([f"blk{i}" for i in range(40)], size=600)
        assert kde_mod.blocked_folds_are_disjoint(groups, folds=5)

    def test_blocked_cv_picks_a_wider_bandwidth_than_random_folds(self):
        """The leak, demonstrated rather than asserted in prose.

        Random folds put a point and its near neighbour on opposite sides of
        the split, so a too-small bandwidth scores well on held-out
        likelihood. Blocking by a spatial group removes that, and the selected
        bandwidth is never narrower for it.
        """
        rng = np.random.default_rng(SEED)
        # Tight clusters: each block is a knot of near-duplicate points, which
        # is what makes the random-fold score exploitable.
        centres = rng.uniform(300_000, 350_000, size=(30, 2))
        pts, groups = [], []
        for i, c in enumerate(centres):
            k = 30
            pts.append(rng.normal(c, 40.0, size=(k, 2)))
            groups += [f"blk{i}"] * k
        pts = np.vstack(pts)
        cands = kde_mod.candidate_bandwidths(50.0, 5000.0, 10)

        blocked, _, _ = kde_mod.select_bandwidth(
            pts, groups, candidates=cands, folds=5, seed=SEED, blocked=True
        )
        random, _, _ = kde_mod.select_bandwidth(
            pts, None, candidates=cands, folds=5, seed=SEED, blocked=False
        )
        assert blocked > random, (
            f"blocked CV chose {blocked:.0f} m, random folds {random:.0f} m -- "
            "the leak should push the random-fold answer smaller"
        )

    def test_blocked_cv_needs_enough_blocks(self):
        pts = self._points(50)
        with pytest.raises(ValueError, match="blocked CV wants at least"):
            kde_mod.select_bandwidth(
                pts, ["a"] * 25 + ["b"] * 25,
                candidates=kde_mod.candidate_bandwidths(100, 1000, 3),
                folds=5, blocked=True,
            )

    def test_candidates_are_log_spaced(self):
        c = kde_mod.candidate_bandwidths(100.0, 2000.0, 5)
        ratios = c[1:] / c[:-1]
        assert np.allclose(ratios, ratios[0])
        assert c[0] == pytest.approx(100.0)
        assert c[-1] == pytest.approx(2000.0)


# ---------------------------------------------------------------------------
# ST-DBSCAN
# ---------------------------------------------------------------------------


class TestSTDBSCAN:
    def test_two_thresholds_split_what_one_would_merge(self):
        """The whole point of the method, as a test.

        Two knots of points at the same place, six months apart. A space-only
        epsilon sees one cluster (the year-long smear); the space-AND-time
        epsilon sees two.
        """
        rng = np.random.default_rng(SEED)
        xy = np.vstack([
            rng.normal([340_000, 130_000], 30.0, size=(20, 2)),
            rng.normal([340_000, 130_000], 30.0, size=(20, 2)),
        ])
        t = np.concatenate([np.linspace(0, 5, 20), np.linspace(4400, 4405, 20)])

        both = st_dbscan.cluster(
            xy, t, eps_space_m=300.0, eps_time_h=6.0, min_samples=5
        )
        space_only = st_dbscan.cluster(
            xy, t, eps_space_m=300.0, eps_time_h=None, min_samples=5
        )
        assert len(set(both[both >= 0])) == 2
        assert len(set(space_only[space_only >= 0])) == 1

    def test_time_threshold_alone_does_not_merge_distant_points(self):
        rng = np.random.default_rng(SEED)
        xy = np.vstack([
            rng.normal([340_000, 130_000], 30.0, size=(20, 2)),
            rng.normal([360_000, 150_000], 30.0, size=(20, 2)),  # 28 km away
        ])
        t = np.zeros(40)  # simultaneous
        labels = st_dbscan.cluster(
            xy, t, eps_space_m=300.0, eps_time_h=6.0, min_samples=5
        )
        assert len(set(labels[labels >= 0])) == 2

    def test_neighbourhood_has_a_diagonal(self):
        """DBSCAN counts a point in its own neighbourhood for min_samples."""
        xy = np.array([[0.0, 0.0], [10_000.0, 0.0]])
        g = st_dbscan.neighbourhood(xy, np.zeros(2), eps_space_m=1.0, eps_time_h=1.0)
        assert g[0, 0] == st_dbscan.NEIGHBOUR
        assert g[1, 1] == st_dbscan.NEIGHBOUR
        assert g[0, 1] == 0  # not stored: 10 km apart

    def test_empty_clusters_carries_declared_types(self):
        import pyarrow as pa
        empty = st_dbscan.empty_clusters()
        assert isinstance(empty, pa.Table)
        assert empty.num_rows == 0
        assert empty.schema.field("run").type == pa.string()
        assert empty.schema.field("first_crash_date").type == pa.date32()
        assert list(empty.column_names) == st_dbscan.CLUSTER_COLUMNS


# ---------------------------------------------------------------------------
# CRS discipline
# ---------------------------------------------------------------------------


class TestCRS:
    def test_no_web_mercator_anywhere_in_src_analysis(self):
        """3857 is for tiles. It must not appear in a package full of metres.

        The Phase 4 negative-control test in tests/test_geo.py, which MEASURES
        how wrong 3857 is at 39.3N, remains the only place the number legally
        appears in tests/ -- and this file's own mentions of it are in prose,
        which is why the scan is over src/analysis/ code.
        """
        offenders = []
        for path in sorted(ANALYSIS_SRC.rglob("*.py")):
            with path.open("rb") as fh:
                for tok in tokenize.tokenize(fh.readline):
                    # Comments and string literals are where 3857 is EXPLAINED
                    # -- every metric operation in this package carries a note
                    # saying why it is not in Web Mercator. What must never
                    # appear is 3857 as an executable value.
                    if tok.type in (tokenize.COMMENT, tokenize.STRING):
                        continue
                    if re.search(r"\b3857\b", tok.string):
                        offenders.append(
                            f"{path.relative_to(REPO)}:{tok.start[0]}: {tok.line.strip()}"
                        )
        assert not offenders, "EPSG:3857 as code in src/analysis/:\n" + "\n".join(offenders)

    def test_configured_crs_are_projected_and_named(self):
        crs = config.geo()["analysis"]["crs"]
        assert crs["apportionment"] == 5070   # equal-area, for area ratios
        assert crs["local"] == 26985          # NAD83 / Maryland, metres
        assert 3857 not in crs.values()

    def test_local_crs_matches_the_phase_4_snap_zone(self):
        """A bandwidth and a snap distance must be the same kind of metre."""
        assert (config.geo()["analysis"]["crs"]["local"]
                == config.geo()["crs"]["snap"]["MD"])

    def test_projected_points_land_in_the_maryland_zone(self):
        crashes = pd.DataFrame({
            "longitude": [-77.2, -77.0], "latitude": [39.1, 39.05],
        })
        pts = frames.projected_points(crashes)
        assert pts.crs.to_epsg() == 26985
        # NAD83 / Maryland puts Montgomery County near (330-380 km E, 100-160 km N).
        assert (pts["x_m"].between(300_000, 420_000)).all()
        assert (pts["y_m"].between(80_000, 200_000)).all()


# ---------------------------------------------------------------------------
# the config <-> contract agreement
# ---------------------------------------------------------------------------


def test_contract_population_floor_matches_config():
    """The contract hardcodes the floor; a config change must not orphan it."""
    contract = contracts.load_contract(analysis_build.ANALYSIS_CONTRACT)
    rules = contract["tables"]["analysis.cell_stats_h3_r8"]["x-table-constraints"]["row_rules"]
    rule = next(r for r in rules if r["name"] == "rate_defined_iff_population")
    floor = int(config.geo()["analysis"]["min_population_for_rate"])
    assert f"population >= {floor}" in rule["predicate"], (
        f"config min_population_for_rate is {floor} but the contract rule says "
        f"{rule['predicate']!r} -- restate the contract or the rule is vacuous"
    )


def test_every_written_table_has_a_contract():
    contract = contracts.load_contract(analysis_build.ANALYSIS_CONTRACT)
    for name, (contract_name, _, _) in analysis_build.TABLE_SPECS.items():
        assert contract_name in contract["tables"], name


# ---------------------------------------------------------------------------
# the real build: apportionment, the rate floor, contracts, determinism
# ---------------------------------------------------------------------------


@pytest.fixture(scope="module")
def montgomery_bg(geo_reference_store):
    """TIGER block groups for Montgomery County, from the cached reference."""
    bg = frames.load_block_group_layer(geo_reference_store, "24")
    return bg[bg["GEOID"].str.startswith("24031")]


@pytest.fixture(scope="module")
def montgomery_cells(geo_reference_store):
    """The filled r8 cell universe for the county, and the interior subset."""
    counties = frames.load_county_layer(geo_reference_store)
    polygon = frames.county_polygon(counties, "24031")
    return frames.fill_cells(polygon, 8)


class TestCellFill:
    def test_overlap_fill_is_a_superset_of_the_centre_fill(self, montgomery_cells):
        """The reason `contain='overlap'` is not a detail.

        Centre-containment drops every cell whose centroid is outside the
        county but whose area is partly inside -- and a crash on the county
        line lands in exactly such a cell. Its cell would then have no row in
        the weights matrix.
        """
        cells, interior, stats = montgomery_cells
        assert set(interior) < set(cells)
        assert stats["contain_mode"] == "overlap"
        assert stats["cells_on_boundary"] > 0
        assert stats["cells_overlap"] == len(cells)

    def test_cell_polygons_are_in_the_county_and_the_right_way_round(
        self, montgomery_cells
    ):
        """(lat, lng) vs (x, y): getting it backwards is a silent zero.

        `h3.cell_to_boundary` yields (lat, lng); shapely wants (lng, lat).
        Swapped, the polygons land in the Indian Ocean, intersect no block
        group, and every population comes back zero -- with no exception
        raised anywhere.
        """
        cells, _, _ = montgomery_cells
        polys = frames.cell_polygons(cells[:200])
        minx, miny, maxx, maxy = polys.total_bounds
        assert -78.0 < minx < maxx < -76.0, "longitude is not in Maryland"
        assert 38.5 < miny < maxy < 39.8, "latitude is not in Maryland"

    def test_every_cell_is_a_valid_r8_id(self, montgomery_cells):
        cells, _, _ = montgomery_cells
        assert all(h3.get_resolution(c) == 8 for c in cells)
        assert cells == sorted(cells)


class TestApportionment:
    def test_is_mass_preserving(self, montgomery_cells, montgomery_bg, geo_root):
        """No person is created or destroyed. The whole rate rests on this."""
        cells, _, _ = montgomery_cells
        con = frames.connect()
        try:
            pops = frames.load_block_group_population(con, geo_root, "24031")
        finally:
            con.close()
        stats = frames.assert_mass_preserved(cells, montgomery_bg, pops)
        assert stats["relative_error"] < 1e-9
        assert stats["population_allocated"] > 0
        assert stats["block_groups"] == len(pops)

    def test_areas_are_computed_in_an_equal_area_crs(
        self, montgomery_cells, montgomery_bg, geo_root
    ):
        """The apportionment is a ratio of areas; 5070 is what makes it honest."""
        cells, _, _ = montgomery_cells
        con = frames.connect()
        try:
            pops = frames.load_block_group_population(con, geo_root, "24031")
        finally:
            con.close()
        _, stats = frames.apportion_population(cells[:400], montgomery_bg, pops)
        assert stats["crs_epsg"] == 5070

    def test_a_block_group_inside_one_cell_gives_it_everything(self):
        """The degenerate case, checked directly rather than inferred.

        A synthetic block group wholly inside a single r8 cell must allocate
        100% of its population to that cell and nothing anywhere else.
        """
        import geopandas as gpd
        import shapely

        cell = h3.latlng_to_cell(39.1, -77.2, 8)
        cells = sorted(h3.grid_disk(cell, 1))
        boundary = shapely.Polygon(
            [(lng, lat) for lat, lng in h3.cell_to_boundary(cell)]
        )
        # A tiny square well inside the target cell.
        cx, cy = boundary.centroid.x, boundary.centroid.y
        tiny = shapely.box(cx - 0.0004, cy - 0.0004, cx + 0.0004, cy + 0.0004)
        assert boundary.contains(tiny)

        bg = gpd.GeoDataFrame(
            {"GEOID": ["240310000001"]}, geometry=[tiny], crs="EPSG:4326"
        )
        pops = pd.DataFrame({
            "bg_geoid": ["240310000001"], "county_geoid": ["24031"],
            "population": [1000], "aland_m2": [10_000], "awater_m2": [0],
        })
        allocated, stats = frames.apportion_population(cells, bg, pops)
        target = allocated.set_index("h3_cell")
        assert target.loc[cell, "population"] == pytest.approx(1000.0)
        assert target["population"].sum() == pytest.approx(1000.0)
        assert (target.drop(index=cell)["population"] == 0).all()


class TestRealBuild:
    def test_every_table_validates_against_the_contract(
        self, analysis_root, analysis_con
    ):
        """The build validates before writing; this re-checks what landed."""
        contract = contracts.load_contract(analysis_build.ANALYSIS_CONTRACT)
        violations = []
        for name, (contract_name, columns, _) in analysis_build.TABLE_SPECS.items():
            path = analysis_root / f"{name}.parquet"
            if not path.exists():
                continue
            violations += contracts.validate_relation(
                analysis_con, f"analysis_{name}", contract, contract_name,
                check_row_count_min=False,
            )
        assert not violations, "\n".join(v.render() for v in violations)

    def test_column_order_is_the_contract_order(self, analysis_root):
        import pyarrow.parquet as pq
        for name, (_, columns, _) in analysis_build.TABLE_SPECS.items():
            path = analysis_root / f"{name}.parquet"
            if path.exists():
                assert pq.ParquetFile(path).schema.names == columns, name

    def test_zero_crash_cells_are_present(self, analysis_con):
        """A hot-spot test over only the non-zero cells conditions on the outcome."""
        n = analysis_con.execute(
            "SELECT COUNT(*) FROM analysis_cell_stats_h3_r8 WHERE n_crashes = 0"
        ).fetchone()[0]
        assert n > 0, "the fill produced no empty cells -- it is not a fill"

    def test_rate_floor_is_applied_exactly(self, analysis_con):
        floor = int(config.geo()["analysis"]["min_population_for_rate"])
        bad = analysis_con.execute(f"""
            SELECT COUNT(*) FROM analysis_cell_stats_h3_r8
            WHERE rate_is_defined <> (population >= {floor})
        """).fetchone()[0]
        assert bad == 0

    def test_no_infinite_or_nan_rate_survives(self, analysis_con):
        bad = analysis_con.execute("""
            SELECT COUNT(*) FROM analysis_cell_stats_h3_r8
            WHERE rate_per_1k_pop IS NOT NULL AND NOT isfinite(rate_per_1k_pop)
        """).fetchone()[0]
        assert bad == 0
        nulls_where_defined = analysis_con.execute("""
            SELECT COUNT(*) FROM analysis_cell_stats_h3_r8
            WHERE rate_is_defined AND rate_per_1k_pop IS NULL
        """).fetchone()[0]
        assert nulls_where_defined == 0

    def test_severity_counts_nest(self, analysis_con):
        bad = analysis_con.execute("""
            SELECT COUNT(*) FROM analysis_cell_stats_h3_r8
            WHERE n_fatal > n_injury OR n_injury > n_crashes
        """).fetchone()[0]
        assert bad == 0

    def test_significance_flags_agree_with_their_thresholds(self, analysis_con):
        """The invariant the contract's row_rules enforce, checked on disk too."""
        for table in ("analysis_lisa_h3_r8", "analysis_gi_star_h3_r8"):
            bad = analysis_con.execute(
                f"SELECT COUNT(*) FROM {table} WHERE significant <> (p_sim <= p_fdr)"
            ).fetchone()[0]
            assert bad == 0, table

    def test_corrected_significance_is_a_subset_of_uncorrected(self, analysis_con):
        for table in ("analysis_lisa_h3_r8", "analysis_gi_star_h3_r8"):
            bad = analysis_con.execute(
                f"SELECT COUNT(*) FROM {table} "
                f"WHERE significant AND NOT significant_uncorrected"
            ).fetchone()[0]
            assert bad == 0, table

    def test_hotspot_class_sign_agrees_with_z(self, analysis_con):
        bad = analysis_con.execute("""
            SELECT COUNT(*) FROM analysis_gi_star_h3_r8
            WHERE hotspot_class <> 'NS'
              AND (gi_z > 0) <> (hotspot_class LIKE 'HOT%')
        """).fetchone()[0]
        assert bad == 0

    def test_every_statistic_row_has_a_cell(self, analysis_con):
        for table in ("analysis_lisa_h3_r8", "analysis_gi_star_h3_r8",
                      "analysis_hotspot_contrast"):
            orphans = analysis_con.execute(f"""
                SELECT COUNT(*) FROM {table} t
                WHERE NOT EXISTS (SELECT 1 FROM analysis_cell_stats_h3_r8 c
                                  WHERE c.h3_r8 = t.h3_r8)
            """).fetchone()[0]
            assert orphans == 0, table

    def test_contrast_rows_actually_differ(self, analysis_con):
        bad = analysis_con.execute("""
            SELECT COUNT(*) FROM analysis_hotspot_contrast
            WHERE class_raw_count = class_rate_per_1k_pop
        """).fetchone()[0]
        assert bad == 0

    def test_kde_surface_is_in_a_projected_crs_and_finite(self, analysis_con):
        row = analysis_con.execute("""
            SELECT COUNT(*) AS n,
                   COUNT(DISTINCT crs_epsg) AS crs_count,
                   MIN(crs_epsg) AS crs,
                   COUNT(*) FILTER (WHERE NOT isfinite(density)) AS bad
            FROM analysis_kde_surface
        """).fetchone()
        assert row[0] > 0
        assert row[1] == 1
        assert row[2] == config.geo()["analysis"]["crs"]["local"] == 26985
        assert row[3] == 0

    def test_exclusion_ledger_balances(self, analysis_manifest):
        ex = analysis_manifest["stats"]["exclusions"]
        assert ex["rows_kept"] + ex["excluded_total"] == ex["rows_considered"]
        assert sum(ex["excluded_by_reason"].values()) == ex["excluded_total"]

    def test_manifest_records_what_the_prose_needs(self, analysis_manifest):
        """Nothing in ANALYSIS.md may be a number the manifest cannot produce."""
        inputs = analysis_manifest["inputs"]
        for key in ("analysis_build_sha", "crash_geo_sha256", "period",
                    "seed", "permutations", "alpha", "study_area"):
            assert key in inputs, key
        stats = analysis_manifest["stats"]
        for key in ("exclusions", "cell_fill", "apportionment", "cell_stats",
                    "weights", "lisa", "gi_star", "contrast",
                    "clustering_exists"):
            assert key in stats, key
        for variable, s in stats["lisa"].items():
            assert "I" in s["global"] and "p_sim" in s["global"], variable

    def test_build_sha_is_a_hash_of_inputs_not_of_time(self, geo_root):
        area = frames.study_area_from_config()
        hashes = analysis_build.input_hashes(geo_root)
        a = analysis_build.build_sha(hashes, area)
        b = analysis_build.build_sha(hashes, area)
        assert a == b and len(a) == 64
        # A different period is a different analysis, so a different sha.
        other = frames.study_area_from_config(period="2020-01-01:2021-12-31")
        assert analysis_build.build_sha(hashes, other) != a


class TestDeterminism:
    def test_two_builds_are_byte_identical(self, tmp_path, geo_root):
        """The Phase 5 idempotency claim, over the parquet files.

        Figures are excluded by construction (matplotlib embeds its version in
        the PNG) and so is `_analysis_manifest.json` (`built_at` is wall clock
        by design). Everything a number in ANALYSIS.md comes from is a parquet
        file, and those must match to the byte.
        """
        from tests.conftest import build_analysis_into

        def run(name: str) -> dict[str, str]:
            dest = tmp_path / name
            build_analysis_into(dest, geo_root, skip_figures=True)
            return {
                p.name: hashlib.sha256(p.read_bytes()).hexdigest()
                for p in sorted(dest.glob("*.parquet"))
            }

        first, second = run("a"), run("b")
        assert first == second, "analysis parquet is not reproducible"
        assert first, "the build wrote no parquet at all"

    def test_manifest_differs_only_in_wall_clock(self, tmp_path, geo_root):
        from tests.conftest import build_analysis_into

        payloads = []
        for name in ("m1", "m2"):
            dest = tmp_path / name
            build_analysis_into(dest, geo_root, skip_figures=True)
            payload = json.loads((dest / "_analysis_manifest.json").read_text())
            payload.pop("built_at")
            # Both point at their own tmp directory, which is not a result.
            payload.pop("out_root")
            for info in payload["outputs"].values():
                info.pop("path", None)
            payloads.append(payload)
        assert payloads[0] == payloads[1]
