# Phase 5 — spatial analysis: implementation report

Scope: `src/analysis/` (frames, weights, correction, lisa, hotspots, kde,
st_dbscan, figures, build), `contracts/analysis.schema.json`,
`config/geo.toml [analysis]`, `tests/test_analysis.py`, `ANALYSIS.md`,
`output/figures/`.

`ANALYSIS.md` is the graded artefact. This is the author's copy: what I built,
where the brief and the data disagreed, what I bounded, what the tests caught,
and every measurement with the command that reproduces it.

---

## What I built

**`frames.py`** — the one place the analysis corpus is defined. `StudyArea` is
a frozen dataclass carrying county, sources, period, resolution, k and the rate
floor; every other module takes it rather than re-reading config, which is what
makes the `--sensitivity` run the same code with different bounds. `load_crashes`
joins `crash_geo` to `fact_crash` for `severity_ordinal` and returns an
`ExclusionLedger` that must balance (`total_in - dropped == kept`, asserted).
`fill_cells` fills the county polygon with r8 cells in **overlap** mode and
returns the wholly-inside subset beside it. `apportion_population` intersects
block groups with cells in **EPSG:5070** and normalises weights per block group
so the allocation is mass-preserving by construction. `cell_stats` assembles the
per-cell table and *asserts* that no crash's cell is outside the universe.
`projected_points` is the only reprojection of the point set (4326 → 26985).

**`weights.py`** — `libpysal.W` from `h3_index.disk_weights`, built from a
sorted id list with an explicit `id_order`, refusing duplicates, asserting
symmetry, and reporting islands *and* connected components. Row-standardised
for Moran's I (the lag is a neighbourhood mean), binary for Gi\* (the statistic
is a neighbourhood sum) — the module docstring argues each.

**`correction.py`** — not in the brief; added because `esda.fdr`'s return value
is ambiguous in a way that changes a published finding. See "Bugs the tests and
the data caught" below.

**`lisa.py`** — global `esda.Moran` (bracketed by `np.random.seed`, because it
has no `seed=` parameter and draws from the global RNG) and `esda.Moran_Local`,
both `alternative="two-sided"`, FDR-corrected, quadrants classified only where
significant. `clusters_exist` is the gate the prose has to pass.

**`hotspots.py`** — `esda.G_Local(star=True)`, which *refuses* a
row-standardised W rather than silently re-transforming it. `classify` maps
(z, p) to the five classes with two separately computed BH thresholds.
`contrast` produces the raw-vs-normalised table with `place_names` labels from
the modal snapped OSM road.

**`kde.py`** — Scott/Silverman closed forms, `GridSearchCV` bandwidth selection
with `GroupKFold` on H3 r7 (and plain `KFold` for the leak contrast), and the
surface as a binned Gaussian convolution verified against sklearn's exact
estimator every build.

**`st_dbscan.py`** — the two-threshold neighbourhood as a sparse 0/1 graph
(cKDTree for space, a time filter on the surviving pairs) handed to
`DBSCAN(metric="precomputed")`. Clustering on `crash_datetime_utc`; the
day-of-week/hour profile on `crash_datetime_local`, because "Friday night" is a
local-clock concept and an elapsed-time difference is not.

**`figures.py`** — eight PNGs, one shared palette, every figure carrying study
area, period, N, the correction and the CRS of any distance shown.

**`build.py`** — the CLI, the manifest, contract validation before the first
write, and the warnings that stop a resolution-limited null from being
published as a finding.

---

## Things the brief or the spec said that the data doesn't do

### 1. 999 permutations makes the FDR correction unable to reject anything

The most consequential finding of the phase. The brief specifies 999
permutations. On this corpus that produces a **completely empty** corrected
Gi\* map for the per-capita rate — and it is an artefact of the permutation
budget, not a property of the crashes.

A permutation p-value's floor is `1/(m+1)`. Benjamini–Hochberg rejects at rank
*k* only when `p_(k) <= k*alpha/n`. At rank 1 that requires
`1/(m+1) <= alpha/n`, i.e.

```
m + 1 >= n / alpha = 1942 / 0.05 = 38,840
```

At m = 999 the floor is 1e-03 and the rank-1 critical value is 2.57e-05, so a
lone genuinely extreme cell **cannot be rejected however extreme it is**.
`esda.fdr` then returns `alpha/n`, which is indistinguishable in the output
from a genuine very-strict FDR threshold.

Measured (two-sided p, which is the correct null for a hot/cold classification):

| permutations | Gi\* raw, BH-significant | Gi\* per-capita, BH-significant |
|---:|---:|---:|
| 999 | 0 | 0 |
| 9,999 | 63 | 0 |
| **99,999** | **124** | **1** |

Raised to 99,999 in `config/geo.toml`, with the arithmetic in the comment.
Going further does not help: at this budget the rate's second-smallest p is
2.1e-04 against a critical value of 7.4e-05, so those p-values are *resolved*
and simply do not beat their thresholds. Cost is ~13 s of a 1m42s build.

`tests/test_analysis.py::TestFDR::test_configured_permutations_clear_the_bh_rank_one_bound`
fails if anyone lowers it.

### 2. `esda`'s default `alternative` is one-sided, and it is about to change

`esda.Moran_Local` and `G_Local` default to `alternative=None`, which resolves
to `'directed'` — a one-sided p in whichever direction the statistic fell — and
emit a DeprecationWarning saying the default becomes `'two-sided'` next major
release.

This is a correctness issue, not a warning to silence. This phase classifies
**both** HOT and COLD from one p-value, so a one-sided p makes every reported
α = 0.05 an effective 0.10. Set explicitly to `"two-sided"` in both modules.
It halves the apparent significance and it is the right number: Gi\* raw
uncorrected drops from 854 to 629, and the corrected count from 447 to 124.
It also pins the behaviour, so a dependency upgrade cannot move a published
p-value.

### 3. `esda` returns p-values greater than 1

Measured: 1.00122 on two raw-count cells at 99,999 permutations. The two-sided
p is twice the smaller tail with a continuity correction on both sides, and
when the observed statistic sits almost exactly at the median of its null
distribution the doubling overshoots. Both affected cells have |z| < 0.08, i.e.
they are the most thoroughly non-significant cells on the map, so clamping
cannot change a classification — but a p above 1 is not a p, and it fails the
output contract's `[0, 1]` range check. `correction.clamp_pvalues` clamps and
**counts**; the count is in the manifest, so if this ever happens to a cell
that is not at z ≈ 0, the number is what would show it.

### 4. `esda.fdr` returns the same number for two opposite outcomes

`esda.fdr(p, alpha)` returns `alpha/n` both when BH rejects exactly the single
strongest cell (the rank-1 critical value *is* `alpha/n`) and when BH rejects
nothing at all (its explicit fallback branch). The output table cannot tell
them apart, and on this corpus both happened — the first at 99,999
permutations, the second at 999.

`correction.py` recomputes the BH step-up independently and reports
`bh_rejections`, `fell_back_to_bonferroni` and `resolution_limited` beside the
threshold. The threshold itself still comes from `esda.fdr`; this is a
diagnostic, not a replacement. The build warns on both conditions.

### 5. Scott and Silverman are the same number in two dimensions

Silverman's factor is `(n(d+2)/4)^(-1/(d+4))`; at d = 2 the `(d+2)/4` term is
exactly 1 and it collapses to Scott's `n^(-1/(d+4))`. Both give 1,261 m here.
Reported as two rows anyway so a reader can see they agree rather than being
told, but only one surface is computed.

### 6. ST-DBSCAN at the brief's implied thresholds finds nothing

300 m / 6 h / `min_samples` 10 returned **zero** clusters over 70,692 crashes.
Ten crashes within 300 m and six hours of each other is a pile-up, not a
pattern. Replaced with a measured sweep (in `config/geo.toml`); 300 m / 72 h /
5 gives 120 clusters at a median span of 4.0 days against the space-only run's
2,199.5 days.

### 7. "A recurring Friday-night corridor" is not what ST-DBSCAN finds

The assignment's phrase describes *cyclic* recurrence; ST-DBSCAN over linear
time finds *bursts*. Two crashes on consecutive Fridays are 168 hours apart and
no `eps_time` links them without linking the whole intervening week. The
clusters confirm it: the modal weekday holds 17–43% of each cluster against 14%
for no pattern. `ANALYSIS.md` says so rather than relabelling bursts.

### 8. The per-capita weights matrix is disconnected; the raw one is not

Dropping the 591 cells below the population floor splits the remaining 1,351
into two components — the Agricultural Reserve's unpopulated cells were the
bridge between the upcounty settlements and the rest of the county. libpysal
warns about this; `weights.build` now counts components so it is a manifest
field rather than a log line. It is a property of the *filter*, not the
crashes, and it is one more reason the normalised map is weaker.

### 9. The `.gitignore` in the working tree does the opposite of the brief

`git diff .gitignore` **removes** `IMPLEMENTATION_GUIDE.md` and `ai docs/` from
the ignore list, i.e. it makes them trackable — while the brief §7 says never to
commit `ai docs/`. Left uncommitted and flagged rather than committed as a
"chore(gitignore)". The `requirements.txt` diff was likewise not the analysis
libraries the brief predicted (it was `ipykernel`/`matplotlib` for the EDA
notebook); the four analysis libraries were appended and committed separately.

---

## What I bounded, and what the unbounded version costs

- **Texas: not analysed.** The slice is OIDs 1–100,000 of 3,088,450 (92,703
  geocoded, all 254 counties, 2020–2024). Two reasons, and the second is
  decisive. The OID selection rule is undocumented and verifying it means
  sweeping the full table — Phase 1 measured that at 4.6 h and 13 GB. And the
  density is fatal to the method: 29,832 r8 cells at 3.1 crashes each, where
  filling the state polygon would give ~950,000 cells averaging 0.1. Gi\* over
  that tests the OID assignment as much as the crash process. A defensible
  Texas analysis is county-level or r5/r6 over the metros, and it is a
  different study.
- **FARS: not analysed.** A tri-state fatal-only r7 surface is legitimate and
  would have taken ~2 h including the write-up. It would also have needed its
  own study-area and period argument and would have competed with the
  Montgomery write-up for the reader's attention.
- **k=2 sensitivity: not run.** `weights.build(k=2)` works and is tested (18
  neighbours); running both variables at both k with 99,999 permutations is
  ~4 minutes of compute and about 400 words of interpretation that would say
  "the corridor is still the corridor". Worth doing before publication, not
  before the three required analyses.
- **The `--sensitivity` period (2015–2025) is wired but its numbers are not in
  `ANALYSIS.md`.** One `python -m src.analysis.build --sensitivity --out-root
  …` run, ~2 minutes.
- **Isochrones / Valhalla: out of scope by the brief, and correctly so.**
  Endpoint `https://valhalla1.openstreetmap.de/isochrone`, no key, fair use,
  send an `X-Client-Id`. The fact worth carrying to MEMO.md is that network
  distance exceeds Euclidean by roughly **1.2–1.4×** in US road grids, so any
  "within 10 km of a trauma centre" buffer overstates coverage by that factor.
  Crash-to-trauma-centre access is a next-quarter item.

---

## Bugs the tests and the data caught

1. **DuckDB returns `crash_geo.geometry` as `GEOMETRY`, not `BLOB`**, when the
   spatial extension decodes the GeoParquet metadata — but as `BLOB` without
   it. `ST_GeomFromWKB(GEOMETRY)` is a BinderException. Now the accessor is
   chosen from `typeof()`, so both work.
2. **`DATE` arrives from `.df()` as `datetime64[us]`**, and pandas *refuses* to
   compare it to a `datetime.date` rather than coercing. Caught immediately;
   the period bounds are lifted to `pd.Timestamp` explicitly.
3. **An empty pandas frame carries no types into DuckDB.** An empty object
   column registers as `INTEGER` and an empty `datetime64` as `TIMESTAMP`, so
   `--skip-st-dbscan` and "the contrast found nothing" both failed the
   contract's *type* check for reasons unrelated to the data. Both now have
   declared pyarrow schemas (`CLUSTER_SCHEMA`, `CONTRAST_SCHEMA`) and `build`
   registers pandas or arrow. `st_dbscan` needed both shapes — arrow cannot go
   through `pd.concat`, pandas cannot carry zero-row types — so `_empty_frame`
   and `empty_clusters` are deliberately separate and `run` converts at the
   boundary.
4. **The planted-cluster test was wrong, not the code.** It asserted all seven
   cells of a planted patch come back `HOT_99`; the patch's outer ring has
   three unboosted neighbours each, so `HOT_95` there is the *correct* answer
   for a neighbourhood statistic. Now it asserts all seven are hot and the
   centre is `HOT_99`.
5. **The CRS scan flagged its own documentation.** Scanning lines for `3857`
   caught the comments explaining why 3857 is never used. Rewritten to tokenise
   and skip `COMMENT` and `STRING` tokens, so it tests code and not prose.
6. **The KDE candidate range censored its own optimum.** The first range
   (100–2,000 m) returned 2,000 m — the top of the range — and the build's
   `at_range_edge` warning caught it. A maximum on a boundary is not a maximum.
   Widened to 16 km, where the curve has a genuine interior maximum at 2,947 m
   and falls away on both sides.

---

## The evaluator swap, and why it is not a shortcut

`sklearn.neighbors.KernelDensity.score_samples` over the published grid
(442,500 cells × 70,692 crashes) ran for **over 25 minutes per bandwidth**, and
there are three distinct bandwidths. A synthetic benchmark under-predicted this
badly, because crash points lie along a road network spread across a 50 km
county and the tree can barely prune; `rtol=1e-4` only brought it to ~4.4 min.

A kernel density estimate *is* the point measure convolved with the kernel, so
on a regular grid the estimate is a convolution:
`scipy.ndimage.gaussian_filter` on the 2-D histogram computes the same object
in O(grid) instead of O(grid × points). Runtime went to **~1 second**. The two
error terms are binning each crash to its 100 m cell centre and truncating the
kernel at 4σ, and both are bounded and measured rather than asserted — the
build evaluates sklearn's exact estimator on a sample of live grid cells every
run:

| bandwidth | max relative error vs exact |
|---:|---:|
| 2,947 m (the recommended surface) | 6.53e-03 |
| 1,261 m | 1.05e-02 |
| 500 m | 4.78e-02 |

The error scales with the bin-to-bandwidth ratio, as it must. sklearn is still
used for the thing it is good at: pointwise held-out likelihood during
bandwidth selection.

---

## Verification

**Two-build byte identity.** `TestDeterminism::test_two_builds_are_byte_identical`
runs the real build twice into two `tmp_path` directories and compares sha256
over every parquet file. `test_manifest_differs_only_in_wall_clock` compares
the manifests with `built_at` and the (tmp) output paths removed. Figures are
explicitly outside the claim — matplotlib embeds a version string — and the
data behind every figure is a parquet table that is inside it.

**Locality of a one-row change.** A local statistic is a function of the cell
and its k=1 neighbourhood, so a changed `crash_geo` row can only move that
cell's `cell_stats` row and the local statistics in its ring. The *global* I
and the FDR threshold are functions of the whole map and do move, and because
the threshold moves a distant cell can cross it — that is a property of
multiple-testing correction, not a bug. The build docstring states this; the
cell-level claim is what the contracts and the FK tests pin.

**Test counts.**

| run | result | wall clock |
|---|---|---:|
| fixture corpus, whole suite | **322 passed, 4 xfailed** | 1m15s |
| fixture corpus, `tests/test_analysis.py` | 62 passed | 25s |
| `CRASH_TEST_FULL_BRONZE=1`, whole suite | **317 passed, 5 skipped, 4 xfailed** | 10m41s |
| `CRASH_TEST_FULL_BRONZE=1`, `tests/test_analysis.py` | 62 passed, 0 skipped | 5m42s |

Phase 5 adds 62 tests to Phase 4's 260, and the 260 still pass unchanged. The
5 skips under full bronze are pre-existing fixture-extract tests that are
vacuous against the real corpus; no analysis test skips under either corpus,
which matters because a silently skipped integration test is indistinguishable
from a passing one.

The analysis suite is slower on the full corpus (5m42s vs 25s) for a reason
worth knowing: the cell universe is the FILLED COUNTY, so it is 1,942 cells
either way — what grows is the geo build feeding it, not the statistics.

**`grep -rn 3857 src/analysis` returns nothing**, including in prose: every
CRS comment in the package names Web Mercator in words instead, so the
reviewer's one-line check is unambiguous. `tests/test_analysis.py` tokenises
every module and asserts no such EPSG code appears as an executable value,
skipping COMMENT and STRING tokens -- the first version of that test flagged
its own docstrings.

**No network, no mocks.** Every statistical assertion is on a synthetic field
with a known answer (a smooth gradient, its own shuffle, Poisson noise with a
planted patch); every pipeline assertion is on a real build from the committed
bronze extract. The reference store is opened `offline=True`.

---

## The measurements

Every number in `ANALYSIS.md`, with the command. All of these are fields in
`data/gold/analysis/_analysis_manifest.json` after
`python -m src.analysis.build`; the JSON path is given where it is not obvious.

### Corpus (`stats.exclusions`)

| | |
|---|---:|
| Montgomery feed rows considered | 125,005 |
| no usable coordinate | 105 |
| outside county 24031 | 288 |
| outside 2019-01-01…2025-12-31 | 53,920 |
| **study corpus** | **70,692** |
| injury crashes (severity_ordinal ≥ 2) | 20,942 |
| fatal crashes (= 5) | 252 |
| with a named snapped road | 54,486 |

Out-of-county detail (`stats.exclusions.detail.outside_by_county_geoid`):
24033 Prince George's 162, 11001 DC 46, 51059 Fairfax VA 32, 24021 Frederick 29,
24027 Howard 17, 24013 Carroll 1, 51013 Arlington VA 1. (Frederick is 24021 and
Howard is 24027 — an easy pair to transpose, and the first draft of
`ANALYSIS.md` did.)

### Cells (`stats.cell_fill`, `stats.cell_stats`, `stats.apportionment`)

| | |
|---|---:|
| r8 cells overlapping the county | 1,942 |
| wholly inside | 1,705 |
| straddling the boundary | 237 |
| with ≥ 1 crash | 1,402 |
| with zero crashes | 540 |
| below the 50-resident floor | 591 |
| of those, containing crashes | 228 |
| apportionment relative error | 0.0 |
| apportionment CRS | EPSG:5070 |

### Weights (`stats.weights`)

| universe | n | islands | components | at full degree 6 |
|---|---:|---:|---:|---:|
| raw_count | 1,942 | 0 | 1 | 1,748 |
| rate_per_1k_pop | 1,351 | 0 | **2** | 1,119 |

### Global Moran's I (`stats.lisa.<var>.global`)

| variable | I | E[I] | z | p_sim |
|---|---:|---:|---:|---:|
| raw_count | 0.5044 | −0.00052 | 37.94 | 1e-05 (floor) |
| rate_per_1k_pop | 0.1890 | −0.00074 | 11.80 | 1e-05 (floor) |

### LISA (`stats.lisa.<var>.local`)

| variable | uncorrected | after BH | HH | LL | LH | HL |
|---|---:|---:|---:|---:|---:|---:|
| raw_count | 637 | 132 | 55 | 75 | 2 | 0 |
| rate_per_1k_pop | 193 | 1 | 0 | 1 | 0 | 0 |

### Gi\* (`stats.gi_star.<var>`)

| variable | n | uncorr. α=.05 | BH α=.05 | BH α=.01 | Bonferroni | BH threshold |
|---|---:|---:|---:|---:|---:|---:|
| raw_count | 1,942 | 629 | 124 | 0 | 2.57e-05 | 3.19e-03 |
| rate_per_1k_pop | 1,351 | 193 | 1 | 0 | 3.70e-05 | 3.70e-05 |

Classes: raw_count HOT_95 55, COLD_95 69, NS 1,818. rate COLD_95 1, NS 1,350.
`p_sim_clamped_to_unit_interval`: 2 (raw_count), 0 (rate).

### Contrast (`stats.contrast`)

650 of 1,942 cells change class: `RATE_UNDEFINED` 591, `HOT_RAW_ONLY` 55,
`COLD_RAW_ONLY` 4, `HOT_RATE_ONLY` **0**. Named top cells are in
`stats.top_hot_cells.raw_count`.

### KDE (`stats.kde`)

| method | bandwidth (m) | integral | peak /km² | max rel. err vs exact |
|---|---:|---:|---:|---:|
| cv_blocked | 2,947.23 | 1.0000 | 190.8 | 6.53e-03 |
| scott | 1,260.65 | 1.0000 | 341.9 | 1.05e-02 |
| silverman | 1,260.65 | — (shared) | 341.9 | — |
| practitioner | 500.00 | 1.0000 | 941.9 | 4.78e-02 |

Blocked CV 2,947 m vs random-fold 276 m, ratio **10.68**. Grid 100 m,
442,500 cells, EPSG:26985. Full likelihood tables in `stats.kde.cv_table` and
`stats.kde.cv_table_random_folds`.

### ST-DBSCAN (`stats.st_dbscan`)

| run | clusters | clustered | noise | median size | median span | max span |
|---|---:|---:|---:|---:|---:|---:|
| spatiotemporal | 120 | 677 | 70,015 | 5 | 4.0 d | 10 d |
| space_only | 168 | 69,508 | 1,184 | 9 | 2,199.5 d | 2,556 d |

Span ratio 549.88. Largest space-only cluster: 66,900 crashes.

### Outputs

| table | rows | sha256 (first 16) |
|---|---:|---|
| cell_stats_h3_r8 | 1,942 | 6a814d6e09acddba |
| lisa_h3_r8 | 3,293 | aae14948ef0f3e2f |
| gi_star_h3_r8 | 3,293 | 411a8eb2d685994e |
| hotspot_contrast | 650 | 760e0d687a57b8b9 |
| kde_surface | 1,770,000 | 85ef707e5e24dc40 |
| st_clusters | 288 | 0eb4f37f5be8d657 |
| crash_year_counts | 12 | 65a91889d3a64c36 |

Build wall clock 1m42s on the full corpus. `cell_stats_h3_r8` changed hash
once during the phase, when `_geo_build_sha` stopped being an empty string --
which is exactly what a lineage column moving is supposed to look like.

### The permutation sweep (reproduce)

```
for m in 999 9999 99999; do
  python -m src.analysis.build --permutations $m --skip-kde --skip-st-dbscan \
    --skip-figures --out-root /tmp/perm_$m --json | jq '.stats.gi_star'
done
```

### The ST-DBSCAN sweep (reproduce)

The sweep table in `config/geo.toml [analysis.st_dbscan]` comes from calling
`st_dbscan.cluster` directly over the projected corpus at each
(eps_space, eps_time, min_samples) triple; `frames.load_crashes` +
`frames.projected_points` supply the input.

---

## For DATA_QUALITY.md

- **228 r8 cells contain crashes but fewer than 50 apportioned residents.**
  Not a defect in itself, but it means any per-capita rate at this grain is
  undefined for 30% of the cells that matter, and it is the reason the
  normalised hot-spot map is empty. Worth stating anywhere a per-capita crash
  rate is quoted.
- **The Georgia Avenue cell at 903 crashes and 3,687 residents** (245 per
  1,000 over seven years) is not implausible for a major arterial, but it is
  the kind of number that should be spot-checked against a coordinate-stacking
  artefact: if an agency geocodes to a block centroid, one centroid can absorb
  a corridor. Phase 4's snapping gives a handle on this — a cell whose crashes
  all snap to one `osm_way_id` at near-identical `offset_m` is stacked.
- **58 of the 75 LL cells have no snapped road name at all**, i.e. they contain
  no crash that snapped to a named OSM way. Expected for the Agricultural
  Reserve, and a useful negative control: if a *down-county* cell ever shows
  this, the snap failed rather than the crashes being absent.
- **Two cells returned p-values above 1** from `esda` (1.00122). Clamped and
  counted; see finding 3.

## For DECISIONS.md

| decision | chosen | rejected alternative | why |
|---|---|---|---|
| study area | `pip_county_geoid = '24031'` | `jurisdiction = 'MD'` | the polygon is where the crash was; the label is where the report was filed. The label keeps 241 out-of-county rows with no denominator, including 46 in DC. |
| source scope | `MONTGOMERY_MD` only | + 49 FARS-primary in-county rows | FARS is fatal-only 2019–2024 against all-severity 2015–2026; mixing regimes makes a rate mean two things, for 0.04% more rows. |
| period | 2019–2025 complete years | all years; 2021–2025 | 2026 is partial; excluding 2020 would hide a real exposure change; 2015–2018 predate a reporting-mix break and run as `--sensitivity`. |
| grain | H3 r8 | r9; block group | r9 puts most cells at 0–1 crashes; BG has native population but non-uniform neighbourhood size, which is the thing being tested. MAUP consequence stated. |
| population | area-apportioned onto cells | BG-native analysis | keeps the raw and normalised analyses on one grain, which the contrast requires. BG-native is defensible and would give different numbers. |
| apportionment CRS | EPSG:5070 | EPSG:26985 | the apportionment is a ratio of areas; equal-area is the only projection that makes that ratio the Census's number. 26985 is fine at county scale but its correctness depends on the area being small. |
| weights | k=1 hex, sorted ids | distance band; queen on a grid | hexes give every interior cell exactly 6 equidistant neighbours, so the neighbourhood is uniform and a high statistic is data, not geometry. |
| transform | row-standardised (Moran), binary (Gi\*) | one for both | Moran's lag is a mean, Gi\*'s is a sum; row-standardising Gi\* divides out the intensity being tested. |
| alternative | `"two-sided"`, explicit | esda's `'directed'` default | both HOT and COLD are read off one p; a one-sided p doubles the effective α. Also pins behaviour against esda's announced default change. |
| permutations | 99,999 | 999 (the brief); 9,999 | below n/α = 38,840 the correction cannot reject rank 1; 999 produced an empty map that looked like a finding. |
| correction | Benjamini–Hochberg | Bonferroni | BH controls the expected false-discovery *proportion*, which is the operational question ("what share of dispatches are wasted"); Bonferroni asks "is there even one mistake". Both counts reported. |
| zero-population cells | flag + exclude from the rate, keep in raw | a population floor of 1; drop entirely | dropping hides the cells the contrast is about; a floor of 1 gives 40-crash interchanges a rate per 6 residents. |
| bandwidth | report all four, recommend 1,261 m | take the CV answer | CV optimises held-out likelihood, not operational usefulness; the CV number's job is to bound how much of the 500 m detail generalises. |
| CV folds | `GroupKFold` on H3 r7 | random `KFold` | measured 10.7× difference in the same direction; random folds leak across autocorrelated neighbours. |
| KDE evaluator | binned Gaussian convolution | `KernelDensity.score_samples` on the grid | >25 min/bandwidth vs ~1 s, for a measured 6.5e-03 relative error on the recommended surface. Verified against the exact estimator every build. |
| ST-DBSCAN ε | 300 m / 72 h / 5 | 300 m / 6 h / 10 | the latter returns zero clusters on 70,692 crashes. Chosen from a sweep, tabulated in config. |
| study scope | Montgomery only | + statewide Texas | 3.1 crashes per r8 cell over an undocumented OID slice; a Gi\* there tests the slice rule. |

## For MEMO.md

1. Crashes in Montgomery County are genuinely spatially clustered, not randomly
   scattered — and the clustering is four times stronger in raw volume
   (Moran's I 0.50) than in per-resident risk (0.19).
2. Adjusting for population does not refine the hot-spot map, it erases it: all
   55 hot cells lose significance per capita, and none appears. The reason is
   that the highest-risk cells have the fewest residents, because arterial
   crashes are caused by people who live somewhere else.
3. Population is a denominator, never a feature. It is the only census variable
   this pipeline loads; income, tenure, vehicles and commute are never fetched,
   which is a stronger guarantee than a policy.
4. Ranking outbound contact by crash density is a ranking by exposure, and in
   this county exposure tracks the demographics the assignment puts off limits.
5. Crash-to-trauma-centre drive-time is the obvious next-quarter addition
   (Valhalla, `https://valhalla1.openstreetmap.de/isochrone`, no key, send an
   `X-Client-Id`). Budget for network distance running **1.2–1.4×** Euclidean
   in US road grids — a straight-line coverage claim overstates access by
   roughly that factor.

---

## Open items for Phases 6–7

- **The spatially blocked backtest is written but not built.** Phase 7 should
  block by H3 r7 (what §4 used) or by county for a multi-state model, and
  report the blocked score as *the* score. The 10.7× bandwidth error measured
  here is the evidence for why, and it is a KDE result that transfers directly
  to a model.
- **`cell_stats_h3_r8` is the join table for a context feature.** `n_crashes`,
  `rate_per_km2` and the KDE `intensity_per_km2` at a record's H3 cell are
  road-level context with provenance to a public count. `rate_per_1k_pop` is
  the one column that must not become a feature.
- **The k=2 and `--sensitivity` runs are one command each** and belong in the
  final write-up if there is time.
- **Sunrise/sunset for `is_night`** is still Phase 3's fixed clock rule, and
  `st_clusters.night_fraction` inherits it. Noted in Phase 4's report; still
  open.
- **`hotspot_contrast.place_name` labels 54,486 of 70,692 crashes.** The
  unnamed remainder are unsnapped or on unnamed ways; a cell whose modal name
  is null is not necessarily roadless.
