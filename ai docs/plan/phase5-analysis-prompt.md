# Phase 5 — Spatial analysis: implementation brief

You are implementing Phase 5 of the Crash-to-Contact take-home in this repo. Phases 1–4
are complete and committed on `feature/dimensional-modeling` (Phase 4 = commits
`b90acad` … `968864e`: `crash_geo` GeoParquet enrichment, `dim_block_group`, H3 r9/r8/r7,
coordinate-derived timezone, Montgomery road snapping, ERA5 weather). Your job is
ASSIGNMENT.md §3c: **at least three spatial analyses, implemented properly, with the
output interpreted in prose.** "An unlabeled heatmap is not an analysis." The
deliverable is as much the written interpretation as the code.

The three to build (they match the empty scaffold files in `src/analysis/`):

1. **Moran's I + LISA** (`src/analysis/lisa.py`) — test that crash rates cluster spatially
   *before* asserting that they do.
2. **Getis-Ord Gi\*** with an FDR correction (`src/analysis/hotspots.py`) — hot/cold cells,
   and the raw-count vs population-normalised contrast that shows normalisation
   *changes the answer*.
3. **KDE** (`src/analysis/kde.py`) — a continuous intensity surface where bandwidth
   selection is the whole exercise.

Optional fourth if time allows: **ST-DBSCAN** (`src/analysis/st_dbscan.py`) over
`(x, y, t)` in a projected CRS — the "recurring Friday-night corridor, not a year-long
smear". Do not start it before the three are finished and written up.

Read these before writing code, in this order:

1. `ASSIGNMENT.md` §3c (the technique table — read every "what we are looking for"
   cell; each is a grading rubric line), §3a (CRS rules still apply: any distance, area,
   bandwidth or DBSCAN epsilon is metric and happens in a projected CRS, never 3857),
   §4 (why ACS variables other than population never enter a per-record feature — your
   normalisation is the *legitimate* ACS use, and the report must say so in one sentence),
   §6 line ~312 (GeoParquet / file-size justification carries over to any spatial output).
2. `ai docs/implementation/phase4-geo-report.md` — what `crash_geo` contains, the
   measurements, and its closing "Open items for Phases 5–7" (what it promised you:
   `h3_r8`/`h3_r9`, `bg_geoid` → population, `geometry` ready for EPSG:5070,
   `h3_index.grid_disk` / `disk_weights` for the weights matrix, `osm_way_id` + `offset_m`
   for corridor-level clustering). Its "For DECISIONS.md" section is the house style.
   Also read §"Things the spec … said that the data doesn't do" items 2–4: nine Texas
   counties are mis-decoded in `fact_crash.geography_sk`, five crashes are outside the
   US, and Montgomery reports 79 crashes in DC — all of which affect how you define a
   study area.
3. `ai docs/implementation/phase1-ingestion-report.md` §TxDOT — **how the 100k TxDOT slice
   was bounded.** A spatial statistic on a bounded slice describes the slice's selection
   rule, not Texas. Decide from that section whether any Texas analysis is
   interpretable; the default answer is "Montgomery only, with the reason", see §2.
4. `config/geo.toml` (`[h3]`, `[crs]`, `[census]` — reuse the CRS codes; add an
   `[analysis]` block, §4), `config/sources.toml [crs]`, `src/config.py` (`geo()`,
   `GOLD_DIR`), `src/geo/h3_index.py` (`grid_disk`, `disk_weights`, `cell_area_km2`),
   `src/transform/common.py` (`write_parquet` validate-then-write, `BuildManifest`,
   `connect`, `ROW_GROUP_SIZE`), `src/geo/build.py` (the CLI/manifest pattern you copy),
   `src/contracts.py` + `contracts/gold.schema.json` (`crash_geo`, `dim_block_group`).
5. `tests/conftest.py`, `tests/test_geo.py` (how gold + `crash_geo` are built into
   `tmp_path` from the committed bronze extract; no mocks; `CRASH_TEST_FULL_BRONZE=1` for
   real data; no network in tests).

Environment: Python venv at `.venv`; DuckDB 1.5.5 (spatial), geopandas 1.1.4, shapely
2.1.2, pyproj 3.8, h3 4.5.0 (v4 API only), **esda 2.10.0, libpysal 4.15.0,
scikit-learn 1.9.0, scipy 1.18.1, matplotlib 3.11.1**, numpy 2.5, pandas 3.0, pyarrow 25,
pytest 9. `statsmodels` and `pointpats` are **not** installed. `git status` shows
`requirements.txt` and `.gitignore` modified but uncommitted — inspect `git diff` on
both; if the diff is only the analysis libraries (`esda`, `libpysal`, `scikit-learn`)
and the `ai docs/` ignore line, commit them as your first `chore(deps)` /
`chore(gitignore)` commits with a one-line reason each.

Full local gold + geo exists under `data/gold/` (gitignored). Rebuild if needed:
`python -m src.transform.model` then `python -m src.geo.build` (warm reference cache;
snapping ~1 min, weather zero HTTP calls from cache). Run `.venv/bin/python -m pytest -q`
first and confirm the Phase 4 end state: **260 passed, 4 xfailed** on the fixture.

---

## 1. Scope

**In scope (build fully):**

- `src/analysis/common.py` (or `frame.py`) — the one place that builds the analysis
  frames every module shares: the H3 r8 cell table for a study area (cell, crash count,
  injury/fatal count, cell polygon, land-area-weighted population, rate), the block-group
  table, and the projected point set. One code path, so the three analyses agree on N,
  period and study area by construction.
- `src/analysis/weights.py` — the spatial weights matrix from `h3_index.disk_weights`
  (k=1 → 6 hex neighbours; islands and edge cells handled explicitly), built into a
  `libpysal.weights.W` with a **sorted, stable id order** so permutation results do not
  move between runs. Optionally a distance-band alternative in EPSG:26985 for
  sensitivity — only if you have time to interpret it.
- `src/analysis/lisa.py` — global Moran's I (`esda.Moran`) and local (`esda.Moran_Local`)
  on the population-normalised rate and on raw counts, both with a fixed seed and 999
  permutations; quadrant classification (HH/LL/HL/LH) at an FDR-corrected significance.
- `src/analysis/hotspots.py` — `esda.G_Local(star=True)` on raw counts and on the
  normalised rate; Benjamini–Hochberg (`esda.fdr`) on the permutation p-values; the
  before/after-correction counts; the **contrast table**: cells that are hot under raw
  counts but not under normalisation and vice-versa, with named places.
- `src/analysis/kde.py` — KDE on crash points in **EPSG:26985** (Montgomery) via
  `sklearn.neighbors.KernelDensity` (Gaussian) on a regular grid; bandwidth chosen by a
  method you name and defend (grid-search cross-validation of log-likelihood is the
  honest one; Silverman/Scott rules-of-thumb are the reference points you compare it to;
  a fixed 500 m is the "what a practitioner would pick" control). Report all three and
  the surface each produces; explain what a too-small and a too-large bandwidth do to the
  reading. Output the surface as a parquet grid (x, y, density) in 26985 with the CRS in
  the manifest, and a PNG.
- `src/analysis/build.py` — the CLI: `python -m src.analysis.build [--gold-root …]
  [--out-root …] [--study-area montgomery] [--period 2019-01-01:2025-12-31] [--seed N]
  [--skip-kde] [--json]`. Writes the output tables (§3), the figures and
  `_analysis_manifest.json`. Validates every table against the contract **before** the
  first write.
- `contracts/analysis.schema.json` (same dialect as `gold.schema.json`) and the
  `[analysis]` block in `config/geo.toml` (§4).
- `tests/test_analysis.py` (+ conftest fixtures).
- **The prose**: `ANALYSIS.md` at the repo root (committed, with figures under
  `output/figures/` — PNGs, ≤ 300 KB each, committed), and
  `ai docs/implementation/phase5-analysis-report.md` in the shape of the Phase 4 report.
  `ANALYSIS.md` is the graded artefact; the report is for the author.

**Out of scope (do not build; leave hooks):** isochrones / Valhalla (name the endpoint
and the 1.2–1.4× network-vs-Euclidean fact in the report's MEMO section and stop);
spatial cross-validation (no model is built in this phase — but write the one paragraph
Phase 7's backtest will need: block by H3 r7 or county, why random splits leak); any
per-record scoring feature (Phase 7); any modification of `src/geo/`, `src/transform/`,
`fact_crash` or `crash_geo` column sets; ACS variables beyond population; any Texas road
snapping or weather. If a read-only helper is genuinely missing from `src/geo/h3_index.py`
or `src/transform/common.py`, add it and say so in the report.

---

## 2. What `crash_geo` already gives you (verified 2026-09-08 on local data — re-measure)

`data/gold/crash_geo.parquet`: 268,493 rows, one per `fact_crash.crash_sk`.

| jurisdiction | rows | geometry | h3_r8 | bg_geoid | snapped | weather status |
|---|---|---|---|---|---|---|
| MD | 128,026 | 127,914 | 127,914 | 127,834 | 124,853 | all rows carry a status |
| TX | 121,556 | 114,259 | 114,259 | 114,239 | 0 | NOT_IN_SCOPE |
| FL | 18,911 | 18,891 | 18,891 | 18,890 | 0 | NOT_IN_SCOPE |

53,313 distinct r8 cells across all three. `dim_block_group`: 36,105 block groups, 345
counties, population `B01003_001E` (ACS 2023 5-year) non-null on every row, `aland_m2`
stored from TIGER, `density_per_km2` precomputed. Columns you will use: `crash_sk`,
`jurisdiction`, `primary_source_system`, `crash_date`, `geometry` (4326 WKB),
`geo_quality`, `pip_county_geoid`, `county_agrees_with_source`, `tract_geoid`,
`bg_geoid`, `h3_r9/r8/r7`, `crash_datetime_local` (naive wall clock — correct for
day-of-week / hour-of-day questions), `crash_datetime_utc`, `tz_iana`, `osm_way_id`,
`offset_m`, `osm_highway`, `era5_*`. Severity lives on `fact_crash` (`severity_ordinal`
0–5 via `dim_severity`) — join on `crash_sk`.

**Study-area decision (make it, defend it, write it down):**

- **Montgomery County, MD is the primary corpus** — the only complete, multi-year,
  all-severity source (2015-01-01 → 2026-09-02, 124,900 geocoded), with snapping and
  weather. Define the study area as `pip_county_geoid = '24031'` (the polygon answer),
  not `jurisdiction = 'MD'`: that drops the 79 DC crashes and the 162 Prince George's
  crashes Montgomery police reported, whose inclusion would put "hotspots" outside the
  county with no population denominator. Count what you dropped.
- **Period**: choose deliberately. The full series includes the 2020 pandemic drop and a
  partial 2026; a per-year table of counts is the first figure. Recommended: the
  analyses on **2019–2025 complete years** with a sensitivity check on all years; state
  the choice in `config/geo.toml [analysis]`.
- **TxDOT**: read the Phase 1 report's slice rule first. If the slice is an OID range
  (a de facto time window of statewide crashes) it is *spatially* representative and a
  statewide r8 Gi\* is interpretable at low density; if it is anything else, do not
  analyse it — say why in one paragraph. Either way, the nine mis-decoded counties do
  not matter here because you use `pip_county_geoid`, and the five out-of-US points are
  excluded by `pip_status = 'MATCHED'`.
- **FARS** is fatal-only and sparse (43k rows nationally over six years across three
  states); a fatal-crash Gi\* at r7 for the tri-state area is a legitimate secondary
  analysis if you want one contrast between an all-severity and a fatal-only surface.
  Optional; do not let it eat the Montgomery write-up.

---

## 3. Outputs and the table design

All under `data/gold/analysis/` (gitignored), plus figures under `output/figures/`
(committed) and the prose in `ANALYSIS.md` (committed):

- **`cell_stats_h3_r8`** — one row per r8 cell in the study area (including zero-crash
  cells inside the county — a hotspot test on only the cells that had crashes is
  biased; fill the county polygon with `h3.polygon_to_cells` / `h3shape_to_cells` and
  say which cells are land). Columns: `h3_r8`, `study_area`, `period_start`,
  `period_end`, `n_crashes`, `n_injury` (severity ≥ 2), `n_fatal`, `population`,
  `land_area_km2`, `rate_per_1k_pop`, `rate_per_km2`, `n_neighbors`, `is_edge_cell`,
  plus lineage (`_analysis_build_sha`, `_geo_build_sha` it was built from).
- **`lisa_h3_r8`** — `h3_r8`, `variable` ∈ {`raw_count`, `rate_per_1k_pop`}, `local_i`,
  `z_score`, `p_sim`, `p_fdr`, `quadrant` ∈ {HH, LL, HL, LH, NS}, `significant`.
- **`gi_star_h3_r8`** — `h3_r8`, `variable`, `gi_z`, `p_sim`, `p_fdr`, `hotspot_class`
  ∈ {HOT_99, HOT_95, COLD_95, COLD_99, NS} after correction, and the uncorrected class
  beside it so the correction's effect is a column, not a sentence.
- **`hotspot_contrast`** — the cells whose class differs between `raw_count` and
  `rate_per_1k_pop`, with `osm_name`/place hints from the majority snapped road name in
  the cell, so the prose can name them.
- **`kde_surface`** — regular grid in EPSG:26985 (`x_m`, `y_m`, `density`) for the chosen
  bandwidth; the two comparison bandwidths as separate `bandwidth_m` values in the same
  table or separate files — say which.
- **`_analysis_manifest.json`** — input hashes (`crash_geo`, `dim_block_group`,
  `fact_crash`), config snapshot, seed, N per analysis, study-area and period, global
  Moran's I with p, counts of significant cells before/after FDR, chosen bandwidth and
  the CV score table, output hashes, warnings.
- **Figures** (matplotlib, PNG): every figure has a title, the study area, the period,
  N, the CRS of any distance shown, a legend with the significance level and the
  correction, and a scale bar or axis units. Minimum set: per-year counts; LISA
  cluster map; Gi\* raw vs normalised side by side; KDE at three bandwidths; the
  contrast map. Consistent colours across figures (same class → same colour).

**Population onto hexagons.** Block groups and H3 cells do not nest. Area-weight the BG
population onto r8 cells: intersect BG polygons (TIGER, 4269 → **EPSG:5070** for the
area computation — comment it) with cell polygons, allocate population by the share of
the BG's land area falling in the cell. Assert the allocation is mass-preserving (sum
over cells of a BG's shares = BG population ± rounding) as a test. State the assumption
(uniform population within a BG) and its failure mode (a BG that is half park). The
alternative — running the normalised analysis at BG grain, where population is native
— is defensible; if you prefer it, do it and explain why the raw-count analysis is then
also at BG grain (the two must share a grain or the contrast is meaningless). Zero-
population cells with crashes (highway cells, the airport, parkland) are the interesting
case: they are infinite-rate under naive division. Decide (exclude from the rate
analysis with a flag; or a small population floor) and report how many cells that is.

Contracts: `contracts/analysis.schema.json` in the gold dialect —
`x-column-order-is-contract`, typed `properties`, `x-table-constraints` with
`unique_keys` on `(h3_r8, variable)` etc., enumerations for the class columns,
`row_count_min`. Wire it into `src/contracts.py` the way gold is.

---

## 4. Method requirements

**4.1 Weights.** k=1 hex ring from `h3_index.disk_weights` (6 neighbours interior;
fewer at the county edge — record `n_neighbors` and `is_edge_cell`). Row-standardise
for Moran's I (`transform='r'`); binary for Gi\* (`transform='b'`), and say why each.
Build the `W` from a sorted list of ids; assert `w.islands == []` after county filling,
or report the islands. Sensitivity (optional): k=2.

**4.2 Moran's I / LISA.** Global first: `esda.Moran(y, w, permutations=999, seed=…)`;
report I, expected I, z, and both analytic and permutation p. **If the global test is
not significant, say so and stop calling things clusters.** Then `Moran_Local` with the
same seed; FDR-correct `p_sim`; classify quadrants only where significant. Run on
`rate_per_1k_pop` (primary) and `raw_count` (contrast). Interpretation must address:
does clustering exist at all; where are the HH clusters and what road is under them;
what the LH/HL outliers are (a hot cell in a cold neighbourhood is usually one
intersection — name it from `osm_name`).

**4.3 Getis-Ord Gi\*.** `esda.G_Local(y, w, star=True, transform='B', permutations=999,
seed=…)`. FDR: `esda.fdr(p_sim, alpha=0.05)` → the corrected threshold; classify with
it. Report: significant-cell counts uncorrected vs corrected at 0.05 and 0.01; the
raw-vs-normalised contrast as a table of *named* places, e.g. "the I-270 / Rockville
Pike corridor is hot under both; downtown Silver Spring is hot on raw counts and not
significant per capita; the Agricultural Reserve is cold on both". Show that
normalisation changes the answer — the assignment says so explicitly — and if on your
data it barely does, that is a finding: report it honestly with the counts.

**4.4 KDE.** Points in EPSG:26985. Grid resolution 100 m (config). Bandwidths: Scott
and Silverman rules (computed, in metres), CV-selected (`GridSearchCV` over a log-spaced
range, e.g. 100–2,000 m, 5-fold, fixed seed — **spatially blocked folds** by r7 cell,
not random, and say why: random folds leak between autocorrelated neighbours and
inflate the score, which is exactly the trap ASSIGNMENT.md's last table row describes),
and the 500 m practitioner control. Report the CV log-likelihood table and plot all
three surfaces with identical colour scale. Interpretation: what the small bandwidth
shows (intersections), what the large one shows (the urban/rural gradient, i.e.
population), and why the chosen one is the useful middle. Sanity: the surface
integrates to ≈ 1 (density) or ≈ N (intensity) — test it.

**4.5 ST-DBSCAN (optional fourth).** `(x_m, y_m)` in 26985 and `t` in hours from
`crash_datetime_utc`; two epsilons (`eps_space_m`, `eps_time_h`) from config;
`min_samples` from config. Implement the two-threshold neighbourhood directly with
`scipy.spatial.cKDTree` on space and a time filter (sklearn's DBSCAN cannot take two
radii — a precomputed sparse distance matrix that is 0/1 on both conditions works).
The result the assignment wants: clusters tight in *both* dimensions, e.g. the same
corridor on consecutive Friday nights; report the top clusters with road name,
day-of-week/hour profile (from `crash_datetime_local`), and duration. If you do it,
also show the contrast: the same `eps_space_m` with no time limit is the year-long smear.

**4.6 CRS discipline.** Storage 4326; hex weights are topological (H3 on the sphere —
no projection, comment it); population apportionment areas in **EPSG:5070**; KDE,
bandwidths, DBSCAN epsilons in **EPSG:26985** for Montgomery (`config/geo.toml
[crs.snap] MD`). `grep -rn 3857 src/analysis tests/test_analysis.py` must return nothing
(the Phase 4 negative-control test remains the only place it appears in `tests/`).

**4.7 Config (`config/geo.toml [analysis]`).** `study_area` (county GEOID), `period`,
`h3_resolution = 8`, `neighbor_k = 1`, `permutations = 999`, `seed`, `alpha = 0.05`,
`fdr = true`, `min_population_for_rate`, `kde.grid_m = 100`, `kde.bandwidths_m`
(candidate range), `kde.cv_folds`, `kde.block_resolution = 7`, `st_dbscan.*`. Every
number in a docstring or the prose comes from a manifest field or a named command.

---

## 5. Determinism, idempotency

- Two consecutive `python -m src.analysis.build` runs on unchanged inputs produce
  byte-identical parquet and identical manifest values except `built_at`. Permutation
  inference is seeded from config; ids are sorted; `write_parquet` gets a total sort
  order (`h3_r8, variable`; `x_m, y_m, bandwidth_m` for the surface).
- `_analysis_build_sha` is a hash of the inputs (crash_geo sha, dim_block_group sha,
  fact_crash sha, the `[analysis]` config), never of time; it moves exactly when an input
  moves. A re-run of Phase 4 that changes one `crash_geo` row must change the cell
  statistics for that cell and nothing outside its k=1 neighbourhood in the local
  statistics (the global I and the FDR threshold may move — say so; test the cell-level
  claim).
- Figures are not required to be byte-identical (matplotlib embeds versions), but the
  data behind each figure is a parquet table that is.

---

## 6. Tests (`tests/test_analysis.py`) — real behaviour, no mocks, no network

- **Weights**: every interior cell of a filled polygon has exactly 6 neighbours; W is
  symmetric; ids are sorted; no islands after filling; a known cell's neighbours match
  `h3.grid_disk(cell, 1)` minus itself.
- **Moran's I on synthetic fields**: a smooth gradient over a filled hex patch →
  I > 0 and p_sim < 0.01; a checkerboard (alternating by ring parity or random
  permutation of the same values) → I ≤ 0 / not significant. Same seed → identical I
  and p across two calls.
- **Gi\* on a planted cluster**: Poisson noise everywhere plus a planted 7-cell hot
  patch → the patch is `HOT_99` after FDR; on pure noise the corrected significant
  count is ≤ the expected false-discovery share (run with a fixed seed, assert a bound,
  not zero).
- **FDR**: `esda.fdr` threshold ≤ alpha; the corrected significant set ⊆ uncorrected.
- **Population apportionment**: mass-preserving per BG (sum of shares = population);
  a BG entirely inside one cell gives 100% to it; areas computed in 5070 (assert the
  frame's CRS at the call).
- **Rate**: `min_population_for_rate` flag set exactly where population < floor; no
  inf/NaN in `rate_per_1k_pop` on flagged rows.
- **KDE**: the surface integrates to ≈ 1 within 2% for the chosen bandwidth on a
  synthetic point set; Scott/Silverman formulas match hand-computed values; blocked CV
  folds contain no r7 cell in more than one fold.
- **Contrast**: on the committed bronze extract (fixture scale) the pipeline runs
  end-to-end and writes every table with the contract validating; on
  `CRASH_TEST_FULL_BRONZE=1` (or a `data/gold` guard), at least one cell changes class
  between raw and normalised — if not, the test records the count and passes only
  with a clear skip reason, never silently.
- **Contracts**: a row with `significant = true` and `p_fdr > alpha` fails with table
  and column named.
- **Idempotency**: per §5 — two builds into `tmp_path`, identical hashes.
- **CRS**: no `3857` in `src/analysis/`; every `to_crs` call sits next to a comment.

---

## 7. Conventions

- Match Phases 1–4: module docstrings explain **why**; a comment at every CRS choice
  and at every statistical choice (transform, permutations, correction); numbers are
  measured, dated and reproducible by a named command; thresholds are config, not code.
- Commit as you go: `feat(analysis): …`, `feat(contracts): …`, `test(analysis): …`,
  `chore(config): …`, `docs(analysis): …`. Commit only files you fill in this phase;
  other empty scaffold files (`src/scoring/*`, `orchestration/*`,
  `src/compliance/vault.py`, `lineage.py`, `rules.yaml`) stay untracked. Never commit
  anything under `data/`, `config/settings.toml`, `*.parquet` outside
  `tests/fixtures/`, or `ai docs/`. Figures under `output/figures/` **are** committed
  (they are the analysis); keep each ≤ 300 KB.
- New dependencies: none expected beyond what `requirements.txt` already lists. If you
  need one, add it with a one-line reason in the report.
- Plots: matplotlib only; static PNG; label everything (§3). No interactive maps, no
  basemap tiles (network).
- Logs carry counts and statistics, never per-row coordinates at INFO.
- When the spec, this brief and the data disagree, the data wins and the disagreement
  goes in the report.

---

## 8. Deliverable: the prose

**`ANALYSIS.md`** (committed, ~1,200–2,000 words, business-literate but technical, with
the figures inline). Structure: study area and period and why (with the counts of what
was excluded and why: DC/PG crashes, non-matched geometry, zero-population cells);
**Does clustering exist?** (global Moran's I, both variables, the honest answer); **Where**
(LISA map, the HH clusters named by road, the outliers explained); **Hot spots and the
normalisation contrast** (Gi\* maps side by side, the FDR before/after counts, the
contrast table with named places, one paragraph on what a per-capita view changes and
why the business should care — a "hot" downtown cell may just be where the people are);
**Intensity surface** (KDE: the bandwidth table, three surfaces, what each bandwidth is
good for, which one you would put in front of an operations team); optional ST-DBSCAN;
**Limitations** (edge effects at the county boundary, MAUP — the r8 choice is one
choice; the population-uniformity assumption; officer-reported coordinates' precision;
the pandemic year); **What this means for Phase 7** (aggregate crash density is a
legitimate context feature, ACS population is a denominator not a feature, spatially
blocked backtest splits).

**`ai docs/implementation/phase5-analysis-report.md`**: What I built (per module);
Things the spec or this brief said that the data doesn't do; What I bounded and why
(Texas, FARS, ST-DBSCAN, k=2 sensitivity — with cost/time for the unbounded version);
Bugs the tests caught; **Verification** (two-build byte identity; the one-changed-row
locality test; test counts fixture vs full); **The measurements** (every number in
`ANALYSIS.md` with the command that reproduces it; global I table; significant-cell
counts uncorrected/FDR at 0.05/0.01 for both variables; the contrast counts; the
bandwidth CV table; zero-population cell count; edge-cell count); **For
DATA_QUALITY.md** (what the analysis surfaced: e.g. coordinate stacking at a default
point, duplicate-location artefacts in KDE, a cell whose crash count is implausible);
**For DECISIONS.md** (study area by polygon not jurisdiction; period; grain r8 vs BG;
apportionment vs BG-native; weights k=1 binary/row-standardised; 999 permutations +
seed; BH FDR vs Bonferroni; bandwidth selection method; zero-population handling;
Montgomery-only vs statewide — each with the rejected alternative); **For MEMO.md**
(three to five plain sentences: clustering is real / is not; the normalisation
sentence; the "population is a denominator, not a feature" sentence; isochrones as the
next-quarter item with the Valhalla endpoint and 1.2–1.4× fact); Open items for Phases
6–7.

---

## 9. Definition of done

- `python -m src.analysis.build` runs end-to-end from local gold twice with identical
  parquet hashes; the manifest records seed, N, period, study area, global I, FDR
  counts, bandwidths and every input hash.
- Three analyses are implemented with `esda` / `sklearn` as named, on shared frames, with
  the FDR correction applied and its effect measured, and the raw-vs-normalised contrast
  quantified and named.
- `ANALYSIS.md` exists with labelled figures and prose that answers each "what we are
  looking for" cell of ASSIGNMENT.md §3c for the techniques you chose; nothing in it is
  a number that the manifest or a named command cannot reproduce.
- `contracts/analysis.schema.json` validates every output; `grep -rn 3857 src/analysis`
  is empty; every metric op names its projected CRS in a comment.
- `pytest -q` green on the fixture and with `CRASH_TEST_FULL_BRONZE=1`; the 260 Phase 1–4
  tests still pass unchanged; no test touches the network.
- `git log` shows small commits; `git status` shows no data, settings or `ai docs/`
  staged; figures committed; the report written with the measurements above.
