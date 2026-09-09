# Phase 4 — Geospatial enrichment: implementation brief

You are implementing Phase 4 of the Crash-to-Contact take-home in this repo. Phases 1–3
are complete and committed on `feature/dimensional-modeling` (Phase 3 = commits
`6f8e51d` … `cab1185`: gold star schema, deterministic surrogate keys, FARS↔local entity
resolution). Your job is the geospatial layer over gold: census tract / block-group
point-in-polygon, H3 indexing, **coordinate-derived IANA timezone with UTC
localisation**, road snapping with linear referencing (scoped), an ERA5 weather join
(scoped), and a GeoParquet 1.1.0 output with a `bbox` covering column. Every metric
operation happens in a projected CRS named in a code comment. Nothing in EPSG:3857.

This section carries the most weight of any technical section (ASSIGNMENT.md §3), and
the timezone piece is a **compliance control** (Part 5 calling windows are computed from
it), not a geography nicety.

Read these before writing code, in this order:

1. `ASSIGNMENT.md` §3 (lines 135–200): 3a CRS rules, 3b the six enrichments, 3c what
   Phase 5 will need from you. Also §6 line 312 (GeoParquet 1.1.0, `bbox` covering,
   justify file sizes) and §4 (why ACS income must not become a per-record feature).
2. `ai docs/implementation/phase3-model-report.md` — what gold contains, and its
   closing "Open items for Phase 4" section. Its "For DECISIONS.md" section is the
   house style. Also skim `phase2-silver-report.md` §"No timezone localisation" (line
   ~157) and the FARS `HOUR/MINUTE = 99` handling (line ~481).
3. `config/geo.toml` (header explains why the bbox envelope is a strict *superset* and
   why your TIGER polygon test refines what it accepted but never revisits what it
   rejected), `config/sources.toml` (`[census]`, `[osm]`, `[weather]`, `[crs]` blocks —
   the CRS codes you will use are already listed there), `config/model.toml`.
4. `src/config.py` (`GOLD_DIR`, `geo()`, `key("census_api_key")` — a key is present in
   the gitignored `config/settings.toml`; never print it), `src/transform/common.py`
   (`write_parquet` validate-then-write, `BuildManifest`, `GEOD`, `GEO_QUALITY_VALUES`,
   `ROW_GROUP_SIZE`), `src/transform/model.py` + `build.py` (the CLI/manifest pattern
   you copy), `src/ingest/http.py` (the retrying client — reuse it for TIGER, Geofabrik,
   ACS and Open-Meteo downloads), `src/contracts.py` + `contracts/gold.schema.json`.
5. `tests/conftest.py`, `tests/test_model.py` (how gold is built into `tmp_path` from
   the committed bronze extract, no mocks, `CRASH_TEST_FULL_BRONZE=1` for real data).
6. `contracts/lead_output.schema.json` — downstream needs `crash_datetime_utc`-style
   timestamps and a per-record timezone; keep the field names compatible in spirit.

Environment: Python venv at `.venv`; DuckDB 1.5.5 (spatial extension available),
geopandas 1.1.4, shapely 2.1.2, pyproj 3.8, **h3 4.5.0 (v4 API only)**,
**timezonefinder 8.3.0**, osmnx 2.1.1 (installed but it does not read PBF), pyarrow 25,
pytest 9. `pyrosm` is *not* installed. Full local gold exists under `data/gold/`
(gitignored) built from local silver; rebuild with `python -m src.transform.model` if you
need to (silver: `python -m src.transform.build`, ~26 s). Run `.venv/bin/python -m
pytest -q` first and confirm the Phase 3 end state: **182 passed, 4 xfailed** on the
fixture (177 passed, 5 skipped, 4 xfailed with `CRASH_TEST_FULL_BRONZE=1`).

---

## 1. Scope

**In scope (build fully):**

- `src/geo/reference.py` — download + cache + hash reference data under
  `data/reference/` (gitignored): TIGER 2025 `TRACT` and `BG` zips for FIPS 24/48/12,
  TIGER 2025 `COUNTY` (for the polygon refinement of the bbox envelope), the Geofabrik
  Maryland PBF (and a Texas PBF only if you snap a Texas county — see §5), ACS 5-year
  responses. Record `sha256`, `Last-Modified`/`ETag`, size and download timestamp for
  every file in the geo manifest — same discipline as FARS zips in Phase 1. Raw bytes
  are kept as downloaded (byte-for-byte, like bronze); parsed forms live beside them.
- `src/geo/envelope.py` — county-polygon refinement of the silver bbox check: for every
  `geo_quality = OK` point, the TIGER county it actually falls in (`pip_county_geoid`)
  and whether that agrees with the source-stated county (`county_agrees_with_source`).
  Do **not** change silver's `geo_quality`; this is an additional, finer tier.
- `src/geo/census_join.py` — point-in-polygon to tract + block group (TIGER 2025), and
  `dim_block_group` (GEOID, tract GEOID, county GEOID, `ALAND`/`AWATER` m² from TIGER,
  population `B01003_001E` from ACS 2023 5-year, vintage columns).
- `src/geo/h3_index.py` — `h3.latlng_to_cell` at r9 (stored finest), r8 via
  `cell_to_parent`, plus r7 for rollups; helper for `grid_disk` that Phase 5 will call.
- `src/geo/tz.py` — IANA zone from coordinates via `timezonefinder`, localisation of
  the naive `crash_datetime_local` to UTC, DST gap/ambiguity handling and flags,
  county-level fallback for rows without a usable coordinate, `tz_source` provenance.
- `src/geo/snap.py` — nearest OSM road segment from the Geofabrik extract for
  Montgomery County (fully), snap distance as a first-class quality attribute with a
  config-driven rejection threshold, inherited `highway`/`maxspeed`/`lanes`/`name`/`ref`,
  and linear referencing (`offset_m` along the segment, `offset_frac`, segment length).
- `src/geo/weather.py` — Open-Meteo ERA5 archive join for a bounded slice (Montgomery
  crashes from 2024-01-01), keyed on the **UTC** hour (which is why tz runs first),
  raw responses cached byte-for-byte, rate limits respected and counted.
- `src/geo/build.py` — the CLI that runs the sequence and writes the outputs:
  `python -m src.geo.build [--gold-root …] [--reference-root …] [--skip-snap]
  [--skip-weather] [--offline] [--json]`. `--offline` must fail loudly naming the missing
  reference file, never silently skip a stage.
- Gold outputs (§3), `contracts/gold.schema.json` extensions, `config/geo.toml`
  extensions (§4), `tests/test_geo.py` (+ conftest fixtures), and
  `ai docs/implementation/phase4-geo-report.md` in the shape of the Phase 3 report.

**Out of scope (do not build; leave hooks):** the three spatial analyses (Phase 5 —
but `crash_geo` must give Phase 5 everything it needs: H3 r8/r9, block-group population
denominators, points ready to project to EPSG:5070); eligibility/calling windows (Phase
6 — but every record must leave this phase with `tz_iana`, `tz_source`,
`crash_datetime_utc`); scoring; isochrones / Valhalla; GHCNh; statewide Texas or any
Florida road snapping (there is no Florida crash source outside FARS); ACS variables
beyond population (§6). Do not modify `src/ingest/`, `src/transform/`, or
`fact_crash`'s column set; if a read-only helper is genuinely missing from
`src/transform/common.py`, add it and say so in the report.

---

## 2. What gold already gives you (verified 2026-09-08 on local data — re-measure)

`data/gold/fact_crash.parquet`: 268,493 rows; `latitude`/`longitude` DOUBLE in
EPSG:4326; `crash_datetime_local` is a **naive** `TIMESTAMP` — the source's wall clock,
never localised; `geography_sk` = integer county GEOID (e.g. 24031), FK to
`dim_geography` (349 rows: 345 COUNTY, 3 STATE, 1 UNKNOWN).

| primary_source_system | geo_quality | rows | lat non-null | datetime non-null |
|---|---|---|---|---|
| MONTGOMERY_MD | OK | 124,900 | 124,900 | 124,900 |
| MONTGOMERY_MD | OUT_OF_ENVELOPE | 105 | 0 | 105 |
| NHTSA_FARS | OK | 43,443 | 43,443 | 43,436 |
| NHTSA_FARS | SENTINEL | 45 | 0 | 45 |
| TXDOT_CRIS | OK | 92,721 | 92,721 | 92,721 |
| TXDOT_CRIS | MISSING | 7,279 | 0 | 7,279 |

Jurisdictions: MD 128,026 · TX 121,556 · FL 18,911 (FL = FARS only). Dates: Montgomery
2015-01-01 → 2026-09-02; TxDOT 2020–2024 (100k bounded slice); FARS 2019–2024. Seven
FARS rows have a date but no time (`HOUR=99`) — they get `crash_date` but no UTC stamp.
Coordinates are already NULL for every non-OK row, so "geocodable" ≡ `geo_quality='OK'`.

Silver TxDOT (`data/silver/txdot/crash_current.parquet`) also carries `coord_source`,
`coord_pairs_disagree` and the raw pairs, should you want to report snap distance by
coordinate provenance — optional, but it is a good DATA_QUALITY.md sentence.

---

## 3. Outputs and the table design

Decision to make and defend (the Phase 3 report left it open): widen `dim_geography`
vs. new tables. **Recommended:** leave `fact_crash` and `dim_geography` untouched
(Phase 3's tests and hashes must not move) and add:

- **`crash_geo`** — exactly one row per `fact_crash.crash_sk` (all 268,493, including
  rows with NULL geometry), the single enrichment table Phases 5–7 read. Columns, in
  groups: identity (`crash_sk`, `jurisdiction`, `primary_source_system`, `crash_date`);
  geometry (`geometry` POINT EPSG:4326 or NULL, `geo_quality` copied through,
  `pip_county_geoid`, `county_agrees_with_source`, `tract_geoid`, `bg_geoid`,
  `pip_status` ∈ {`MATCHED`, `NO_POLYGON`, `NO_GEOMETRY`}); H3 (`h3_r9`, `h3_r8`,
  `h3_r7` as strings); time (`crash_datetime_local` copied, `tz_iana`, `tz_source` ∈
  {`COORDINATE`, `COUNTY_FALLBACK`, `JURISDICTION_DEFAULT`, `UNRESOLVED`},
  `crash_datetime_utc` as `timestamp[us, tz=UTC]`, `utc_offset_minutes`,
  `tz_gap_adjusted`, `tz_ambiguous`, `time_status` ∈ {`OK`, `TIME_UNKNOWN`}); snapping
  (`snap_status` ∈ {`SNAPPED`, `REJECTED_DISTANCE`, `NOT_ATTEMPTED`, `NO_GEOMETRY`},
  `osm_way_id`, `snap_distance_m`, `segment_length_m`, `offset_m`, `offset_frac`,
  `osm_highway`, `osm_maxspeed`, `osm_lanes`, `osm_name`, `osm_ref`, `snap_crs_epsg`);
  weather (`weather_status` ∈ {`JOINED`, `NOT_IN_SCOPE`, `NO_GEOMETRY`, `NO_TIME`},
  `era5_hour_utc`, `era5_temperature_2m_c`, `era5_precipitation_mm`, `era5_rain_mm`,
  `era5_snowfall_cm`, `era5_weather_code`, `era5_wind_speed_10m_kmh`, `era5_grid_lat`,
  `era5_grid_lon`); lineage (`_geo_build_sha`, reference-data version columns or a
  manifest pointer). Enumerations go in the contract.
- **`dim_block_group`** — one row per 2025 TIGER block group in the three states, with
  the ACS population and TIGER land area (`density_per_km2` = pop / (ALAND / 1e6) — an
  arithmetic on stored m², not a geometric op). Tract is a prefix of the BG GEOID
  (`bg_geoid[:11]`); say so rather than adding a separate tract dim unless you have a
  reason.
- **GeoParquet 1.1.0** — write `crash_geo` as GeoParquet with the `bbox` covering
  column (`GeoDataFrame.to_parquet(schema_version="1.1.0", write_covering_bbox=True)`),
  hive-partitioned `data/gold/crash_geo/jurisdiction=XX/year=YYYY/part-0.parquet`,
  rows sorted by `h3_r9` within a partition so the bbox covering actually prunes (a
  random row order makes every row group's bbox the whole county). Choose the row-group
  size deliberately — Phase 3 uses 122,880 for columnar scans; bbox pruning works at
  row-group granularity, so smaller groups (e.g. 16k–32k) may serve spatial reads
  better. Measure both on a real bbox query with DuckDB and put the numbers and the
  choice in the report; that is the "justify your file sizes" answer. Also write the
  same table once as a plain (non-partitioned) parquet if downstream simplicity wins;
  say which is canonical. Non-geometry NULL rows keep their row (geometry NULL is valid
  GeoParquet).
- **`_geo_manifest.json`** in `data/gold/` (or extend `_build_manifest.json` under a
  `geo` key — pick one): input hashes (fact_crash, every reference file), config
  snapshot, per-stage counts (PIP hit rate, tz source distribution, snap accept/reject,
  weather joined), Open-Meteo request count against the 600/min · 10,000/day limits,
  output hashes, warnings.

Contracts: extend `contracts/gold.schema.json` with `crash_geo` and `dim_block_group`
in the same dialect (`x-column-order-is-contract`, typed `properties`,
`x-table-constraints` with `unique_keys` on `crash_sk` / `bg_geoid`, `foreign_keys`
`crash_geo.crash_sk → fact_crash`, `crash_geo.bg_geoid → dim_block_group` with NULL
allowed only when `pip_status != 'MATCHED'`, `row_count_min`). Validate every table
before the first write; a failure leaves the previous outputs untouched.

---

## 4. CRS discipline (the section-zero rule)

- Storage and PIP: **EPSG:4326**. Point-in-polygon is topological, not metric, so it
  needs no projection — write that in the comment where you call `sjoin(predicate=
  "within")`. Make sure both sides are 4326 (TIGER ships NAD83 EPSG:4269; treat it as
  4326-equivalent at this precision and say so, or `to_crs(4326)` explicitly).
- Snapping, nearest-segment distance, linear referencing: **EPSG:26985** for Maryland;
  if you snap a Texas county use the zone from `[crs]` in `sources.toml` (32139 central,
  or 3083 statewide Albers) and say why. Distances come from `sjoin_nearest(...,
  distance_col=…, max_distance=…)` or STRtree in the projected CRS; `line.project(point)`
  in the same CRS gives `offset_m`.
- Any tri-state area/rate you happen to compute (e.g. block-group density is *stored*
  ALAND, not computed — but if you do compute area): **EPSG:5070**.
- Great-circle sanity checks (e.g. crash-to-snapped-point cross-check): `common.GEOD`
  on the WGS84 ellipsoid, already used in Phases 2–3.
- `grep -rn 3857 src/ tests/` at the end must hit only comments and the negative-control
  test in §7. Every reprojection call sits next to a comment saying which CRS and why.

Put the CRS choices in `config/geo.toml` (`[crs]` per jurisdiction or reference
`sources.toml [crs]`) and read them from there, so the comment says *why* and the
config says *which*.

---

## 5. Stage-by-stage requirements

**5.1 Reference data.** TIGER 2025 URLs are in ASSIGNMENT.md §3b and
`sources.toml [census] tiger_base`. Geofabrik: download `maryland-latest.osm.pbf`
(~203 MB) once, record its sha256 and the server `Last-Modified` in the manifest and in
`config/geo.toml` as the pinned version — "latest" is not reproducible, so the hash is
the version. Never use Overpass. Reading the PBF without new dependencies: DuckDB
spatial's `ST_ReadOSM(path)` yields nodes/ways/tags; filter ways with a `highway` tag,
`unnest` the node refs with ordinality, join node coordinates, `ST_MakeLine` in ref
order, clip to the Montgomery bbox (padded, from `geo.toml`). If that proves too slow
or awkward, `pyosmium` is the acceptable new dependency (add to `requirements.txt` with
a one-line reason). Cache the resulting road GeoParquet under `data/reference/osm/`.

**5.2 Census PIP.** `sjoin` crashes (OK rows only) to block groups; derive tract from
the BG GEOID and cross-check against a direct tract join on a sample (should agree
100% — a disagreement means TIGER tract/BG layers are inconsistent, which is a
finding). Report: PIP hit rate per jurisdiction; rows inside the padded Montgomery bbox
but outside the county polygon (this is the refinement `geo.toml` promised) by county
they *are* in; TxDOT `cnty_id` / FARS `COUNTY` vs `pip_county_geoid` disagreement
counts. Points exactly on a boundary: `within` excludes, `intersects` may double-match;
pick one, de-duplicate deterministically (smallest GEOID), count the cases.

**5.3 ACS.** Population only: `B01003_001E` for every block group in states 24, 48, 12
from `acs5` 2023 (`for=block%20group:*&in=state:XX%20county:*` — if the API refuses the
county wildcard, one request per county from `config/counties.csv`; cache each raw
response). Use `config.key("census_api_key")`; the assignment warns the "500 unkeyed
queries" doc page is stale. **Deliberately do not fetch** income, tenure, vehicles or
commute variables: population is a denominator for Phase 5's normalised hotspots
(aggregate use); the others are the protected-class proxies Part 4 warns about, and a
feature that was never loaded cannot leak into scoring. Write that sentence in the
report for the memo. Also handle ACS annotation values (`-666666666` etc.) → NULL.

**5.4 H3.** v4 API only (`latlng_to_cell`, `cell_to_parent`, `grid_disk`,
`cell_area`). Store r9, r8, r7 as strings. Assert `cell_to_parent(h3_r9, 8) == h3_r8`
for all rows (cheap invariant, real test). Note H3 works on the sphere — no
reprojection, no 3857 — say so in the comment.

**5.5 Timezone and UTC (compliance-critical).**
- `TimezoneFinder(in_memory=True).timezone_at(lng=, lat=)` for every OK row →
  `tz_iana`, `tz_source='COORDINATE'`. Expect `America/New_York` for MD; TX split
  between `America/Chicago` and `America/Denver` (El Paso/Hudspeth); FL split between
  `America/New_York` and `America/Chicago` (western Panhandle). Report the counts —
  they are the memo's "Texas is two zones" evidence.
- Rows without a usable coordinate (7,279 TxDOT MISSING, 45 FARS SENTINEL, 105
  Montgomery OUT_OF_ENVELOPE): fall back to the **county** zone, derived from the data
  itself — the modal coordinate-derived zone among OK rows in that county, with its
  share; a county whose share < 100% (Gulf County FL is genuinely split; check others)
  is flagged `SPLIT_TZ` and the fallback is recorded as low-confidence.
  `tz_source='COUNTY_FALLBACK'`. No county → `JURISDICTION_DEFAULT` (MD → New_York; TX →
  Chicago; FL → New_York) with the flag. Phase 6 will turn non-`COORDINATE` sources into
  `GEOCODE_TIER_INSUFFICIENT`; you only have to make the provenance unambiguous.
- Localise, don't convert: the feeds publish naive **local** wall clock. Use
  `zoneinfo`; DST policy per D10 of the guide: nonexistent local times (spring-forward
  gap) → shift forward by the gap and set `tz_gap_adjusted`; ambiguous times (fall-back
  hour) → `fold=0` (earlier offset, DST still in effect) and set `tz_ambiguous`. State
  both choices and *why the flag matters more than the choice* in the module docstring.
  Detect gap/ambiguity explicitly (compare `utcoffset()` under `fold=0/1`, and check
  round-trip for nonexistence) — do not rely on a library silently normalising.
- Measure and report how many real rows fall in a gap or an ambiguous hour per year
  (there will be a handful each March/November; these are the rows a naive pipeline
  gets wrong by an hour — exactly the calling-window off-by-one Part 5 cares about).
- Output `crash_datetime_utc` with tz-aware UTC dtype (`timestamp[us, tz=UTC]` in
  parquet; DuckDB reads it as `TIMESTAMPTZ`); `utc_offset_minutes`; rows without a time
  → `time_status='TIME_UNKNOWN'`, UTC NULL.
- Replace nothing in `dim_time`; note in the report that `is_night` remains a clock rule
  and that coordinate-derived sun times are possible now (`utc_offset` + lat/lon) if
  Phase 7 wants them.

**5.6 Road snapping + linear referencing (Montgomery fully; Texas optional).**
Project crashes and roads to EPSG:26985; `sjoin_nearest` with `distance_col=
'snap_distance_m'`, `max_distance` = the rejection threshold from `config/geo.toml
[snap]`. **Set the threshold from the measured distribution, not from taste**: report
p50/p90/p95/p99 of snap distance, then choose (e.g. p95 rounded, or a fixed 50 m) and
justify; the assignment's "400 m is fiction" is the outer bound, not the answer. Ties
and multi-match at equal distance: keep the lowest `osm_way_id`. Inherit
`highway`, `maxspeed`, `lanes`, `name`, `ref`; **report null rates** of `maxspeed` and
`lanes` overall and by `highway` class — do not impute. Normalise `maxspeed` units
("35 mph" → 35, keep a raw column) only if trivial; otherwise leave raw and say so.
Linear referencing: `offset_m = line.project(point)` in 26985, `offset_frac =
offset_m / length`, `segment_length_m`. Sanity: the snapped point
`line.interpolate(offset_m)` is within `snap_distance_m` (+ tolerance) of the crash by
`GEOD` — assert it on a sample. Texas: if time allows, snap **one** county (El Paso
48141 is a good choice — it doubles as the Mountain-time demonstration) from the Texas
PBF (683 MB; `ST_ReadOSM` + bbox clip); otherwise mark all Texas rows
`snap_status='NOT_ATTEMPTED'` and record the cut in the report with the cost estimate.

**5.7 Weather (Montgomery, 2024-01-01 → latest, keyed on UTC hour).** Do not request
per crash. Bucket crash locations to a grid coarser than ERA5's ~9 km (H3 r5 cells,
~8.5 km edge, or 0.1° rounding — say which), request `hourly=temperature_2m,
precipitation,rain,snowfall,weather_code,wind_speed_10m` with `timezone=UTC` for each
cell centroid, one request per cell per calendar year (Open-Meteo weights long hourly
requests as multiple calls; chunking by year keeps each call small). Expect on the
order of 20–40 cells × 3 years ≈ 60–120 requests — count them in the manifest against
600/min and 10,000/day, and throttle (`tenacity` on 429, ≤ 5 req/s). Cache every raw
response JSON byte-for-byte under `data/reference/open_meteo/`; a re-run with a warm
cache makes **zero** network calls (test this with a fake cache dir). Join on
`date_trunc('hour', crash_datetime_utc)` and the cell; record the grid coordinate
Open-Meteo returned (`era5_grid_lat/lon`), which differs from the requested one. Then
the one analytic sentence the memo wants: agreement rate between officer-reported
`dim_weather_condition.is_precipitation` and `era5_precipitation_mm > 0` (by hour), and
what disagreement means for a smooth 9–25 km reanalysis that is never missing versus a
point observation that often is. Rows outside the slice → `weather_status=
'NOT_IN_SCOPE'`; document that the 10k/day cap makes a full-history national join a
multi-day batch and how you would schedule it.

---

## 6. Determinism, idempotency, restatement

- Two consecutive `python -m src.geo.build` runs on unchanged inputs and a warm
  reference cache produce byte-identical parquet (total sort order: `crash_sk` for the
  flat table; `h3_r9, crash_sk` within GeoParquet partitions). Timestamps belong in the
  manifest, never in a parquet column.
- Reference data is versioned by hash; a changed TIGER/PBF/ACS hash is a restatement
  → full rebuild of the affected columns, manifest records old and new hashes. Do not
  attempt SCD2 on enrichment columns — say why (they are derived, reproducible from
  hashed inputs; lineage is the manifest).
- A gold rebuild that changes one `fact_crash` row (Phase 3's TxDOT amendment fixture)
  must change exactly that `crash_geo` row; everything else byte-identical — extend
  `tests/test_idempotency.py` or add to `test_geo.py`.
- `timezonefinder` and H3 are pure functions; the OSM nearest-neighbour is deterministic
  given the tie rule. Weather depends on the cache: a cold-cache run may differ if
  Open-Meteo's ERA5 is revised — record the response `generationtime_ms`/headers so the
  report can say so.

---

## 7. Tests (`tests/test_geo.py`; no network in any test)

Unit tests on in-memory geometries and small committed fixtures; integration tests
skip with a clear reason unless `data/reference/` is present (`CRASH_TEST_FULL_REFERENCE=1`).

- **Timezone spot checks:** El Paso (31.7619, −106.4850) → `America/Denver`; Pensacola
  (30.4213, −87.2169) → `America/Chicago`; Rockville (39.0840, −77.1528) →
  `America/New_York`; Miami → `America/New_York`.
- **DST:** `2024-03-10 02:30` naive at a Montgomery coordinate → shifted, `tz_gap_adjusted`
  true, UTC = 07:30Z; `2024-11-03 01:30` → `tz_ambiguous` true, `fold=0` → UTC 05:30Z;
  an ordinary time → neither flag. Same for a `America/Chicago` coordinate.
- **Fallback provenance:** a row with NULL coordinates and county 48141 →
  `America/Denver`, `tz_source='COUNTY_FALLBACK'`; NULL coordinates and no county →
  `JURISDICTION_DEFAULT`; a FARS `HOUR=99` row → `time_status='TIME_UNKNOWN'`, UTC NULL,
  `crash_date` intact.
- **PIP:** build three small synthetic polygons in memory (or a committed ≤ 50 KB
  GeoJSON clip of a few real Montgomery block groups — public geometry, no PII); a point
  inside → its GEOID and tract = GEOID[:11]; a point outside all → `NO_POLYGON`; a
  boundary point → exactly one match under your rule. Integration (skipped without
  reference data): a pinned Rockville coordinate → a pinned 2025 tract/BG GEOID you
  derive once and freeze in the test.
- **H3:** `cell_to_parent(r9, 8) == r8` on the fixture; a known coordinate → a pinned r8
  cell; NULL geometry → NULL cells; the code imports no v3 names.
- **Snapping / linear referencing:** synthetic two-segment network in EPSG:26985; a
  point 10 m from segment A → `SNAPPED` to A with `offset_m` ≈ expected and
  `offset_frac` in [0, 1]; a point 600 m from everything → `REJECTED_DISTANCE`,
  attributes NULL; equidistant tie → lowest way id; `snap_crs_epsg == 26985`.
- **CRS negative control:** a 500 m buffer of a Baltimore-latitude point in EPSG:26985
  vs the "same" buffer built in EPSG:3857 and reprojected: area ratio ≈ 1/cos²(39.3°)
  ≈ 1.67 — the test asserts the 3857 result is *wrong* by that factor, proving why the
  rule exists. This is the only place `3857` may appear in `tests/`.
- **Weather join:** a committed real Open-Meteo response for one cell and a few days
  (public data, small) → hour alignment across the 2024-03-10 transition is correct on
  the UTC key; warm cache ⇒ zero HTTP calls (assert via a client stub or by pointing the
  cache at `tmp_path` with the file present and the network client replaced by one that
  raises).
- **GeoParquet:** the written file's `geo` metadata has `version == "1.1.0"`, a
  `covering.bbox` entry, `primary_column == "geometry"`, CRS 4326; a DuckDB bbox query
  on the partitioned dataset returns the same rows as a brute-force lat/lon filter.
- **Contracts:** `crash_geo` validates; a row with `pip_status='MATCHED'` and NULL
  `bg_geoid` fails with table and column named.
- **Idempotency:** per §6.

---

## 8. Conventions

- Match Phases 1–3: module docstrings explain **why**; a comment at every CRS choice;
  tests exercise real behaviour; numbers in docstrings are measured, dated and
  reproducible by a named command; thresholds are config, not code.
- Commit as you go: `feat(geo): …`, `feat(contracts): …`, `test(geo): …`,
  `chore(config): …`. Commit only files you fill in this phase; other empty scaffold
  files (`src/analysis/*`, `src/scoring/*`, `orchestration/*`, `src/compliance/vault.py`,
  `lineage.py`) stay untracked. Never commit anything under `data/`, `config/
  settings.toml`, `*.pbf`, `*.zip`, or `ai docs/`.
- New dependencies: none expected (`pyosmium` acceptable per §5.1). Add to
  `requirements.txt` with a one-line reason in the report.
- Logs carry counts, never coordinates of individual rows at INFO, never the Census key.
- Network calls only in `reference.py` and `weather.py`, only through the retrying
  client, never from tests. Overpass is never called.
- When the spec, this brief and the data disagree, the data wins and the disagreement
  goes in the report.

---

## 9. Deliverable: the report

Write `ai docs/implementation/phase4-geo-report.md` with: What I built (per module, a
paragraph each); Things the spec or this brief said that the data doesn't do; What I
bounded and why (Texas snapping, weather slice, ACS variables — with the cost/time
estimate for the unbounded version); Bugs the tests caught; **Verification** (row
reconciliation `fact_crash` = `crash_geo`; two-build byte identity; the amended-row
test; test counts fixture vs full); **The measurements**: PIP hit rate and county
disagreement table; bbox-vs-polygon refinement counts; timezone distribution per
jurisdiction and per source, `tz_source` distribution, DST gap/ambiguous row counts per
year; snap distance p50/p90/p95/p99, threshold chosen and why, accept/reject counts,
`maxspeed`/`lanes` null rates by highway class; weather request count vs limits,
officer-vs-ERA5 precipitation agreement; GeoParquet partition/file/row-group sizes and
the bbox-pruning measurement; **For DATA_QUALITY.md**; **For DECISIONS.md** (every
decision in §3–§5 with the rejected alternative: widened dim vs new table, PIP predicate,
DST fold policy, snap threshold, weather bucketing, row-group size, ACS scope); **For
MEMO.md** (three to five plain sentences: two time zones in Texas and Florida, what a
naive-time pipeline gets wrong, why ERA5 and not stations, why population but not
income); Open items for Phases 5–7 (what `crash_geo` gives the analyses; what Phase 6
should read for `tz_source`; sun-times for `is_night`).

---

## 10. Definition of done

- `python -m src.geo.build` runs end-to-end from local gold with a warm reference cache,
  twice, with identical parquet hashes; `--offline` fails loudly when a reference file is
  missing; the manifest records every reference hash, threshold, count and API call
  total.
- Every `fact_crash` row has exactly one `crash_geo` row with `tz_iana`, `tz_source`,
  and (where a time exists) `crash_datetime_utc`; every OK-geometry row has tract, BG,
  `h3_r8`/`h3_r9`; every Montgomery OK row has a snap outcome with a recorded distance;
  every in-slice Montgomery row has ERA5 values keyed on the UTC hour.
- `crash_geo` is valid GeoParquet 1.1.0 with a `bbox` covering column; the file-size and
  row-group choice is measured and written down.
- `grep -rn 3857 src/ tests/` shows only comments and the negative-control test; every
  metric op names its projected CRS in a comment.
- `pytest -q` green on the fixture and with `CRASH_TEST_FULL_BRONZE=1`; the 182 Phase 3
  tests still pass unchanged; no test touches the network.
- `git log` shows small commits; `git status` shows no data, settings, reference files
  or `ai docs/` staged; the report is written with the measurements above.
