# Phase 4 — Geospatial enrichment

Scope: `src/geo/{reference,envelope,census_join,h3_index,tz,snap,weather,build}.py`,
`config/geo.toml` (Phase 4 blocks), `contracts/gold.schema.json` (`gold.crash_geo`,
`gold.dim_block_group`), `tests/test_geo.py` + geo fixtures in `tests/conftest.py`,
two committed fixtures under `tests/fixtures/geo/`.
Status: complete and verified end to end, on the fixture and on the full local corpus.

```
python -m src.geo.reference --all          # 252 MB of reference data, hashed
python -m src.geo.build                    # 3 outputs, ~26 s warm, byte-identical on re-run
python -m src.geo.build --offline          # fails loudly, naming the missing file
pytest -q                                  # 260 passed, 4 xfailed        (fixture, 52 s)
CRASH_TEST_FULL_BRONZE=1 CRASH_TEST_FULL_REFERENCE=1 pytest -q   # full corpus
```

Measured 2026-09-08 on the local gold (268,493 crashes: Montgomery 125,005 · TxDOT
100,000-row OID slice · FARS 2019–2024 in MD/TX/FL 43,488). Every number below is
reproduced by `python -m src.geo.build --json`, which writes them to
`data/gold/_geo_manifest.json`.

`fact_crash` and `dim_geography` are unchanged. Phase 3's 15 parquet hashes are
byte-for-byte what they were.

---

## What I built

**`src/geo/reference.py`** — the reference cache. Downloads TIGER 2025 COUNTY (national),
TRACT and BG for FIPS 12/24/48, the ACS 2023 5-year population table, and the Geofabrik
Maryland PBF through Phase 1's retrying client, writes the raw bytes byte-for-byte, and
records per file the source URL, sha256, byte count, server `Last-Modified`/`ETag` and
download timestamp in `data/reference/_reference_manifest.json`. Reference data is
versioned **by hash, not by URL**: `maryland-latest.osm.pbf` 302s to a dated file whose
bytes change nightly, so the sha256 is the version and it is pinned in `config/geo.toml
[reference.osm]`. `--offline` never touches the network and raises `MissingReference`
naming the file and the command that fetches it — it does not skip a stage, because a
`crash_geo` with silently NULL tract GEOIDs is indistinguishable from one built over
crashes in the ocean.

**`src/geo/envelope.py`** — the polygon refinement `config/geo.toml`'s header promised.
For every geocoded crash it answers the two questions the bounding box could not:
`pip_county_geoid` (the TIGER county the point is actually in) and
`county_agrees_with_source`. It does **not** touch silver's `geo_quality`. Silver's tier
is a statement computable with no reference data and no network; this is an additional,
finer tier that needs 84 MB of polygons, and collapsing the two would mean a TIGER
re-vintage could silently move rows between quality classes.

**`src/geo/census_join.py`** — point-in-polygon to block group, tract as the GEOID
prefix, and `dim_block_group`. The join runs in EPSG:4326 with **no projection at all**,
because point-in-polygon is topological rather than metric — the one spatial operation in
this phase where "always reproject" is the wrong reflex. TIGER's NAD83 (EPSG:4269) is
reprojected to 4326 explicitly anyway, so the claim is a call and not a comment.
`dim_block_group` carries TIGER's stored `ALAND`/`AWATER` and the ACS population;
`density_per_km2` is arithmetic on a stored area, not a geometric operation.

**`src/geo/h3_index.py`** — r9 computed from the coordinate, r8 and r7 derived with
`cell_to_parent`. Deriving upward rather than calling `latlng_to_cell` three times makes
`cell_to_parent(h3_r9, 8) == h3_r8` true *by construction*; the build asserts it anyway on
every row. h3-py 4.x only — a test parses the module's AST and asserts the set of `h3.*`
calls is exactly `{latlng_to_cell, cell_to_parent, grid_disk, cell_area}`. `grid_disk` and
`disk_weights` are the hooks Phase 5's neighbourhood smoothing will call.

**`src/geo/tz.py`** — the compliance control. `timezonefinder` 8.3.0 offline gives
`tz_iana` from the coordinate; rows without one fall back to the county's modal
coordinate-derived zone *derived from the corpus itself*, then to the jurisdiction
default, with `tz_source` recording which. The naive local wall clock is **localised**
(never converted) with `zoneinfo`. Gap and ambiguity are detected explicitly by comparing
`utcoffset()` under `fold=0` and `fold=1` rather than trusting a library to normalise
silently, and both are flagged as well as resolved — because the flag, not the policy, is
what lets Phase 6 refuse to compute a calling window from a timestamp known to be an hour
uncertain.

**`src/geo/snap.py`** — nearest OSM way in EPSG:26985, snap distance stored for every
attempted row including rejections, ties broken on the lowest `osm_way_id`, and linear
referencing (`offset_m`, `offset_frac`, `segment_length_m`). The road network is read
straight out of the 214 MB PBF by DuckDB spatial's `ST_ReadOSM` — no new dependency — and
cached as GeoParquet keyed by the PBF's sha256, so a new extract is a cache miss rather
than a stale hit.

**`src/geo/weather.py`** — Open-Meteo ERA5 for a bounded Montgomery slice, keyed on the
**UTC** hour (which is why `tz` runs first). Crash locations are bucketed to H3 r5 cells
and requested one (cell, year) at a time; every raw response is cached byte-for-byte and a
warm-cache run makes zero network calls.

**`src/geo/build.py`** — the CLI. Reads gold, runs the six stages in dependency order,
validates both new tables against the contract **before the first write**, and emits
`crash_geo.parquet`, the hive-partitioned `crash_geo/` dataset, `dim_block_group.parquet`
and `_geo_manifest.json`.

---

## Things the spec, the brief or the data said that the data doesn't do

### 1. There is no Census API key, and the live API refuses unkeyed requests

The brief says a key is present in the gitignored `config/settings.toml`. It is not:
`census_api_key` is the empty string. Verified live 2026-09-08 — an unkeyed
`api.census.gov/data/2023/acs/acs5?...for=block%20group:*` returns **HTTP 302 to a
"Missing Key" HTML page**, both with and without a county wildcard. That is exactly what
ASSIGNMENT.md predicts about the stale "500 unkeyed queries" documentation, and the docs
are indeed stale.

`ensure_acs()` implements the keyed API route (with the per-county fallback for a refused
wildcard, and with the key stripped from the manifest so it is never written down) and
takes it whenever a key is configured. With no key it takes a **second official Census
route**: the table-based Summary File,
`www2.census.gov/programs-surveys/acs/summary_file/2023/table-based-SF/data/5YRData/acsdt5y2023-b01003.dat`
— same vintage, same table, same estimates, no key, 18.3 MB, cached and hashed like every
other reference file. `dim_block_group.acs_route` records which route produced each row,
and the manifest says `summary_file` for this build.

Result: **36,105 of 36,105 block groups have a population**, 57,739,962 people in total
(MD 4,079 BGs · TX 18,638 · FL 13,388), zero nulls, zero GEOID mismatches between the ACS
2023 geography and TIGER 2025.

### 2. The polygon test caught a decoding bug in silver: nine Texas counties are wrong

This is the most consequential finding of the phase. `pip_county_geoid` disagrees with the
source-stated county on **1,798 rows**, and **1,336 of them (74%) are one systematic
defect**:

`src/transform/txdot.py` decodes CRIS `cnty_id` to FIPS as `FIPS = 2 * cnty_id - 1`,
justified by "Texas county FIPS are odd numbers assigned in alphabetical order, and CRIS's
`cnty_id` is the same alphabetical ordinal", spot-checked on Harris, Bexar, Dallas,
Tarrant and El Paso. The rule is right almost everywhere and wrong for exactly nine
counties, because **the two sources collate "Mc" differently**. The Census FIPS sequence
sorts `Mc` as `Mac` (McCulloch, McLennan, McMullen, *then* Madison, Marion, Martin, Mason,
Matagorda, Maverick); CRIS sorts plainly (Madison … Maverick, *then* McCulloch …
McMullen). The five spot-checked counties all sit outside that block, so the check passed.

The nine-county permutation predicted by that hypothesis reproduces **every single
observed disagreement pair**, exactly:

| source says | polygon says | rows |
|---|---|---|
| 48321 Matagorda | 48309 McLennan | 873 |
| 48317 Martin | 48323 Maverick | 192 |
| 48315 Marion | 48321 Matagorda | 101 |
| 48307 McCulloch | 48313 Madison | 72 |
| 48311 McMullen | 48317 Martin | 44 |
| 48309 McLennan | 48315 Marion | 22 |
| 48319 Mason | 48307 McCulloch | 19 |
| 48323 Maverick | 48311 McMullen | 9 |
| 48313 Madison | 48319 Mason | 4 |
| | **total** | **1,336** |

**Not fixed here**, because the brief puts `src/transform/` out of scope and the fix
belongs where the decode lives. The fix is a nine-row exception table (or, better, an
explicit CRIS county lookup instead of an arithmetic rule) in `src/transform/txdot.py`,
plus a regression test that asserts the two collations agree — which is what this
polygon check has now become. Blast radius: `fact_crash.geography_sk` for ~1.4% of TxDOT
rows, and the county-level `tz` fallback for TxDOT rows in those counties that have no
coordinate. **No timezone impact**: all nine counties are `America/Chicago`, so no
calling window moves. `crash_geo.pip_county_geoid` already carries the correct answer, so
a consumer can prefer it today.

The remaining **462 disagreements** are the legitimate kind: a crash on a county line, or
a report filed by the responding agency's county (Montgomery MD → Prince George's 162,
Collin ↔ Denton 24, Williamson → Travis 15). Counted, never corrected — `geography_sk`
stays the source's answer and `crash_geo` carries the polygon's, so both are visible.

### 3. Five crashes are not in the United States, and the timezone lookup is what found them

Coordinate-derived zones outside a jurisdiction's plausible set are counted and warned
(`config/geo.toml [tz.expected_zones]`). Texas produced `America/Ciudad_Juarez` ×2,
`America/Matamoros` ×2 and `Etc/GMT+6` ×1 — four crashes across the Rio Grande and one in
the Gulf of Mexico. The county PIP agrees: those same rows are 5 of the 6
`county_pip_status = NO_POLYGON` results against the national county layer (the sixth is
offshore of Jacksonville). They pass the statewide bbox because the bbox is a rectangle
and the border is not. **Not corrected** — the coordinate is the evidence, and overwriting
it would hide the defect the zone lookup just surfaced.

### 4. Filtering the county layer to three states made the refinement lie

The first version loaded TIGER COUNTY filtered to FIPS 12/24/48 and reported 101 geocoded
crashes in "no county". 79 of them were Montgomery-reported crashes **in the District of
Columbia**, which the filter had removed — a result indistinguishable from a point in the
Atlantic. `load_counties` now defaults to the national layer (3,235 polygons, no
measurable cost) and the county hit rate went from 99.961% to **99.9977%**.

### 5. The bbox covering column does not prune at this data volume, and the row order is
what would make it

Measured, not assumed — see [The measurements](#geoparquet-partitioning-and-the-bbox-measurement).
A real partition (`jurisdiction=MD/year=2024`) is 11,141 rows; at Phase 3's columnar
row-group size of 122,880 that is **one row group**, so the covering bbox can never skip
anything while still costing four doubles per row. The partitioning by (jurisdiction,
year) is what actually prunes here. The row-group tier is set to 4,096 so it exists and
scales, and the measurement shows the mechanism working at aggregate scale.

### 6. `sjoin_nearest` returns every tied match, and the ties are real

225 Montgomery crashes are exactly equidistant from two ways — the normal case at an
intersection where both approaches are the same distance away. Without an explicit rule
the row count and the chosen way both depend on the spatial index build order, which
would break byte-identity. Lowest `osm_way_id` wins; the count is reported.

### 7. `ST_ReadOSM` reads the 214 MB extract in 18 seconds; `pyosmium` was not needed

The brief allowed `pyosmium` as a new dependency if DuckDB proved too slow or awkward. Two
streaming passes over the PBF (ways with a `highway` tag; nodes inside the padded clip
box), then `unnest` + `ST_MakeLine` in node order, produce 178,354 ways in ~18 s.
**No new dependency was added in this phase.** `requirements.txt` is unchanged.

---

## What I bounded and why

- **Texas road snapping: not attempted.** 136,164 rows leave with
  `snap_status = 'NOT_ATTEMPTED'`, a recorded outcome rather than a silent NULL. Cost of
  the unbounded version: the Texas Geofabrik extract is 683 MB (~3.2× Maryland, so ~60 s
  of `ST_ReadOSM` at the measured rate, plus a ~9-minute download at the ~2 MB/min
  Geofabrik gave this host) and the 92,721 geocoded TxDOT rows are spread over 254
  counties, so the clip box is the whole state and the road table is roughly 20× the
  178k-way Montgomery one — a few minutes of `sjoin_nearest`, and a per-county CRS loop
  (`[crs.snap]` already has `TX = 32139` and `TX_statewide = 3083`) because one state-plane
  zone is not honest across Texas. El Paso alone (48141) would have doubled as the
  Mountain-time demonstration for ~3,584 rows; the timezone distribution already
  demonstrates that without it.
- **Weather: Montgomery, 2024-01-01 onward.** 27,527 rows joined from **36 requests** (13
  H3 r5 cells × 3 years) against a 10,000/day cap — 0.36% of the daily budget. The
  unbounded version is three jurisdictions × 2015–2026: roughly 1,900 r5 cells × 12 years
  ≈ 23,000 requests, i.e. **three days** against the 10,000/day cap. That is a scheduled
  backfill with a resume cursor (the cache path *is* the cursor — `fetch_cell_year`
  returns from disk before a client is constructed, so a killed run resumes for free), not
  a stage in an interactive build. At 5 req/s it is ~77 minutes of wall clock spread over
  three days.
- **ACS: population only.** `B19013` (median household income), `B25044` (vehicles by
  tenure), `B08301`/`B08303` (commute) are deliberately not fetched. They are the
  protected-class proxies ASSIGNMENT.md Part 4 warns about, and the strongest guarantee
  that a feature does not leak into a lead score is that the pipeline never loaded it.
  Population is different in kind: it is a **denominator**, used in aggregate to normalise
  a hotspot rate, and it never becomes a per-record attribute of a lead. A test parses
  `reference.py` and `census_join.py` and asserts none of those table codes appears in any
  non-docstring string literal — they exist only in prose explaining their absence.
- **No `dim_tract`.** `tract_geoid` is `bg_geoid[:11]` by the Census's own GEOID
  construction, cross-checked against a direct TIGER TRACT join on a 5,000-row sample:
  **5,000 agree, 0 disagree**. A table whose only content is a substring of another
  table's key is not a dimension.
- **No SCD2 on enrichment columns.** They are derived — a pure function of (coordinate,
  reference file) — so their history is reconstructible by re-running against the older
  reference bytes, whose hash the manifest holds. A second version history would be a
  second thing that can disagree with the first.

---

## Bugs the tests caught

- **`h3.latlng_to_cell(nan, nan, 9)` raises rather than returning null.** A DOUBLE column
  with nulls arrives as NaN, not `None`, so the `is None` guard missed every ungeocoded
  row and the build logged 7,429 warnings. `_missing()` now tests `value != value` as
  well, and a test passes `None`, `np.nan` and `pd.NA`.
- **An all-NULL object column is typeless, and typeless is INTEGER.** With `--skip-snap`,
  `osm_highway` and friends held only `None`, reached DuckDB as INTEGER, and failed the
  contract's `string` type. `_typed_snap()` now pins every dtype, so a skipped stage
  produces the same *schema* as a full build and only different values — which is what
  makes the two comparable at all.
- **DuckDB's `.df()` widens DATE to `datetime64`.** `crash_date` was silently becoming a
  midnight TIMESTAMP; the contract caught it. Cast back to `datetime.date` before the
  write, so parquet gets `date32`.
- **The CRS negative control was asserting the ratio upside down.** The first version
  asserted `mercator / honest ≈ 1.67`; the measurement said 0.598. Both are the same
  factor — a 500 m buffer *built* in 3857 covers 500·cos(lat) ≈ 387 m of ground, so its
  ground area is cos²(lat) of the honest one and the honest one is 1/cos²(lat) = 1.67× it.
  The test now asserts both directions, including the assignment's own 387 m figure.
- **A grep-based CRS test cannot tell code from prose.** The first "no 3857 in `src/`"
  test failed on the docstring explaining *why* 3857 is banned. Both that test and the
  "h3 v4 only" test now parse the AST: comments never reach it, and a numeric `3857`
  constant is by construction a value the code would use.
- **DuckDB hands back `bytearray`; `shapely.from_wkb` wants `bytes`.** Caught on the first
  real PBF read.
- **Two Phase 3 tests enumerate every table in the gold contract.** Adding `crash_geo` and
  `dim_block_group` to `contracts/gold.schema.json` broke
  `test_gold_tables_pass_their_contract` and `test_every_foreign_key_resolves_with_no_nulls`
  with a `CatalogException` — they looked for a `gold_crash_geo` view in a fixture that
  builds only Phase 3's tables. Scoped both to `model.COLUMNS` (the tables `build_gold`
  actually writes). The assertions are unchanged; only the enumeration is.
- **GeoParquet's `bbox` column is not a contract column.** The contract validates the
  *logical* table, which is what `build_geo` checks before writing (geometry as WKB, no
  `bbox`). The read-back test now projects to that same shape rather than validating a
  physically different one.

---

## Verification

### Row reconciliation, `fact_crash` → `crash_geo`

| | rows |
|---|---|
| `fact_crash` | 268,493 |
| `crash_geo` rows / distinct `crash_sk` | 268,493 / 268,493 |
| missing from `crash_geo` / not in `fact_crash` | 0 / 0 |
| with geometry (= `geo_quality = 'OK'`) | 261,064 |
| OK rows with a block group | 260,963 |
| OK rows with H3 r9/r8/r7 | 261,064 |
| rows with `tz_iana` (every row) | 268,493 |
| rows with `crash_datetime_utc` | 268,486 |
| `time_status = 'TIME_UNKNOWN'` (FARS `HOUR = 99`) | 7 |

The 7 `TIME_UNKNOWN` rows are exactly the seven FARS rows Phase 3 gave `time_sk = -1`.
They keep `crash_date` and have a NULL UTC — collapsing an unknown hour to midnight would
invent a crash time.

### Byte identity

Two consecutive `python -m src.geo.build` runs, warm reference cache:

| output | rows | row groups | size | sha256 (first 16) |
|---|---|---|---|---|
| `crash_geo.parquet` (canonical) | 268,493 | 3 | 30.98 MB | `a63ab1df299d0bad` |
| `crash_geo/` (24 partitions) | 268,493 | 74 | 29.05 MB | all 24 files identical |
| `dim_block_group.parquet` | 36,105 | 1 | 1.60 MB | `ec5ebd35549f5ace` |

Identical on both runs, including every one of the 24 partition files. The second run made
**0 Open-Meteo requests and 36 cache hits**. `_geo_build_sha` is a hash of the inputs
(fact_crash's sha256 + every reference file's sha256 + the geo config), never of the clock.

### Restatement — one changed row changes one row

`test_changing_one_fact_row_changes_exactly_one_crash_geo_row`: rewrite `fact_crash` with
one OK row's latitude moved 0.01°, rebuild, diff. Exactly **one** `crash_geo` row differs;
the row count is unchanged. `_geo_build_sha` moves for every row by design (it hashes the
inputs, and `fact_crash` changed) and is excluded from the row diff — the same treatment
Phase 3 gives `_silver_build_sha`.

### `--offline`

```
$ python -m src.geo.build --offline --reference-root /tmp/nope-empty
src.geo.reference.MissingReference: /tmp/nope-empty/tiger/2025/BG/tl_2025_12_bg.zip is missing.
  fetch it with: python -m src.geo.reference --all
```

Fails naming the file and the fix. It does not skip a stage.

### CRS audit

`grep -rn 3857 src/ tests/` hits **only** comments, docstrings and the negative-control
test. `test_no_source_file_contains_3857_as_executable_code` proves it by AST: a numeric
3857 constant anywhere in `src/` fails the build. Every reprojection in `src/geo/` sits
next to a comment naming the CRS and the reason.

### Tests

| suite | fixture | full corpus |
|---|---|---|
| Phase 3 end state | 182 passed, 4 xfailed | 177 passed, 5 skipped, 4 xfailed |
| after Phase 4 (`test_geo.py`, +78 tests) | **260 passed, 4 xfailed, 52 s** | **255 passed, 5 skipped, 4 xfailed, 5m45s** |

182 + 78 = 260 on the fixture and 177 + 78 = 255 on the full corpus: every Phase 3 test
still passes and Phase 4 adds no new skip. The five full-corpus skips are Phase 3's
fixture-only restatement scenarios, unchanged.

No test touches the network. The Open-Meteo test proves it structurally: it points the
cache at `tmp_path` with the committed response in place and hands in a client whose
`get()` raises. Integration tests skip with a clear reason when `data/reference/` is
absent; `CRASH_TEST_FULL_REFERENCE=1` turns that skip into a failure.

---

## The measurements

### Point-in-polygon hit rate

| source | rows | with geometry | matched to a block group | no polygon | hit rate |
|---|---|---|---|---|---|
| MONTGOMERY_MD | 125,005 | 124,900 | 124,821 | 79 | 99.937% |
| TXDOT_CRIS | 100,000 | 92,721 | 92,703 | 18 | 99.981% |
| NHTSA_FARS | 43,488 | 43,443 | 43,439 | 4 | 99.991% |
| **all** | **268,493** | **261,064** | **260,963** | **101** | **99.961%** |

The 101 `NO_POLYGON` rows are outside every block group in the three states: 79 Montgomery
crashes in the District of Columbia and Virginia (real, see below), 6 outside the United
States, and 16 across other state lines. Against the **national county** layer only 6 rows
have no polygon at all — hit rate **99.9977%**.

Boundary ties: 19 crashes lie exactly on a shared block-group edge and matched two
polygons under `intersects`; each was resolved to the smallest GEOID. Zero ties on the
county layer. Nineteen out of 261,064 is the shape you want — the layers are consistent.

Tract cross-check: 5,000-row fixed-seed sample, `bg_geoid[:11]` vs a direct TIGER TRACT
join — **5,000 agree, 0 disagree**.

### The bbox → polygon refinement (what the padded envelope let in)

`config/geo.toml` pads the Montgomery envelope 0.02° beyond the county so the superset
property is true rather than approximately true. That padding is a margin, and this is its
size: of 125,005 geocoded Montgomery-reported crashes, **288 are outside Montgomery
County**.

| actually in | rows |
|---|---|
| 24033 Prince George's County, MD | 162 |
| 11001 District of Columbia | 46 |
| 51059 Fairfax County, VA | 32 |
| 24021 Frederick County, MD | 29 |
| 24027 Howard County, MD | 17 |
| 24013 Baltimore County, MD | 1 |
| 51013 Arlington County, VA | 1 |

None is a data error. The Montgomery feed is a *police-agency* feed (Phase 3 established
it carries no Maryland State Police), and an agency files reports slightly outside its
county line. `geo_quality` is untouched; `crash_geo` records where the point is.

### Source county vs polygon county

1,798 of 261,058 comparable rows disagree (0.69%). **1,336 are the CRIS Mc/Ma collation
defect** described above; the remaining 462 are county-line and reporting-county cases.

### Timezone

**Zone by jurisdiction** — the memo's evidence that Texas and Florida are each two zones:

| jurisdiction | America/New_York | America/Chicago | America/Denver | other |
|---|---|---|---|---|
| MD | 128,026 | — | — | — |
| TX | — | 117,455 | **4,096** | 5 (see §3 above) |
| FL | 17,700 | **1,211** | — | — |

**Zone by source system**

| source | New_York | Chicago | Denver | other |
|---|---|---|---|---|
| MONTGOMERY_MD | 125,005 | — | — | — |
| TXDOT_CRIS | — | 96,412 | 3,584 | 4 |
| NHTSA_FARS | 20,721 | 22,254 | 512 | 1 |

**`tz_source` distribution**

| source | rows | |
|---|---|---|
| `COORDINATE` | 261,064 | every geocoded row |
| `COUNTY_FALLBACK` | 7,429 | 7,279 TxDOT `MISSING` + 105 Montgomery `OUT_OF_ENVELOPE` + 45 FARS `SENTINEL` |
| `JURISDICTION_DEFAULT` | 0 | every no-coordinate row's county had geocoded neighbours |
| `UNRESOLVED` | 0 | |

1,537 rows are marked `tz_low_confidence` (a fallback in a county whose coordinate-derived
zone is not unanimous). 351 counties got a zone from the data itself; **4 are split**:

| county | modal zone | share | coordinate rows | reading |
|---|---|---|---|---|
| 12045 Gulf County, FL | America/Chicago | **52.6%** | 19 | genuinely split — the Apalachicola River line runs through it |
| 48029 Bexar, TX | America/Chicago | 99.99% | 9,294 | one stray coordinate |
| 48141 El Paso, TX | America/Denver | 99.94% | 3,589 | one stray coordinate |
| 48479 Webb, TX | America/Chicago | 99.85% | 1,357 | the two Matamoros points |

Gulf County is the real one, and it is exactly the case the brief predicted: a county-level
timezone lookup would be wrong for nearly half of it.

**UTC offsets actually produced:** −240 (EDT) 94,900 · −300 (EST/CDT) 127,022 · −360
(CST/MDT) 45,085 · −420 (MDT) 1,479.

**DST — the rows a naive pipeline gets wrong by exactly one hour**

| year | gap-adjusted (spring forward) | ambiguous (fall back) |
|---|---|---|
| 2015 | — | 3 |
| 2016 | — | 2 |
| 2018 | — | 2 |
| 2019 | — | 6 |
| 2020 | 1 | 11 |
| 2021 | — | 10 |
| 2022 | — | 6 |
| 2023 | 1 | 5 |
| 2024 | — | 10 |
| 2025 | — | 4 |
| **total** | **2** | **59** |

Sixty-one rows in eleven years. They are the whole point: each one is a wall clock that is
either impossible or happened twice, and a pipeline that localises without flagging them
produces a UTC instant that is confidently an hour wrong. `tz_gap_adjusted` and
`tz_ambiguous` carry the uncertainty into Phase 6, which can refuse to compute a calling
window from them. The asymmetry (2 vs 59) is expected: the gap hour does not exist so
nothing can be stamped inside it except by a source that rounds, whereas the repeated hour
is a real hour of real driving.

### Road snapping (Montgomery, EPSG:26985)

178,354 OSM ways in the padded clip box. 124,900 crashes attempted.

**Snap distance distribution** (all 124,853 rows that found a candidate within 500 m):

| p50 | p75 | p90 | p95 | p99 | p99.5 | p99.9 | max | mean |
|---|---|---|---|---|---|---|---|---|
| 2.94 m | 5.85 m | 9.66 m | 13.93 m | 31.33 m | 41.09 m | 98.93 m | 431.6 m | 4.74 m |

**Threshold: 50 m**, and here is why rather than taste. The histogram breaks:

| band | rows | rows per metre |
|---|---|---|
| 0–5 m | 86,172 | 17,234 |
| 5–10 m | 27,051 | 5,410 |
| 10–20 m | 8,385 | 839 |
| 20–30 m | 1,868 | 187 |
| 30–40 m | 712 | 71 |
| 40–50 m | 236 | 24 |
| **50–75 m** | **251** | **10** |
| 75–100 m | 55 | 2.2 |
| 100–500 m | 123 | 0.3 |

The density falls by roughly an order of magnitude per band up to 50 m and then flattens
into a long thin tail. 50 m is where the road-adjacent distribution ends and the
geocoding-failure tail begins; it is also ~1.6× p99, so it accepts essentially the entire
plausible distribution. Alternatives measured: 35 m (p99 rounded) rejects 924 rows
(0.74%), 30 m rejects 1,377 (1.10%), 100 m rejects 123 (0.10%). The assignment's 400 m
would reject 4 rows and is the outer bound, not an answer.

**Outcomes**

| status | rows |
|---|---|
| `SNAPPED` | 124,424 (99.6% of attempted) |
| `REJECTED_DISTANCE` | 476 — 251 at 50–75 m, 113 at 75–150 m, 56 at 150–300 m, 9 beyond, 47 with no road within 500 m |
| `NOT_ATTEMPTED` (TxDOT + FARS) | 136,164 |
| `NO_GEOMETRY` | 7,429 |

225 ties broken on the lowest `osm_way_id`.

**Linear-reference cross-check.** For 2,000 sampled snapped rows,
`line.interpolate(offset_m)` was reprojected to 4326 and its distance to the crash
measured geodesically with `pyproj.Geod` on the WGS84 ellipsoid — a different method in a
different frame from the projected `sjoin_nearest` distance. **Max absolute disagreement
2 mm, mean 0.2 mm, 2,000/2,000 within 1 m.** Two independent methods agreeing is evidence;
one method repeated is not.

**`maxspeed` / `lanes` null rates — reported, never imputed.** Overall on 124,424 snapped
rows: `maxspeed` 64.1% null, `lanes` 46.7% null. The overall number hides the shape:

| highway class | rows | maxspeed null | lanes null |
|---|---|---|---|
| motorway | 1,736 | 1.7% | 0.5% |
| trunk | 4,944 | 14.2% | 0.0% |
| primary | 35,704 | 30.2% | 1.7% |
| secondary | 14,088 | 51.3% | 30.6% |
| tertiary | 19,543 | 73.3% | 48.4% |
| unclassified | 699 | 90.4% | 67.0% |
| residential | 19,624 | 93.0% | 91.0% |
| service | 24,163 | 99.5% | 98.8% |
| motorway_link | 1,653 | 91.6% | 35.0% |
| primary_link | 1,606 | 97.5% | 38.5% |
| secondary_link | 171 | 100.0% | 63.2% |

Exactly the pattern the assignment predicts: densely tagged on major corridors, absent
outside them. Imputing a residential speed limit would be inventing a survey. `maxspeed`
is normalised to `osm_maxspeed_mph` where it parses (44,690 of 44,720 raw values; the 30
that do not are `walk`, `signals` and multi-value tags) and the raw string is kept.

### Weather (Montgomery, 2024-01-01 →, keyed on the UTC hour)

| | |
|---|---|
| bucket | H3 r5 (~247 km², ~8.5 km edge — just coarser than ERA5's ~9 km grid) |
| requests | **36** (13 cells × 3 years), 0 on a warm re-run |
| against limits | 0.36% of 10,000/day; 6% of one minute's 600 |
| rows in scope / joined | 27,539 / **27,527** |
| `NO_DATA` | 7 — all 2026-09-02, past the ERA5 archive's ~5-day lag |
| `NO_GEOMETRY` | 5 |
| ERA5 hours cached | 283,704 |

**Officer-reported precipitation vs ERA5 `> 0` mm**, on the 27,242 joined rows where the
officer recorded a condition:

| | ERA5 dry | ERA5 wet |
|---|---|---|
| **officer dry** | 21,622 | 2,491 |
| **officer wet** | 884 | 2,245 |

Agreement 87.6%. Officer says wet 11.5% of the time; ERA5 says wet 17.4%. The asymmetry is
the whole story: **ERA5 is wet 2.8× more often than the officer where they disagree**,
because ERA5 reports the hourly accumulation over a 9–25 km box and any trace above zero
counts, while the officer reports what was falling on the windscreen at that moment. A
0.1 mm hourly mean smeared over a county is not rain a driver would notice. The 884 rows
the other way are the interesting ones — a genuinely local shower the reanalysis smoothed
away, or a wet road hours after the rain stopped.

That is why the ERA5 column is a **context** feature and not a causal one, and why the
choice was reanalysis rather than stations: a station join is *systematically* missing —
rural crashes are further from stations than urban ones, so every downstream rate would be
biased by population density. Reanalysis trades point accuracy for completeness, and
completeness is what makes the column comparable across 269,000 crashes.

<a id="geoparquet-partitioning-and-the-bbox-measurement"></a>
### GeoParquet, partitioning, and the bbox measurement

Both artefacts are GeoParquet **1.1.0** with `covering.bbox`, `primary_column =
"geometry"`, WKB encoding and CRS EPSG:4326 (asserted by tests on the flat file and on
every partition file).

| artefact | rows | files | row groups | size | order |
|---|---|---|---|---|---|
| `crash_geo.parquet` **(canonical)** | 268,493 | 1 | 3 @ 122,880 | 30.98 MB | `crash_sk` |
| `crash_geo/jurisdiction=XX/year=YYYY/` | 268,493 | 24 | 74 @ 4,096 | 29.05 MB | `h3_r9, crash_sk` |

The **flat file is canonical** because it matches what Phases 5–7 actually do: join on
`crash_sk` and scan every row to build a hotspot surface. The partitioned copy exists for
the read the flat file is bad at — a bounding-box filter — and is the one whose row order
is chosen to make the covering bbox work.

**The measurement.** All Maryland geocoded rows (127,914), written at four row-group sizes
in three row orders, queried with two bbox sizes. "Pruned" is the share of row groups whose
covering bbox does not intersect the query box, read straight from parquet statistics.

| order | row-group size | groups | size | county box (13×11 km) pruned | corridor box (1.7×1.1 km) pruned |
|---|---|---|---|---|---|
| `h3_r9` | 4,096 | 32 | 14.93 MB | 28.1% | **65.6%** |
| `h3_r9` | 8,192 | 16 | 14.86 MB | 6.2% | 37.5% |
| `h3_r9` | 32,768 | 4 | 14.92 MB | 0% | 0% |
| `h3_r9` | 122,880 | 2 | 14.77 MB | 0% | 0% |
| hilbert | 4,096 | 32 | 14.81 MB | 40.6% | **81.2%** |
| hilbert | 8,192 | 16 | 14.73 MB | 18.8% | 75.0% |
| `crash_sk` (no spatial order) | 4,096 | 32 | **17.46 MB** | **0%** | **0%** |
| `crash_sk` (no spatial order) | 122,880 | 2 | 15.83 MB | 0% | 0% |

Three things fall out.

1. **The row order is the whole mechanism.** Without a spatial sort, pruning is 0% at
   every row-group size for every query — each group's bbox is the whole state. This is
   the brief's point, measured.
2. **Sorting also shrinks the file**, which was the unexpected result: 14.86 MB vs
   17.04 MB at 8,192 rows/group, a **12.8% saving**, because adjacent rows share
   coordinate prefixes and the four bbox doubles compress far better in sorted order.
   That benefit is real today even where pruning is not.
3. **Row-group size only matters if a partition holds more than one group.** A real
   partition here is 11,141 rows, so 122,880 makes the covering column *inert* while still
   costing four doubles per row. **4,096 chosen**: three groups per partition today for
   +0.8% bytes (1,491.5 KB vs 1,479.8 KB on `MD/2024`), and at the assignment's 10×
   volume a partition is ~110k rows / 27 groups, where the 66% corridor pruning above
   becomes real. Query time at this scale is 2–8 ms and dominated by decompression, not
   pruning — 122,880 was consistently the *slowest* (7.6 ms vs ~3 ms) because a single
   huge row group can be neither skipped nor parallelised.

**Hilbert ordering prunes better than H3 (81% vs 66% on the corridor box)** and is the
honest rejected alternative: H3's hex-string order is a Z-order-like walk within a base
cell, and Z-order has long jumps whose bounding boxes overlap. `h3_r9` was kept because
Phase 5 groups by H3 — a file already sorted on the column it will `GROUP BY` gives cheap
dictionary encoding and prunable min/max statistics on `h3_r9` itself, which a Hilbert
sort does not. If bbox filtering were the dominant read pattern, Hilbert would win.

Reproduce the whole table with:

```
python -m src.geo.build --measure-row-groups
```

It rewrites an existing `crash_geo.parquet` at each size in each order into a temp
directory and reads the pruning straight out of parquet's own row-group statistics for
the `bbox.*` columns — a property of the file rather than a timing that depends on what
else the machine is doing.

### Reference data

| file | size | sha256 (first 16) | server Last-Modified |
|---|---|---|---|
| `tiger/2025/COUNTY/tl_2025_us_county.zip` | 83.99 MB | `9c6e9d9076abce26` | 2025-09-23 |
| `tiger/2025/BG/tl_2025_{12,24,48}_bg.zip` | 22.54 / 9.77 / 50.58 MB | `cc2697ea7f6527b7` / `cdd98b882a6ab588` / `34af53533807263d` | 2025-09-22 |
| `tiger/2025/TRACT/tl_2025_{12,24,48}_tract.zip` | 13.85 / 5.88 / 32.79 MB | `1b4630c772eb6c29` / `d5f56e491c91da8f` / `b930f9e7742a071e` | 2025-09-22 |
| `acs/2023/summary_file/acsdt5y2023-b01003.dat` | 18.31 MB | `24ae3f523b4c5433` | 2024-10-31 |
| `osm/maryland-latest.osm.pbf` | 214.08 MB | `a138d4e83fdc3fd8` | 2026-09-07 |

252 MB total, all gitignored. The PBF hash is pinned in `config/geo.toml
[reference.osm] maryland_sha256`; a mismatch is logged as a restatement of every snap
column.

---

## For DATA_QUALITY.md

- **CRIS `cnty_id` → FIPS is wrong for nine Texas counties.** 1,336 rows (1.4% of geocoded
  TxDOT) carry the wrong `geography_sk` because the Census sorts "Mc" as "Mac" and CRIS
  does not. *Detection*: `crash_geo.county_agrees_with_source`, or
  `_geo_manifest.json → stats.county_refinement.disagreement_pairs`. *Disposition*:
  reported, not corrected here (`src/transform/` is out of Phase 4's scope);
  `crash_geo.pip_county_geoid` has the right answer today. *Fix*: a nine-county exception
  in `src/transform/txdot.py` plus a regression test comparing the two collations. *No
  timezone impact* — all nine counties are Central.
- **Five crashes are outside the United States.** Four across the Rio Grande
  (`America/Ciudad_Juarez` ×2, `America/Matamoros` ×2) and one in the Gulf of Mexico
  (`Etc/GMT+6`); a sixth is offshore of Jacksonville. They pass the statewide bbox because
  a border is not a rectangle. *Detection*: `[tz.expected_zones]` mismatch, and
  `county_pip_status = 'NO_POLYGON'` against the national county layer. *Disposition*:
  counted and warned, never corrected — the coordinate is the evidence.
- **288 Montgomery-reported crashes are not in Montgomery County** (162 Prince George's,
  46 DC, 32 Fairfax VA, 29 Frederick, 17 Howard, 1 Baltimore County, 1 Arlington VA).
  Not an error: the feed is a police-agency feed and the envelope is deliberately a padded
  superset. *Detection*: `stats.bbox_vs_polygon`.
- **61 crashes in eleven years have a wall clock that is impossible or ambiguous.** 2 in
  the spring-forward gap, 59 in the fall-back repeated hour. *Detection*:
  `tz_gap_adjusted` / `tz_ambiguous`. *Disposition*: resolved by a stated policy and
  flagged, so Phase 6 can refuse them.
- **7,429 crashes have no coordinate and therefore no coordinate-derived timezone.** They
  take the county's modal zone; 1,537 of those are `tz_low_confidence` because the county
  is not unanimous. Gulf County FL is genuinely split 52.6/47.4.
- **`maxspeed` is absent for 64% and `lanes` for 47% of snapped Montgomery crashes**, from
  1.7% on motorways to 99.5% on service roads. Reported per class; never imputed.
- **476 Montgomery crashes are further than 50 m from any road** (47 further than 500 m,
  9 beyond 300 m). *Disposition*: `snap_status = 'REJECTED_DISTANCE'` with the distance
  kept and the road attributes NULL.
- **19 crashes sit exactly on a block-group boundary** and 225 exactly between two OSM
  ways. Both resolved by a stated deterministic rule (smallest GEOID; lowest way id) and
  counted.
- **The ACS API requires a key that this host does not have.** The build takes the
  keyless Census Summary File route and records `acs_route = 'summary_file'` per row.

---

## For DECISIONS.md

Each with the rejected alternative.

- **A new `crash_geo` table, not a widened `dim_geography` or `fact_crash`.** Rejected:
  widening `dim_geography` (tract/BG/H3 are properties of a *point*, not of a county, so
  they cannot hang off a county dimension without changing its grain); adding columns to
  `fact_crash` (moves Phase 3's hashes and re-runs its 182 tests for an enrichment that is
  rebuildable from hashed reference inputs, and couples a 250 MB polygon download to the
  fact build). `crash_geo` has exactly one row per `crash_sk`, including rows with no
  geometry, so it joins 1:1 without a left join.
- **No `dim_tract`.** Rejected: a separate tract dimension, whose only content would be
  `bg_geoid[:11]` — proven equal to a direct TIGER TRACT join on a 5,000-row sample.
- **PIP predicate `intersects` + smallest-GEOID tie-break.** Rejected: `within` (silently
  drops a point on a shared edge — a real loss with no signal); `intersects` alone
  (double-counts 19 rows and breaks the one-row-per-crash grain). The tie count is
  reported so a large number would be visible as a layer inconsistency.
- **PIP runs in EPSG:4326 with no projection.** Rejected: reprojecting first — PIP is
  topological, so it buys nothing and costs a reprojection of 36,105 polygons. TIGER's
  NAD83 is still converted explicitly rather than relabelled.
- **National county layer, not the three in-scope states.** Rejected: the filtered layer,
  which reported 79 DC/Virginia crashes as "in no county" — indistinguishable from a point
  in the ocean.
- **DST: gap → shift forward; ambiguity → `fold=0` (DST still in effect); both flagged.**
  Rejected: `fold=1` (arbitrary in the other direction); `NaT` for both (throws away 61
  crashes that have a date, an approximate time, and a county); silently trusting a
  library's normalisation (the flag is the deliverable, and an inherited default is not a
  decision). Both policies are config (`[tz.dst]`).
- **Timezone fallback derived from the corpus, not from a shipped county→zone table.**
  Rejected: a static lookup (one more thing to keep current, and it cannot express that
  Gulf County is 52/48); the state's zone (the error the assignment warns about twice).
  Cost: a county with no geocoded row gets no entry — which is correct, and is what
  `JURISDICTION_DEFAULT` is for.
- **Snap threshold 50 m, from the measured distribution.** Rejected: 35 m (p99 rounded —
  rejects 924 rows that the histogram says are still road-adjacent); 400 m (the
  assignment's outer bound, rejects 4); any number chosen before measuring. The threshold
  is config, and the distance is stored for rejected rows so the choice is auditable.
- **Snapping in EPSG:26985 per jurisdiction, `snap_crs_epsg` stored per row.** Rejected:
  EPSG:3857 (wrong by 1/cos(39.1°) = 1.288 — a 50 m threshold silently becomes 38.8 m);
  EPSG:5070 for snapping (equal-area distorts local distance and shape, which is what
  snapping measures); a single statewide CRS for Texas (state plane is only honest near
  its central meridian — `TX_statewide = 3083` is configured for a multi-county run).
- **Footways, cycleways, paths and steps excluded from the road network.** Rejected:
  including them (snapping a car crash to a footpath 8 m away beats the carriageway 30 m
  away, and it would make the `maxspeed`/`lanes` null rates an artefact of the filter
  rather than a fact about OSM).
- **`ST_ReadOSM` in DuckDB, no new dependency.** Rejected: `pyosmium` (a new dependency to
  do what the database already does in 18 s); `pyrosm` (not installed); Overpass (never —
  fair-use quota, and a statewide pull is explicitly forbidden).
- **Weather bucketed to H3 r5, one request per (cell, year).** Rejected: one request per
  crash (124,900 requests, 5 hours, 12× the daily cap for 13 cells' worth of information);
  one request per cell for the whole range (Open-Meteo weights a long hourly range as
  multiple calls); 0.1° rounding (a fine alternative — H3 was chosen because the pipeline
  already indexes in H3 and a second grid system is a second thing to reason about).
- **Row-group size 4,096 for the partitioned copy, 122,880 for the flat one.** Rejected:
  122,880 everywhere (makes the covering bbox inert inside an 11k-row partition while
  still costing four doubles per row); 1,024 (11 groups per partition, +6.8% bytes for
  pruning the partition scheme already provides).
- **Rows sorted by `h3_r9` within a partition.** Rejected: no spatial sort (0% pruning at
  every size, and 12.8% larger files); Hilbert ordering, which prunes *better* (81% vs
  66% on a corridor box) and is the right answer if bbox filtering dominates — `h3_r9`
  wins because Phase 5 groups by H3 and a file sorted on its own `GROUP BY` key is cheaper
  to read.
- **Flat file canonical, partitioned dataset for spatial reads.** Rejected: partitioned-
  only (Phase 5's whole-dataset scan pays 24 file opens for nothing); flat-only (no
  partition pruning for the bbox reads Phase 5's isochrone and corridor work will do).
  Both are GeoParquet 1.1.0 with identical content; a test asserts they hold the same rows.
- **ACS: population only, and the keyless Summary File when no key is configured.**
  Rejected: fetching income/tenure/vehicles/commute "in case they are useful" (they are
  the Part 4 proxies, and a feature that was never loaded cannot leak); failing the build
  with no key (there is a second official Census route to the same estimates); a
  hard-coded population table (unversioned, unhashed, unreproducible).
- **Restatement by hash, no SCD2 on enrichment columns.** Rejected: SCD2 (these are
  derived values, reproducible from hashed inputs; a second history is a second thing that
  can disagree). Lineage is `_geo_build_sha` plus the manifest.
- **Coordinate errors counted and warned, never corrected.** Rejected: clamping the five
  out-of-country points to the border, or nulling them (both destroy the evidence that a
  geocoder is misbehaving).

---

## For MEMO.md

Three to five plain sentences, no jargon.

Texas and Florida each span two time zones, and this pipeline reads every crash's time
zone from its coordinates rather than from its state: 4,096 Texas crashes are Mountain
time, not Central, and 1,211 Florida crashes are Central, not Eastern. Getting that wrong
would put a call an hour outside the permitted window for six thousand records — and a
system that guessed from the state would never know it had.

Sixty-one crashes over eleven years carry a wall-clock time that is either impossible (the
hour the clocks skip forward) or happened twice (the hour they repeat). We resolve each one
by a stated rule and, more importantly, we mark it, so the compliance step can decline to
compute a calling window from a timestamp we know is an hour uncertain rather than acting
confidently on a guess.

Weather comes from ERA5 reanalysis rather than weather stations, because station data is
missing exactly where it would bias us — rural crashes are further from stations than urban
ones, so a station-based feature would quietly encode population density. The trade-off is
resolution: reanalysis says it rained somewhere in a 9-kilometre box during that hour,
which is why it reports wet conditions almost three times as often as the attending
officer does, and why we treat it as context rather than as cause.

We loaded census population and deliberately did not load income, car ownership or commute
data, even though the same free service offers all of them. Population is a denominator
that makes crash rates comparable between a dense neighbourhood and a sparse one; the
others are close proxies for race and income, and the safest guarantee that they never
influence who gets called is that they were never brought into the system at all.

Checking every crash against the real county boundary — rather than the rectangle we used
earlier — surfaced 1,336 Texas records filed under the wrong county, caused by two
government agencies alphabetising "Mc" differently. Nobody would have found that by
reading the data; it took putting each point on a map.

---

## Open items for Phases 5–7

- **What `crash_geo` gives Phase 5.** `h3_r8`/`h3_r9` for Getis-Ord and LISA over 53,313
  r8 cells; `bg_geoid` joining to `dim_block_group.population` for the normalised-vs-raw
  contrast the assignment asks for; `geometry` in 4326 ready to project to **EPSG:5070**
  for any tri-state per-km² rate; `h3_index.grid_disk` / `disk_weights` for the spatial
  weights matrix (sorted, so a Getis-Ord result does not move between runs);
  `osm_way_id` + `offset_m` for corridor-level ST-DBSCAN rather than point-level.
- **What Phase 6 should read.** `tz_source` — treat anything other than `COORDINATE` as
  `GEOCODE_TIER_INSUFFICIENT` (7,429 rows), and treat `tz_low_confidence` (1,537 rows) and
  `tz_gap_adjusted` / `tz_ambiguous` (61 rows) as the calling-window refusals. Compute
  windows from `crash_datetime_utc` + `tz_iana`, never from `crash_datetime_local`.
- **Sun times for `is_night`.** `dim_time.is_night` is still Phase 3's fixed clock rule
  (before 06:00 or from 20:00). With `utc_offset_minutes` and a coordinate now on every
  row, real sunrise/sunset is a pure function of (date, lat, lon) — a December 17:30 crash
  is dark and a June 17:30 crash is not, and the clock rule calls both daylight. Worth
  doing in Phase 7 if any scoring feature depends on darkness.
- **The TxDOT county fix.** Nine-county exception in `src/transform/txdot.py`; the
  regression test already exists in effect as `county_agrees_with_source`.
- **Texas snapping and the weather backfill**, both costed above.
- **Attribute enrichment across the bridge** (Phase 3's open item) is now cheaper: 208
  matched Montgomery crashes could take FARS's `FUNC_SYS`, but they could equally take
  OSM's `highway` class, which `crash_geo` now has for 124,424 Montgomery rows — a better
  source, and one that does not change the meaning of a `fact_crash` column.
