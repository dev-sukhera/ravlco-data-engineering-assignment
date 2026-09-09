# Phase 3 — Silver → Gold: dimensional model and entity resolution

Scope: `src/transform/model.py`, `src/transform/resolve.py`, `src/transform/conformed.py`,
`contracts/gold.schema.json`, `config/model.toml`, four seed CSVs
(`counties.csv`, `road_class_crosswalk.csv`, `weather_crosswalk.csv`,
`non_motorist_type_crosswalk.csv`), `GOLD_DIR`/`model()` in `src/config.py`, the extended
bronze test extract, and the test suite.
Status: complete and verified end to end, on the fixture and on the full local corpus.

```
python -m src.transform.model             # 15 parquet files, ~4.6 s, byte-identical on re-run
python -m src.transform.resolve --report  # the FARS ∩ {Montgomery, TxDOT} match census
pytest -q                                 # 182 passed, 4 xfailed         (fixture,  43 s)
CRASH_TEST_FULL_BRONZE=1 pytest -q        # 177 passed, 5 skipped, 4 xfailed (full, 4m09s)
```

Measured 2026-09-08 on the local silver (Montgomery 125,005 · TxDOT 100,000-row OID
slice · FARS 2019–2024 national 222,695).

---

## What I built

**`src/transform/model.py`** — the gold builder. It reads silver's current slices,
restricts to the configured jurisdictions, runs entity resolution, and materialises a
star: `fact_crash` (one row per *resolved* crash), `bridge_crash_source` (one row per
contributing silver record), `fact_driver`, `fact_non_motorist`, six dimensions plus
`dim_non_motorist_type`, and four `map_*_source` tables that are the crosswalks as
data. Surrogate keys are the first 60 bits of SHA-256 over the primary record's
`crash_uid` (or the source's party key), so two builds agree byte for byte and a key
never moves when a FARS match is found or lost. Every fact foreign key is NOT NULL and
every dimension carries a `-1` UNKNOWN member; the contract has no `orphans_allowed_when`
anywhere. Same validate-then-write discipline as `build.py`: every table is checked
against `contracts/gold.schema.json` before the first `.part` file is written.

**`src/transform/resolve.py`** — cross-source entity resolution, scoped by a proof
rather than an assertion. Blocking on (jurisdiction, county, ±1 day); scoring on
geodesic distance (pyproj `Geod` on WGS84, registered as a DuckDB scalar UDF, behind an
equirectangular pre-filter so the Python call only runs on pairs that could possibly
match) and wall-clock minutes; three tiers with thresholds in `config/model.toml`;
deterministic one-to-one mutual-best assignment with `crash_uid` as the final tie-break;
a reason for every unmatched record on both sides; and a separate 30-day-death probe for
FARS fatalities whose nearest neighbour the local feed did not call fatal.

**`src/transform/conformed.py`** — the three new conformed vocabularies (road class,
officer-reported weather, non-motorist type) and the loader/registrar/drift check their
seed CSVs share, generalising Phase 2's severity crosswalk pattern. The vocabulary is
code (it fixes surrogate keys and what an analyst may GROUP BY); the mapping is data
(drift is a CSV row with a note, never a Python edit). A `conformed_code` the vocabulary
does not know fails at load, not at join.

**`contracts/gold.schema.json`** — 15 tables in the bronze/silver dialect with
`x-table-constraints`: 17 foreign keys, unique keys on every grain, row-count floors for
the production corpus.

**Fixture extension** — `tests/fixtures/make_extracts.py` now also pulls every 2019
Montgomery fatal crash (32 incidents, 44 driver rows, 15 non-motorist rows) and every
2019 FARS accident in Montgomery County (36, with 47 vehicle and 81 person rows), so the
committed extract holds real FARS ∩ Montgomery pairs. Added, not re-sampled: every prior
block is unchanged, the three synthetic ids are unchanged, and the existing 127 tests
passed on the new extract before a single gold test was written. +448 bronze rows
(3,195 → 3,643). `MANIFEST.json` gains `known_pairs` listing both sides so the test can
re-derive the expected matches independently of `resolve.py`.

---

## Things the spec, the brief, or Phase 2 said that the data doesn't do

1. **FARS has more Montgomery County fatal crashes than the county feed does — and the
   gap is agency coverage, not matching.** FARS 24/031: 36/44/40/46/45/46 for 2019–2024
   (257). Montgomery fatal-typed crashes the same years: 32/40/33/41/34/36 (216). Of the
   257 FARS records, 208 matched (all tier A), 31 have a local candidate on the date but
   none the county recorded as fatal, 18 have a fatal candidate too far away. The
   Montgomery feed's `agency_name` is Montgomery County Police, Rockville, Gaithersburg
   and "MONTGOMERY" — **no Maryland State Police**, which polices I-270, I-495 and I-370.
   The interstate fatalities are in FARS and not in the county feed. This is a coverage
   statement the memo needs: the Montgomery dataset is a county-police dataset, not a
   county dataset.

2. **Montgomery's two fatal signals disagree on 31 crashes and both are needed.**
   `acrs_report_type = 'Fatal Crash'` with a max-over-parties ordinal of 1–4: 31
   crashes (12 at 4, 10 at 3, 7 at 2, 2 at 1); one `Injury Crash` with ordinal 5. Sixteen
   of the 31 matched a FARS record — a fatal block on the ordinal alone would have
   missed them. Those 16 are exactly the `RESOLVED_MAX` rows in `fact_crash`, where the
   resolved severity is 5 and `severity_ordinal_primary` keeps the county's own 1–4.

3. **No accepted match straddles midnight.** The brief and my own early exploration
   assumed a 23:50/00:10 case would appear; over 1,157 accepted matches, `dd ≠ 0` occurs
   zero times. The ±1-day block costs nothing and stays (the unit test proves the path
   works), but the docstrings now say "can", not "does".

4. **The 30-day-death case is real and small: 24 FARS records, 21 otherwise unmatched.**
   Two in Maryland, 22 in Texas have a non-fatal local crash within 250 m and 30 min.
   They are reported, not admitted (`admit_non_fatal_candidates = false`), because
   admitting them would raise the primary record's severity on proximity alone. The
   config flag flips the policy without a code change; the test exercises both.

5. **The CRIS guide is unreachable, so every TxDOT road-class and weather code is
   UNKNOWN.** `https://www.txdot.gov/.../cris-guide.pdf` refused the connection from the
   build host (HTTP 000, two attempts, browser UA). No other citation exists.
   `wthr_cond_id = 11` is 78,336 of 100,000 rows and is "almost certainly Clear" — and
   "almost certainly" is a guess, so the crosswalk rows say UNDECODED, `is_lossy` is
   true, and a test asserts no TxDOT row sits on any member but UNKNOWN. That puts
   100,000 crashes in `UNKNOWN` for both dims; the reconciliation in the manifest shows
   it. Decoding is a one-CSV-edit change once the guide is in hand.

6. **Montgomery `route_type` is ownership, not functional class.** "Maryland (State)"
   spans MD-200 (freeway) to two-lane collectors; "County" includes six-lane arterials.
   Forcing these into FHWA classes would invent data; UNKNOWN would discard the one
   thing the source says. Two bridging members (`STATE_HIGHWAY_UNCLASSIFIED`,
   `COUNTY_MUNICIPAL_UNCLASSIFIED`, `functional_class_known = false`) carry 102,585 crashes
   honestly. Only `Interstate (State)` (1,932), `Local Route` (580) and `Ramp` (824) map to
   a real class.

7. **FARS vehicle and person `MAKE`/`BODY_TYP` labels exist in the codebook; `MODEL` is
   make-relative and has no flat label.** `fact_driver.vehicle_make` is the codebook
   label for FARS and free text for Montgomery; `vehicle_model` is NULL for FARS rather
   than a bare integer that would collide with Montgomery's strings.

8. **Montgomery publishes no driver age, no work-zone flag, no fault for non-motorists
   beyond Yes/No/Unknown; TxDOT publishes no persons and no hit-and-run flag at the crash
   grain.** Each is a typed NULL with a comment in `_crash_attr_sql`, not a zero.

---

## What I bounded and why

- **Gold is current-state only.** History stays in silver's SCD2; gold carries
  `silver_version_no`, `silver_valid_from`, `is_amended` and the sha256 of the silver
  table each row came from. Rejected: a second SCD2 in gold (two histories of one event
  that can disagree). The "what did this crash look like on date X" query is a join from
  the fact's `primary_crash_uid` to the silver history table on `valid_from`/`valid_to`.
- **Scope is MD, TX, FL.** 178,050 FARS crashes in 47 other states are excluded, counted
  per state in the manifest. FL has no all-severity source, so FL gold is FARS-only
  (18,911 crashes) and the census says `OUTSIDE_LOCAL_COVERAGE` for every one of them.
- **FARS passengers and other occupants (PER_TYP 2/3/4/9/10) are out of party scope:
  31,785 persons**, counted in the manifest, asserted by a test. No other source has
  passengers; a one-source fact is a silver table under another name.
- **Dimension attributes from the primary record only.** A Montgomery crash matched to
  FARS could borrow FARS's `FUNC_SYS` to replace `STATE_HIGHWAY_UNCLASSIFIED`; it does
  not, because the fact's attributes should describe one record. Left as a Phase 4/7
  enrichment (208 crashes would benefit).
- **`dim_time.is_night` is a fixed clock rule** (before 06:00 or from 20:00), named as a
  placeholder for Phase 4's coordinate-derived sun times.
- **No county names beyond the Census list** and no tract/block-group members yet.
  `dim_geography` has every county in the three states so Phase 4 has parents.

---

## Bugs the tests caught (and one the profiler caught)

- **`UNION ALL BY NAME` with unaliased NULL literals.** The first TxDOT and FARS
  attribute selects relied on positional union; DuckDB refused duplicate anonymous
  names. Every column is now aliased in every branch — which is also what makes the
  three branches readable side by side.
- **The disjointness "proof" was a 15-billion-row self-join.** `er_local a JOIN er_local
  b ON same jurisdiction` over 125,005 Maryland rows: 74 seconds to answer a question
  about three two-letter codes. Aggregating to `DISTINCT (source_system, jurisdiction)`
  first made it instant. The rest of resolution runs in 1.7 s; the whole gold build in
  4.6 s.
- **FARS coverage as a date range made a single-record unit test lie.** With one FARS
  record on 2019-03-01, a local crash on 2019-03-03 was "outside coverage". FARS is
  published by calendar year, so coverage is now a year range — which is also the
  honest statement for the 188 Montgomery fatal crashes from 2015–2018 and 2025–2026.
- **The provenance hash moves for every row of a restated table, by design, and the
  first restatement test did not expect it.** `_silver_build_sha` is the sha256 of the
  silver *table*; a TxDOT amendment re-hashes `txdot/crash_current`, so all 157 fixture
  TxDOT fact rows changed in that one column. The test now excludes it from the
  row-diff (exactly one row changes) and asserts separately that the sha moved for
  TxDOT rows and for no other source.
- **The `__NULL__` crosswalk row is not lossy.** A test asserting every TxDOT crosswalk
  row is flagged lossy tripped on the null-value row, whose note is empty. Correct
  behaviour; the test now excludes it.
- **A test-only `IN (1,)` would have been a SQL syntax error.** Caught before it ran,
  by reading; the per-type tuples are now rendered explicitly.

---

## Verification

### Row-count reconciliation, silver → gold

| step | rows |
|---|---|
| `silver.crash` current | 447,700 |
| excluded by declared scope (FARS, 47 states) | −178,050 |
| **scoped** | **269,650** |
| `bridge_crash_source` rows / distinct `crash_uid` | 269,650 / 269,650 |
| bridge primaries = `fact_crash` rows | 268,493 |
| matched FARS records (bridge non-primaries) | 1,157 |
| `fact_crash` with `source_count > 1` | 1,157 |
| `fact_crash` with `severity_grain = RESOLVED_MAX` | 16 |
| `fact_crash` by primary: Montgomery / TxDOT / FARS | 125,005 / 100,000 / 43,488 |
| `fact_driver` (Montgomery / FARS) | 290,177 (220,043 / 70,134) |
| `fact_non_motorist` | 20,141 |
| FARS persons out of party scope (PER_TYP 2/3/4/9/10) | 31,036 / 588 / 29 / 105 / 27 |
| `geography_sk` unknown / state-level | 0 / 0 |
| `time_sk = -1` | 7 |

Montgomery `driver_count` equals Phase 2's `driver_row_count` on every row; the 785
driverless crashes are rows with `driver_count = 0`. FARS person-derived `fatal_count`
equals the accident file's `FATALS` on every real row (the fixture's synthetic +1
revision is the one deliberate exception, excluded by name in the test).

### Byte identity

Two consecutive `python -m src.transform.model` runs, all 15 files:

| table | rows | sha256 (first 16) |
|---|---|---|
| fact_crash | 268,493 | `01a105d949376d8f` |
| bridge_crash_source | 269,650 | `744bb6ce296d0366` |
| fact_driver | 290,177 | `ba5c66fb88bd84e8` |
| fact_non_motorist | 20,141 | `e419d29c1096d206` |
| dim_date | 4,263 | `33a0d645ddb97f5a` |
| dim_time | 1,441 | `355db200e851cd55` |
| dim_geography | 349 | `6bf59c1e2c3b2be3` |
| dim_road_class / weather / nm_type / severity | 13 / 14 / 6 / 6 | `fb7f955b8c00…` `696190bb197c…` `587b89a18b81…` `a869413e129e…` |
| map_severity / road_class / weather / nm_type | 29 / 41 / 50 / 28 | `8bbf69c788c1…` `d08fe7f8b0ca…` `227d76068597…` `791351ea5c22…` |

Identical on both runs, and identical again after the resolve refactors (the census is
in the manifest, not the parquet). `test_gold_is_byte_identical_across_two_builds` asserts
it on the fixture on every run.

### Restatement

- TxDOT amendment (fixture `17490094`): exactly one `fact_crash` row differs (excluding
  the table-level provenance sha), `crash_sk` unchanged, `is_amended` false→true,
  `silver_version_no` 1→2, dims/bridge/party facts byte-identical.
- FARS reissue removing a *matched* 2019 Montgomery County accident (a third partition
  synthesised inside the test): the Montgomery `crash_sk` unchanged, `in_fars` true→false,
  `source_count` 2→1, severity reverts to the primary's own value, the FARS row leaves
  the bridge, and the FARS key never existed as a fact row in either build.

### Tests

| suite | fixture | full corpus |
|---|---|---|
| before Phase 3 | 127 passed, 4 xfailed | 125 passed, 2 skipped, 4 xfailed |
| after (same tests on the extended extract) | 127 passed, 4 xfailed | — |
| after, with `test_model.py` (39) + `test_resolve.py` (16) | **182 passed, 4 xfailed, 43 s** | **177 passed, 5 skipped, 4 xfailed, 4m09s** |

The five full-corpus skips are the fixture-only restatement scenarios and the
`known_pairs` test, each skipping with a reason.

---

## The entity-resolution census

`python -m src.transform.resolve --report`, full local silver:

```
FARS in scope 44,645 · local crashes 225,005 (1,401 fatal candidates) · blocked pairs 176,232
matched 1,157 in 2 passes · unmatched FARS 43,488 · unmatched local fatal 244
midnight-straddle matches: 0

jur  year   FARS  FARS in covered counties  local fatal    A    B    C  matched
MD   2019    496                        36           32   29    0    0       29
MD   2020    546                        44           40   39    0    0       39
MD   2021    524                        40           33   31    0    0       31
MD   2022    534                        46           41   40    0    0       40
MD   2023    577                        45           34   34    0    0       34
MD   2024    552                        46           36   35    0    0       35
TX   2020   3522                      3522          204  175   11    1      187
TX   2021   4070                      4069          176  165    5    0      170
TX   2022   3966                      3966          193  176   10    1      187
TX   2023   3877                      3877          215  194   11    0      205
TX   2024   3774                      3774          209  193    6    1      200
FL   2019-24 18,911 — no local source; MD 2015-18/2025-26: 188 local fatal, FARS not loaded
```

**Match rate, honestly stated.** Maryland: 208 of 257 FARS Montgomery County fatalities
(80.9%) match a county-feed crash; 96.3% of the county's own fatal-typed crashes in
FARS years (208 of 216) match FARS. Texas: 949 of 997 TxDOT fatal crashes in the slice
(95.2%) match FARS; the FARS side is bounded by the slice (100k of 3.09M rows ≈ 3.2%),
and 949 of 22,505 FARS Texas records (4.2%) is consistent with that.

**Unmatched, by reason**

| side | reason | n | reading |
|---|---|---|---|
| FARS MD | `NO_LOCAL_ROWS_IN_COUNTY` | 2,972 | other Maryland counties — no source |
| FARS MD | `NO_FATAL_CANDIDATE_ON_DATE` | 31 | county feed has a crash that day, none fatal-typed |
| FARS MD | `CANDIDATE_TOO_FAR` | 18 | fatal candidate exists, > 1 km / > 2 h |
| FARS TX | `NO_FATAL_CANDIDATE_ON_DATE` / `NO_CANDIDATE_ON_DATE` | 11,156 / 9,406 | the slice does not contain the crash |
| FARS TX | `CANDIDATE_TOO_FAR` | 991 | |
| FARS TX | `CANDIDATE_TAKEN` | 2 | two FARS records, one TxDOT crash within thresholds |
| FARS FL | `OUTSIDE_LOCAL_COVERAGE` | 18,911 | |
| Montgomery fatal | `OUTSIDE_FARS_COVERAGE` | 188 | 2015–2018, 2025–2026 |
| Montgomery fatal | `NO_FARS_ON_DATE` / `FARS_TOO_FAR` | 5 / 3 | |
| TxDOT fatal | `FARS_TOO_FAR` / `NO_FARS_ON_DATE` | 25 / 23 | |

**Match quality.** Tier A (1,111): median distance 8.5 m, p95 117 m, max 243 m; median
Δt 0, max 10 min. Tier B (43, all Texas, all Δt = 0): median 465 m, max 944 m — same
minute, several hundred metres apart, consistent with a geocode placed at the wrong
end of a segment. Tier C (3): TxDOT `MISSING` coordinates, matched on county + date +
minute. Montgomery matches by fatal signal: 192 both signals, **16 report-type only**.

**30-day-death probe:** 2 Maryland and 22 Texas FARS records have a tier-A-quality
non-fatal neighbour; 21 are otherwise unmatched. Reported, not admitted.

**Disjointness:** `{MONTGOMERY_MD: [MD], TXDOT_CRIS: [TX]}`, zero source pairs share a
jurisdiction. A column-level proof, asserted by a test.

---

## For DATA_QUALITY.md

- **Agency coverage of the Montgomery feed.** 49 of 257 FARS Montgomery County
  fatalities (19%) have no fatal counterpart in the county feed; the feed lists no
  Maryland State Police reports. Detection: `resolve --report`, reasons
  `NO_FATAL_CANDIDATE_ON_DATE` + `CANDIDATE_TOO_FAR`. Disposition: FARS rows stay their
  own `fact_crash` rows; the memo states the feed is county-police, not county.
- **Montgomery fatal-signal disagreement.** 31 `Fatal Crash` reports with party max
  1–4, one `Injury Crash` with a K party. Detection: `acrs_report_type` vs
  `severity_ordinal` on `montgomery/crash_current`. Disposition: both signals feed the
  fatal block; 16 became `RESOLVED_MAX` with FARS confirmation; `severity_ordinal_primary`
  preserves the county's value.
- **30-day deaths.** 24 FARS fatalities adjacent (≤250 m, ≤30 min) to a local non-fatal
  crash. Detection: `nonfatal_tier_a_pairs` in the census. Disposition: reported, not
  admitted; config flag documents the alternative.
- **TxDOT `road_cls_id`, `wthr_cond_id` undecoded.** 100,000 crashes on UNKNOWN in two
  dims because the CRIS guide is unreachable and nothing else is citable. Detection:
  `reconciliation.road_class_by_code`/`weather_by_code`. Disposition: `is_lossy` rows in
  the crosswalk; test forbids any decoded TxDOT value; one CSV edit to fix.
- **Lossy crosswalks.** Road class: Montgomery ownership → two `_UNCLASSIFIED` members
  (102,585 crashes); FARS 98/99 → UNKNOWN. Weather: Montgomery `FOGGY` collapses
  fog/smog/smoke; `WINTRY MIX` has no FARS/TxDOT equivalent (kept as its own member, 252);
  the old generation had no freezing-rain code. Non-motorist: e-bike vs pedal bike
  collapsed; the new generation's "Other Pedestrian (person in a building, skater, …)"
  bundles FARS 8 and 10. Every lossy row is one CSV line with a `notes` sentence and
  `is_lossy = true`.
- **TxDOT has no party grain.** `fact_driver`/`fact_non_motorist` have zero TxDOT rows;
  `fact_crash.count_source = 'CRIS_COUNTS'` marks the crash-level injury counts.
- **FARS passengers out of party scope.** 31,785 persons (PER_TYP 2/3/4/9/10) counted in
  the manifest, in no fact.

## For DECISIONS.md

Each with the rejected alternative.

- **Gold is a pure function of silver's current slices; history lives in silver.**
  Rejected: SCD2 in gold (two version histories that can disagree); a
  `fact_crash_history` table (the silver join answers it). Gold carries the pointer
  (`silver_version_no`, `silver_valid_from`, `is_amended`, `_silver_build_sha`) so a
  restatement is visible and an 18-month-old decision walks back to exact bytes.
- **Surrogate key = 60-bit SHA-256 of the primary `crash_uid`, precedence Montgomery >
  TxDOT > FARS.** Rejected: `row_number()` (renumbers on any insertion); UUIDs (break
  byte-identity); FARS as primary (its key would vanish when the local record arrives,
  and amendments arrive on the local record).
- **One row per RESOLVED crash, with a bridge.** Rejected: one row per source record
  with a `same_as` column (violates "exactly one row per crash"); collapsing without a
  bridge (loses the FARS record's identity and the match evidence).
- **Resolution scoped to FARS ∩ {Montgomery, TxDOT} by a column-level disjointness
  proof.** Rejected: all-pairs resolution (Montgomery and TxDOT share no jurisdiction;
  measuring it is a `DISTINCT` on two columns); asserting FARS is "a different universe"
  (it is the same crashes, and 1,157 of them are shown to be).
- **Two-number scoring (geodesic metres, wall-clock minutes) with tiers, not a weighted
  score.** Rejected: a learned or weighted similarity (nothing to defend it with; the
  observed separation is three orders of magnitude — median match 8.5 m, median
  non-match > 1 km); string matching on street names (three vocabularies, no gain).
- **Fatal block on the UNION of Montgomery's two signals.** Rejected: ordinal only
  (misses 16 real matches); report type only (misses the one Injury Crash with a K
  party).
- **30-day-death pairs reported, not admitted.** Rejected: admitting them (raises a
  primary record's severity on proximity alone); ignoring them (the memo would
  understate FARS-vs-local disagreement). The config flag records both options.
- **Blocking on county, not on a spatial index.** Rejected: H3 cell blocking (Phase 4
  has H3; county is what all three sources publish today and it leaves 176k pairs, which
  is nothing).
- **Geodesic distance on the ellipsoid via pyproj, behind an equirectangular
  pre-filter.** Rejected: EPSG:3857 (29% scale error at 39°N — a match/miss difference at
  250 m); a per-state projected CRS (two CRSs for one function); haversine (spherical,
  and the pipeline already has one geodesy routine).
- **Ownership-based bridging members in `dim_road_class`.** Rejected: forcing
  Montgomery's route system into FHWA classes (invents data); UNKNOWN (discards what the
  source says).
- **TxDOT codes UNKNOWN rather than inferred from frequency.** Rejected: "11 is
  obviously Clear" (a guess in a crosswalk that carries decisions).
- **Scope as data (`config/model.toml`), exclusions counted per state.** Rejected:
  hard-coding `('MD','TX','FL')` in SQL; loading all 50 states into gold (176k rows of
  facts nothing downstream can use, and the scoring phase would have to re-scope).
- **Passengers out of party scope, counted.** Rejected: `fact_passenger` (one-source
  fact); folding into `fact_driver` (wrong grain, double-counts in per-vehicle rollups).
- **Calendar-year FARS coverage.** Rejected: min/max date (a one-record unit test showed
  it lies; NHTSA publishes by year).

## For MEMO.md

The pipeline knows about 269,650 crashes in Maryland, Texas and Florida across 2015–2026,
resolved to 268,493 distinct events. 1,157 events appear in two sources — every one a
fatal crash confirmed by both a state or county feed and the federal fatality census —
with a median position disagreement of 8.5 metres and a median time disagreement of zero
minutes. In Montgomery County, 81% of federally recorded fatalities match a county-police
crash record; the remaining 19% are on roads policed by the Maryland State Police, whose
reports the county feed does not carry. In Texas, 95% of fatal crashes in the sample
match the federal record. Florida has fatal-crash data only, from the federal census: no
all-severity Florida source is in scope, so no Florida non-fatal crash exists in the
model. Where the two sources disagree on severity, 16 crashes the county recorded as
injury crashes were fatal by federal count — and the model records both values.

## Open items for Phase 4

- **Geometry and GeoParquet.** `fact_crash` carries `latitude`/`longitude` DOUBLE in
  EPSG:4326 and 122,880-row groups. Phase 4 adds the geometry column and `bbox` covering.
- **`dim_geography` extension.** County members exist for all 349 counties in the three
  states; tract / block group / H3 hang off `county_geoid`. Decide: columns on a widened
  geography dim (one FK on the fact) vs. separate `dim_tract`, `dim_h3` (one FK each).
- **Timezone localisation** replaces `dim_time.is_night`'s clock rule and makes
  `crash_datetime_local` → UTC. Both sides of every accepted match are in one county, so
  Phase 3 never needed a zone.
- **Attribute enrichment across the bridge**: 208 Montgomery crashes could take FARS's
  `FUNC_SYS` where the county publishes ownership only. Cheap, but changes the meaning of
  a fact column; decide with Phase 7's scoring features in view.
- **TxDOT code decoding** once the CRIS guide is reachable: edit two CSVs, rebuild,
  100,000 crashes leave UNKNOWN. No code change.
- **TxDOT beyond the 100k slice.** The Texas match rate is bounded by the slice; a full
  sweep should recover ~95% of ~3,600 FARS Texas fatalities per year.
