# Phase 3 — Dimensional model + entity resolution: implementation brief

You are implementing Phase 3 of the Crash-to-Contact take-home in this repo. Phase 1
(bronze ingestion) and Phase 2 (bronze → silver, SCD2, contracts, drift detector,
severity crosswalk, thin `silver.crash` grain) are complete and committed. Your job is
silver → gold: a dimensional model with a crash fact at exactly one row per crash,
party facts at their own grain, six conformed dimensions, cross-source entity
resolution that is **measured rather than asserted**, and the tests and numbers that
let the memo defend all of it.

Read these before writing code, in this order:

1. `ASSIGNMENT.md` Part 2 (lines 120–133) — the six required things. Also Part 6
   (contracts at every boundary, idempotent backfill) and Part 3a (CRS rules: any
   metric operation in EPSG:3857 zeroes a section).
2. `ai docs/implementation/phase2-silver-report.md` — what silver actually contains,
   the eight places the data contradicted the brief, and the "Open items for Phase 3"
   section at the end. Treat its "For DECISIONS.md" section as the house style.
3. `src/transform/common.py` (module docstring, `SCD2_COLUMNS`, `write_parquet`,
   `BuildManifest`, `BuildContext`, `read_silver`, `GEOD`/`geodesic_distance_m`),
   `src/transform/unified.py` (the `silver.crash` grain you build on and its
   docstring explaining why ER was deliberately kept out of it), `src/transform/build.py`
   (the validate-then-write pattern), `src/transform/severity_crosswalk.py` and
   `config/severity_crosswalk.csv` (the seed-CSV-plus-loader pattern every new
   crosswalk must copy), `src/contracts.py` + `contracts/silver.schema.json`
   (the contract dialect, incl. `x-table-constraints`).
4. `tests/conftest.py`, `tests/test_known_defects.py` (esp.
   `test_crash_fact_grain_is_one_row_per_crash`, `test_pipeline_is_idempotent_under_restatement`
   and the `pipeline_runner` fixture) — the test style: real behaviour on committed
   bronze extracts, no mocks.
5. `contracts/lead_output.schema.json` — downstream: `source_system` enum,
   `severity_ordinal` 0–5, `jurisdiction` two-letter. Gold feeds this.

Environment: Python 3.14.7 venv at `.venv`; DuckDB 1.5.5, pyarrow 25, pandas 3.0.5,
pyproj 3.8, geopandas 1.1.4, shapely 2.1.2, jsonschema 4.26, pytest 9. Full local
silver exists under `data/silver/` (gitignored): `crash_current.parquet` 447,700 rows
(Montgomery 125,005 · TxDOT 100,000 bounded slice · FARS 222,695 national 2019–2024),
plus per-source `*_current` / `*_history` tables and `fars/codebook.parquet`. Run
`.venv/bin/python -m pytest -q` first and confirm the starting state (Phase 2 left it
at 127 passed, 4 xfailed). Rebuild silver only if you need to: `python -m
src.transform.build` (~26 s).

---

## 1. Scope

**In scope (build fully):**

- `src/transform/model.py` — the gold builder and CLI:
  `python -m src.transform.model [--silver-root …] [--gold-root …] [--jurisdiction MD --jurisdiction TX …] [--json]`.
  Reads silver current slices, builds every gold table in memory, validates against
  `contracts/gold.schema.json`, then writes `data/gold/*.parquet` + `_build_manifest.json`
  through `common.write_parquet` (validate-then-write, `.part` + durable rename, total
  sort order — exactly as `build.py` does).
- `src/transform/resolve.py` — cross-source entity resolution: blocking, scoring,
  one-to-one assignment, the bridge table, and a `--report` CLI that prints the match
  census for the memo.
- Conformed dimensions and their seed crosswalks: `config/road_class_crosswalk.csv`,
  `config/weather_crosswalk.csv` (same shape and loader pattern as
  `severity_crosswalk.csv`), `dim_severity` materialised from the existing severity
  crosswalk.
- `contracts/gold.schema.json` + wiring in `src/contracts.py` if anything is missing
  (FK resolution across gold tables should already work through `resolve`).
- `GOLD_DIR` in `src/config.py`; a `[model]` block in `config/sources.toml` or a new
  `config/model.toml` holding `jurisdictions = ["MD","TX","FL"]` and the ER thresholds
  (they are data, not code).
- `tests/test_model.py` and `tests/test_resolve.py`, plus any conftest fixtures
  (gold built into `tmp_path` from the committed bronze extract, like silver is).
- `ai docs/implementation/phase3-model-report.md` in the same shape as the Phase 2
  report.

**Out of scope (do not build; leave hooks):** timezone localisation, tract/BG
point-in-polygon, H3, road snapping, GeoParquet geometry/`bbox` column (Phase 4 — gold
keeps `latitude`/`longitude` DOUBLE in EPSG:4326 and the 122,880 row groups); ERA5
weather join (Phase 4; `dim_weather_condition` is the *officer-reported* condition);
spatial analysis (Phase 5); anything compliance, scoring, orchestration. CRSS is never
touched. Do not change `src/ingest/`, and change Phase 2 transform modules only for a
genuinely missing read-only helper (say so in the report).

---

## 2. What is already measured (verify, don't rediscover)

Measured on the local silver on 2026-09-07. Confirm in your report; where your build
changes a number, report both.

**Coverage and overlap universe**

| source | crash_date range | crashes | severity 5 |
|---|---|---|---|
| MONTGOMERY_MD | 2015-01-01 → 2026-09-02 | 125,005 | 373 (`MAX_OVER_PARTIES`) |
| TXDOT_CRIS (100k OID slice) | 2020-01-01 → 2024-12-31 | 100,000 | 997 |
| NHTSA_FARS (national) | 2019-01-01 → 2024-12-31 | 222,695 | 222,695 |

- FARS Montgomery County MD (`state_fips='24'`, `county_fips='031'`): 36 / 44 / 40 /
  46 / 45 / 46 accidents for 2019–2024. Montgomery's own fatal crashes (ordinal 5) for
  the same years: 29 / 39 / 29 / 38 / 31 / 34. **FARS has more fatal crashes in the
  county than the county feed does.** Find out why before you tune thresholds:
  candidates are agency coverage (Montgomery's `agency_name` includes state police
  and municipal departments — check whether the missing ones are on interstates),
  FARS's 30-day death rule vs Montgomery's at-scene severity, and coordinate
  quarantine (`geo_quality != 'OK'`). Report the decomposition.
- Montgomery's `acrs_report_type = 'Fatal Crash'` disagrees with the party max: 372
  crashes are Fatal Crash + ordinal 5, but **31 are Fatal Crash with ordinal 1–4**
  (12 at 4, 10 at 3, 7 at 2, 2 at 1) and one Injury Crash carries ordinal 5. A "fatal"
  block that keys on the ordinal alone misses 31 candidates. Block on
  `acrs_report_type = 'Fatal Crash' OR severity_ordinal = 5`, and report which side
  each match came from.
- FARS Texas: 3,296 / 3,522 / 4,070 / 3,966 / 3,877 / 3,774 accidents for 2019–2024.
  TxDOT fatal crashes in the slice: 204 / 176 / 193 / 215 / 209 for 2020–2024. The
  slice is OIDs 1–100,000 of ~3.09M, so the *expected* recoverable FARS∩TxDOT match
  set is bounded by the 997 TxDOT fatals, not by FARS. Report observed vs. expected.
- Montgomery ↔ TxDOT are disjoint by jurisdiction (`MD` vs `TX`). One sentence and one
  zero-row assertion; nothing more.
- FARS MD accidents have **zero** null `crash_datetime_local` (3,229 rows), so time is
  usable in MD matching; check the TX null rate before assuming the same.
- `county_fips` is populated for TxDOT (verified FIPS = 2·cnty_id − 1 in Phase 2:
  Harris `201` 20,139 rows, Bexar `029`, Dallas `113`) and FARS (`031` etc.).
  Montgomery is always `24031` by construction — silver carries no county column for
  it; the dimension supplies it.

**Source vocabularies the dimensions must conform**

- Montgomery `route_type` (20 values incl. NULL 16,640): `Maryland (State)`,
  `Maryland (State) Route`, `County`, `County Route`, `Municipality`,
  `Municipality Route`, `US (State)`, `Interstate (State)`, `Ramp`, `Local Route`,
  `Government`, `Government Route`, `Other Public Roadway`, `Bicycle Route`, `Spur`,
  `Private Route`, `Crossover`, `Service Road`, `Unknown`. This is an **ownership /
  route-system** taxonomy, not FHWA functional class — the crosswalk to a functional
  class is lossy and must say so (a "Maryland (State)" route may be an arterial or a
  collector).
- Montgomery `weather` (24 values across two dictionary generations): `CLEAR`/`Clear`,
  `RAINING`/`Rain`, `CLOUDY`/`Cloudy`, `SNOW`/`Snow`, `FOGGY`/`Fog, Smog, Smoke`,
  `WINTRY MIX`, `SLEET`/`Sleet Or Hail`, `SEVERE WINDS`/`Severe Crosswinds`,
  `BLOWING SNOW`/`Blowing Snow`, `Freezing Rain Or Freezing Drizzle`,
  `BLOWING SAND, SOIL, DIRT`/`Blowing Sand, Soil, Dirt`, `OTHER`, `N/A`, `UNKNOWN`/`Unknown`.
  Two generations again — the crosswalk is per value, never per date, and the drift
  detector in `dictionaries.py` must cover these columns (register their vocabularies).
- FARS labels are already in `silver/fars/codebook.parquet` (`tbl, col, code, label,
  first_year, last_year`): `WEATHER` 13 codes (1 Clear … 12 Freezing Rain, 98 Not
  Reported, 99 Unknown), `FUNC_SYS` 1–7 + 96/98/99 (label wording changed between
  years — same code, two spellings; conform on code), `PER_TYP` 17 labels (1 driver, 2
  passenger, 5 pedestrian, 6 bicyclist, 7 other cyclist, 8/11/12/13 personal
  conveyance, 19 unknown non-motorist), `LGT_COND`, `RUR_URB`, `ROUTE`, `STATE`,
  `HARM_EV`, `MAN_COLL`. **No `COUNTY` labels** — county names are not in the codebook.
- TxDOT `road_cls_id` ∈ {1–9}, `wthr_cond_id` ∈ {0,2,3,4,5,6,7,8,11,12} (11 = 78,336
  rows, 12 = 12,627 — almost certainly Clear/Cloudy but **do not guess**),
  `light_cond_id`, `surf_cond_id`, `traffic_cntl_id` are opaque integers. The only
  citable lookup is the CRIS Automated Interface guide V29.0
  (`https://www.txdot.gov/content/dam/docs/division/trf/crash-records/cris-guide.pdf`).
  Decode a code only with a citation to that document (page/table) in the crosswalk
  `notes`; otherwise the conformed value is `UNKNOWN` with the raw id preserved and the
  count reported. An undecoded row is honest; a guessed row is a data-quality defect.

---

## 3. Architecture decisions (made — do not relitigate; record the why in docstrings)

**Gold is a pure function of silver's current slices.** History already lives in
silver's SCD2 tables (one mechanism for TxDOT amendments and FARS reissues — Phase 2
decision). Gold is rebuilt whole, deterministically, and must be byte-identical across
two builds over the same silver. It carries `silver_version_no`, `silver_valid_from`
and `is_amended` on the facts so a restatement is *visible* in gold without gold
keeping its own history. Rejected: SCD2 in gold as well (two version histories of the
same event that can disagree), and a `fact_crash_history` table (a join to silver
history answers every question it would; write that join as a documented query in the
report instead).

**Engine.** DuckDB SQL in-process, same as silver. Python only for the ER scoring /
assignment step if DuckDB cannot express it cleanly (try SQL first: blocking +
geodesic distance + window-function ranking is all SQL), and for the crosswalk
loaders. No pandas round-trips on full tables. Spark stays the named, unused escape
hatch.

**Star, not snowflake.** Tables and grains:

| table | grain | key |
|---|---|---|
| `fact_crash` | one row per **resolved crash** | `crash_sk` |
| `bridge_crash_source` | one row per (resolved crash, contributing source record) | `(crash_sk, crash_uid)` |
| `fact_driver` | one row per driver | `driver_sk` |
| `fact_non_motorist` | one row per non-motorist | `non_motorist_sk` |
| `dim_date` | one row per calendar day covering every fact date | `date_sk` (yyyymmdd int) |
| `dim_time` | one row per minute of day + one unknown member | `time_sk` (hhmm int; −1 unknown) |
| `dim_geography` | one row per county in scope + state-level members for out-of-county | `geography_sk` |
| `dim_road_class` | one row per conformed road class | `road_class_sk` |
| `dim_weather_condition` | one row per conformed condition | `weather_condition_sk` |
| `dim_severity` | six rows, ordinal 0–5 | `severity_sk` = ordinal |
| `map_severity_source`, `map_road_class_source`, `map_weather_source` | the crosswalks as data | `(source_system, source_column, source_value)` |

**Surrogate keys are deterministic and stable under restatement.** `crash_sk` is a
64-bit integer derived from a stable hash (e.g. first 8 bytes of SHA-256, or DuckDB
`hash()` — pick one, document it, never `row_number()` and never random) of the
**primary** source record's `crash_uid`. The primary source of a resolved crash is
chosen by fixed precedence `MONTGOMERY_MD > TXDOT_CRIS > NHTSA_FARS` (the operational
all-severity record over the fatality census, because the operational record is the
one an amendment arrives on). Consequences you must implement and test: (a) a
Montgomery crash keeps its `crash_sk` whether or not a FARS match is found; (b) when a
match *is* found, the FARS record's own `crash_sk` disappears from `fact_crash` and
the FARS `crash_uid` appears in the bridge under the Montgomery `crash_sk`; (c) an
amended TxDOT row changes attributes, never the key. Party surrogates hash the party
natural key (`MONTGOMERY_MD:` + `person_id`; `NHTSA_FARS:` + `year-st_case-veh_no-per_no`).
Dimension surrogates for the small dims are stable small integers assigned in the seed
CSV or by a fixed ordering — not by insertion order.

**Documented natural key per source** (already enforced in silver; restate in the
gold contract description): Montgomery `report_number`; TxDOT `crash_id`; FARS
`(year, st_case)`. The resolved crash's natural key is the primary record's
`(source_system, source_record_id)`; the bridge carries every contributing one.

**Scope is data.** Gold covers jurisdictions listed in config (`MD`, `TX`, `FL`).
FARS rows outside them are excluded from the facts, and the excluded count per
state is in the manifest and the report — the crash count changes here **by
declared scope, visibly**, never silently. FL has no all-severity source, so FL gold
crashes are FARS-only; say so.

**Severity of a resolved crash** = max ordinal across its linked source records, with
`severity_grain = 'RESOLVED_MAX'` when sources disagree and the single source's grain
otherwise. Keep `severity_ordinal_primary` (the primary source's own value) beside it
so the lossiness is measurable: a Montgomery "suspected serious" matched to a FARS
fatality is the 30-day-death case, and that count is a `DATA_QUALITY.md` entry
(see §6). `severity_ordinal` stays 0–5 to satisfy the lead output contract.

**TxDOT has no party rows.** The CRIS crash layer publishes injury *counts*
(`death_cnt`, `sus_serious_injry_cnt`, …) and involvement flags, not persons.
`fact_crash` carries those counts as measures for every source (Montgomery and FARS
derive them from their party tables, and the derived counts are cross-checked against
FARS `fatals`/`persons` in the report). `fact_driver` / `fact_non_motorist` therefore
have zero TxDOT rows, by construction, and the contract says so.

**FARS persons split by `PER_TYP`.** 1 → `fact_driver`; 5, 6, 7, 8, 11, 12, 13, 19 →
`fact_non_motorist`; 2, 3, 4, 9, 10 (passengers, occupants of not-in-transport
vehicles, in-building) → **neither** fact, counted in the manifest as
`fars_persons_out_of_party_scope`. Rejected: a third `fact_passenger` (no other source
has passengers; a one-source fact is a silver table with a different name) and folding
passengers into `fact_driver` (wrong grain, and it would double-count drivers in any
per-vehicle roll-up).

**Unknown members, not NULL foreign keys.** Every dim has an explicit unknown member
(`-1` / `UNKNOWN`) and every fact FK is NOT NULL. FARS `HOUR=99` → `time_sk = -1`, a
quarantined coordinate → still the county from the source's own county field where one
exists, else the state-level member. The contract enforces FK integrity with zero
orphans, no `orphans_allowed_when`.

**CRS.** Every distance in ER is geodesic on WGS84 via `common.GEOD` /
`geodesic_distance_m` **or** planar after reprojecting to the state CRS from
`config/sources.toml [crs]` (`26985` for MD, `32139` or `3083` for TX). Never 3857.
Comment the CRS at every metric operation, as Phase 2 did.

---

## 4. Facts — required content

**`fact_crash`** (one row per resolved crash): `crash_sk`, `primary_crash_uid`,
`primary_source_system`, `natural_key` (primary), `jurisdiction`, `date_sk`, `time_sk`,
`crash_datetime_local` (naive, as silver), `geography_sk`, `road_class_sk`,
`weather_condition_sk`, `severity_sk`, `severity_ordinal`, `severity_ordinal_primary`,
`severity_grain`, `latitude`, `longitude`, `geo_quality`, `source_count` (1–3),
`in_fars`, `in_txdot`, `in_montgomery` (booleans), measures: `driver_count`,
`non_motorist_count`, `vehicle_count`, `fatal_count`, `serious_injury_count`,
`minor_injury_count`, `possible_injury_count` (from party ordinals for MD/FARS, from
CRIS counts for TX; NULL where a source cannot say, with a `count_source` column),
context flags available in all sources or NULL: `hit_run`, `pedestrian_involved`,
`bicyclist_involved`, `work_zone`, `intersection_related`, `alcohol_suspected`,
`drug_suspected`; lineage: `is_amended`, `silver_version_no`, `silver_valid_from`,
`_silver_build_sha` (the primary silver table's sha256 from the silver manifest —
the pointer that makes an 18-month-later reconstruction possible).

**`bridge_crash_source`**: `crash_sk`, `crash_uid`, `source_system`,
`source_record_id`, `is_primary`, `match_method` (`SINGLE_SOURCE` |
`FATAL_DATE_GEO_TIME` | `FATAL_DATE_COUNTY_TIME` for sentinel-coordinate FARS rows |
…), `match_score`, `distance_m`, `time_delta_min`, `date_delta_days`. Every
`silver.crash` `crash_uid` in scope appears here **exactly once** — that is the
reconciliation test between silver and gold.

**`fact_driver`**: `driver_sk`, `crash_sk`, `source_system`, `party_natural_key`,
`date_sk`, `severity_sk`, `severity_ordinal`, `severity_kabco`, `severity_note`,
`age` (FARS; NULL for Montgomery, which publishes none — say so), `alcohol_status`,
`drug_status` (Montgomery's normalised statuses; FARS from `drinking`/`drugs` mapped
into the same four-value vocabulary with the mapping in a seed CSV), `at_fault`,
`vehicle_year`, `vehicle_make`, `vehicle_model`, `vehicle_body_type`, `speed_limit`,
`licence_state`, lineage columns as above. `crash_sk` is the *resolved* crash — a FARS
driver on a matched Montgomery crash points at the Montgomery `crash_sk`.

**`fact_non_motorist`**: same skeleton; `non_motorist_type` conformed
(`PEDESTRIAN`, `BICYCLIST`, `OTHER_CYCLIST`, `PERSONAL_CONVEYANCE`, `OTHER`, `UNKNOWN`)
from Montgomery `pedestrian_type` and FARS `per_typ`, plus the Montgomery-only
descriptors carried as-is.

Party-to-crash integrity: every party row's `crash_sk` exists in `fact_crash`;
Montgomery's 785 driverless crashes have `driver_count = 0`, not a missing row; the
Phase 2 `has_driver_rows` / `has_non_motorist_rows` flags reconcile exactly against
the gold counts (write that as a test).

---

## 5. Dimensions — required content

- **`dim_date`**: generate from `min(crash_date)` to `max(crash_date)` across the
  facts; `date_sk`, `date`, `year`, `quarter`, `month`, `day`, `day_of_week`
  (ISO), `is_weekend`, `iso_week`, `day_name`, `month_name`. No holidays (would need a
  jurisdiction-specific calendar; note it as a Phase 7 scoring hook).
- **`dim_time`**: `time_sk` (hhmm), `hour`, `minute`, `hour_bucket`
  (`00-05`, `06-09`, `10-15`, `16-19`, `20-23`), `is_night` (a fixed clock rule —
  say it is a placeholder until Phase 4 gives real sun times), plus the −1 unknown
  member. Local wall clock: this is pre-localisation by design; the docstring says so.
- **`dim_geography`**: `geography_sk`, `level` (`COUNTY` | `STATE`), `jurisdiction`
  (two-letter), `state_fips`, `county_fips` (3-digit, NULL at state level),
  `county_geoid` (5-digit), `county_name` — populate names from a committed seed
  `config/counties.csv` for the three states (Census county list; cite the source
  and vintage in the file header; ≤ 400 rows). Include every county that appears in
  the facts plus every county in the three states, so Phase 4's tract join has a
  parent to hang from. Phase 4 adds tract/BG/H3 as *separate* dims or as columns on
  an extended geography dim — leave a docstring note, do not pre-build them.
- **`dim_road_class`**: conformed vocabulary aligned to FHWA functional class since
  FARS already speaks it: `INTERSTATE`, `FREEWAY_EXPRESSWAY`, `PRINCIPAL_ARTERIAL`,
  `MINOR_ARTERIAL`, `MAJOR_COLLECTOR`, `MINOR_COLLECTOR`, `LOCAL`, `RAMP`,
  `PRIVATE_OR_NOT_IN_INVENTORY`, `OTHER`, `UNKNOWN`. `config/road_class_crosswalk.csv`
  maps every Montgomery `route_type` value, every FARS `func_sys` code and every TxDOT
  `road_cls_id` (cited or `UNKNOWN`) into it, with a `notes` column naming the
  lossiness (Montgomery ownership ≠ functional class; TxDOT undecoded). Also carry the
  raw source value on the fact? No — the fact carries the FK only; the raw value stays
  in silver and the map table is the audit trail.
- **`dim_weather_condition`**: `CLEAR`, `CLOUDY`, `RAIN`, `SNOW`, `SLEET_HAIL`,
  `FREEZING_RAIN`, `FOG_SMOKE`, `SEVERE_WIND`, `BLOWING_SNOW`, `BLOWING_SAND`,
  `WINTRY_MIX`, `OTHER`, `NOT_REPORTED`, `UNKNOWN`. Map both Montgomery generations,
  FARS `weather` codes (from the codebook labels), TxDOT `wthr_cond_id` (cited or
  `UNKNOWN`). Keep `NOT_REPORTED` (FARS 98, Montgomery `N/A`) distinct from `UNKNOWN`
  (FARS 99, Montgomery `UNKNOWN`) — same reasoning as severity 0 vs O.
- **`dim_severity`**: `severity_sk` = ordinal 0–5, `kabco`, `label`, `is_injury`,
  `is_fatal`, `description` with the lossiness sentences from the crosswalk notes.
  `map_severity_source` is the existing `config/severity_crosswalk.csv` materialised.

Every crosswalk loader must raise on an unmapped source value (drift), exactly like
`severity_crosswalk.assert_all_mapped`; `model.py` honours `--allow-unmapped` the
same way `build.py` does, writing `UNMAPPED` and a manifest warning.

---

## 6. Entity resolution — required behaviour

Scoped resolution per the overlap analysis: the only possible overlaps are
FARS ∩ Montgomery (MD fatal) and FARS ∩ TxDOT (TX fatal). Montgomery ∩ TxDOT is
disjoint by jurisdiction.

- **Blocking**: same `jurisdiction`; FARS `county_fips` = candidate's county (`031` for
  every Montgomery row; TxDOT `county_fips`); `|crash_date difference| ≤ 1 day` (FARS
  dates are local; midnight-adjacent crashes straddle days); the non-FARS side is a
  fatal candidate under the *union* rule from §2 (`acrs_report_type='Fatal Crash' OR
  severity_ordinal=5` for Montgomery; `crash_fatal_fl OR severity_ordinal=5 OR
  death_cnt>0` for TxDOT). Then, **separately and reported separately**, run the same
  match with the fatal block removed for Montgomery only, to measure the 30-day-death
  cases (FARS fatal, Montgomery at-scene non-fatal). Do not admit those into the
  bridge unless the geo+time evidence is as strong as the fatal-block matches; report
  how many there are either way.
- **Scoring**: geodesic distance (m) when both sides have `geo_quality='OK'`;
  `|Δt|` in minutes when both have a time; a composite score you define and document
  (e.g. distance ≤ 250 m and Δt ≤ 30 min → tier A; ≤ 1,000 m and ≤ 120 min → tier B;
  FARS sentinel coordinates → county+date+time only, tier C). Thresholds live in
  config. Justify them with the *observed* distance/time distributions of the tier-A
  matches (report the histogram) rather than by fiat.
- **Assignment**: one-to-one, deterministic — rank candidates per FARS record by
  (tier, distance, Δt, crash_uid) and per non-FARS record likewise, accept mutual best
  pairs, then repeat on the remainder or accept one pass and report the residual
  ambiguity. Ties are broken on `crash_uid` so two builds agree byte-for-byte.
- **Report** (`python -m src.transform.resolve --report`, pasted into the build
  report): per jurisdiction and year — FARS in-scope count, candidate non-FARS fatal
  count, matched count by tier, unmatched FARS with reason (`NO_CANDIDATE_ON_DATE`,
  `CANDIDATE_TOO_FAR`, `CANDIDATE_TAKEN`, `COORDS_QUARANTINED_BOTH_SIDES`),
  unmatched non-FARS fatals with reason, the 30-day-death count, and for FARS∩TxDOT
  observed vs expected given the 100k slice. This table is the assignment's "prove it
  rather than asserting it" and goes into the memo verbatim.
- **Determinism test**: run resolution twice → identical bridge; shuffle input row
  order → identical bridge.

---

## 7. Restatement and idempotency at the gold layer

Reuse the silver `pipeline_runner` fixture: extend it (or add a `gold_runner`) so a
run builds silver **and** gold into `tmp_path`. Tests:

1. Build twice over the same bronze → every gold parquet sha256 identical
   (`_build_manifest.json` excluded, as in silver).
2. Add the fixture's synthetic amended TxDOT partition (the manifest names
   `txdot_amended_crash_id = 17490094`) → in gold exactly one `fact_crash` row changes,
   its `crash_sk` is unchanged, `is_amended`/`silver_version_no` reflect the amendment,
   every other gold file is byte-identical.
3. Add the fixture's FARS 2019 reissue partition (`revised 100001`, `removed 100002`)
   → the revised case's fact row changes with a stable key, the removed case's row is
   gone from `fact_crash` and `bridge_crash_source`, everything else byte-identical.
4. Removing a FARS row that was **matched** to a Montgomery crash must leave the
   Montgomery `crash_sk` in place with `in_fars=false` and `source_count` decremented —
   if the committed extract has no such pair, create one (see §9) so this is a real test.

---

## 8. Contracts

`contracts/gold.schema.json`, same dialect and conventions as the silver contract
(`x-column-order-is-contract`, `required`, typed `properties`, `x-table-constraints`
with `unique_keys`, `foreign_keys`, `row_count_min`). Foreign keys: every fact FK →
its dim; `bridge.crash_sk` → `fact_crash`; party facts → `fact_crash`. No
`orphans_allowed_when` anywhere in gold — unknown members exist for exactly this
reason. `severity_ordinal` 0–5, `jurisdiction` `^[A-Z]{2}$`, `source_system` the
lead-output enum, `severity_grain` enum extended with `RESOLVED_MAX`. `model.py`
validates every table before the first write; a failure leaves the previous gold
untouched. Add a test that a relation violating an FK raises with the table and
column named.

---

## 9. Tests and fixtures

Fixtures build gold into `tmp_path` from the same committed bronze extract silver
uses (`tests/fixtures/bronze/`, `CRASH_TEST_FULL_BRONZE=1` switches to real data).
Check whether the extract contains at least one genuine FARS ∩ Montgomery pair (FARS
2019 `state_fips='24'`, `county_fips='031'` accidents and the Montgomery incidents on
those dates within a few hundred metres). It almost certainly does not — the FARS
sample was drawn nationally. Extend `tests/fixtures/make_extracts.py` to **add**
(never remove or re-sample — keep the existing seed and row set) the 2019 Montgomery
County FARS accidents with their vehicle/person rows and the Montgomery incident,
driver and non-motorist rows for the matching report numbers, regenerate the extract,
update `MANIFEST.json` (record the intended pairs under `synthetic` or a new
`known_matches` key), and confirm the whole existing suite still passes on the new
extract. These are public crash records with opaque party GUIDs — no PII. Keep the
extract small (aim ≤ +150 rows total).

`tests/test_model.py`:
- `fact_crash` unique on `crash_sk`; `bridge_crash_source` unique on `crash_uid`;
  every in-scope `silver.crash` `crash_uid` appears in the bridge exactly once;
  `fact_crash` row count = bridge primaries.
- The Drivers-derived negative control from Phase 2 restated at gold:
  `count(fact_driver)` / `count(fact_crash where in_montgomery)` ≈ 1.8.
- FK integrity for every FK (no orphans, no NULLs); unknown members present.
- `dim_date` covers every fact `date_sk`; FARS `HOUR=99` rows have `time_sk=-1` and a
  populated `date_sk`.
- Resolved severity ≥ every contributing source's ordinal; `severity_grain` is
  `RESOLVED_MAX` iff sources disagree.
- Party counts on `fact_crash` reconcile with Phase 2's `driver_row_count` /
  `non_motorist_row_count` and with FARS `fatals`/`persons` (report tolerances).
- Every distinct silver value of `route_type`, `weather`, `func_sys`, FARS `weather`,
  `road_cls_id`, `wthr_cond_id` maps (or is deliberately `UNKNOWN` with a note); an
  invented value raises.
- Scope exclusion: FARS rows with `jurisdiction` not in config are absent and their
  count is in the manifest.
- Surrogate stability: the `crash_sk` for a known Montgomery `report_number` equals a
  pinned constant (guards against anyone changing the hash function silently).

`tests/test_resolve.py`:
- Unit tests on small in-memory relations: 200 m / 10 min apart → tier A; 5 km → no
  match; two candidates → the closer one wins and the other is `CANDIDATE_TAKEN`;
  FARS sentinel coordinates → tier C by county+date+time; midnight straddle (23:50 vs
  00:10 next day) matches; a Montgomery non-fatal + FARS fatal pair is *reported*
  under the 30-day analysis and admitted/not per your documented rule.
- Determinism and order-independence (§6).
- Montgomery ∩ TxDOT candidate set is empty.
- The known pair(s) in the extended fixture resolve to one `crash_sk` with the
  Montgomery record primary.

Idempotency tests per §7.

---

## 10. Conventions

- Match Phases 1–2: module docstrings explain **why**; a comment at every CRS choice;
  tests exercise real behaviour; numbers in docstrings are measured, dated, and
  reproducible by a named command.
- Commit as you go: `feat(model): …`, `feat(resolve): …`, `feat(contracts): …`,
  `test(model): …`, `chore(config): …`. Commit only files you fill in this phase;
  other empty scaffold files stay untracked. Never commit anything under `data/`,
  `config/settings.toml`, `ai docs/`, or `IMPLEMENTATION_GUIDE.md` (the repo owner
  decides on disclosure of AI working notes in `AI_USE.md`).
- Do not modify `fixtures/synthetic_parties.csv`, `contracts/lead_output.schema.json`,
  `contracts/silver.schema.json` (except to add `RESOLVED_MAX` if you decide the enum
  belongs there too — say so), or `src/ingest/`.
- New dependencies: none expected. If one is unavoidable, add it to `requirements.txt`
  with a one-line reason in the report.
- Logs carry counts, never `person_id`/`report_number` values at INFO.
- When the spec, this brief and the data disagree, the data wins and the disagreement
  goes in the report.

---

## 11. Deliverable: the report

Write `ai docs/implementation/phase3-model-report.md` with: What I built (per module,
a paragraph each); Things the spec or this brief said that the data doesn't do; What
I bounded and why; Bugs the tests caught; Verification (row-count reconciliation
silver → gold per table incl. scope exclusions and bridge = silver; two-build byte
identity hashes; restatement test outcomes; test counts fixture vs full); **The
entity-resolution census** (the §6 table, plus the FARS-vs-Montgomery fatal-count
decomposition from §2 and the 30-day-death count); **For DATA_QUALITY.md** (lossy
crosswalks with counts per bucket; undecoded TxDOT codes; the 31 Fatal-Crash /
non-5-ordinal crashes; TxDOT has no party grain; FARS passengers out of party scope);
**For DECISIONS.md** (every decision in §3 and §6 with the rejected alternative, in
the Phase 2 report's style); **For MEMO.md** (three to five sentences a business
reader can use on how many crashes the pipeline knows about, how many appear in two
sources, and what the match rate means); Open items for Phase 4 (what gold still
lacks for tz/tract/H3/GeoParquet, and how `dim_geography` extends).

---

## 12. Definition of done

- `python -m src.transform.model` builds all of gold from local silver in one command,
  twice, with identical parquet hashes; the manifest records input silver hashes,
  output hashes, scope exclusions and ER stats.
- `fact_crash` is provably one row per resolved crash; every in-scope silver crash is
  in the bridge exactly once; every FK resolves; every crosswalk value maps or is a
  documented `UNKNOWN`.
- `python -m src.transform.resolve --report` prints the match census with reasons for
  every unmatched fatal on both sides, and the number is explained, not just stated.
- `pytest -q` green on the fixture and with `CRASH_TEST_FULL_BRONZE=1`; the existing
  127 tests still pass on the extended extract; gold idempotency and both restatement
  scenarios pass.
- `git log` shows small commits; `git status` shows no data, settings, or `ai docs/`
  files staged; the report is written.
