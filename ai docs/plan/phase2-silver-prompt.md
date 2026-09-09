# Phase 2 — Silver transforms + defect handling: implementation brief

You are implementing Phase 2 of the Crash-to-Contact take-home in this repo. Phase 1
(bronze ingestion) is complete and committed. Your job is bronze → silver: typed,
defect-handled, versioned per-source tables, the tests that prove the defects are
fixed, and the measured numbers for `DATA_QUALITY.md`.

Read these before writing code, in this order:

1. `ASSIGNMENT.md` Part 1 "Known defects", Part 2, Part 6 (contracts, tests, drift, idempotent backfill).
2. `ai docs/implementation/phase1-ingestion-report.md` — what bronze actually looks like and what was bounded.
3. `src/ingest/watermark.py` (bronze layout, `WatermarkStore.load_partitions/artifacts`, `row_sha256`, `durable_replace`) and the module docstrings of `src/ingest/montgomery.py`, `txdot.py`, `fars.py`.
4. `tests/test_known_defects.py` and `tests/test_idempotency.py` — the test style this repo uses (real behaviour, not mocks).
5. `contracts/lead_output.schema.json` — the downstream contract silver ultimately feeds (`source_system` enum, `severity_ordinal` 0–5, `jurisdiction` two-letter).

Environment: Python 3.14 venv at `.venv`, DuckDB 1.5.5, pyarrow 25, pandas 3, pyproj 3.8, pytest 9. Bronze data is present locally under `data/bronze/` (gitignored): Montgomery has two overlapping partitions per dataset, TxDOT has one 100,000-row bounded partition, FARS has one partition per year 2019–2024. Run `.venv/bin/python -m pytest -q` first: 8 pass, 4 xfail, 3 error (the three silver-fixture tests) is the starting state.

---

## 1. Scope

**In scope (build fully):**

- `src/transform/common.py` — shared silver infrastructure: bronze partition discovery, dedupe/SCD2 versioning helper, deterministic parquet writer, build manifest.
- `src/transform/montgomery.py`, `txdot.py`, `fars.py` — one transform per source, each producing versioned silver tables.
- `src/transform/severity_crosswalk.py` + seed CSV — the single 0–5 ordinal.
- `src/transform/dictionaries.py` — code-dictionary normalisation (the `driver_substance_abuse` grammar) and the value-set **drift detector**.
- `src/transform/build.py` — `python -m src.transform.build [--source ...] [--bronze-root ...] [--silver-root ...]` CLI that rebuilds silver from bronze deterministically.
- `src/transform/report.py` — `python -m src.transform.report` prints every defect's measured count from the full local bronze, for `DATA_QUALITY.md`.
- `src/contracts.py` + `contracts/bronze.schema.json` + `contracts/silver.schema.json` — enforced contracts at both boundaries (currently empty files).
- A thin unified crash-grain table `silver.crash` (one row per crash across all three sources) — enough for the grain test and for Phases 3–4 to build on.
- `tests/conftest.py` fixtures, all seven tests in `tests/test_known_defects.py` made real, plus the additional tests listed in §9.
- A build report at `ai docs/implementation/phase2-silver-report.md` in the same shape as the Phase 1 report.

**Out of scope (do not build; leave hooks):** conformed dimensions, party facts across sources, cross-source entity resolution, surrogate-key model (`src/transform/model.py` stays empty — Phase 3); timezone localisation, H3, census PIP, road snapping, GeoParquet geometry column (Phase 4); orchestration wiring (Phase 8); anything compliance. CRSS is never touched.

---

## 2. What is already measured in bronze (verify, don't rediscover)

These were measured on the local bronze on 2026-09-07 with union-by-name reads over all partitions. Confirm them in your report; where your dedupe changes a number, report both.

**Montgomery (`bhju-22kf` incidents, `mmzv-x632` drivers, `n7fk-dce5` non-motorists)**

- Two partitions per dataset overlap: incidents 132,190 rows / 125,005 distinct `:id` / 125,005 distinct `report_number` (1:1). Drivers 232,411 rows / 220,043 distinct `:id` / 220,043 distinct `person_id`. Silver must dedupe on the natural key, keeping the latest `:updated_at` then latest `_bronze_load_ts`.
- Drivers average 1.87 rows per `report_number` — the ~1.8× fan-out the assignment names.
- Anti-join on `report_number`: **785** incident report numbers have no driver row; **0** driver report numbers lack an incident. Investigate the 785 (non-motorist-only crashes? parked/driverless?) before deciding disposition.
- Rough envelope (lat 38.9–39.36, lon −77.54 to −76.87): **114** incident rows outside it, zero null and zero zero-valued coordinates. Examples land in Anne Arundel, Pennsylvania, and Virginia.
- `driver_substance_abuse` in Drivers has exactly 21 distinct values. Old scheme (UPPERCASE single value): `NONE DETECTED`, `ALCOHOL PRESENT`, `ALCOHOL CONTRIBUTED`, `ILLEGAL DRUG PRESENT`, `ILLEGAL DRUG CONTRIBUTED`, `MEDICATION PRESENT`, `MEDICATION CONTRIBUTED`, `COMBINED SUBSTANCE PRESENT`, `COMBINATION CONTRIBUTED`, `OTHER`, `UNKNOWN`, `N/A`. New scheme: `<alcohol token>, <drug token>` with alcohol ∈ {`Not Suspect of Alcohol Use`, `Suspect of Alcohol Use`, `Unknown`} and drug ∈ {`Not Suspect of Drug Use`, `Suspect of Drug Use`, `Unknown`}. Null spellings: SQL NULL, `N/A`, `UNKNOWN`, `Unknown, Unknown`.
- **Cutover overlap by crash date: first new-scheme row 2023-12-28, last old-scheme row 2024-01-03.** Both schemes coexist in that window (e.g. 2023-12-28 has 2 new + 39 old; 2024-01-03 has 37 new + 2 old). No single cutover date classifies correctly.
- The same dictionary generation change hits `injury_severity` (`NO APPARENT INJURY` vs `No Apparent Injury`, etc. — 11 distinct values across both cases). The drift detector must be generic over columns, not special-cased to one.
- In **Incidents**, `driver_substance_abuse` is a per-crash **comma-join of every driver's value** (e.g. `Not Suspect of Alcohol Use, Not Suspect of Drug Use, Not Suspect of Alcohol Use, Not Suspect of Drug Use` = two drivers; `N/A, NONE DETECTED` = two old-scheme drivers; mixed-scheme joins exist). This is the worst form of the embedded-comma defect.
- `crash_date_time` is naive local ISO (`2025-12-25T06:10:00.000`); `:updated_at` is UTC with `Z`. 125,005 incident rows share one `:updated_at` from a 2024-06-12 bulk reload.

**TxDOT (`cris_crash`, 100,000 rows, OIDs 1–100,000, one partition, `crash_id` unique)**

- `crash_date` is `YYYY-MM-DD`, `crash_time` is `HH:MM:SS`, both strings; range 2020-01-01 → 2024-12-31.
- Coordinate combos: `located_fl='1'` ⇔ derived `latitude/longitude` populated (91,876 rows; of which 68,932 have null `rpt_*`). `located_fl='0'` with only `rpt_*` populated: 845. No coordinates at all: 7,279. `_geometry_x/_y` (wkid 3081) is present exactly when derived coords are, plus the 845.
- Where both pairs exist, 1,065 rows disagree by > 0.01°.
- `amend_supp_fl='1'`: 5,936 rows. `crash_sev_id` values: 0,1,2,3,4,5 and one `95`. Cross-checking against `death_cnt`/`sus_serious_injry_cnt`/`nonincap_injry_cnt`/`poss_injry_cnt`/`non_injry_cnt` in the data confirms 1 = suspected serious (A), 2 = suspected minor (B), 3 = possible (C), 4 = fatal (K), 5 = not injured (O), 0 = unknown. Verify this yourself with a consistency query and treat any unmapped code (95) as unknown, never as fatal.
- All coordinates fall inside a Texas bbox (lat 25.8–36.5, lon −106.65 to −93.5).

**FARS (2019–2024, national, one partition per year)**

- Accident columns differ by year (2019: 96, 2024: 85); the `*NAME` label columns are present. Read with `union_by_name`.
- **Sentinel formatting varies by year and the longitude sentinel is three-digit:** 2019 writes `99.9999` / `999.9999`; 2024 writes `77.77770000` / `777.777700000`. String equality on `77.7777` matches 68 rows in 2019 and **zero** in 2024. Compare numerically (cast, then `abs(v − s) < 1e-4` for s ∈ {77.7777, 88.8888, 99.9999} on latitude and {777.7777, 888.8888, 999.9999} on longitude) and additionally reject anything outside [−90, 90] / [−180, 180].
- `HOUR='99'` / `MINUTE='99'`: 244 (2019), 253 (2024) accident rows. Person `AGE` 998/999: 1,809 / 2,253. Person `INJ_SEV`: 0 O, 1 C, 2 B, 3 A, 4 K, 5 injured-severity-unknown, 6 died prior to crash, 9 unknown.
- In-scope states per year ≈ TX 3.3–3.8k, FL 2.9k, MD 0.5k crashes; Montgomery County MD (`STATE='24', COUNTY='31'`) ≈ 36–46 per year.

---

## 3. Architecture decisions (made — do not relitigate; record the why in docstrings)

**Engine.** DuckDB SQL in-process for reading bronze (`read_parquet(..., union_by_name=true)`), typing, dedupe, versioning and writing. Python only for the dictionary grammar (unit-testable) and the drift detector. No pandas round-trips on full tables except where DuckDB genuinely can't do it. No Spark.

**Layout.** `data/silver/{source}/{table}_history.parquet` (every version, SCD2) and `{table}_current.parquet` (materialised `is_current` slice), plus `data/silver/crash_current.parquet` (the unified crash grain) and `data/silver/_build_manifest.json`. Silver root defaults to `config.DATA_DIR / "silver"`; add `SILVER_DIR` to `src/config.py`. Do not write GeoParquet yet: silver carries `lat`/`lon` as DOUBLE in EPSG:4326 and Phase 4 adds the geometry column and bbox covering.

**Versioning = bronze snapshot partitions + hash-diff SCD2 in silver** (one mechanism for TxDOT amendments and FARS reissues). Every history row carries `natural_key`, `row_hash`, `valid_from`, `valid_to`, `is_current`, `version_no`, `_bronze_load_ts`, `_bronze_row_sha256`, `_bronze_raw_path`. `row_hash` is computed over the **conformed** attribute columns (not bronze metadata), so a re-serialisation with different key order does not create a version.

**Determinism is a hard requirement.** Silver must be a pure function of bronze contents. Therefore: `valid_from` is the source mutation stamp where one exists (`:updated_at` for Montgomery) and the bronze `_bronze_load_ts` that first carried the hash otherwise (TxDOT, FARS); `valid_to` is the next version's `valid_from`. Never `now()` inside a table. Output rows are explicitly `ORDER BY` natural key then `valid_from`; column order is fixed by the contract; writes go to a `.part` file then `durable_replace`. Wall-clock timestamps belong only in `_build_manifest.json`, which is excluded from byte-identity comparisons. Verify empirically that two builds over the same bronze yield identical sha256 per parquet file (check whether DuckDB `COPY` is stable under threads; if not, write through pyarrow from an ordered result) and document which writer you used and why.

**Which partitions to read.** Discover partitions through `WatermarkStore.load_partitions()`/`artifacts()` and the on-disk tree, not by hardcoding. Montgomery: every partition is a delta; union all, dedupe per natural key. FARS: every partition is a complete snapshot of a year; diff snapshots in `load_ts` order per year; a `ST_CASE` absent from the newer snapshot closes its version (`valid_to` set, `is_current=false`, `deleted_in_load_ts`). TxDOT: a partition is a snapshot of the OID range recorded in its cursor (`oid_floor/oid_ceiling`, sticky `bounded` flag); deletions are only inferable for complete, unbounded sweeps — implement the diff, and gate deletion detection on the cursor flag. Montgomery deletes are not observable through keyset incremental ingest; say so in the docstring and the report.

**Defective rows are kept, never dropped.** Out-of-envelope or sentinel coordinates: keep the row, copy raw values to `lat_raw`/`lon_raw`, set canonical `lat`/`lon` to NULL, set `geo_quality` ∈ {`OK`, `OUT_OF_ENVELOPE`, `SENTINEL`, `MISSING`}. Unparseable dictionary values: keep the row, set the normalised field to `UNMAPPED`, and let the drift detector fail the build unless `--allow-unmapped`. Orphan report numbers: keep, flag. The crash count must never silently change between bronze and silver; the report must reconcile row counts layer to layer.

**Envelopes are config, not code.** Add a `[envelope]` table to `config/sources.toml` (or a new `config/geo.toml`) with a generous bbox per source that is a strict superset of the true jurisdiction polygon, so bbox rejection is definitive and Phase 4's polygon test only refines what bbox accepted. Montgomery: the assignment's bbox padded slightly; TxDOT: Texas bbox; FARS: CONUS+AK+HI/PR plausibility only (national data).

**Time.** Silver stores `crash_datetime_local` as a naive TIMESTAMP exactly as published, plus `crash_date` DATE. Localisation to UTC needs the coordinate-derived timezone and is Phase 4; do not localise with a state default. FARS `HOUR/MINUTE=99` → `crash_datetime_local` NULL, `crash_date` populated. Montgomery `:updated_at` → `source_updated_at_utc` as naive-UTC TIMESTAMP (same convention as the watermark store; pytz is not installed).

**Natural keys.** Montgomery: incidents `report_number`, drivers `person_id`, non-motorists `person_id` (each carries `report_number` as the crash FK). TxDOT: `crash_id` (not `ESRI_OID`, which is service-assigned). FARS: accident `(year, ST_CASE)`, vehicle `(year, ST_CASE, VEH_NO)`, person `(year, ST_CASE, VEH_NO, PER_NO)`. Uniqueness on the current slice is contract-enforced.

**Unified `silver.crash`.** Columns: `crash_uid` (deterministic `uuid5(NAMESPACE, source_system + ':' + source_record_id)` — never random), `source_system` (`MONTGOMERY_MD` | `TXDOT_CRIS` | `NHTSA_FARS`, matching the output contract), `source_record_id`, `jurisdiction` (`MD`, `TX`, or FARS `STATE` FIPS → two-letter), `crash_date`, `crash_datetime_local`, `lat`, `lon`, `geo_quality`, `severity_ordinal`, `severity_source_value`, `is_amended` (TxDOT `amend_supp_fl`, else NULL), `version_no`, `_bronze_load_ts`. One row per crash per source; Phase 3 resolves cross-source overlap on top of this.

---

## 4. Montgomery transform — required behaviour

- Read all partitions of the three datasets with `union_by_name`; dedupe by natural key as above; build SCD2 history with `valid_from = :updated_at`.
- Type everything: coordinates DOUBLE, `crash_date_time` TIMESTAMP, `speed_limit`/`vehicle_year`/`number_of_lanes` INTEGER with `TRY_CAST` and a `_typed_ok` failure count in the report, booleans for `hit_run`, `driverless_vehicle`, `parked_vehicle`, `driver_at_fault`.
- Envelope check per §3; also emit the distance from the bbox centre in the report for the far outliers (compute distance geodesically with `pyproj.Geod` or after reprojecting to EPSG:26985 — comment the CRS choice; never EPSG:3857).
- `driver_substance_abuse` normalisation in Drivers (per-driver grain): produce `substance_scheme` ∈ {`OLD_SINGLE`, `NEW_PAIR`, `NULL`}, `alcohol_status` and `drug_status` ∈ {`NOT_SUSPECTED`, `SUSPECTED`, `UNKNOWN`, `NOT_APPLICABLE`}, `substance_detail` (the old-scheme granular value, e.g. `MEDICATION_CONTRIBUTED`, retained because the new scheme is lossier), and the raw value. Classify **per value by grammar, never by date**. Decide and document the mapping of `OTHER`, `COMBINED SUBSTANCE PRESENT`, and the `PRESENT` vs `CONTRIBUTED` distinction (recommend: both → `SUSPECTED`, with `CONTRIBUTED` preserved in `substance_detail`). Implement as a pure function over the distinct-values set, materialised as a lookup table joined in SQL, so the same function is the drift detector's parser.
- Crash-level substance flags on the incident silver table are **derived by aggregating Drivers** (any-driver-suspected), not by parsing the Incidents column. Parse the Incidents column only with a grammar parser (split on `, `, consume old-scheme tokens singly and new-scheme alcohol/drug tokens as pairs) as a cross-check, and report the agreement rate against the driver-derived value.
- `injury_severity` normalised to one case-insensitive vocabulary; feed the crosswalk.
- Anti-join both directions on the current slices; write counts to the report; flag incidents with no driver as `has_driver_rows=false` and join non-motorists to see how many of the 785 are explained.
- Measure the overlap window by crash date and by `:created_at`, report both, and state which one the test uses.

## 5. TxDOT transform — required behaviour

- Coordinate precedence: derived `latitude/longitude` when `located_fl='1'`; else `rpt_latitude/rpt_longitude`; else NULL with `geo_quality='MISSING'`. Record `coord_source` ∈ {`CRIS_DERIVED`, `OFFICER_REPORTED`, `NONE`}. Reproject `_geometry_x/_y` from EPSG:3081 to EPSG:4326 with pyproj (comment: this is a reprojection for canonical storage, not a metric op) and report the max/median discrepancy against the derived pair as a self-consistency check. Where both pairs exist, compute the geodesic distance between them and report the distribution; flag `coord_pairs_disagree` above a threshold you choose and justify.
- Parse `crash_date` + `crash_time` into `crash_datetime_local`; `report_date` to DATE. Report parse failure counts.
- Decode `crash_sev_id` via the crosswalk after verifying it against the injury-count columns (write that query as a test). Type the other `*_id` columns as INTEGER but leave them undecoded; document that ~60 of them are opaque without the CRIS lookups (CRIS guide V29.0 URL is in the assignment) and decode `wthr_cond_id`/`light_cond_id` only if you can source the lookup table with a citation — otherwise leave for Phase 3.
- SCD2 on `crash_id` with `valid_from = _bronze_load_ts`; `is_amended = amend_supp_fl='1'`. County: verify the CRIS `cnty_id` → FIPS relationship (Texas county FIPS are odd numbers in alphabetical order, so FIPS = 2·cnty_id − 1 for Harris=101→201, Bexar=15→029, Dallas=57→113, Tarrant=220→439, El Paso=71→141) and, if it holds for all values, emit `county_fips`; if not, leave NULL and say so.

## 6. FARS transform — required behaviour

- Silver carries `accident`, `vehicle`, `person` only, all years, national (small enough; Phase 3 scopes to MD/TX/FL). Drop the `*NAME` label columns from the typed tables but keep them available in a `fars_codebook` lookup extracted from the data (code, name, year range) — it doubles as the severity crosswalk evidence.
- Sentinels per §2, driven by a seed `config/fars_sentinels.csv` (`table, column, sentinel_values, meaning`) rather than a blanket 7/8/9 rule, because 9 is a legitimate code in some fields. Cover at least: LATITUDE/LONGITUD, HOUR, MINUTE, AGE, INJ_SEV 9, and any field you type. Map before any join or aggregate.
- Snapshot diff per year across partitions as in §3. Build `crash_date` from YEAR/MONTH/DAY; `jurisdiction` from `STATE`.
- Crash-level severity for FARS is `5` by definition (fatality census) — also compute the max person `INJ_SEV` through the crosswalk and report any accident whose persons show no `INJ_SEV=4` (there will be a few; keep them, flag them).

## 7. Severity crosswalk

Seed CSV `config/severity_crosswalk.csv` with columns `source_system, source_field, source_value, kabco, severity_ordinal, notes`. Ordinal: 0 unknown/not reported, 1 no apparent injury (O), 2 possible (C), 3 suspected minor (B), 4 suspected serious (A), 5 fatal (K). Document lossiness explicitly in the CSV notes and the report: FARS 5 "injured, severity unknown" (recommend ordinal 0 with `severity_note='INJURED_UNKNOWN'`, never 2), FARS 6 "died prior to crash" (0, not 5), MoCo has no "injured unknown", TxDOT `crash_sev_id` is crash-level while the others are person-level. Montgomery crash severity = max ordinal over drivers + non-motorists, cross-checked against `acrs_report_type` (report the disagreement count). `severity_crosswalk.py` loads the CSV and raises on an unmapped value (drift, not silent 0).

## 8. Contracts

`contracts/bronze.schema.json` and `contracts/silver.schema.json` are JSON Schema 2020-12 documents (same dialect as the output contract) describing each table's row: types, nullability, enums, ranges (`lat` in [−90, 90], `severity_ordinal` 0–5, `geo_quality` enum). Add a top-level `x-table-constraints` block per table for what row-level JSON Schema cannot express: `unique_keys`, `foreign_keys` (with `orphans_allowed_when` naming the flag column), `row_count_min`. `src/contracts.py` validates a DuckDB relation against a table's entry (types via `DESCRIBE`, the rest via generated SQL) and raises with a readable list of violations. `build.py` validates every silver table before renaming `.part` into place. Bronze contract: all columns VARCHAR, the six `_bronze_*` columns required, plus per-source required raw columns. Add a test that validates real bronze pages and a freshly built silver against both.

## 9. Drift detector

`dictionaries.py` holds the accepted vocabulary per `(dataset, column)` as data (seed CSV or a dict literal with a citation of where each token was observed). `detect_drift(dataset, column, values) -> DriftReport` returns unmapped tokens with first/last `crash_date_time` and `:created_at` seen and row counts. `build.py` fails on drift by default. `python -m src.transform.drift --dataset mmzv-x632 --column driver_substance_abuse --vocabulary-asof 2023-12-27` replays bronze with only the pre-cutover vocabulary accepted and must fire naming the new-scheme tokens and the 2023-12-28 first-seen date; run it for `injury_severity` too, and paste both outputs into the report. This is the Part 6 "show it firing against historical data" evidence.

## 10. Tests

Fixtures live in `tests/conftest.py`. Use **small committed extracts** under `tests/fixtures/bronze/` in the exact bronze format (all-string parquet plus the `_bronze_*` columns, in the real directory layout `{source}/{dataset}/{load_ts}/page_*.parquet`), generated once by a committed script `tests/fixtures/make_extracts.py` that samples real bronze rows exhibiting each defect (these are public crash records with no PII; keep it to a few hundred rows per table). The extract must include: out-of-envelope incidents; old-scheme, new-scheme and overlap-window driver rows; mixed-scheme incident join strings; orphan incident report numbers; multi-driver crashes; the same Montgomery `:id` in two partitions with different `:updated_at`; TxDOT rows for every coordinate combo and both `amend_supp_fl` values, plus the same `crash_id` in two partitions with a changed attribute (a synthetic amendment); FARS accident rows with 2019-style and 2024-style sentinels, HOUR=99, and the same `ST_CASE` across two partitions with one row revised and one removed. Silver fixtures are built by running the transform on the extract into `tmp_path`. A `CRASH_TEST_FULL_BRONZE=1` env flag optionally points the fixtures at the real `data/bronze` so the same tests can run at full volume.

The four scaffold xfail tests keep their signatures and become the bronze side, with `xfail(strict=True)` so an unexpected pass fails the suite (a defect that isn't present in the fixture is a broken fixture). Put each invariant in a shared assertion helper and add a `test_silver_*` twin that runs the identical helper on the silver fixture. Invariants must be substantive on bronze, e.g. the cutover test asserts "there exists a single date that classifies every row's scheme correctly" (false on bronze because of the overlap) and on silver asserts every row in the window carries a grammar-resolved scheme and no `UNMAPPED`.

Also implement: `test_crash_fact_grain_is_one_row_per_crash` (uniqueness of `crash_uid` and of natural key per source on `silver.crash`, plus a negative control showing the Drivers-derived count is ≈1.8× higher), `test_fars_sentinel_coordinates_excluded` (both sentinel formats, numerically; no silver `lat` > 72 outside Alaska rows), `test_pipeline_is_idempotent_under_restatement` with a `pipeline_runner` fixture defined at the silver level (build twice → identical per-file sha256; add a bronze partition containing one amended TxDOT row → exactly one new history version, everything else byte-identical; add a FARS restatement partition → the revised `ST_CASE` gets a new version, the removed one is closed, the unchanged ones get nothing). Add unit tests for the substance grammar (every distinct value in §2, the incident join strings, the ambiguity cases with `Unknown`), the crosswalk (raises on unmapped, FARS 5/6 → 0), the contract validator (a violating relation raises), the drift detector (fires on the new tokens, silent on the old vocabulary), and the TxDOT `crash_sev_id` consistency query.

## 11. Conventions

- Match Phase 1's style: module docstrings that explain **why**, not what; comments at every CRS choice; tests that exercise real behaviour rather than monkeypatched internals.
- Commit as you go with messages like `feat(transform): ...`, `test(transform): ...`, `feat(contracts): ...`. Commit only the files you fill in this phase; the other empty scaffold files stay untracked. Never commit anything under `data/`, `config/settings.toml`, or `ai docs/`.
- Do not modify `fixtures/synthetic_parties.csv`, `contracts/lead_output.schema.json`, or anything under `src/ingest/` beyond adding a read-only helper if one is genuinely missing (say so in the report if you do).
- Any new dependency goes in `requirements.txt` with a one-line reason in the report. Prefer none.
- Never print row contents containing `person_id`/`report_number` to logs at INFO; counts only.
- When the spec and the data disagree, the data wins and the disagreement goes in the report (Phase 1 found two such cases; expect more).

## 12. Deliverable: the report

Write `ai docs/implementation/phase2-silver-report.md` with these sections: What I built (per module, a paragraph each); Things the spec said that the data doesn't do; What I bounded and why; Bugs the tests caught; Verification table (row-count reconciliation bronze → silver per table, dedupe deltas, byte-identity hashes from two builds, test counts); **For DATA_QUALITY.md** — one entry per known defect with detection query, measured count, and disposition, ready to paste; **For DECISIONS.md** — the decisions in §3 plus any you had to add, with the rejected alternative for each; Open items for Phase 3/4 (what `silver.crash` still lacks, the polygon envelope refinement, timezone localisation, code lookups not decoded).

## 13. Definition of done

- `python -m src.transform.build` rebuilds all of silver from local bronze in one command, twice, with identical parquet hashes; the manifest records input partitions and output hashes.
- Every silver table passes its contract; `silver.crash` is provably one row per crash.
- `pytest -q` is green: the four scaffold tests xfail strictly on bronze, their silver twins pass, the three previously erroring tests pass, and all new tests pass. No test depends on `data/bronze` unless the env flag is set.
- The drift CLI output showing the detector firing on the 2023-12-28 cutover is in the report.
- All nine known defects have a measured count and a disposition in the report.
- `git log` shows the work in small commits; `git status` shows no data or settings files staged.
