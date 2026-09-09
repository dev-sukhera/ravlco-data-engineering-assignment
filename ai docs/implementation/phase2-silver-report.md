# Phase 2 — Bronze → Silver: Build Report

Scope: `src/transform/` (common, dictionaries, severity_crosswalk, montgomery,
txdot, fars, unified, build, drift, report), `src/contracts.py`, both contract
schemas, three config seeds, and the test suite.
Status: complete and verified end to end, on the fixture and on the full local bronze.

```
python -m src.transform.build          # 16 parquet files, 1,562,546 silver history rows, ~26s
python -m src.transform.report         # every defect's measured count
python -m src.transform.drift --dataset mmzv-x632 \
       --column driver_substance_abuse --vocabulary-asof 2023-12-27
pytest -q                              # 127 passed, 4 xfailed        (fixture,  24s)
CRASH_TEST_FULL_BRONZE=1 pytest -q     # 125 passed, 2 skipped, 4 xfailed (full, 173s)
```

---

## What I built

**`src/transform/common.py`** — the machinery every table shares. Partition
discovery reads the union of the watermark manifest and the on-disk tree, because
neither alone is right: a partition on disk but not in the manifest is a crash
between write and record and holds real rows, while one in the manifest but not on
disk has been archived and silver does not get to veto bronze retention. The SCD2
builder is one SQL generator with two modes — delta for Montgomery's keyset reads,
snapshot with deletion inference for TxDOT and FARS — and a `row_hash` computed over
the conformed attributes only, so a re-ingest under a new `load_ts` cannot
manufacture a version. The writer materialises an ordered arrow table and writes it
through Phase 1's `durable_replace`.

**`src/transform/dictionaries.py`** — the `driver_substance_abuse` grammar as a pure
function over a string, plus a value-set drift detector that is generic over
`(dataset, column)`. The grammar is materialised over the *distinct-value* set into a
lookup table and joined in SQL, so the parser the unit tests exercise is the parser
that runs, and the Python round trip is over 21 strings rather than 220,043 rows.

**`src/transform/severity_crosswalk.py` + `config/severity_crosswalk.csv`** — 29
rows mapping three scales to one 0–5 ordinal, with the lossy cases argued in the
`notes` column. It is a CSV because a crosswalk is a claim about what two
vocabularies mean to each other and wants to be reviewable by someone who reads the
CRIS guide but not Python. An unmapped value raises; it is drift, not a silent 0.

**`src/transform/montgomery.py`** — three tables, delta SCD2, `valid_from` =
Socrata's `:updated_at`. Crash-level substance flags are aggregated from the Drivers
table; the Incidents concatenation is parsed with the grammar only as a cross-check.

**`src/transform/txdot.py`** — one table, snapshot SCD2 gated on the OID range the
ingest cursor says was actually swept, coordinate precedence derived from the data
rather than asserted, and `crash_sev_id` decoded only because its meaning is
provable from the injury-count columns in the same row.

**`src/transform/fars.py`** — accident/vehicle/person, all years and all states,
snapshot SCD2 scoped per year, sentinels from a per-column seed CSV, and a
`codebook` table extracted from NHTSA's own `<COL>NAME` label columns.

**`src/transform/unified.py`** — `silver.crash`, one row per crash *per source*,
deliberately not entity-resolved.

**`src/contracts.py` + the two schemas** — JSON Schema 2020-12 per row plus an
`x-table-constraints` block for the three things JSON Schema cannot say: grain,
referential expectations, and a row-count floor. Hand-rolled rather than Great
Expectations or Soda because against a DuckDB relation this is a `DESCRIBE` plus a
dozen generated `COUNT` queries, and every violation is a count with example keys
rather than a boolean.

**`src/transform/build.py`** — one command, validate-then-write.
**`src/transform/report.py`** / **`drift.py`** — the two evidence CLIs.

---

## Things the spec said that the data doesn't do

Phase 1 found two. This phase found six more. In every case the data won.

1. **`driver_substance_abuse` has no SQL NULLs in Drivers.** The brief lists "SQL
   NULL, N/A, UNKNOWN, Unknown, Unknown" as the spellings of null. In 232,411 bronze
   rows there are 21 distinct values and **zero** SQL NULLs. The null does appear —
   in `injury_severity` (4,618 driver rows) and in Incidents'
   `driver_substance_abuse` (785 rows, exactly the crashes with no driver). Silver
   handles all four spellings; only three of them exist in the column the brief
   names.

2. **Four Montgomery columns carry the crash-level concatenation, not one.** A
   substance column is single-valued only on the table whose grain matches it.
   Everywhere else it is the denormalised roll-up of the *other* party type:

   | column | grain | distinct values | naive single-parse failures |
   |---|---|---|---|
   | `mmzv-x632.driver_substance_abuse` | one driver | 21 | 0 |
   | `n7fk-dce5.non_motorist_substance_abuse` | one non-motorist | 20 | 0 |
   | `mmzv-x632.non_motorist_substance_abuse` | all non-motorists | 29 | 8 |
   | `n7fk-dce5.driver_substance_abuse` | all drivers | 33 | 16 |
   | `bhju-22kf.driver_substance_abuse` | all drivers | 121 | 99 |
   | `bhju-22kf.non_motorist_substance_abuse` | all non-motorists | 29 | 8 |

   The grammar leaves **zero** unmapped tokens across all six.

3. **The two dictionary generations use different join semantics on the crash-level
   column.** This is the sharpest finding of the phase and nothing documents it. The
   old generation publishes the **distinct set** of its drivers' values — two drivers
   both `NONE DETECTED` produce one token. The new generation publishes the full
   **list** — two drivers both `Not Suspect of Alcohol Use, Not Suspect of Drug Use`
   produce both pairs. Measured over the current slice, with no exceptions in either
   direction:

   | crash's driver generation | crashes | string matches driver ROW count | string matches DISTINCT-value count |
   |---|---|---|---|
   | `OLD_SINGLE` | 96,862 | 42,316 (the single-driver ones) | **96,862 (100%)** |
   | `NEW_PAIR` | 27,358 | **27,358 (100%)** | 12,455 |

   So the crash-level string cannot count parties under either generation without
   first knowing which generation it is in — and the Drivers rollup is the only safe
   source. Both properties are columns on `montgomery/crash` so a test asserts them.

4. **`number_of_lanes` is also comma-joined.** 3,830 incident rows carry values like
   `"2, 3"` or `"1, 4"` where the crash spans two roadway segments. The embedded-comma
   defect is not confined to the dictionary columns. `TRY_CAST` nulls them (never 2 or
   23) and `number_of_lanes_raw` keeps the string.

5. **`person_id` is not unique in Non-Motorists.** 9 ids appear twice under one
   `report_number`, distinguished only by Socrata's `:id` and disagreeing on a
   denormalised crash-level field (`traffic_control`: `Stop Sign` vs `No Controls`).
   The grain is one row per non-motorist, so the duplicate is resolved deterministically
   by taking the greatest `:id`, with `duplicate_person_id` preserved. 7,521 distinct
   `:id` → 7,512 silver rows.

6. **Neither the FARS vehicle nor the person file has a `YEAR` column.** Only
   `accident` publishes one. `ST_CASE` restarts every year, so the natural keys
   `(year, ST_CASE, VEH_NO)` and `(year, ST_CASE, VEH_NO, PER_NO)` would collide
   across six years without a year from somewhere. It comes from `_bronze_dataset`,
   the partition path segment Phase 1 writes into every row — which makes the bronze
   provenance block load-bearing rather than decorative.

7. **FARS longitude sentinels are three-digit, not `99`.** They are `777.7777` /
   `888.8888` / `999.9999`, mirroring the latitude values. A rule written for `99`
   would match nothing.

8. **The `_bronze_*` provenance block is not uniform across sources.** The HTTP
   sources write six columns; FARS writes five (`_bronze_member` and
   `_bronze_zip_sha256` instead of `_bronze_page`/`_bronze_raw_path`/
   `_bronze_row_sha256`), because its unit of raw preservation is the annual zip, not
   a page. The bronze contract records both shapes rather than failing every FARS page
   for a difference that is correct.

---

## What I bounded and why

**No cross-source entity resolution.** `silver.crash` is a conformed grain, one row
per crash *per source*, and a Texas fatality legitimately appears in both
`TXDOT_CRIS` and `NHTSA_FARS`. Resolving them here would make every downstream count
depend on the match rule, so every count would silently move whenever the rule was
tuned. Here the count is a fact about the sources; in Phase 3 it becomes a claim about
identity, on top of this table. `src/transform/model.py` stays empty.

**No timezone localisation.** `crash_datetime_local` is the naive wall clock exactly
as published. Localising needs the coordinate-derived IANA zone, which needs the
coordinate pipeline, which is Phase 4. Storing a naive stamp in a UTC column would be
the "convert instead of localize" error the assignment warns about, so the column is
named for what it holds.

**Envelopes are bbox only.** Each is a strict superset of its jurisdiction polygon,
which is what makes rejection definitive: a point outside a superset bbox is outside
the jurisdiction and no polygon test can rescue it. Phase 4's TIGER point-in-polygon
join refines what bbox accepted and never revisits what it rejected.

**~60 TxDOT `*_id` columns are typed but not decoded.** 64 `*_id` columns exist; 4 are
keys or decoded (`crash_id`, `case_id`, `cnty_id`, `crash_sev_id`). The rest need the
CRIS Automated Interface guide V29.0 lookups, which are in a PDF and not
machine-readable from the published URL. Inventing labels from a guess would be worse
than a number, so they stay integers with a citation. `wthr_cond_id` and
`light_cond_id` are carried into silver so Phase 3 can decode them in one place.

**No GeoParquet geometry column, no bbox covering.** Silver carries lat/lon as DOUBLE
in EPSG:4326. Phase 4 adds the geometry column and the covering; the row-group size is
already set to 122,880 so those boundaries are the ones it will prune on.

**Montgomery deletions are not observable.** Every partition is a keyset delta, so
absence means "unchanged since the watermark", not "deleted". Detecting a deleted
report would need a periodic full-key census (`$select=:id`, ~125k ids, one cheap
sweep) diffed against silver's current slice — a Phase 8 scheduled job, not something
a delta transform can infer. `deleted_in_load_ts` is always NULL for Montgomery and
the docstring says so.

---

## Bugs the tests caught

All three were in `src/contracts.py`, and all three were found by the unit tests
rather than by the build.

- **Range and enum checks ran on a column whose type had already failed.** A VARCHAR
  column contracted as `integer` produced a `BinderException` from
  `WHERE ordinal >= 0` instead of a reported type violation — so a schema that failed
  in the most basic way possible crashed the validator instead of explaining itself.
  The type violation now short-circuits the column's other checks.

- **`orphans_allowed_when` was inverted.** It restricted the foreign-key check to
  exactly the rows it was supposed to exempt, so a licensed orphan failed and an
  unlicensed one passed — the constraint was doing the opposite of its name. Fixed,
  with a `coalesce(..., false)` so a NULL flag fails closed rather than reading as a
  licence.

- **A table with no declared unique key crashed on an `IndexError`.** Every bronze
  table is in that position (a multi-partition read is legitimately not unique on
  anything), so `test_bronze_pages_satisfy_the_bronze_contract` was the first thing to
  hit it.

One design fault, caught by the determinism experiment rather than a test, is in
"Verification" below.

---

## Verification

### Row-count reconciliation, bronze → silver

| table | bronze rows | bronze distinct key | silver history | silver current | dedupe delta |
|---|---|---|---|---|---|
| montgomery/crash | 132,190 | 125,005 `report_number` | 125,005 | 125,005 | −7,185 |
| montgomery/driver | 232,411 | 220,043 `person_id` | 220,043 | 220,043 | −12,368 |
| montgomery/non_motorist | 8,034 | 7,521 `:id` / 7,512 `person_id` | 7,512 | 7,512 | −522 |
| txdot/crash | 100,000 | 100,000 `crash_id` | 100,000 | 100,000 | 0 |
| fars/accident | 222,695 | 222,695 `(year, ST_CASE)` | 222,695 | 222,695 | 0 |
| fars/vehicle | 343,261 | 343,261 | 343,261 | 343,261 | 0 |
| fars/person | 544,030 | 544,030 | 544,030 | 544,030 | 0 |
| **silver.crash** | — | — | — | **447,700** | — |

The Montgomery deltas are the overlap between two bronze partitions, not loss: the
earlier partial partition's 7,185 / 12,368 / 522 rows are byte-identical duplicates
of rows in the full one, and **zero** `:id`s differ in content between partitions. The
non-motorist table is the only place silver is smaller than bronze's distinct-`:id`
count, and the difference is exactly the 9 duplicated `person_id`s.

`silver.crash` = 125,005 + 100,000 + 222,695 = 447,700, all distinct `crash_uid`.

### Byte-identity

Two consecutive `python -m src.transform.build` runs, and a third at `--threads 1`,
produce identical sha256 for all 16 parquet files.

| file | rows | sha256 |
|---|---|---|
| `crash_current.parquet` | 447,700 | `2df2d6c6d9513e1f…` |
| `montgomery/crash_history.parquet` | 125,005 | `f5b3295d4c0badbd…` |
| `montgomery/driver_history.parquet` | 220,043 | `e6dc853694cfac8a…` |
| `montgomery/non_motorist_history.parquet` | 7,512 | `7be6a3be5104cae8…` |
| `txdot/crash_history.parquet` | 100,000 | `8885d7f4b5398395…` |
| `fars/accident_history.parquet` | 222,695 | `8dfc198b35b094f9…` |
| `fars/vehicle_history.parquet` | 343,261 | `b14a4abc6d2162ce…` |
| `fars/person_history.parquet` | 544,030 | `c6023663b894ddaa…` |
| `fars/codebook.parquet` | 4,455 | `a5721235835c9c9a…` |

Each `_current` file currently hashes identically to its `_history` twin, because
every key has exactly one version in the local corpus — no restatement has happened
yet. The fixture's synthetic TxDOT amendment and FARS reissue are what separate them,
and the tests assert the separation.

### The writer, and the mistake I nearly shipped

I set out to establish which parquet writer was deterministic. Over an ordered
Montgomery Drivers relation at `threads=1/2/8`:

| writer | ORDER BY `:id` (not unique) | ORDER BY `(:id, _bronze_load_ts)` |
|---|---|---|
| DuckDB `COPY` | 3 different sha256 | 1 sha256 |
| pyarrow | 3 different sha256 | 1 sha256 |

**The writer was never the problem.** `:id` repeats 12,368 times across overlapping
partitions, so `ORDER BY :id` is not a total order, and SQL does not consider that an
error — it just breaks the tie by whichever thread finished first. Both writers are
byte-stable once the sort key is total. Had I only compared writers at a fixed thread
count I would have concluded either one was fine and shipped a build whose output
depended on the machine it ran on.

`write_parquet()` therefore **asserts the sort key is total** and raises
`NonTotalOrder` rather than writing; `test_writer_refuses_a_non_total_sort_key` covers
it. The contract asserts the same property from the other side — history tables are
validated as unique on `(natural_key, valid_from, _bronze_load_ts)`, which is exactly
the order they are written in.

pyarrow is still the writer, for three reasons that are not correctness: explicit
`row_group_size` (122,880, sized for Phase 4's bbox pruning) rather than DuckDB's
internal heuristic, reuse of Phase 1's `durable_replace` so silver gets the same
fsync-then-rename durability as bronze, and writing the same arrow table the contract
validator inspected rather than re-executing the query.

### Self-consistency checks that could have failed and didn't

- **`located_fl` ⇔ CRIS-derived coordinates**: 0 exceptions in 100,000 rows. The
  coordinate precedence rule is empirical, not aesthetic.
- **EPSG:3081 → EPSG:4326 reprojection vs the published derived pair**: max
  discrepancy **0.1 mm** over 85,919 distinct pairs. The two are the same point.
- **`crash_sev_id` vs the injury-count columns**: every code's profile matches the
  crosswalk (4 → all 997 have `death_cnt > 0`; 1 → all 4,119 have
  `sus_serious_injry_cnt > 0` and no deaths; 5 → all 62,454 have `non_injry_cnt > 0`
  and every injury count zero). Note 5 is the *least* severe code.
- **FARS `FATALS` vs the person file**: 0 accidents of 222,695 disagree, and 0 lack a
  person with `INJ_SEV = 4`.
- **Incidents substance string vs the Drivers rollup**: 0 disagreements on either
  suspicion flag across all 124,220 comparable crashes.

---

## For DATA_QUALITY.md

Ready to paste. Every number is printed by `python -m src.transform.report`, which
prints the detection query alongside it.

### 1. Coordinates that pass a null check and are still wrong

*Source*: `bhju-22kf`.
*Detection*:
```sql
SELECT COUNT(*) FROM (SELECT DISTINCT ":id", latitude, longitude FROM incidents)
WHERE TRY_CAST(latitude AS DOUBLE) NOT BETWEEN 38.9 AND 39.36
   OR TRY_CAST(longitude AS DOUBLE) NOT BETWEEN -77.54 AND -76.87
```
*Measured*: **114** rows outside the assignment's stated bbox, **105** outside the
padded envelope actually used, **0** null or zero-valued coordinates. Furthest
**211.6 km** from the county; median offender **16.0 km**. Examples land in Anne
Arundel County, Pennsylvania and Virginia.
*Disposition*: **Kept, never dropped.** `lat_raw`/`lon_raw` preserve the published
values, canonical `latitude`/`longitude` are NULLed, `geo_quality =
'OUT_OF_ENVELOPE'`, and `distance_from_envelope_m` carries the geodesic distance to
the nearest envelope edge (WGS84 ellipsoid via `pyproj.Geod` — a distance, so not
computed in any projected CRS and emphatically not EPSG:3857, whose scale error at 39°N
is 1/cos 39° ≈ 1.29). Dropping them would change the crash count between layers and
destroy the evidence that a report exists; a wrong coordinate is not a wrong crash.
The envelope is padded 0.02° beyond the assignment's figure because the assignment's
bbox is the county bounds rounded to two decimals and therefore *clips* the real
polygon — a crash on the actual county line rounds outside. Both counts are reported
so a reviewer can reproduce the one in the brief.

### 2. Two generations of code dictionary concatenated in one column

*Source*: `mmzv-x632.driver_substance_abuse` (and five more columns — see finding 2 above).
*Detection*: `SELECT driver_substance_abuse, COUNT(*) FROM drivers GROUP BY 1`, then
classify each distinct value with `dictionaries.parse_substance`.
*Measured*: 21 distinct values — 12 old-generation (172,116 deduped rows) and 9
new-generation (47,927). Zero SQL NULLs in this column. In Incidents the same tokens
appear in **121** distinct concatenations.
*Disposition*: parsed by grammar into `substance_scheme`, `alcohol_status`,
`drug_status` and `substance_detail`, with `substance_raw` retained. 0 UNMAPPED. The
four spellings of null stay distinct: `N/A` → `NOT_APPLICABLE` (no driver to test —
parked or driverless), `UNKNOWN` and `Unknown, Unknown` → `UNKNOWN` (a driver, not
tested), SQL NULL → scheme `NULL`. `PRESENT` and `CONTRIBUTED` both map to
`SUSPECTED` — an officer who records causation has necessarily also detected — with
the causation claim preserved verbatim in `substance_detail`.

### 3. The dictionary cutover overlaps

*Source*: `mmzv-x632`.
*Detection*: group deduped driver rows by crash date and count each generation; keep
dates where both are non-zero.
*Measured*: overlap by crash date is **2023-12-28** (2 new vs 39 old) and
**2024-01-03** (37 new vs 2 old); first new-generation crash date 2023-12-28, last
old-generation 2024-01-03. **The best possible single cutover date still misclassifies
4 driver rows.** Measured by minimising, over every candidate date in the data, the
count of rows a `crash_date >= D` rule would file under the wrong generation.
*Disposition*: no cutover date is used anywhere. Every value is classified by grammar,
so the window needs no special case. `:created_at` is unusable as a proxy — a
2024-06-12 bulk reload stamped 172,096 old-generation and 3,637 new-generation rows
with one creation date, reporting a seven-month "overlap" that is an artefact of our
own read. `test_dictionary_cutover_overlap_handled` asserts the non-existence of a
correct cutover date on bronze; its silver twin asserts every row in the window
resolved through the grammar.

### 4. The tables disagree about the crash universe

*Source*: `bhju-22kf` ⟷ `mmzv-x632`.
*Detection*: anti-join on `report_number` in both directions.
*Measured*: **785** incident report numbers have no driver row; **0** driver report
numbers have no incident row. Of the 785, **111** are explained by a non-motorist row
(pedestrian- or cyclist-only crashes) and **674** have *no party row at all*. By type:
678 Property Damage, 106 Injury, 1 Fatal. Spread evenly across 2015–2026 (40–140 per
year), so it is not a one-off load failure. Corroborating evidence: those same 785
crashes are exactly the rows whose Incidents `driver_substance_abuse` is SQL NULL.
*Disposition*: an inner join is never used. Crashes with no driver row are kept and
flagged `has_driver_rows = false`; party tables join back with LEFT JOIN. The
contract's foreign key is enforced in the direction that holds (driver → crash, 0
orphans) and the other direction is a flag, not a constraint.

### 5. Grain fan-out

*Source*: `mmzv-x632`.
*Detection*: `SELECT COUNT(*)::DOUBLE / COUNT(DISTINCT report_number) FROM drivers`.
*Measured*: **1.871** raw driver rows per report number, **1.7714** after deduping the
overlapping partitions on `:id`. Both are reported because the dedupe changes the
number and a reviewer reproducing it from raw bronze will get the first one.
Aggregating any crash-level field off Drivers overcounts by that factor.
*Disposition*: the real key is enforced — `person_id` on the party tables,
`report_number` on the crash table, both as `unique_keys` in the silver contract on
the current slice. Every crash-level rollup is aggregated from the party tables
(`MAX`, `BOOL_OR`, `COUNT`), never read off a driver row, and
`test_silver_crash_level_flags_are_not_read_off_a_driver_row` asserts the crash flags
equal the aggregate.

### 6. Two competing coordinate pairs in TxDOT

*Source*: `cris_crash`.
*Detection*: group by `(located_fl, derived populated, officer populated)`.
*Measured*: exactly four combinations —

| `located_fl` | derived | officer | rows |
|---|---|---|---|
| 1 | yes | no | 68,932 |
| 1 | yes | yes | 22,944 |
| 0 | no | yes | 845 |
| 0 | no | no | 7,279 |

`located_fl = 1` **if and only if** the derived pair is populated: 0 exceptions.
Where both pairs exist they disagree by > 0.01° in **1,065** rows; the geodesic
distance between them has median **10.5 m**, p95 **944 m**, max **1,190 km**.
*Disposition*: precedence is CRIS-derived → officer-reported → NULL, recorded per row
in `coord_source`. The rule is empirical: the derived pair is geocoded and
quality-controlled by CRIS against the state road network and is present for 91,876
rows against the officer pair's 23,789, so preferring the officer pair would leave
68,932 rows (68.9%) uncoordinated for no gain. The officer pair is the sole source for
the 845 rows CRIS could not locate, where using it is strictly better than a null.
`coord_pairs_disagree` flags the 1,065 so a consumer can weight them.
Cross-check: reprojecting `_geometry_x/_y` from EPSG:3081 lands within **0.1 mm** of
the derived pair over 85,919 distinct pairs.

### 7. Amended reports

*Source*: `cris_crash.amend_supp_fl`.
*Measured*: **5,936 / 100,000 (5.9%)**. Restatement is not an edge case here.
*Disposition*: SCD2 on `crash_id` with a row hash over the conformed attributes. An
amendment produces exactly one new version and closes the previous one; a re-ingest of
identical content produces none, because the hash excludes `_bronze_*` metadata.
Deletion detection is scoped to the OID range the ingest cursor says was **actually
swept** — `(oid_floor, last_objectid]`, not `(oid_floor, oid_ceiling]`. The local
cursor reads floor 0, ceiling 3,088,450, `last_objectid` 100,000: the ceiling is the
universe the sweep aimed at, `last_objectid` is how far the bounded slice got. Scoping
to the ceiling would infer the deletion of 2.99M crashes that were never requested.
Where partitions declare different swept ranges the transform downgrades to delta mode
with a warning in the manifest — a missed deletion is recoverable, a fabricated one is
not.
`test_pipeline_is_idempotent_under_restatement` builds twice (identical hashes), then
adds the bronze partition containing one amended row and asserts exactly one crash
gains a second version, no other source's files change a byte, and the unified crash
grain changes in exactly one row.

### 8. String dates and integer code dictionaries

*Source*: `cris_crash`.
*Measured*: 3 date/time columns typed `esriFieldTypeString`; **64** `*_id` columns, of
which **60** are opaque code dictionaries.
*Disposition*: dates parsed to DATE/TIME/TIMESTAMP with **0** parse failures on all
three columns. `crash_sev_id` is decoded *because its meaning is verifiable from the
data*: `verify_crash_sev_consistency()` cross-checks every code against
`death_cnt`/`sus_serious_injry_cnt`/`nonincap_injry_cnt`/`poss_injry_cnt`/
`non_injry_cnt` in the same row, and it is also a test. Code 95 (1 row, every injury
count zero) is undocumented in CRIS V29.0 and maps to unknown (0), never to a
severity. `cnty_id` is decoded to county FIPS via `FIPS = 2 × cnty_id − 1` (Texas
county FIPS are odd numbers in alphabetical order, and CRIS's id is the same
alphabetical ordinal) — spot-checked against Harris 101→201, Bexar 15→029, Dallas
57→113, Tarrant 220→439, El Paso 71→141, and safe to apply in bulk because `cnty_id`
runs 1..254 with no nulls and no out-of-range values across all 100,000 rows, and
Texas has exactly 254 counties. The other 60 stay integers, with the CRIS guide V29.0
URL cited.

### 9. Sentinel values, not nulls

*Source*: FARS `accident` and `person`.
*Detection*: numeric, `abs(cast(v AS DOUBLE) − s) < 1e-4`.
*Measured*: **873** accident rows across 2019–2024 carry a sentinel coordinate
(68 / 170 / 122 / 214 / 129 / 170 by year). **An exact string match on
`'77.7777'`/`'88.8888'`/`'99.9999'` finds 68 of them — all in 2019, none in any later
year.** Six distinct spellings of three sentinels are present: `77.7777` (8),
`77.77770000` (428), `88.8888` (1), `88.88880000` (72), `99.9999` (59),
`99.99990000` (305). `HOUR = '99'`: 244–305 per year, 1,668 total. `AGE` 998/999:
14,498 person rows. `INJ_SEV = 9`: 8,580.
*Disposition*: driven by `config/fars_sentinels.csv` per `(table, column)` — a blanket
7/8/9 rule would corrupt `LGT_COND`, `HARM_EV` and `MAN_COLL`, where 9 is a
substantive code, and a test asserts those three carry no sentinel rule. Coordinates
are matched numerically *and* range-checked against [−90, 90] / [−180, 180], so a new
sentinel format cannot place a crash in the Arctic Ocean before anyone has added it to
the CSV. `HOUR`/`MINUTE = 99` NULLs `crash_datetime_local` and leaves `crash_date`
populated — collapsing an unknown hour to midnight would invent ~280 crashes a year at
00:00. Sentinels are mapped **before** any join or aggregate: a `MAX(INJ_SEV)` that
has not had 9 removed returns 9. Silver's maximum latitude outside Alaska is
**49.002°**. `TRAV_SP = 997` is deliberately *not* a sentinel: "151 mph or greater" is
a top-code, not an absence, and nulling it would discard a real observation.

### Bonus: three defects the assignment does not list

- **`number_of_lanes` is comma-joined** — 3,830 incident rows. See finding 4 above.
- **Non-motorist `person_id` is not unique** — 9 ids. See finding 5.
- **`acrs_report_type` disagrees with the party-level severity in 3,975 crashes**
  (3.2% of the 124,331 crashes where a party-derived comparison is possible). The crash-level
  ordinal is `MAX` over the party rows, with `acrs_report_type` used only as the
  fallback for the 785 crashes that have no party rows at all; `severity_grain`
  records which rule fired.

---

## The drift detector, firing on historical data

Part 6 asks for the detector that would have caught the `driver_substance_abuse`
change, shown firing against the historical data. `--vocabulary-asof` reconstructs the
vocabulary as it stood on a date **from the data itself** — every value whose first
observed crash date is on or before it — and replays all history against it, so the
demonstration cannot be accused of having been rigged by choosing which tokens to
leave out.

```
$ python -m src.transform.drift --dataset mmzv-x632 \
      --column driver_substance_abuse --vocabulary-asof 2023-12-27

--- mmzv-x632.driver_substance_abuse replayed against the vocabulary as it stood on 2023-12-27 (12 tokens) ---
[DRIFT] mmzv-x632.driver_substance_abuse  rows=232411  accepted_vocabulary=12
  9 value(s) outside the accepted vocabulary, 60295 row(s):
  'Not Suspect of Alcohol Use, Not Suspect of Drug Use' rows=51181    crash_date_time 2023-12-28T12:59:00.000 .. 2026-09-02T14:54:00.000  :created_at 2024-06-12T18:59:45.528Z .. 2026-09-04T05:46:40.220Z
  'Unknown, Unknown'                                   rows=7038     crash_date_time 2023-12-28T12:59:00.000 .. 2026-09-02T12:30:00.000  :created_at 2024-06-12T18:59:45.528Z .. 2026-09-04T05:46:40.220Z
  'Suspect of Alcohol Use, Not Suspect of Drug Use'    rows=1464     crash_date_time 2024-01-01T00:30:00.000 .. 2026-08-30T21:20:00.000  :created_at 2024-06-12T18:59:45.528Z .. 2026-09-04T05:46:40.220Z
  'Suspect of Alcohol Use, Unknown'                    rows=179      crash_date_time 2024-01-01T07:25:00.000 .. 2026-08-06T22:55:00.000  :created_at 2024-06-12T18:59:45.528Z .. 2026-08-14T05:47:27.911Z
  'Unknown, Not Suspect of Drug Use'                   rows=146      crash_date_time 2024-01-06T22:55:00.000 .. 2026-08-26T15:24:00.000  :created_at 2024-06-12T18:59:45.528Z .. 2026-09-04T05:46:40.220Z
  'Suspect of Alcohol Use, Suspect of Drug Use'        rows=125      crash_date_time 2024-01-06T02:40:00.000 .. 2026-08-23T07:39:00.000  :created_at 2024-06-12T18:59:45.528Z .. 2026-09-04T05:46:40.220Z
  'Not Suspect of Alcohol Use, Suspect of Drug Use'    rows=77       crash_date_time 2024-01-25T13:20:00.000 .. 2026-08-03T13:56:00.000  :created_at 2024-06-12T18:59:45.528Z .. 2026-08-14T05:47:27.911Z
  'Not Suspect of Alcohol Use, Unknown'                rows=75       crash_date_time 2024-01-27T16:50:00.000 .. 2026-09-01T18:41:00.000  :created_at 2024-06-12T18:59:45.528Z .. 2026-09-04T05:46:40.220Z
  'Unknown, Suspect of Drug Use'                       rows=10       crash_date_time 2024-04-11T23:39:00.000 .. 2025-10-18T13:36:00.000  :created_at 2024-10-25T05:46:29.253Z .. 2026-02-20T06:46:29.300Z

$ echo $?
1
```

All nine new-generation tokens named, and the first dated to **2023-12-28**, four days
before the new year. Against the current vocabulary the same command is silent:

```
$ python -m src.transform.drift --dataset mmzv-x632 --column driver_substance_abuse

--- mmzv-x632.driver_substance_abuse against the current vocabulary ---
[ok] mmzv-x632.driver_substance_abuse  rows=232411  accepted_vocabulary=21

$ echo $?
0
```

The detector is generic over `(dataset, column)` because it has to be — the same
2024 generation change re-cased `injury_severity` in the same week, and a detector
special-cased to the substance column would have caught one and slept through the
other:

```
$ python -m src.transform.drift --dataset mmzv-x632 \
      --column injury_severity --vocabulary-asof 2023-12-27

--- mmzv-x632.injury_severity replayed against the vocabulary as it stood on 2023-12-27 (5 tokens) ---
[DRIFT] mmzv-x632.injury_severity  rows=232411  accepted_vocabulary=5
  5 value(s) outside the accepted vocabulary, 55677 row(s):
  'No Apparent Injury'                                 rows=46038    crash_date_time 2023-12-28T12:59:00.000 .. 2026-09-02T14:54:00.000  ...
  'Suspected Minor Injury'                             rows=5154     crash_date_time 2024-01-01T07:25:00.000 .. 2026-09-02T08:15:00.000  ...
  'Possible Injury'                                    rows=3931     crash_date_time 2024-01-01T17:32:00.000 .. 2026-09-01T13:30:00.000  ...
  'Suspected Serious Injury'                           rows=480      crash_date_time 2024-01-02T16:15:00.000 .. 2026-08-31T20:53:00.000  ...
  'Fatal Injury'                                       rows=74       crash_date_time 2024-02-29T15:03:00.000 .. 2026-08-04T05:25:00.000  ...
```

Same code path, same week, same first-seen date. Exit code 1 on drift so a scheduler
or CI job can act without parsing the text — this is the alert that should have
existed. In the build, drift **fails the build** unless `--allow-unmapped` is passed,
which writes the values as `UNMAPPED` and records a warning in the manifest.

---

## For DECISIONS.md

Each with the alternative I rejected.

- **DuckDB SQL in-process for everything except the grammar and the writer.**
  Rejected: pandas round trips (a 220k-row Python loop to answer a 21-row lookup), and
  Spark/Sedona — named explicitly as the escape hatch for national multi-year scale
  (50 states × 1975–2024, ~10⁸ rows), not used because the entire silver build over
  1.5M rows finishes in 26 s on one core and a cluster would add scheduling latency, a
  serialisation boundary and an operational surface to a workload that fits in cache.

- **Versioning = bronze snapshot partitions + hash-diff SCD2, one mechanism for TxDOT
  amendments and FARS reissues.** Rejected: separate handling per source (two code
  paths to keep correct for one event), and event sourcing (neither source publishes
  events; we would be inventing them from diffs anyway, so the diff is the honest
  primitive).

- **`row_hash` over the conformed attributes only.** Rejected: hashing the raw
  payload, which would make a re-serialisation with different key order look like a
  restatement — and Socrata does exactly that.

- **`valid_from` is a source stamp where one exists, else the bronze `load_ts` that
  first carried the hash.** Rejected: `now()` (destroys byte-identity on the first
  rebuild), and TxDOT's `report_date` (that is the report's filing date, a different
  fact, and pressing it into service as a mutation stamp would misdate every
  amendment).

- **Deletion inference only in snapshot mode, scoped to the range actually read.**
  Rejected: inferring deletion from absence in a delta partition (would delete most of
  Montgomery on every run), and scoping TxDOT to the cursor's `oid_ceiling` (would
  have deleted 2.99M crashes that were never requested — this one nearly shipped).

- **Defective rows are kept and flagged, never dropped.** Rejected: dropping
  out-of-envelope coordinates, which changes the crash count between layers and
  destroys the evidence that a report exists. A wrong coordinate is not a wrong crash.

- **Envelopes as config, each a strict superset of its jurisdiction polygon.**
  Rejected: hardcoding (buries a falsifiable claim in a code path nobody re-derives),
  and sizing the bbox to the polygon (would make bbox and Phase 4's PIP disagree, and
  force silver to re-open decisions gold already made).

- **The severity crosswalk is a CSV that raises on an unmapped value.** Rejected: a
  Python dict (not reviewable by a domain reader), and defaulting an unknown code to
  0 (hides a dictionary change as a data-quality statistic).

- **FARS `INJ_SEV` 5 → 0, not 2; 6 → 0, not 5.** Rejected: mapping "injured, severity
  unknown" to *possible injury*, which invents a severity the record does not assert;
  and mapping "died prior to crash" to *fatal*, which attributes a death to a crash
  that did not cause it. `severity_note` preserves both distinctions. NHTSA's own
  labels, extracted into `fars/codebook.parquet`, are the evidence.

- **Crash-level substance flags aggregated from Drivers, with the Incidents string as
  a cross-check only.** Rejected: parsing the Incidents string as the source of truth.
  It cannot count parties (see finding 3), and it is NULL for exactly the 785 crashes
  that have no drivers.

- **`silver.crash` is one row per crash per source, not entity-resolved.** Rejected:
  resolving here, which would make every downstream count a function of the match rule.

- **`crash_uid` is deterministic (`source_system:source_record_id`).** Rejected: a
  UUID, which breaks byte-identity on the first rebuild and makes a lead id unstable
  across runs — for a record carrying an eligibility decision that is a compliance
  problem, not an inconvenience.

- **pyarrow writer with an asserted-total sort key.** Rejected: DuckDB `COPY` (equally
  correct once the order is total, but its row-group boundaries are an internal
  heuristic rather than a contract), and trusting the ORDER BY without asserting it —
  which is the mistake the experiment caught.

- **Contracts hand-rolled against JSON Schema + `x-table-constraints`.** Rejected:
  Great Expectations and Soda, both of which do this well and bring a dependency tree,
  a config format and a results store for what is a `DESCRIBE` plus a dozen generated
  `COUNT` queries. The rules live in JSON either way; this way there is no new
  dependency and a violation reports a count with example keys.

- **Column order is part of the contract.** Rejected: order-insensitive validation.
  Determinism depends on the order, so a reordered contract must fail loudly rather
  than silently rewriting every file with new bytes and identical content.

- **Validate-then-write.** Rejected: writing then validating. A contract failure must
  leave the previous silver exactly as it was; a half-replaced directory is worse than
  a stale one because the stale one is at least internally consistent.

- **`row_count_min` is skipped for fixture-scale builds (`--small-corpus`).**
  Rejected: dropping the floor entirely (it is the check that catches a truncated or
  empty production build) and scaling it dynamically (a floor that moves with the data
  cannot detect the data shrinking).

- **Committed test extracts of real bronze rows, with two synthetic restatement rows.**
  Rejected: fully synthetic fixtures — they would test the transform against my idea of
  the data, and this phase found eight places where my idea of the data was wrong.

No new dependencies. `pyproj` was already in `requirements.txt` and is the only
addition to the transform's import surface beyond duckdb/pyarrow.

---

## Open items for later phases

**Phase 3 (modelling and entity resolution)**
- Conformed dimensions: date, time, geography, road class, weather, severity.
  Silver carries the raw codes and the harmonised ordinal; the dimension tables are
  not built.
- Cross-source entity resolution on top of `silver.crash`. The overlap is real and
  bounded: FARS carries 3,296–4,070 Texas fatalities and 36–46 Montgomery County
  fatalities per year, against TxDOT's 997 fatal crashes in the 100k slice and
  Montgomery's 403. A candidate-key blocking on (date, county, severity=5) is the
  obvious start.
- Surrogate keys. `src/transform/model.py` is still empty by design.
- The ~60 opaque TxDOT `*_id` columns, once the CRIS V29.0 lookups are extracted.
  `wthr_cond_id` and `light_cond_id` are already carried into silver for it.

**Phase 4 (geospatial)**
- Timezone localisation from the coordinate. Texas spans Central and Mountain (El Paso
  and Hudspeth), Florida spans Eastern and Central. `crash_datetime_local` is named for
  what it is so nothing downstream can mistake it for UTC.
- The polygon envelope refinement: TIGER point-in-polygon replaces bbox as the
  jurisdiction test. It only has to adjudicate what bbox accepted.
- GeoParquet 1.1.0 geometry column and `bbox` covering. Row groups are already sized
  at 122,880 for it.
- H3 r8/r9, census tract/block-group join, road snapping with the snap distance as a
  first-class quality attribute.

**Operational, unbuilt**
- **A Montgomery deletion census.** Deletes are invisible to keyset incremental
  ingest. A periodic `$select=:id` sweep (~125k ids) diffed against silver's current
  slice would close the gap; nothing in a delta transform can.
- **TxDOT beyond the 100k bounded slice.** Deletion detection needs two sweeps of the
  same OID range, and only one sweep exists. The mechanism is tested against the
  fixture's synthetic second partition.
- **A second FARS partition for a real year.** The restatement path is tested against
  a synthetic reissue; the real one arrives the next time NHTSA revises a file in
  place. Phase 1 measured that revision order does not follow year order (2021 carries
  a later Last-Modified than 2022–2024), which is why the diff is per-year and
  hash-driven.
- Orchestration wiring (Phase 8). `build_silver()` takes `sources`, `bronze_root` and
  `silver_root` as arguments precisely so a Dagster asset can call it per source
  without a subprocess.
