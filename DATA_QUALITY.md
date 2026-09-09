# Data quality findings

Counts below describe the committed local corpus. Run `python -m src.transform.report`
to reproduce the silver findings; geo and model counts are in the named manifests.
Rows are retained unless a section says otherwise: a bad attribute is not evidence that
the crash did not occur.

## 1. Montgomery coordinates that pass a null check

**What.** The incident feed has no null or zero coordinates, but some points are far
outside Montgomery County.

**How detected.** `tests/test_known_defects.py::test_coordinates_within_montgomery_envelope`
and `::test_silver_coordinates_within_montgomery_envelope`
and the report query count distinct incident rows outside the configured padded envelope.
The later polygon check is `data/gold/_geo_manifest.json:stats.bbox_vs_polygon`.

**What the pipeline does.** It keeps the crash and raw coordinates, nulls canonical
geometry, sets `OUT_OF_ENVELOPE`, and records geodesic distance. Spatial analysis excludes
the geometry; compliance fails closed. This preserves non-spatial counts and the evidence.

**Count.** 114 rows are outside the assignment box and 105 outside the padded operational
envelope; zero are null or zero. The farthest is 211.6 km away. Source: `python -m
src.transform.report`; the manifest confirms 105 unusable coordinates under
`stats.exclusions.excluded_by_reason.no_usable_coordinate`.

## 2. Two substance-code generations in one column

**What.** `driver_substance_abuse` mixes 12 uppercase, single-value tokens with nine
title-case alcohol/drug pairs. Commas delimit both the pair and crash-level lists, and
four related columns contain concatenated party values.

**How detected.** `tests/test_known_defects.py::test_substance_abuse_dictionary_normalised`
and `::test_silver_substance_abuse_dictionary_normalised`
profiles distinct values and runs every value through `parse_substance`; the generic drift
replay is captured in `output/drift_firing.log`.

**What the pipeline does.** A vocabulary grammar derives scheme, alcohol status, drug
status and detail while retaining the raw value. `N/A`, unknown spellings and SQL null
remain distinct. No naive comma split is used; unmapped future tokens block the build.

**Count.** 220,043 current driver rows contain 21 distinct values: 172,116 old-generation
and 47,927 new-generation, with zero unmapped. Incidents contain 121 distinct
concatenations. Source: `python -m src.transform.report`.

Historical replay quoted from `output/drift_firing.log`:

```text
driver_substance_abuse @ 2023-12-27: DRIFT, 9 values, 60,295 rows
injury_severity @ 2023-12-27: DRIFT, 5 values, 55,677 rows
```

## 3. The dictionary cutover overlaps

**What.** A date switch cannot identify the vocabulary generation.

**How detected.** `tests/test_known_defects.py::test_dictionary_cutover_overlap_handled`
groups deduplicated drivers by crash date and generation, then minimizes the errors for
every possible hard cutover date.

**What the pipeline does.** It classifies each value by grammar, not date. The source's
bulk-reload creation timestamp is not used as a proxy.

**Count.** Both generations occur from 2023-12-28 through 2024-01-03: 2 new/39 old on
the first day and 37 new/2 old on the last. Even the best date misclassifies four rows.
Source: `python -m src.transform.report`; first-seen drift is also in
`output/drift_firing.log`.

## 4. Incidents and Drivers disagree about the crash universe

**What.** An inner join would silently remove incident reports with no driver row.

**How detected.** `tests/test_known_defects.py::test_incidents_drivers_report_number_reconciliation`
and its `test_silver_…` twin
runs anti-joins on `report_number` in both directions.

**What the pipeline does.** Incidents define crash grain. Drivers and non-motorists join
to it from the many side; `has_driver_rows` records absence. The enforced foreign key is
party to crash, never crash to party.

**Count.** Incident-minus-driver is 785; driver-minus-incident is 0. Of the 785, 111 have
a non-motorist and 674 have no party row; 678 are property-damage, 106 injury and one
fatal. Source: `python -m src.transform.report`.

## 5. Driver grain fans out crash facts

**What.** Drivers are one row per driver and repeat crash attributes, so crash aggregation
from that table overcounts multi-driver reports.

**How detected.** `tests/test_known_defects.py::test_crash_fact_grain_is_one_row_per_crash`
compares row count with distinct `report_number`; contracts assert crash and party keys.

**What the pipeline does.** Crash facts come from Incidents. Party-derived flags use
explicit `MAX`, `BOOL_OR`, and `COUNT` rollups at crash grain.

**Count.** The raw fan-out is 1.871 rows per report and 1.7714 after deduplicating
overlapping bronze loads. Source: `python -m src.transform.report`.

## 6. TxDOT has two coordinate pairs

**What.** Officer-reported coordinates are much less complete than the CRIS-derived pair;
the pairs sometimes materially disagree.

**How detected.** The reproducible query printed by `python -m src.transform.report` groups
the four populated/null combinations and checks `located_fl`; it measures pair distance.

**What the pipeline does.** It prefers the derived pair, falls back to the officer pair,
otherwise leaves geometry null. `coord_source` and `coord_pairs_disagree` preserve the
choice and uncertainty.

**Count.** Of the 100,000-row bounded slice: 68,932 have derived-only coordinates,
22,944 both, 845 officer-only and 7,279 neither. Of rows with both, 1,065 differ by more
than 0.01 degrees. Source: `python -m src.transform.report`.

## 7. TxDOT amended reports restate history

**What.** `amend_supp_fl` makes append-only current-state logic incorrect.

**How detected.** `tests/test_known_defects.py::test_pipeline_is_idempotent_under_restatement`
builds twice, adds one amended bronze version, and verifies one history change. The report
profiles the flag.

**What the pipeline does.** Bronze is immutable; silver applies content-hash SCD2 on the
natural crash key. Identical content adds no version. Deletion inference is limited to
the OID range actually swept, avoiding invented deletions outside the bounded pull.

**Count.** 5,936 of 100,000 rows (5.9%) carry the amendment flag. Source: `python -m
src.transform.report`.

## 8. TxDOT string dates and opaque integer dictionaries

**What.** Three date/time fields are strings and most `*_id` fields have no label in the
public layer.

**How detected.** The ArcGIS schema profile and `verify_crash_sev_consistency()` compare
types and severity codes with injury counts; contract tests reject parse failures.

**What the pipeline does.** Dates are parsed with explicit types. Verifiable severity and
county codes are decoded; the other identifiers remain integers with the CRIS guide cited.
No label is guessed from frequency.

**Count.** Three string date/time columns have zero parse failures. There are 64 `*_id`
columns: four keys/decoded fields and 60 deliberately opaque dictionaries; severity code
95 occurs once and maps to unknown. Source: `python -m src.transform.report`.

## 9. FARS sentinel values are not null

**What.** Coordinates and coded fields use numeric fill values whose spelling changes by
year; ordinary null checks would create false geography and times.

**How detected.** `tests/test_known_defects.py::test_fars_sentinel_coordinates_excluded`
uses per-table, per-column numeric-tolerance rules from `config/fars_sentinels.csv`.

**What the pipeline does.** It maps sentinels before joins or aggregation, range-checks
coordinates, preserves raw values, and leaves dates usable when time is unknown. It does
not apply a blanket 7/8/9 rule because those values are substantive in other dictionaries.

**Count.** 873 accidents have a sentinel coordinate (68/170/122/214/129/170 for
2019–2024); `HOUR=99` occurs 1,668 times, `AGE` 998/999 occurs 14,498 times, and
`INJ_SEV=9` occurs 8,580 times. Source: `python -m src.transform.report`.

## Additional defects found

### Comma-joined lane counts

**What.** `number_of_lanes` contains values such as `2, 3`.

**How detected.** The transform report counts values that cannot be safely cast.

**What the pipeline does.** It retains `number_of_lanes_raw` and returns null for the
numeric value rather than inventing 2 or 23.

**Count.** 3,830 incident rows. Source: `python -m src.transform.report`.

### Duplicate non-motorist identifiers

**What.** Nine `person_id` values appear twice within the same report.

**How detected.** The current-slice unique-key contract and duplicate profile.

**What the pipeline does.** It deterministically keeps the greatest Socrata row ID and
sets `duplicate_person_id`; raw rows remain in bronze.

**Count.** 7,521 source row IDs become 7,512 party rows: nine duplicates. Source:
`data/gold/_build_manifest.json:outputs.fact_non_motorist` and the transform report.

### Crash-level and party-derived severity disagree

**What.** Montgomery's report label does not always agree with the most severe party.

**How detected.** The report compares `acrs_report_type` with party-level ordinal.

**What the pipeline does.** Party maximum is primary when parties exist; the report label
is fallback for the 785 crashes with none. `severity_grain` records the rule.

**Count.** 3,975 of 124,331 comparable crashes (3.2%). Source: `python -m
src.transform.report`.

### Texas county codes were mis-decoded

**What.** Two agencies alphabetise nine `Mc…` county names differently; the initial
formula assigned 1,336 Texas crashes to the wrong county.

**How detected.** Point-in-polygon county versus source county in
`data/gold/_geo_manifest.json:stats.county_refinement.disagreement_pairs`; the corrected
lookup is covered by geo tests.

**What the pipeline does.** It uses a reviewed county lookup and retains both source and
polygon counties so future disagreement remains visible.

**Count.** Nine affected county-code mappings and 1,336 rows. Source: Phase 4 report §2;
the manifest's broader post-fix comparison reports 1,798 source/polygon disagreements,
including legitimate border and source-location differences.

### Five coordinates are outside the United States

**What.** Five nominal Texas points resolve to Mexican or fixed-offset zones.

**How detected.** Coordinate-derived timezone results in
`data/gold/_geo_manifest.json:stats.timezone.unexpected_zones`.

**What the pipeline does.** It keeps and flags them; it never clamps or silently moves
coordinates. County fallback is used only when geometry is absent, not when it contradicts
the claimed jurisdiction.

**Count.** Five: two `America/Ciudad_Juarez`, two `America/Matamoros`, one `Etc/GMT+6`.
Source: the geo manifest field above.

### Spatial rates and statistical resolution

**What.** Per-capita rates are unstable where apportioned residential population is tiny;
permutation p-values also have a finite resolution and the library returned two values
above one.

**How detected.** `data/gold/analysis/_analysis_manifest.json:stats.cell_stats` and
`stats.gi_star`; analysis tests sweep the permutation count and clamp/count invalid p-values.

**What the pipeline does.** It leaves low-population rates undefined, publishes raw and
normalised analyses together, uses 99,999 permutations, applies FDR, and counts clamps.

**Count.** 228 cells contain crashes but fewer than 50 apportioned residents; 591 rates
are undefined. Two raw-count p-values were clamped from above one. Source: analysis
manifest fields above.

### Weather is smooth and effectively never null

**What.** ERA5 is reanalysis on a coarse grid, so availability is not evidence of local
observation and null-rate monitoring cannot detect weather bias.

**How detected.** `data/gold/_geo_manifest.json:stats.weather` reconciles scope and joins;
`stats.weather_precipitation_agreement` compares ERA5 with officer observations.

**What the pipeline does.** Weather is context only, carries join status and provenance,
and is never treated as crash cause. Station data was not substituted because its
missingness is geographically biased.

**Count.** 27,527 of 27,534 joinable scoped rows joined; seven lacked data. Wet/dry
agreement with known officer weather is 87.611%. Source: geo manifest fields above.

### Fixture geography is evidence, not production data

**What.** One synthetic point is outside the Montgomery envelope; a second fixture note
claims a roughly 400 m road distance that the current OSM extract does not reproduce.

**How detected.** Party enrichment and snapping fields in
`data/gold/compliance/_compliance_manifest.json:stats.enrichment`.

**What the pipeline does.** The outlier fails closed. The contradictory note does not
create a code: measured geometry wins. Party and crash snap thresholds are configured
separately because residences and crashes are different populations.

**Count.** One out-of-envelope fixture row; five of 28 MD party points exceed the 50 m
threshold; the disputed point measures 9.12 m. Source: compliance manifest and Phase 6
report §2–3.

## Sources deliberately not treated as defects

CRSS is not ingested. It is a weighted probability sample, not another crash census, and
cannot produce state estimates; unioning it with FARS or local records would corrupt both
counts and uncertainty. This is a scope decision, not missing data.

There is no Florida crash feed in scope. The FARS Florida rows have no report filing date,
so all 18,911 receive monitoring label `NOT_APPLICABLE` for the statutory-window sensor.
The trailing-30-day structural-incompleteness check still exists as the Dagster
`fl_structural_incompleteness` asset check and refuses an unannotated aggregate. Source:
`data/gold/compliance/_compliance_manifest.json:stats.fl_incompleteness`.
