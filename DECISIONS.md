# Decision log

Dates are the dates of the commits that first made each choice executable. Planning notes
are not treated as decisions. Evidence points to a commit, test, or manifest field.

### 2026-09-07 — Use an in-process analytical stack

**Decision.** Python, DuckDB, GeoPandas and Shapely run on one node.
**Rejected.** PostGIS for a serving tier; a distributed engine for this batch corpus.
**Why.** The workload fits memory and has no concurrent-query requirement. **Spark/Sedona
is the escape hatch for national, multi-year scale when partitions must execute across
machines; it was not used because 268k rows—and even the projected 2.68m—fit comfortably
in one node and distributed shuffle/operations would cost more than the work.**
**Evidence.** `b1e8a1d`; `OPERABILITY.md` §Right-sizing.

### 2026-09-07 — Watermark Montgomery mutations, not crash dates

**Decision.** Persist the expanded `(:updated_at, :id)` keyset cursor in DuckDB, using a
fresh connection per operation.
**Rejected.** Crash-date watermarks, offset-only pagination, and a shared long-lived
connection.
**Why.** Late rows can describe old crashes, offsets move during writes, and one
connection would serialize parallel assets. The service rejects tuple syntax, so the
predicate is an equivalent disjunction.
**Evidence.** `0a272ae`; `tests/test_idempotency.py`.

### 2026-09-07 — Write bronze before advancing its cursor

**Decision.** Durably replace data, then advance the watermark.
**Rejected.** Advance-first ordering.
**Why.** A retry may create a visible duplicate page, which silver can absorb; advancing
first can silently skip rows.
**Evidence.** `d829cda`; ingestion idempotency tests.

### 2026-09-07 — Snapshot TxDOT OIDs and page by the discovered key

**Decision.** Read the service descriptor, use `ESRI_OID` keyset ranges, and scope cursor
state to one sweep.
**Rejected.** Hard-coded `OBJECTID`, `resultOffset`, an unbounded ID response, or carrying
the last OID across restatement sweeps.
**Why.** The live service differs from its label and times out on the unbounded request;
stable indexed ranges survive a moving table.
**Evidence.** `bac0bdd`, `1fe934c`; TxDOT ingest tests.

### 2026-09-07 — Bound the demonstrated TxDOT pull

**Decision.** Commit evidence from 100,000 rows and retain an explicit `--full` path.
**Rejected.** Spending the take-home window on all 3,088,450 rows or disguising the slice
as a census.
**Why.** The measured full pull is about 1,545 pages, 4.6 hours and 13 GB; the bounded flag
travels with the cursor.
**Evidence.** `bac0bdd`; Phase 1 report §What I bounded.

### 2026-09-07 — Preserve source bytes and source coordinates in bronze

**Decision.** Gzip TxDOT pages while hashing uncompressed bytes; retain native WKID 3081
and versioned FARS ZIPs.
**Rejected.** Reprojecting or normalizing raw data, and hashing only compressed output.
**Why.** Bronze must prove the received payload; reprojection is a reversible transform.
**Evidence.** `bac0bdd`, `49d2486`; raw-payload tests.

### 2026-09-07 — Use snapshot-plus-hash SCD2 in silver

**Decision.** One content-diff mechanism handles TxDOT amendments and FARS reissues.
**Rejected.** Append-only current state, per-source versioning logic, and invented events.
**Why.** Neither source publishes mutation events; unchanged re-runs should add no version.
**Evidence.** `6cd1874`, `1cf6b71`; `test_pipeline_is_idempotent_under_restatement`.

### 2026-09-07 — Hash conformed attributes and require total output order

**Decision.** Exclude load metadata from row hashes and make the writer refuse a
non-total sort key.
**Rejected.** Raw-payload hashes, UUIDs, wall-clock version stamps, and trusting a partial
`ORDER BY`.
**Why.** Serialization and thread order are not business restatements.
**Evidence.** `d33478b`; `test_writer_refuses_a_non_total_sort_key`.

### 2026-09-07 — Keep defective rows and quarantine bad attributes

**Decision.** Preserve crashes with suspect coordinates or codes, flag the attribute,
and fail downstream claims closed.
**Rejected.** Dropping the whole crash or silently repairing it.
**Why.** A wrong location is not proof of a nonexistent crash; deletion breaks layer
reconciliation.
**Evidence.** `6cd1874`; known-defect coordinate test.

### 2026-09-07 — Prefer CRIS-derived coordinates

**Decision.** Derived pair, then officer pair, then null; record source and disagreement.
**Rejected.** Officer-first and `located_fl` without inspecting coordinates.
**Why.** The derived pair is agency-processed and supplies 68,932 rows the officer pair
lacks; officer coordinates still rescue 845 rows.
**Evidence.** `6cd1874`; `python -m src.transform.report`.

### 2026-09-07 — Use reviewed data crosswalks and fail on unknown values

**Decision.** Severity and conformed vocabularies live in CSV, preserve lossiness, and
stop on unmapped tokens.
**Rejected.** Python dictionaries, frequency-guessed labels, and defaulting unknown to
zero.
**Why.** A new code is schema drift, not automatically “no injury.”
**Evidence.** `f50cf68`, `c331c43`; `output/drift_firing.log`.

### 2026-09-08 — Keep history in silver and resolve one gold crash

**Decision.** Gold is a pure current-state function with one primary crash and a bridge
to every source record.
**Rejected.** Duplicate SCD2 histories, one row per source with `same_as`, or collapsing
without evidence.
**Why.** One history avoids disagreement; the bridge retains provenance and match facts.
**Evidence.** `4789bf9`; `data/gold/_build_manifest.json:stats.entity_resolution`.

### 2026-09-08 — Use stable hashed surrogate keys and local-source precedence

**Decision.** Derive a 60-bit SHA-256 key from the primary crash UID; prefer Montgomery,
then TxDOT, then FARS.
**Rejected.** Row numbers, random UUIDs, and FARS-primary records.
**Why.** Inserts must not renumber facts, and a later local match must preserve amendment
lineage.
**Evidence.** `4789bf9`; model key-stability tests.

### 2026-09-08 — Resolve only genuine cross-source overlap

**Decision.** Match FARS fatalities to Maryland/Texas local fatal candidates using
county blocking, geodesic distance and wall-clock time tiers.
**Rejected.** All-pairs matching, street-name similarity, a learned score, or asserting
all sources are disjoint.
**Why.** MD and TX do not overlap each other; FARS overlaps both, with 1,157 measured
matches. Twenty-four nearby nonfatal signals are reported but not admitted.
**Evidence.** `e200f6a`; `data/gold/_build_manifest.json:stats.entity_resolution`.

### 2026-09-08 — Keep passengers outside the requested party model

**Decision.** Count but do not create a one-source passenger fact.
**Rejected.** Folding passengers into drivers or adding another fact outside scope.
**Why.** Either alternative changes grain or expands the assignment without improving
the required cross-source model.
**Evidence.** `4789bf9`; Phase 3 report records 31,785 excluded FARS persons.

### 2026-09-08 — Separate point enrichment from geography dimensions

**Decision.** Add one `crash_geo` row per crash and derive tract from block group.
**Rejected.** Widening county-grain geography, mutating the core fact, or duplicating a
tract dimension.
**Why.** Point attributes have different grain and rebuild from separately hashed inputs.
**Evidence.** `72dff3d`, `b5188ed`; geo reconciliation manifest.

### 2026-09-08 — Use boundary-safe polygon assignment and national counties

**Decision.** `intersects`, smallest-GEOID tie-break, and the national county layer.
**Rejected.** `within`, unresolved ties, and filtering polygons to the three states.
**Why.** Shared-edge points otherwise disappear or duplicate; the three-state filter
misclassified DC/Virginia points as ocean.
**Evidence.** `72dff3d`; `stats.county_refinement` in the geo manifest.

### 2026-09-08 — Derive timezone from coordinates with explicit DST policy

**Decision.** Localize naive wall time using coordinate-derived IANA zones; shift spring
gaps forward, choose `fold=0` in fall ambiguity, and flag both.
**Rejected.** State/area-code timezone, conversion of naive time, `fold=1`, or silent
library defaults.
**Why.** Texas and Florida span zones; the flags preserve 61 uncertain records without
pretending certainty.
**Evidence.** `2142c90`; `data/gold/_geo_manifest.json:stats.timezone`.

### 2026-09-08 — Snap locally at a measured 50-metre threshold

**Decision.** Maryland crash snapping uses its local metric projection, excludes paths,
stores distance and linear offset, and rejects beyond 50 m.
**Rejected.** Web-map distance, 35 m, 400 m, footpaths, and Overpass at scale.
**Why.** The measured distribution supports 50 m; a car should not prefer a footpath.
**Evidence.** `dfc66e3`; `data/gold/_geo_manifest.json:stats.snap`.

### 2026-09-08 — Split party and crash snap policies

**Decision.** Party enrichment has an independently named threshold.
**Rejected.** Reusing the crash threshold implicitly.
**Why.** Residences and roadway crashes are different populations even when today’s
numeric thresholds happen to match.
**Evidence.** `43c329e`; compliance manifest `stats.enrichment.snap`.

### 2026-09-08 — Use population only from ACS

**Decision.** Load population through the official keyless Summary File fallback and use
it only as an aggregate denominator.
**Rejected.** Failing without an API key, loading income/tenure/vehicle fields “just in
case,” or using them for priority.
**Why.** Population supports comparable rates; the other fields are protected-class
proxies and should never cross the scoring boundary.
**Evidence.** `686c77f`, `451730c`; scoring denylist tests.

### 2026-09-08 — Use ERA5 as bounded contextual weather

**Decision.** Request one H3-cell/year series for Montgomery 2024 onward.
**Rejected.** One request per crash, full history, and GHCNh station substitution.
**Why.** Reanalysis is complete but coarse; stations are point-accurate and unevenly
missing. Weather remains context, not cause.
**Evidence.** `dfc66e3`; geo manifest `stats.weather`.

### 2026-09-08 — Use H3 r8 and honest spatial validation

**Decision.** Analyze Montgomery 2019–2025 at r8, with area-apportioned population,
hex-neighbor weights, explicit two-sided tests, 99,999 permutations, and BH FDR.
**Rejected.** R9, block-group/native grain, random folds, 999 permutations, and
Bonferroni as the primary operational correction.
**Why.** The chosen grain balances sparsity; 999 permutations cannot resolve the needed
FDR threshold, and random folds leaked enough to change KDE bandwidth 10.7×.
**Evidence.** `cbfccaf`, `ca3a769`; analysis manifest.

### 2026-09-08 — Recommend the interpretable KDE surface

**Decision.** Report all candidates and recommend 1,261 m while using spatial CV’s
2,947 m result as a resolution bound.
**Rejected.** Treating the CV likelihood optimum or a 500 m practitioner map as the sole
truth.
**Why.** Held-out likelihood and operational corridor interpretation are different
objectives.
**Evidence.** `ca3a769`; `ANALYSIS.md` §KDE.

### 2026-09-08 — Default contact decisions to ineligible

**Decision.** Run every gate, accumulate all codes, and require an affirmative basis.
Only dated holds produce `BLOCKED_UNTIL`; bars and refresh holds are ineligible.
**Rejected.** First-failure short-circuiting, null-date blocks, and permissive defaults.
**Why.** A partial reason set hides remediation and makes exclusion counts false.
**Evidence.** `23969d6`; engine precedence tests.

### 2026-09-08 — Treat Maryland as a channel bar

**Decision.** Read Md. Gen. Prov. §4-320 as reaching this use of the police feed and name
the channel rules in a null-window row.
**Rejected.** Restricting §4-320 to the MVA custodian, inventing a wait period, or leaving
the row unexplained.
**Why.** This is the conservative legal judgment; the narrower reading still leaves Md.
Rule 19-307.3 barring a lawyer’s live call.
**Evidence.** `a3f6365`, `b85dd38`; `COMPLIANCE.md` §Source eligibility.

### 2026-09-08 — Use exclusive blackout arithmetic

**Decision.** Open on anchor date plus days plus one.
**Rejected.** Inclusive boundary-day arithmetic.
**Why.** The statutes do not resolve the boundary and the downside includes criminal
exposure.
**Evidence.** `1b3fbcf`; blackout boundary tests.

### 2026-09-08 — Make identity provenance explicit

**Decision.** Provenance is an input, never inferred from the crash source; the fixture
acts consumer-direct only when valid consent exists.
**Rejected.** Source-name inference, treating fixtures as production, or barring the
whole test harness so channel paths never execute.
**Why.** DPPA turns on how identity was obtained, and no production identity join exists.
**Evidence.** `ec74a5c`, `33b9e08`; crash-only manifest.

### 2026-09-08 — Tokenize with HMAC and content-address lineage

**Decision.** Keep direct identifiers in a logged vault, expose ZIP5 at the statutory
boundary, and derive immutable decision IDs from content.
**Rejected.** Bare hashes, random UUID mappings, sequential lineage IDs, and rebuild-time
timestamps.
**Why.** Small phone spaces make bare hashes reversible; deterministic IDs make re-runs
auditable and byte-identical.
**Evidence.** `a27d883`, `69c4c91`; vault and lineage tests.

### 2026-09-08 — Correct two mid-build compliance failures

**Decision.** Quote YAML `"YES"` for the RND value and exempt valid consent from the
catch-all live-solicitation rule.
**Rejected.** Keeping YAML’s boolean coercion or an unconditional catch-all bar.
**Why.** The former let reassigned numbers pass; the latter made valid consent unable to
authorize the test path.
**Evidence.** `1fb5f13`; `test_p014_reassigned_number_loses_the_safe_harbour`.

### 2026-09-08 — Evaluate the complete crash corpus

**Decision.** Run all 268,493 crash records with no identity layer.
**Rejected.** The planned 2,000-row stratified sample.
**Why.** The full run took under a minute and gives exact jurisdiction counts.
**Evidence.** `43c329e`; compliance manifest `stats.crash_only`.

### 2026-09-08 — Score only admitted records with an a-priori rule

**Decision.** Use six named crash-context contributions; missing facts stay unavailable;
ineligible records receive no score.
**Rejected.** A fitted model, imputation, nearest-crash substitution, socioeconomic or
routing features, and tuning weights on the holdout.
**Why.** The available outcome cannot justify an opaque model, and scoring cannot cure a
legal bar.
**Evidence.** `c05d687`, `26cfafa`, `01ddeb1`; scoring manifest and tests.

### 2026-09-09 — Orchestrate assets with Dagster OSS

**Decision.** Model source, transform, quality, lineage and output boundaries as assets;
retry only network work.
**Rejected.** Airflow/Prefect, hosted orchestration now, and retrying deterministic
contract failures.
**Why.** Asset checks map directly to the medallion boundaries; retry cannot cure a
repeatable bad schema.
**Evidence.** `ea37f84`; `tests/test_operability.py`.

### 2026-09-09 — Rebuild from immutable bronze during backfill

**Decision.** Date ranges select the operational request, while silver/gold re-resolve
all immutable bronze and compliance/scoring rebuild their whole corpus.
**Rejected.** Refetching by crash date or inventing partitioned business logic absent
from the builders.
**Why.** Crash-date acquisition loses late records and amendments.
**Evidence.** `ea37f84`; `python -m orchestration.backfill ... --prove`.

### 2026-09-09 — Enforce contracts at every durable boundary

**Decision.** Retain the JSON-Schema-derived validator and add parsed-bronze enforcement.
**Rejected.** Great Expectations, Soda, dbt, and merely documenting bronze validation.
**Why.** Existing DuckDB checks cover types, keys, ranges and row rules without another
stateful framework.
**Evidence.** `f5bd50b`; operability contract tests.

### 2026-09-09 — Use dual GeoParquet layouts

**Decision.** Keep a flat canonical scan and state/year partitions, ordered by H3 with
4,096-row groups and covering boxes.
**Rejected.** Flat-only, partition-only, 122,880-row partition groups, or Hilbert order.
**Why.** Whole-corpus analysis and localized spatial reads have different access paths;
H3 ordering aligns with analysis grouping.
**Evidence.** `25fddc8`; `OPERABILITY.md` §GeoParquet storage.

### 2026-09-09 — Keep operational integrations bounded

**Decision.** Co-locate Dagster OSS and report retention rather than deleting data.
**Rejected.** Hosted Dagster, Slack/PagerDuty, a metadata catalog, dbt, concurrent
PostGIS serving, and a deletion function without backup propagation.
**Why.** Those are production operations requiring owners, credentials, and rollback;
they do not improve this reproducible local defense.
**Evidence.** `8f6a367`; `OPERABILITY.md`.

### 2026-09-09 — Disclose planning artifacts in the repository

**Decision.** Commit `IMPLEMENTATION_GUIDE.md`, `ai docs/`, and the exploratory notebook;
describe them in `AI_USE.md`.
**Rejected.** Restore their ignore rules and refer to unreviewable local notes.
**Why.** The assignment explicitly rewards disclosure, and the artifacts provide a
phase-by-phase record a reviewer can audit.
**Evidence.** Phase 9 documentation commit and `.gitignore` diff.
