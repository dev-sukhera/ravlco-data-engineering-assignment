# Phase 8 — Operability: implementation brief

You are implementing Phase 8 of the Crash-to-Contact take-home in this repo. Phases 0–7 are
complete and committed on `main` (Phase 7 = commits `451730c` … `a35bccf`: `src/scoring/`,
`config/scoring.toml`, `contracts/scoring.schema.json`, `SCORING.md`). Your job is
ASSIGNMENT.md §6 **Operability**, in full: an orchestrated dependency graph with retry
semantics and partition recovery, a documented idempotent backfill command **proven**
byte-identical, the schema-drift detector **shown firing** on the historical
`driver_substance_abuse` cutover, enforced data contracts at every layer boundary, GeoParquet
1.1.0 storage with the `bbox` covering column and justified file sizes, the right-sizing
answer (single node, with Spark/Sedona named as the unused escape hatch), and a monthly cost
estimate at 10× volume.

Most of the *mechanics* already exist from earlier phases. The graded gap is that they are
seven separate `python -m` commands with no graph, no retry policy, no partition model, no
single backfill entry point, and no committed proof. Phase 8 is the phase that turns them
into one operable system and **writes the evidence down**. Do not rebuild what works;
wrap it, prove it, and document it.

Read these before writing code, in this order:

1. `ASSIGNMENT.md` §6 in full (eight bullets; every one is a rubric line), §1's Montgomery
   incremental-ingestion bullet (late-arriving records, durable watermark — the backfill
   must respect it, not bypass it), §0, and §8's deliverable tree (`orchestration/` is a
   required directory).
2. `ai docs/implementation/phase6-compliance-report.md` §"Open items for Phases 7–8" — it
   names the asset boundaries (`vault → party_enrichment → leads → crash_only_decisions →
   exclusion_by_code`, with `decision_lineage` as an **append-only sink that no asset may
   materialise over**), says the FL-window monitor should be a sensor/asset check rather
   than a build stage, and states that the compliance byte-identity is already proven so the
   backfill "needs the CLI wrapped, not re-argued". `ai docs/implementation/phase7-scoring-report.md`
   §"Open items": `build_compliance()` and `build_scoring()` are import-safe and take
   explicit roots.
3. The seven existing entry points and their signatures — you wrap these, you do not
   duplicate their logic in the orchestration layer:
   - `src/ingest/montgomery.py` (keyset watermark on `(:updated_at, :id)`, `--since`,
     `--dataset`, `--reset`, `--db`), `src/ingest/txdot.py` (OID keyset, `--oid-min/max`,
     `--full`), `src/ingest/fars.py` (`--years`, `--check-only`, `--force`; SHA-256 per
     ZIP, changed hash ⇒ year restatement), `src/ingest/watermark.py` (`WatermarkStore`,
     bronze manifest).
   - `src/transform/build.py` (`--source`, `--bronze-root`, `--silver-root`,
     `--no-watermark-store`, validate-then-write against `contracts/silver.schema.json`),
     `src/transform/drift.py` (`observe`, `detect_drift`, `--vocabulary-asof`), and
     `src/transform/common.py` (`BuildManifest`, `write_parquet` with total-order
     enforcement, `discover_partitions`, `hash_files`, `connect`).
   - `src/geo/build.py` (`write_geoparquet`, `measure_row_groups`, hive partitioning
     `jurisdiction=XX/year=YYYY`), `src/analysis/build.py`, `src/compliance/build.py`
     (`--as-of`, `--gold-root`, `--out-root`, `--vault-dir`), `src/scoring/build.py`.
4. `src/contracts.py` — the hand-rolled validator (types via `DESCRIBE`, `x-table-constraints`
   with `unique_keys`, `foreign_keys`, `row_count_min`, `row_rules`) and the six contract
   files under `contracts/`. Audit **where each contract is actually enforced** (grep for
   `BRONZE_CONTRACT`, `SILVER_CONTRACT`, `GOLD`, `COMPLIANCE_CONTRACT`, `SCORING_CONTRACT`,
   `LEAD_OUTPUT_CONTRACT` across `src/`). If a contract file exists but no build calls it,
   that boundary is *documented*, not *enforced*, and §6 says enforced. Close every gap you
   find and list them in the report.
5. `ai docs/implementation/phase2-silver-report.md` §"The drift detector, firing on
   historical data" and `ai docs/implementation/phase4-geo-report.md` (the measured
   row-group table behind `config/geo.toml [geoparquet]`). Both requirements are
   *partially* met already: the detector fires from a CLI and the GeoParquet is 1.1.0 with
   `bbox`. Phase 8 adds the operational wiring (an asset check that fails the run) and the
   committed evidence.
6. `config/compliance.toml`'s header (the house style: every number carries its reason;
   the frozen clock `as_of = 2026-09-01` is why builds are byte-identical), `src/config.py`
   (accessors per config file — add `operability()` if you add a config file).
7. `tests/test_idempotency.py`, `tests/test_compliance.py`, `tests/test_scoring.py` — no
   mocks, no network; build-backed tests skip naming what is missing when `data/` is
   absent. Ingest tests use a real local HTTP server; follow that pattern if you test the
   ingest assets.

Environment: Python venv at `.venv`, **Python 3.14.7**; DuckDB 1.5.x (spatial), pandas 3,
pyarrow 25, pytest 9. **Neither Dagster nor Prefect is installed.** First action after
reading: `.venv/bin/pip install dagster dagster-webserver` and confirm it imports on 3.14.
If Dagster does not yet support 3.14, try Prefect; if neither installs cleanly, create a
second venv on Python 3.12 **for orchestration only** and document it in the README and the
report — do not downgrade the project venv, and do not silently pick an orchestrator by
what happened to install. Whatever you pin, add it to `requirements.txt` with a one-line
reason, under a `# Phase 8 orchestration` header, the way Phase 5 did. Baseline before
you touch anything: `.venv/bin/python -m pytest -q` → expected **445 passed, 4 xfailed**
(verify and record the real number). Full local data exists under `data/` (gitignored):
bronze for all three sources (FARS 2019–2024, Montgomery three datasets, TxDOT
`cris_crash` slice), silver SCD2 tables, gold (`fact_crash` 268,493 rows, dims, `crash_geo`
hive-partitioned), `data/gold/analysis`, `data/gold/compliance`, `data/gold/scoring`,
`data/vault`. Network is available for ingest assets but **no test may need it**.

`orchestration/dagster_defs.py` and `orchestration/backfill.py` exist as **zero-byte
untracked files** — they are yours, and this phase commits `orchestration/`. `notebooks/`
is untracked Phase 2 EDA material: leave it alone and do not commit it. Do not "fix" the
engine, the vault, the scorer, the transforms or the geo build along the way; if you find a
defect there, write it in the report. Never modify `fixtures/` or
`contracts/lead_output.schema.json`.

---

## 1. Scope

Build, in `orchestration/`:

| File | Responsibility |
|---|---|
| `dagster_defs.py` | The `Definitions` object: one asset per layer artefact, mirroring the phase DAG (`bronze_* → silver_* → gold_model → crash_geo → analysis → vault → leads → crash_only_decisions → exclusion_by_code → scoring_backtest → sample_leads`). Partitioned assets: FARS by **year** (`StaticPartitionsDefinition` 2019–2024, matching `config/sources.toml`), Montgomery by **daily load date**, TxDOT by **OID range** or as a single unpartitioned asset with the reason stated. `RetryPolicy` on every asset that touches the network (bounded attempts, exponential backoff — tenacity already handles HTTP-level retries inside ingest; the asset-level policy is for the run, and you say in a comment which failure each layer handles). Asset checks (§3). The FL-incompleteness sensor/asset check (§3). `decision_lineage` modelled as an external/observable asset, never materialised over. |
| `backfill.py` | `python -m orchestration.backfill --start YYYY-MM-DD --end YYYY-MM-DD [--layer silver|gold|geo|compliance|scoring|all] [--fars-years …] [--dry-run] [--prove]`: rebuilds the requested range from bronze **as it stands** (bronze is append-only and is never rewritten by a backfill), through every downstream layer, into the default roots or `--out-root`. `--prove` runs it twice into two temp roots and prints a hash table per artefact with a PASS/FAIL verdict. Importable (`backfill(start, end, …) -> BackfillResult`) so the test and the Dagster job share one code path. |
| `assets/` (optional package) | If `dagster_defs.py` exceeds ~400 lines, split by layer; keep `dagster_defs.py` as the single import the README names. |

And outside it:

- **Contracts at every boundary, enforced.** After the audit in "Read these" #4, every
  build validates its inputs' contract on read *or* its outputs' contract before write (the
  repo convention is validate-then-write; keep it). The boundaries are: source→bronze
  (`bronze.schema.json` — this is the one most likely to be unenforced; the ingest modules
  write raw pages verbatim and a parsed parquet, so the parsed parquet is what the contract
  governs), bronze→silver, silver→gold, gold→crash_geo, gold→analysis, gold→compliance,
  compliance→scoring, compliance→`output/sample_leads.csv` (`lead_output.schema.json`,
  row-by-row, already done in Phase 6 — verify). Each Dagster asset gets an
  `@asset_check` that re-runs the same validator, so a contract failure is visible in the
  UI as a failed check, not only as a raised exception in a log.
- **Drift detection as a gate.** An `@asset_check` on the Montgomery silver assets that
  calls `transform.drift.detect_drift` against the current vocabulary and **fails** on any
  unmapped token. Then the evidence: run the historical demonstration
  (`python -m src.transform.drift --dataset mmzv-x632 --column driver_substance_abuse
  --vocabulary-asof 2023-12-27`, and the same for `injury_severity`) and commit its
  output to `output/drift_firing.log` (or a section of `OPERABILITY.md` — pick one and say
  which) with the command, the date, and the row counts. Add a test that feeds the detector
  pre-cutover then cross-cutover values from the committed bronze test extracts and
  asserts it fires on the new-generation tokens and is silent on the old ones.
- **Partition recovery, demonstrated.** Show — in the report and in a test — that a failed
  FARS year (e.g. delete `data/silver/fars/…/2021` or corrupt one silver partition in a temp
  root) is recovered by materialising that one partition, and that every *other*
  partition's bytes are untouched afterwards (hash before, hash after, diff is exactly the
  recovered partition). Same for one `crash_geo` hive partition (`jurisdiction=MD/year=2024`).
- **GeoParquet, justified.** Do not rewrite `write_geoparquet`. Verify with pyarrow that
  every file under `data/gold/crash_geo/` carries `geo` metadata with `version: "1.1.0"`,
  `covering.bbox` naming the struct column, and `crs` as PROJJSON for EPSG:4326; assert it
  in a test. Then write the file-size justification: table of partition → rows → bytes →
  row groups, the measured pruning from Phase 4 (`measure_row_groups`), why 4,096-row groups
  and hive-by-jurisdiction/year, and what changes at 10× (this is where the cost estimate's
  storage line comes from).
- **Right-sizing.** One paragraph in `OPERABILITY.md`: DuckDB + GeoPandas on one node,
  measured wall-clock and peak RSS per layer on the current corpus (measure them — `/usr/bin/time -l`
  on macOS — and put the numbers in a table), the 10× projection, and Spark/Sedona **named
  as the escape hatch for national multi-year scale with the reason it was not used**. That
  sentence is explicitly worth points; write it to be quoted.
- **Cost at 10× daily.** A table in `OPERABILITY.md` (and a three-line summary in
  `README.md`): compute (one VM or one scheduled container, sized from the measured RSS ×
  10 with headroom), storage (bronze append-only growth/day × 30 + silver/gold at 10× the
  measured bytes), egress/API (Socrata, ArcGIS, Open-Meteo at 600/min / 10k/day — say
  where the daily cap binds), orchestration (Dagster OSS on the same node vs. Dagster+),
  and a total with the assumptions stated so a reviewer can change one number. Dated
  prices with the source URL; no invented precision.
- `OPERABILITY.md` (§7) and `ai docs/implementation/phase8-operability-report.md` (§7).
- `tests/test_operability.py` (§5).
- `README.md`: `orchestration/` layout line, quickstart additions (`dagster dev -f
  orchestration/dagster_defs.py`, the backfill command with `--prove`), and the output of
  `dagster asset list -f orchestration/dagster_defs.py` pasted verbatim as the graph
  evidence (a screenshot under `output/figures/` is welcome but the text listing is the
  thing that survives a clean checkout).

Not in scope, and say so in the report: a hosted Dagster deployment, alerting integrations
(Slack/PagerDuty), a metadata catalogue, dbt, Great Expectations/Soda (the hand-rolled
validator is the deliberate choice — `src/contracts.py`'s docstring already argues it),
any change to what the pipeline computes.

---

## 2. The partition model — decide it first, write it down

The assignment says "rebuild any date range and produce byte-identical output" and
"how a failed partition is recovered without a full rebuild". The three sources have three
different natural partitions and you must not pretend otherwise:

- **FARS** is partitioned by **year**; the bronze ZIP hash is the restatement signal. A
  year is the recovery unit at every layer.
- **Montgomery** bronze is **load-timestamp** partitioned with a keyset watermark; there is
  no crash-date partition in bronze by design (a crash-date watermark is the trap §1 warns
  about). A "date range backfill" therefore means: rebuild silver/gold/geo/compliance for
  crashes whose `crash_date` falls in the range, **from all bronze loads**, and the SCD2
  machinery in silver makes that idempotent. Say this explicitly; a reviewer who expects
  bronze to be re-fetched for a date range needs to read why that would be wrong.
- **TxDOT** bronze is OID-keyset; the amendment flag drives SCD2 versions. Same treatment
  as Montgomery for date-range rebuilds.
- **`crash_geo`** is hive-partitioned `jurisdiction/year` and is the cleanest demonstration
  of single-partition recovery downstream.
- **Compliance and scoring** are whole-corpus by construction (the fixture is 40 rows; the
  crash-only universe is evaluated as of the frozen clock). They are rebuilt in full and
  are cheap; byte identity there is already proven and you re-prove it inside the backfill.

Put the resulting partition table (layer × partition key × recovery unit × what the
backfill does) at the top of `OPERABILITY.md`. If you add a config file
(`config/operability.toml`: retry attempts/backoff, partition definitions, the measured
layer timings the cost estimate reads), every number carries its reason in a comment.

---

## 3. Checks and sensors

- One `@asset_check` per contract boundary (§1), reusing `src.contracts` — the check
  returns the violation list as metadata, never a bare boolean.
- The drift check on Montgomery silver (§1), **blocking**: downstream assets do not run
  on a drift failure.
- `fact_crash` grain check: exactly one row per crash (the contract's `unique_keys`
  already says so; the check makes it visible in the run).
- The FL-incompleteness monitor from `src/compliance/fl_incompleteness.py` as an asset
  check on any analysis/scoring asset that reports a trailing FL window — it should fail
  (or warn, with the reason) when the requested window overlaps the statutory 60-day gap.
- `decision_lineage`: an observable source asset whose check asserts the parquet is
  append-only — row count and the hash of the first N rows never decrease across
  observations (persist the last observation in the asset's metadata or a small JSON
  under `data/gold/compliance/`).
- The GeoParquet metadata check (§1) on `crash_geo`.

Retry semantics, stated per layer in a docstring on the definitions: network assets retry
(HTTP retries inside tenacity, run-level `RetryPolicy` outside); deterministic transforms
do **not** retry (a failure is a bug or a contract violation, and retrying a deterministic
function is noise); `decision_lineage` writes are never retried at the asset level because
the lineage store is content-addressed and a partial write is already idempotent — verify
that claim against `src/compliance/lineage.py` before you write it.

---

## 4. Determinism, idempotency — the proof

Same bar as Phases 6–7, now across the whole chain:

- `python -m orchestration.backfill --start 2024-01-01 --end 2024-03-31 --prove` runs the
  range twice into two temp roots and prints, per artefact, `sha256(run1) == sha256(run2)`.
  Every parquet and the CSV must match; every `_*_manifest.json` may differ **only** in
  `built_at`. The `_compliance_build_sha` and `_geo_build_sha` hash inputs, never time —
  do not change that.
- Then the harder one: run the backfill into the **live** roots and show that artefacts
  outside the range are byte-unchanged (hash `data/gold/crash_geo/jurisdiction=MD/year=2023`
  before and after a 2024 backfill). That is the "without a full rebuild" evidence.
- Both results go in the report with the actual hashes, and a test reproduces the first
  one on the committed bronze test extracts (`tests/fixtures/bronze/`) so it runs on a
  clean checkout.
- The Dagster job for the same range must call the **same** `backfill()` function; a test
  materialises the assets in-process (`materialize([...])`, no webserver) over the test
  extracts and compares hashes against the CLI run.

---

## 5. Tests (`tests/test_operability.py`) — real behaviour, no mocks, no network

Unit-level tests run on `tests/fixtures/bronze/` alone; build-backed tests skip naming what
is missing when `data/` is absent.

- Definitions load: `from orchestration.dagster_defs import defs` succeeds, every asset
  has a non-empty description, every network asset has a `RetryPolicy`, every
  contract-bearing asset has at least one check, and the asset graph is acyclic and
  matches the expected layer order (assert the upstream set of `leads` includes
  `crash_geo` and `vault`, etc.).
- Partition definitions: FARS partition keys equal `config.sources()["fars"]["years"]`
  as strings; Montgomery partitions are daily.
- Backfill byte identity on the test extracts (two runs into `tmp_path`, hash table
  equal, manifests differ only in `built_at`).
- Backfill locality: after backfilling one range, artefacts outside the range are
  unchanged (hash before/after).
- Partition recovery: corrupt one FARS silver year in a temp root, materialise that one
  partition, assert only that partition's hash changed and it now equals the original.
- Drift: pre-cutover vocabulary + cross-cutover values from the extracts ⇒ fires with the
  new-generation tokens named; current vocabulary ⇒ silent. Also assert the Dagster check
  returns `passed=False` on the drifted input.
- Contracts: every boundary's asset check passes on the test-extract build; injecting one
  out-of-range value (e.g. a `severity_ordinal = 9`) into a temp gold table makes the
  gold check fail with that column named in metadata.
- GeoParquet: every `crash_geo` file has `geo` metadata `version == "1.1.0"`,
  `covering.bbox`, and a 4326 CRS; the `bbox` struct's per-row values bound the geometry.
- Lineage append-only check: appending rows passes; a truncated copy fails.
- In-process Dagster materialisation over the extracts equals the CLI backfill's hashes.
- No wall clock in any artefact bytes: grep `orchestration/` for `date.today` /
  `datetime.now` outside manifest fields.

---

## 6. Conventions

- Match Phases 1–7: module docstrings explain **why**; every number in config carries its
  reason; numbers in prose are measured, dated and reproducible by a named command.
- Commit as you go, **one-line conventional commits, no body, no co-author trailer, never
  mentioning an AI tool**: `feat(orchestration): …`, `feat(contracts): …`,
  `test(operability): …`, `chore(config): …`, `chore(output): …`, `docs(operability): …`.
  Commit only files you fill in this phase. Never commit `data/`, `config/settings.toml`,
  `*.parquet` outside `tests/fixtures/`, `ai docs/`, `IMPLEMENTATION_GUIDE.md`,
  `notebooks/`, a `.dagster/` home directory, or `tmp*` roots. `orchestration/`,
  `OPERABILITY.md`, `output/drift_firing.log` (if you choose it) **are** committed. Add
  `.dagster/` and `dagster_home/` to `.gitignore` and set `DAGSTER_HOME` to a gitignored
  path in the README instructions.
- Logs carry counts and hashes, never a name, street, E.164 number or party coordinate.
  `orchestration/` never imports the vault directly; it calls `build_compliance()`.
- `grep -rn 3857 orchestration` must be empty; orchestration performs no geometry operation.
- The orchestration layer contains **no business logic**: if you find yourself writing a
  SQL string or a transform inside an asset body, it belongs in `src/` and the asset calls
  it. A reviewer must be able to run every layer without Dagster installed.
- When this brief and the data disagree, measure, choose, and put the disagreement in the
  report and `OPERABILITY.md`.

---

## 7. Deliverable: the prose

**`OPERABILITY.md`** (≈1,200–1,800 words, business-readable in the way `SCORING.md` and
`COMPLIANCE.md` are), in this order:

1. The partition table (§2) and one paragraph on what "backfill a date range" means per
   source and why bronze is never re-fetched by a backfill.
2. The asset graph: the `dagster asset list` output, retry semantics per layer, and the
   partition-recovery procedure with the before/after hashes.
3. The backfill command, the `--prove` output pasted verbatim, and the locality proof.
4. Contracts: the boundary table (boundary → contract file → where enforced → what it
   checks: types, nullability, ranges, keys, row rules) — including any boundary you found
   unenforced and closed.
5. Drift: what the detector does, the historical firing output, and how it now gates the
   run.
6. Storage: GeoParquet 1.1.0, `bbox`, partitioning, the measured row-group pruning and
   the file-size justification, and the storage growth line.
7. Right-sizing: the measured timing/RSS table, the 10× projection, the Spark/Sedona
   escape-hatch sentence, and the cost table with dated, sourced prices.
8. Decisions and rejected alternatives (Prefect/Airflow, Great Expectations/Soda, dbt,
   re-fetching bronze on backfill, retrying deterministic assets, PostGIS) in the
   timestamped form Phase 9 lifts into `DECISIONS.md`.

**`ai docs/implementation/phase8-operability-report.md`** in the shape of the Phase 7
report: what you built (file table), where the brief and the repo disagreed (the
orchestrator install on 3.14, any unenforced contract boundary you found, anything the
partition model could not honour), what you bounded and why, bugs the tests caught,
verification (pytest count before/after, the `--prove` hashes, the locality hashes, the
partition-recovery hashes, the drift firing output), open items for Phase 9.

Update `README.md` per §1. Do not fill in `MEMO.md`, `DECISIONS.md` or `AI_USE.md` — those
are Phase 9 — but leave `OPERABILITY.md` §7 (cost + right-sizing) and §8 (decisions)
ready to lift.

---

## 8. Definition of done

- [ ] `orchestration/dagster_defs.py` loads; the asset graph mirrors bronze → silver →
      gold → geo → analysis → compliance → scoring → output with partitions on FARS years
      and Montgomery load dates, `RetryPolicy` on network assets, and asset checks for
      every contract boundary, drift, grain, GeoParquet metadata, FL window, and
      lineage append-only.
- [ ] `python -m orchestration.backfill … --prove` prints an all-PASS hash table; a test
      reproduces it on the committed extracts; the locality and partition-recovery proofs
      are in the report with real hashes.
- [ ] Every contract boundary is enforced in a build **and** surfaced as an asset check;
      the audit of previously unenforced boundaries is in the report.
- [ ] The drift detector's historical firing is committed as evidence and it gates the
      Montgomery silver asset; a test proves both directions.
- [ ] Every `crash_geo` file is GeoParquet 1.1.0 with `covering.bbox`; the file-size
      justification is written with measured numbers.
- [ ] `OPERABILITY.md` carries the timing/RSS table, the 10× cost table with dated sources,
      and the Spark/Sedona escape-hatch paragraph.
- [ ] `README.md` quickstart runs from a clean checkout: install, `dagster dev`, backfill,
      and the asset list are all pasted from real runs.
- [ ] Full suite green (baseline count + your new tests), xfails unchanged; no test needs
      the network or `data/`.
- [ ] Commit history for this phase reads as a story: deps → config → assets → checks →
      backfill → contracts audit → drift gate → tests → evidence → docs.
