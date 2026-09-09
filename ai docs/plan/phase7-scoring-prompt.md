# Phase 7 — Lead scoring: implementation brief

You are implementing Phase 7 of the Crash-to-Contact take-home in this repo. Phases 0–6
are complete and committed on `feature/dimensional-modeling` (Phase 6 = commits
`ec74a5c` … `5753a45`: `src/compliance/`, `COMPLIANCE.md`, `contracts/compliance.schema.json`,
`output/sample_leads.csv`). Your job is ASSIGNMENT.md §4: **a ranked, explainable priority
score per eligible record**, where every score decomposes into named contributions, every
feature carries provenance back to its source field, and whatever the score claims to do is
backed by a documented, reproducible backtest. The graded constraint is the ACS one: a
submission that ranks leads by neighbourhood income, or by anything correlated with it, "has
told us a great deal". The score must be provably free of that, and the prose must address
the question head-on rather than avoid it.

Read these before writing code, in this order:

1. `ASSIGNMENT.md` §4 in full (it is short; every sentence is a rubric line), §3c's
   "Spatial cross-validation" row (random splits leak across spatially autocorrelated
   neighbours — block by county or H3 cell), §0, and §7 (the memo must answer the ACS /
   protected-proxy question; you write the paragraph now so Phase 9 lifts it).
2. `src/compliance/leads.py` — `lead_row()` emits `priority_score = None` and
   `score_components = None` (comment says "Phase 7"); `score_inputs()` is the hook Phase 6
   left you: **ELIGIBLE and BLOCKED_UNTIL rows only** — an INELIGIBLE record is not a lead
   with a low score, it is not a lead. Keep that rule. Read `src/compliance/build.py`'s
   docstring (stage order, validate-before-write, determinism, `_compliance_build_sha` hashes
   inputs never time) — the scorer slots into that pipeline and must not break any of it.
3. `contracts/lead_output.schema.json` (**do not modify**): `priority_score` is
   `number|null`, `score_components` is `object|null` — "Named contributions. No opaque
   blob." `contracts/compliance.schema.json` table `compliance.leads` already types both
   columns; you add row rules there.
4. `ai docs/implementation/phase6-compliance-report.md` §"Open items for Phases 7–8" and
   §"What I bounded and why"; `ai docs/implementation/phase5-analysis-report.md` for the
   H3/county blocking and the population-normalisation work — the **legitimate** ACS use
   already in the repo, which you must keep separate from prioritisation and name as such.
5. `src/config.py` (`model()`, `compliance()`, `geo()` accessors — add `scoring()`),
   `src/transform/common.py` (`write_parquet` with a total sort order, `BuildManifest`,
   `connect`, `hash_files`), `src/analysis/build.py` (the CLI + manifest + input-sha pattern
   you copy for the backtest), `src/contracts.py` (`validate`, `row_rules`),
   `config/model.toml` and `config/compliance.toml` (the house style for a config block:
   every number carries the measurement or the reason).
6. `tests/test_compliance.py`, `tests/test_fixture_golden.py`, `tests/test_analysis.py` —
   no mocks, no network, build-backed tests skip (naming what is missing) when `data/gold/`
   is absent.

Environment: Python venv at `.venv`; DuckDB 1.5.5 (spatial), pandas 3.0, pyarrow 25,
scikit-learn, scipy, esda/libpysal (Phase 5), pytest 9. **Add nothing to
`requirements.txt` unless a one-line pinned reason accompanies it** — a rule-based additive
score needs none of it beyond numpy/pandas/duckdb. Baseline before you touch anything:
`.venv/bin/python -m pytest -q` → expected **433 passed, 4 xfailed** (verify and record the
real number in your report). Full local gold exists under `data/gold/` (gitignored):
`fact_crash` 268,493 rows (Montgomery 125,005 / TxDOT 100,000 slice / FARS 43,488,
2019–2024), `crash_geo` with H3 r7/r8/r9, snapped OSM attributes for Maryland, ERA5 weather
for a 27,527-row slice; `data/gold/compliance/` with `leads.parquet` (40 rows: 21 ELIGIBLE,
4 BLOCKED_UNTIL, 15 INELIGIBLE), `crash_only_decisions.parquet` (268,493 rows, **every one
INELIGIBLE** — that is the honest Phase 6 answer and you do not change it), lineage and the
exclusion table.

`src/scoring/score.py` and `src/scoring/backtest.py` exist as **zero-byte untracked files**.
Phase 6's brief said "never commit `src/scoring/`" because it was not that phase's work; that
instruction is lifted for this phase and this directory only. `orchestration/`,
`notebooks/`, `audit/` and `REQUIREMENTS_AUDIT.md` are also untracked; **leave them alone
and do not commit them** — they are Phase 8 and review material. `REQUIREMENTS_AUDIT.md`
item A01 is this phase's acceptance criteria and is worth reading; its other items
(A02–A16) are not your scope. Do not "fix" the eligibility engine, the vault, or the
consent checks along the way; if you find a defect there, write it in the report.

---

## 1. Scope

Build, in `src/scoring/`:

| File | Responsibility |
|---|---|
| `score.py` | The additive score: `score_lead(inputs: Mapping) -> Score` returning `priority_score` and a `score_components` dict; `score_leads(rows)` over `leads.score_inputs()`; pure, no I/O, no wall clock (`as_of` is passed in). |
| `features.py` | One function per feature. Each returns a `Feature(name, value, source_field, source_table, transform, status)` — the provenance the contract demands lives here, not in prose. |
| `backtest.py` | The reproducible backtest over gold crashes: temporal holdout + spatially blocked folds + the random-split negative control. Writes parquet and a manifest. |
| `build.py` | `python -m src.scoring.build` — runs the backtest, validates against `contracts/scoring.schema.json`, writes `data/gold/scoring/`, prints the tables that go into `SCORING.md`. Importable and side-effect-free at import (Phase 8 wraps it as a Dagster asset). |

And outside it:

- `config/scoring.toml` with a `[scoring]` block — weights, decay half-life, points tables,
  rounding precision, the **feature denylist** and the backtest parameters. Data, not code:
  a reviewer changes a weight, rebuilds, and the manifest records which numbers produced
  which ranking. Add `config.scoring()`.
- `contracts/scoring.schema.json` for the backtest tables, plus **row rules on
  `compliance.leads`** in `contracts/compliance.schema.json`: `INELIGIBLE ⇒ priority_score
  IS NULL AND score_components IS NULL`; `ELIGIBLE|BLOCKED_UNTIL ⇒ priority_score IS NOT
  NULL`.
- Integration into `src/compliance/leads.py` / `build.py`: `score_inputs()` grows the crash
  context (see §2), `lead_row()` populates the two fields, the compliance build's stage order
  becomes `… -> evaluate -> score -> crash-only -> …`, and `_compliance_build_sha` hashes
  `config/scoring.toml` too, so a weight change moves every decision's build sha.
- Regenerated `output/sample_leads.csv` + `output/sample_leads.schema_check.json` with
  scores populated on the 25 eligible/blocked rows and null on the 15 ineligible ones, still
  validating row-by-row against the unmodified contract. `score_components` is written with
  the same JSON-in-CSV encoding `output/README.md` already documents — update that README.
- `SCORING.md` (§8) and `ai docs/implementation/phase7-scoring-report.md` (§8).
- `tests/test_scoring.py` (§6) and the fixture golden extended with the scores.

Not in scope, and say so in the report: a trained model, isochrones/drive-time as a
feature, any live enrichment, re-scoring the crash-only run as a contact queue (it is all
INELIGIBLE; see §4).

---

## 2. What the score claims, and what it may use

**The claim, in one sentence, which you put at the top of `SCORING.md` and test:**
*Among records the compliance layer says may be contacted, order by the likely seriousness
of the underlying crash and the freshness of the record, using only facts about the crash
itself.* Write the claim first; everything else is either a component of it or a test of it.

### Features — allowed, with their provenance

Every feature carries `source_table.source_field` and a transform. Recommended set (cut,
do not add, and record cuts):

| Component | Source field | Transform |
|---|---|---|
| `severity` | `gold.fact_crash.severity_ordinal` (via `config/severity_crosswalk.csv`, ordinal 0–5; on a fixture lead this is `leads.severity_ordinal`, null when no crash matched) | points table indexed by ordinal, from config; **unknown (0) scores as unknown, not as high** |
| `recency` | `leads.incident_date` (fixture) / `gold.fact_crash.crash_date` (gold) against the frozen `as_of` | exponential decay with a config half-life in days; the same `as_of = 2026-09-01` the engine uses, passed in, never `date.today()` |
| `vulnerable_road_user` | `gold.fact_crash.pedestrian_involved`, `bicyclist_involved` | flag points |
| `hit_run` | `gold.fact_crash.hit_run` | flag points |
| `road_context` | `gold.dim_road_class.fhwa_class` via `fact_crash.road_class_sk`; optionally `gold.crash_geo.osm_maxspeed_mph` **only where `snap_status = SNAPPED`** (report the null rate you inherit from Phase 4 — do not impute) | small points table |
| `adverse_weather` | `gold.dim_weather_condition.is_adverse` via `fact_crash.weather_condition_sk`; ERA5 `era5_precipitation_mm` as a fallback **only where `weather_status = JOINED`** | flag points |

Weights are a-priori, live in `config/scoring.toml`, and are justified in a comment each
(severity is the largest term by construction; nothing else may outweigh one severity step —
say why). The backtest measures whether they work; it does not fit them. If you decide a fit
is warranted, the fitted values are still written back into the TOML as data with the
fitting command recorded, and the backtest evaluates them on data the fit never saw.

### Features — the denylist, enforced by a test

`config/scoring.toml [scoring.denylist]` names every table and column the scorer must
never read, and `tests/test_scoring.py` greps `src/scoring/` and the feature list for them:
`dim_block_group` and every ACS column in it (`B19013`, `B01003`, `B25`, median income,
tenure, vehicle availability, anything under `gold.dim_block_group`), `zip5`, `census_bg`
and `tract_geoid` *as features* (they are permitted as a **blocking key** in the backtest
and nowhere else — the test distinguishes the two), `jurisdiction` as a points term (it
governs legality, not priority), and every `contact.*` / `consent.*` / line-type field
(channel routing is the engine's job; putting it in the score is a second, unaudited gate).
ACS's one legitimate use in this repo is Phase 5's population denominator for hotspot rates —
aggregate analysis, not per-contact targeting. Name that split explicitly in `SCORING.md`;
it *is* the answer §4 is fishing for.

### The missing-crash policy — the most important design decision in this phase

Only 6 of 40 fixture parties match a gold crash (`match_method =
JURISDICTION_DATE_DISTANCE`); 34 carry `severity_ordinal = NULL` and no crash context. This
is by construction (the fixture is synthetic; the production identity join does not exist)
and you must not paper over it:

- A component whose source field is absent contributes **0** with `status = UNAVAILABLE`
  and its provenance still recorded. Never impute, never substitute a jurisdiction mean.
- `score_components` carries a `coverage` term: how many of N components were available.
  A score built from `recency` alone is a *valid* score of a record about which little is
  known, and the decomposition says so. Rank by score, tie-break deterministically
  (`priority_score DESC, incident_date DESC, lead_id ASC`), and name the tie-break in config.
- Do **not** score a NO_MATCH lead from the *nearest* crash, or from the fixture's own
  `fixture_note`. The party is the record; the crash is either matched or it is not.

---

## 3. The backtest — what "reproducible" and "honest" mean here

The score's components include severity, so "top decile captures X% of severe crashes" is
circular if severity is in the score being tested. Test the claim without the tautology:

1. **Context-only ranking predicts severity.** Over gold `fact_crash` (all three sources,
   with `severity_ordinal` known), compute the score *with the severity term removed* and
   ask whether it ranks crashes with `severity_ordinal >= K` (K from config; recommend 3 =
   suspected serious injury or worse) ahead of the rest. Report precision@10%, lift over
   random, and lift over a one-feature baseline (VRU flag alone), per jurisdiction and
   overall. FARS is fatal-only and will trivially satisfy any K — report it separately or
   exclude it with the reason stated; the interesting universes are Montgomery and TxDOT.
2. **Temporal holdout.** Parameters are set (or, if fitted, fitted) on crash years
   ≤ 2023; every reported number is on 2024 (Montgomery also has 2025–2026 — report it as a
   second holdout, and note the recent-window incompleteness Phase 6's FL monitor exists
   for).
3. **Spatially blocked folds.** Folds by county *and* by H3 r7 (both keys exist on
   `crash_geo`). Report the blocked metric beside a **random-split negative control** on the
   same data; the gap is the leakage the assignment's §3c row warns about. If the gap is
   small, say so — that is a finding, not a failure.
4. **The full score on the 25 eligible/blocked fixture rows** is printed as a ranked table
   with every component — this is the demo, not a backtest, and the report must not call
   it one.

Everything the backtest reports is written to `data/gold/scoring/` as parquet
(`backtest_metrics`, `backtest_folds`, `crash_context_scores`), validated against
`contracts/scoring.schema.json` **before** the first write, with a `_scoring_manifest.json`
carrying the input hashes (`fact_crash`, `crash_geo`, the dims, `config/scoring.toml`), the
parameters, the row counts and `built_at`. One command reproduces every number in
`SCORING.md`; the command is written next to each table.

`crash_context_scores` is a backtest artefact over the whole corpus. It is **not** a lead
list: the crash-only run is INELIGIBLE everywhere, and `SCORING.md` says in its first
section that the production ranking is therefore empty by law, not by accident. Do not
write a `priority_score` column onto `crash_only_decisions`.

---

## 4. Outputs

| Path | What |
|---|---|
| `data/gold/compliance/leads.parquet` | unchanged columns, `priority_score` / `score_components` now populated on ELIGIBLE and BLOCKED_UNTIL |
| `output/sample_leads.csv`, `output/sample_leads.schema_check.json` | regenerated, committed |
| `data/gold/scoring/backtest_metrics.parquet`, `backtest_folds.parquet`, `crash_context_scores.parquet`, `_scoring_manifest.json` | backtest artefacts, gitignored |
| `config/scoring.toml`, `contracts/scoring.schema.json` | committed |
| `SCORING.md` | committed |

`score_components` shape (a dict of named terms, JSON-serialisable, no nesting deeper than
two levels, keys sorted so the CSV is byte-stable):

```
{
  "severity":        {"contribution": 30.0, "value": 3, "source": "gold.fact_crash.severity_ordinal", "status": "OK"},
  "recency":         {"contribution": 8.41, "value": 41,  "source": "leads.incident_date", "status": "OK"},
  "road_context":    {"contribution": 0.0,  "value": null, "source": "gold.dim_road_class.fhwa_class", "status": "UNAVAILABLE"},
  ...
  "coverage":        {"available": 2, "of": 6},
  "ruleset":         {"scoring_config_sha256": "...", "as_of": "2026-09-01"}
}
```

`priority_score` equals the sum of `contribution` over the components, rounded to the
precision in config — and a contract row rule asserts that equality.

---

## 5. Determinism, idempotency

Same bar as Phase 6, no exceptions: two compliance builds over unchanged inputs are
byte-identical across every parquet table and the committed CSV, with **zero** appended
lineage rows — scoring must not touch `evaluated_at`, the lineage id or anything the
engine wrote. Two backtest runs are byte-identical across the three scoring tables; the
manifest differs only in `built_at`. Floats are rounded at the boundary, not left to
platform summation order; every table goes through `common.write_parquet` with a total
sort. Fold assignment is a deterministic function of the blocking key, never `random_state`
alone (a seed makes a random split *repeatable*; it does not make it *blocked*).

---

## 6. Tests (`tests/test_scoring.py`) — real behaviour, no mocks, no network

Unit tests run without `data/gold/`; build-backed tests skip naming what is missing.

- Decomposition: `priority_score == sum(contributions)` for every scored row; every
  component carries `source`, `status`, `contribution`.
- Status gate: an INELIGIBLE row is never scored (null both fields); ELIGIBLE and
  BLOCKED_UNTIL are; `score_inputs()` still hands over eligible/blocked only.
- Monotonicity: all else equal, a higher `severity_ordinal` never scores lower; a more
  recent `incident_date` never scores lower; ordinal 0 (unknown) does not outscore
  ordinal 1.
- Missing context: a NO_MATCH lead scores with `UNAVAILABLE` components contributing 0,
  `coverage` reporting it, and a *finite* score.
- The denylist: `src/scoring/` and the configured feature list contain none of the
  denylisted tables/columns; the test also asserts `census_bg`/`tract_geoid` appear only
  in `backtest.py`'s blocking, and that `dim_block_group.parquet` is never opened by the
  scorer (patch-free: inspect the DuckDB relation names the build registers, or grep).
- No wall clock: `score_lead` with two different `as_of` values gives two different
  `recency` contributions; nothing in `src/scoring/` imports `date.today`/`datetime.now`
  outside the manifest.
- Tie-break: two rows with equal scores rank in the documented order.
- Backtest reproducibility: two runs into `tmp_path`, identical hashes; the blocked and the
  random-split metrics are both present; temporal holdout never evaluates on a train year.
- Config guard: a weight in the TOML that is not a finite number, or a component named in
  the TOML with no feature function, refuses to construct the scorer.
- Extend `tests/test_fixture_golden.py`: the golden snapshot gains `priority_score` per
  party; a weight change breaks the golden (that is the point).
- Contract: the regenerated `output/sample_leads.csv` validates row-by-row, and the new
  `compliance.leads` row rules hold.

---

## 7. Conventions

- Match Phases 1–6: module docstrings explain **why**; every number in config carries
  its reason; numbers in prose are measured, dated and reproducible by a named command.
- Commit as you go, **one-line conventional commits, no body, no co-author trailer, never
  mentioning an AI tool**: `feat(scoring): …`, `feat(contracts): …`, `test(scoring): …`,
  `chore(config): …`, `chore(output): …`, `docs(scoring): …`. Commit only files you fill in
  this phase. Never commit `data/`, `config/settings.toml`, `*.parquet` outside
  `tests/fixtures/`, `ai docs/`, `IMPLEMENTATION_GUIDE.md`, `REQUIREMENTS_AUDIT.md`,
  `notebooks/`, `orchestration/`, `audit/`. `output/sample_leads.csv` and
  `output/sample_leads.schema_check.json` **are** committed.
- Logs carry counts and scores, never a name, street, E.164 number or party coordinate.
  `src/scoring/` never imports the vault.
- `grep -rn 3857 src/scoring` must be empty; scoring performs no geometry operation at all.
- When this brief and the data disagree, measure, choose, and put the disagreement in the
  report and `SCORING.md`.

---

## 8. Deliverable: the prose

**`SCORING.md`** (≈1,000–1,500 words, business-readable, the way `ANALYSIS.md` and
`COMPLIANCE.md` are), in this order:

1. The claim (one sentence) and the fact that the production ranking is empty because
   every crash-only decision is INELIGIBLE — the score exists for the day a lawful
   identity source does.
2. The component table: name, source field, transform, weight, why the weight.
3. The missing-crash policy and the coverage term.
4. **The ACS paragraph, head-on.** Why block-group income, tenure and vehicle availability
   (and their correlates — `zip5`, `census_bg` as a feature, anything learned from them) are
   excluded from prioritisation: protected-class proxies, redlining-adjacent design,
   jurisdiction exposure; the one legitimate ACS use in the repo (Phase 5 population
   denominators, aggregate analysis) and why that line is the right one; how the denylist
   test enforces it. This paragraph is lifted verbatim into `MEMO.md` in Phase 9 — write it
   to that standard, under 250 words.
5. The backtest: method, holdout, blocking, the metrics tables with the command that
   reproduces each, the random-vs-blocked gap, and an honest reading of the numbers
   (including "the context features carry little signal beyond severity" if that is what
   the data says).
6. Decisions and rejected alternatives (a trained model, ACS features, scoring the
   crash-only universe, fitting weights) in the timestamped form Phase 9 lifts into
   `DECISIONS.md`.

**`ai docs/implementation/phase7-scoring-report.md`** in the shape of the Phase 6 report:
what you built (file table), things the brief said that the data doesn't do, what you
bounded and why, bugs the tests caught, verification (the two byte-identity runs with
hashes, the pytest count before and after), open items for Phases 8–9.

Update `README.md`'s `src/scoring/` line and add the two commands
(`python -m src.compliance.build`, `python -m src.scoring.build`) to the quickstart in the
same style as the other phases. Do not fill in `MEMO.md`, `DECISIONS.md` or `AI_USE.md` —
those are Phase 9 — but leave `SCORING.md` §4 and §6 ready to lift.

---

## 9. Definition of done

- [ ] Every ELIGIBLE and BLOCKED_UNTIL lead carries a finite `priority_score` equal to the
      sum of named `score_components`, each with source-field provenance and a status;
      every INELIGIBLE lead carries null for both. Contract row rules enforce it.
- [ ] `config/scoring.toml` holds every weight, points table, half-life, K, holdout year,
      blocking keys, precision and the denylist; a test proves the denylist.
- [ ] `python -m src.scoring.build` reproduces every number in `SCORING.md` from gold,
      with blocked and random-split metrics side by side and a temporal holdout.
- [ ] `output/sample_leads.csv` regenerated with scores, schema-valid, committed, 40 rows,
      fixture-derived, no PII beyond the fixture's own synthetic values.
- [ ] Two compliance builds byte-identical, zero appended lineage; two backtest runs
      byte-identical.
- [ ] Full suite green (baseline count + your new tests), xfails unchanged.
- [ ] `SCORING.md` with the claim, the component table, the ACS paragraph and the backtest
      reading; the report in `ai docs/implementation/`.
- [ ] Commit history for this phase reads as a story: config → features → score →
      contracts → integration → output → backtest → tests → docs.
