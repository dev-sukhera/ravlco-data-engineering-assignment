# Phase 9 — documents implementation report

Date: 2026-09-09

## Disclosure decision assumed

I chose the transparent option. `.gitignore` no longer hides
`IMPLEMENTATION_GUIDE.md` or `ai docs/`; those planning/build artifacts and
`notebooks/eda_bronze_silver.ipynb` are committed. `AI_USE.md` names that choice. To
reverse it, restore the two ignore entries and remove those artifacts in one repository
policy commit, then change the first paragraph of `AI_USE.md` to call them local working
notes. No pipeline output depends on the choice.

## What was written

- `DATA_QUALITY.md`: the nine assignment defects in order, each with What / How detected /
  What the pipeline does / Count, followed by discovered defects, CRSS and Florida scope.
- `DECISIONS.md`: dated executable choices, rejected alternatives and commit/test/manifest
  evidence from ingestion through operations, including decisions changed mid-build.
- `COMPLIANCE.md`: prose retained; added the eight defined zero-occurrence reason codes so
  every member of `ReasonCode` is named. The ruleset and exclusion tables did not change.
- `README.md`: separate no-data and full-data paths, measured test expectations, a
  fixture-only build, Ohio demo, privacy-safe record walkthrough and document index.
- `AI_USE.md`: tooling, phase-by-phase assistance, human judgments, primary-source checking
  and specific errors found by tests.
- `MEMO.md`: 1,822 words, six numbered business answers, exact zero-volume result,
  production and fixture exclusions, per-state legal answer, product alternatives, next
  quarter and failure modes.
- `requirements.txt`: the clean clone exposed undeclared runtime dependency `jsonschema`;
  it was added in its own packaging-fix commit.

No file under `src/`, `fixtures/`, either protected contract, or the committed sample was
edited.

## Reports versus manifests

The manifests won every disagreement.

| disagreement | report/brief expectation | authoritative result |
|---|---|---|
| clean-clone tests | Phase 8 reported 456 pass with local data | 384 pass, 72 skips, 4 xfail without ignored `data/`; 456/4 with it |
| clean dependency install | existing environment imported compliance | clean import failed because `jsonschema` was undeclared; requirements fixed |
| record walkthrough | fixture label appeared convenient | privacy-safe CSV omits the label; walkthrough uses committed `lead_id` |
| fixture no-data statuses | full fixture is 21/4/15 | without the optional road extract it is 24/4/12; README names both |
| production evaluation | Phase 6 allowed a sample | committed manifest says `sample_per_jurisdiction: all`, 268,493 evaluated |
| fixture road note | approximately 400 m | current hashed OSM extract measures 9.12 m; no code emitted |
| TxDOT county disagreement | initial formula looked complete | polygon validation found nine mappings/1,336 rows; lookup was corrected |

`COMPLIANCE.md`'s 45-row exclusion table was queried directly from
`data/gold/compliance/exclusion_by_code.parquet` and agrees row for row. The header prefix
`e4321de0…` agrees with the full ruleset SHA-256
`e4321de0bb131fa4f8d37e48736cbb1731e4a76790f22815c6a197ca6a5e45c9`.

## What I bounded

The memo omits implementation vocabulary, full reason-code citations, the 45-row exclusion
table, statistical tables, detailed cost arithmetic and operational commands to remain a
business document below 2,000 words. Those live in `COMPLIANCE.md`, `ANALYSIS.md`,
`SCORING.md`, `OPERABILITY.md`, `DATA_QUALITY.md` and this report. The production exclusion
table in the memo groups equal-count controls; the fixture table reports every nonzero code
count in prose. The complete row-level table remains in compliance documentation and
Parquet.

The clean-clone transcript is trimmed to final counts and material warnings; dependency
download lines, progress dots and known third-party warnings are omitted.

## Clean-venv quickstart transcript

Repository: `/tmp/phase9-clean.Y6vcdQ/repo`, cloned with `git clone .`; Python 3.14.7.

```text
$ python3 -m venv .venv
$ .venv/bin/pip install -r requirements.txt
Successfully installed ... duckdb-1.5.5 ... jsonschema-4.26.0 ... pytest-9.1.1 ...

$ MPLCONFIGDIR=/tmp/phase9-mpl .venv/bin/pytest -q
384 passed, 72 skipped, 4 xfailed, 3 warnings in 48.66s

$ python -m src.compliance.build --gold-root "$quick_root/gold" \
    --out-root "$quick_root/out" --vault-dir "$quick_root/vault" \
    --skip-crash-only --skip-snap --small-corpus --no-sample
fixture leads: 40 rows
  BLOCKED_UNTIL 4
  ELIGIBLE      24
  INELIGIBLE    12
trap P007: America/Denver, 09:00-21:00
trap P008: America/Chicago, 08:00-19:00
lineage: 40 rows (40 appended, 0 already present)

$ pytest -q tests/test_compliance.py::test_ohio_45_day_window_blocks_with_no_file_under_src_changed
1 passed in 0.33s

$ python -c '<README committed-record walkthrough>'
source_system: OTHER
eligibility_status: INELIGIBLE
reason_codes: [RND_REASSIGNED, ELIGIBLE_CONSENTED]
decision_lineage_id: 16b137fd...95d86a
```

The first clean run caught missing `jsonschema`; after adding it to `requirements.txt`,
the transcript above passed. The eight loopback HTTP tests need permission to bind
`127.0.0.1` in this execution sandbox; with that permission they pass. On a normal local
machine they require no external network.

## Verification

```text
$ wc -w MEMO.md
1822 MEMO.md

$ pytest -q
456 passed, 4 xfailed, 8 warnings in 186.67s

$ grep -rn "3857" src/ | grep -v "rendering\|never\|not "
src/geo/snap.py ... explanatory wrong-scale docstring/comment
src/geo/h3_index.py ... explanatory rejection comment
src/transform/resolve.py ... explanatory 29% error docstring
src/transform/montgomery.py ... explanatory rejection comment
src/transform/common.py ... explanatory rejection comment
```

The grep has no use of Web Mercator: every surviving source match says it is wrong. Binary
`__pycache__` matches are local ignored artifacts.

```text
$ python -m src.compliance.build
crash_only_decisions 268493 rows 050ca10a3f802e9e
exclusion_by_code        45 rows e3b5dd63695a567e
sample_leads_csv         40 rows 914434311caff945
ELIGIBLE total: 0
lineage: 268573 rows (0 appended, 268533 already present)

$ shasum -a 256 output/sample_leads.csv  # before and after
914434311caff945cb5eee6540c67d5f02a882641126d6bb345edbc8030960c9

$ git status --short output/
# no output

$ sha256sum src/compliance/rules.yaml
e4321de0bb131fa4f8d37e48736cbb1731e4a76790f22815c6a197ca6a5e45c9
```

## Memo evidence map

| memo question | number or claim | manifest field / reproducible evidence |
|---|---|---|
| 1 | six FARS years; 100k TxDOT bound; 10× sizing | build manifest source years; Phase 1 command/report; `OPERABILITY.md` timed command |
| 2 | 268,493 total; MD 128,026; TX 121,556; FL 18,911; eligible 0 | compliance `stats.crash_only.evaluated`, `.corpus_by_jurisdiction`, `.eligible_total` |
| 2 | fixture 40: 21/4/15 and every code count | compliance `stats.fixture.rows`, `.by_status`, `.by_reason_code`; exclusion Parquet |
| 3 | no production identity join and every crash ineligible | compliance `stats.crash_match.null_model`, `stats.crash_only`; `fixtures/README.md` |
| 4 | 55 raw hot cells disappear after normalization | analysis `stats.contrast.by_kind.HOT_RAW_ONLY`; `stats.contrast.hot_under_both` |
| 5 | 10× one-node plan and about $60/month | timed build table and dated arithmetic in `OPERABILITY.md` |
| 6 | P020 9.12 m; 1,336 county rows; weather; permutation; DST; frozen clock | compliance enrichment/report; geo county/weather/timezone stats; analysis Gi* stats; compliance input `as_of` |

## Data-quality count map

| finding | count(s) | manifest field or reproducing command |
|---|---|---|
| 1 coordinates | 114 assignment box; 105 operational envelope | `python -m src.transform.report`; analysis `stats.exclusions...no_usable_coordinate` |
| 2 mixed dictionary | 21 values; 172,116 old; 47,927 new; 121 combinations | `python -m src.transform.report`; `output/drift_firing.log` |
| 3 overlap | 2023-12-28–2024-01-03; best rule misses 4 | `python -m src.transform.report`; drift first-seen line |
| 4 anti-joins | incident−driver 785; driver−incident 0; 111/674 split | `python -m src.transform.report` anti-join block |
| 5 fan-out | 1.871 raw; 1.7714 deduped | `python -m src.transform.report` grain block |
| 6 TxDOT coordinates | 68,932/22,944/845/7,279; 1,065 disagreement | `python -m src.transform.report` coordinate block |
| 7 amendments | 5,936/100,000 | `python -m src.transform.report`; silver build manifest warning/stats |
| 8 TxDOT types | 3 dates, 64 IDs, 60 opaque, one code 95 | `python -m src.transform.report` dictionary block |
| 9 FARS sentinels | 873 coordinates; 1,668 hour; 14,498 age; 8,580 severity | `python -m src.transform.report` sentinel block |
| lane strings | 3,830 | `python -m src.transform.report` bonus block |
| non-motorist duplicate IDs | 9 | transform report; gold `outputs.fact_non_motorist.rows` reconciliation |
| severity disagreement | 3,975/124,331 | `python -m src.transform.report` bonus block |
| Texas county decode | 9 mappings/1,336 rows | Phase 4 reproduction query; geo `stats.county_refinement.disagreement_pairs` |
| outside-US geometry | 5 | geo `stats.timezone.unexpected_zones` |
| spatial-rate quality | 228 low-pop crash cells; 591 undefined; 2 p-value clamps | analysis `stats.cell_stats`; `stats.gi_star.raw_count.p_sim_clamped_to_unit_interval` |
| weather | 27,527/27,534 joined; 87.611% agreement | geo `stats.weather`; `stats.weather_precipitation_agreement` |
| fixture geography | 1 envelope; 5 snap failures; P020 9.12 m | compliance `stats.enrichment`; Phase 6 reproduction |
| Florida structural scope | 18,911 not applicable | compliance `stats.fl_incompleteness.fars_florida` |

## Open items for the live defense

**One-page walk-one-record script.** Use the README command for committed lead
`LD_e0a4d69fad770249`. Explain: the source record ID is tokenized; valid consent is
affirmative evidence but the reassigned-number response is a stronger bar; both codes and
parallel citations remain visible; the status collapses to ineligible; the content-addressed
lineage ID retrieves the immutable input/rule snapshot. Then show the same lineage ID in
`data/gold/compliance/decision_lineage.parquet` and the rule in `rules.yaml`. Do not open
the vault or print fixture PII.

**Ohio demo.** Run:

```bash
pytest -q tests/test_compliance.py::test_ohio_45_day_window_blocks_with_no_file_under_src_changed
```

Narrate that the test copies the blackout table, adds one Ohio row, bumps the rules version,
gets a dated hold with the row's citation, verifies existing decision outcomes do not move,
and proves every file under the compliance source directory is byte-identical.

Remaining production work is intentionally open: legal approval and product choice;
version-body persistence; a lawful Florida and identity source; statewide Texas snapping;
hosted operations/alerts; commercial weather terms; proxy audit; key rotation and deletion
propagation. These are next-quarter work, not hidden incompleteness in this documentation
phase.
