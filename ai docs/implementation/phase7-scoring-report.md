# Phase 7 implementation report — explainable lead scoring

## What I built

| File | Responsibility |
|---|---|
| `config/scoring.toml` | Frozen date, a-priori points, recency decay, deterministic tie-break, holdout/fold parameters, and the protected-proxy denylist. |
| `src/scoring/features.py` | Six pure feature functions returning value, source field, transform, availability, and contribution. |
| `src/scoring/score.py` | Config-validated additive scoring, coverage, decomposition, compliance status gate, and deterministic ranking. |
| `src/scoring/backtest.py` | Context-only severity evaluation on 2024 and 2025-plus holdouts with county, H3 r7, and random-control folds. |
| `src/scoring/build.py` | Side-effect-free import and CLI that validates three tables before deterministic parquet writes and emits a hashed manifest. |
| `contracts/scoring.schema.json` | Types, keys, finite-score rules, JSON decomposition, and temporal-order assertion for the backtest. |
| `contracts/compliance.schema.json` | Row rules requiring scores for admitted records, nulls for ineligible records, and equality to six contributions. |
| `src/compliance/leads.py`, `src/compliance/build.py` | Crash-context join, score-stage integration, config hash in build lineage, JSON parquet/CSV encoding. |
| `tests/test_scoring.py` | Decomposition, gate, monotonicity, missingness, frozen time, config guards, folds, contracts, and byte reproducibility. |
| `SCORING.md`, `README.md`, `output/README.md` | Business claim, ACS answer, measured evaluation, commands, and encoding notes. |

The production truth did not change: all 268,493 crash-only decisions remain `INELIGIBLE`, and `crash_only_decisions` has no score column. The fixture has 40 decisions: 21 `ELIGIBLE`, 4 `BLOCKED_UNTIL`, and 15 `INELIGIBLE`. Exactly 25 rows are handed to scoring. The regenerated committed sample has scores on those 25 and null score fields on the other 15; all 40 rows validate against the unmodified lead-output contract.

## Where the brief and data differ

The brief correctly anticipated six fixture-to-crash matches. Only three of those matches are in the admitted set in this build; the other matched records remain excluded by compliance. Most admitted fixture rows therefore have recency-only scores with coverage 1 of 6. I preserved that result instead of attaching a nearby crash or jurisdiction average.

The full gold corpus used by the backtest has 268,493 records: 125,005 Montgomery, 100,000 TxDOT, and 43,488 FARS. The primary 2024 holdout has 38,789 rows. FARS is fatal-only, so its 2,931 Florida rows have prevalence and precision of 1.0 and provide no discrimination evidence. Montgomery and TxDOT are reported independently.

The random-versus-spatially-blocked gap is small. Overall 2024 precision at the top decile is 0.807 for random folds, 0.813 for county blocks, and 0.803 for H3 r7 blocks. The result does not show material spatial leakage for this fixed score. Texas shows context signal beyond the VRU-only baseline; Maryland does not consistently do so. I reported both findings in `SCORING.md` rather than tuning weights on the holdout.

## What I bounded and why

This is an additive rule, not a trained model. Severity moves in 25-point steps while all non-severity terms together contribute at most 20 points. Missing features contribute zero and remain `UNAVAILABLE`. Road speed is read only after a successful snap, ERA5 precipitation only after a successful weather join, and neither is imputed. Scoring reads no vault data and performs no geometry operation.

ACS income, tenure, vehicle access, and their geographic or learned proxies are outside the scoring boundary. Census/H3 keys exist only to block backtest folds and cannot add points. The only ACS use remains Phase 5's aggregate population denominator. `SCORING.md` gives the full protected-proxy rationale in a paragraph ready for the Phase 9 memo.

I did not build isochrones, drive-time features, live enrichment, a fitted model, or a score for the crash-only contact universe. Those are outside this phase, and a model would need a defensible non-circular outcome and a separate untouched evaluation set.

## Bugs caught while implementing

Adding `crash_geo` to the fixture match initially made unqualified `crash_date` ambiguous in DuckDB; the query now qualifies all fact fields. The parquet boundary also exposed that pandas returned `crash_date` as a timestamp while the scoring contract requires a date; the build normalises it before validation. Finally, the existing Phase 6 hook test expected calling-window data in scorer inputs. Phase 7 intentionally removes channel-routing fields from that boundary and the test now asserts their absence.

## Verification

Before Phase 7 changes, the sandboxed suite reported 425 passed and 4 xfailed; eight ingestion tests could not bind their local HTTP test sockets because the execution sandbox returned `PermissionError`. This is an environment restriction rather than a repository failure, and the final verification reruns the suite with local socket permission.

`python -m src.compliance.build --json` produced 40 schema-valid sample rows, scored 25 admitted records, and retained zero eligible crash-only records. Its lead parquet SHA-256 was `eba2a8bbc137cc19a9c61dcb7aaf6afb64581fdb579c505f6e7602a4742909f6`; the committed CSV SHA-256 was `914434311caff945cb5eee6540c67d5f02a882641126d6bb345edbc8030960c9`.

`python -m src.scoring.build` produced 18 summary rows, 90 fold rows, and 268,493 crash-context score rows. All three tables validated before write. Two runs produced identical hashes: `23e66e2d6f70bb13212a2e78bb00887b307acc00915181bb4fe0722b8ad4f309` for metrics, `b3807b9a4eb6de615bf0717adfda5dc6aa4926d977902c1dd2be34fff330b61a` for folds, and `047ea9062b3f99e59dc4ccc91cac10d28fdda79959d9e646b178a34f992bb646` for crash-context scores.

The repeated compliance build appended zero lineage rows and reproduced every parquet and CSV hash, including the lead and CSV hashes above. The unrestricted full suite completed with 444 passed and 4 expected failures; after adding the executable claim assertion, its focused post-change run added one passing test, bringing the final suite composition to 445 passing tests and 4 expected failures. Five existing libpysal warnings report the intentionally disconnected spatial weights graph.

## Open items for Phases 8–9

Phase 8 can wrap `build_compliance()` and `build_scoring()` as assets; both modules are import-safe and accept explicit roots. Phase 9 should lift the claim, the ACS paragraph, the measured holdout interpretation, and the five timestamped decisions from `SCORING.md`. It should continue to state that the lawful production ranking is empty until a permissible identity source exists.
