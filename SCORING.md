# Explainable lead scoring

Among records the compliance layer says may be contacted, order by the likely seriousness of the underlying crash and the freshness of the record, using only facts about the crash itself.

The production ranking is currently empty. Every record in `crash_only_decisions` is `INELIGIBLE` because the public crash sources contain no lawful identity and contact layer. The score is applied only to the synthetic fixture's `ELIGIBLE` and `BLOCKED_UNTIL` records and exists for a future lawful identity source. An ineligible record receives null score fields and never enters a queue.

## Components

The score is additive and configured in `config/scoring.toml`. One severity step is 25 points; all other terms together are capped at 20, so context cannot reverse a one-step injury-severity difference. `python -m src.compliance.build` reproduces the fixture ranking and stores every contribution in `score_components`.

| Component | Source | Transform and maximum | Reason |
|---|---|---|---|
| Severity | `gold.fact_crash.severity_ordinal` | configured ordinal table, 0/25/50/75/100/125 | Injury seriousness is the primary claim. Ordinal 0 is unknown and scores zero. |
| Recency | `leads.incident_date` | exponential decay, 90-day half-life, 8 points | Newer records are more operationally useful without overcoming severity. |
| Vulnerable road user | `gold.fact_crash.pedestrian_involved,bicyclist_involved` | Boolean OR, 5 points | Pedestrian and cyclist crashes have a defensible seriousness relationship. |
| Hit-and-run | `gold.fact_crash.hit_run` | Boolean flag, 3 points | Crash conduct is relevant context but weak beside injury severity. |
| Road context | `gold.dim_road_class.fhwa_class` | configured table, at most 2 points | High-speed road classes add a small risk signal. A successfully snapped OSM speed is a fallback; it is never imputed. |
| Adverse weather | `gold.dim_weather_condition.is_adverse` | Boolean flag, 2 points | Weather is coarse context. Joined ERA5 precipitation is the explicit fallback. |

Every component records its value, contribution, source field, transform, and `OK` or `UNAVAILABLE` status. `coverage` reports how many of the six inputs were available. `ruleset` records the frozen `as_of` and scoring-config SHA-256. The sum is rounded to two decimal places only at the output boundary.

## Missing crash context

The fixture is synthetic and most parties do not match a gold crash. A missing source fact contributes zero with `UNAVAILABLE`; the scorer does not impute a jurisdiction average, borrow the nearest crash, or read `fixture_note`. Recency can therefore produce a finite score by itself, while coverage tells a consumer that five crash-context components are absent. Equal scores use the deterministic order `priority_score DESC, incident_date DESC, lead_id ASC`.

## ACS and protected proxies

Block-group income, tenure, vehicle availability, and variables learned from them are excluded from lead prioritisation. ZIP5, census block group, and tract are excluded as score features as well. These fields are strong proxies for race, wealth, housing stability, and access to transport; using them to decide who receives solicitation attention creates a redlining-adjacent allocation rule, adds jurisdiction-specific fair-lending and consumer-protection exposure, and does not support the score's crash-seriousness claim. Correlated features do not become acceptable merely because the ACS column name disappears, so the design also rejects fitted socioeconomic proxies. The repo's legitimate ACS use is Phase 5 population denominators for aggregate hotspot rates: it estimates crash burden across areas and does not rank an individual for contact. Census and H3 identifiers may assign spatial backtest folds, where they prevent neighbouring observations leaking across evaluation partitions; they never add points. The configured denylist and source-inspection tests enforce this boundary, including the ban on opening `dim_block_group` from scoring code.

## Backtest

`python -m src.scoring.build` reproduces the three parquet tables and manifest under `data/gold/scoring/`. The evaluation removes the severity term, then asks whether the remaining context ranks crashes with severity ordinal 3 or higher into the top decile. It reports precision, lift over the holdout prevalence, and lift over a vulnerable-road-user-only baseline. Parameters are fixed through 2023; 2024 is the primary holdout. Montgomery 2025 onward is a second, incomplete holdout and is read with the recent-window caution from the compliance monitor. FARS is reported separately because every FARS row is fatal and its lift is necessarily 1.0.

The 2024 results are:

| Fold strategy | Universe | Records | Prevalence | Precision@10% | Lift over random | Lift over VRU |
|---|---:|---:|---:|---:|---:|---:|
| County blocked | Overall | 38,789 | 0.335 | 0.813 | 2.432 | 1.124 |
| H3 r7 blocked | Overall | 38,789 | 0.335 | 0.803 | 2.399 | 1.083 |
| Random control | Overall | 38,789 | 0.335 | 0.807 | 2.413 | 1.091 |
| County blocked | Maryland | 11,146 | 0.225 | 0.525 | 2.333 | 0.988 |
| County blocked | Texas | 24,712 | 0.305 | 0.925 | 3.033 | 1.596 |
| County blocked | Florida/FARS | 2,931 | 1.000 | 1.000 | 1.000 | 1.000 |

The random-versus-blocked gap is small: overall precision is 0.807 under random folds, 0.813 under county blocking, and 0.803 under H3 blocking. There is no evidence that random splitting materially inflates this fixed rule's result. Texas context adds signal beyond the VRU baseline, while Maryland county-blocked lift over VRU is 0.988. That is an honest limit: these context features do not consistently carry signal beyond the strongest single component. The full score includes observed severity because it ranks known records; the context-only experiment avoids claiming validation by predicting severity with itself.

The 2025-plus Montgomery result is also modest: county-blocked precision is 0.459 at prevalence 0.183, or 2.511 lift over random, but only 0.945 lift over VRU. This window is partial and is not evidence for retuning.

The fixture's admitted rows are a demonstration rather than a backtest. Their score decomposition is inspectable in the committed CSV. The full-corpus `crash_context_scores` table is likewise an evaluation artefact, never a contact list, and no score is written onto `crash_only_decisions`.

## Decisions

- **2026-09-08 — Use an a-priori additive rule.** Six transparent crash facts meet the explainability requirement and keep each ranking reproducible. A trained model would add fitting and calibration claims the available outcome does not justify.
- **2026-09-08 — Exclude socioeconomic and routing attributes.** ACS and geographic proxies do not support the seriousness claim. Contact, consent, line type, and jurisdiction belong to compliance and routing rather than priority.
- **2026-09-08 — Preserve missingness.** Missing crash facts score zero and carry `UNAVAILABLE`; means, nearest-crash substitution, and hidden defaults would create facts the identity join did not establish.
- **2026-09-08 — Keep the production queue empty.** Scoring cannot cure an ineligible disposition. The crash-only corpus remains unscored as a contact queue.
- **2026-09-08 — Do not fit the weights.** Backtest numbers evaluate the declared rule. They do not feed back into the same TOML, avoiding evaluation-set tuning.

Isochrones, drive-time features, live enrichment, and a trained model remain outside Phase 7.
