# AI assistance disclosure

## Disclosure choice

This repository takes the transparent option: `IMPLEMENTATION_GUIDE.md`, every planning
brief and implementation report under `ai docs/`, and the exploratory notebook are part
of the submitted record. They are not hidden by `.gitignore`. The artifacts show both the
instructions given to the tool and what actually happened; when a brief and measured data
disagree, the implementation report records the disagreement.

## Tool and working pattern

Claude Code assisted throughout. Work was organized as one implementation brief and one
build report per phase. The tool helped inspect sources, propose designs, implement Python
and SQL, write tests, run measurements, and draft documentation. It did not make the
output trustworthy by itself: executable tests, data contracts, manifest reconciliation,
primary-source review, and human acceptance of scope and legal judgments were the gates.

## Assistance by phase

| Phase | AI-assisted work | Verification boundary |
|---|---|---|
| 1 — ingestion | HTTP clients, pagination, watermark storage, FARS restatement and tests | Live service descriptors/counts, raw hashes, repeat pulls |
| 2 — silver | profiling queries, parsers, SCD2, contracts, defect tests | Bronze/silver reconciliation and byte-identity runs |
| 3 — model | conformed dimensions, entity-resolution tiers, bridge and tests | Match census, unmatched reasons, stable-key tests |
| 4 — geo | reference cache, polygon joins, timezones, road/weather enrichment, GeoParquet | CRS audit, polygon checks, snap/weather measurements |
| 5 — analysis | Moran/LISA, Gi*, FDR diagnosis, KDE/ST-DBSCAN, figures and prose | permutation sweeps, blocked CV, manifest counts |
| 6 — compliance | primary-source research, rules-as-data, engine, vault, consent, lineage, fixture build and prose | statute checking, golden fixture, PII grep, Ohio test |
| 7 — scoring | additive features, gate, spatial backtest, contracts and documentation | decomposition/denylist tests and held-out metrics |
| 8 — operability | Dagster graph, backfill/recovery proof, boundary checks, sizing and cost draft | full proof hashes, CLI listing, timed local builds |
| 9 — documents | consolidation and cross-reference drafting | manifests, repository commands, clean-clone quickstart |

## Human decisions

The owner retained the decisions that change product meaning or risk. These include using
the public but mislabeled TxDOT layer while treating the label as a provenance risk;
preferring CRIS-derived coordinates; quarantining rather than dropping invalid geography;
limiting TxDOT acquisition, weather history, FARS years, and road snapping; excluding
CRSS, isochrones, GHCNh, statewide Texas snapping, socioeconomic scoring, and an ML model;
and keeping the production contact queue empty.

Legal judgment was also human-owned. In particular: Md. Gen. Prov. §4-320 is treated as
reaching the proposed use of the Montgomery police feed even though the narrower
custodian-only reading is recorded; the TxDOT publication may be used as a redacted crash
source but not as evidence of permission to contact; the default remains ineligible; and
valid consent is the only exception to the configured lawyer/agent live-solicitation bar
inside the synthetic test harness. These decisions and rejected alternatives are in
`DECISIONS.md` and `COMPLIANCE.md`; this file is not legal advice.

## Primary sources checked by hand

On 2026-09-08, the compliance citations were opened and compared against primary statute,
regulation, court-opinion, and bar-rule text. Four focused verification searches were run
during the compliance phase. That review corrected the Texas calling-hours citation to
Tex. Bus. & Com. Code §301.051 rather than the commonly repeated §302.101, established
Maryland's hours at Md. Code Com. Law §14-4502(c)(1), and checked the federal and
state-source/channel rules summarized in `COMPLIANCE.md`. The cited text was then encoded
in `src/compliance/rules.yaml` or `config/blackout_windows.csv`; the ruleset hash binds a
decision to that reviewed body.

## Mistakes caught by tests and measurements

AI assistance produced plausible mistakes. Keeping this list is part of the disclosure:

- Phase 1 initially retried only the HTTP request, not a failed streaming body; used
  second-resolution load partitions that could overwrite; and failed to complete an empty
  TxDOT terminal page.
- Phase 2 inverted `orphans_allowed_when`, ran range rules after a type failure, accepted
  missing unique-key declarations incorrectly, and nearly blamed the Parquet writer for
  nondeterminism caused by a non-total sort key.
- Phase 3 initially allowed unstable or over-broad resolution candidates; profiling found
  fatal-signal disagreement and nearby nonfatal pairs that were deliberately not merged.
- Phase 4 initially decoded nine Texas `Mc…` counties incorrectly, filtered county
  polygons too narrowly, accepted tied nearest roads as one row, and learned that the
  covering box did not prune at the original row-group size.
- Phase 5 discovered that 999 permutations could not resolve the FDR threshold, the
  library's one-sided default was wrong for the claim, two returned p-values exceeded
  one, and random spatial folds selected a KDE bandwidth 10.7 times smaller.
- Phase 6 YAML parsed bare `YES` as boolean, so a reassigned-number rule never fired; the
  catch-all solicitation bar lacked its consent exception; Parquet null strings arrived
  as NaN; a build hash polluted content comparison; and a contract incorrectly forbade
  affirmative evidence beside a temporary hold.
- Phase 7 an unqualified crash date became ambiguous after a join, pandas returned a
  timestamp where the contract required a date, and the scorer boundary initially carried
  channel fields it did not need.
- Phase 8 file-based Dagster loading broke a relative import, macOS `/tmp` path aliases
  broke proof normalization, and a test attempted to provide a covering-box column that
  GeoPandas owns.

The resolution pattern was consistent: add or strengthen a test, rerun it, and record the
measured correction in the phase report. AI-generated prose was not used as evidence for
a number or a legal rule.
