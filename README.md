# Crash-to-Contact

**Start with [`ASSIGNMENT.md`](ASSIGNMENT.md).** This README only covers the scaffold.

The pipeline ingests three public crash sources, builds conformed and spatial models,
evaluates contact eligibility, and produces an explainable scoring backtest. See
`OPERABILITY.md` for its partition, recovery, contract, sizing, and cost evidence.

## Quickstart

```bash
python -m venv .venv && source .venv/bin/activate
pip install -r requirements.txt
cp config/settings.example.toml config/settings.toml   # add your Census API key
python -m src.ingest.montgomery --since 2026-01-01
python -m src.compliance.build
python -m src.scoring.build

# Local orchestration UI (DAGSTER_HOME is intentionally gitignored)
mkdir -p .dagster
export DAGSTER_HOME="$PWD/.dagster"
dagster dev -f orchestration/dagster_defs.py

# Idempotent date-range proof in two isolated roots
python -m orchestration.backfill --start 2024-01-01 --end 2024-03-31 --prove
```

## Layout

```
src/ingest/      one module per source; bronze layer
src/transform/   bronze -> silver; conformed model
src/geo/         projection, snapping, indexing, timezone
src/compliance/  the eligibility engine — read compliance/rules.yaml first
src/scoring/     explainable additive lead scoring and spatially blocked backtest
contracts/       JSON Schema for each layer boundary
orchestration/   Dagster asset graph, checks, recovery, and backfill CLI
tests/           see tests/test_known_defects.py for the four you must catch
output/          sample_leads.csv — NO REAL PII
```

## Dagster asset listing

Verbatim output of `dagster asset list -f orchestration/dagster_defs.py` (the observable
external `decision_lineage` is intentionally not materialisable and therefore omitted):

```text
analysis
bronze_fars
bronze_montgomery
bronze_txdot
crash_geo
crash_only_decisions
exclusion_by_code
gold_model
leads
party_enrichment
sample_leads
scoring_backtest
silver_fars
silver_montgomery
silver_txdot
vault
```

## 10× monthly operating estimate

At ten times the current daily volume, budget about **$60/month**: roughly $52 for a
two-hour daily 128-GiB scheduled node, under $1 for object storage, and $8 for work disk.
Dagster OSS is co-located at $0; API ingress is $0 at the projected cache rate, subject to
Open-Meteo's commercial terms and 10,000-request daily limit. Assumptions and dated source
links are in `OPERABILITY.md`.

## Notes

- `config/sources.toml` has every endpoint pre-filled and verified as of 2026-09-01.
- Bronze is append-only. Backfills read it as it stands and never bypass durable source
  watermarks.
- Contract failures and Montgomery vocabulary drift are blocking Dagster asset checks.
