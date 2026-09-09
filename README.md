# Crash-to-Contact

**Start with [`ASSIGNMENT.md`](ASSIGNMENT.md).** This README covers reproducible local
operation and points to the evidence behind the submission.

The pipeline ingests three public crash sources, builds conformed and spatial models,
evaluates contact eligibility, and produces an explainable scoring backtest. See
`OPERABILITY.md` for its partition, recovery, contract, sizing, and cost evidence.

## Quickstart A — no network and no source data (under five minutes)

Python 3.12+ is supported. This path uses only committed test extracts and the fabricated
party fixture; it does not require `data/`, an API key, or a network call after dependency
installation.

```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
pytest -q

# Fixture-only compliance build in disposable directories; leaves the committed sample alone.
quick_root=$(mktemp -d /tmp/crash-contact-quick.XXXXXX)
mkdir -p "$quick_root/gold"
python -m src.compliance.build \
  --gold-root "$quick_root/gold" --out-root "$quick_root/out" \
  --vault-dir "$quick_root/vault" --skip-crash-only --skip-snap \
  --small-corpus --no-sample

# Live-defense change: Ohio adds a 45-day window without changing engine code.
pytest -q tests/test_compliance.py::test_ohio_45_day_window_blocks_with_no_file_under_src_changed

# Walk one synthetic decision already committed: status, codes, citations and lineage.
python -c 'import csv,json; r=next(x for x in csv.DictReader(open("output/sample_leads.csv")) if x["lead_id"]=="LD_e0a4d69fad770249"); print(json.dumps({k:(json.loads(r[k]) if k in {"reason_codes","legal_basis"} else r[k]) for k in ("source_system","source_record_id","eligibility_status","reason_codes","legal_basis","decision_lineage_id")},indent=2))'
```

Expected test result is **456 passed, 4 xfailed**. The four bronze assertions are marked
`xfail(strict=True)` because the committed extracts deliberately retain the known defects;
their silver twins pass. The fixture-only build reports 40 decisions. Without the optional
OSM extract it records snapping as unavailable, so five snap-dependent golden comparisons
are not asserted; the committed full fixture run is 21 eligible, 4 blocked, 15 ineligible.
The one-record command prints no name, address, telephone number, or coordinate.

## Quickstart B — full pipeline

Copy the local settings and add a free Census API key. The current reference cache can use
the official ACS Summary File without a key, but a fresh API acquisition requires one.

```bash
cp config/settings.example.toml config/settings.toml
# Edit config/settings.toml: census_api_key = "..."

python -m src.ingest.montgomery
python -m src.ingest.txdot                 # bounded 100,000-row demonstration
python -m src.ingest.fars
python -m src.transform.build
python -m src.transform.model
python -m src.geo.build
python -m src.analysis.build
python -m src.compliance.build
python -m src.scoring.build

# Idempotent date-range proof in two isolated roots.
python -m orchestration.backfill --start 2024-01-01 --end 2024-03-31 --prove

# Local orchestration UI (DAGSTER_HOME is intentionally gitignored)
mkdir -p .dagster
export DAGSTER_HOME="$PWD/.dagster"
dagster dev -f orchestration/dagster_defs.py
```

On the measured local corpus, transform/model/geo-offline/analysis/compliance/scoring take
about 34/5/21/112/55/22 seconds. Network acquisition dominates: the full TxDOT universe is
projected at about 4.6 hours and 13 GB, so the default demonstration is bounded; reference
downloads include a 214 MB Maryland road extract. `OPERABILITY.md` contains the exact
machine profile, recovery semantics, and 10× estimate. Run `dagster asset list -f
orchestration/dagster_defs.py` for the materializable graph; start the UI only when an
interactive process is wanted.

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

## Documents

- [`MEMO.md`](MEMO.md) — six business answers, including the zero-lead conclusion.
- [`DATA_QUALITY.md`](DATA_QUALITY.md) — every known defect, detector, disposition, and count.
- [`DECISIONS.md`](DECISIONS.md) — dated choices, rejected alternatives, and evidence.
- [`AI_USE.md`](AI_USE.md) — assistance, human judgments, verification, and caught mistakes.
- [`COMPLIANCE.md`](COMPLIANCE.md) — cited ruleset prose and both exclusion tables.
- [`README.md`](README.md) — setup, quickstarts, layout, and operating entry points.
- [`ANALYSIS.md`](ANALYSIS.md) — spatial methods, figures, and interpreted findings.
- [`SCORING.md`](SCORING.md) — explainable score, proxy exclusions, and backtest.
- [`OPERABILITY.md`](OPERABILITY.md) — orchestration, contracts, recovery, sizing, and cost.

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
