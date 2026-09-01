# Crash-to-Contact — Starter Scaffold

**Start with [`ASSIGNMENT.md`](ASSIGNMENT.md).** This README only covers the scaffold.

This is a **skeleton, not a solution.** It exists so that every submission shares a
baseline structure and a common output contract, which makes them comparable.

You are free to restructure anything here. If you do, say why in `DECISIONS.md`.
The one thing you should not change without explanation is
`contracts/lead_output.schema.json` — that is the interface the downstream
consumer depends on.

## Quickstart

```bash
python -m venv .venv && source .venv/bin/activate
pip install -r requirements.txt
cp config/settings.example.toml config/settings.toml   # add your Census API key
python -m src.ingest.montgomery --since 2026-01-01
```

## Layout

```
src/ingest/      one module per source; bronze layer
src/transform/   bronze -> silver; conformed model
src/geo/         projection, snapping, indexing, timezone
src/compliance/  the eligibility engine — read compliance/rules.yaml first
src/scoring/     lead prioritisation
contracts/       JSON Schema for each layer boundary
orchestration/   your DAG
tests/           see tests/test_known_defects.py for the four you must catch
output/          sample_leads.csv — NO REAL PII
```

## Files you must fill in

- `MEMO.md`
- `DATA_QUALITY.md`
- `DECISIONS.md`
- `AI_USE.md`
- `COMPLIANCE.md`

## Notes

- `config/sources.toml` has every endpoint pre-filled and verified as of 2026-09-01.
- `config/blackout_windows.csv` is deliberately incomplete. Completing and
  justifying it is part of the exercise.
- `src/compliance/engine.py` raises `NotImplementedError` on purpose.
