# Phase 8 — operability implementation report

Date: 2026-09-09

## What was built

| file | result |
|---|---|
| `orchestration/dagster_defs.py` | 17-node Dagster asset graph, natural partitions, bounded network retries, range job, contract/drift/geo/FL/lineage checks |
| `orchestration/backfill.py` | importable and CLI backfill, isolated two-run proof, stable hash inventory, targeted FARS recovery |
| `config/operability.toml`, `src/config.py` | reviewed retry, daily-partition and lineage-observation settings |
| ingestion modules | parsed bronze Arrow tables validated before durable replacement |
| `tests/test_operability.py` | graph, partitions, proof, recovery, drift, GeoParquet and append-only behaviour without network |
| `output/drift_firing.log` | committed historical cutover replay with commands and counts |
| `OPERABILITY.md`, `README.md` | runbook, evidence, right-sizing, cost and quickstart |

Dagster 1.13.21 and `dagster-webserver` 1.13.21 installed and imported successfully in
the existing Python 3.14.7 environment; a Python 3.12 orchestration environment was not
needed. Both are pinned under the Phase 8 requirements heading.

## Brief versus repository

The contract audit found bronze was documented but not enforced. Silver, gold, geo,
analysis, compliance, scoring and the lead CSV already validated before writes. The
parsed-parquet bronze writers now register their Arrow table in DuckDB and run the same
hand-rolled validator before `durable_replace`; Dagster repeats every boundary validation
as an asset check.

The existing builders operate on deterministic current/whole-corpus tables rather than
accepting crash-date predicates. The backfill therefore does not invent parallel business
logic: a date range selects the operational request and analysis period, while silver/gold
re-resolve from all bronze loads and compliance/scoring rebuild in full. This is required
to preserve late Montgomery records, TxDOT amendments, full-table keys and the frozen
compliance universe. Geo/FARS recovery stages and promotes the natural recovery unit.

The brief describes a FARS year as the recovery unit at every layer, but the committed
silver layout is source-wide (`silver/fars/accident_current.parquet`, not `year=YYYY`).
Changing that storage contract would change the engine, which the brief forbids. A changed
annual ZIP is therefore the acquisition trigger; recovery recomputes FARS in staging and
promotes only the FARS table set. Montgomery and TxDOT bytes remain untouched. The report
does not pretend that the existing silver files are year-partitioned.

`dagster asset list` lists materialisable assets only, so the observable external
`decision_lineage` does not appear in that CLI output although it is present in the
resolved graph. This is the intended proof that Dagster cannot materialise over it.

The prompt expected a clean baseline of 445 passed and 4 xfailed. The sandboxed baseline
reported 437 passed, 4 xfailed and 8 setup errors because the environment denied binding
the local HTTP test server to `127.0.0.1`; those eight are the existing real-HTTP-server
ingest tests, not pipeline failures. An unrestricted verification is recorded below.

## Bounds and rationale

- FARS uses configured years exactly. Montgomery uses daily load partitions beginning
  2019-01-01. TxDOT remains one unpartitioned OID-keyset asset because a date partition
  would misrepresent its source cursor.
- Network assets retry three times with 30-second exponential delay. Deterministic assets
  do not retry.
- Lineage observes the first 100 logically sorted rows plus total count. A decrease or
  changed established prefix fails; a successful observation atomically advances state.
- The FL trailing aggregate remains a warning-grade failed check. It is a structural
  statutory caveat, not a compliance build stage or lead exclusion.
- Proof ignores only manifest `built_at` and the spelling of its caller-selected isolated
  root. Parquet/CSV bytes and every manifest input, parameter, count and embedded SHA stay
  strict.

## Bugs caught while implementing

- Loading Dagster with `-f orchestration/dagster_defs.py` gives the module no package
  parent, so a relative import of `backfill` failed even though normal Python import
  worked. The definition file now uses the absolute package import and the documented
  file-based command succeeds.
- `/tmp` resolves to `/private/tmp` on macOS. Normalising only the resolved proof root
  left the manifest's spelled `/tmp/...` paths different. Both caller spelling and resolved
  spelling are normalised; no data value is altered.
- A GeoParquet test initially supplied its own `bbox`; GeoPandas correctly rejects this
  because `write_covering_bbox=True` owns that column. The test now verifies the generated
  struct and its values.

## Verification and evidence

Historical drift replay on the local 232,411-row driver corpus:

```text
driver_substance_abuse @ 2023-12-27: DRIFT, 9 values, 60,295 rows
injury_severity @ 2023-12-27: DRIFT, 5 values, 55,677 rows
```

The first new-generation value begins on crash date 2023-12-28, demonstrating the overlap
rather than assuming a hard 2024 cutover. Full output is `output/drift_firing.log`.

Fixture backfill proof compares 17 silver artefacts. Representative hashes are:

```text
crash_current                  686b2a874bac1186 == 686b2a874bac1186 PASS
fars/accident_current         94a04ec8eb09bd14 == 94a04ec8eb09bd14 PASS
montgomery/driver_current     fa76d1e3365f8b08 == fa76d1e3365f8b08 PASS
txdot/crash_current           7a9c388c05004504 == 7a9c388c05004504 PASS
VERDICT: PASS
```

The FARS recovery test replaces a corrupt current output with
`94a04ec8eb09bd14…`; hashes under Montgomery and TxDOT remain identical. GeoParquet
locality was tested on a copy of the live tree: MD/2023 stayed `df89bd538be08f23…`, while
corrupt MD/2024 was restored to `67e3d4e2c94b87ca…`.

The exact full-corpus `--prove` command completed with every listed artefact PASS. Key
hashes: `fact_crash=01a105d949376d8f`, `crash_geo=22b8002fedbabfb1`,
`crash_only_decisions=050ca10a3f802e9e`, `crash_context_scores=047ea9062b3f99e5`, and
`sample_leads.csv=27da03aceadd5b61`.

All 24 current `crash_geo` files passed GeoParquet 1.1.0, covering bbox, struct-column and
EPSG:4326 PROJJSON inspection. The full measured layer profile and 10× cost arithmetic are
in `OPERABILITY.md`.

Final unrestricted suite: **456 passed, 4 xfailed** in 183.45 seconds. The 11 Phase 8
tests pass, leaving the prescribed pre-Phase-8 baseline of 445 passing tests; xfails are
unchanged. `tests/test_idempotency.py` separately passed all eight local-server tests.

## Open items for Phase 9

Lift the five timestamped operational decisions and the cost/right-sizing conclusion into
`DECISIONS.md` and `MEMO.md`; complete `AI_USE.md`. Hosted Dagster, alert integrations,
catalogue/dbt adoption, production Open-Meteo commercial pricing, and concurrent PostGIS
serving remain explicitly outside Phase 8. The Phase 6 open item to persist complete
ruleset bodies by version also remains.
