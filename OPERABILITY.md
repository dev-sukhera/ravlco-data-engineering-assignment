# Operability

Measurements and prices in this document were checked on 2026-09-09. Commands run from
the repository root with the committed configuration.

## Partition and recovery model

| layer/source | natural partition | recovery unit | date-range backfill behaviour |
|---|---|---|---|
| FARS bronze | report year, 2019–2024 | one year whose ZIP SHA-256 changed | read the selected annual snapshot; never replace bronze |
| FARS silver | existing source-wide current/history files | all FARS tables, staged after one-year restatement | recompute from annual snapshots, then promote only FARS outputs |
| Montgomery bronze | UTC load date plus `(:updated_at, :id)` cursor | one load partition | read every load, then rebuild crashes in the requested crash-date range |
| TxDOT bronze | OID-keyset sweep | one bounded sweep; deliberately unpartitioned in Dagster | read every sweep so `amend_supp_fl` versions remain visible |
| silver/gold current slices | SCD2 natural key | deterministic table build | re-resolve current state from all bronze loads; late records cannot be skipped |
| `crash_geo` | `jurisdiction/year` hive key | one state-year parquet | stage, validate and atomically promote an affected state-year |
| analysis | configured study period | requested analysis period | rebuild statistics for the requested dates |
| compliance/scoring | frozen whole corpus | whole table | rebuild in full; cheap relative to acquisition and required for corpus-wide keys/rules |

“Backfill 2024-01-01 through 2024-03-31” does **not** mean asking Socrata for crashes
whose crash date lies in that interval. Montgomery bronze is load-time partitioned and
watermarked on update time and row ID; TxDOT is OID-keyset; FARS is annual. Refetching by
crash date would bypass the durable watermarks and lose late amendments. The backfill
therefore treats append-only bronze as immutable input and lets the existing SCD2 builders
reconstruct current state. `--fars-years` bounds annual recovery; compliance and scoring
remain whole-corpus because their contracts and frozen 2026-09-01 clock are corpus-wide.

## Asset graph, retries, and recovery

`dagster asset list -f orchestration/dagster_defs.py` printed:

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

The dependency edges are `bronze_* → silver_* → gold_model → crash_geo → analysis →
vault → party_enrichment → leads → crash_only_decisions → exclusion_by_code →
scoring_backtest → sample_leads`; `leads` also requires `crash_geo` and `vault`.
`decision_lineage` is an observable external asset, so the materialisable-asset listing
correctly omits it. Dagster never builds over that sink.

Only the three network assets retry: three run retries, starting after 30 seconds with
exponential backoff. Tenacity inside ingestion retries a failed HTTP request; Dagster's
outer policy covers an exhausted request budget or lost process. Deterministic transforms
do not retry: the same contract violation will fail again. Lineage is content-addressed;
same input is a no-op and changed input creates a new ID, so an asset-level retry would add
no safety.

Recovery is stage, validate, promote. For FARS,
`recover_fars_year(2024, ...)` rebuilds FARS in a temporary silver root and atomically
promotes only FARS outputs. The test corrupted `accident_current.parquet`; its SHA returned
to `94a04ec8eb09bd14…`, while hashes below `silver/montgomery` and `silver/txdot` did not
change. Geo recovery uses the same pattern with one `jurisdiction=XX/year=YYYY` directory.
In a copy of the live tree, MD/2023 was `df89bd538be08f23…` before and after targeted
MD/2024 recovery; the corrupt target was restored as `67e3d4e2c94b87ca…`. Thus failure
recovery does not require bronze acquisition or replacement of an unrelated partition.

## Idempotent backfill

Run:

```bash
python -m orchestration.backfill --start 2024-01-01 --end 2024-03-31 --prove
```

`--layer silver|gold|geo|compliance|scoring|all`, `--fars-years`, `--dry-run`, and
`--out-root` make scope explicit. `--prove` executes twice under isolated temporary roots,
hashes every parquet and CSV, and compares manifests after removing `built_at` and the
caller-selected temporary-root spelling. No data field is exempt. The exact full-corpus
command above compared every listed artefact and ended `VERDICT: PASS`. Fixture evidence:

```text
artefact                                      run1 sha256      run2 sha256      verdict
silver/crash_current.parquet                  686b2a874bac1186 686b2a874bac1186 PASS
silver/fars/accident_current.parquet          94a04ec8eb09bd14 94a04ec8eb09bd14 PASS
silver/montgomery/driver_current.parquet      fa76d1e3365f8b08 fa76d1e3365f8b08 PASS
silver/txdot/crash_current.parquet            7a9c388c05004504 7a9c388c05004504 PASS
silver/_build_manifest.json                   (normalised equal)                PASS
VERDICT: PASS
```

The full-corpus proof included `fact_crash` `01a105d949376d8f`, `crash_geo`
`22b8002fedbabfb1`, compliance decisions `050ca10a3f802e9e`, scoring output
`047ea9062b3f99e5`, and sample CSV `27da03aceadd5b61`—each equal in both runs.

The function `backfill()` is importable and the Dagster `range_backfill_job` calls that
same function. A dry run validates dates, configured FARS years and layer closure without
writing. The locality test hashes unrelated source directories before and after targeted
FARS recovery.

## Contracts and operational gates

| boundary | contract | enforcement and checks |
|---|---|---|
| source → parsed bronze | `bronze.schema.json` | now validates Arrow before durable write; Dagster re-reads all parsed pages |
| bronze → silver | `silver.schema.json` | `build_silver()` before write plus checks on every current slice |
| silver → gold | `gold.schema.json` | `build_gold()` plus visible grain/type/range/key check |
| gold → geo | `gold.schema.json` | `build_geo()` before write plus `crash_geo` and block-group checks |
| gold → analysis | `analysis.schema.json` | analysis build and asset check over seven tables |
| gold → compliance | `compliance.schema.json` | compliance build and asset check over four tables |
| compliance → scoring | `scoring.schema.json` | scoring build and three-table asset check |
| compliance → CSV | `lead_output.schema.json` | row-by-row Draft 2020-12 plus format checking, repeated as asset check |

The validator checks DuckDB types, required/null columns, enums/ranges, unique keys,
foreign keys, minimum counts and cross-column SQL row rules, returning all violations with
counts and example keys. Bronze was the previously documented-but-unenforced gap; ingestion
now validates the in-memory parsed table before its durable replace. A separate blocking
Montgomery check reads bronze and runs the reviewed vocabularies. The historical replay in
`output/drift_firing.log` shows the 2023-12-27 vocabulary flagging nine substance values /
60,295 rows and five injury values / 55,677 rows. Current vocabulary is silent. The gold
check exposes the one-row-per-crash key; GeoParquet metadata and lineage append-only checks
are also blocking. The FL trailing-30-day check deliberately reports WARN/failure when it
overlaps the statutory 60-day incomplete window, so a right-looking trend cannot silently
publish.

## GeoParquet storage

PyArrow inspection of all 24 hive files confirms GeoParquet `version: 1.1.0`, WKB geometry,
EPSG:4326 PROJJSON, a struct `bbox` column, and `covering.bbox` paths for xmin/ymin/xmax/ymax.
The corpus is 268,493 rows: the flat file is 30.98 MB in three 122,880-row groups; the hive
copy is 29.05 MB in 74 4,096-row groups. Partitions range from FL/2019 (2,952 rows, 0.320 MB,
one group) to TX/2021 (25,245 rows, 2.239 MB, seven groups); MD/2024 is 11,146 rows,
1.492 MB, three groups.

`python -m src.geo.build --measure-row-groups` measured H3-ordered Maryland: 4,096-row
groups prune 28.1% for a 13×11 km box and 65.6% for a 1.7×1.1 km corridor. At 8,192 those
figures fall to 6.2%/37.5%; 32,768 prunes nothing. The chosen 4,096 adds only 0.8% to
MD/2024 and becomes about 27 independently prunable groups at 10×. Hive state/year first
removes whole files; H3 order then makes bbox statistics useful inside them.

## Right-sizing and monthly cost at 10×

Measured on this Mac with `/usr/bin/time -l`, full local corpus, cold process:

| layer | wall seconds | peak RSS GB |
|---|---:|---:|
| silver | 33.8 | 2.39 |
| gold | 5.4 | 1.30 |
| geo, offline cache | 21.0 | 1.94 |
| analysis | 111.8 | 3.26 |
| compliance, 268,493 decisions | 54.7 | 6.24 |
| scoring | 21.6 | 2.56 |

A conservative linear 10× projection is about 41 minutes and 62.4 GB peak RSS. One
128-GiB, 16-vCPU memory-optimised node/container leaves roughly 2× memory headroom and a
two-hour daily reservation leaves 3× time headroom. **DuckDB plus GeoPandas on one node is
the right size here. Spark/Sedona is the escape hatch for national, multi-year scale when
partitions must execute across machines; it was not used because 268k rows—and even the
projected 2.68m—fit comfortably in one node and distributed shuffle/operations would cost
more than the work.** PostGIS becomes useful only for concurrent serving, which this batch
system does not do.

| monthly item | assumption | estimate USD |
|---|---|---:|
| compute | 128-GiB on-demand node, approximately $0.86/hour, 2 h/day × 30 | 52 |
| object storage | 10× silver/gold plus 10× observed bronze growth, ~5 GB at S3 Standard $0.023/GB-month | <1 |
| 100-GB work disk | scheduled container scratch/EBS allowance | 8 |
| API/egress | Socrata/ArcGIS inbound free; same-region storage transfer free; ~130 cached weather cells/day | 0 |
| orchestration | Dagster OSS on the same node | 0 |
| total | rounded planning estimate | **about 60/month** |

AWS on-demand and S3 rates are dated 2026-09-09 ([EC2](https://aws.amazon.com/ec2/pricing/on-demand/),
[S3](https://aws.amazon.com/s3/pricing/)). Open-Meteo's free evaluation tier is 600/minute,
10,000/day and non-commercial only ([pricing](https://open-meteo.com/en/pricing)); the
projected ~130 daily cache misses are below both, while the 10,000 daily cap—not the burst
rate—would bind first as geography expands. Production commercial use needs a paid plan,
quoted rather than invented here. Dagster+ is optional: Solo is $10/month plus credits and
serverless compute is $0.010/minute ([pricing](https://dagster.io/pricing)); it is excluded
because OSS is co-located.

## Decisions and rejected alternatives

- **2026-09-09 — Dagster OSS:** chosen for first-class partitioned assets and blocking
  checks; Prefect/Airflow add no benefit to this local asset graph.
- **2026-09-09 — hand-rolled contracts:** retained because one DuckDB `DESCRIBE` plus
  generated predicates covers the required semantics; Great Expectations/Soda and dbt add
  stores/configuration without stronger boundaries here.
- **2026-09-09 — immutable bronze:** backfills never refetch it; crash-date acquisition
  was rejected because it loses late and restated records.
- **2026-09-09 — deterministic failures do not retry:** only network work retries.
- **2026-09-09 — single node:** PostGIS was rejected absent concurrent serving;
  Spark/Sedona remains the documented national-scale escape hatch.

Hosted Dagster, Slack/PagerDuty alerting, a metadata catalogue, dbt, and changing any
pipeline computation are explicitly outside this phase.
