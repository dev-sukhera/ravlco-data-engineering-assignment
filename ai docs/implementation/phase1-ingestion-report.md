# Phase 1 — Bronze Ingestion: Build Report

Scope: `src/ingest/` (http.py, watermark.py, montgomery.py, txdot.py, fars.py).
Status: complete and verified end to end.

---

## What I built

**`src/ingest/http.py`** — one retrying, rate-paced client for all three sources.
Retries only transient failures (429/5xx/connection resets), honours `Retry-After`
in both delta-seconds and HTTP-date form, exponential jitter otherwise.
`download()` streams and hashes large files.

**`src/ingest/watermark.py`** — DuckDB store at `data/bronze/_watermarks.duckdb`,
three tables: `watermarks` (current cursor), `watermark_history` (append-only
advance log), `bronze_manifest` (path, sha256, bytes, row count, upstream
Last-Modified/ETag per artifact). The cursor is source-shaped JSON because the
three sources genuinely have different cursors. The bronze write helpers live in
this file next to `advance()` so both halves of the write-then-advance contract
read together; the ordering argument is in `advance()`'s docstring and every call
site points back at it.

**`src/ingest/montgomery.py`** — keyset on `(:updated_at, :id)`, watermark on
`:updated_at`, three datasets, `python -m src.ingest.montgomery --since 2026-01-01`
runs exactly as the README says.

**`src/ingest/txdot.py`** — OBJECTID keyset sweep with a chunked `returnIdsOnly`
universe snapshot and a hole check against it.

**`src/ingest/fars.py`** — HEAD-then-hash restatement detection, full-refresh into
a new partition on a changed hash.

**`src/config.py`** — new; ingest/transform/geo/compliance all need `sources.toml`
+ `settings.toml`.

**`tests/test_idempotency.py`** — 8 tests against local HTTP servers implementing
enough SoQL and enough of NHTSA's static host to answer the real queries, so the
table can be mutated mid-run.

---

## Two things the spec said that the live services don't do

1. **The tuple predicate doesn't compile.** `(:updated_at, :id) > ('T','ID')`
   returns `query.compiler.malformed` — this Socrata has no row-value
   constructor. I use the hand-expanded disjunction, verified against the
   server's own `$order` collation. That check matters: `:id` values are opaque
   strings that don't sort in ASCII order, but the filter and the ORDER BY agree
   with each other, which is all keyset needs.
2. **The OID field is `ESRI_OID`, not `OBJECTID`**, and unbounded
   `returnIdsOnly` doesn't survive — `where=1=1&returnIdsOnly=true` over 3.09M
   ids ran past 240s and came back as truncated JSON. The snapshot is chunked;
   the OID field is read from the layer descriptor.

---

## What I bounded

**TxDOT: 100,000 rows (OIDs 1–100,000), not 3.09M.** Measured live: 1,545 pages
× ~10.7s × ~8.6MB = ~4.6h and ~13GB raw. `--full` removes the bound,
`--oid-min/--oid-max` place it anywhere. The bound is recorded in the cursor as a
sticky `bounded` flag, so a slice can't later be mistaken for a census. Pages are
gzipped with the manifest sha256 over the *uncompressed* bytes, so
`gunzip -c page_00001.json.gz | shasum -a 256` still proves byte-for-byte fidelity.

Montgomery and FARS are **not** bounded — full history, all six years.

---

## Three real bugs the testing caught

- **`download()`'s retry covered the request but not the body.** A live 32MB FARS
  transfer stalled at 26MB and killed the run — `iter_content` raised from
  outside the retry loop. The body is now inside the retry and resumes with
  `Range` + `If-Range`; the validator matters specifically because this source is
  revised in place, so a naive resume across a revision would splice two versions
  into one corrupt zip. Verified against a server that truncates the first
  response.
- **`load_ts` had second resolution**, so two runs of the same dataset in one
  second shared a partition and the second overwrote the first's raw pages — in a
  layer whose entire contract is append-only. Now millisecond, with
  `new_partition()` allocating via `mkdir` without `exist_ok`.
- **A TxDOT sweep ending on an empty page never marked itself complete**, so the
  cursor would have stayed `in_progress` forever and the full re-sweep that
  detects `amend_supp_fl` restatements would never have happened.

---

## Verification (after a clean wipe of `data/bronze`)

| Check | Result |
|---|---|
| Montgomery latest partition | 125,005 / 220,043 / 7,521 rows, distinct `:id` 1:1 with rows — **exactly** the live server counts |
| Montgomery re-run | 0 new rows, 0 duplicate ids; two partitions coexisting |
| TxDOT | 100,000 rows, 100,000 distinct OIDs contiguous 1–100,000, **0 holes** against the snapshot |
| FARS | 6 years, 192 members, 68 sentinel coordinates preserved as strings |
| Concurrent MoCo + FARS on one watermark store | no lock contention |
| Tests | 8 passed |

---

## For DECISIONS.md

- Watermark storage: DuckDB, source-shaped JSON cursor, and *why* per-operation
  connections (the lock would otherwise serialise parallel orchestrator assets).
- **Write-then-advance ordering** and the explicit choice of which failure to
  take: a duplicate raw page under a new load_ts (absorbable, visible) over
  silently-skipped rows (undetectable).
- TxDOT slice bound + the `--full` path, with the measured 4.6h/13GB figure.
- TxDOT cursor scoped to one sweep, not across sweeps — and that `ESRI_OID` is
  service-assigned, so `crash_id` is the natural key silver diffs on.
- Bronze does **not** set `outSR`; TxDOT geometry stays in native wkid 3081.
  Reprojection is a transformation and belongs in silver.
- gzip for TxDOT raw pages, with sha256 over uncompressed bytes.
- `src/config.py` and `pytest` added to the scaffold (the scaffold's own tests
  import pytest and it was missing from requirements.txt).
- The SoQL row-value-constructor limitation, since a reviewer will look for the
  tuple syntax.

---

## For DATA_QUALITY.md

Measured, ready to cite:

- **MoCo `:updated_at` is a bulk-reload artifact for most history** — 125,005
  incident rows share `2024-06-12T20:28:27.326`. It's a valid mutation stamp
  going forward, but it does not date pre-June-2024 edits. This is also the
  concrete argument for the `:id` tiebreaker: a single-column cursor loops
  forever or skips 124k rows in one step.
- **The mixed `driver_substance_abuse` dictionary appears in Incidents too**, not
  just Drivers — `"N/A, NONE DETECTED"` in `bhju-22kf`. The assignment only points
  at `mmzv-x632`.
- **TxDOT coordinate precedence, quantified**: 68,932 / 100,000 rows (68.9%) have
  null `rpt_latitude` with a populated derived `latitude`. That's the empirical
  case for preferring the CRIS-derived pair.
- **TxDOT null geometry**: 7,279 / 100,000 (7.3%).
- **TxDOT `amend_supp_fl=1`**: 5,936 / 100,000 (5.9%) — restatement is not an edge
  case here.
- **TxDOT universe**: 3,088,450 rows, OIDs contiguous 1–3,088,450, 189 fields,
  geometry wkid 3081.
- **FARS zip layout is not stable across years**: 2019 stores 27 CSVs at the zip
  root, 2020+ nest 33 inside a folder. Member names are flattened to a lowercased
  stem, and a collision now raises rather than silently overwriting a table.
- **FARS encoding varies per member within one zip** — cp1252 and utf-8-sig side
  by side in the same year.
- **FARS revision dates confirm the restatement problem, and it's worse than the
  brief says**: 2021 carries Last-Modified `2026-06-24`, *later* than
  2022/2023/2024's `2026-04-01`. The most recently revised file is not the most
  recent year, so year order tells you nothing about revision order.

CRSS was not touched anywhere, and no sentinel normalization happens in bronze.
