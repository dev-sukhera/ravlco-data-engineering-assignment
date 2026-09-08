"""Gold + the party fixture in; leads, lineage, the exclusion table and the vault out.

    python -m src.compliance.build
    python -m src.compliance.build --as-of 2026-09-01
    python -m src.compliance.build --gold-root /tmp/g --out-root /tmp/out
    python -m src.compliance.build --ruleset /tmp/rules.yaml --blackout /tmp/bw.csv
    python -m src.compliance.build --json

Stage order, and which parts of it are a correctness constraint
---------------------------------------------------------------
    vault -> enrich -> match -> consent artefacts -> evaluate -> score
          -> crash-only -> exclusion table -> validate -> write -> manifest

`evaluate` cannot run before `enrich`, because four gates read fields the geo
stage produces (`envelope_status`, `tz_source`, `tz_iana`, `snap_status`) and a
missing field is a silently-passing gate. `validate` runs BEFORE the first
write, as in Phases 2-5: a contract failure must leave the previous outputs
exactly as they were, because a half-replaced output directory is worse than a
stale one -- the stale one is at least internally consistent.

The crash-only run is not an afterthought; it is the deliverable. It feeds the
same engine every gold crash with no identity layer attached, and its counts
per jurisdiction are the memo's answer to ASSIGNMENT.md Part 7's direct
question. The fixture run demonstrates the machinery; this one states the
truth about the product.

Determinism
-----------
Two runs over unchanged inputs produce byte-identical parquet, a
byte-identical `output/sample_leads.csv`, identical manifest values except
`built_at`, and ZERO appended lineage rows. Everything that could break that
is removed rather than tolerated:

  * `evaluated_at` and `ingested_at` are frozen (`as_of` at 00:00 UTC; the
    fixture's git commit time). Wall clock appears only in the manifest.
  * Every table is written by `common.write_parquet`, which REFUSES a
    non-total sort order rather than producing bytes that depend on thread
    scheduling.
  * `decision_lineage_id` is content-addressed, so a re-run recognises every
    row it already wrote.
  * `_compliance_build_sha` hashes the INPUTS -- gold's parquet hashes, the
    ruleset file, the blackout table and the `[compliance]` config block --
    never the build time. It moves exactly when something that could change a
    decision changes.

Two things are deliberately NOT part of the byte-identity claim, and both are
named in the report rather than quietly excluded: `_compliance_manifest.json`
(it carries `built_at` by construction, exactly as Phases 2-5 do) and
`data/vault/access_log/<build_sha>.parquet` (an audit log that reset itself on
every rebuild would not be an audit log -- it is content-addressed by the
build sha instead, so an unchanged build rewrites the same partition with
identical bytes and a changed build writes a new one beside it).

Importable and side-effect-free at import, so a Phase 8 Dagster asset can wrap
`build_compliance()` without inheriting a CLI.
"""

from __future__ import annotations

import argparse
import csv
import hashlib
import json
import logging
import sys
from datetime import date, datetime, time, timezone
from pathlib import Path
from typing import Any, Iterable, Mapping, Sequence

import duckdb
import jsonschema
import pandas as pd
import pyarrow as pa

from .. import config, contracts
from ..config import GOLD_DIR, REPO_ROOT
from ..ingest.watermark import durable_replace
from ..transform import common as c
from . import consent as consent_mod
from . import fl_incompleteness as fl_mod
from . import leads as leads_mod
from . import retention as retention_mod
from . import vault as vault_mod
from .engine import EligibilityEngine
from .lineage import LINEAGE_COLUMNS, LineageStore
from .reason_codes import IdentityProvenance
from ..scoring import score as score_mod

log = logging.getLogger("compliance.build")

LEADS = "leads"
DECISION_LINEAGE = "decision_lineage"
CRASH_ONLY = "crash_only_decisions"
EXCLUSION = "exclusion_by_code"

RUN_FIXTURE = "fixture_leads"
RUN_CRASH_ONLY = "crash_only"

# table -> (contract table, total sort order). The sort order is asserted
# total by `common.write_parquet`, not assumed.
TABLE_SPECS: dict[str, tuple[str, list[str]]] = {
    LEADS: ("compliance.leads", ["lead_id"]),
    DECISION_LINEAGE: ("compliance.decision_lineage", ["decision_lineage_id"]),
    CRASH_ONLY: ("compliance.crash_only_decisions", ["crash_sk"]),
    EXCLUSION: ("compliance.exclusion_by_code",
                ["run", "jurisdiction", "eligibility_status", "reason_code"]),
}

# The contract's column order for `leads`, read from the schema so the two can
# never drift: the schema IS the order (x-column-order-is-contract).
def _columns(table: str) -> list[str]:
    doc = contracts.load_contract(contracts.COMPLIANCE_CONTRACT)
    return list(doc["tables"][table]["properties"])


# ---------------------------------------------------------------------------
# arrow schemas -- explicit, because an all-null column has no type
# ---------------------------------------------------------------------------

_TS = pa.timestamp("us", tz="UTC")

GEO_STRUCT = pa.struct([
    ("zip5", pa.string()), ("census_tract", pa.string()), ("census_bg", pa.string()),
    ("h3_r8", pa.string()), ("iana_timezone", pa.string()),
    ("road_class", pa.string()), ("snap_distance_m", pa.float64()),
])
WINDOW_STRUCT = pa.struct([
    ("earliest", pa.string()), ("latest", pa.string()), ("basis", pa.string()),
])
CONTACT_STRUCT = pa.struct([
    ("phone_token", pa.string()), ("line_type", pa.string()),
    ("line_type_asof", pa.string()), ("dnc_scrub_asof", pa.string()),
    ("rnd_response", pa.string()), ("calling_window_local", WINDOW_STRUCT),
])
CONSENT_STRUCT = pa.struct([
    ("obtained_at", pa.string()), ("disclosure_hash", pa.string()),
    ("disclosure_url", pa.string()), ("ip_address", pa.string()),
    ("user_agent", pa.string()), ("sellers_named", pa.list_(pa.string())),
    ("source_chain", pa.list_(pa.string())), ("revoked_at", pa.string()),
])

LEADS_SCHEMA = pa.schema([
    ("lead_id", pa.string()), ("source_system", pa.string()),
    ("source_record_id", pa.string()), ("ingested_at", _TS),
    ("incident_date", pa.date32()), ("report_filing_date", pa.date32()),
    ("jurisdiction", pa.string()), ("geo", GEO_STRUCT),
    ("severity_ordinal", pa.int32()), ("contact", CONTACT_STRUCT),
    ("consent", CONSENT_STRUCT), ("eligibility_status", pa.string()),
    ("blocked_until_date", pa.date32()),
    ("reason_codes", pa.list_(pa.string())), ("legal_basis", pa.list_(pa.string())),
    ("decision_lineage_id", pa.string()), ("evaluated_at", _TS),
    ("ruleset_version", pa.string()),
    # JSON text here is the parquet encoding of the object carried by the lead
    # contract and CSV.  An explicit type keeps an all-null small run stable.
    ("priority_score", pa.float64()), ("score_components", pa.string()),
    ("party_token", pa.string()), ("party_id_label", pa.string()),
    ("crash_sk", pa.int64()), ("match_method", pa.string()),
    ("identity_provenance", pa.string()), ("tz_source", pa.string()),
    ("snap_status", pa.string()), ("envelope_status", pa.string()),
    ("_compliance_build_sha", pa.string()), ("_geo_build_sha", pa.string()),
    ("ruleset_sha256", pa.string()), ("blackout_sha256", pa.string()),
])

CRASH_ONLY_SCHEMA = pa.schema([
    ("crash_sk", pa.int64()), ("jurisdiction", pa.string()),
    ("source_system", pa.string()), ("crash_date", pa.date32()),
    ("eligibility_status", pa.string()), ("blocked_until_date", pa.date32()),
    ("reason_codes", pa.string()), ("decision_lineage_id", pa.string()),
    ("evaluated_at", _TS), ("ruleset_version", pa.string()),
    ("_compliance_build_sha", pa.string()),
])

EXCLUSION_SCHEMA = pa.schema([
    ("run", pa.string()), ("jurisdiction", pa.string()),
    ("eligibility_status", pa.string()), ("reason_code", pa.string()),
    ("disposition", pa.string()), ("records", pa.int64()),
])

LINEAGE_SCHEMA = pa.schema([(name, pa.string()) for name in LINEAGE_COLUMNS])


# ---------------------------------------------------------------------------
# the manifest
# ---------------------------------------------------------------------------


class ComplianceManifest:
    """Inputs by hash, decisions by status and code, outputs by hash.

    The only artefact here that carries wall-clock time, which is why the
    byte-identity check is defined over the parquet and the CSV and not over
    the whole directory.
    """

    def __init__(self, out_root: Path, gold_root: Path) -> None:
        self.out_root = out_root
        self.gold_root = gold_root
        self.built_at = datetime.now(timezone.utc).isoformat()
        self.inputs: dict[str, Any] = {}
        self.outputs: dict[str, Any] = {}
        self.stats: dict[str, Any] = {}
        self.warnings: list[str] = []

    def warn(self, message: str) -> None:
        log.warning("%s", message)
        self.warnings.append(message)

    def write(self, path: Path | None = None) -> Path:
        dest = path or (self.out_root / "_compliance_manifest.json")
        dest.parent.mkdir(parents=True, exist_ok=True)
        payload = {
            "built_at": self.built_at,
            "gold_root": str(self.gold_root),
            "out_root": str(self.out_root),
            "inputs": dict(sorted(self.inputs.items())),
            "outputs": dict(sorted(self.outputs.items())),
            "stats": dict(sorted(self.stats.items())),
            "warnings": self.warnings,
        }
        tmp = dest.with_name(dest.name + ".part")
        tmp.write_text(json.dumps(payload, indent=2, default=str) + "\n")
        durable_replace(tmp, dest)
        return dest


def build_sha(
    *, gold_hashes: Mapping[str, str], ruleset_sha: str, blackout_sha: str,
    fixture_sha: str, npa_sha: str, as_of: date, scoring_sha: str = "",
) -> str:
    """A hash of the INPUTS. Never of the build time, never of the output.

    Moves exactly when something that could change a decision moves: a gold
    table, the ruleset, the blackout window table, the NPA fallback table, the
    fixture, the `[compliance]` config block, or the evaluation date. That is
    what makes it a restatement marker rather than a decoration.
    """
    h = hashlib.sha256()
    for name, sha in sorted(gold_hashes.items()):
        h.update(f"{name}={sha}".encode())
    for label, value in (("ruleset", ruleset_sha), ("blackout", blackout_sha),
                         ("fixture", fixture_sha), ("npa", npa_sha),
                         ("as_of", as_of.isoformat()), ("scoring", scoring_sha)):
        h.update(f"{label}={value}".encode())
    h.update(json.dumps(config.compliance(), sort_keys=True, default=str).encode())
    h.update(json.dumps(config.scoring(), sort_keys=True, default=str).encode())
    return h.hexdigest()


def _sha(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def gold_hashes(gold_root: Path) -> dict[str, str]:
    manifest = gold_root / "_build_manifest.json"
    if manifest.exists():
        payload = json.loads(manifest.read_text())
        return {k: v["sha256"] for k, v in payload.get("outputs", {}).items()}
    return {p.stem: _sha(p) for p in sorted(gold_root.glob("*.parquet"))}


def geo_build_sha(gold_root: Path) -> str | None:
    """`_geo_build_sha` read from the crash_geo COLUMN, not from the manifest.

    Phase 5's last commit fixed exactly this: the manifest is a JSON file
    beside the data and the column is in the data, and the column is the one a
    row's lineage actually points at.
    """
    path = gold_root / "crash_geo.parquet"
    if not path.exists():
        return None
    con = duckdb.connect()
    try:
        p = str(path).replace("'", "''")
        row = con.execute(
            f"SELECT DISTINCT _geo_build_sha FROM read_parquet('{p}') "
            f"WHERE _geo_build_sha IS NOT NULL LIMIT 2"
        ).fetchall()
    finally:
        con.close()
    if len(row) != 1:
        return None
    return str(row[0][0])


# ---------------------------------------------------------------------------
# the build
# ---------------------------------------------------------------------------


def build_compliance(
    *,
    gold_root: Path | str | None = None,
    out_root: Path | str | None = None,
    vault_dir: Path | str | None = None,
    ruleset_path: Path | str | None = None,
    blackout_path: Path | str | None = None,
    reference_root: Path | str | None = None,
    sample_path: Path | str | None = None,
    schema_check_path: Path | str | None = None,
    as_of: date | None = None,
    crash_only_limit: int | None = None,
    skip_snap: bool = False,
    skip_crash_only: bool = False,
    validate: bool = True,
    small_corpus: bool = False,
    write_sample: bool = True,
) -> dict[str, Any]:
    """Run the whole phase. Returns the manifest payload."""
    cfg = config.compliance()
    gold = Path(gold_root) if gold_root is not None else GOLD_DIR
    out = Path(out_root) if out_root is not None else gold / str(cfg["out_subdir"])
    vault_path = vault_mod.vault_root(vault_dir)
    as_of = as_of or date.fromisoformat(str(cfg["as_of_date"]))
    fixture = Path(cfg["fixture_path"])
    if not fixture.is_absolute():
        fixture = REPO_ROOT / fixture

    manifest = ComplianceManifest(out, gold)
    engine = EligibilityEngine(
        ruleset_path=ruleset_path, blackout_path=blackout_path, as_of=as_of
    )
    npa_path = config.compliance_path("npa_timezone_path")

    hashes = gold_hashes(gold)
    scoring_path = config.CONFIG_DIR / "scoring.toml"
    scoring_sha = _sha(scoring_path)
    sha = build_sha(
        gold_hashes=hashes, ruleset_sha=engine.rules.sha256,
        blackout_sha=engine.blackout_sha256, fixture_sha=_sha(fixture),
        npa_sha=_sha(npa_path), as_of=as_of, scoring_sha=scoring_sha,
    )
    engine.build_sha = sha
    geo_sha = geo_build_sha(gold)

    manifest.inputs = {
        "as_of": as_of.isoformat(),
        "gold": hashes,
        "geo_build_sha": geo_sha,
        "ruleset": {"path": str(engine.rules.path), "version": engine.rules.version,
                    "sha256": engine.rules.sha256},
        "blackout": {"path": str(engine.blackout_path),
                     "sha256": engine.blackout_sha256,
                     "rows": len(engine.blackout)},
        "npa_timezone": {"path": str(npa_path), "sha256": _sha(npa_path)},
        "fixture": {"path": str(fixture), "sha256": _sha(fixture)},
        "scoring": {"path": str(scoring_path), "sha256": scoring_sha},
        "compliance_build_sha": sha,
        "config": dict(sorted(config.compliance().items())),
    }

    # -- 1. vault -------------------------------------------------------
    vault = vault_mod.Vault(root=vault_path, as_of=as_of, build_sha=sha)
    vault_mod.load_fixture_into_vault(
        vault, fixture, actor=leads_mod.ACTOR,
        purpose="build fixture-derived leads (ASSIGNMENT.md Part 5 harness)",
    )
    parties = vault.analytic_view(
        actor=leads_mod.ACTOR, purpose="party enrichment and eligibility evaluation"
    )
    # The coordinates never leave the vault as an OUTPUT column; the geo stage
    # reads them here and emits only derived values. This is the one read that
    # touches them and it is logged like every other.
    raw = vault.read(vault_mod.PARTIES, actor=leads_mod.ACTOR,
                     purpose="coordinate enrichment (derived geography only)")
    parties = parties.assign(
        party_latitude=raw["party_latitude"].to_numpy(),
        party_longitude=raw["party_longitude"].to_numpy(),
    )

    # -- 2. enrichment --------------------------------------------------
    enrichment = leads_mod.enrich_parties(
        parties, reference_root=Path(reference_root) if reference_root else None,
        skip_snap=skip_snap,
    )
    manifest.stats["enrichment"] = enrichment.stats

    # -- 3. crash match -------------------------------------------------
    matches, match_stats = leads_mod.match_crashes(
        parties, enrichment.frame, gold_root=gold
    )
    manifest.stats["crash_match"] = match_stats

    # -- 4. engine records + consent artefacts --------------------------
    honour = int(engine.rules.consent_spec["revocation"]["honour_business_days"])
    internal = _internal_dnc_tokens(vault)
    ebr = _ebr_events(vault)
    records, provenances, revocations = leads_mod.build_records(
        parties, enrichment.frame, matches, as_of=as_of,
        honour_business_days=honour, seller=engine.seller,
        internal_dnc=internal, ebr_by_token=ebr,
    )

    # -- 5. evaluate ----------------------------------------------------
    ingested_at, ingested_basis = leads_mod.fixture_ingested_at(fixture, as_of)
    store = LineageStore.open(out / f"{DECISION_LINEAGE}.parquet")
    rows: list[dict[str, Any]] = []
    candidates: list[dict[str, Any]] = []
    lineage_rows: list[dict[str, Any]] = []
    for record in records:
        decision = engine.evaluate(record)
        store.add(decision.lineage_row)
        lineage_rows.append(decision.lineage_row)
        row = leads_mod.lead_row(record, decision, ingested_at=ingested_at,
                                 as_of=as_of, geo_build_sha=geo_sha)
        row.update(leads_mod.lineage_columns(record, decision, build_sha=sha,
                                             geo_build_sha=geo_sha))
        candidates.append({**row, **{
            key: record.get(key) for key in (
                "pedestrian_involved", "bicyclist_involved", "hit_run",
                "fhwa_class", "is_adverse", "crash_snap_status",
                "osm_maxspeed_mph", "weather_status", "era5_precipitation_mm"
            )
        }})
        # Normalised ONCE, here, so the parquet, the CSV and the jsonschema
        # payload are all the same object. pandas' float NaN is not JSON null
        # and `jsonschema` rejects it against `["string", "null"]`; a row that
        # validates in one encoding and not in another is the kind of drift
        # the committed schema-check file exists to make impossible.
        rows.append(_denan(row))

    # -- 6. score after the legal status gate ---------------------------
    scored = score_mod.score_leads(
        leads_mod.score_inputs(candidates), as_of=as_of
    )
    scores_by_id = {item["lead_id"]: item for item in scored}
    for row in rows:
        item = scores_by_id.get(row["lead_id"])
        if item is not None:
            row["priority_score"] = item["priority_score"]
            row["score_components"] = item["score_components"]

    manifest.stats["fixture"] = _fixture_stats(rows, records, engine, ingested_basis)
    manifest.stats["traps"] = _trap_derivations(records, rows)

    # -- 7. the crash-only (production-truth) run -----------------------
    crash_rows: list[dict[str, Any]] = []
    if not skip_crash_only:
        limit = (crash_only_limit if crash_only_limit is not None
                 else int(cfg["crash_only_sample_per_jurisdiction"]))
        crash_rows, crash_stats = _crash_only(engine, gold, sha, limit=limit)
        manifest.stats["crash_only"] = crash_stats
        for row in crash_rows:
            store.add(row.pop("_lineage_row"))

    # -- 8. the exclusion table -----------------------------------------
    exclusion = _exclusion_by_code(engine, rows, crash_rows)

    # -- 9. monitoring and retention ------------------------------------
    monitor = fl_mod.Monitor.from_rules(engine.rules)
    manifest.stats["fl_incompleteness"] = _fl_stats(monitor, rows, gold, as_of, cfg)
    manifest.stats["retention"] = retention_mod.as_manifest(
        retention_mod.report(
            engine.rules,
            {
                "consent_provenance": pd.DataFrame([p.as_row() for p in provenances]),
                "revocations": pd.DataFrame([r.as_row() for r in revocations]),
                "decision_lineage": store.frame(),
                "vault_access_log": vault.access_frame(),
                "vault_parties": parties,
            },
            as_of=as_of,
        )
    )

    # -- 10. validate BEFORE the first write ----------------------------
    tables = {
        LEADS: pa.Table.from_pylist(
            [_arrow_lead(row) for row in rows], schema=LEADS_SCHEMA),
        DECISION_LINEAGE: pa.Table.from_pylist(
            [{k: _str_or_none(r.get(k)) for k in LINEAGE_COLUMNS}
             for r in store.frame().to_dict("records")],
            schema=LINEAGE_SCHEMA),
        EXCLUSION: pa.Table.from_pylist(exclusion, schema=EXCLUSION_SCHEMA),
    }
    if crash_rows:
        tables[CRASH_ONLY] = pa.Table.from_pylist(
            [{k: row.get(k) for k in CRASH_ONLY_SCHEMA.names} for row in crash_rows],
            schema=CRASH_ONLY_SCHEMA)

    con = c.connect()
    try:
        for name, table in tables.items():
            con.register(f"v_{name}", table)
        if validate:
            _validate(con, tables, small_corpus=small_corpus)
        sample = rows[: int(len(rows))]
        row_results = validate_lead_rows(sample)
        failed = [r for r in row_results if not r["valid"]]
        if failed and validate:
            raise contracts.ContractViolation(
                f"{len(failed)} of {len(row_results)} lead row(s) fail "
                f"contracts/lead_output.schema.json:\n"
                + "\n".join(f"  {r['lead_id']}: {r['error']}" for r in failed[:5])
            )

        # -- 11. write --------------------------------------------------
        out.mkdir(parents=True, exist_ok=True)
        for name, table in tables.items():
            _contract_table, order = TABLE_SPECS[name]
            manifest.outputs[name] = c.write_parquet(
                con, f"v_{name}", out / f"{name}.parquet",
                columns=list(table.schema.names), order_by=order,
            )
    finally:
        con.close()

    _write_vault_side(vault, provenances, revocations)
    manifest.outputs["vault_access_log"] = {
        "path": str(vault.flush_access_log()), "rows": vault.access_count,
    }
    manifest.stats["vault"] = {
        "root": str(vault_path),
        "access_rows": vault.access_count,
        "tables": sorted(vault_mod.TABLES),
        "analytic_columns": list(vault_mod.ANALYTIC_COLUMNS),
        "direct_identifiers_withheld": list(vault_mod.DIRECT_IDENTIFIERS),
    }
    manifest.stats["lineage"] = {
        "rows_total": store.total, "appended": store.appended, "reused": store.reused,
    }

    if write_sample:
        sample_dest = Path(sample_path) if sample_path is not None else \
            REPO_ROOT / str(cfg["sample_leads_path"])
        check_dest = Path(schema_check_path) if schema_check_path is not None else \
            REPO_ROOT / str(cfg["sample_schema_check"])
        manifest.outputs["sample_leads_csv"] = write_sample_csv(rows, sample_dest)
        manifest.outputs["sample_schema_check"] = write_schema_check(
            row_results, check_dest, ruleset_version=engine.rules.version, as_of=as_of
        )

    manifest.stats["scoring"] = {
        "phase": 7,
        "rows_handed_to_scorer": len(scored),
        "config_sha256": scoring_sha,
        "note": ("ELIGIBLE and BLOCKED_UNTIL only. An INELIGIBLE record is not a "
                 "lead with a low score, it is not a lead."),
    }
    manifest.write()
    return {
        "built_at": manifest.built_at, "out_root": str(out), "gold_root": str(gold),
        "as_of": as_of.isoformat(), "compliance_build_sha": sha,
        "inputs": manifest.inputs, "outputs": manifest.outputs,
        "stats": manifest.stats, "warnings": manifest.warnings,
    }


# ---------------------------------------------------------------------------
# stages
# ---------------------------------------------------------------------------


def _internal_dnc_tokens(vault: vault_mod.Vault) -> set[str]:
    frame = vault.read(vault_mod.INTERNAL_DNC, actor=leads_mod.ACTOR,
                       purpose="internal company-specific DNC suppression check",
                       columns=["party_token"])
    return set(frame["party_token"].astype(str)) if not frame.empty else set()


def _ebr_events(vault: vault_mod.Vault) -> dict[str, list[dict[str, Any]]]:
    frame = vault.read(vault_mod.EBR, actor=leads_mod.ACTOR,
                       purpose="established business relationship lookup")
    out: dict[str, list[dict[str, Any]]] = {}
    for row in frame.to_dict("records") if not frame.empty else []:
        out.setdefault(str(row["party_token"]), []).append(
            {"kind": row.get("kind"), "occurred_at": row.get("occurred_at")}
        )
    return out


def _write_vault_side(
    vault: vault_mod.Vault,
    provenances: Sequence[consent_mod.ConsentProvenance],
    revocations: Sequence[consent_mod.Revocation],
) -> None:
    """Append-only, so a re-run adds nothing and never rewrites a stored row."""
    if provenances:
        vault.append(vault_mod.CONSENT_PROVENANCE,
                     [p.as_row() for p in provenances], order_by=["party_token"])
    if revocations:
        vault.append(vault_mod.REVOCATIONS, [r.as_row() for r in revocations],
                     order_by=["party_token", "seller", "received_at"])


def _crash_only(
    engine: EligibilityEngine, gold: Path, sha: str, *, limit: int
) -> tuple[list[dict[str, Any]], dict[str, Any]]:
    """Every gold crash with NO identity layer. The memo's answer, computed.

    Stratified per jurisdiction by default (`limit`), taken deterministically
    by `ORDER BY crash_sk` rather than by a random sample: the decision is a
    pure function of (jurisdiction, provenance, dates, geo tier), so the
    distinct outcomes saturate long before the corpus does and a seeded random
    sample would only add a seed to defend. `limit = 0` runs all of them.
    """
    fact = gold / "fact_crash.parquet"
    if not fact.exists():
        return [], {"attempted": False, "reason": f"no gold fact_crash at {fact}"}

    geo = gold / "crash_geo.parquet"
    con = duckdb.connect()
    try:
        f = str(fact).replace("'", "''")
        if geo.exists():
            g = str(geo).replace("'", "''")
            join = (f"LEFT JOIN read_parquet('{g}') g USING (crash_sk)")
            geo_cols = ("g.tz_iana, g.tz_source, g.snap_status, g.snap_distance_m, "
                        "g.h3_r8, g.bg_geoid, g.tract_geoid")
        else:
            join = ""
            geo_cols = ("NULL AS tz_iana, NULL AS tz_source, NULL AS snap_status, "
                        "NULL AS snap_distance_m, NULL AS h3_r8, NULL AS bg_geoid, "
                        "NULL AS tract_geoid")
        window = "" if limit <= 0 else (
            f"QUALIFY row_number() OVER (PARTITION BY f.jurisdiction "
            f"ORDER BY f.crash_sk) <= {int(limit)}"
        )
        frame = con.execute(
            f"""SELECT f.crash_sk, f.jurisdiction, f.primary_source_system,
                       f.crash_date, f.geo_quality, f.severity_ordinal, {geo_cols}
                FROM read_parquet('{f}') f {join}
                {window}
                ORDER BY f.crash_sk"""
        ).df()
        total = con.execute(
            f"SELECT jurisdiction, COUNT(*) FROM read_parquet('{f}') GROUP BY 1"
        ).fetchall()
    finally:
        con.close()

    rows: list[dict[str, Any]] = []
    for row in frame.to_dict("records"):
        record = {
            "lead_id": f"CO_{int(row['crash_sk'])}",
            "party_token": None,
            "jurisdiction": str(row["jurisdiction"]),
            "identity_provenance": IdentityProvenance.PUBLIC_CRASH_REPORT.value,
            "record_types": ["crash_report", "written_solicitation",
                             "telephone_solicitation"],
            "incident_date": row["crash_date"],
            # A police crash report has no filing date in any of the three
            # feeds. The Florida 60-day data gate anchors on one, so it
            # correctly yields ANCHOR_DATE_MISSING rather than a date nobody
            # can produce -- which is itself part of the honest answer.
            "report_filing_date": None,
            "tz_iana": row.get("tz_iana"),
            "tz_source": row.get("tz_source"),
            "snap_status": row.get("snap_status"),
            "snap_distance_m": row.get("snap_distance_m"),
            "envelope_status": _envelope_from_quality(row.get("geo_quality")),
            "h3_r8": row.get("h3_r8"),
        }
        decision = engine.evaluate_crash_only(record)
        rows.append({
            "crash_sk": int(row["crash_sk"]),
            "jurisdiction": str(row["jurisdiction"]),
            "source_system": str(row["primary_source_system"]),
            "crash_date": _as_date(row["crash_date"]),
            "eligibility_status": decision.status,
            "blocked_until_date": decision.blocked_until_date,
            "reason_codes": "|".join(decision.reason_codes),
            "decision_lineage_id": decision.decision_lineage_id,
            "evaluated_at": decision.evaluated_at,
            "ruleset_version": decision.ruleset_version,
            "_compliance_build_sha": sha,
            "_lineage_row": decision.lineage_row,
        })

    by_jurisdiction: dict[str, dict[str, int]] = {}
    for row in rows:
        by_jurisdiction.setdefault(row["jurisdiction"], {}).setdefault(
            row["eligibility_status"], 0)
        by_jurisdiction[row["jurisdiction"]][row["eligibility_status"]] += 1
    return rows, {
        "attempted": True,
        "sample_per_jurisdiction": limit or "all",
        "evaluated": len(rows),
        "corpus_by_jurisdiction": {str(j): int(n) for j, n in total},
        "by_jurisdiction_status": by_jurisdiction,
        "eligible_total": sum(1 for r in rows if r["eligibility_status"] == "ELIGIBLE"),
    }


def _envelope_from_quality(quality: Any) -> str:
    """Phase 2's `geo_quality` IS the envelope verdict; it is not recomputed."""
    return str(quality) if quality is not None else c.GEO_MISSING


def _exclusion_by_code(
    engine: EligibilityEngine,
    lead_rows: Sequence[Mapping[str, Any]],
    crash_rows: Sequence[Mapping[str, Any]],
) -> list[dict[str, Any]]:
    """Counts by run x jurisdiction x status x code, over both runs."""
    counts: dict[tuple[str, str, str, str], int] = {}
    for row in lead_rows:
        for code in row["reason_codes"]:
            key = (RUN_FIXTURE, row["jurisdiction"], row["eligibility_status"], code)
            counts[key] = counts.get(key, 0) + 1
    for row in crash_rows:
        for code in str(row["reason_codes"]).split("|"):
            if not code:
                continue
            key = (RUN_CRASH_ONLY, row["jurisdiction"], row["eligibility_status"], code)
            counts[key] = counts.get(key, 0) + 1
    return [
        {"run": run, "jurisdiction": jurisdiction, "eligibility_status": status,
         "reason_code": code, "disposition": engine.rules.disposition(code),
         "records": n}
        for (run, jurisdiction, status, code), n in sorted(counts.items())
    ]


def _fixture_stats(rows, records, engine, ingested_basis: str) -> dict[str, Any]:
    by_status: dict[str, int] = {}
    by_code: dict[str, int] = {}
    by_jurisdiction: dict[str, dict[str, int]] = {}
    for row in rows:
        by_status[row["eligibility_status"]] = by_status.get(
            row["eligibility_status"], 0) + 1
        by_jurisdiction.setdefault(row["jurisdiction"], {}).setdefault(
            row["eligibility_status"], 0)
        by_jurisdiction[row["jurisdiction"]][row["eligibility_status"]] += 1
        for code in row["reason_codes"]:
            by_code[code] = by_code.get(code, 0) + 1
    return {
        "rows": len(rows),
        "ingested_at_basis": ingested_basis,
        "by_status": dict(sorted(by_status.items())),
        "by_jurisdiction_status": {k: dict(sorted(v.items()))
                                   for k, v in sorted(by_jurisdiction.items())},
        "by_reason_code": dict(sorted(by_code.items())),
        "eligible": by_status.get("ELIGIBLE", 0),
        # Named rather than buried: the fixture carries no line_type_asof, so
        # the LINE_TYPE_STALE rule cannot fire on it. Deriving one would
        # manufacture a freshness fact the fixture never asserted.
        "line_type_asof_missing": sum(
            1 for r in records if r.get("line_type_asof") is None),
        "consent_on_file": sum(1 for r in records if r.get("consent_on_file")),
        "consent_revoked": sum(1 for r in records if r.get("consent_revoked")),
    }


def _trap_derivations(records, rows) -> dict[str, Any]:
    """The two timezone trap rows, derived in full, for the report.

    fixtures/README.md: "Two records in particular exist to catch a specific
    mistake; you will find them if your timezone derivation is correct and
    miss them if it is not." This is where the pipeline shows its working.
    """
    wanted = {"P007", "P008"}
    out: dict[str, Any] = {}
    for record, row in zip(records, rows):
        label = str(record.get("party_id_label"))
        if label not in wanted:
            continue
        window = row["contact"]["calling_window_local"] or {}
        out[label] = {
            "jurisdiction": record["jurisdiction"],
            "npa": record.get("npa"),
            "coordinate_zone": record.get("tz_iana"),
            "tz_source": record.get("tz_source"),
            "jurisdiction_default_zone": config.geo()["tz"]["jurisdiction_default"].get(
                record["jurisdiction"]),
            "calling_window": window,
            "eligibility_status": row["eligibility_status"],
            "reason_codes": row["reason_codes"],
        }
    return out


def _fl_stats(monitor, rows, gold: Path, as_of: date, cfg) -> dict[str, Any]:
    """The 316.066(2) monitor over the FL leads and over FARS Florida."""
    frame = pd.DataFrame([
        {"jurisdiction": r["jurisdiction"],
         "report_filing_date": r["report_filing_date"]}
        for r in rows
    ])
    aggregate_days = int(cfg["fl_trailing_aggregate_days"])
    stats: dict[str, Any] = {
        "trailing_days": monitor.trailing_days,
        "legal_basis": monitor.legal_basis,
        "window_start": monitor.window_start(as_of).isoformat(),
        "fixture_leads": monitor.trailing_aggregate(
            frame, as_of=as_of, aggregate_days=aggregate_days, annotate=True),
    }
    fact = gold / "fact_crash.parquet"
    if fact.exists():
        con = duckdb.connect()
        try:
            p = str(fact).replace("'", "''")
            fars = con.execute(
                f"""SELECT jurisdiction, CAST(NULL AS VARCHAR) AS report_filing_date
                    FROM read_parquet('{p}') WHERE jurisdiction = 'FL'"""
            ).df()
        finally:
            con.close()
        labelled = monitor.label_rows(fars, as_of)
        stats["fars_florida"] = {
            "rows": int(len(fars)),
            "labels": {str(k): int(v) for k, v in
                       labelled[fl_mod.LABEL_COLUMN].value_counts(dropna=False).items()},
            "note": ("FARS publishes no report filing date, so the trailing-window "
                     "test cannot be evaluated on it. Labelled NOT_APPLICABLE with "
                     "the reason rather than silently treated as complete."),
        }
    return stats


# ---------------------------------------------------------------------------
# validation
# ---------------------------------------------------------------------------


def _validate(con, tables: Mapping[str, pa.Table], *, small_corpus: bool) -> None:
    violations = []
    for name in tables:
        contract_table, _order = TABLE_SPECS[name]
        violations += contracts.validate_relation(
            con, f"v_{name}",
            contracts.load_contract(contracts.COMPLIANCE_CONTRACT),
            contract_table,
            check_row_count_min=not small_corpus,
        )
    contracts.raise_for(violations, context="compliance.schema.json")


def validate_lead_rows(rows: Sequence[Mapping[str, Any]]) -> list[dict[str, Any]]:
    """Every output row against `contracts/lead_output.schema.json`, row by row.

    Format checking ON: the contract declares `format: "date"` and
    `format: "date-time"` on six fields and jsonschema ignores both by default,
    so validating without a FormatChecker would pass a `blocked_until_date` of
    "soon". `contracts/lead_output.schema.json` is NOT modified -- it is the
    interface the downstream contact centre consumes and this pipeline is the
    producer, not its owner.
    """
    schema = json.loads(contracts.LEAD_OUTPUT_CONTRACT.read_text())
    validator = jsonschema.Draft202012Validator(
        schema, format_checker=jsonschema.FormatChecker()
    )
    out: list[dict[str, Any]] = []
    for row in rows:
        payload = {k: v for k, v in row.items() if k in schema["properties"]}
        errors = sorted(validator.iter_errors(payload), key=lambda e: list(e.path))
        out.append({
            "lead_id": row.get("lead_id"),
            "valid": not errors,
            "error": None if not errors else
            f"{'/'.join(str(p) for p in errors[0].path)}: {errors[0].message}",
            "error_count": len(errors),
        })
    return out


# ---------------------------------------------------------------------------
# the committed deliverables
# ---------------------------------------------------------------------------

CSV_NESTED = ("geo", "contact", "consent", "reason_codes", "legal_basis",
              "score_components")


def write_sample_csv(rows: Sequence[Mapping[str, Any]], dest: Path) -> dict[str, Any]:
    """`output/sample_leads.csv` -- committed, fixture-derived, no PII.

    Nested contract objects and the two decision arrays are JSON-encoded in
    their cells, with `sort_keys=False` so the key order matches the contract
    and a reviewer can read a cell. The validator re-parses each cell back
    into the contract's object shape before `jsonschema.validate`, so what is
    checked is what is written.
    """
    columns = [k for k in json.loads(
        contracts.LEAD_OUTPUT_CONTRACT.read_text())["properties"]]
    dest.parent.mkdir(parents=True, exist_ok=True)
    tmp = dest.with_name(dest.name + ".part")
    with tmp.open("w", newline="", encoding="utf-8") as fh:
        writer = csv.DictWriter(fh, fieldnames=columns, lineterminator="\n")
        writer.writeheader()
        for row in sorted(rows, key=lambda r: r["lead_id"]):
            writer.writerow({
                col: (json.dumps(row.get(col), ensure_ascii=False)
                      if col in CSV_NESTED else row.get(col))
                for col in columns
            })
    durable_replace(tmp, dest)
    return {"path": str(dest), "rows": len(rows), "columns": len(columns),
            "sha256": _sha(dest), "bytes": dest.stat().st_size}


def read_sample_csv(path: Path) -> list[dict[str, Any]]:
    """Re-parse a committed sample back into contract objects. The test's eyes."""
    out: list[dict[str, Any]] = []
    with Path(path).open(newline="", encoding="utf-8") as fh:
        for raw in csv.DictReader(fh):
            row: dict[str, Any] = {}
            for key, value in raw.items():
                if key in CSV_NESTED:
                    row[key] = json.loads(value) if value else None
                elif value == "":
                    row[key] = None
                elif key in ("priority_score",):
                    row[key] = float(value)
                elif key == "severity_ordinal":
                    row[key] = int(value)
                else:
                    row[key] = value
            out.append(row)
    return out


def write_schema_check(
    results: Sequence[Mapping[str, Any]], dest: Path, *,
    ruleset_version: str, as_of: date,
) -> dict[str, Any]:
    """Cheap, committed evidence that the sample validates. Per row."""
    payload = {
        "contract": "contracts/lead_output.schema.json",
        "validator": "jsonschema Draft202012Validator, format_checker=FormatChecker()",
        "as_of": as_of.isoformat(),
        "ruleset_version": ruleset_version,
        "rows": len(results),
        "valid": sum(1 for r in results if r["valid"]),
        "invalid": sum(1 for r in results if not r["valid"]),
        "results": [dict(r) for r in results],
    }
    dest.parent.mkdir(parents=True, exist_ok=True)
    tmp = dest.with_name(dest.name + ".part")
    tmp.write_text(json.dumps(payload, indent=2) + "\n")
    durable_replace(tmp, dest)
    return {"path": str(dest), "rows": payload["rows"], "valid": payload["valid"],
            "sha256": _sha(dest)}


# The contract's row is JSON-ready: dates and timestamps are ISO STRINGS,
# because that is what `output/sample_leads.csv` carries and what
# `jsonschema` validates with `format: "date"` / `format: "date-time"`
# checking on. The parquet wants real DATE and TIMESTAMP columns. Converting
# here rather than keeping two parallel row builders means the bytes that are
# written and the payload that is validated are the SAME object, differing
# only in the encoding of six fields.
_ARROW_DATE_FIELDS = ("incident_date", "report_filing_date", "blocked_until_date")
_ARROW_TS_FIELDS = ("ingested_at", "evaluated_at")


def _arrow_lead(row: Mapping[str, Any]) -> dict[str, Any]:
    out = {k: _denan(row.get(k)) for k in LEADS_SCHEMA.names}
    if isinstance(out.get("score_components"), dict):
        out["score_components"] = json.dumps(
            out["score_components"], sort_keys=True, separators=(",", ":")
        )
    for field in _ARROW_DATE_FIELDS:
        out[field] = _as_date(out.get(field))
    for field in _ARROW_TS_FIELDS:
        out[field] = _as_datetime(out.get(field))
    return out


def _denan(value: Any) -> Any:
    """NaN -> None, recursively through the nested contract objects.

    pandas uses float NaN for a missing value in an object column, and a NaN
    reaching a pyarrow string field is an ArrowTypeError several frames from
    the column that caused it. Normalising once at the boundary is cheaper
    than defending every producer, and NULL is what a missing string means in
    the contract anyway.
    """
    if isinstance(value, float) and pd.isna(value):
        return None
    if value is pd.NaT or value is pd.NA:
        return None
    if isinstance(value, dict):
        return {k: _denan(v) for k, v in value.items()}
    if isinstance(value, list):
        return [_denan(v) for v in value]
    return value


def _as_datetime(value: Any) -> datetime | None:
    if value is None:
        return None
    if isinstance(value, datetime):
        return value if value.tzinfo else value.replace(tzinfo=timezone.utc)
    parsed = datetime.fromisoformat(str(value))
    return parsed if parsed.tzinfo else parsed.replace(tzinfo=timezone.utc)


def _str_or_none(value: Any) -> str | None:
    if value is None or (isinstance(value, float) and pd.isna(value)):
        return None
    return str(value)


def _as_date(value: Any) -> date | None:
    if value is None or pd.isna(value):
        return None
    if isinstance(value, datetime):
        return value.date()
    if isinstance(value, date):
        return value
    return date.fromisoformat(str(value)[:10])


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------


def _cli(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(
        prog="python -m src.compliance.build",
        description="Evaluate contact eligibility over the party fixture and over "
                    "gold, and write the leads, lineage and exclusion tables.",
    )
    ap.add_argument("--as-of", type=date.fromisoformat, default=None,
                    help="the frozen clock; default from config/compliance.toml")
    ap.add_argument("--gold-root", type=Path, default=None)
    ap.add_argument("--out-root", type=Path, default=None,
                    help="default: <gold-root>/compliance")
    ap.add_argument("--vault-dir", type=Path, default=None)
    ap.add_argument("--ruleset", type=Path, default=None)
    ap.add_argument("--blackout", type=Path, default=None)
    ap.add_argument("--reference-root", type=Path, default=None)
    ap.add_argument("--sample", type=Path, default=None)
    ap.add_argument("--crash-only-limit", type=int, default=None,
                    help="rows per jurisdiction; 0 for the whole corpus")
    ap.add_argument("--skip-snap", action="store_true")
    ap.add_argument("--skip-crash-only", action="store_true")
    ap.add_argument("--no-sample", action="store_true",
                    help="do not rewrite the committed output/sample_leads.csv")
    ap.add_argument("--no-validate", action="store_true")
    ap.add_argument("--small-corpus", action="store_true",
                    help="skip row_count_min floors (fixture-scale builds)")
    ap.add_argument("--json", action="store_true")
    ap.add_argument("-v", "--verbose", action="store_true")
    args = ap.parse_args(argv)
    logging.basicConfig(level=logging.DEBUG if args.verbose else logging.INFO,
                        format="%(levelname)s %(name)s %(message)s")

    result = build_compliance(
        gold_root=args.gold_root, out_root=args.out_root, vault_dir=args.vault_dir,
        ruleset_path=args.ruleset, blackout_path=args.blackout,
        reference_root=args.reference_root, sample_path=args.sample,
        as_of=args.as_of, crash_only_limit=args.crash_only_limit,
        skip_snap=args.skip_snap, skip_crash_only=args.skip_crash_only,
        validate=not args.no_validate, small_corpus=args.small_corpus,
        write_sample=not args.no_sample,
    )
    if args.json:
        print(json.dumps(result, indent=2, default=str))
    else:
        _print_summary(result)
    return 0


def _print_summary(result: Mapping[str, Any]) -> None:
    stats = result["stats"]
    print(f"\ncompliance -> {result['out_root']}   as_of {result['as_of']}")
    print(f"  ruleset {result['inputs']['ruleset']['version']} "
          f"sha {result['inputs']['ruleset']['sha256'][:16]}   "
          f"build_sha {result['compliance_build_sha'][:16]}")
    for name, info in sorted(result["outputs"].items()):
        if "sha256" in info and "rows" in info:
            print(f"  {name:<24} {info['rows']:>8} rows  {info['sha256'][:16]}")

    fixture = stats["fixture"]
    print(f"\n  fixture leads: {fixture['rows']} rows")
    for status, n in fixture["by_status"].items():
        print(f"    {status:<16} {n:>4}")
    print("    by jurisdiction:")
    for jurisdiction, by_status in fixture["by_jurisdiction_status"].items():
        print(f"      {jurisdiction}  {by_status}")
    print("    top reason codes:")
    for code, n in sorted(fixture["by_reason_code"].items(),
                          key=lambda kv: (-kv[1], kv[0]))[:12]:
        print(f"      {code:<38} {n:>4}")

    crash = stats.get("crash_only", {})
    if crash.get("attempted"):
        print(f"\n  crash-only (no identity layer): {crash['evaluated']:,} evaluated "
              f"of {sum(crash['corpus_by_jurisdiction'].values()):,}")
        for jurisdiction, by_status in sorted(crash["by_jurisdiction_status"].items()):
            print(f"    {jurisdiction}  {by_status}")
        print(f"    ELIGIBLE total: {crash['eligible_total']}")

    for label, trap in sorted(stats.get("traps", {}).items()):
        print(f"\n  trap {label}: coords -> {trap['coordinate_zone']} "
              f"(state default {trap['jurisdiction_default_zone']}), "
              f"npa {trap['npa']}")
        window = trap["calling_window"]
        if window:
            print(f"    window {window['earliest']}-{window['latest']}")
            print(f"    {window['basis']}")

    lineage = stats["lineage"]
    print(f"\n  lineage: {lineage['rows_total']} rows "
          f"({lineage['appended']} appended, {lineage['reused']} already present)")
    print(f"  vault reads logged: {stats['vault']['access_rows']}")
    for w in result["warnings"]:
        print(f"  WARNING: {w}")


if __name__ == "__main__":
    sys.exit(_cli())
