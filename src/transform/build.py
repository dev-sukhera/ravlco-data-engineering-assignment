"""Rebuild silver from bronze, deterministically.

    python -m src.transform.build
    python -m src.transform.build --source montgomery --source txdot
    python -m src.transform.build --bronze-root /tmp/b --silver-root /tmp/s
    python -m src.transform.build --allow-unmapped        # write UNMAPPED, warn
    python -m src.transform.build --no-validate           # skip the contract

One command rebuilds every silver table from whatever bronze partitions exist.
There is no incremental mode and that is the design: silver is a pure function
of bronze, bronze is append-only, and the whole local corpus rebuilds in seconds.
An incremental path would add a second code path to keep correct in exchange for
nothing at this scale, and it is exactly where restatement bugs live.

Ordering inside a run:

  1. Each source transform builds its typed views and SCD2 history tables
     in-memory. Nothing touches disk yet.
  2. The unified crash grain is assembled from those tables.
  3. Every table is validated against contracts/silver.schema.json.
  4. Only then is anything written -- to `<name>.parquet.part`, fsynced, and
     renamed into place.

Validate-then-write, not write-then-validate. A contract failure must leave the
previous silver exactly as it was: a half-replaced silver directory is worse
than a stale one, because the stale one is at least internally consistent.

`--allow-unmapped` is the drift escape hatch. Without it, a dictionary value
nobody has mapped fails the build. With it, the value is written as UNMAPPED and
the manifest records a warning -- which is the right behaviour at 3am when a new
token appears and the pipeline still has to deliver, but it is opt-in so that
"nobody noticed" is never the default.
"""

from __future__ import annotations

import argparse
import json
import logging
import sys
from pathlib import Path
from typing import Any, Iterable

from .. import contracts
from ..config import BRONZE_DIR, SILVER_DIR
from ..ingest.watermark import WatermarkStore
from . import common as c
from . import fars as fars_transform
from . import montgomery as moco_transform
from . import txdot as txdot_transform
from . import unified

log = logging.getLogger("transform.build")

ALL_SOURCES = ("montgomery", "txdot", "fars")

# (source, silver table name, in-memory relation, contract table, sort key)
#
# The sort key is a TOTAL order for every table -- write_parquet() asserts it and
# refuses the write otherwise, because a tie makes the parquet bytes depend on
# thread scheduling.
TABLES: dict[str, list[tuple[str, str, str, list[str]]]] = {
    "montgomery": [
        ("crash_history", "moco_crash_final", "montgomery.crash",
         ["natural_key", "valid_from", "_bronze_load_ts"]),
        ("crash_current", "moco_crash_current", "montgomery.crash",
         ["natural_key"]),
        ("driver_history", "moco_driver_history", "montgomery.driver",
         ["natural_key", "valid_from", "_bronze_load_ts"]),
        ("driver_current", "moco_driver_current", "montgomery.driver",
         ["natural_key"]),
        ("non_motorist_history", "moco_non_motorist_history",
         "montgomery.non_motorist", ["natural_key", "valid_from", "_bronze_load_ts"]),
        ("non_motorist_current", "moco_non_motorist_current",
         "montgomery.non_motorist", ["natural_key"]),
    ],
    "txdot": [
        ("crash_history", "txd_crash_history", "txdot.crash",
         ["natural_key", "valid_from", "_bronze_load_ts"]),
        ("crash_current", "txd_crash_current", "txdot.crash", ["natural_key"]),
    ],
    "fars": [
        ("accident_history", "fars_accident_history", "fars.accident",
         ["natural_key", "valid_from", "_bronze_load_ts"]),
        ("accident_current", "fars_accident_current", "fars.accident",
         ["natural_key"]),
        ("vehicle_history", "fars_vehicle_history", "fars.vehicle",
         ["natural_key", "valid_from", "_bronze_load_ts"]),
        ("vehicle_current", "fars_vehicle_current", "fars.vehicle",
         ["natural_key"]),
        ("person_history", "fars_person_history", "fars.person",
         ["natural_key", "valid_from", "_bronze_load_ts"]),
        ("person_current", "fars_person_current", "fars.person", ["natural_key"]),
        ("codebook", "fars_codebook", "fars.codebook",
         ["tbl", "col", "code", "label"]),
    ],
}

# The history-table column order: natural_key, the key columns, the conformed
# attributes, then the SCD2 block. Fixed here so the contract, the writer and
# every reader agree.
HISTORY_LAYOUT: dict[str, tuple[list[str], list[str]]] = {
    "montgomery.crash": (moco_transform.CRASH_KEY,
                         moco_transform.CRASH_ATTRIBUTES + moco_transform.CRASH_DERIVED),
    "montgomery.driver": (["person_id"], moco_transform.DRIVER_ATTRIBUTES),
    "montgomery.non_motorist": (["person_id"], moco_transform.NON_MOTORIST_ATTRIBUTES),
    "txdot.crash": (txdot_transform.NATURAL_KEY, txdot_transform.ATTRIBUTES),
    "fars.accident": (fars_transform.ACCIDENT_KEY, fars_transform.ACCIDENT_ATTRIBUTES),
    "fars.vehicle": (fars_transform.VEHICLE_KEY, fars_transform.VEHICLE_ATTRIBUTES),
    "fars.person": (fars_transform.PERSON_KEY, fars_transform.PERSON_ATTRIBUTES),
}


def history_columns(contract_table: str) -> list[str]:
    key, attrs = HISTORY_LAYOUT[contract_table]
    return ["natural_key", *key, *attrs, *c.SCD2_COLUMNS[1:]]


def build_silver(
    *,
    sources: Iterable[str] = ALL_SOURCES,
    bronze_root: Path | None = None,
    silver_root: Path | None = None,
    store: WatermarkStore | None = None,
    allow_unmapped: bool = False,
    validate: bool = True,
    small_corpus: bool = False,
    threads: int | None = None,
) -> dict[str, Any]:
    """Build every silver table for `sources`. Returns the manifest payload."""
    sources = [s for s in ALL_SOURCES if s in set(sources)]
    if not sources:
        raise ValueError(f"no known sources selected (known: {ALL_SOURCES})")

    bronze = Path(bronze_root) if bronze_root else BRONZE_DIR
    silver = Path(silver_root) if silver_root else SILVER_DIR
    con = c.connect(threads=threads)
    manifest = c.BuildManifest(silver_root=silver, bronze_root=bronze,
                               sources=list(sources))
    ctx = c.BuildContext(
        con=con, bronze_root=bronze, silver_root=silver, manifest=manifest,
        store=store, allow_unmapped=allow_unmapped, validate=validate,
    )

    transforms = {
        "montgomery": moco_transform.build,
        "txdot": txdot_transform.build,
        "fars": fars_transform.build,
    }
    for source in sources:
        log.info("building %s", source)
        manifest.add_stat(source, transforms[source](ctx))

    log.info("building unified crash grain")
    manifest.add_stat("unified", unified.build(ctx, sources))

    # Materialise the current slices as their own relations so the contract sees
    # exactly the rows that will be written, not a view the writer re-evaluates.
    for source in sources:
        for table, relation, contract_table, _ in TABLES[source]:
            if table.endswith("_current"):
                history = relation.replace("_current", "_history")
                if history == "moco_crash_history":
                    history = "moco_crash_final"
                con.execute(
                    f"CREATE OR REPLACE TABLE {relation} AS "
                    f"SELECT * FROM {history} WHERE is_current"
                )

    plan: list[tuple[str, str, str, list[str], list[str], Path]] = []
    for source in sources:
        for table, relation, contract_table, order_by in TABLES[source]:
            cols = (
                [r[0] for r in con.execute(
                    f"DESCRIBE SELECT * FROM {relation}").fetchall()]
                if contract_table == "fars.codebook"
                else history_columns(contract_table)
            )
            plan.append((source, table, relation, cols, order_by,
                         silver / source / f"{table}.parquet"))
    plan.append(("", unified.TABLE, "silver_crash", unified.COLUMNS,
                 ["crash_uid"], silver / f"{unified.TABLE}.parquet"))

    if validate:
        _validate(ctx, plan, sources, small_corpus=small_corpus)

    for source, table, relation, cols, order_by, dest in plan:
        info = c.write_parquet(con, relation, dest, columns=cols, order_by=order_by)
        manifest.add_output(f"{source}/{table}" if source else table, info)
        log.info("wrote %s (%s rows, %s)", dest, info["rows"], info["sha256"][:12])

    manifest.write()
    con.close()
    return {
        "outputs": manifest.outputs,
        "stats": manifest.stats,
        "warnings": manifest.warnings,
        "silver_root": str(silver),
    }


def _validate(ctx: c.BuildContext, plan, sources, *, small_corpus: bool = False) -> None:
    """Every table against contracts/silver.schema.json, before anything is written."""
    contract = contracts.load_contract(contracts.SILVER_CONTRACT)
    resolve = {
        ct: rel
        for source in sources
        for _t, rel, ct, _o in TABLES[source]
        if _t.endswith("_current")
    }
    resolve["silver.crash"] = "silver_crash"

    violations: list[contracts.Violation] = []
    for source, table, relation, cols, _order, _dest in plan:
        contract_table = (
            "silver.crash" if not source else
            next(ct for t, r, ct, o in TABLES[source] if t == table)
        )
        # Validate the projected, ordered relation -- the exact shape that will
        # be written, including column order.
        projected = f"{relation}__projected"
        col_sql = ", ".join(c.quote_ident(x) for x in cols)
        ctx.con.execute(
            f"CREATE OR REPLACE VIEW {projected} AS SELECT {col_sql} FROM {relation}"
        )
        # A history table is not unique on the contract's grain -- that is what
        # SCD2 means -- so it is validated on the total order it is written in
        # instead. Same assertion from two directions: write_parquet() refuses a
        # non-total order and the contract refuses a non-unique key.
        is_history = table.endswith("_history")
        violations += contracts.validate_relation(
            ctx.con, projected, contract, contract_table,
            unique_keys=[_order] if is_history else None,
            check_row_count_min=not small_corpus,
        )
        if table.endswith("_current") or not source:
            violations += contracts.validate_foreign_keys(
                ctx.con, contract, contract_table, projected, resolve
            )
    contracts.raise_for(violations, context="silver.schema.json")


def _cli(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(
        prog="python -m src.transform.build",
        description="Rebuild the silver layer from bronze, deterministically.",
    )
    ap.add_argument("--source", action="append", choices=ALL_SOURCES,
                    help="build only this source (repeatable; default: all)")
    ap.add_argument("--bronze-root", type=Path, default=None)
    ap.add_argument("--silver-root", type=Path, default=None)
    ap.add_argument("--allow-unmapped", action="store_true",
                    help="write unmapped dictionary values instead of failing")
    ap.add_argument("--no-validate", action="store_true",
                    help="skip contract validation (for debugging a new table)")
    ap.add_argument("--small-corpus", action="store_true",
                    help="skip the contract's row_count_min floor -- for builds "
                         "over the committed test extracts rather than a full "
                         "bronze corpus")
    ap.add_argument("--threads", type=int, default=None)
    ap.add_argument("--no-watermark-store", action="store_true",
                    help="ignore the bronze watermark store; discover partitions "
                         "from the on-disk tree only")
    ap.add_argument("--json", action="store_true", help="print the manifest as JSON")
    ap.add_argument("-v", "--verbose", action="store_true")
    args = ap.parse_args(argv)

    logging.basicConfig(
        level=logging.DEBUG if args.verbose else logging.INFO,
        format="%(levelname)s %(name)s %(message)s",
    )

    store = None
    if not args.no_watermark_store:
        db = (Path(args.bronze_root) if args.bronze_root else BRONZE_DIR) / "_watermarks.duckdb"
        if db.exists():
            store = WatermarkStore(db)
        else:
            log.warning("no watermark store at %s -- discovering partitions from "
                        "the on-disk tree only", db)

    result = build_silver(
        sources=args.source or ALL_SOURCES,
        bronze_root=args.bronze_root,
        silver_root=args.silver_root,
        store=store,
        allow_unmapped=args.allow_unmapped,
        validate=not args.no_validate,
        small_corpus=args.small_corpus,
        threads=args.threads,
    )

    if args.json:
        print(json.dumps(result, indent=2, default=str))
    else:
        print(f"\nsilver -> {result['silver_root']}")
        for name, info in sorted(result["outputs"].items()):
            print(f"  {name:<34} {info['rows']:>9} rows  "
                  f"{info['bytes'] / 1e6:>7.2f} MB  {info['sha256'][:16]}")
        for w in result["warnings"]:
            print(f"  WARNING: {w}")
    return 0


if __name__ == "__main__":
    sys.exit(_cli())
