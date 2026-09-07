"""Schema-drift detection CLI -- and the proof it would have fired.

    python -m src.transform.drift
    python -m src.transform.drift --dataset mmzv-x632 --column driver_substance_abuse
    python -m src.transform.drift --dataset mmzv-x632 --column driver_substance_abuse \
                                  --vocabulary-asof 2023-12-27
    python -m src.transform.drift --dataset mmzv-x632 --column injury_severity \
                                  --vocabulary-asof 2023-12-27

The assignment asks for the detector that would have caught the
`driver_substance_abuse` dictionary change, "firing against the historical
data". `--vocabulary-asof DATE` is that demonstration: it reconstructs the
vocabulary as it stood on DATE **from the data itself** -- every value whose
first observed crash_date_time is on or before DATE -- and then replays the full
history against it. Nothing is hardcoded, so the demonstration cannot be
accused of having been rigged by choosing which tokens to leave out.

Run with `--vocabulary-asof 2023-12-27` (the day before the first new-generation
row) it names every new-generation token and dates the first one to 2023-12-28.
Run against the current vocabulary it is silent. Both outputs are in the build
report.

The detector is generic over (dataset, column) on purpose. The same 2024
dictionary generation change re-cased `injury_severity` in the same week, and a
detector special-cased to the substance column would have caught one and slept
through the other -- so `--column injury_severity --vocabulary-asof 2023-12-27`
fires too, on the same code path.
"""

from __future__ import annotations

import argparse
import logging
import sys
from pathlib import Path

from ..config import BRONZE_DIR
from ..ingest.watermark import WatermarkStore
from . import common as c
from .dictionaries import (
    ObservedValue,
    VOCABULARIES,
    detect_drift,
    vocabulary,
    vocabulary_asof,
)

log = logging.getLogger("transform.drift")

# (dataset, column) -> the bronze directory it lives in.
DATASET_SOURCE = {
    "bhju-22kf": "montgomery",
    "mmzv-x632": "montgomery",
    "n7fk-dce5": "montgomery",
}


def observe(dataset: str, column: str, *, bronze_root: Path | None = None,
            store: WatermarkStore | None = None) -> list[ObservedValue]:
    """Read the distinct values of one bronze column with their evidence window.

    One GROUP BY over every partition. Reads BRONZE, not silver: the detector's
    job is to fire before the transform has had a chance to normalise anything
    away.
    """
    source = DATASET_SOURCE.get(dataset)
    if source is None:
        raise KeyError(
            f"unknown dataset {dataset!r} (known: {sorted(DATASET_SOURCE)})"
        )
    con = c.connect()
    partitions = c.discover_partitions(
        source, dataset, bronze_root=bronze_root, store=store
    )
    if not partitions:
        raise FileNotFoundError(f"no bronze partitions for {source}/{dataset}")
    c.bronze_view(con, "drift_src", partitions)
    rows = con.execute(
        f"""SELECT {c.quote_ident(column)}, COUNT(*),
                   MIN(crash_date_time), MAX(crash_date_time),
                   MIN(":created_at"), MAX(":created_at")
            FROM drift_src GROUP BY 1"""
    ).fetchall()
    con.close()
    return [ObservedValue(*r) for r in rows]


def _cli(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(
        prog="python -m src.transform.drift",
        description="Detect value-set drift in a bronze code-dictionary column.",
    )
    ap.add_argument("--dataset", help="Socrata dataset id (default: all registered)")
    ap.add_argument("--column", help="column to check (default: all for the dataset)")
    ap.add_argument("--vocabulary-asof", metavar="YYYY-MM-DD",
                    help="rebuild the accepted vocabulary from values first seen "
                         "on or before this crash date, then replay all history "
                         "against it")
    ap.add_argument("--bronze-root", type=Path, default=None)
    ap.add_argument("-v", "--verbose", action="store_true")
    args = ap.parse_args(argv)
    logging.basicConfig(level=logging.DEBUG if args.verbose else logging.WARNING)

    pairs = sorted(VOCABULARIES)
    if args.dataset:
        pairs = [p for p in pairs if p[0] == args.dataset]
    if args.column:
        pairs = [p for p in pairs if p[1] == args.column]
    if not pairs:
        print(f"no registered vocabulary matches "
              f"dataset={args.dataset!r} column={args.column!r}", file=sys.stderr)
        return 2

    root = args.bronze_root or BRONZE_DIR
    db = root / "_watermarks.duckdb"
    store = WatermarkStore(db) if db.exists() else None

    drifted = 0
    for dataset, column in pairs:
        values = observe(dataset, column, bronze_root=args.bronze_root, store=store)
        if args.vocabulary_asof:
            accepted = vocabulary_asof(dataset, column, values, args.vocabulary_asof)
            # Intersect with the reviewed vocabulary so that a value which was
            # ALREADY unmapped before the as-of date does not get retroactively
            # blessed by having been seen early. Replaying history must not be
            # able to make the detector quieter than it is today.
            reviewed = set(vocabulary(dataset, column))
            accepted = tuple(v for v in accepted if v in reviewed)
            print(f"\n--- {dataset}.{column} replayed against the vocabulary as it "
                  f"stood on {args.vocabulary_asof} ({len(accepted)} tokens) ---")
        else:
            accepted = None
            print(f"\n--- {dataset}.{column} against the current vocabulary ---")

        report = detect_drift(dataset, column, values, accepted=accepted)
        print(report.render())
        drifted += bool(report.drifted)

    # Exit 1 on drift so a scheduler or a CI job can act on it without parsing
    # the text. This is the alert the assignment says should have existed.
    return 1 if drifted else 0


if __name__ == "__main__":
    sys.exit(_cli())
