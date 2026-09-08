"""One idempotent entry point for date-range recovery.

Bronze is deliberately never fetched or rewritten here. Montgomery and TxDOT
are load/OID-keyset streams, so a crash-date range is rebuilt from *all* bronze
loads; otherwise late amendments would disappear. The existing deterministic
builders remain the only implementation of business logic.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import shutil
import tempfile
from dataclasses import dataclass, field
from datetime import date
from pathlib import Path
from typing import Any, Iterable

from src import config
from src.analysis.build import build_analysis
from src.compliance.build import build_compliance
from src.geo.build import build_geo
from src.scoring.build import build_scoring
from src.transform.build import ALL_SOURCES, build_silver
from src.transform.model import build_gold

LAYERS = ("silver", "gold", "geo", "compliance", "scoring", "all")
_CLOSURE = {
    "silver": ("silver",),
    "gold": ("silver", "gold"),
    "geo": ("silver", "gold", "geo"),
    "compliance": ("silver", "gold", "geo", "compliance"),
    "scoring": ("silver", "gold", "geo", "compliance", "scoring"),
    "all": ("silver", "gold", "geo", "analysis", "compliance", "scoring"),
}


@dataclass(frozen=True)
class ArtifactHash:
    artifact: str
    sha256: str
    bytes: int
    kind: str = "bytes"


@dataclass
class BackfillResult:
    start: date
    end: date
    layer: str
    root: Path
    layers_run: tuple[str, ...]
    artifacts: dict[str, ArtifactHash] = field(default_factory=dict)
    manifests: dict[str, dict[str, Any]] = field(default_factory=dict)
    dry_run: bool = False
    fars_years: tuple[int, ...] = ()

    @property
    def passed(self) -> bool:
        return bool(self.artifacts) and not self.dry_run


def _parse_date(value: str | date) -> date:
    return value if isinstance(value, date) else date.fromisoformat(value)


def _sha(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def _normalised_manifest_sha(path: Path, root: Path) -> str:
    """Hash a manifest without its clock or run-root spelling.

    Existing layer manifests intentionally record absolute output paths. Two
    isolated proof roots therefore differ textually even when every data byte
    is identical; replacing only that caller-selected prefix keeps the proof
    strict over inputs, parameters, row counts, and hashes.
    """
    payload = json.loads(path.read_text(encoding="utf-8"))
    payload.pop("built_at", None)
    root_spellings = {str(root), str(root.absolute()), str(root.resolve())}

    def canonical(value: Any) -> Any:
        if isinstance(value, str):
            for root_text in sorted(root_spellings, key=len, reverse=True):
                value = value.replace(root_text, "<RUN_ROOT>")
            return value
        if isinstance(value, list):
            return [canonical(item) for item in value]
        if isinstance(value, dict):
            return {key: canonical(item) for key, item in value.items()}
        return value

    payload = canonical(payload)
    raw = json.dumps(payload, sort_keys=True, separators=(",", ":"), default=str)
    return hashlib.sha256(raw.encode()).hexdigest()


def hash_artifacts(root: Path) -> dict[str, ArtifactHash]:
    """Hash all durable data outputs below ``root`` in stable path order."""
    out: dict[str, ArtifactHash] = {}
    if not root.exists():
        return out
    for path in sorted(p for p in root.rglob("*") if p.is_file()):
        if path.name.endswith(".part") or path.suffix in {".duckdb", ".sqlite"}:
            continue
        rel = path.relative_to(root).as_posix()
        if path.name.endswith("_manifest.json"):
            out[rel] = ArtifactHash(rel, _normalised_manifest_sha(path, root), path.stat().st_size,
                                    "manifest_without_built_at")
        elif path.suffix in {".parquet", ".csv", ".json"}:
            out[rel] = ArtifactHash(rel, _sha(path), path.stat().st_size)
    return out


def _roots(out_root: Path | None) -> tuple[Path, Path, Path, Path]:
    if out_root is None:
        return (config.SILVER_DIR, config.GOLD_DIR, config.DATA_DIR / "vault",
                config.REPO_ROOT / "output")
    return out_root / "silver", out_root / "gold", out_root / "vault", out_root / "output"


def backfill(
    start: str | date,
    end: str | date,
    *,
    layer: str = "all",
    fars_years: Iterable[int] | None = None,
    dry_run: bool = False,
    out_root: Path | str | None = None,
    bronze_root: Path | str | None = None,
    reference_root: Path | str | None = None,
    small_corpus: bool = False,
) -> BackfillResult:
    """Rebuild a range through ``layer`` by calling the canonical builders.

    The range selects the operational recovery request. Current silver/gold
    tables and whole-corpus compliance/scoring artefacts are intentionally
    rematerialised because their contracts span the full current slice. Hive
    GeoParquet files are deterministic, so partitions outside the selected
    years remain byte-identical. Bronze is read-only throughout.
    """
    start_d, end_d = _parse_date(start), _parse_date(end)
    if end_d < start_d:
        raise ValueError("--end must be on or after --start")
    if layer not in _CLOSURE:
        raise ValueError(f"unknown layer {layer!r}; choose from {LAYERS}")
    configured_years = tuple(int(y) for y in config.sources()["fars"]["years"])
    requested_years = tuple(sorted(set(int(y) for y in (fars_years or configured_years))))
    unknown = sorted(set(requested_years) - set(configured_years))
    if unknown:
        raise ValueError(f"FARS years are not configured: {unknown}")

    base = Path(out_root) if out_root is not None else config.DATA_DIR
    result = BackfillResult(start_d, end_d, layer, base, _CLOSURE[layer],
                            dry_run=dry_run, fars_years=requested_years)
    if dry_run:
        return result

    silver, gold, vault, output = _roots(Path(out_root) if out_root is not None else None)
    bronze = Path(bronze_root) if bronze_root is not None else config.BRONZE_DIR
    refs = Path(reference_root) if reference_root is not None else None
    sources = tuple(source for source in ALL_SOURCES if (bronze / source).exists())
    if not sources:
        raise FileNotFoundError(f"no bronze sources under {bronze}")

    for name in result.layers_run:
        if name == "silver":
            result.manifests[name] = build_silver(
                sources=sources, bronze_root=bronze, silver_root=silver, store=None,
                small_corpus=small_corpus)
        elif name == "gold":
            result.manifests[name] = build_gold(
                silver_root=silver, gold_root=gold, small_corpus=small_corpus)
        elif name == "geo":
            result.manifests[name] = build_geo(
                gold_root=gold, reference_root=refs, offline=True,
                small_corpus=small_corpus)
        elif name == "analysis":
            result.manifests[name] = build_analysis(
                gold_root=gold, out_root=gold / "analysis", reference_root=refs,
                period=f"{start_d.isoformat()}:{end_d.isoformat()}",
                skip_figures=True, small_corpus=small_corpus)
        elif name == "compliance":
            result.manifests[name] = build_compliance(
                gold_root=gold, out_root=gold / "compliance", vault_dir=vault,
                reference_root=refs, sample_path=output / "sample_leads.csv",
                schema_check_path=output / "sample_leads.schema_check.json",
                skip_snap=True, small_corpus=small_corpus)
        elif name == "scoring":
            result.manifests[name] = build_scoring(
                gold_root=gold, out_root=gold / "scoring", small_corpus=small_corpus)
    result.artifacts = hash_artifacts(base)
    return result


def prove(start: str | date, end: str | date, **kwargs: Any) -> tuple[BackfillResult, BackfillResult, bool]:
    """Run twice in isolated roots and compare every durable output byte."""
    with tempfile.TemporaryDirectory(prefix="crash-backfill-proof-") as tmp:
        work = Path(tmp)
        one = backfill(start, end, out_root=work / "run1", **kwargs)
        two = backfill(start, end, out_root=work / "run2", **kwargs)
        keys = sorted(set(one.artifacts) | set(two.artifacts))
        passed = bool(keys) and all(
            key in one.artifacts and key in two.artifacts
            and one.artifacts[key].sha256 == two.artifacts[key].sha256 for key in keys)
        print(f"{'artefact':72} {'run1 sha256':16} {'run2 sha256':16} verdict")
        for key in keys:
            left, right = one.artifacts.get(key), two.artifacts.get(key)
            verdict = "PASS" if left and right and left.sha256 == right.sha256 else "FAIL"
            print(f"{key:72} {(left.sha256[:16] if left else '-'):16} "
                  f"{(right.sha256[:16] if right else '-'):16} {verdict}")
        print(f"VERDICT: {'PASS' if passed else 'FAIL'}")
        return one, two, passed


def recover_fars_year(
    year: int, *, bronze_root: Path, silver_root: Path, small_corpus: bool = False
) -> dict[str, ArtifactHash]:
    """Recover FARS silver in staging and promote only FARS output files."""
    if year not in {int(y) for y in config.sources()["fars"]["years"]}:
        raise ValueError(f"FARS year {year} is not configured")
    with tempfile.TemporaryDirectory(prefix=f"fars-{year}-recovery-") as tmp:
        stage = Path(tmp) / "silver"
        build_silver(sources=["fars"], bronze_root=bronze_root,
                     silver_root=stage, store=None, small_corpus=small_corpus)
        destination = Path(silver_root) / "fars"
        destination.mkdir(parents=True, exist_ok=True)
        for source in sorted((stage / "fars").glob("*.parquet")):
            target = destination / source.name
            part = target.with_name(target.name + ".part")
            shutil.copyfile(source, part)
            part.replace(target)
    return hash_artifacts(Path(silver_root) / "fars")


def recover_geo_partition(
    jurisdiction: str,
    year: int,
    *,
    gold_root: Path,
    reference_root: Path | None = None,
    small_corpus: bool = False,
) -> ArtifactHash:
    """Stage the canonical geo build and promote one hive state/year only."""
    gold_root = Path(gold_root)
    with tempfile.TemporaryDirectory(prefix=f"geo-{jurisdiction}-{year}-recovery-") as tmp:
        stage = Path(tmp) / "gold"
        stage.mkdir(parents=True)
        # Only canonical top-level gold inputs are copied. Existing geo and
        # downstream directories cannot influence the staged build.
        for source in sorted(gold_root.glob("*.parquet")):
            if source.name not in {"crash_geo.parquet", "dim_block_group.parquet"}:
                shutil.copy2(source, stage / source.name)
        build_geo(gold_root=stage, reference_root=reference_root, offline=True,
                  small_corpus=small_corpus)
        relative = Path(f"jurisdiction={jurisdiction}") / f"year={year}" / "part-0.parquet"
        source = stage / "crash_geo" / relative
        if not source.exists():
            raise FileNotFoundError(f"staged build did not produce {relative}")
        target = gold_root / "crash_geo" / relative
        target.parent.mkdir(parents=True, exist_ok=True)
        part = target.with_name(target.name + ".part")
        shutil.copyfile(source, part)
        part.replace(target)
    return ArtifactHash(relative.as_posix(), _sha(target), target.stat().st_size)


def _parser() -> argparse.ArgumentParser:
    ap = argparse.ArgumentParser(prog="python -m orchestration.backfill")
    ap.add_argument("--start", required=True)
    ap.add_argument("--end", required=True)
    ap.add_argument("--layer", choices=LAYERS, default="all")
    ap.add_argument("--fars-years", nargs="+", type=int)
    ap.add_argument("--dry-run", action="store_true")
    ap.add_argument("--prove", action="store_true")
    ap.add_argument("--out-root", type=Path)
    ap.add_argument("--bronze-root", type=Path)
    ap.add_argument("--reference-root", type=Path)
    ap.add_argument("--small-corpus", action="store_true", help=argparse.SUPPRESS)
    return ap


def main(argv: list[str] | None = None) -> int:
    args = _parser().parse_args(argv)
    options = dict(layer=args.layer, fars_years=args.fars_years,
                   bronze_root=args.bronze_root, reference_root=args.reference_root,
                   small_corpus=args.small_corpus)
    if args.prove:
        if args.dry_run:
            raise SystemExit("--prove and --dry-run are mutually exclusive")
        _, _, passed = prove(args.start, args.end, **options)
        return 0 if passed else 1
    result = backfill(args.start, args.end, dry_run=args.dry_run,
                      out_root=args.out_root, **options)
    if result.dry_run:
        print(f"DRY RUN {result.start}..{result.end}: {' -> '.join(result.layers_run)}; "
              f"FARS years={list(result.fars_years)}; bronze unchanged")
    else:
        print(f"backfill complete: {len(result.artifacts)} artefacts under {result.root}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
