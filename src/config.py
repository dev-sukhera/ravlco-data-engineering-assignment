"""Config loading for every layer.

Two files, deliberately separated:

  config/sources.toml   endpoints and dataset ids -- committed, shared by all
                        candidates, verified live 2026-09-01.
  config/settings.toml  API keys and run defaults -- gitignored, per-developer.

Added to the scaffold (it ships no config module) because ingest, transform, geo
and compliance all need the same two files and none of them should be re-parsing
TOML by hand. See DECISIONS.md.
"""

from __future__ import annotations

import functools
import os
import tomllib
from pathlib import Path
from typing import Any

REPO_ROOT = Path(__file__).resolve().parent.parent
CONFIG_DIR = REPO_ROOT / "config"
DATA_DIR = Path(os.environ.get("CRASH_DATA_DIR", REPO_ROOT / "data"))
BRONZE_DIR = DATA_DIR / "bronze"
SILVER_DIR = DATA_DIR / "silver"
GOLD_DIR = DATA_DIR / "gold"


@functools.cache
def sources() -> dict[str, Any]:
    """config/sources.toml -- endpoints, dataset ids, FARS years."""
    with (CONFIG_DIR / "sources.toml").open("rb") as fh:
        return tomllib.load(fh)


@functools.cache
def geo() -> dict[str, Any]:
    """config/geo.toml -- coordinate envelopes per source.

    Separate from sources.toml because an envelope is a claim about the world
    that changes on a different clock from an endpoint URL, and because the
    file has to be editable without touching anything a candidate is told not
    to modify. See the header of config/geo.toml for the superset argument.
    """
    with (CONFIG_DIR / "geo.toml").open("rb") as fh:
        return tomllib.load(fh)


def envelope(source: str) -> dict[str, Any]:
    """The [envelope.<source>] block, or a raise naming what is configured.

    A missing envelope is a build error, never a silent pass-everything: the
    whole point of the check is that a source without a declared envelope has
    not been thought about yet.
    """
    envelopes = geo().get("envelope", {})
    if source not in envelopes:
        raise KeyError(
            f"no [envelope.{source}] in config/geo.toml "
            f"(configured: {sorted(envelopes)})"
        )
    return envelopes[source]


@functools.cache
def model() -> dict[str, Any]:
    """config/model.toml -- gold scope and entity-resolution thresholds.

    Its own file because these are the numbers the memo has to defend and the
    live defence will ask to change: a jurisdiction is added or a match
    threshold tightened here, never in `src/transform/model.py`.
    """
    with (CONFIG_DIR / "model.toml").open("rb") as fh:
        return tomllib.load(fh)


@functools.cache
def compliance() -> dict[str, Any]:
    """config/compliance.toml [compliance] -- the eligibility engine's run.

    Its own file because Phase 6 is the section that decides the outcome and
    every number in it changes a legal disposition: the frozen `as_of`, the
    ruleset version this build refuses to run without, the window arithmetic,
    and the paths to the two files that between them say what the law is
    (`src/compliance/rules.yaml` and `config/blackout_windows.csv`).

    Deliberately NOT merged into settings.toml, which is gitignored: a
    reviewer must be able to read and diff the parameters that produced a
    decision, and a per-developer file is exactly the wrong place for them.
    """
    with (CONFIG_DIR / "compliance.toml").open("rb") as fh:
        return tomllib.load(fh)["compliance"]


def compliance_path(name: str) -> Path:
    """A repo-relative path from [compliance], resolved against REPO_ROOT.

    The config stores `src/compliance/rules.yaml`, not an absolute path, so
    the file is quotable in the report and identical on every machine. An
    absolute value is honoured unchanged so a test can point at a tmp_path
    copy without rewriting the config.
    """
    value = Path(str(compliance()[name]))
    return value if value.is_absolute() else REPO_ROOT / value


@functools.cache
def settings() -> dict[str, Any]:
    """config/settings.toml -- gitignored keys. Falls back to the example file
    so a fresh clone can at least import; callers that need a key must check."""
    path = CONFIG_DIR / "settings.toml"
    if not path.exists():
        path = CONFIG_DIR / "settings.example.toml"
    with path.open("rb") as fh:
        return tomllib.load(fh)


def key(name: str, default: str = "") -> str:
    """A key from [keys] in settings.toml, overridable by environment.

    Environment wins so that CI and the orchestrator can inject credentials
    without writing a settings.toml. Empty string means "not configured" --
    never None, so callers can test truthiness without a None check.
    """
    env = os.environ.get(f"CRASH_{name.upper()}")
    if env:
        return env
    return str(settings().get("keys", {}).get(name, default) or default)


def run_setting(name: str, default: Any = None) -> Any:
    """A value from [run] in settings.toml."""
    return settings().get("run", {}).get(name, default)
