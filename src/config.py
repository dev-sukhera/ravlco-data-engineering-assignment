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


@functools.cache
def sources() -> dict[str, Any]:
    """config/sources.toml -- endpoints, dataset ids, FARS years."""
    with (CONFIG_DIR / "sources.toml").open("rb") as fh:
        return tomllib.load(fh)


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
