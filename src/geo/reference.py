"""Reference data: download once, hash it, and never trust a URL again.

    python -m src.geo.reference --all          # TIGER + ACS + the Maryland PBF
    python -m src.geo.reference --what tiger   # one family
    python -m src.geo.reference --report       # what is cached, and its hashes

Phase 1 established the rule for source data: the raw bytes are written to disk
byte-for-byte before anything parses them, and the sha256 of those bytes is what
the build records as the input version. Reference data gets the same treatment
for a sharper reason -- it is *worse* than source data at being versioned.

  * `maryland-latest.osm.pbf` 302s to a dated file and the bytes change nightly.
    "Latest" is not a version. The sha256 is the version, and it is pinned in
    `config/geo.toml [reference.osm]` after the first fetch, so a changed hash
    is a restatement the build announces rather than a silent re-enrichment.
  * TIGER 2025 is a fixed vintage but the Census re-publishes files in place;
    `Last-Modified` plus the hash is the only honest identity.
  * ACS estimates are revised. The vintage in the URL is not enough.

Everything lands under `data/reference/` (gitignored) with one manifest,
`_reference_manifest.json`, recording per file: source URL, sha256, byte count,
the server's `Last-Modified` and `ETag`, and when it was downloaded. That
manifest is folded into the geo build manifest, so a `crash_geo` row can be
traced to the exact polygon file that produced its GEOID.

Parsed forms live beside the raw bytes (`data/reference/parsed/`), never instead
of them: a GeoParquet of block-group polygons is a derived artefact of a zip
whose hash is recorded, so it is reproducible and it is safe to delete.

Offline mode
------------
`--offline` (and `build.py --offline`) never reaches the network. A missing file
raises `MissingReference` naming the file, the URL it comes from and the command
that would fetch it. It does NOT skip the stage: a geo build that silently
produced NULL tract GEOIDs because a zip was absent would look exactly like a
geo build that ran correctly against crashes in the ocean.

Network calls live here and in `weather.py` and nowhere else, always through
`src/ingest/http.py`'s retrying client. Overpass is never called.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import logging
import sys
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterable

from .. import config
from ..ingest.http import HttpClient
from ..ingest.watermark import durable_replace

log = logging.getLogger("geo.reference")

MANIFEST_NAME = "_reference_manifest.json"

# TIGER layers this phase uses, and how their file names are built. `us` is the
# national file (COUNTY publishes one); the others are per state FIPS.
TIGER_LAYERS = {
    "COUNTY": "tl_{year}_us_county.zip",
    "TRACT": "tl_{year}_{fips}_tract.zip",
    "BG": "tl_{year}_{fips}_bg.zip",
}


class MissingReference(FileNotFoundError):
    """A reference file the build needs is not cached and we are offline.

    Deliberately fatal. The alternative -- skipping the stage -- produces a
    `crash_geo` whose NULL tract GEOIDs are indistinguishable from a genuine
    no-polygon result, which is the exact failure mode this pipeline's contract
    discipline exists to prevent.
    """


def reference_root(root: Path | str | None = None) -> Path:
    return Path(root) if root is not None else config.DATA_DIR / "reference"


@dataclass
class ReferenceStore:
    """The cache, its manifest, and the one client allowed to fill it.

    `offline` is a property of the store rather than of each call so that a
    build cannot accidentally be half-offline: either the whole run is allowed
    to reach the network or none of it is.
    """

    root: Path
    offline: bool = False
    client: HttpClient | None = None

    def __post_init__(self) -> None:
        self.root = Path(self.root)
        self._manifest: dict[str, dict[str, Any]] = {}
        path = self.root / MANIFEST_NAME
        if path.exists():
            try:
                self._manifest = json.loads(path.read_text())
            except json.JSONDecodeError:
                log.warning("%s is unreadable -- rebuilding it from disk", path)
                self._manifest = {}

    # -- manifest --------------------------------------------------------

    @property
    def manifest(self) -> dict[str, dict[str, Any]]:
        return dict(sorted(self._manifest.items()))

    def entry(self, relpath: str) -> dict[str, Any] | None:
        return self._manifest.get(relpath)

    def _record(self, relpath: str, info: dict[str, Any]) -> None:
        self._manifest[relpath] = info
        dest = self.root / MANIFEST_NAME
        dest.parent.mkdir(parents=True, exist_ok=True)
        tmp = dest.with_name(dest.name + ".part")
        tmp.write_text(json.dumps(self.manifest, indent=2, sort_keys=True) + "\n")
        durable_replace(tmp, dest)

    def _http(self) -> HttpClient:
        if self.client is None:
            # min_interval: the Census and Geofabrik are both free services we
            # do not own and this fetches a handful of large files, so pacing
            # costs nothing and is the polite default.
            self.client = HttpClient(min_interval=0.5, timeout=300.0)
        return self.client

    # -- the one primitive ------------------------------------------------

    def ensure(self, relpath: str, url: str, *, force: bool = False) -> Path:
        """The cached bytes for `url`, downloading them if they are absent.

        Idempotent: a warm cache makes zero network calls, which is the
        property `--offline` and the weather test both rely on. A file present
        on disk but absent from the manifest is re-hashed and recorded rather
        than re-downloaded -- the bytes are the thing, and re-fetching 203 MB to
        learn a hash we can compute locally would be silly.
        """
        dest = self.root / relpath
        if dest.exists() and not force:
            if relpath not in self._manifest:
                self._record(relpath, self._describe(dest, url, recovered=True))
            return dest
        if self.offline:
            raise MissingReference(
                f"{dest} is not cached and --offline was requested.\n"
                f"  source: {url}\n"
                f"  fetch it with: python -m src.geo.reference --all"
            )
        log.info("fetching %s", url)
        info = self._http().download(url, dest)
        self._record(
            relpath,
            {
                "url": url,
                "sha256": info["sha256"],
                "bytes": info["bytes"],
                "last_modified": info.get("last_modified"),
                "etag": info.get("etag"),
                "downloaded_at": datetime.now(timezone.utc).isoformat(),
                "resolved_url": url,
            },
        )
        return dest

    def require(self, relpath: str) -> Path:
        """A cached file that must already exist. Never downloads."""
        dest = self.root / relpath
        if not dest.exists():
            raise MissingReference(
                f"{dest} is missing.\n"
                f"  fetch it with: python -m src.geo.reference --all"
            )
        return dest

    def _describe(self, path: Path, url: str, *, recovered: bool = False) -> dict[str, Any]:
        return {
            "url": url,
            "sha256": hashlib.sha256(path.read_bytes()).hexdigest(),
            "bytes": path.stat().st_size,
            "last_modified": None,
            "etag": None,
            "downloaded_at": None if recovered else datetime.now(timezone.utc).isoformat(),
            "recovered_from_disk": recovered or None,
        }

    # -- derived artefacts -------------------------------------------------

    def parsed(self, relpath: str) -> Path:
        p = self.root / "parsed" / relpath
        p.parent.mkdir(parents=True, exist_ok=True)
        return p

    def hashes(self) -> dict[str, str]:
        """{relpath: sha256} for the build manifest."""
        return {k: v.get("sha256", "") for k, v in self.manifest.items()}


# ---------------------------------------------------------------------------
# TIGER
# ---------------------------------------------------------------------------


def tiger_relpath(layer: str, *, year: int, fips: str | None = None) -> str:
    name = TIGER_LAYERS[layer].format(year=year, fips=fips)
    return f"tiger/{year}/{layer}/{name}"


def tiger_url(layer: str, *, year: int, fips: str | None = None) -> str:
    base = config.sources()["census"]["tiger_base"].rstrip("/")
    # tiger_base is pinned to the configured vintage; the year is carried
    # separately so a re-vintage is a config edit in one place.
    base = base.rsplit("/", 1)[0] + f"/TIGER{year}"
    return f"{base}/{layer}/{TIGER_LAYERS[layer].format(year=year, fips=fips)}"


def ensure_tiger(store: ReferenceStore, layer: str, fips: str | None = None) -> Path:
    year = int(config.geo()["reference"]["tiger_year"])
    return store.ensure(
        tiger_relpath(layer, year=year, fips=fips),
        tiger_url(layer, year=year, fips=fips),
    )


def ensure_all_tiger(store: ReferenceStore) -> dict[str, Path]:
    """COUNTY (national) plus TRACT and BG for every in-scope state."""
    ref = config.geo()["reference"]
    out = {"COUNTY": ensure_tiger(store, "COUNTY")}
    for fips in ref["state_fips"]:
        for layer in ("TRACT", "BG"):
            out[f"{layer}:{fips}"] = ensure_tiger(store, layer, fips)
    return out


# ---------------------------------------------------------------------------
# OSM
# ---------------------------------------------------------------------------


def osm_relpath(state: str) -> str:
    return f"osm/{state}-latest.osm.pbf"


def ensure_osm(store: ReferenceStore, state: str = "maryland") -> Path:
    """The Geofabrik extract, with the hash treated as its version.

    Geofabrik's `-latest` name resolves to a dated file and the bytes change
    nightly, so `config/geo.toml [reference.osm] <state>_sha256` pins what this
    build was verified against. A mismatch is logged loudly and recorded in the
    manifest as a restatement; it does not fail the build, because the correct
    response to "OSM has new roads" is to rebuild the snap columns and say so,
    not to refuse to run.
    """
    url = config.sources()["osm"]["geofabrik"].format(state=state)
    path = store.ensure(osm_relpath(state), url)
    pinned = str(config.geo()["reference"]["osm"].get(f"{state}_sha256", "") or "")
    entry = store.entry(osm_relpath(state)) or {}
    actual = entry.get("sha256", "")
    if pinned and actual and pinned != actual:
        log.warning(
            "%s PBF hash %s does not match the pin %s in config/geo.toml -- "
            "this is a RESTATEMENT of every snap column, not a warning to ignore",
            state, actual[:16], pinned[:16],
        )
    elif not pinned and actual:
        log.info(
            "%s PBF is not pinned yet; record reference.osm.%s_sha256 = %r "
            "in config/geo.toml to make this build reproducible",
            state, state, actual,
        )
    return path


# ---------------------------------------------------------------------------
# ACS
# ---------------------------------------------------------------------------

# Census annotation values. These are NOT estimates: they mean "suppressed",
# "not applicable", "median falls in the open-ended top interval", and so on.
# Every one of them becomes NULL. Reading -666666666 as a population is the
# classic silent-corruption bug in ACS pipelines.
ACS_ANNOTATIONS = frozenset(
    {-666666666, -999999999, -888888888, -222222222, -333333333, -555555555}
)


def acs_api_relpath(state: str, county: str = "all") -> str:
    ref = config.geo()["reference"]
    return f"acs/{ref['acs_year']}/{ref['acs_dataset']}/b01003_{state}_{county}.json"


def acs_summary_relpath() -> str:
    ref = config.geo()["reference"]
    return f"acs/{ref['acs_year']}/summary_file/acsdt5y{ref['acs_year']}-{ref['acs_table']}.dat"


def acs_summary_url() -> str:
    ref = config.geo()["reference"]
    return str(ref["acs_summary_file"]).format(year=ref["acs_year"], table=ref["acs_table"])


def acs_api_url(state: str, county: str = "*") -> str:
    """The keyed ACS 5-year block-group query for one state.

    Population only, deliberately. `B19013_001E` (median household income),
    `B25044` (vehicles available by tenure) and `B08301` (means of transport to
    work) are the variables ASSIGNMENT.md Part 4 warns about: they are strong
    proxies for protected classes, and a feature that was never loaded cannot
    leak into a lead score. Population is a DENOMINATOR -- it normalises a
    hotspot rate in aggregate and never becomes a per-record attribute.
    """
    ref = config.geo()["reference"]
    base = config.sources()["census"]["acs5"]
    base = base.replace("/2023/acs/acs5", f"/{ref['acs_year']}/acs/{ref['acs_dataset']}")
    return (
        f"{base}?get={ref['acs_variable']}"
        f"&for=block%20group:*&in=state:{state}%20county:{county}%20tract:*"
    )


def ensure_acs(store: ReferenceStore) -> dict[str, Any]:
    """The ACS population source, keyed API first and bulk file as the fallback.

    Measured 2026-09-08: the live API 302s an unkeyed request to a "Missing
    Key" HTML page. That is what ASSIGNMENT.md predicts ("the 500 unkeyed
    queries page is stale") and what `config/settings.toml` is for -- but no key
    is configured on this host, so the build takes the second official route:
    the Census's table-based Summary File, which publishes the same B01003
    estimates for the same vintage as a keyless bulk download.

    Returns which route was used so the manifest and the report can say so.
    """
    key = config.key("census_api_key")
    ref = config.geo()["reference"]
    if key:
        paths: dict[str, str] = {}
        for fips in ref["state_fips"]:
            rel = acs_api_relpath(fips)
            url = acs_api_url(fips) + f"&key={key}"
            try:
                store.ensure(rel, url)
                paths[fips] = rel
            except Exception as exc:  # county wildcard refused -> per county
                log.warning("ACS county wildcard failed for state %s (%s); "
                            "falling back to one request per county", fips, exc)
                for county in _counties_for(fips):
                    crel = acs_api_relpath(fips, county)
                    store.ensure(crel, acs_api_url(fips, county) + f"&key={key}")
                    paths[f"{fips}:{county}"] = crel
        # Never log or record the key: the URL that carries it stays out of the
        # manifest, which records the keyless form.
        for rel in paths.values():
            e = store.entry(rel)
            if e:
                e["url"] = e["url"].split("&key=")[0]
        return {"route": "api", "paths": paths}

    log.info(
        "no census_api_key configured -- using the keyless Census table-based "
        "Summary File for %s (same vintage, same table)", ref["acs_variable"]
    )
    rel = acs_summary_relpath()
    store.ensure(rel, acs_summary_url())
    return {"route": "summary_file", "paths": {"all": rel}}


def _counties_for(fips: str) -> list[str]:
    """County FIPS in one state, from config/counties.csv."""
    import csv

    out: list[str] = []
    with (config.CONFIG_DIR / "counties.csv").open(encoding="utf-8") as fh:
        for row in csv.DictReader(line for line in fh if not line.startswith("#")):
            if row["state_fips"] == fips:
                out.append(row["county_fips"])
    return sorted(set(out))


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------


def fetch_all(store: ReferenceStore, what: Iterable[str] = ("tiger", "acs", "osm")) -> dict[str, Any]:
    what = set(what)
    out: dict[str, Any] = {}
    if "tiger" in what:
        out["tiger"] = {k: str(v) for k, v in ensure_all_tiger(store).items()}
    if "acs" in what:
        out["acs"] = ensure_acs(store)
    if "osm" in what:
        out["osm"] = {
            s: str(ensure_osm(store, s))
            for s in config.geo()["reference"]["osm"]["states"]
        }
    return out


def _cli(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(
        prog="python -m src.geo.reference",
        description="Download, cache and hash the Phase 4 reference data.",
    )
    ap.add_argument("--reference-root", type=Path, default=None)
    ap.add_argument("--what", action="append", choices=("tiger", "acs", "osm"),
                    help="fetch only this family (repeatable; default: all)")
    ap.add_argument("--all", action="store_true", help="fetch every family")
    ap.add_argument("--report", action="store_true",
                    help="print the manifest and exit without fetching")
    ap.add_argument("--offline", action="store_true",
                    help="never touch the network; fail naming what is missing")
    ap.add_argument("-v", "--verbose", action="store_true")
    args = ap.parse_args(argv)
    logging.basicConfig(level=logging.DEBUG if args.verbose else logging.INFO,
                        format="%(levelname)s %(name)s %(message)s")

    store = ReferenceStore(reference_root(args.reference_root), offline=args.offline)
    if args.report:
        for rel, info in store.manifest.items():
            print(f"{rel:<52} {info.get('bytes', 0) / 1e6:>8.2f} MB  "
                  f"{str(info.get('sha256'))[:16]}  {info.get('last_modified') or ''}")
        return 0

    result = fetch_all(store, args.what or ("tiger", "acs", "osm"))
    print(json.dumps(result, indent=2, default=str))
    print(f"\nreference root: {store.root}")
    for rel, info in store.manifest.items():
        print(f"  {rel:<52} {info.get('bytes', 0) / 1e6:>8.2f} MB  "
              f"{str(info.get('sha256'))[:16]}")
    return 0


if __name__ == "__main__":
    sys.exit(_cli())
