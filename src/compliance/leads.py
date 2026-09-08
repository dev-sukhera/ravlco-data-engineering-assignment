"""The fixture join: party -> enrichment -> crash match -> decision -> lead row.

fixtures/README.md is explicit that this join is mandatory and equally
explicit about what it is not:

    "This fixture supplies the identity layer the real sources deliberately do
     not [...] **Your memo must still state that no such join is available in
     production, and what that means for the product.** The fixture is a test
     harness, not a demonstration that the join exists."

So: **the party is the record.** The fixture has no `report_number`, no
`crash_sk` and no key into gold -- it has coordinates and dates. Each fixture
row becomes exactly one lead whether or not a crash can be found for it, and
where none can be found the lead says so (`source_system = OTHER`,
`match_method = NO_MATCH`) instead of borrowing a crash that is not its own.


The party coordinate gets the same enrichment a crash gets
-----------------------------------------------------------
Envelope -> county/tract/block-group point-in-polygon -> H3 r8 -> IANA zone
from the coordinate -> road snap for the in-scope jurisdiction. All of it is
Phase 4's code called on a different frame; none of it is reimplemented here,
because two implementations of "which tract is this point in" is one more than
a pipeline can keep honest.

Each enrichment degrades to a RECORDED STATUS rather than to a null when its
reference data is absent:

    pip_status  = UNAVAILABLE   TIGER polygons not downloaded
    snap_status = UNAVAILABLE   the OSM extract is not on this machine
    snap_status = NOT_IN_SCOPE  the jurisdiction gets no snap by config

and NONE of those four is a reason code. An enrichment that was never in
scope, or whose 200 MB input is not on a CI box, is not a defect in the
record. Only `REJECTED_DISTANCE` -- an attempted snap that failed -- is.


The crash match, recorded honestly
----------------------------------
    same jurisdiction
    AND |crash_date - incident_date| <= [compliance] match_date_tolerance_days
    AND distance <= [compliance] match_max_distance_m

Distance is computed in the Phase 4 projected CRS for the row's jurisdiction
(`config/geo.toml [crs.snap]`: MD 26985, TX 32139, FL 26958), never in Web
Mercator, whose scale error is 1/cos(latitude) -- 1.29 at 39N. The match count
is reported per jurisdiction in the manifest, and it is expected to be ZERO
almost everywhere: the fixture is synthetic, TxDOT is a bounded 100k slice and
FARS is fatalities only, so only a Montgomery row could plausibly coincide
with a real crash. No crash is ever fabricated to fill the gap.
"""

from __future__ import annotations

import hashlib
import logging
import math
import subprocess
from dataclasses import dataclass
from datetime import date, datetime, time, timedelta, timezone
from pathlib import Path
from typing import Any, Iterable, Mapping, Sequence

import numpy as np
import pandas as pd

from .. import config
from ..transform import common as c
from . import consent as consent_mod
from . import vault as vault_mod
from .engine import EligibilityDecision, EligibilityEngine
from .reason_codes import IdentityProvenance

log = logging.getLogger("compliance.leads")

SNAP_UNAVAILABLE = "UNAVAILABLE"
SNAP_NOT_IN_SCOPE = "NOT_IN_SCOPE"
PIP_UNAVAILABLE = "UNAVAILABLE"

MATCH_NONE = "NO_MATCH"
MATCH_SPATIOTEMPORAL = "JURISDICTION_DATE_DISTANCE"

# `source_system` when no gold crash backs the lead. The contract's enum has
# exactly four members and this is the one that means "not one of the three".
SOURCE_OTHER = "OTHER"

ACTOR = "compliance.leads"


# ---------------------------------------------------------------------------
# enrichment
# ---------------------------------------------------------------------------


@dataclass
class Enrichment:
    """What the geo stage produced, and what it could not."""

    frame: pd.DataFrame
    stats: dict[str, Any]


def enrich_parties(
    parties: pd.DataFrame,
    *,
    reference_root: Path | None = None,
    skip_snap: bool = False,
    offline: bool = True,
) -> Enrichment:
    """Envelope, PIP, H3, timezone and snap for every party coordinate."""
    from ..geo import census_join, h3_index, reference as geo_reference
    from ..geo import snap as snap_mod
    from ..geo import tz as tz_mod

    cfg = config.compliance()
    envelope_by = dict(cfg["envelope_by_jurisdiction"])
    snap_jurisdictions = set(cfg["snap_jurisdictions"])

    out = pd.DataFrame(index=parties.index)
    out["party_token"] = parties["party_token"].to_numpy()
    lat = pd.to_numeric(parties["party_latitude"], errors="coerce")
    lon = pd.to_numeric(parties["party_longitude"], errors="coerce")
    jurisdictions = parties["jurisdiction"].astype(str)

    # -- envelope: a degree comparison on EPSG:4326, no projection ------
    status: list[str] = []
    distance: list[float | None] = []
    for j, la, lo in zip(jurisdictions, lat, lon):
        name = envelope_by.get(j)
        if name is None or pd.isna(la) or pd.isna(lo):
            status.append(c.GEO_MISSING)
            distance.append(None)
            continue
        env = config.envelope(name)
        inside = (env["min_lat"] <= la <= env["max_lat"]
                  and env["min_lon"] <= lo <= env["max_lon"])
        status.append(c.GEO_OK if inside else c.GEO_OUT_OF_ENVELOPE)
        # Geodesic on the WGS84 ellipsoid, exactly as Phase 2 reports it, so
        # "100 km outside Montgomery" is a number with a defined meaning.
        distance.append(0.0 if inside
                        else round(c.distance_from_envelope_m(name, la, lo), 1))
    out["envelope_status"] = status
    out["distance_from_envelope_m"] = distance

    # -- H3: pure function of the coordinate, no reference data ---------
    cells = [h3_index.index_point(la, lo) for la, lo in zip(lat, lon)]
    for res_col in ("h3_r9", "h3_r8", "h3_r7"):
        out[res_col] = [cell.get(res_col) for cell in cells]

    # -- timezone from the COORDINATE, never the state ------------------
    finder = tz_mod.ZoneFinder()
    zones = [finder.zone_at(la, lo) for la, lo in zip(lat, lon)]
    resolved = [
        tz_mod.resolve_zone(coordinate_zone=z, county_geoid=None,
                            jurisdiction=j, county_zones={})
        for z, j in zip(zones, jurisdictions)
    ]
    out["tz_iana"] = [r[0] for r in resolved]
    out["tz_source"] = [r[1] for r in resolved]
    out["tz_low_confidence"] = [r[2] for r in resolved]

    stats: dict[str, Any] = {
        "rows": int(len(parties)),
        "envelope_status": _counts(out["envelope_status"]),
        "tz_source": _counts(out["tz_source"]),
        "tz_iana": _counts(out["tz_iana"]),
    }

    # -- census PIP: needs TIGER, degrades to a recorded status ---------
    out["pip_county_geoid"] = None
    out["tract_geoid"] = None
    out["bg_geoid"] = None
    out["pip_status"] = PIP_UNAVAILABLE
    store = None
    try:
        store = geo_reference.ReferenceStore(
            geo_reference.reference_root(reference_root), offline=offline
        )
        import geopandas as gpd
        import shapely

        points = gpd.GeoDataFrame(
            {"party_token": out["party_token"]},
            geometry=[
                None if (pd.isna(la) or pd.isna(lo)) else shapely.Point(lo, la)
                for la, lo in zip(lat, lon)
            ],
            crs=f"EPSG:{config.geo()['crs']['storage']}",
        )
        block_groups = census_join.load_block_groups(store)
        bg, bg_stats = census_join.point_in_polygon(
            points, block_groups, key="party_token", out_col="bg_geoid"
        )
        bg = bg.set_index("party_token").reindex(out["party_token"].to_numpy())
        out["bg_geoid"] = bg["bg_geoid"].to_numpy()
        out["pip_status"] = bg["pip_status"].to_numpy()
        # tract = bg[:11] by the Census's own GEOID construction; Phase 4
        # verified that against a direct TIGER TRACT join on 5,000 rows.
        out["tract_geoid"] = [None if g is None or pd.isna(g) else str(g)[:11]
                              for g in out["bg_geoid"]]
        out["pip_county_geoid"] = [None if g is None or pd.isna(g) else str(g)[:5]
                                   for g in out["bg_geoid"]]
        stats["pip"] = bg_stats
    except Exception as exc:                      # reference data absent
        log.warning("party census join unavailable (%s); pip_status=UNAVAILABLE", exc)
        stats["pip"] = {"available": False, "reason": str(exc)}

    # -- road snap: in-scope jurisdictions only -------------------------
    out["snap_status"] = SNAP_NOT_IN_SCOPE
    out["snap_distance_m"] = None
    out["osm_highway"] = None
    out["snap_crs_epsg"] = None
    in_scope = jurisdictions.isin(snap_jurisdictions).to_numpy()
    if skip_snap:
        out.loc[in_scope, "snap_status"] = SNAP_UNAVAILABLE
        stats["snap"] = {"attempted": False, "reason": "--skip-snap"}
    elif in_scope.any() and store is not None:
        try:
            import geopandas as gpd
            import shapely

            roads = snap_mod.build_road_network(store)
            snap_stats: dict[str, Any] = {"attempted": True, "by_jurisdiction": {}}
            for j in sorted(set(jurisdictions[in_scope])):
                sel = (jurisdictions == j).to_numpy() & in_scope
                part = gpd.GeoDataFrame(
                    {"party_token": out.loc[sel, "party_token"]},
                    geometry=[
                        None if (pd.isna(la) or pd.isna(lo)) else shapely.Point(lo, la)
                        for la, lo in zip(lat[sel], lon[sel])
                    ],
                    crs=f"EPSG:{config.geo()['crs']['storage']}",
                )
                # One projected CRS per jurisdiction, from config/geo.toml
                # [crs.snap]. Maryland is a single state-plane zone (26985,
                # metres) so it is unambiguous county-wide. Web Mercator
                # would be wrong by 1/cos(39.1) = 1.288 and would silently
                # turn a 50 m threshold into 38.8 m.
                epsg = snap_mod.snap_crs_for(j)
                # The threshold is the PARTY one from config/compliance.toml,
                # not config/geo.toml's crash threshold. See that file for why
                # a residence and a crash are different populations even when
                # the number is currently the same.
                result, s = snap_mod.snap_points(
                    part, roads, key="party_token", epsg=epsg,
                    max_distance_m=float(cfg["party_snap_max_distance_m"]),
                    search_distance_m=float(cfg["party_snap_search_distance_m"]),
                )
                result = result.set_index("party_token").reindex(
                    out.loc[sel, "party_token"].to_numpy()
                )
                out.loc[sel, "snap_status"] = result["snap_status"].to_numpy()
                out.loc[sel, "snap_distance_m"] = result["snap_distance_m"].to_numpy()
                out.loc[sel, "osm_highway"] = result["osm_highway"].to_numpy()
                out.loc[sel, "snap_crs_epsg"] = epsg
                snap_stats["by_jurisdiction"][j] = s
            distances = pd.to_numeric(out["snap_distance_m"], errors="coerce").dropna()
            snap_stats["max_distance_m"] = float(cfg["party_snap_max_distance_m"])
            snap_stats["distance_percentiles_m"] = (
                {str(q): round(float(distances.quantile(q / 100)), 2)
                 for q in (50, 75, 90, 95, 99)} if len(distances) else {}
            )
            snap_stats["rejected"] = int(
                (out["snap_status"] == "REJECTED_DISTANCE").sum())
            stats["snap"] = snap_stats
        except Exception as exc:                  # the PBF is not on this box
            log.warning("party road snap unavailable (%s); snap_status=UNAVAILABLE", exc)
            out.loc[in_scope, "snap_status"] = SNAP_UNAVAILABLE
            stats["snap"] = {"attempted": True, "available": False, "reason": str(exc)}
    else:
        stats["snap"] = {"attempted": False,
                         "reason": "no in-scope jurisdiction or no reference store"}
    stats["snap_status"] = _counts(out["snap_status"])
    return Enrichment(frame=out, stats=stats)


def _counts(series: pd.Series) -> dict[str, int]:
    return {str(k): int(v) for k, v in series.value_counts(dropna=False).items()}


# ---------------------------------------------------------------------------
# the crash match
# ---------------------------------------------------------------------------


def match_crashes(
    parties: pd.DataFrame,
    enriched: pd.DataFrame,
    *,
    gold_root: Path,
    con=None,
) -> tuple[pd.DataFrame, dict[str, Any]]:
    """Best effort, per the config thresholds. Returns (matches, stats)."""
    import duckdb
    from ..geo import snap as snap_mod

    cfg = config.compliance()
    tolerance = int(cfg["match_date_tolerance_days"])
    max_distance = float(cfg["match_max_distance_m"])

    out = pd.DataFrame({
        "party_token": parties["party_token"].to_numpy(),
        "crash_sk": pd.Series([pd.NA] * len(parties), dtype="Int64"),
        "match_method": MATCH_NONE,
        "match_distance_m": pd.Series([np.nan] * len(parties), dtype="float64"),
        "source_system": SOURCE_OTHER,
        "severity_ordinal": pd.Series([pd.NA] * len(parties), dtype="Int64"),
        "pedestrian_involved": pd.Series([pd.NA] * len(parties), dtype="boolean"),
        "bicyclist_involved": pd.Series([pd.NA] * len(parties), dtype="boolean"),
        "hit_run": pd.Series([pd.NA] * len(parties), dtype="boolean"),
        "fhwa_class": pd.Series([pd.NA] * len(parties), dtype="Int64"),
        "is_adverse": pd.Series([pd.NA] * len(parties), dtype="boolean"),
        "crash_snap_status": None,
        "osm_maxspeed_mph": np.nan,
        "weather_status": None,
        "era5_precipitation_mm": np.nan,
    })
    fact = Path(gold_root) / "fact_crash.parquet"
    stats: dict[str, Any] = {
        "attempted": bool(fact.exists()),
        "tolerance_days": tolerance,
        "max_distance_m": max_distance,
        "crs_by_jurisdiction": {},
        "matched_by_jurisdiction": {},
        "candidates_by_jurisdiction": {},
    }
    if not fact.exists():
        stats["reason"] = f"no gold fact_crash at {fact}"
        return out, stats

    incident = pd.to_datetime(parties["incident_date"], errors="coerce")
    lo_date = (incident.min() - timedelta(days=tolerance)).date()
    hi_date = (incident.max() + timedelta(days=tolerance)).date()
    owned = con is None
    con = con or duckdb.connect()
    try:
        path = str(fact).replace("'", "''")
        road = str(Path(gold_root, "dim_road_class.parquet")).replace("'", "''")
        weather = str(Path(gold_root, "dim_weather_condition.parquet")).replace("'", "''")
        geo = str(Path(gold_root, "crash_geo.parquet")).replace("'", "''")
        geo_select = (
            "g.snap_status, g.osm_maxspeed_mph, g.weather_status, "
            "g.era5_precipitation_mm" if Path(gold_root, "crash_geo.parquet").exists()
            else "NULL::VARCHAR snap_status, NULL::DOUBLE osm_maxspeed_mph, "
                 "NULL::VARCHAR weather_status, NULL::DOUBLE era5_precipitation_mm"
        )
        geo_join = (f"LEFT JOIN read_parquet('{geo}') g USING (crash_sk)"
                    if Path(gold_root, "crash_geo.parquet").exists() else "")
        crashes = con.execute(
            f"""SELECT f.crash_sk, f.jurisdiction, f.primary_source_system,
                       f.crash_date, f.latitude, f.longitude, f.severity_ordinal,
                       f.pedestrian_involved, f.bicyclist_involved, f.hit_run,
                       r.fhwa_class, w.is_adverse, {geo_select}
                FROM read_parquet('{path}') f
                LEFT JOIN read_parquet('{road}') r USING (road_class_sk)
                LEFT JOIN read_parquet('{weather}') w USING (weather_condition_sk)
                {geo_join}
                WHERE f.crash_date BETWEEN DATE '{lo_date}' AND DATE '{hi_date}'
                  AND f.latitude IS NOT NULL AND f.longitude IS NOT NULL
                ORDER BY f.crash_sk"""
        ).df()
    finally:
        if owned:
            con.close()
    stats["crash_rows_in_date_window"] = int(len(crashes))
    if crashes.empty:
        return out, stats

    import geopandas as gpd
    import shapely

    crash_date = pd.to_datetime(crashes["crash_date"])
    for jurisdiction in sorted(set(parties["jurisdiction"].astype(str))):
        sel = (parties["jurisdiction"].astype(str) == jurisdiction).to_numpy()
        pool = crashes[crashes["jurisdiction"] == jurisdiction]
        stats["candidates_by_jurisdiction"][jurisdiction] = int(len(pool))
        if pool.empty or not sel.any():
            stats["matched_by_jurisdiction"][jurisdiction] = 0
            continue
        # The SAME projected CRS Phase 4 snapped in, per jurisdiction. A
        # metric threshold demands a metric CRS; 4326 degrees are not metres,
        # and Web Mercator "metres" are not metres either at these latitudes.
        epsg = snap_mod.snap_crs_for(jurisdiction)
        stats["crs_by_jurisdiction"][jurisdiction] = epsg
        pool_g = gpd.GeoDataFrame(
            pool.reset_index(drop=True),
            geometry=gpd.points_from_xy(pool["longitude"], pool["latitude"]),
            crs="EPSG:4326",
        ).to_crs(epsg=epsg)
        party = parties[sel]
        party_g = gpd.GeoDataFrame(
            {"party_token": party["party_token"].to_numpy(),
             "incident_date": pd.to_datetime(party["incident_date"]).to_numpy()},
            geometry=gpd.points_from_xy(
                pd.to_numeric(party["party_longitude"], errors="coerce"),
                pd.to_numeric(party["party_latitude"], errors="coerce"),
            ),
            crs="EPSG:4326",
        ).to_crs(epsg=epsg)

        matched = 0
        pool_dates = pd.to_datetime(pool_g["crash_date"]).to_numpy()
        for i, row in party_g.iterrows():
            if row.geometry is None or row.geometry.is_empty:
                continue
            delta = np.abs(
                (pool_dates - np.datetime64(row["incident_date"]))
                / np.timedelta64(1, "D")
            )
            near_in_time = delta <= tolerance
            if not near_in_time.any():
                continue
            candidates = pool_g[near_in_time]
            distances = candidates.geometry.distance(row.geometry)
            best = distances.idxmin()
            if distances[best] > max_distance:
                continue
            idx = out.index[out["party_token"] == row["party_token"]][0]
            out.loc[idx, "crash_sk"] = int(candidates.loc[best, "crash_sk"])
            out.loc[idx, "match_method"] = MATCH_SPATIOTEMPORAL
            out.loc[idx, "match_distance_m"] = float(distances[best])
            out.loc[idx, "source_system"] = str(
                candidates.loc[best, "primary_source_system"])
            severity = candidates.loc[best, "severity_ordinal"]
            out.loc[idx, "severity_ordinal"] = (
                pd.NA if pd.isna(severity) else int(severity))
            for column in ("pedestrian_involved", "bicyclist_involved", "hit_run",
                           "fhwa_class", "is_adverse", "osm_maxspeed_mph", "weather_status",
                           "era5_precipitation_mm"):
                value = candidates.loc[best, column]
                out.at[idx, column] = pd.NA if pd.isna(value) else value
            snap_value = candidates.loc[best, "snap_status"]
            out.at[idx, "crash_snap_status"] = (
                pd.NA if pd.isna(snap_value) else snap_value)
            matched += 1
        stats["matched_by_jurisdiction"][jurisdiction] = matched
    stats["matched_total"] = int((out["match_method"] != MATCH_NONE).sum())
    stats["null_model"] = _match_null_model(parties, stats)
    return out, stats


def _match_null_model(parties: pd.DataFrame, stats: Mapping[str, Any]) -> dict[str, Any]:
    """How many of these matches would a coincidence produce? Usually all of them.

    A spatiotemporal match is only evidence of an identity link if it is
    UNLIKELY BY CHANCE, and in a county with thousands of crashes in a two-day
    window it is not. The null model is deliberately crude and stated as such:
    scatter the candidate crashes uniformly over the jurisdiction envelope and
    ask how many land inside a `match_max_distance_m` disc around each party.

        E[matches] = n_parties * candidates * (pi * r^2) / envelope_area

    Crashes are on roads and parties are at addresses, so uniformity is wrong
    in both directions and this is an order of magnitude, not a probability.
    That is enough for the only question it has to answer: if E[matches] is of
    the same order as the observed count, NO INDIVIDUAL MATCH IS EVIDENCE OF
    ANYTHING, and the memo must not describe the result as an identity join.
    Recorded in `_compliance_manifest.json` so the claim is reproducible.
    """
    cfg = config.compliance()
    envelope_by = dict(cfg["envelope_by_jurisdiction"])
    radius = float(cfg["match_max_distance_m"])
    out: dict[str, Any] = {
        "method": "uniform-scatter over the jurisdiction envelope",
        "radius_m": radius,
        "caveat": ("An order of magnitude, not a probability: crashes lie on "
                   "roads and parties at addresses, so uniformity is wrong in "
                   "both directions. It answers one question -- whether a "
                   "match is distinguishable from a coincidence."),
        "by_jurisdiction": {},
    }
    for jurisdiction, candidates in stats.get("candidates_by_jurisdiction", {}).items():
        name = envelope_by.get(jurisdiction)
        n_parties = int((parties["jurisdiction"].astype(str) == jurisdiction).sum())
        if not name or not candidates or not n_parties:
            continue
        env = config.envelope(name)
        # Envelope area on the ellipsoid, from its own edge lengths -- the same
        # geodesic machinery Phase 2 uses, so no projection is introduced for
        # a number that only needs one significant figure.
        height_m = c.geodesic_distance_m(env["min_lat"], env["min_lon"],
                                         env["max_lat"], env["min_lon"])
        width_m = c.geodesic_distance_m(
            (env["min_lat"] + env["max_lat"]) / 2, env["min_lon"],
            (env["min_lat"] + env["max_lat"]) / 2, env["max_lon"])
        area_km2 = (height_m * width_m) / 1e6
        disc_km2 = math.pi * (radius / 1000.0) ** 2
        expected = n_parties * candidates * disc_km2 / area_km2 if area_km2 else 0.0
        observed = int(stats["matched_by_jurisdiction"].get(jurisdiction, 0))
        out["by_jurisdiction"][jurisdiction] = {
            "parties": n_parties,
            "candidate_crashes_in_date_window": int(candidates),
            "envelope_area_km2": round(area_km2, 1),
            "expected_matches_by_chance": round(expected, 2),
            "observed_matches": observed,
            "distinguishable_from_chance": bool(observed > 3 * expected),
        }
    return out


# ---------------------------------------------------------------------------
# ingested_at -- a property of the input, so two builds agree
# ---------------------------------------------------------------------------


def fixture_ingested_at(fixture_path: Path, as_of: date) -> tuple[datetime, str]:
    """(timestamp, how it was derived).

    The git commit time of the fixture file. That is a property of the INPUT,
    identical on every clone of the repo, reproducible by
    `git log -1 --format=%cI -- fixtures/synthetic_parties.csv`, and stable
    across two builds -- which build time is not, and byte-identity is the
    whole claim. Falls back to `as_of` at 00:00 UTC outside a git checkout
    (a source tarball), which is equally stable and is recorded as the
    fallback in the manifest rather than passed off as the commit time.
    """
    try:
        result = subprocess.run(
            ["git", "log", "-1", "--format=%cI", "--", str(fixture_path)],
            cwd=str(config.REPO_ROOT), capture_output=True, text=True, timeout=10,
        )
        stamp = result.stdout.strip()
        if result.returncode == 0 and stamp:
            return datetime.fromisoformat(stamp).astimezone(timezone.utc), "git_commit_time"
    except (OSError, ValueError, subprocess.SubprocessError) as exc:
        log.debug("git commit time unavailable for %s: %s", fixture_path, exc)
    return (datetime.combine(as_of, time.min, tzinfo=timezone.utc),
            "as_of_midnight_utc_fallback")


def lead_id_for(party_token: str) -> str:
    """A surrogate. Never a natural key containing PII (the contract's words).

    Derived from the party token so it is stable across builds without a
    sequence or a mapping table, and prefixed so a lead id can never be
    mistaken for a party token in a log line.
    """
    digest = hashlib.sha256(party_token.encode("utf-8")).hexdigest()
    return f"LD_{digest[:16]}"


# ---------------------------------------------------------------------------
# building the engine's records
# ---------------------------------------------------------------------------


def build_records(
    parties: pd.DataFrame,
    enriched: pd.DataFrame,
    matches: pd.DataFrame,
    *,
    as_of: date,
    honour_business_days: int,
    seller: str,
    internal_dnc: set[str] | None = None,
    ebr_by_token: Mapping[str, list[dict[str, Any]]] | None = None,
    identity_provenance: str = IdentityProvenance.SYNTHETIC_FIXTURE.value,
) -> tuple[list[dict[str, Any]], list[consent_mod.ConsentProvenance],
           list[consent_mod.Revocation]]:
    """One engine record per fixture row, plus the consent artefacts it implies.

    `record_types` declares which blackout rows may reach the row. Every party
    is a `crash_report` (the Florida data gate keys on it), a
    `written_solicitation` (the bar rules) and a `telephone_solicitation` (the
    Maryland channel bar) -- and pointedly NOT an `aviation_accident`, which
    is what stops the wildcard `US` row from applying to every crash on a
    road.
    """
    internal = internal_dnc or set()
    ebr_map = ebr_by_token or {}
    joined = parties.reset_index(drop=True).join(
        enriched.reset_index(drop=True).drop(columns=["party_token"])
    ).join(matches.reset_index(drop=True).drop(columns=["party_token"]))

    records: list[dict[str, Any]] = []
    provenances: list[consent_mod.ConsentProvenance] = []
    revocations: list[consent_mod.Revocation] = []

    for row in joined.to_dict("records"):
        token = str(row["party_token"])
        incident = _as_date(row.get("incident_date"))
        filed = _as_date(row.get("report_filing_date"))
        on_file = bool(row.get("consent_on_file"))
        revoked = bool(row.get("consent_revoked"))

        provenance = None
        if on_file:
            provenance = consent_mod.synthesise_provenance(
                party_id=str(row["party_id_label"]),
                party_token=token,
                obtained_on=filed or incident or as_of,
                timezone_name=row.get("tz_iana"),
                seller=seller,
            )
            provenances.append(provenance)
        row_revocations: list[consent_mod.Revocation] = []
        if revoked:
            revocation = consent_mod.synthesise_revocation(
                party_token=token,
                received_on=filed or incident or as_of,
                honour_business_days=honour_business_days,
                seller=seller,
            )
            row_revocations.append(revocation)
            revocations.append(revocation)

        age = row.get("dnc_scrub_age_days")
        records.append({
            "lead_id": lead_id_for(token),
            "party_token": token,
            "party_id_label": row["party_id_label"],
            "phone_token": row.get("phone_token"),
            "jurisdiction": str(row["jurisdiction"]),
            "identity_provenance": identity_provenance,
            "record_types": ["crash_report", "written_solicitation",
                             "telephone_solicitation"],
            "incident_date": incident,
            "report_filing_date": filed,
            "zip5": row.get("zip5"),
            "npa": row.get("npa"),
            "line_type": row.get("line_type") or None,
            "line_type_asof": row.get("line_type_asof"),
            "rnd_response": (row.get("rnd_response") or None),
            "on_national_dnc": bool(row.get("on_national_dnc")),
            "dnc_scrub_age_days": None if pd.isna(age) else int(age),
            "consent_on_file": on_file,
            "consent_revoked": revoked,
            "consent_provenance": provenance,
            "revocations": row_revocations,
            "internal_dnc_listed": token in internal,
            "ebr": ebr_map.get(token, []),
            "seller": seller,
            "tz_iana": row.get("tz_iana"),
            "tz_source": row.get("tz_source"),
            "envelope_status": row.get("envelope_status"),
            "distance_from_envelope_m": row.get("distance_from_envelope_m"),
            "snap_status": row.get("snap_status"),
            "snap_distance_m": _float_or_none(row.get("snap_distance_m")),
            "h3_r8": row.get("h3_r8"),
            "tract_geoid": row.get("tract_geoid"),
            "bg_geoid": row.get("bg_geoid"),
            "road_class": row.get("osm_highway"),
            "crash_sk": None if pd.isna(row.get("crash_sk")) else int(row["crash_sk"]),
            "match_method": row.get("match_method"),
            "source_system": row.get("source_system"),
            "severity_ordinal": (None if pd.isna(row.get("severity_ordinal"))
                                 else int(row["severity_ordinal"])),
            "pedestrian_involved": _bool_or_none(row.get("pedestrian_involved")),
            "bicyclist_involved": _bool_or_none(row.get("bicyclist_involved")),
            "hit_run": _bool_or_none(row.get("hit_run")),
            "fhwa_class": (None if pd.isna(row.get("fhwa_class"))
                           else int(row["fhwa_class"])),
            "is_adverse": _bool_or_none(row.get("is_adverse")),
            "crash_snap_status": (None if pd.isna(row.get("crash_snap_status"))
                                  else row.get("crash_snap_status")),
            "osm_maxspeed_mph": _float_or_none(row.get("osm_maxspeed_mph")),
            "weather_status": (None if pd.isna(row.get("weather_status"))
                               else row.get("weather_status")),
            "era5_precipitation_mm": _float_or_none(
                row.get("era5_precipitation_mm")),
            "fixture_note": row.get("fixture_note") or None,
        })
    return records, provenances, revocations


# ---------------------------------------------------------------------------
# the contract row
# ---------------------------------------------------------------------------


def lead_row(
    record: Mapping[str, Any],
    decision: EligibilityDecision,
    *,
    ingested_at: datetime,
    as_of: date,
    geo_build_sha: str | None,
) -> dict[str, Any]:
    """One row in the shape of `contracts/lead_output.schema.json`.

    What is NOT here is the point: no name, no street, no city, no E.164
    number and no coordinate. The contract has no latitude or longitude field
    for exactly that reason, and `output/sample_leads.csv` is committed.
    """
    window = decision.calling_window
    provenance = record.get("consent_provenance")
    scrub_age = record.get("dnc_scrub_age_days")
    priority_score = None
    score_components = None
    if decision.status in ("ELIGIBLE", "BLOCKED_UNTIL"):
        # Imported here to keep the compliance module usable on its own while
        # making the stage boundary explicit: legality is decided first.
        from ..scoring.score import score_lead
        scored = score_lead(record, as_of=as_of)
        priority_score = scored.priority_score
        score_components = scored.score_components
    return {
        "lead_id": record["lead_id"],
        "source_system": record.get("source_system") or SOURCE_OTHER,
        # With no matched crash the record IS its own source record, so the
        # id is the token -- a surrogate, not an identifier.
        "source_record_id": (str(record["crash_sk"]) if record.get("crash_sk")
                             else str(record["party_token"])),
        "ingested_at": ingested_at.isoformat(),
        "incident_date": _iso_date(record.get("incident_date")),
        "report_filing_date": _iso_date(record.get("report_filing_date")),
        "jurisdiction": record["jurisdiction"],
        "geo": {
            "zip5": record.get("zip5") or None,
            "census_tract": record.get("tract_geoid"),
            "census_bg": record.get("bg_geoid"),
            "h3_r8": record.get("h3_r8"),
            "iana_timezone": record.get("tz_iana"),
            "road_class": record.get("road_class"),
            "snap_distance_m": record.get("snap_distance_m"),
        },
        "severity_ordinal": record.get("severity_ordinal"),
        "contact": {
            "phone_token": record.get("phone_token"),
            "line_type": record.get("line_type"),
            "line_type_asof": _iso_dt(record.get("line_type_asof")),
            # Derived from the fixture's own `dnc_scrub_age_days` against the
            # frozen `as_of`: an age in days and a date are the same fact, and
            # the contract asks for the date.
            "dnc_scrub_asof": (
                None if scrub_age is None
                else datetime.combine(as_of - timedelta(days=int(scrub_age)),
                                      time.min, tzinfo=timezone.utc).isoformat()
            ),
            "rnd_response": record.get("rnd_response"),
            "calling_window_local": window.as_contract() if window else None,
        },
        "consent": provenance.as_contract() if provenance is not None else None,
        "eligibility_status": decision.status,
        "blocked_until_date": _iso_date(decision.blocked_until_date),
        "reason_codes": list(decision.reason_codes),
        "legal_basis": list(decision.legal_basis),
        "decision_lineage_id": decision.decision_lineage_id,
        "evaluated_at": decision.evaluated_at.isoformat(),
        "ruleset_version": decision.ruleset_version,
        "priority_score": priority_score,
        "score_components": score_components,
    }


def lineage_columns(record: Mapping[str, Any], decision: EligibilityDecision,
                    *, build_sha: str, geo_build_sha: str | None) -> dict[str, Any]:
    """The provenance columns the parquet `leads` table carries beside the contract."""
    return {
        "party_token": record["party_token"],
        "party_id_label": record.get("party_id_label"),
        "crash_sk": record.get("crash_sk"),
        "match_method": record.get("match_method"),
        "identity_provenance": record.get("identity_provenance"),
        "tz_source": record.get("tz_source"),
        "snap_status": record.get("snap_status"),
        "envelope_status": record.get("envelope_status"),
        "_compliance_build_sha": build_sha,
        "_geo_build_sha": geo_build_sha,
        "ruleset_sha256": decision.ruleset_sha256,
        "blackout_sha256": decision.blackout_sha256,
    }


def score_inputs(
    rows: Sequence[Mapping[str, Any]]
) -> list[dict[str, Any]]:
    """Crash-only facts handed to scoring after the compliance status gate.

    ELIGIBLE and BLOCKED_UNTIL records only. An INELIGIBLE record is not a
    lead with a low score, it is not a lead -- ranking it would put it in a
    queue, and a queue is where things get dialled. `priority_score` and
    `score_components` stay null in this phase; the contract says
    "Named contributions. No opaque blob", so whatever fills them will be a
    dict of named terms. Geography and contact fields are deliberately absent:
    they may route or block a lead, but they cannot increase its priority.
    """
    return [
        {
            "lead_id": r["lead_id"],
            "eligibility_status": r["eligibility_status"],
            "severity_ordinal": r["severity_ordinal"],
            "incident_date": r["incident_date"],
            "pedestrian_involved": r.get("pedestrian_involved"),
            "bicyclist_involved": r.get("bicyclist_involved"),
            "hit_run": r.get("hit_run"),
            "fhwa_class": r.get("fhwa_class"),
            "is_adverse": r.get("is_adverse"),
            "snap_status": r.get("crash_snap_status"),
            "osm_maxspeed_mph": r.get("osm_maxspeed_mph"),
            "weather_status": r.get("weather_status"),
            "era5_precipitation_mm": r.get("era5_precipitation_mm"),
        }
        for r in rows
        if r["eligibility_status"] in ("ELIGIBLE", "BLOCKED_UNTIL")
    ]


def _as_date(value: Any) -> date | None:
    if value is None or value == "" or (isinstance(value, float) and pd.isna(value)):
        return None
    if isinstance(value, datetime):
        return value.date()
    if isinstance(value, date):
        return value
    try:
        return date.fromisoformat(str(value)[:10])
    except ValueError:
        return None


def _iso_date(value: Any) -> str | None:
    d = _as_date(value)
    return d.isoformat() if d else None


def _iso_dt(value: Any) -> str | None:
    if value is None or (isinstance(value, float) and pd.isna(value)):
        return None
    if isinstance(value, datetime):
        return value.isoformat()
    return str(value)


def _float_or_none(value: Any) -> float | None:
    if value is None or pd.isna(value):
        return None
    return round(float(value), 2)


def _bool_or_none(value: Any) -> bool | None:
    if value is None or pd.isna(value):
        return None
    return bool(value)
