"""Gold: the dimensional model. Silver in, star schema out, deterministically.

    python -m src.transform.model
    python -m src.transform.model --jurisdiction MD --jurisdiction TX
    python -m src.transform.model --silver-root /tmp/s --gold-root /tmp/g --small-corpus
    python -m src.transform.model --json

Grain, table by table
---------------------
  fact_crash            one row per RESOLVED crash            crash_sk
  bridge_crash_source   one row per (resolved crash, source record)
  fact_driver           one row per driver                    driver_sk
  fact_non_motorist     one row per non-motorist              non_motorist_sk
  dim_date, dim_time, dim_geography, dim_road_class, dim_weather_condition,
  dim_severity, and the map_*_source tables that are the crosswalks as data.

"Resolved" is the word that carries the assignment's "exactly one row per
crash". `silver.crash` has one row per crash PER SOURCE, and a Texas fatality is
legitimately in it twice -- once from TxDOT, once from FARS. `resolve.py` decides
which pairs are the same event; this module collapses each pair to one fact row
and records both contributors in the bridge. Every in-scope silver crash appears
in the bridge exactly once, which is the reconciliation between the two layers
and a test.

Surrogate keys
--------------
`crash_sk` is the first 60 bits of SHA-256 over the PRIMARY source record's
`crash_uid`. Deterministic, so two builds agree byte for byte; stable, so a key
never moves when a FARS match is found or lost -- the primary is chosen by a
fixed precedence (Montgomery > TxDOT > FARS, config/model.toml) and a FARS record
that gets matched simply stops being a primary. `row_number()` would renumber
the whole table on any insertion; a UUID would break byte-identity on the first
rebuild. Party keys hash the source's own party key the same way.

Gold is a pure function of silver's CURRENT slices. History -- TxDOT amendments,
FARS reissues -- already lives in silver's SCD2 tables under one mechanism, and a
second version history in gold would be a second thing that can disagree. What
gold carries instead is the pointer: `silver_version_no`, `silver_valid_from`,
`is_amended` and the sha256 of the silver table the row came from, so a
restatement is VISIBLE in gold and an 18-month-old decision can be walked back
to the exact silver bytes that produced it.

Unknown members, never NULL keys. Every dimension has a -1 / UNKNOWN row and
every fact foreign key is NOT NULL, so `FARS HOUR = 99` is `time_sk = -1` and a
quarantined coordinate still lands on its county from the source's own county
field. The contract enforces zero orphans with no exceptions; the exceptions
were all turned into members.

Scope is data: config/model.toml lists the jurisdictions gold covers. FARS is
national and everything outside the list is excluded -- with the count per state
written to the manifest, so the crash count changes by declared scope and never
silently.

CRS: gold keeps latitude/longitude as DOUBLE in EPSG:4326, unchanged from
silver. No geometry column and no metric operation happens here; the only
distance in Phase 3 is the entity-resolution one, computed geodesically in
resolve.py. GeoParquet geometry and the bbox covering column are Phase 4.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import logging
import sys
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterable

import duckdb

from .. import config, contracts
from ..config import GOLD_DIR, SILVER_DIR
from ..ingest.watermark import durable_replace
from . import common as c
from . import conformed
from . import resolve
from . import severity_crosswalk

log = logging.getLogger("transform.model")

GOLD_CONTRACT = contracts.CONTRACTS_DIR / "gold.schema.json"

# The 60-bit surrogate: positive BIGINT, deterministic, collision probability
# for 10^6 keys ~ 4e-7. Positive so it survives a round trip through any
# consumer that reads BIGINT as unsigned.
SK = "CAST(('0x' || left(sha256({expr}), 15)) AS BIGINT)"

UNKNOWN_SK = conformed.UNKNOWN_SK

# Column order IS the contract (see contracts/gold.schema.json); fixed here so
# the writer, the validator and the schema agree.
COLUMNS: dict[str, list[str]] = {
    "fact_crash": [
        "crash_sk", "primary_crash_uid", "primary_source_system", "natural_key",
        "jurisdiction", "date_sk", "time_sk", "crash_date", "crash_datetime_local",
        "geography_sk", "road_class_sk", "weather_condition_sk", "severity_sk",
        "severity_ordinal", "severity_ordinal_primary", "severity_grain",
        "latitude", "longitude", "geo_quality",
        "source_count", "in_montgomery", "in_txdot", "in_fars",
        "driver_count", "non_motorist_count", "vehicle_count",
        "fatal_count", "serious_injury_count", "minor_injury_count",
        "possible_injury_count", "count_source", "fars_fatal_count",
        "hit_run", "pedestrian_involved", "bicyclist_involved", "work_zone",
        "intersection_related", "alcohol_suspected", "drug_suspected",
        "is_amended", "silver_version_no", "silver_valid_from", "_silver_build_sha",
    ],
    "bridge_crash_source": [
        "crash_sk", "crash_uid", "source_system", "source_record_id", "is_primary",
        "match_method", "match_tier", "dist_m", "time_delta_min", "date_delta_days",
    ],
    "fact_driver": [
        "driver_sk", "crash_sk", "crash_uid", "source_system", "party_natural_key",
        "date_sk", "severity_sk", "severity_ordinal", "severity_kabco", "severity_note",
        "age", "alcohol_status", "drug_status", "at_fault",
        "vehicle_year", "vehicle_make", "vehicle_model", "vehicle_body_type",
        "speed_limit", "licence_state",
        "silver_version_no", "silver_valid_from",
    ],
    "fact_non_motorist": [
        "non_motorist_sk", "crash_sk", "crash_uid", "source_system", "party_natural_key",
        "date_sk", "severity_sk", "severity_ordinal", "severity_kabco", "severity_note",
        "non_motorist_type_sk", "age", "alcohol_status", "drug_status", "at_fault",
        "pedestrian_movement", "pedestrian_location", "safety_equipment",
        "silver_version_no", "silver_valid_from",
    ],
    "dim_date": [
        "date_sk", "date", "year", "quarter", "month", "day", "day_of_week",
        "is_weekend", "iso_week", "day_name", "month_name",
    ],
    "dim_time": ["time_sk", "hour", "minute", "hour_bucket", "is_night"],
    "dim_geography": [
        "geography_sk", "level", "jurisdiction", "state_fips", "county_fips",
        "county_geoid", "county_name",
    ],
    "dim_road_class": ["road_class_sk", "road_class_code", "road_class_label",
                       "functional_class_known", "fhwa_class"],
    "dim_weather_condition": ["weather_condition_sk", "weather_condition_code",
                              "weather_condition_label", "is_precipitation", "is_adverse"],
    "dim_non_motorist_type": ["non_motorist_type_sk", "non_motorist_type_code",
                              "non_motorist_type_label"],
    "dim_severity": ["severity_sk", "kabco", "label", "is_injury", "is_fatal", "description"],
    "map_severity_source": ["source_system", "source_column", "source_value", "kabco",
                            "severity_ordinal", "severity_label", "notes"],
    "map_road_class_source": ["source_system", "source_column", "source_value",
                              "conformed_code", "road_class_sk", "is_lossy", "notes"],
    "map_weather_source": ["source_system", "source_column", "source_value",
                           "conformed_code", "weather_condition_sk", "is_lossy", "notes"],
    "map_non_motorist_type_source": ["source_system", "source_column", "source_value",
                                     "conformed_code", "non_motorist_type_sk", "is_lossy", "notes"],
}

# Total sort order per table -- write_parquet refuses anything less.
ORDER_BY: dict[str, list[str]] = {
    "fact_crash": ["crash_sk"],
    "bridge_crash_source": ["crash_sk", "crash_uid"],
    "fact_driver": ["driver_sk"],
    "fact_non_motorist": ["non_motorist_sk"],
    "dim_date": ["date_sk"],
    "dim_time": ["time_sk"],
    "dim_geography": ["geography_sk"],
    "dim_road_class": ["road_class_sk"],
    "dim_weather_condition": ["weather_condition_sk"],
    "dim_non_motorist_type": ["non_motorist_type_sk"],
    "dim_severity": ["severity_sk"],
    "map_severity_source": ["source_system", "source_column", "source_value"],
    "map_road_class_source": ["source_system", "source_column", "source_value"],
    "map_weather_source": ["source_system", "source_column", "source_value"],
    "map_non_motorist_type_source": ["source_system", "source_column", "source_value"],
}

# In-memory relation that holds each table before it is written.
RELATION: dict[str, str] = {
    **{t: t for t in COLUMNS},
    "map_weather_source": "map_weather_condition_source",
}

STATE_NAMES = {"MD": "Maryland", "TX": "Texas", "FL": "Florida"}

# FARS PER_TYP -> which party fact. 2/3/4/9/10 are occupants or persons in
# buildings: no other source has them, so they are out of party scope and
# COUNTED, not silently dropped.
FARS_DRIVER_PER_TYP = (1,)
FARS_NON_MOTORIST_PER_TYP = (5, 6, 7, 8, 11, 12, 13, 19)


# ---------------------------------------------------------------------------
# manifest
# ---------------------------------------------------------------------------


@dataclass
class GoldManifest:
    """What silver went in (by hash), what gold came out (by hash), the
    resolution census, and the scope exclusions. Only artefact with wall time."""

    gold_root: Path
    silver_root: Path
    built_at: str = field(default_factory=lambda: datetime.now(timezone.utc).isoformat())
    inputs: dict[str, str] = field(default_factory=dict)
    outputs: dict[str, dict[str, Any]] = field(default_factory=dict)
    stats: dict[str, Any] = field(default_factory=dict)
    warnings: list[str] = field(default_factory=list)

    def warn(self, message: str) -> None:
        log.warning("%s", message)
        self.warnings.append(message)

    def write(self) -> Path:
        dest = self.gold_root / "_build_manifest.json"
        dest.parent.mkdir(parents=True, exist_ok=True)
        payload = {
            "built_at": self.built_at,
            "silver_root": str(self.silver_root),
            "gold_root": str(self.gold_root),
            "config": {"model": config.model()},
            "inputs": dict(sorted(self.inputs.items())),
            "outputs": dict(sorted(self.outputs.items())),
            "stats": dict(sorted(self.stats.items())),
            "warnings": self.warnings,
        }
        tmp = dest.with_name(dest.name + ".part")
        tmp.write_text(json.dumps(payload, indent=2, default=str) + "\n")
        durable_replace(tmp, dest)
        return dest


# ---------------------------------------------------------------------------
# silver in
# ---------------------------------------------------------------------------

SILVER_TABLES = {
    # view name          (source, table)
    "silver_crash":     ("", "crash_current"),
    "moco_crash":       ("montgomery", "crash_current"),
    "moco_driver":      ("montgomery", "driver_current"),
    "moco_nm":          ("montgomery", "non_motorist_current"),
    "txd_crash":        ("txdot", "crash_current"),
    "fars_accident":    ("fars", "accident_current"),
    "fars_person":      ("fars", "person_current"),
    "fars_vehicle":     ("fars", "vehicle_current"),
    "fars_codebook":    ("fars", "codebook"),
}


def _silver_path(root: Path, source: str, table: str) -> Path:
    return root / source / f"{table}.parquet" if source else root / f"{table}.parquet"


def register_silver(con: duckdb.DuckDBPyConnection, root: Path) -> set[str]:
    """One view per silver table that exists; returns the sources present.

    A missing source is not an error: `--source txdot` in Phase 2 produces a
    silver with no Montgomery tables, and gold over that must still build.
    """
    present: set[str] = set()
    for view, (source, table) in SILVER_TABLES.items():
        p = _silver_path(root, source, table)
        if not p.exists():
            continue
        con.execute(f"CREATE OR REPLACE VIEW {view} AS SELECT * FROM "
                    f"read_parquet('{str(p).replace(chr(39), chr(39) * 2)}')")
        if source:
            present.add(source)
    if not _silver_path(root, "", "crash_current").exists():
        raise FileNotFoundError(f"no silver crash_current at {root}")
    return present


def silver_hashes(root: Path) -> dict[str, str]:
    """sha256 per silver table -- from the silver manifest when it exists (it is
    what the build recorded), else recomputed from the files."""
    manifest = root / "_build_manifest.json"
    if manifest.exists():
        payload = json.loads(manifest.read_text())
        return {k: v["sha256"] for k, v in payload.get("outputs", {}).items()}
    return {
        str(p.relative_to(root).with_suffix("")): hashlib.sha256(p.read_bytes()).hexdigest()
        for p in sorted(root.rglob("*.parquet"))
    }


def scope_crashes(con: duckdb.DuckDBPyConnection,
                  jurisdictions: Iterable[str] | None = None) -> dict[str, Any]:
    """`scoped_crash` = silver.crash restricted to the configured jurisdictions."""
    juris = sorted(set(jurisdictions or config.model()["scope"]["jurisdictions"]))
    lits = ", ".join(f"'{j}'" for j in juris)
    con.execute(f"CREATE OR REPLACE TABLE scoped_crash AS "
                f"SELECT * FROM silver_crash WHERE jurisdiction IN ({lits})")
    excluded = con.execute(
        f"SELECT source_system, jurisdiction, COUNT(*) FROM silver_crash "
        f"WHERE jurisdiction NOT IN ({lits}) GROUP BY 1, 2 ORDER BY 3 DESC"
    ).fetchall()
    return {
        "jurisdictions": juris,
        "in_scope": con.execute("SELECT COUNT(*) FROM scoped_crash").fetchone()[0],
        "excluded_total": sum(r[2] for r in excluded),
        "excluded_by_source_jurisdiction": [list(r) for r in excluded],
    }


# ---------------------------------------------------------------------------
# dimensions
# ---------------------------------------------------------------------------


def build_dimensions(con: duckdb.DuckDBPyConnection, jurisdictions: list[str]) -> None:
    # Crosswalks as data.
    severity_crosswalk.register(con, table="map_severity_source")
    for name in ("road_class", "weather_condition", "non_motorist_type"):
        conformed.register(con, name)
        conformed.register_dim(con, name)

    con.execute("""
        CREATE OR REPLACE TABLE dim_severity AS
        SELECT * FROM (VALUES
          (0, NULL,  'Unknown / not reported', false, false,
           'No severity recorded, OR injured with severity unknown (FARS INJ_SEV 5), OR died prior to crash (FARS 6). NOT "no injury": a null here is a missing fact, and O is a recorded one.'),
          (1, 'O',   'No apparent injury',     false, false,
           'KABCO O. Property damage only at the party level.'),
          (2, 'C',   'Possible injury',        true,  false,
           'KABCO C. LOSSY across sources: Montgomery "Possible Injury", FARS INJ_SEV 1, TxDOT crash_sev_id 3.'),
          (3, 'B',   'Suspected minor injury', true,  false,
           'KABCO B. TxDOT calls this "non-incapacitating"; same MMUCC concept.'),
          (4, 'A',   'Suspected serious injury', true, false,
           'KABCO A. TxDOT "suspected serious" / "incapacitating".'),
          (5, 'K',   'Fatal injury',           true,  true,
           'KABCO K. FARS is a census of exactly these crashes; a FARS death may occur up to 30 days after the crash, so a local at-scene severity below 5 can be a 5 in FARS.')
        ) t(severity_sk, kabco, label, is_injury, is_fatal, description)
    """)

    con.execute("""
        CREATE OR REPLACE TABLE dim_time AS
        SELECT -1 AS time_sk, NULL::INTEGER AS hour, NULL::INTEGER AS minute,
               'UNKNOWN' AS hour_bucket, NULL::BOOLEAN AS is_night
        UNION ALL
        SELECT h * 100 + m, h, m,
               CASE WHEN h <= 5 THEN '00-05' WHEN h <= 9 THEN '06-09'
                    WHEN h <= 15 THEN '10-15' WHEN h <= 19 THEN '16-19' ELSE '20-23' END,
               -- Fixed clock rule, local wall time, pre-localisation. A
               -- placeholder until Phase 4 has coordinates -> timezone -> sun
               -- times; named is_night rather than is_dark for that reason.
               (h < 6 OR h >= 20)
        FROM range(24) t1(h), range(60) t2(m)
    """)

    # Geography: every county in the three states from the Census list (so
    # Phase 4's tracts have a parent), plus one state-level member per
    # jurisdiction for rows whose county is unknown, plus UNKNOWN.
    counties = str(config.CONFIG_DIR / "counties.csv").replace("'", "''")
    state_fips = config.sources()["census"]["state_fips"]
    state_rows = ", ".join(
        f"({int(state_fips[j])}, 'STATE', '{j}', '{state_fips[j]}', NULL, NULL, '{STATE_NAMES[j]}')"
        for j in jurisdictions if j in state_fips
    )
    con.execute(f"""
        CREATE OR REPLACE TABLE dim_geography AS
        SELECT -1 AS geography_sk, 'UNKNOWN' AS level, NULL::VARCHAR AS jurisdiction,
               NULL::VARCHAR AS state_fips, NULL::VARCHAR AS county_fips,
               NULL::VARCHAR AS county_geoid, NULL::VARCHAR AS county_name
        UNION ALL
        SELECT * FROM (VALUES {state_rows})
            t(geography_sk, level, jurisdiction, state_fips, county_fips, county_geoid, county_name)
        UNION ALL
        SELECT CAST(county_geoid AS INTEGER), 'COUNTY', state, state_fips, county_fips,
               county_geoid, county_name
        FROM read_csv('{counties}', header = true, comment = '#',
                      types = {{'state_fips': 'VARCHAR', 'county_fips': 'VARCHAR',
                                'county_geoid': 'VARCHAR'}})
        WHERE state IN ({", ".join(f"'{j}'" for j in jurisdictions)})
    """)


def build_dim_date(con: duckdb.DuckDBPyConnection) -> None:
    lo, hi = con.execute(
        "SELECT MIN(d), MAX(d) FROM (SELECT crash_date d FROM fact_crash "
        "UNION ALL SELECT crash_date FROM fact_driver_src "
        "UNION ALL SELECT crash_date FROM fact_non_motorist_src)"
    ).fetchone()
    con.execute(f"""
        CREATE OR REPLACE TABLE dim_date AS
        SELECT CAST(strftime(d, '%Y%m%d') AS INTEGER) AS date_sk, d AS date,
               year(d) AS year, quarter(d) AS quarter, month(d) AS month, day(d) AS day,
               isodow(d) AS day_of_week, isodow(d) >= 6 AS is_weekend,
               weekofyear(d) AS iso_week, dayname(d) AS day_name, monthname(d) AS month_name
        FROM (SELECT CAST(unnest(generate_series(DATE '{lo}', DATE '{hi}', INTERVAL 1 DAY)) AS DATE) d)
    """)


# ---------------------------------------------------------------------------
# bridge
# ---------------------------------------------------------------------------


def build_bridge(con: duckdb.DuckDBPyConnection) -> None:
    """Every scoped crash_uid exactly once; matched FARS rows hang off the
    local record's key; everything else is its own primary."""
    con.execute(f"""
        CREATE OR REPLACE TABLE bridge_crash_source AS
        WITH matched_fars AS (SELECT fars_uid FROM er_matches),
        primaries AS (
            SELECT s.crash_uid, s.source_system, s.source_record_id,
                   {SK.format(expr='s.crash_uid')} AS crash_sk
            FROM scoped_crash s
            WHERE s.crash_uid NOT IN (SELECT fars_uid FROM matched_fars)
        )
        SELECT p.crash_sk, p.crash_uid, p.source_system, p.source_record_id,
               true AS is_primary,
               CASE WHEN EXISTS (SELECT 1 FROM er_matches m WHERE m.local_uid = p.crash_uid)
                    THEN 'PRIMARY_OF_MATCH' ELSE 'SINGLE_SOURCE' END AS match_method,
               NULL::VARCHAR AS match_tier, NULL::DOUBLE AS dist_m,
               NULL::BIGINT AS time_delta_min, NULL::INTEGER AS date_delta_days
        FROM primaries p
        UNION ALL
        SELECT p.crash_sk, s.crash_uid, s.source_system, s.source_record_id,
               false,
               CASE m.tier WHEN 'A' THEN 'FATAL_DATE_GEO_TIME_A'
                           WHEN 'B' THEN 'FATAL_DATE_GEO_TIME_B'
                           ELSE 'FATAL_DATE_COUNTY_TIME' END,
               m.tier, m.dist_m, m.dt_min, CAST(pr.dd AS INTEGER)
        FROM er_matches m
        JOIN er_pairs pr USING (fars_uid, local_uid)
        JOIN scoped_crash s ON s.crash_uid = m.fars_uid
        JOIN primaries p ON p.crash_uid = m.local_uid
    """)


# ---------------------------------------------------------------------------
# facts
# ---------------------------------------------------------------------------


def _crash_attr_sql(present: set[str]) -> str:
    """Per-source crash attributes in ONE shape, unioned. Everything a fact
    needs that silver.crash does not already carry."""
    parts: list[str] = []
    if "montgomery" in present:
        parts.append("""
        SELECT 'MONTGOMERY_MD:' || m.report_number AS crash_uid,
               m.report_number AS natural_key,
               coalesce(m.route_type, '__NULL__') AS road_value,
               coalesce(m.weather, '__NULL__') AS weather_value,
               '24031' AS county_geoid, '24' AS state_fips,
               coalesce(d.n_driver, 0) AS driver_count,
               coalesce(n.n_nm, 0) AS non_motorist_count,
               coalesce(d.n_vehicle, 0) AS vehicle_count,
               coalesce(d.k, 0) + coalesce(n.k, 0) AS fatal_count,
               coalesce(d.a, 0) + coalesce(n.a, 0) AS serious_injury_count,
               coalesce(d.b, 0) + coalesce(n.b, 0) AS minor_injury_count,
               coalesce(d.c, 0) + coalesce(n.c, 0) AS possible_injury_count,
               'PARTY_ROWS' AS count_source,
               m.hit_run,
               coalesce(n.any_ped, false) AS pedestrian_involved,
               coalesce(n.any_bike, false) AS bicyclist_involved,
               NULL::BOOLEAN AS work_zone,          -- Montgomery publishes no work-zone field
               CASE WHEN m.junction IN ('INTERSECTION', 'INTERSECTION RELATED',
                                        'Intersection or Related', 'Intersection') THEN true
                    WHEN m.junction IN ('NON INTERSECTION', 'Non-Junction', 'Through Roadway',
                                        'Non Intersection') THEN false
               END AS intersection_related,
               m.any_driver_alcohol_suspected AS alcohol_suspected,
               m.any_driver_drug_suspected AS drug_suspected,
               m.valid_from AS silver_valid_from,
               'montgomery/crash_current' AS silver_table
        FROM moco_crash m
        LEFT JOIN (
            SELECT report_number, COUNT(*) n_driver, COUNT(DISTINCT vehicle_id) n_vehicle,
                   COUNT(*) FILTER (WHERE severity_ordinal = 5) k,
                   COUNT(*) FILTER (WHERE severity_ordinal = 4) a,
                   COUNT(*) FILTER (WHERE severity_ordinal = 3) b,
                   COUNT(*) FILTER (WHERE severity_ordinal = 2) c
            FROM moco_driver WHERE is_current GROUP BY 1) d USING (report_number)
        LEFT JOIN (
            SELECT nm.report_number, COUNT(*) n_nm,
                   COUNT(*) FILTER (WHERE nm.severity_ordinal = 5) k,
                   COUNT(*) FILTER (WHERE nm.severity_ordinal = 4) a,
                   COUNT(*) FILTER (WHERE nm.severity_ordinal = 3) b,
                   COUNT(*) FILTER (WHERE nm.severity_ordinal = 2) c,
                   bool_or(x.conformed_code = 'PEDESTRIAN') any_ped,
                   bool_or(x.conformed_code IN ('BICYCLIST', 'OTHER_CYCLIST')) any_bike
            FROM moco_nm nm
            LEFT JOIN map_non_motorist_type_source x
              ON x.source_system = 'MONTGOMERY_MD' AND x.source_column = 'pedestrian_type'
             AND x.source_value = coalesce(nm.pedestrian_type, '__NULL__')
            WHERE nm.is_current GROUP BY 1) n USING (report_number)
        WHERE m.is_current
        """)
    if "txdot" in present:
        parts.append("""
        SELECT 'TXDOT_CRIS:' || t.crash_id AS crash_uid, t.crash_id AS natural_key,
               coalesce(CAST(t.road_cls_id AS VARCHAR), '__NULL__') AS road_value,
               coalesce(CAST(t.wthr_cond_id AS VARCHAR), '__NULL__') AS weather_value,
               CASE WHEN t.county_fips IS NOT NULL THEN '48' || t.county_fips END AS county_geoid,
               '48' AS state_fips,
               -- CRIS publishes counts, not persons: no party grain exists.
               NULL::BIGINT AS driver_count, NULL::BIGINT AS non_motorist_count,
               NULL::BIGINT AS vehicle_count,
               t.death_cnt AS fatal_count, t.sus_serious_injry_cnt AS serious_injury_count,
               t.nonincap_injry_cnt AS minor_injury_count, t.poss_injry_cnt AS possible_injury_count,
               'CRIS_COUNTS' AS count_source,
               NULL::BOOLEAN AS hit_run,             -- no hit-and-run flag in the crash layer
               t.pedestrian_involved_fl AS pedestrian_involved,
               t.bicyclist_involved_fl AS bicyclist_involved,
               t.road_constr_zone_fl AS work_zone,
               t.at_intrsct_fl AS intersection_related,
               NULL::BOOLEAN AS alcohol_suspected,   -- substance flags live on CRIS person rows we do not have
               NULL::BOOLEAN AS drug_suspected,
               t.valid_from AS silver_valid_from, 'txdot/crash_current' AS silver_table
        FROM txd_crash t WHERE t.is_current
        """)
    if "fars" in present:
        parts.append("""
        SELECT 'NHTSA_FARS:' || a.year || '-' || a.st_case AS crash_uid,
               a.year || '-' || a.st_case AS natural_key,
               coalesce(CAST(a.func_sys AS VARCHAR), '__NULL__') AS road_value,
               coalesce(CAST(a.weather AS VARCHAR), '__NULL__') AS weather_value,
               CASE WHEN a.county_fips IS NOT NULL THEN a.state_fips || a.county_fips END AS county_geoid,
               a.state_fips,
               coalesce(p.n_driver, 0) AS driver_count, coalesce(p.n_nm, 0) AS non_motorist_count,
               a.ve_total AS vehicle_count,
               coalesce(p.k, 0) AS fatal_count, coalesce(p.a, 0) AS serious_injury_count,
               coalesce(p.b, 0) AS minor_injury_count, coalesce(p.c, 0) AS possible_injury_count,
               'PARTY_ROWS' AS count_source,
               v.hit_run,
               coalesce(p.any_ped, false) OR coalesce(a.peds, 0) > 0 AS pedestrian_involved,
               coalesce(p.any_bike, false) AS bicyclist_involved,
               CASE WHEN a.wrk_zone IS NULL THEN NULL ELSE a.wrk_zone > 0 END AS work_zone,
               CASE WHEN a.typ_int = 1 THEN false
                    WHEN a.typ_int BETWEEN 2 AND 11 THEN true END AS intersection_related,
               p.alcohol AS alcohol_suspected, p.drugs AS drug_suspected,
               a.valid_from AS silver_valid_from, 'fars/accident_current' AS silver_table
        FROM fars_accident a
        LEFT JOIN (
            SELECT year, st_case,
                   COUNT(*) FILTER (WHERE per_typ = 1) n_driver,
                   COUNT(*) FILTER (WHERE per_typ IN (5, 6, 7, 8, 11, 12, 13, 19)) n_nm,
                   COUNT(*) FILTER (WHERE severity_ordinal = 5) k,
                   COUNT(*) FILTER (WHERE severity_ordinal = 4) a,
                   COUNT(*) FILTER (WHERE severity_ordinal = 3) b,
                   COUNT(*) FILTER (WHERE severity_ordinal = 2) c,
                   bool_or(per_typ = 5) any_ped,
                   bool_or(per_typ IN (6, 7)) any_bike,
                   -- drinking/drugs: 0 no, 1 yes, 8 not reported, 9 unknown.
                   -- Any 1 -> true; all 0 -> false; otherwise not known.
                   CASE WHEN bool_or(drinking = 1) THEN true
                        WHEN bool_and(drinking = 0) THEN false END alcohol,
                   CASE WHEN bool_or(drugs = 1) THEN true
                        WHEN bool_and(drugs = 0) THEN false END drugs
            FROM fars_person WHERE is_current GROUP BY 1, 2) p USING (year, st_case)
        LEFT JOIN (
            SELECT year, st_case,
                   CASE WHEN bool_or(hit_run = 1) THEN true
                        WHEN bool_and(hit_run = 0) THEN false END hit_run
            FROM fars_vehicle WHERE is_current GROUP BY 1, 2) v USING (year, st_case)
        WHERE a.is_current
        """)
    return "\nUNION ALL BY NAME\n".join(f"({p})" for p in parts)


def build_fact_crash(con: duckdb.DuckDBPyConnection, present: set[str],
                     hashes: dict[str, str]) -> None:
    con.execute(f"CREATE OR REPLACE TABLE crash_attr AS {_crash_attr_sql(present)}")

    sha_cases = " ".join(
        f"WHEN '{k}' THEN '{v}'" for k, v in sorted(hashes.items())
        if k.endswith("crash_current") or k.endswith("accident_current")
    )
    con.execute(f"""
        CREATE OR REPLACE TABLE fact_crash AS
        WITH members AS (
            -- every source record under each resolved crash, with its silver row
            SELECT b.crash_sk, b.is_primary, s.*
            FROM bridge_crash_source b JOIN scoped_crash s USING (crash_uid)
        ),
        agg AS (
            SELECT crash_sk,
                   COUNT(*) AS source_count,
                   bool_or(source_system = 'MONTGOMERY_MD') AS in_montgomery,
                   bool_or(source_system = 'TXDOT_CRIS') AS in_txdot,
                   bool_or(source_system = 'NHTSA_FARS') AS in_fars,
                   MAX(severity_ordinal) AS severity_max,
                   COUNT(DISTINCT severity_ordinal) AS severity_variants
            FROM members GROUP BY 1
        ),
        fars_link AS (
            SELECT m.crash_sk, a.fatals AS fars_fatal_count
            FROM members m
            JOIN fars_accident a ON a.year || '-' || a.st_case = m.source_record_id AND a.is_current
            WHERE m.source_system = 'NHTSA_FARS'
        )
        SELECT p.crash_sk,
               p.crash_uid AS primary_crash_uid,
               p.source_system AS primary_source_system,
               x.natural_key,
               p.jurisdiction,
               CAST(strftime(p.crash_date, '%Y%m%d') AS INTEGER) AS date_sk,
               CASE WHEN p.crash_datetime_local IS NULL THEN -1
                    ELSE hour(p.crash_datetime_local) * 100 + minute(p.crash_datetime_local)
               END AS time_sk,
               p.crash_date, p.crash_datetime_local,
               CASE WHEN gc.geography_sk IS NOT NULL THEN gc.geography_sk
                    WHEN gs.geography_sk IS NOT NULL THEN gs.geography_sk
                    ELSE {UNKNOWN_SK} END AS geography_sk,
               rc.road_class_sk,
               wc.weather_condition_sk,
               agg.severity_max AS severity_sk,
               agg.severity_max AS severity_ordinal,
               p.severity_ordinal AS severity_ordinal_primary,
               CASE WHEN agg.severity_variants > 1 THEN 'RESOLVED_MAX'
                    ELSE p.severity_grain END AS severity_grain,
               p.latitude, p.longitude, p.geo_quality,
               CAST(agg.source_count AS INTEGER) AS source_count,
               agg.in_montgomery, agg.in_txdot, agg.in_fars,
               CAST(x.driver_count AS INTEGER) AS driver_count,
               CAST(x.non_motorist_count AS INTEGER) AS non_motorist_count,
               CAST(x.vehicle_count AS INTEGER) AS vehicle_count,
               CAST(x.fatal_count AS INTEGER) AS fatal_count,
               CAST(x.serious_injury_count AS INTEGER) AS serious_injury_count,
               CAST(x.minor_injury_count AS INTEGER) AS minor_injury_count,
               CAST(x.possible_injury_count AS INTEGER) AS possible_injury_count,
               x.count_source,
               CAST(fl.fars_fatal_count AS INTEGER) AS fars_fatal_count,
               x.hit_run, x.pedestrian_involved, x.bicyclist_involved, x.work_zone,
               x.intersection_related, x.alcohol_suspected, x.drug_suspected,
               p.is_amended,
               p.version_no AS silver_version_no,
               x.silver_valid_from,
               CASE x.silver_table {sha_cases} END AS _silver_build_sha
        FROM members p
        JOIN agg USING (crash_sk)
        JOIN crash_attr x ON x.crash_uid = p.crash_uid
        LEFT JOIN fars_link fl USING (crash_sk)
        LEFT JOIN map_road_class_source rc
               ON rc.source_system = p.source_system AND rc.source_value = x.road_value
        LEFT JOIN map_weather_condition_source wc
               ON wc.source_system = p.source_system AND wc.source_value = x.weather_value
        LEFT JOIN dim_geography gc ON gc.level = 'COUNTY' AND gc.county_geoid = x.county_geoid
        LEFT JOIN dim_geography gs ON gs.level = 'STATE' AND gs.state_fips = x.state_fips
        WHERE p.is_primary
    """)


def build_party_facts(con: duckdb.DuckDBPyConnection, present: set[str]) -> dict[str, Any]:
    stats: dict[str, Any] = {}
    drivers: list[str] = []
    nms: list[str] = []
    if "montgomery" in present:
        drivers.append(f"""
        SELECT {SK.format(expr="'MONTGOMERY_MD:' || d.person_id")} AS driver_sk,
               b.crash_sk, b.crash_uid, 'MONTGOMERY_MD' AS source_system,
               d.person_id AS party_natural_key, d.crash_date,
               d.severity_ordinal, d.severity_kabco, NULL::VARCHAR AS severity_note,
               NULL::INTEGER AS age,              -- Montgomery publishes no driver age
               d.alcohol_status, d.drug_status, d.driver_at_fault AS at_fault,
               d.vehicle_year, d.vehicle_make, d.vehicle_model, d.vehicle_body_type,
               d.speed_limit, d.drivers_license_state AS licence_state,
               d.version_no AS silver_version_no, d.valid_from AS silver_valid_from
        FROM moco_driver d
        JOIN bridge_crash_source b ON b.crash_uid = 'MONTGOMERY_MD:' || d.report_number
        WHERE d.is_current
        """)
        nms.append(f"""
        SELECT {SK.format(expr="'MONTGOMERY_MD:' || n.person_id")} AS non_motorist_sk,
               b.crash_sk, b.crash_uid, 'MONTGOMERY_MD' AS source_system,
               n.person_id AS party_natural_key, n.crash_date,
               n.severity_ordinal, n.severity_kabco, NULL::VARCHAR AS severity_note,
               coalesce(n.pedestrian_type, '__NULL__') AS type_value,
               NULL::INTEGER AS age,
               n.alcohol_status, n.drug_status,
               CASE n.at_fault WHEN 'Yes' THEN true WHEN 'No' THEN false END AS at_fault,
               n.pedestrian_movement, n.pedestrian_location, n.safety_equipment,
               n.version_no AS silver_version_no, n.valid_from AS silver_valid_from
        FROM moco_nm n
        JOIN bridge_crash_source b ON b.crash_uid = 'MONTGOMERY_MD:' || n.report_number
        WHERE n.is_current
        """)
    if "fars" in present:
        substance = """
            CASE {col} WHEN 1 THEN 'SUSPECTED' WHEN 0 THEN 'NOT_SUSPECTED' ELSE 'UNKNOWN' END"""
        drivers.append(f"""
        SELECT {SK.format(expr="'NHTSA_FARS:' || p.natural_key")} AS driver_sk,
               b.crash_sk, b.crash_uid, 'NHTSA_FARS' AS source_system,
               p.natural_key AS party_natural_key, p.crash_date,
               p.severity_ordinal, p.severity_kabco, p.severity_note,
               p.age,
               {substance.format(col='p.drinking')} AS alcohol_status,
               {substance.format(col='p.drugs')} AS drug_status,
               NULL::BOOLEAN AS at_fault,         -- FARS records no fault determination
               v.mod_year AS vehicle_year,
               mk.label AS vehicle_make,
               NULL::VARCHAR AS vehicle_model,    -- FARS MODEL codes are make-relative; no flat label
               bt.label AS vehicle_body_type,
               NULL::INTEGER AS speed_limit,      -- FARS VSPD_LIM is not carried into silver
               NULL::VARCHAR AS licence_state,    -- L_STATE is a FIPS code; not conformed here
               p.version_no AS silver_version_no, p.valid_from AS silver_valid_from
        FROM fars_person p
        JOIN bridge_crash_source b ON b.crash_uid = 'NHTSA_FARS:' || p.year || '-' || p.st_case
        LEFT JOIN fars_vehicle v ON v.year = p.year AND v.st_case = p.st_case
                                AND v.veh_no = p.veh_no AND v.is_current
        LEFT JOIN fars_codebook mk ON mk.tbl = 'vehicle' AND mk.col = 'MAKE'
                                  AND mk.code = CAST(v.make AS VARCHAR)
                                  AND mk.last_year = (SELECT MAX(last_year) FROM fars_codebook
                                                      WHERE tbl='vehicle' AND col='MAKE'
                                                        AND code = CAST(v.make AS VARCHAR))
        LEFT JOIN fars_codebook bt ON bt.tbl = 'vehicle' AND bt.col = 'BODY_TYP'
                                  AND bt.code = CAST(v.body_typ AS VARCHAR)
                                  AND bt.last_year = (SELECT MAX(last_year) FROM fars_codebook
                                                      WHERE tbl='vehicle' AND col='BODY_TYP'
                                                        AND code = CAST(v.body_typ AS VARCHAR))
        WHERE p.is_current AND p.per_typ IN ({', '.join(map(str, FARS_DRIVER_PER_TYP))})
        """)
        nms.append(f"""
        SELECT {SK.format(expr="'NHTSA_FARS:' || p.natural_key")} AS non_motorist_sk,
               b.crash_sk, b.crash_uid, 'NHTSA_FARS' AS source_system,
               p.natural_key AS party_natural_key, p.crash_date,
               p.severity_ordinal, p.severity_kabco, p.severity_note,
               CAST(p.per_typ AS VARCHAR) AS type_value,
               p.age,
               {substance.format(col='p.drinking')} AS alcohol_status,
               {substance.format(col='p.drugs')} AS drug_status,
               NULL::BOOLEAN AS at_fault,
               NULL::VARCHAR AS pedestrian_movement, NULL::VARCHAR AS pedestrian_location,
               NULL::VARCHAR AS safety_equipment,
               p.version_no AS silver_version_no, p.valid_from AS silver_valid_from
        FROM fars_person p
        JOIN bridge_crash_source b ON b.crash_uid = 'NHTSA_FARS:' || p.year || '-' || p.st_case
        WHERE p.is_current AND p.per_typ IN ({', '.join(map(str, FARS_NON_MOTORIST_PER_TYP))})
        """)
        stats["fars_persons_out_of_party_scope"] = dict(con.execute(f"""
            SELECT per_typ, COUNT(*) FROM fars_person p
            JOIN bridge_crash_source b ON b.crash_uid = 'NHTSA_FARS:' || p.year || '-' || p.st_case
            WHERE p.is_current AND per_typ NOT IN ({', '.join(map(str, FARS_DRIVER_PER_TYP + FARS_NON_MOTORIST_PER_TYP))})
            GROUP BY 1 ORDER BY 1""").fetchall())

    empty_driver = """SELECT NULL::BIGINT driver_sk, NULL::BIGINT crash_sk, NULL::VARCHAR crash_uid,
        NULL::VARCHAR source_system, NULL::VARCHAR party_natural_key, NULL::DATE crash_date,
        NULL::INTEGER severity_ordinal, NULL::VARCHAR severity_kabco, NULL::VARCHAR severity_note,
        NULL::INTEGER age, NULL::VARCHAR alcohol_status, NULL::VARCHAR drug_status, NULL::BOOLEAN at_fault,
        NULL::INTEGER vehicle_year, NULL::VARCHAR vehicle_make, NULL::VARCHAR vehicle_model,
        NULL::VARCHAR vehicle_body_type, NULL::INTEGER speed_limit, NULL::VARCHAR licence_state,
        NULL::INTEGER silver_version_no, NULL::VARCHAR silver_valid_from WHERE false"""
    empty_nm = """SELECT NULL::BIGINT non_motorist_sk, NULL::BIGINT crash_sk, NULL::VARCHAR crash_uid,
        NULL::VARCHAR source_system, NULL::VARCHAR party_natural_key, NULL::DATE crash_date,
        NULL::INTEGER severity_ordinal, NULL::VARCHAR severity_kabco, NULL::VARCHAR severity_note,
        NULL::VARCHAR type_value, NULL::INTEGER age, NULL::VARCHAR alcohol_status,
        NULL::VARCHAR drug_status, NULL::BOOLEAN at_fault, NULL::VARCHAR pedestrian_movement,
        NULL::VARCHAR pedestrian_location, NULL::VARCHAR safety_equipment,
        NULL::INTEGER silver_version_no, NULL::VARCHAR silver_valid_from WHERE false"""

    con.execute("CREATE OR REPLACE TABLE fact_driver_src AS "
                + "\nUNION ALL BY NAME\n".join(f"({q})" for q in (drivers or [empty_driver])))
    con.execute("CREATE OR REPLACE TABLE fact_non_motorist_src AS "
                + "\nUNION ALL BY NAME\n".join(f"({q})" for q in (nms or [empty_nm])))

    con.execute("""
        CREATE OR REPLACE TABLE fact_driver AS
        SELECT driver_sk, crash_sk, crash_uid, source_system, party_natural_key,
               CAST(strftime(crash_date, '%Y%m%d') AS INTEGER) AS date_sk,
               severity_ordinal AS severity_sk, severity_ordinal, severity_kabco, severity_note,
               CAST(age AS INTEGER) AS age, alcohol_status, drug_status, at_fault,
               CAST(vehicle_year AS INTEGER) AS vehicle_year, vehicle_make, vehicle_model,
               vehicle_body_type, CAST(speed_limit AS INTEGER) AS speed_limit, licence_state,
               CAST(silver_version_no AS INTEGER) AS silver_version_no, silver_valid_from
        FROM fact_driver_src
    """)
    con.execute("""
        CREATE OR REPLACE TABLE fact_non_motorist AS
        SELECT s.non_motorist_sk, s.crash_sk, s.crash_uid, s.source_system, s.party_natural_key,
               CAST(strftime(s.crash_date, '%Y%m%d') AS INTEGER) AS date_sk,
               s.severity_ordinal AS severity_sk, s.severity_ordinal, s.severity_kabco, s.severity_note,
               x.non_motorist_type_sk,
               CAST(s.age AS INTEGER) AS age, s.alcohol_status, s.drug_status, s.at_fault,
               s.pedestrian_movement, s.pedestrian_location, s.safety_equipment,
               CAST(s.silver_version_no AS INTEGER) AS silver_version_no, s.silver_valid_from
        FROM fact_non_motorist_src s
        LEFT JOIN map_non_motorist_type_source x
          ON x.source_system = s.source_system
         AND x.source_column = CASE s.source_system WHEN 'MONTGOMERY_MD' THEN 'pedestrian_type'
                                                    ELSE 'per_typ' END
         AND x.source_value = s.type_value
    """)
    return stats


# ---------------------------------------------------------------------------
# drift: every source value must have a crosswalk row
# ---------------------------------------------------------------------------


def check_crosswalks(con: duckdb.DuckDBPyConnection, present: set[str], *,
                     allow_unmapped: bool, manifest: GoldManifest) -> None:
    checks = []
    if "montgomery" in present:
        checks += [
            ("road_class", "crash_attr", "MONTGOMERY_MD", "route_type",
             "road_value", "crash_uid LIKE 'MONTGOMERY_MD:%'"),
            ("weather_condition", "crash_attr", "MONTGOMERY_MD", "weather",
             "weather_value", "crash_uid LIKE 'MONTGOMERY_MD:%'"),
            ("non_motorist_type", "fact_non_motorist_src", "MONTGOMERY_MD", "pedestrian_type",
             "type_value", "source_system = 'MONTGOMERY_MD'"),
        ]
    if "txdot" in present:
        checks += [
            ("road_class", "crash_attr", "TXDOT_CRIS", "road_cls_id",
             "road_value", "crash_uid LIKE 'TXDOT_CRIS:%'"),
            ("weather_condition", "crash_attr", "TXDOT_CRIS", "wthr_cond_id",
             "weather_value", "crash_uid LIKE 'TXDOT_CRIS:%'"),
        ]
    if "fars" in present:
        checks += [
            ("road_class", "crash_attr", "NHTSA_FARS", "func_sys",
             "road_value", "crash_uid LIKE 'NHTSA_FARS:%'"),
            ("weather_condition", "crash_attr", "NHTSA_FARS", "weather",
             "weather_value", "crash_uid LIKE 'NHTSA_FARS:%'"),
            ("non_motorist_type", "fact_non_motorist_src", "NHTSA_FARS", "per_typ",
             "type_value", "source_system = 'NHTSA_FARS'"),
        ]
    for vocab, table, system, column, expr, where in checks:
        try:
            conformed.assert_all_mapped(
                con, vocab, table=f"(SELECT * FROM {table} WHERE {where})",
                source_system=system, source_column=column, value_expr=expr)
        except conformed.UnmappedValue as exc:
            if not allow_unmapped:
                raise
            manifest.warn(str(exc))

    # With --allow-unmapped an unmapped value becomes the UNKNOWN member and a
    # warning, never a NULL key.
    con.execute(f"UPDATE fact_crash SET road_class_sk = {UNKNOWN_SK} WHERE road_class_sk IS NULL")
    con.execute(f"UPDATE fact_crash SET weather_condition_sk = {UNKNOWN_SK} "
                f"WHERE weather_condition_sk IS NULL")
    con.execute(f"UPDATE fact_non_motorist SET non_motorist_type_sk = {UNKNOWN_SK} "
                f"WHERE non_motorist_type_sk IS NULL")


# ---------------------------------------------------------------------------
# build
# ---------------------------------------------------------------------------


def build_gold(
    *,
    silver_root: Path | None = None,
    gold_root: Path | None = None,
    jurisdictions: Iterable[str] | None = None,
    allow_unmapped: bool = False,
    validate: bool = True,
    small_corpus: bool = False,
    threads: int | None = None,
) -> dict[str, Any]:
    silver = Path(silver_root) if silver_root else SILVER_DIR
    gold = Path(gold_root) if gold_root else GOLD_DIR
    con = c.connect(threads=threads)
    manifest = GoldManifest(gold_root=gold, silver_root=silver)

    present = register_silver(con, silver)
    manifest.inputs = silver_hashes(silver)
    manifest.stats["sources_present"] = sorted(present)

    scope = scope_crashes(con, jurisdictions)
    manifest.stats["scope"] = scope
    juris = scope["jurisdictions"]

    log.info("dimensions")
    build_dimensions(con, juris)

    log.info("entity resolution")
    manifest.stats["entity_resolution"] = resolve.run(con, present=present)

    log.info("bridge + facts")
    build_bridge(con)
    build_fact_crash(con, present, manifest.inputs)
    manifest.stats.update(build_party_facts(con, present))
    check_crosswalks(con, present, allow_unmapped=allow_unmapped, manifest=manifest)
    build_dim_date(con)

    manifest.stats["reconciliation"] = reconcile(con)

    plan = [(t, RELATION[t], COLUMNS[t], ORDER_BY[t], gold / f"{t}.parquet") for t in COLUMNS]
    if validate:
        _validate(con, plan, small_corpus=small_corpus)

    for table, relation, cols, order_by, dest in plan:
        info = c.write_parquet(con, relation, dest, columns=cols, order_by=order_by)
        manifest.outputs[table] = info
        log.info("wrote %s (%s rows, %s)", dest.name, info["rows"], info["sha256"][:12])
    manifest.write()
    con.close()
    return {"outputs": manifest.outputs, "stats": manifest.stats,
            "warnings": manifest.warnings, "gold_root": str(gold)}


def reconcile(con: duckdb.DuckDBPyConnection) -> dict[str, Any]:
    """Silver -> gold row accounting. Every number here is asserted by a test."""
    q = lambda sql: con.execute(sql).fetchone()[0]
    return {
        "scoped_silver_crashes": q("SELECT COUNT(*) FROM scoped_crash"),
        "bridge_rows": q("SELECT COUNT(*) FROM bridge_crash_source"),
        "bridge_distinct_crash_uid": q("SELECT COUNT(DISTINCT crash_uid) FROM bridge_crash_source"),
        "bridge_primaries": q("SELECT COUNT(*) FROM bridge_crash_source WHERE is_primary"),
        "fact_crash_rows": q("SELECT COUNT(*) FROM fact_crash"),
        "fact_crash_multi_source": q("SELECT COUNT(*) FROM fact_crash WHERE source_count > 1"),
        "fact_crash_resolved_max": q(
            "SELECT COUNT(*) FROM fact_crash WHERE severity_grain = 'RESOLVED_MAX'"),
        "fact_driver_rows": q("SELECT COUNT(*) FROM fact_driver"),
        "fact_non_motorist_rows": q("SELECT COUNT(*) FROM fact_non_motorist"),
        "by_primary_source": dict(con.execute(
            "SELECT primary_source_system, COUNT(*) FROM fact_crash GROUP BY 1 ORDER BY 1").fetchall()),
        "geography_unknown": q(f"SELECT COUNT(*) FROM fact_crash WHERE geography_sk = {UNKNOWN_SK}"),
        "geography_state_level": q(
            "SELECT COUNT(*) FROM fact_crash f JOIN dim_geography g USING (geography_sk) "
            "WHERE g.level = 'STATE'"),
        "time_unknown": q("SELECT COUNT(*) FROM fact_crash WHERE time_sk = -1"),
        "road_class_by_code": dict(con.execute(
            "SELECT road_class_code, COUNT(*) FROM fact_crash JOIN dim_road_class USING (road_class_sk) "
            "GROUP BY 1 ORDER BY 2 DESC").fetchall()),
        "weather_by_code": dict(con.execute(
            "SELECT weather_condition_code, COUNT(*) FROM fact_crash "
            "JOIN dim_weather_condition USING (weather_condition_sk) GROUP BY 1 ORDER BY 2 DESC").fetchall()),
    }


def _validate(con, plan, *, small_corpus: bool) -> None:
    contract = contracts.load_contract(GOLD_CONTRACT)
    resolve_map = {f"gold.{t}": rel for t, rel, *_ in plan}
    violations: list[contracts.Violation] = []
    for table, relation, cols, _order, _dest in plan:
        projected = f"{relation}__projected"
        col_sql = ", ".join(c.quote_ident(x) for x in cols)
        con.execute(f"CREATE OR REPLACE VIEW {projected} AS SELECT {col_sql} FROM {relation}")
        violations += contracts.validate_relation(
            con, projected, contract, f"gold.{table}",
            check_row_count_min=not small_corpus)
        violations += contracts.validate_foreign_keys(
            con, contract, f"gold.{table}", projected, resolve_map)
    contracts.raise_for(violations, context="gold.schema.json")


def _cli(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(prog="python -m src.transform.model",
                                 description="Build the gold dimensional model from silver.")
    ap.add_argument("--silver-root", type=Path, default=None)
    ap.add_argument("--gold-root", type=Path, default=None)
    ap.add_argument("--jurisdiction", action="append",
                    help="override config/model.toml scope (repeatable)")
    ap.add_argument("--allow-unmapped", action="store_true",
                    help="map unmapped crosswalk values to UNKNOWN and warn, instead of failing")
    ap.add_argument("--no-validate", action="store_true")
    ap.add_argument("--small-corpus", action="store_true",
                    help="skip row_count_min floors (fixture-scale builds)")
    ap.add_argument("--threads", type=int, default=None)
    ap.add_argument("--json", action="store_true")
    ap.add_argument("-v", "--verbose", action="store_true")
    args = ap.parse_args(argv)
    logging.basicConfig(level=logging.DEBUG if args.verbose else logging.INFO,
                        format="%(levelname)s %(name)s %(message)s")
    result = build_gold(
        silver_root=args.silver_root, gold_root=args.gold_root,
        jurisdictions=args.jurisdiction, allow_unmapped=args.allow_unmapped,
        validate=not args.no_validate, small_corpus=args.small_corpus, threads=args.threads,
    )
    if args.json:
        print(json.dumps(result, indent=2, default=str))
    else:
        print(f"\ngold -> {result['gold_root']}")
        for name, info in sorted(result["outputs"].items()):
            print(f"  {name:<30} {info['rows']:>9} rows  {info['bytes'] / 1e6:>7.2f} MB  "
                  f"{info['sha256'][:16]}")
        rec = result["stats"]["reconciliation"]
        print(f"\n  scoped silver crashes {rec['scoped_silver_crashes']} -> bridge "
              f"{rec['bridge_rows']} rows / {rec['bridge_distinct_crash_uid']} distinct uid -> "
              f"fact_crash {rec['fact_crash_rows']} ({rec['fact_crash_multi_source']} multi-source)")
        for w in result["warnings"]:
            print(f"  WARNING: {w}")
    return 0


if __name__ == "__main__":
    sys.exit(_cli())
