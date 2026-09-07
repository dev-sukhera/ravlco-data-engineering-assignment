"""Montgomery County, MD -- bronze to silver.

Three bronze datasets at three grains become three versioned silver tables:

    bhju-22kf  Incidents      -> montgomery/crash_{history,current}.parquet
    mmzv-x632  Drivers        -> montgomery/driver_{history,current}.parquet
    n7fk-dce5  Non-Motorists  -> montgomery/non_motorist_{history,current}.parquet

Natural keys: `report_number` for the crash, `person_id` for each party. Both
party tables carry `report_number` as the crash FK.

`:id` is Socrata's row identity and is deliberately NOT the natural key. It is
1:1 with `report_number` on Incidents today (125,005 each), but it belongs to
the publishing platform rather than to the county's report, and a republish
under a new id must not read as a new crash.


Versioning mode: DELTA, and deletions are not observable
--------------------------------------------------------
Every Montgomery partition is whatever the keyset walk on `(:updated_at, :id)`
returned since the last cursor. A record's absence from a partition therefore
means "unchanged since the watermark", not "deleted" -- so `MODE_DELTA` is used
and `deleted_in_load_ts` is always NULL here.

This is a real gap, not an oversight. **If the county deletes a crash report,
no incremental read of this API will ever tell us.** Detecting it would need a
periodic full-key census (`$select=:id` over the whole table, ~125k ids, one
cheap sweep) diffed against silver's current slice. That is a Phase 8 scheduled
job, not something a delta transform can infer, and inferring deletion from
absence here would delete most of the table on every run.


valid_from is `:updated_at`
---------------------------
Socrata's own mutation stamp, so the version history is the county's, not ours,
and a rebuild reproduces it exactly. Its limitation is measured and worth
stating: 125,005 incident rows share `2024-06-12T20:28:27.326` from a bulk
reload, so it dates that reload rather than any individual pre-June-2024 edit.
It is still the right choice -- the alternative, `_bronze_load_ts`, dates OUR
read and would make every version boundary an artefact of when the pipeline
happened to run.


Crash-level substance flags come from Drivers, not from Incidents
-----------------------------------------------------------------
`bhju-22kf.driver_substance_abuse` is the `, `-joined concatenation of every
driver's value -- 121 distinct strings, mixing both dictionary generations
within a single crash. Silver's crash-level flags are aggregated from the
Drivers table, which has one unambiguous row per driver, and the Incidents
string is parsed with the grammar only as a CROSS-CHECK. The disagreement
count between the two is reported rather than reconciled: they disagree
structurally for the 785 crashes with no driver rows at all, and reconciling
would mean picking one to overwrite the other.
"""

from __future__ import annotations

import logging
from typing import Any

from ..config import envelope
from . import common as c
from .dictionaries import (
    SCHEME_UNMAPPED,
    ObservedValue,
    detect_drift,
    parse_substance,
    parse_substance_list,
)
from .severity_crosswalk import assert_all_mapped, register as register_crosswalk

log = logging.getLogger(__name__)

SOURCE = "montgomery"
SOURCE_SYSTEM = "MONTGOMERY_MD"
JURISDICTION = "MD"

INCIDENTS = "bhju-22kf"
DRIVERS = "mmzv-x632"
NON_MOTORISTS = "n7fk-dce5"

# --------------------------------------------------------------------------
# conformed column lists -- these ARE the contract's column order
# --------------------------------------------------------------------------

CRASH_KEY = ["report_number"]
CRASH_ATTRIBUTES = [
    "local_case_number",
    "agency_name",
    "acrs_report_type",
    "crash_datetime_local",
    "crash_date",
    "latitude",
    "longitude",
    "lat_raw",
    "lon_raw",
    "geo_quality",
    "distance_from_envelope_m",
    "hit_run",
    "route_type",
    "road_name",
    "cross_street_name",
    "road_grade",
    "road_condition",
    "road_alignment",
    "road_division",
    "number_of_lanes",
    "number_of_lanes_raw",
    "lane_direction",
    "lane_type",
    "direction",
    "distance",
    "distance_unit",
    "collision_type",
    "weather",
    "surface_condition",
    "light",
    "traffic_control",
    "junction",
    "intersection_type",
    "first_harmful_event",
    "second_harmful_event",
    "at_fault",
    "municipality",
    "off_road_description",
    "related_non_motorist",
    "driver_substance_abuse_raw",
    "non_motorist_substance_abuse_raw",
    "source_updated_at_utc",
    "source_created_at_utc",
    "source_version",
    "socrata_id",
]

PARTY_COMMON = [
    "report_number",
    "crash_datetime_local",
    "crash_date",
    "latitude",
    "longitude",
    "lat_raw",
    "lon_raw",
    "geo_quality",
    "injury_severity_raw",
    "injury_severity_norm",
    "severity_ordinal",
    "severity_kabco",
    "substance_raw",
    "substance_scheme",
    "alcohol_status",
    "drug_status",
    "substance_detail",
    "source_updated_at_utc",
    "source_created_at_utc",
    "source_version",
    "socrata_id",
]

DRIVER_ATTRIBUTES = PARTY_COMMON + [
    "driver_at_fault",
    "driverless_vehicle",
    "parked_vehicle",
    "driver_distracted_by",
    "drivers_license_state",
    "circumstance",
    "vehicle_id",
    "vehicle_year",
    "vehicle_make",
    "vehicle_model",
    "vehicle_body_type",
    "vehicle_movement",
    "vehicle_going_dir",
    "vehicle_damage_extent",
    "vehicle_first_impact_location",
    "speed_limit",
]

NON_MOTORIST_ATTRIBUTES = PARTY_COMMON + [
    "pedestrian_type",
    "pedestrian_movement",
    "pedestrian_actions",
    "pedestrian_location",
    "safety_equipment",
    "at_fault",
    "duplicate_person_id",
]

# Crash-level columns derived from the party tables after both exist. Appended
# to the crash history table, so they live at the end of its column order.
CRASH_DERIVED = [
    "driver_row_count",
    "non_motorist_row_count",
    "has_driver_rows",
    "has_non_motorist_rows",
    "max_party_severity_ordinal",
    "severity_ordinal",
    "severity_source_value",
    "severity_grain",
    "any_driver_alcohol_suspected",
    "any_driver_drug_suspected",
    "driver_substance_scheme",
    "incident_string_scheme",
    "incident_string_party_count",
    "incident_string_matches_driver_rows",
    "incident_string_matches_driver_distinct",
    "substance_crosscheck_agrees",
]


# --------------------------------------------------------------------------
# helpers
# --------------------------------------------------------------------------


def _geo_case(source: str, lat: str, lon: str) -> str:
    """geo_quality for a lat/lon pair that is never a sentinel here.

    Montgomery publishes zero nulls and zero zeroes in `latitude`/`longitude`
    (verified: 0 of 132,190 bronze rows), which is exactly why the assignment
    calls it out -- a null check passes and the row is still in Pennsylvania.
    MISSING is still handled, because "never" is a measurement of today's data,
    not a guarantee about tomorrow's.
    """
    inside = c.envelope_sql(source, lat, lon)
    return (
        f"CASE WHEN {lat} IS NULL OR {lon} IS NULL THEN '{c.GEO_MISSING}' "
        f"WHEN {inside} THEN '{c.GEO_OK}' ELSE '{c.GEO_OUT_OF_ENVELOPE}' END"
    )


def _canonical(source: str, lat: str, lon: str, col: str) -> str:
    """The canonical coordinate: the raw value inside the envelope, else NULL.

    Defective rows are KEPT. The raw values survive in lat_raw/lon_raw and the
    row stays in the table with geo_quality='OUT_OF_ENVELOPE'. Dropping them
    would change the crash count between bronze and silver, which is the one
    thing the reconciliation must never do -- and a coordinate 170 km outside
    the county is still evidence that a report exists, it is just not evidence
    about where.
    """
    return f"CASE WHEN {c.envelope_sql(source, lat, lon)} THEN {col} ELSE NULL END"


def _register_substance_lookup(ctx: c.BuildContext, relation: str, column: str,
                               table: str) -> str:
    """Materialise the grammar over the DISTINCT values of one column.

    This is the join that keeps the parser in Python and the work in SQL: the
    distinct-value set is 21 rows for Drivers and 121 for Incidents, so the
    Python round trip is over a handful of strings rather than 220,000 rows, and
    the *same* `parse_substance()` the unit tests exercise is what produces the
    lookup. A SQL reimplementation of the grammar would be a second
    implementation to keep in sync, and the drift detector would be testing the
    wrong one.
    """
    con = ctx.con
    values = [
        r[0]
        for r in con.execute(
            f"SELECT DISTINCT {c.quote_ident(column)} FROM {relation}"
        ).fetchall()
    ]
    con.execute(
        f"""CREATE OR REPLACE TABLE {table} (
            raw VARCHAR, scheme VARCHAR, alcohol_status VARCHAR,
            drug_status VARCHAR, substance_detail VARCHAR)"""
    )
    rows = []
    for v in values:
        s = parse_substance(v)
        rows.append((v, s.scheme, s.alcohol_status, s.drug_status, s.substance_detail))
    if rows:
        con.executemany(f"INSERT INTO {table} VALUES (?,?,?,?,?)", rows)
    return table


def _register_incident_substance_lookup(ctx: c.BuildContext, relation: str,
                                        column: str, table: str) -> str:
    """Same, for the crash-level concatenation: one row per distinct string.

    Records what the grammar recovered -- how many parties, which generations,
    and whether any driver is suspected -- so the Incidents column can be
    cross-checked against Drivers without ever being trusted.
    """
    con = ctx.con
    values = [
        r[0]
        for r in con.execute(
            f"SELECT DISTINCT {c.quote_ident(column)} FROM {relation}"
        ).fetchall()
    ]
    con.execute(
        f"""CREATE OR REPLACE TABLE {table} (
            raw VARCHAR, party_count INTEGER, scheme VARCHAR,
            any_alcohol_suspected BOOLEAN, any_drug_suspected BOOLEAN,
            unmapped_tokens INTEGER)"""
    )
    rows = []
    for v in values:
        parties = parse_substance_list(v)
        schemes = {p.scheme for p in parties}
        if not parties:
            scheme = "NULL"
        elif SCHEME_UNMAPPED in schemes:
            scheme = SCHEME_UNMAPPED
        elif len(schemes) > 1:
            scheme = "MIXED"
        else:
            scheme = next(iter(schemes))
        rows.append(
            (
                v,
                len(parties),
                scheme,
                any(p.alcohol_status == "SUSPECTED" for p in parties),
                any(p.drug_status == "SUSPECTED" for p in parties),
                sum(1 for p in parties if p.is_unmapped),
            )
        )
    if rows:
        con.executemany(f"INSERT INTO {table} VALUES (?,?,?,?,?,?)", rows)
    return table


def _drift_observations(ctx: c.BuildContext, relation: str, column: str) -> list[ObservedValue]:
    """One ObservedValue per distinct value, with its crash-date evidence window.

    A single GROUP BY. The detector never touches the database -- that is what
    lets `test_drift_detector` run it against a literal list.
    """
    rows = ctx.con.execute(
        f"""
        SELECT {c.quote_ident(column)} AS v, COUNT(*) AS n,
               MIN(crash_date_time), MAX(crash_date_time),
               MIN(created_at), MAX(created_at)
        FROM {relation} GROUP BY 1
        """
    ).fetchall()
    return [ObservedValue(*r) for r in rows]


# --------------------------------------------------------------------------
# the transform
# --------------------------------------------------------------------------


def build(ctx: c.BuildContext) -> dict[str, Any]:
    """Build all three Montgomery silver tables. Returns per-table stats."""
    con = ctx.con
    stats: dict[str, Any] = {}
    register_crosswalk(con)

    inc_parts = ctx.partitions(SOURCE, INCIDENTS)
    drv_parts = ctx.partitions(SOURCE, DRIVERS)
    nm_parts = ctx.partitions(SOURCE, NON_MOTORISTS)
    for parts in (inc_parts, drv_parts, nm_parts):
        for p in parts:
            ctx.manifest.add_input(p)

    c.bronze_view(con, "moco_inc_raw", inc_parts)
    c.bronze_view(con, "moco_drv_raw", drv_parts)
    c.bronze_view(con, "moco_nm_raw", nm_parts)

    for view, label in (("moco_inc_raw", "incidents"), ("moco_drv_raw", "drivers"),
                        ("moco_nm_raw", "non_motorists")):
        stats[f"bronze_rows_{label}"] = con.execute(
            f"SELECT COUNT(*) FROM {view}"
        ).fetchone()[0]

    _typed_incidents(ctx, stats)
    _typed_parties(ctx, stats)

    _drift_checks(ctx, stats)

    crash_hist = _crash_history(ctx, stats)
    driver_hist = _party_history(
        ctx, "moco_drv_typed", "driver", DRIVER_ATTRIBUTES, stats
    )
    nm_hist = _party_history(
        ctx, "moco_nm_typed", "non_motorist", NON_MOTORIST_ATTRIBUTES, stats
    )
    _augment_crash_with_parties(ctx, crash_hist, driver_hist, nm_hist, stats)
    _reconcile(ctx, stats)

    return stats


def _envelope_distance_table(ctx: c.BuildContext, relation: str, table: str) -> str:
    """Geodesic distance to the envelope, per DISTINCT out-of-envelope coordinate.

    Python touches only the offending pairs -- 68 distinct coordinates behind
    114 rows on the full local bronze -- never the 125,005-row table. The
    distance is geodesic on the WGS84 ellipsoid (see common.GEOD): these are
    Maryland points but the same helper serves Texas, and a projected CRS would
    need a different zone for each. EPSG:3857 would be wrong by ~29% at 39N.
    """
    con = ctx.con
    lat, lon = "TRY_CAST(latitude AS DOUBLE)", "TRY_CAST(longitude AS DOUBLE)"
    rows = con.execute(
        f"""SELECT DISTINCT {lat} AS la, {lon} AS lo FROM {relation}
            WHERE {lat} IS NOT NULL AND {lon} IS NOT NULL
              AND NOT {c.envelope_sql(SOURCE, lat, lon)}"""
    ).fetchall()
    con.execute(f"CREATE OR REPLACE TABLE {table} (la DOUBLE, lo DOUBLE, d DOUBLE)")
    if rows:
        con.executemany(
            f"INSERT INTO {table} VALUES (?,?,?)",
            [(la, lo, c.distance_from_envelope_m(SOURCE, la, lo)) for la, lo in rows],
        )
    return table


def _typed_incidents(ctx: c.BuildContext, stats: dict[str, Any]) -> None:
    """Type the incident columns and resolve coordinates against the envelope."""
    con = ctx.con
    lat, lon = 'TRY_CAST(r.latitude AS DOUBLE)', 'TRY_CAST(r.longitude AS DOUBLE)'
    _envelope_distance_table(ctx, "moco_inc_raw", "moco_inc_geodist")

    con.execute(
        f"""
        CREATE OR REPLACE VIEW moco_inc_typed AS
        SELECT
            r.report_number,
            r.local_case_number,
            r.agency_name,
            r.acrs_report_type,
            -- Naive local wall clock, exactly as published
            -- ("2025-12-25T06:10:00.000"). NOT localised to UTC: that needs the
            -- coordinate-derived IANA zone and is Phase 4. Storing a naive
            -- stamp in a UTC column would be the exact "convert instead of
            -- localize" error the assignment warns about.
            TRY_CAST(r.crash_date_time AS TIMESTAMP)    AS crash_datetime_local,
            TRY_CAST(r.crash_date_time AS DATE)         AS crash_date,
            {_canonical(SOURCE, lat, lon, lat)}         AS latitude,
            {_canonical(SOURCE, lat, lon, lon)}         AS longitude,
            {lat}                                       AS lat_raw,
            {lon}                                       AS lon_raw,
            {_geo_case(SOURCE, lat, lon)}               AS geo_quality,
            CAST(coalesce(g.d, 0.0) AS DOUBLE)          AS distance_from_envelope_m,
            r.hit_run = 'Yes'                           AS hit_run,
            r.route_type, r.road_name, r.cross_street_name, r.road_grade,
            r.road_condition, r.road_alignment, r.road_division,
            -- number_of_lanes is ALSO a comma-joined concatenation, which the
            -- assignment does not mention: 3,830 incident rows carry values
            -- like "2, 3" or "1, 4" where the crash spans two roadway segments
            -- with different lane counts. TRY_CAST nulls them (never 2 or 23);
            -- the raw string is kept so the information is not destroyed and so
            -- the count is auditable.
            TRY_CAST(r.number_of_lanes AS INTEGER)      AS number_of_lanes,
            r.number_of_lanes                           AS number_of_lanes_raw,
            r.lane_direction, r.lane_type, r.direction,
            TRY_CAST(r.distance AS DOUBLE)              AS distance,
            r.distance_unit, r.collision_type, r.weather, r.surface_condition,
            r.light, r.traffic_control, r.junction, r.intersection_type,
            r.first_harmful_event, r.second_harmful_event, r.at_fault,
            r.municipality, r.off_road_description, r.related_non_motorist,
            r.driver_substance_abuse            AS driver_substance_abuse_raw,
            r.non_motorist_substance_abuse      AS non_motorist_substance_abuse_raw,
            -- Socrata publishes these as UTC with a trailing Z. Stored as
            -- naive-UTC TIMESTAMP, the same convention the Phase 1 watermark
            -- store uses, because DuckDB's tz-aware -> Python conversion wants
            -- pytz and there is exactly one timezone in play.
            TRY_CAST(replace(r.":updated_at", 'Z', '') AS TIMESTAMP) AS source_updated_at_utc,
            TRY_CAST(replace(r.":created_at", 'Z', '') AS TIMESTAMP) AS source_created_at_utc,
            r.":version"                                AS source_version,
            r.":id"                                     AS socrata_id,
            r.":updated_at"                             AS updated_at,
            r.":created_at"                             AS created_at,
            r.crash_date_time,
            r._bronze_load_ts, r._bronze_row_sha256, r._bronze_raw_path
        FROM moco_inc_raw r
        LEFT JOIN moco_inc_geodist g
               ON g.la = TRY_CAST(r.latitude AS DOUBLE)
              AND g.lo = TRY_CAST(r.longitude AS DOUBLE)
        """
    )

    stats["incidents_number_of_lanes_typed_fail"] = con.execute(
        "SELECT COUNT(*) FROM moco_inc_raw WHERE number_of_lanes IS NOT NULL "
        "AND TRY_CAST(number_of_lanes AS INTEGER) IS NULL"
    ).fetchone()[0]
    stats["incidents_crash_date_time_typed_fail"] = con.execute(
        "SELECT COUNT(*) FROM moco_inc_raw WHERE crash_date_time IS NOT NULL "
        "AND TRY_CAST(crash_date_time AS TIMESTAMP) IS NULL"
    ).fetchone()[0]
    stats["incidents_coord_typed_fail"] = con.execute(
        "SELECT COUNT(*) FROM moco_inc_raw WHERE (latitude IS NOT NULL "
        "AND TRY_CAST(latitude AS DOUBLE) IS NULL) OR (longitude IS NOT NULL "
        "AND TRY_CAST(longitude AS DOUBLE) IS NULL)"
    ).fetchone()[0]


def _typed_parties(ctx: c.BuildContext, stats: dict[str, Any]) -> None:
    """Type both party tables and normalise their own-grain substance column."""
    con = ctx.con
    lat, lon = 'TRY_CAST(r.latitude AS DOUBLE)', 'TRY_CAST(r.longitude AS DOUBLE)'

    _register_substance_lookup(
        ctx, "moco_drv_raw", "driver_substance_abuse", "moco_drv_substance"
    )
    _register_substance_lookup(
        ctx, "moco_nm_raw", "non_motorist_substance_abuse", "moco_nm_substance"
    )

    common = f"""
            r.report_number,
            TRY_CAST(r.crash_date_time AS TIMESTAMP)    AS crash_datetime_local,
            TRY_CAST(r.crash_date_time AS DATE)         AS crash_date,
            {_canonical(SOURCE, lat, lon, lat)}         AS latitude,
            {_canonical(SOURCE, lat, lon, lon)}         AS longitude,
            {lat}                                       AS lat_raw,
            {lon}                                       AS lon_raw,
            {_geo_case(SOURCE, lat, lon)}               AS geo_quality,
            r.injury_severity                           AS injury_severity_raw,
            -- One case-insensitive vocabulary. The 2024 dictionary generation
            -- change re-cased every value (NO APPARENT INJURY -> No Apparent
            -- Injury) without changing any meaning, so upper() collapses the
            -- two generations; the raw spelling is kept alongside so the
            -- generation stays recoverable.
            upper(r.injury_severity)                    AS injury_severity_norm,
            CAST(x.severity_ordinal AS INTEGER)         AS severity_ordinal,
            x.kabco                                     AS severity_kabco,
            s.raw                                       AS substance_raw,
            s.scheme                                    AS substance_scheme,
            s.alcohol_status                            AS alcohol_status,
            s.drug_status                               AS drug_status,
            s.substance_detail                          AS substance_detail,
            TRY_CAST(replace(r.":updated_at", 'Z', '') AS TIMESTAMP) AS source_updated_at_utc,
            TRY_CAST(replace(r.":created_at", 'Z', '') AS TIMESTAMP) AS source_created_at_utc,
            r.":version"                                AS source_version,
            r.":id"                                     AS socrata_id,
    """

    severity_join = f"""
        LEFT JOIN severity_crosswalk x
               ON x.source_system = '{SOURCE_SYSTEM}'
              AND x.source_column = 'injury_severity'
              AND x.source_value = coalesce(r.injury_severity, '__NULL__')
    """

    con.execute(
        f"""
        CREATE OR REPLACE VIEW moco_drv_typed AS
        SELECT
            r.person_id,
            {common}
            r.driver_at_fault = 'Yes'                   AS driver_at_fault,
            r.driverless_vehicle = 'Yes'                AS driverless_vehicle,
            r.parked_vehicle = 'Yes'                    AS parked_vehicle,
            r.driver_distracted_by, r.drivers_license_state, r.circumstance,
            r.vehicle_id,
            TRY_CAST(r.vehicle_year AS INTEGER)         AS vehicle_year,
            r.vehicle_make, r.vehicle_model, r.vehicle_body_type,
            r.vehicle_movement, r.vehicle_going_dir, r.vehicle_damage_extent,
            r.vehicle_first_impact_location,
            TRY_CAST(r.speed_limit AS INTEGER)          AS speed_limit,
            r.":updated_at" AS updated_at, r.":created_at" AS created_at,
            r.crash_date_time,
            r._bronze_load_ts, r._bronze_row_sha256, r._bronze_raw_path
        FROM moco_drv_raw r
        LEFT JOIN moco_drv_substance s
               ON s.raw IS NOT DISTINCT FROM r.driver_substance_abuse
        {severity_join}
        """
    )

    # duplicate_person_id: 9 non-motorist person_ids appear twice under one
    # report_number, distinguished only by Socrata's :id and disagreeing on a
    # denormalised crash-level field (traffic_control). person_id stays the
    # natural key -- this table's grain is one row per non-motorist -- and the
    # duplicate is resolved deterministically by taking the greatest :id, with
    # the flag kept so the count is never silently absorbed.
    con.execute(
        f"""
        CREATE OR REPLACE VIEW moco_nm_typed AS
        SELECT * EXCLUDE (rn) FROM (
        SELECT
            r.person_id,
            {common}
            r.pedestrian_type, r.pedestrian_movement, r.pedestrian_actions,
            r.pedestrian_location, r.safety_equipment, r.at_fault,
            count(*) OVER (PARTITION BY r.person_id, r._bronze_load_ts) > 1
                                                        AS duplicate_person_id,
            r.":updated_at" AS updated_at, r.":created_at" AS created_at,
            r.crash_date_time,
            r._bronze_load_ts, r._bronze_row_sha256, r._bronze_raw_path,
            row_number() OVER (
                PARTITION BY r.person_id, r._bronze_load_ts ORDER BY r.":id" DESC
            ) AS rn
        FROM moco_nm_raw r
        LEFT JOIN moco_nm_substance s
               ON s.raw IS NOT DISTINCT FROM r.non_motorist_substance_abuse
        {severity_join}
        ) WHERE rn = 1
        """
    )

    for view, label in (("moco_drv_typed", "drivers"), ("moco_nm_typed", "non_motorists")):
        assert_all_mapped(
            con, table=view, source_system=SOURCE_SYSTEM,
            source_column="injury_severity",
            value_expr="coalesce(injury_severity_raw, '__NULL__')",
        )
    stats["drivers_vehicle_year_typed_fail"] = con.execute(
        "SELECT COUNT(*) FROM moco_drv_raw WHERE vehicle_year IS NOT NULL "
        "AND TRY_CAST(vehicle_year AS INTEGER) IS NULL"
    ).fetchone()[0]
    stats["drivers_speed_limit_typed_fail"] = con.execute(
        "SELECT COUNT(*) FROM moco_drv_raw WHERE speed_limit IS NOT NULL "
        "AND TRY_CAST(speed_limit AS INTEGER) IS NULL"
    ).fetchone()[0]
    stats["non_motorists_duplicate_person_id"] = con.execute(
        "SELECT COUNT(DISTINCT person_id) FROM (SELECT person_id FROM moco_nm_raw "
        "GROUP BY person_id, _bronze_load_ts HAVING COUNT(*) > 1)"
    ).fetchone()[0]


def _drift_checks(ctx: c.BuildContext, stats: dict[str, Any]) -> None:
    """Run the value-set drift detector over every dictionary column.

    Generic over (dataset, column) by construction: the same call catches a new
    substance token and a re-cased injury_severity, which is the whole point --
    the 2024 generation change hit both in the same week.
    """
    checks = [
        ("moco_drv_typed", DRIVERS, "driver_substance_abuse", "substance_raw"),
        ("moco_drv_typed", DRIVERS, "injury_severity", "injury_severity_raw"),
        ("moco_nm_typed", NON_MOTORISTS, "injury_severity", "injury_severity_raw"),
        ("moco_inc_typed", INCIDENTS, "acrs_report_type", "acrs_report_type"),
        ("moco_inc_typed", INCIDENTS, "driver_substance_abuse",
         "driver_substance_abuse_raw"),
    ]
    reports = []
    for relation, dataset, column, expr in checks:
        obs = ctx.con.execute(
            f"""SELECT {c.quote_ident(expr)}, COUNT(*), MIN(crash_date_time),
                       MAX(crash_date_time), MIN(created_at), MAX(created_at)
                FROM {relation} GROUP BY 1"""
        ).fetchall()
        report = detect_drift(dataset, column, [ObservedValue(*r) for r in obs])
        reports.append(report)
        stats[f"drift_{dataset}_{column}"] = {
            "drifted": report.drifted,
            "unmapped_values": len(report.unmapped),
            "unmapped_rows": report.unmapped_rows,
            "values": [t.value for t in report.unmapped],
        }
        if report.drifted:
            message = report.render()
            if ctx.allow_unmapped:
                ctx.manifest.warn(f"drift (allowed): {message}")
            else:
                raise ValueError(
                    "value-set drift detected -- the build is refusing to write a\n"
                    "silver table containing values nobody has mapped. Re-run with\n"
                    "--allow-unmapped to write them as UNMAPPED and keep going.\n\n"
                    + message
                )
    stats["drift_reports"] = [r.render() for r in reports]


def _crash_history(ctx: c.BuildContext, stats: dict[str, Any]) -> str:
    """SCD2 the incident table on report_number."""
    sql = c.scd2_sql(
        source_relation="moco_inc_typed",
        natural_key=CRASH_KEY,
        attributes=CRASH_ATTRIBUTES,
        valid_from_expr="updated_at",
        mode=c.MODE_DELTA,
    )
    ctx.con.execute(f"CREATE OR REPLACE TABLE moco_crash_history AS {sql}")
    stats["silver_rows_crash_history"] = ctx.con.execute(
        "SELECT COUNT(*) FROM moco_crash_history"
    ).fetchone()[0]
    stats["silver_rows_crash_current"] = ctx.con.execute(
        "SELECT COUNT(*) FROM moco_crash_history WHERE is_current"
    ).fetchone()[0]
    return "moco_crash_history"


def _party_history(ctx: c.BuildContext, relation: str, table: str,
                   attributes: list[str], stats: dict[str, Any]) -> str:
    sql = c.scd2_sql(
        source_relation=relation,
        natural_key=["person_id"],
        attributes=attributes,
        valid_from_expr="updated_at",
        mode=c.MODE_DELTA,
    )
    name = f"moco_{table}_history"
    ctx.con.execute(f"CREATE OR REPLACE TABLE {name} AS {sql}")
    stats[f"silver_rows_{table}_history"] = ctx.con.execute(
        f"SELECT COUNT(*) FROM {name}"
    ).fetchone()[0]
    stats[f"silver_rows_{table}_current"] = ctx.con.execute(
        f"SELECT COUNT(*) FROM {name} WHERE is_current"
    ).fetchone()[0]
    return name


def _augment_crash_with_parties(ctx: c.BuildContext, crash: str, driver: str,
                                nm: str, stats: dict[str, Any]) -> None:
    """Attach the crash-level rollups that can only be computed from the parties.

    Everything here is aggregated from the party tables' CURRENT slices, which
    is what makes it correct: aggregating a crash-level attribute off Drivers
    directly would count a two-vehicle crash twice (1.77 driver rows per report
    number on the deduped data, 1.87 before dedupe).
    """
    con = ctx.con
    _register_incident_substance_lookup(
        ctx, "moco_inc_typed", "driver_substance_abuse_raw", "moco_inc_substance"
    )

    con.execute(
        f"""
        CREATE OR REPLACE TABLE moco_crash_enriched AS
        WITH drv AS (
            SELECT report_number,
                   COUNT(*)                                        AS driver_row_count,
                   COUNT(DISTINCT substance_raw)                   AS driver_distinct_substance,
                   MAX(severity_ordinal)                           AS max_driver_severity,
                   BOOL_OR(alcohol_status = 'SUSPECTED')           AS any_driver_alcohol_suspected,
                   BOOL_OR(drug_status = 'SUSPECTED')              AS any_driver_drug_suspected,
                   CASE WHEN COUNT(DISTINCT substance_scheme) > 1 THEN 'MIXED'
                        ELSE MAX(substance_scheme) END             AS driver_substance_scheme
            FROM {driver} WHERE is_current GROUP BY 1
        ),
        nmo AS (
            SELECT report_number,
                   COUNT(*)                AS non_motorist_row_count,
                   MAX(severity_ordinal)   AS max_nm_severity
            FROM {nm} WHERE is_current GROUP BY 1
        )
        SELECT
            h.*,
            CAST(coalesce(drv.driver_row_count, 0) AS INTEGER)        AS driver_row_count,
            CAST(coalesce(nmo.non_motorist_row_count, 0) AS INTEGER)  AS non_motorist_row_count,
            drv.report_number IS NOT NULL                             AS has_driver_rows,
            nmo.report_number IS NOT NULL                             AS has_non_motorist_rows,
            CAST(greatest(coalesce(drv.max_driver_severity, 0),
                          coalesce(nmo.max_nm_severity, 0)) AS INTEGER)
                                                                      AS max_party_severity_ordinal,
            -- The crash ordinal is MAX over parties where parties exist, and
            -- falls back to the crash-level acrs_report_type only for the 785
            -- crashes that have none. severity_grain records which, so nobody
            -- downstream mistakes a fallback for a person-level observation.
            CAST(CASE WHEN drv.report_number IS NOT NULL OR nmo.report_number IS NOT NULL
                      THEN greatest(coalesce(drv.max_driver_severity, 0),
                                    coalesce(nmo.max_nm_severity, 0))
                      ELSE coalesce(x.severity_ordinal, 0) END AS INTEGER)
                                                                      AS severity_ordinal,
            CASE WHEN drv.report_number IS NOT NULL OR nmo.report_number IS NOT NULL
                 THEN NULL ELSE h.acrs_report_type END                AS severity_source_value,
            CASE WHEN drv.report_number IS NOT NULL OR nmo.report_number IS NOT NULL
                 THEN 'MAX_OVER_PARTIES' ELSE 'CRASH_REPORT_TYPE' END AS severity_grain,
            coalesce(drv.any_driver_alcohol_suspected, false)         AS any_driver_alcohol_suspected,
            coalesce(drv.any_driver_drug_suspected, false)            AS any_driver_drug_suspected,
            coalesce(drv.driver_substance_scheme, 'NULL')             AS driver_substance_scheme,
            coalesce(inc.scheme, 'NULL')                              AS incident_string_scheme,
            CAST(coalesce(inc.party_count, 0) AS INTEGER)             AS incident_string_party_count,
            -- The two dictionary generations JOIN DIFFERENTLY on this column,
            -- which nothing in the assignment or the portal's documentation
            -- says. Measured over the current slice:
            --   OLD_SINGLE crashes: the string is the DISTINCT set of driver
            --     values -- two drivers both "NONE DETECTED" publish ONE token.
            --     96,862 / 96,862 match distinct-count. 42,316 also happen to
            --     match row-count (the single-driver crashes).
            --   NEW_PAIR crashes: the string is the full LIST -- two drivers
            --     both "Not Suspect..., Not Suspect..." publish BOTH pairs.
            --     27,358 / 27,358 match row-count.
            -- So the crash-level string cannot be used to count parties under
            -- either generation without knowing which generation it is in, and
            -- the driver-derived rollup is the only safe source. These two
            -- booleans put the property on the table so a test can assert it.
            drv.report_number IS NOT NULL
              AND coalesce(inc.party_count, 0) = drv.driver_row_count
                                                                      AS incident_string_matches_driver_rows,
            drv.report_number IS NOT NULL
              AND coalesce(inc.party_count, 0) = drv.driver_distinct_substance
                                                                      AS incident_string_matches_driver_distinct,
            -- The cross-check. TRUE when the Incidents concatenation and the
            -- Drivers rollup agree on both suspicion flags. NULL where the crash
            -- has no driver rows to compare against.
            CASE WHEN drv.report_number IS NULL THEN NULL
                 ELSE coalesce(inc.any_alcohol_suspected, false)
                        = coalesce(drv.any_driver_alcohol_suspected, false)
                  AND coalesce(inc.any_drug_suspected, false)
                        = coalesce(drv.any_driver_drug_suspected, false)
            END                                                       AS substance_crosscheck_agrees
        FROM {crash} h
        LEFT JOIN drv ON drv.report_number = h.report_number
        LEFT JOIN nmo ON nmo.report_number = h.report_number
        LEFT JOIN moco_inc_substance inc
               ON inc.raw IS NOT DISTINCT FROM h.driver_substance_abuse_raw
        LEFT JOIN severity_crosswalk x
               ON x.source_system = '{SOURCE_SYSTEM}'
              AND x.source_column = 'acrs_report_type'
              AND x.source_value = h.acrs_report_type
        """
    )
    row = con.execute(
        """SELECT
             SUM(CASE WHEN NOT has_driver_rows THEN 1 ELSE 0 END),
             SUM(CASE WHEN NOT has_driver_rows AND has_non_motorist_rows THEN 1 ELSE 0 END),
             SUM(CASE WHEN NOT has_driver_rows AND NOT has_non_motorist_rows THEN 1 ELSE 0 END),
             SUM(CASE WHEN substance_crosscheck_agrees IS FALSE THEN 1 ELSE 0 END),
             SUM(CASE WHEN substance_crosscheck_agrees IS NOT NULL THEN 1 ELSE 0 END),
             SUM(CASE WHEN incident_string_party_count <> driver_row_count
                       AND has_driver_rows THEN 1 ELSE 0 END),
             SUM(CASE WHEN incident_string_scheme = 'MIXED' THEN 1 ELSE 0 END),
             SUM(CASE WHEN geo_quality = 'OUT_OF_ENVELOPE' THEN 1 ELSE 0 END)
           FROM moco_crash_enriched WHERE is_current"""
    ).fetchone()
    (stats["crash_without_driver_rows"], stats["crash_without_driver_but_non_motorist"],
     stats["crash_without_any_party"], stats["substance_crosscheck_disagreements"],
     stats["substance_crosscheck_comparable"], stats["incident_string_party_count_mismatch"],
     stats["incident_string_mixed_scheme"], stats["crash_out_of_envelope"]) = row

    # The acrs_report_type cross-check the crosswalk notes promise.
    stats["incident_string_join_semantics"] = {
        scheme: {"crashes": n, "matches_driver_rows": mr, "matches_driver_distinct": md}
        for scheme, n, mr, md in con.execute(
            """SELECT driver_substance_scheme, COUNT(*),
                      SUM(CASE WHEN incident_string_matches_driver_rows THEN 1 ELSE 0 END),
                      SUM(CASE WHEN incident_string_matches_driver_distinct THEN 1 ELSE 0 END)
               FROM moco_crash_enriched WHERE is_current AND has_driver_rows
               GROUP BY 1 ORDER BY 2 DESC"""
        ).fetchall()
    }

    stats["acrs_vs_party_severity_disagreements"] = con.execute(
        """SELECT COUNT(*) FROM moco_crash_enriched
           WHERE is_current AND severity_grain = 'MAX_OVER_PARTIES'
             AND ((acrs_report_type = 'Fatal Crash' AND severity_ordinal <> 5)
               OR (acrs_report_type = 'Property Damage Crash' AND severity_ordinal > 1)
               OR (acrs_report_type = 'Injury Crash' AND severity_ordinal NOT BETWEEN 2 AND 4))"""
    ).fetchone()[0]

    dists = con.execute(
        """SELECT distance_from_envelope_m FROM moco_crash_enriched
           WHERE is_current AND geo_quality = 'OUT_OF_ENVELOPE'
           ORDER BY distance_from_envelope_m"""
    ).fetchall()
    if dists:
        vals = [d[0] for d in dists]
        stats["crash_out_of_envelope_max_km"] = round(vals[-1] / 1000, 2)
        stats["crash_out_of_envelope_median_km"] = round(vals[len(vals) // 2] / 1000, 2)
    con.execute(
        "CREATE OR REPLACE TABLE moco_crash_final AS SELECT * FROM moco_crash_enriched"
    )

    # The assignment's own bbox, unpadded, so DATA_QUALITY.md can quote a number
    # a reviewer will reproduce from the text of Part 1.
    env = envelope(SOURCE)
    stats["crash_out_of_assignment_bbox"] = con.execute(
        f"""SELECT COUNT(*) FROM moco_crash_final WHERE is_current AND NOT (
              lat_raw BETWEEN {env['assignment_min_lat']} AND {env['assignment_max_lat']}
              AND lon_raw BETWEEN {env['assignment_min_lon']} AND {env['assignment_max_lon']})"""
    ).fetchone()[0]


def _reconcile(ctx: c.BuildContext, stats: dict[str, Any]) -> None:
    """Anti-join both directions on the CURRENT slices, and the fan-out proof."""
    con = ctx.con
    stats["antijoin_incidents_without_drivers"] = con.execute(
        """SELECT COUNT(*) FROM (SELECT DISTINCT report_number FROM moco_crash_final
             WHERE is_current) i
           WHERE NOT EXISTS (SELECT 1 FROM moco_driver_history d
             WHERE d.is_current AND d.report_number = i.report_number)"""
    ).fetchone()[0]
    stats["antijoin_drivers_without_incidents"] = con.execute(
        """SELECT COUNT(*) FROM (SELECT DISTINCT report_number FROM moco_driver_history
             WHERE is_current) d
           WHERE NOT EXISTS (SELECT 1 FROM moco_crash_final i
             WHERE i.is_current AND i.report_number = d.report_number)"""
    ).fetchone()[0]
    stats["driver_rows_per_report_number"] = round(
        con.execute(
            """SELECT COUNT(*)::DOUBLE / COUNT(DISTINCT report_number)
               FROM moco_driver_history WHERE is_current"""
        ).fetchone()[0],
        4,
    )
    stats["bronze_driver_rows_per_report_number"] = round(
        con.execute(
            "SELECT COUNT(*)::DOUBLE / COUNT(DISTINCT report_number) FROM moco_drv_raw"
        ).fetchone()[0],
        4,
    )

    # The dictionary-overlap window, by crash date. `:created_at` is NOT usable
    # for this: the 2024-06-12 bulk reload stamped 172,096 old-scheme and 3,637
    # new-scheme rows with one creation date, so it reports a seven-month
    # "overlap" that is an artefact of our read. The test uses crash date.
    overlap = con.execute(
        """SELECT MIN(d), MAX(d), COUNT(*) FROM (
             SELECT crash_date AS d,
                    SUM(CASE WHEN substance_scheme = 'NEW_PAIR' THEN 1 ELSE 0 END) n,
                    SUM(CASE WHEN substance_scheme = 'OLD_SINGLE' THEN 1 ELSE 0 END) o
             FROM moco_driver_history WHERE is_current GROUP BY 1
           ) WHERE n > 0 AND o > 0"""
    ).fetchone()
    stats["substance_overlap_first_crash_date"] = str(overlap[0])
    stats["substance_overlap_last_crash_date"] = str(overlap[1])
    stats["substance_overlap_days"] = overlap[2]
    stats["substance_scheme_counts"] = dict(
        con.execute(
            """SELECT substance_scheme, COUNT(*) FROM moco_driver_history
               WHERE is_current GROUP BY 1 ORDER BY 1"""
        ).fetchall()
    )
    stats["substance_first_new_scheme_crash_date"] = str(
        con.execute(
            """SELECT MIN(crash_date) FROM moco_driver_history
               WHERE is_current AND substance_scheme = 'NEW_PAIR'"""
        ).fetchone()[0]
    )
    stats["substance_last_old_scheme_crash_date"] = str(
        con.execute(
            """SELECT MAX(crash_date) FROM moco_driver_history
               WHERE is_current AND substance_scheme = 'OLD_SINGLE'"""
        ).fetchone()[0]
    )
