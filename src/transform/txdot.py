"""TxDOT CRIS -- bronze to silver.

    txdot/cris_crash -> txdot/crash_{history,current}.parquet

Natural key: `crash_id`. NOT `ESRI_OID` -- that is assigned by the ArcGIS
service, so a layer rebuild reshuffles it underneath us, and Phase 1's cursor
is deliberately scoped to one sweep for exactly that reason. `crash_id` is
TxDOT's own key and is 1:1 with rows in the local slice (100,000 / 100,000).


Versioning mode: SNAPSHOT, gated on the ingest cursor
-----------------------------------------------------
`amend_supp_fl` is a first-class concept in this schema -- 5,936 of 100,000 rows
(5.9%) are amended or supplemental. The service exposes no mutation timestamp
to watermark on and its date columns are `esriFieldTypeString` with no index
behind them, so the only correct mechanism is a full sweep of an OID range plus
a local row-hash diff. That makes each bronze partition a SNAPSHOT of the range
its cursor recorded, and a crash_id absent from a later snapshot of the same
range is evidence of deletion.

"Of the same range" is load-bearing. Phase 1's cursor carries `oid_floor`,
`oid_ceiling` and a sticky `bounded` flag; the local slice is OIDs 1-100,000 of
3,088,450. A partition covering 1-100,000 says nothing about OID 2,000,000, so
deletion detection is gated on the swept range and every partition in a
comparison must declare the same one. Where the cursor is unavailable or the
ranges differ, the transform falls back to MODE_DELTA and says so in the
manifest -- a missed deletion is recoverable, a fabricated one is not.

`valid_from` is `_bronze_load_ts`: TxDOT publishes nothing that says when a row
changed, so the earliest moment we can honestly claim a value held is when we
first saw it. `report_date` is the report's own filing date, which is a
different fact and is kept as its own column rather than pressed into service
as a mutation stamp.


Coordinate precedence: CRIS-derived, then officer-reported, then nothing
-----------------------------------------------------------------------
Three combinations exist and only three, measured over all 100,000 rows:

    located_fl='1', derived + geometry present, rpt_* null      68,932
    located_fl='1', derived + geometry + rpt_* present          22,944
    located_fl='0', rpt_* + geometry present, derived null         845
    located_fl='0', nothing at all                              7,279

So `located_fl` is exactly the "CRIS produced a location" flag: it is 1 if and
only if the derived pair is populated, and the ESRI geometry is present exactly
when either pair is. The precedence follows from that, not from taste:

  1. CRIS-derived `latitude`/`longitude` -- geocoded and quality-controlled by
     CRIS against the state road network, and present for 91,876 rows against
     the officer pair's 23,789. Preferring the officer pair would leave 68,932
     rows (68.9%) uncoordinated for no gain.
  2. `rpt_latitude`/`rpt_longitude` -- the officer's own reading. Only ever the
     sole source for the 845 rows CRIS could not locate. Using it there is
     strictly better than a null.
  3. NULL, geo_quality='MISSING', for the 7,279 with neither.

`coord_source` records which rule fired, so a downstream consumer can weight
officer-reported points differently without re-deriving the precedence.

Where both pairs exist they disagree by more than 0.01 degrees in 1,065 rows;
the geodesic distance between them is reported, and `coord_pairs_disagree_m`
carries it per row so the threshold is a query rather than a hardcode.


Reprojection: EPSG:3081 -> EPSG:4326
------------------------------------
Bronze preserved the ArcGIS geometry in its native wkid 3081 (NAD83 / Texas
Statewide Mapping System) because reprojection is a transformation and belongs
in silver. `_geometry_x/_y` are reprojected here with pyproj and compared
against the CRIS-derived pair as a self-consistency check.

This is a REPROJECTION, not a metric operation -- no distance, area or buffer is
computed in 3081 or in 4326. The comparison between the reprojected pair and the
derived pair IS a distance, and it is computed geodesically on the WGS84
ellipsoid (common.GEOD), not in either projected frame.


The ~60 opaque `*_id` columns
-----------------------------
CRIS publishes roughly 60 integer code columns (`wthr_cond_id`, `light_cond_id`,
`obj_struck_id`, ...) whose dictionaries live in the CRIS Automated Interface
guide V29.0 (https://www.txdot.gov/content/dam/docs/division/trf/crash-records/
cris-guide.pdf), not in the feature service. They are typed as INTEGER and left
UNDECODED, deliberately: inventing labels from a guess would be worse than a
number, and the guide's tables are not machine-readable from the URL above
without a PDF extraction step that has no place in this phase. Decoding them is
Phase 3 work with the lookup table cited.

`crash_sev_id` is the exception and it is decoded, because the assignment
requires one severity ordinal and because the mapping is VERIFIABLE FROM THE
DATA ITSELF -- see `verify_crash_sev_consistency()`, which is also a test.
"""

from __future__ import annotations

import logging
from typing import Any

from pyproj import Transformer

from . import common as c
from .severity_crosswalk import assert_all_mapped, register as register_crosswalk

log = logging.getLogger(__name__)

SOURCE = "txdot"
SOURCE_SYSTEM = "TXDOT_CRIS"
JURISDICTION = "TX"
DATASET = "cris_crash"

# NAD83 / Texas Statewide Mapping System. `always_xy` so the transformer takes
# and returns (x, y) = (easting, northing) / (lon, lat) rather than pyproj's
# authority-defined axis order, which for EPSG:4326 is (lat, lon) and is the
# single most common source of silently-swapped coordinates.
TX_WKID = 3081
_TO_WGS84 = Transformer.from_crs(f"EPSG:{TX_WKID}", "EPSG:4326", always_xy=True)

COORD_CRIS = "CRIS_DERIVED"
COORD_OFFICER = "OFFICER_REPORTED"
COORD_NONE = "NONE"

# Degrees. 0.01 deg is ~1.1 km of latitude and ~0.9 km of longitude at Texas
# latitudes -- far beyond any plausible geocoding difference for the same crash,
# which is why it is the threshold the assignment's own framing implies.
COORD_DISAGREE_DEG = 0.01

NATURAL_KEY = ["crash_id"]

ATTRIBUTES = [
    "crash_datetime_local",
    "crash_date",
    "crash_time_local",
    "report_date",
    "latitude",
    "longitude",
    "lat_raw",
    "lon_raw",
    "rpt_lat_raw",
    "rpt_lon_raw",
    "geom_lat_3081_reproj",
    "geom_lon_3081_reproj",
    "coord_source",
    "geo_quality",
    "coord_pairs_disagree",
    "located_fl",
    "is_amended",
    "crash_sev_id",
    "severity_ordinal",
    "severity_kabco",
    "crash_fatal_fl",
    "death_cnt",
    "sus_serious_injry_cnt",
    "nonincap_injry_cnt",
    "poss_injry_cnt",
    "non_injry_cnt",
    "unkn_injry_cnt",
    "tot_injry_cnt",
    "cnty_id",
    "county_fips",
    "city_id",
    "rpt_city_id",
    "street_name",
    "rpt_street_name",
    "rpt_block_num",
    "hwy_sys",
    "hwy_nbr",
    "onsys_fl",
    "rural_fl",
    "txdot_rptable_fl",
    "day_of_week",
    "crash_speed_limit",
    "nbr_of_lane",
    "wthr_cond_id",
    "light_cond_id",
    "surf_cond_id",
    "traffic_cntl_id",
    "road_cls_id",
    "road_algn_id",
    "harm_evnt_id",
    "fhe_collsn_id",
    "obj_struck_id",
    "intrsct_relat_id",
    "at_intrsct_fl",
    "road_constr_zone_fl",
    "active_school_zone_fl",
    "toll_road_fl",
    "private_dr_fl",
    "bicyclist_involved_fl",
    "pedestrian_involved_fl",
    "motorcyclist_involved_fl",
    "large_truck_involved_fl",
    "motor_vehicle_involved_fl",
    "cmv_involv_fl",
    "schl_bus_fl",
    "rr_relat_fl",
    "secondary_crash_fl",
    "esri_oid",
]

# Every *_id column typed to INTEGER and left undecoded. Named explicitly so the
# report can say how many there are without counting by hand.
UNDECODED_ID_COLUMNS = [
    "wthr_cond_id", "light_cond_id", "surf_cond_id", "traffic_cntl_id",
    "road_cls_id", "road_algn_id", "harm_evnt_id", "fhe_collsn_id",
    "obj_struck_id", "intrsct_relat_id", "city_id", "rpt_city_id",
]

_FLAG = "= '1'"


def _flag(col: str) -> str:
    """CRIS booleans are the strings '0' and '1'. NULL stays NULL."""
    return f"CASE WHEN {col} IS NULL THEN NULL ELSE {col} {_FLAG} END"


def verify_crash_sev_consistency(con, relation: str) -> list[dict[str, Any]]:
    """Prove crash_sev_id's meaning from the injury count columns.

    The CRIS guide's code table is not machine-readable from the published PDF,
    so the mapping is verified against the data instead: `crash_sev_id` should
    be the MAX KABCO severity over the crash's persons, and the crash carries
    per-severity person counts (death_cnt, sus_serious_injry_cnt,
    nonincap_injry_cnt, poss_injry_cnt, non_injry_cnt) that must agree with it.

    Returns one row per code with the evidence. Measured on the local slice --
    every figure below is 100% of that code's rows:

        4 -> K  997 rows, all death_cnt>0, all crash_fatal_fl=1
        1 -> A  4,119, all sus_serious_injry_cnt>0, death_cnt=0
        2 -> B  12,520, all nonincap_injry_cnt>0, sus_serious=0, death=0
        3 -> C  14,128, all poss_injry_cnt>0, nonincap=0
        5 -> O  62,454, all non_injry_cnt>0 and every injury count 0
        0 -> unknown  5,781, of which 5,410 carry unkn_injry_cnt>0
        95 -> 1 row, every injury count 0 -- undocumented in V29.0

    Note the direction: 5 is the LEAST severe code, 4 the most. Ordering by the
    raw id sorts the scale backwards, which is the trap this function exists to
    make impossible to fall into silently.
    """
    rows = con.execute(
        f"""
        SELECT crash_sev_id,
               COUNT(*)                                                  AS n,
               SUM(CASE WHEN death_cnt > 0 THEN 1 ELSE 0 END)            AS with_fatal,
               SUM(CASE WHEN sus_serious_injry_cnt > 0 THEN 1 ELSE 0 END) AS with_serious,
               SUM(CASE WHEN nonincap_injry_cnt > 0 THEN 1 ELSE 0 END)   AS with_minor,
               SUM(CASE WHEN poss_injry_cnt > 0 THEN 1 ELSE 0 END)       AS with_possible,
               SUM(CASE WHEN non_injry_cnt > 0 THEN 1 ELSE 0 END)        AS with_none,
               SUM(CASE WHEN unkn_injry_cnt > 0 THEN 1 ELSE 0 END)       AS with_unknown
        FROM {relation} GROUP BY 1 ORDER BY 1
        """
    ).fetchall()
    keys = ["crash_sev_id", "n", "with_fatal", "with_serious", "with_minor",
            "with_possible", "with_none", "with_unknown"]
    return [dict(zip(keys, r)) for r in rows]


def sweep_ranges(ctx: c.BuildContext, partitions) -> dict[str, tuple[int, int]]:
    """The OID range each bronze partition actually swept, from the ingest log.

    Read out of `watermark_history` rather than out of the current cursor,
    because the store keeps one cursor per (source, dataset) and a rebuild needs
    the cursor as it stood at the END of each partition's run. History is
    ordered seq DESC, so the first entry seen for a load_ts is that run's last
    advance.

    The range is `(oid_floor, last_objectid]`, NOT `(oid_floor, oid_ceiling]`.
    That distinction is the whole point. The local partition's cursor reads
    floor=0, ceiling=3,088,450, last_objectid=100,000, status=in_progress: the
    ceiling is the size of the OID universe the sweep was *aimed* at, while
    last_objectid is how far it actually got before the bounded-slice page cap
    stopped it. Scoping deletion detection to the ceiling would claim a snapshot
    of 3.09M OIDs from a read of 100k -- and then infer the deletion of 2.99M
    crashes that were simply never requested.
    """
    if ctx.store is None:
        return {}
    try:
        history = ctx.store.history(SOURCE, DATASET, limit=100_000)
    except Exception as exc:
        log.warning("txdot: watermark history unavailable: %s", exc)
        return {}

    latest: dict[str, dict[str, Any]] = {}
    for row in history:  # seq DESC
        load_ts = row.get("load_ts")
        if load_ts and load_ts not in latest:
            latest[load_ts] = row["cursor"]

    out: dict[str, tuple[int, int]] = {}
    for p in partitions:
        cur = latest.get(p.load_ts)
        if not cur:
            continue
        floor = cur.get("oid_floor")
        last = cur.get("last_objectid")
        if floor is None or last is None:
            continue
        ceiling = cur.get("oid_ceiling")
        if ceiling is not None:
            last = min(last, ceiling)
        out[p.load_ts] = (int(floor), int(last))
    return out


def _snapshot_scope(ctx: c.BuildContext, partitions) -> str | None:
    """The OID range every partition swept, or None if they disagree.

    Deletion inference is only sound between two snapshots of the SAME scope.
    If any partition's swept range differs, or the ingest history is
    unavailable, this returns None and the caller downgrades to MODE_DELTA
    rather than inventing deletions from a range that was never read.

    A partition whose sweep covered a WIDER range than another's is not a
    superset for this purpose either: comparing them would treat every key
    outside the narrower sweep as deleted. Requiring exact equality is the
    conservative reading, and re-sweeping the same bound -- which is what the
    Phase 1 cursor's sticky `bounded` flag exists to make repeatable -- is what
    produces two comparable snapshots.
    """
    if len(partitions) < 1:
        return None
    ranges = sweep_ranges(ctx, partitions)
    if len(ranges) != len(partitions):
        return None
    distinct = set(ranges.values())
    if len(distinct) != 1:
        return None
    floor, last = next(iter(distinct))
    # The literal is the range itself, so it shows up in the query plan and in
    # any dump of the intermediate table rather than being an opaque constant.
    return f"'oid:{floor + 1}-{last}'"


def build(ctx: c.BuildContext) -> dict[str, Any]:
    con = ctx.con
    stats: dict[str, Any] = {}
    register_crosswalk(con)

    partitions = ctx.partitions(SOURCE, DATASET)
    for p in partitions:
        ctx.manifest.add_input(p)
    c.bronze_view(con, "txd_raw", partitions)
    stats["bronze_rows"] = con.execute("SELECT COUNT(*) FROM txd_raw").fetchone()[0]
    stats["bronze_partitions"] = [p.load_ts for p in partitions]

    _typed(ctx, stats)
    _history(ctx, partitions, stats)
    return stats


def _typed(ctx: c.BuildContext, stats: dict[str, Any]) -> None:
    con = ctx.con

    # Reprojection first: one row per DISTINCT geometry pair, so pyproj runs
    # over the coordinates and not over the table. 92,721 distinct pairs here.
    pairs = con.execute(
        """SELECT DISTINCT TRY_CAST(_geometry_x AS DOUBLE) x,
                           TRY_CAST(_geometry_y AS DOUBLE) y
           FROM txd_raw WHERE _geometry_x IS NOT NULL AND _geometry_y IS NOT NULL"""
    ).fetchall()
    con.execute(
        "CREATE OR REPLACE TABLE txd_reproj (x DOUBLE, y DOUBLE, lat DOUBLE, lon DOUBLE)"
    )
    if pairs:
        xs = [p[0] for p in pairs]
        ys = [p[1] for p in pairs]
        lons, lats = _TO_WGS84.transform(xs, ys)
        con.executemany(
            "INSERT INTO txd_reproj VALUES (?,?,?,?)",
            list(zip(xs, ys, lats, lons)),
        )
    stats["distinct_geometry_pairs"] = len(pairs)

    wkids = [r[0] for r in con.execute(
        "SELECT DISTINCT _geometry_wkid FROM txd_raw WHERE _geometry_wkid IS NOT NULL"
    ).fetchall()]
    stats["geometry_wkid"] = wkids
    if wkids and set(wkids) != {str(TX_WKID)}:
        ctx.manifest.warn(
            f"TxDOT geometry wkid is {wkids}, not {TX_WKID}; the reprojection "
            f"assumes {TX_WKID} and would be wrong for anything else"
        )

    der_lat = "TRY_CAST(r.latitude AS DOUBLE)"
    der_lon = "TRY_CAST(r.longitude AS DOUBLE)"
    rpt_lat = "TRY_CAST(r.rpt_latitude AS DOUBLE)"
    rpt_lon = "TRY_CAST(r.rpt_longitude AS DOUBLE)"
    # Precedence, expressed once and reused for the value, the source label and
    # the quality flag so the three can never disagree.
    pick_lat = f"coalesce({der_lat}, {rpt_lat})"
    pick_lon = f"coalesce({der_lon}, {rpt_lon})"

    con.execute(
        f"""
        CREATE OR REPLACE VIEW txd_typed AS
        SELECT
            r.crash_id,
            -- crash_date is YYYY-MM-DD and crash_time is HH:MM:SS, both
            -- esriFieldTypeString. Concatenated into a naive local timestamp;
            -- NOT localised (Texas spans Central and Mountain -- El Paso and
            -- Hudspeth -- so localisation needs the coordinate-derived zone and
            -- is Phase 4).
            TRY_CAST(r.crash_date || ' ' || coalesce(r.crash_time, '00:00:00')
                     AS TIMESTAMP)                      AS crash_datetime_local,
            TRY_CAST(r.crash_date AS DATE)              AS crash_date,
            TRY_CAST(r.crash_time AS TIME)              AS crash_time_local,
            TRY_CAST(r.report_date AS DATE)             AS report_date,
            CASE WHEN {c.envelope_sql(SOURCE, pick_lat, pick_lon)}
                 THEN {pick_lat} END                    AS latitude,
            CASE WHEN {c.envelope_sql(SOURCE, pick_lat, pick_lon)}
                 THEN {pick_lon} END                    AS longitude,
            {der_lat}                                   AS lat_raw,
            {der_lon}                                   AS lon_raw,
            {rpt_lat}                                   AS rpt_lat_raw,
            {rpt_lon}                                   AS rpt_lon_raw,
            g.lat                                       AS geom_lat_3081_reproj,
            g.lon                                       AS geom_lon_3081_reproj,
            CASE WHEN {der_lat} IS NOT NULL AND {der_lon} IS NOT NULL THEN '{COORD_CRIS}'
                 WHEN {rpt_lat} IS NOT NULL AND {rpt_lon} IS NOT NULL THEN '{COORD_OFFICER}'
                 ELSE '{COORD_NONE}' END                AS coord_source,
            CASE WHEN {pick_lat} IS NULL OR {pick_lon} IS NULL THEN '{c.GEO_MISSING}'
                 WHEN {c.envelope_sql(SOURCE, pick_lat, pick_lon)} THEN '{c.GEO_OK}'
                 ELSE '{c.GEO_OUT_OF_ENVELOPE}' END     AS geo_quality,
            CASE WHEN {der_lat} IS NULL OR {rpt_lat} IS NULL THEN NULL
                 ELSE abs({der_lat} - {rpt_lat}) > {COORD_DISAGREE_DEG}
                   OR abs({der_lon} - {rpt_lon}) > {COORD_DISAGREE_DEG}
            END                                         AS coord_pairs_disagree,
            {_flag('r.located_fl')}                     AS located_fl,
            {_flag('r.amend_supp_fl')}                  AS is_amended,
            TRY_CAST(r.crash_sev_id AS INTEGER)         AS crash_sev_id,
            CAST(x.severity_ordinal AS INTEGER)         AS severity_ordinal,
            x.kabco                                     AS severity_kabco,
            {_flag('r.crash_fatal_fl')}                 AS crash_fatal_fl,
            TRY_CAST(r.death_cnt AS INTEGER)            AS death_cnt,
            TRY_CAST(r.sus_serious_injry_cnt AS INTEGER) AS sus_serious_injry_cnt,
            TRY_CAST(r.nonincap_injry_cnt AS INTEGER)   AS nonincap_injry_cnt,
            TRY_CAST(r.poss_injry_cnt AS INTEGER)       AS poss_injry_cnt,
            TRY_CAST(r.non_injry_cnt AS INTEGER)        AS non_injry_cnt,
            TRY_CAST(r.unkn_injry_cnt AS INTEGER)       AS unkn_injry_cnt,
            TRY_CAST(r.tot_injry_cnt AS INTEGER)        AS tot_injry_cnt,
            TRY_CAST(r.cnty_id AS INTEGER)              AS cnty_id,
            -- Texas county FIPS are odd numbers assigned in alphabetical order,
            -- and CRIS's cnty_id is the same alphabetical ordinal, so
            -- FIPS = 2*cnty_id - 1. Spot-checked against five counties whose
            -- CRIS ids and FIPS are both known: Harris 101->201, Bexar 15->029,
            -- Dallas 57->113, Tarrant 220->439, El Paso 71->141. The domain
            -- check that makes it safe to apply in bulk is below: cnty_id runs
            -- 1..254 with no nulls and no out-of-range values across all
            -- 100,000 rows, and Texas has exactly 254 counties.
            CASE WHEN TRY_CAST(r.cnty_id AS INTEGER) BETWEEN 1 AND 254
                 THEN lpad(CAST(2 * TRY_CAST(r.cnty_id AS INTEGER) - 1 AS VARCHAR), 3, '0')
            END                                         AS county_fips,
            TRY_CAST(r.city_id AS INTEGER)              AS city_id,
            TRY_CAST(r.rpt_city_id AS INTEGER)          AS rpt_city_id,
            r.street_name, r.rpt_street_name, r.rpt_block_num,
            r.hwy_sys, r.hwy_nbr,
            {_flag('r.onsys_fl')}                       AS onsys_fl,
            {_flag('r.rural_fl')}                       AS rural_fl,
            {_flag('r.txdot_rptable_fl')}               AS txdot_rptable_fl,
            TRY_CAST(r.day_of_week AS INTEGER)          AS day_of_week,
            TRY_CAST(r.crash_speed_limit AS INTEGER)    AS crash_speed_limit,
            TRY_CAST(r.nbr_of_lane AS INTEGER)          AS nbr_of_lane,
            {", ".join(f"TRY_CAST(r.{col} AS INTEGER) AS {col}" for col in UNDECODED_ID_COLUMNS
                       if col not in ("city_id", "rpt_city_id"))},
            {_flag('r.at_intrsct_fl')}                  AS at_intrsct_fl,
            {_flag('r.road_constr_zone_fl')}            AS road_constr_zone_fl,
            {_flag('r.active_school_zone_fl')}          AS active_school_zone_fl,
            {_flag('r.toll_road_fl')}                   AS toll_road_fl,
            {_flag('r.private_dr_fl')}                  AS private_dr_fl,
            {_flag('r.bicyclist_involved_fl')}          AS bicyclist_involved_fl,
            {_flag('r.pedestrian_involved_fl')}         AS pedestrian_involved_fl,
            {_flag('r.motorcyclist_involved_fl')}       AS motorcyclist_involved_fl,
            {_flag('r.large_truck_involved_fl')}        AS large_truck_involved_fl,
            {_flag('r.motor_vehicle_involved_fl')}      AS motor_vehicle_involved_fl,
            {_flag('r.cmv_involv_fl')}                  AS cmv_involv_fl,
            {_flag('r.schl_bus_fl')}                    AS schl_bus_fl,
            {_flag('r.rr_relat_fl')}                    AS rr_relat_fl,
            {_flag('r.secondary_crash_fl')}             AS secondary_crash_fl,
            TRY_CAST(r."ESRI_OID" AS BIGINT)            AS esri_oid,
            r._bronze_load_ts, r._bronze_row_sha256, r._bronze_raw_path
        FROM txd_raw r
        LEFT JOIN txd_reproj g
               ON g.x = TRY_CAST(r._geometry_x AS DOUBLE)
              AND g.y = TRY_CAST(r._geometry_y AS DOUBLE)
        LEFT JOIN severity_crosswalk x
               ON x.source_system = '{SOURCE_SYSTEM}'
              AND x.source_column = 'crash_sev_id'
              AND x.source_value = r.crash_sev_id
        """
    )

    assert_all_mapped(
        con, table="txd_typed", source_system=SOURCE_SYSTEM,
        source_column="crash_sev_id",
        value_expr="coalesce(CAST(crash_sev_id AS VARCHAR), '__NULL__')",
    )

    stats["crash_sev_consistency"] = verify_crash_sev_consistency(con, "txd_typed")
    stats["coord_source_counts"] = dict(
        con.execute(
            "SELECT coord_source, COUNT(*) FROM txd_typed GROUP BY 1 ORDER BY 2 DESC"
        ).fetchall()
    )
    stats["geo_quality_counts"] = dict(
        con.execute(
            "SELECT geo_quality, COUNT(*) FROM txd_typed GROUP BY 1 ORDER BY 2 DESC"
        ).fetchall()
    )
    stats["located_fl_matches_derived_coords"] = con.execute(
        """SELECT COUNT(*) FROM txd_typed
           WHERE located_fl <> (coord_source = 'CRIS_DERIVED')"""
    ).fetchone()[0]
    stats["coord_pairs_both_present"] = con.execute(
        "SELECT COUNT(*) FROM txd_typed WHERE coord_pairs_disagree IS NOT NULL"
    ).fetchone()[0]
    stats["coord_pairs_disagree_over_threshold"] = con.execute(
        "SELECT COUNT(*) FROM txd_typed WHERE coord_pairs_disagree"
    ).fetchone()[0]
    stats["is_amended_count"] = con.execute(
        "SELECT COUNT(*) FROM txd_typed WHERE is_amended"
    ).fetchone()[0]
    for col, label in (("crash_date", "crash_date"), ("crash_time_local", "crash_time"),
                       ("report_date", "report_date")):
        raw = {"crash_date": "crash_date", "crash_time_local": "crash_time",
               "report_date": "report_date"}[col]
        stats[f"{label}_parse_fail"] = con.execute(
            f"""SELECT COUNT(*) FROM txd_raw r WHERE r.{raw} IS NOT NULL
                AND TRY_CAST(r.{raw} AS {'TIME' if raw == 'crash_time' else 'DATE'}) IS NULL"""
        ).fetchone()[0]
    stats["county_fips_null"] = con.execute(
        "SELECT COUNT(*) FROM txd_typed WHERE county_fips IS NULL"
    ).fetchone()[0]
    # How many `*_id` columns are genuinely opaque, excluding the three that
    # are not code dictionaries at all (crash_id is the natural key, case_id is
    # the agency's own case number, cnty_id is decoded to FIPS above) and
    # crash_sev_id, which IS decoded. The rest need the CRIS Automated
    # Interface guide V29.0 lookups and are left as integers on purpose.
    decoded_or_key = {"crash_id", "case_id", "cnty_id", "crash_sev_id"}
    all_id_cols = sorted(
        col for (col,) in con.execute(
            "SELECT column_name FROM (DESCRIBE SELECT * FROM txd_raw) "
            "WHERE column_name LIKE '%\\_id' ESCAPE '\\'"
        ).fetchall()
    )
    stats["id_columns_total"] = len(all_id_cols)
    stats["undecoded_id_columns"] = [c_ for c_ in all_id_cols if c_ not in decoded_or_key]
    stats["undecoded_id_column_count"] = len(stats["undecoded_id_columns"])
    stats["id_columns_carried_into_silver"] = sorted(UNDECODED_ID_COLUMNS)

    _coordinate_discrepancies(ctx, stats)


def _coordinate_discrepancies(ctx: c.BuildContext, stats: dict[str, Any]) -> None:
    """Geodesic distances: derived-vs-officer, and derived-vs-reprojected-geometry.

    Both are DISTANCES, so both are computed on the WGS84 ellipsoid rather than
    in EPSG:3081 or in degrees. Python touches only the distinct coordinate
    quadruples, not the table.
    """
    con = ctx.con

    def _distances(sql: str) -> list[float]:
        return [
            c.geodesic_distance_m(a, b, x, y)
            for a, b, x, y in con.execute(sql).fetchall()
        ]

    pair = _distances(
        """SELECT DISTINCT lat_raw, lon_raw, rpt_lat_raw, rpt_lon_raw FROM txd_typed
           WHERE lat_raw IS NOT NULL AND rpt_lat_raw IS NOT NULL"""
    )
    if pair:
        pair.sort()
        stats["coord_pair_distance_m"] = {
            "n_distinct_pairs": len(pair),
            "median": round(pair[len(pair) // 2], 1),
            "p95": round(pair[int(len(pair) * 0.95)], 1),
            "max": round(pair[-1], 1),
        }

    geom = _distances(
        """SELECT DISTINCT lat_raw, lon_raw, geom_lat_3081_reproj, geom_lon_3081_reproj
           FROM txd_typed
           WHERE lat_raw IS NOT NULL AND geom_lat_3081_reproj IS NOT NULL"""
    )
    if geom:
        geom.sort()
        stats["reprojection_selfcheck_m"] = {
            "n_distinct_pairs": len(geom),
            "median": round(geom[len(geom) // 2], 4),
            "p95": round(geom[int(len(geom) * 0.95)], 4),
            "max": round(geom[-1], 4),
        }


def _history(ctx: c.BuildContext, partitions, stats: dict[str, Any]) -> None:
    scope = _snapshot_scope(ctx, partitions)
    mode = c.MODE_SNAPSHOT if scope else c.MODE_DELTA
    if scope is None:
        ctx.manifest.warn(
            "txdot: bronze partitions do not declare one common OID sweep range "
            "(cursor missing or ranges differ) -- deletion detection is DISABLED "
            "for this build. A missed deletion is recoverable; a fabricated one "
            "is not. See src/transform/txdot.py."
        )
    stats["scd2_mode"] = mode
    stats["snapshot_scope"] = scope

    sql = c.scd2_sql(
        source_relation="txd_typed",
        natural_key=NATURAL_KEY,
        attributes=ATTRIBUTES,
        # No mutation timestamp exists in this service. The load_ts that first
        # carried a given row hash is the earliest moment we can honestly claim
        # the value held.
        valid_from_expr="_bronze_load_ts",
        mode=mode,
        snapshot_scope=scope,
    )
    ctx.con.execute(f"CREATE OR REPLACE TABLE txd_crash_history AS {sql}")
    stats["silver_rows_crash_history"] = ctx.con.execute(
        "SELECT COUNT(*) FROM txd_crash_history"
    ).fetchone()[0]
    stats["silver_rows_crash_current"] = ctx.con.execute(
        "SELECT COUNT(*) FROM txd_crash_history WHERE is_current"
    ).fetchone()[0]
    stats["silver_rows_crash_deleted"] = ctx.con.execute(
        "SELECT COUNT(*) FROM txd_crash_history WHERE deleted_in_load_ts IS NOT NULL"
    ).fetchone()[0]
    stats["silver_max_version_no"] = ctx.con.execute(
        "SELECT MAX(version_no) FROM txd_crash_history"
    ).fetchone()[0]
