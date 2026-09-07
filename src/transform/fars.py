"""NHTSA FARS -- bronze to silver.

    fars/{year} -> fars/accident_{history,current}.parquet
                   fars/vehicle_{history,current}.parquet
                   fars/person_{history,current}.parquet
                   fars/codebook.parquet

All years and all states are carried. Scoping to MD/TX/FL is Phase 3's job:
silver's contract is "the source, typed and defect-handled", and a silver layer
that has already dropped 47 states cannot answer a question about the 48th
without a re-ingest.

Natural keys: accident `(year, st_case)`, vehicle `(year, st_case, veh_no)`,
person `(year, st_case, veh_no, per_no)`.

**`year` is not in the vehicle or person files.** Only `accident` publishes a
YEAR column; the other two members identify their year only by which zip they
came out of. The key therefore takes it from `_bronze_dataset`, which is the
partition path segment (`data/bronze/fars/2019/...`) that Phase 1 wrote into
every row. ST_CASE is unique only within a year -- it restarts each year -- so
without this the vehicle and person keys would collide across six years and
silently merge unrelated records.


Versioning mode: SNAPSHOT per year
----------------------------------
Each FARS partition is a complete re-download of one year, triggered by Phase 1
detecting a changed content hash at the same URL. So a partition is a snapshot
whose scope is the year, and diffing consecutive snapshots of one year is
exactly the restatement detection the assignment asks for: a revised ST_CASE
gets a new version, an ST_CASE that disappears from a later snapshot of its own
year is closed with `deleted_in_load_ts` set, and the unchanged rows produce
nothing because the row hash is unchanged.

The scope is the YEAR and only the year: the 2023 file says nothing about 2024.
Phase 1 measured that revision order does not follow year order -- 2021 carries
Last-Modified 2026-06-24, later than 2022/2023/2024's 2026-04-01 -- which is
exactly why the diff is per-year and driven by hashes rather than by recency.

`valid_from` is `_bronze_load_ts`: NHTSA publishes no per-record revision stamp
(the file-level Last-Modified is not a row-level fact), so the earliest moment
we can honestly claim a value held is when we first saw it.


Sentinels
---------
Driven by `config/fars_sentinels.csv`, per (table, column), NOT by a blanket
7/8/9 rule -- 9 is a legitimate substantive code in LGT_COND, HARM_EV and
MAN_COLL, and a blanket rule would corrupt all three.

Coordinate sentinels are matched NUMERICALLY. NHTSA changes the decimal
formatting of the same sentinel between years: 2019 writes `77.7777` and 2024
writes `77.77770000`. String equality on `77.7777` matches 8 rows in 2019 and 0
in 2024 while 170 sentinel rows sit there unmatched. Measured across 2019-2024:
873 accident rows carry a sentinel coordinate, and a string match would find 8.
On top of the sentinel list, anything outside [-90,90] / [-180,180] is rejected,
so a new sentinel format cannot place a crash in the Arctic Ocean before anyone
has added it to the CSV.

Sentinels are mapped BEFORE any join or aggregate. A MAX(INJ_SEV) that has not
had 9 removed returns 9.


The *NAME label columns
-----------------------
FARS ships a `<COL>NAME` label beside most coded columns -- 39 of them on the
accident file alone. They are dropped from the typed tables (they are a join
away and they triple the width) but extracted first into `fars/codebook.parquet`
as (table, column, code, label, first_year, last_year). That table doubles as
the evidence for the severity crosswalk: INJ_SEV's own labels, straight out of
NHTSA's files, are what the crosswalk's INJ_SEV rows are checked against.
"""

from __future__ import annotations

import csv
import logging
from dataclasses import dataclass
from typing import Any, Iterable

from ..config import CONFIG_DIR
from . import common as c
from .severity_crosswalk import assert_all_mapped, register as register_crosswalk

log = logging.getLogger(__name__)

SOURCE = "fars"
SOURCE_SYSTEM = "NHTSA_FARS"

SENTINEL_PATH = CONFIG_DIR / "fars_sentinels.csv"

MATCH_NUMERIC = "numeric_abs"
MATCH_EXACT_INT = "exact_int"
NUMERIC_TOLERANCE = 1e-4

# STATE is the FIPS state code as a string. FARS National files carry 51 codes
# (50 states + DC). Mapped to the two-letter jurisdiction the output contract's
# `^[A-Z]{2}$` pattern requires.
STATE_FIPS_TO_USPS: dict[str, str] = {
    "01": "AL", "02": "AK", "04": "AZ", "05": "AR", "06": "CA", "08": "CO",
    "09": "CT", "10": "DE", "11": "DC", "12": "FL", "13": "GA", "15": "HI",
    "16": "ID", "17": "IL", "18": "IN", "19": "IA", "20": "KS", "21": "KY",
    "22": "LA", "23": "ME", "24": "MD", "25": "MA", "26": "MI", "27": "MN",
    "28": "MS", "29": "MO", "30": "MT", "31": "NE", "32": "NV", "33": "NH",
    "34": "NJ", "35": "NM", "36": "NY", "37": "NC", "38": "ND", "39": "OH",
    "40": "OK", "41": "PA", "42": "PA", "44": "RI", "45": "SC", "46": "SD",
    "47": "TN", "48": "TX", "49": "UT", "50": "VT", "51": "VA", "53": "WA",
    "54": "WV", "55": "WI", "56": "WY", "60": "AS", "66": "GU", "69": "MP",
    "72": "PR", "78": "VI",
}
# FARS writes STATE unpadded ("1", not "01"), so both spellings resolve.
STATE_FIPS_TO_USPS.update(
    {k.lstrip("0"): v for k, v in list(STATE_FIPS_TO_USPS.items()) if k.startswith("0")}
)

ACCIDENT_KEY = ["year", "st_case"]
VEHICLE_KEY = ["year", "st_case", "veh_no"]
PERSON_KEY = ["year", "st_case", "veh_no", "per_no"]

ACCIDENT_ATTRIBUTES = [
    "state_fips", "jurisdiction", "county_fips", "city",
    "crash_date", "crash_datetime_local", "crash_hour", "crash_minute",
    "latitude", "longitude", "lat_raw", "lon_raw", "geo_quality",
    "coordinate_sentinel",
    "fatals", "persons", "permvit", "pernotmvit", "peds",
    "ve_total", "ve_forms", "pvh_invl",
    "harm_ev", "man_coll", "reljct1", "reljct2", "typ_int", "wrk_zone",
    "rel_road", "lgt_cond", "weather", "sch_bus", "rail",
    "route", "rur_urb", "func_sys", "rd_owner", "nhs", "sp_jur", "milept",
    "tway_id", "tway_id2", "day_week",
    "not_hour", "not_min", "arr_hour", "arr_min", "hosp_hr", "hosp_mn",
]

VEHICLE_ATTRIBUTES = [
    "state_fips", "jurisdiction", "crash_date",
    "numoccs", "make", "model", "mod_year", "body_typ", "vin",
    "trav_sp", "deformed", "tow_veh", "spec_use", "emer_use", "impact1",
    "veh_sc1", "veh_sc2", "dr_pres", "l_state", "dr_zip", "l_status",
    "hit_run", "reg_stat", "owner", "deaths", "dr_drink",
]

PERSON_ATTRIBUTES = [
    "state_fips", "jurisdiction", "crash_date",
    "age", "sex", "per_typ", "seat_pos", "rest_use", "air_bag", "ejection",
    "drinking", "drugs", "doa", "death_da", "death_mo", "death_yr",
    "hospital", "location",
    "inj_sev", "inj_sev_raw", "severity_ordinal", "severity_kabco",
    "severity_note",
]

# Vehicle/person columns that genuinely differ between 2019 and 2024 are not
# hardcoded away: the read is union_by_name and every column above is emitted
# with a TRY_CAST that yields NULL for a year that does not publish it. The
# accident file's 2019-only columns (CF1/CF2/CF3, WEATHER1/WEATHER2, DRUNK_DR)
# are deliberately NOT in ACCIDENT_ATTRIBUTES: a column present in one year of
# six would put a NULL in the row hash for the other five and add nothing.


@dataclass(frozen=True)
class Sentinel:
    table: str
    column: str
    value: str
    match_kind: str
    meaning: str
    source: str


def load_sentinels(path=None) -> tuple[Sentinel, ...]:
    """Parse config/fars_sentinels.csv, skipping its comment lines."""
    p = path or SENTINEL_PATH
    with open(p, newline="", encoding="utf-8") as fh:
        lines = [ln for ln in fh if ln.strip() and not ln.lstrip().startswith("#")]
    out: list[Sentinel] = []
    for row in csv.DictReader(lines):
        if not row.get("table"):
            continue
        kind = row["match_kind"].strip()
        if kind not in (MATCH_NUMERIC, MATCH_EXACT_INT):
            raise ValueError(f"{p}: unknown match_kind {kind!r}")
        out.append(
            Sentinel(
                table=row["table"].strip(),
                column=row["column"].strip(),
                value=row["sentinel_value"].strip(),
                match_kind=kind,
                meaning=row["meaning"].strip(),
                source=row["source"].strip(),
            )
        )
    if not out:
        raise ValueError(f"{p}: no sentinel rules")
    return tuple(out)


def sentinel_predicate(table: str, column: str, expr: str,
                       sentinels: Iterable[Sentinel] | None = None) -> str:
    """SQL that is TRUE when `expr` holds a sentinel for (table, column).

    Numeric comparison for the coordinate sentinels -- `abs(v - s) < 1e-4` --
    because the same sentinel is written `77.7777` in 2019 and `77.77770000` in
    2024 and string equality catches only the first. Integer-coded fields cast
    once and compare exactly, after which no formatting drift is possible.

    Returns `FALSE` when nothing is registered, so a column with no rule is
    never silently treated as all-sentinel.
    """
    rules = [
        s for s in (sentinels or load_sentinels())
        if s.table == table and s.column == column
    ]
    if not rules:
        return "FALSE"
    terms = []
    for s in rules:
        if s.match_kind == MATCH_NUMERIC:
            terms.append(
                f"abs(TRY_CAST({expr} AS DOUBLE) - {s.value}) < {NUMERIC_TOLERANCE}"
            )
        else:
            terms.append(f"TRY_CAST({expr} AS BIGINT) = {s.value}")
    return "(" + " OR ".join(terms) + ")"


def nulled(table: str, column: str, expr: str, cast: str,
           sentinels: Iterable[Sentinel] | None = None) -> str:
    """`expr` typed to `cast`, or NULL where it holds a sentinel."""
    pred = sentinel_predicate(table, column, expr, sentinels)
    return f"CASE WHEN {pred} THEN NULL ELSE TRY_CAST({expr} AS {cast}) END"


def _state_case() -> str:
    """SQL CASE mapping the STATE FIPS code to a two-letter jurisdiction."""
    whens = " ".join(
        f"WHEN '{k}' THEN '{v}'" for k, v in sorted(STATE_FIPS_TO_USPS.items())
    )
    return f"CASE STATE {whens} ELSE NULL END"


def build(ctx: c.BuildContext) -> dict[str, Any]:
    con = ctx.con
    stats: dict[str, Any] = {}
    register_crosswalk(con)
    sentinels = load_sentinels()
    stats["sentinel_rules"] = len(sentinels)

    years = c.discover_datasets(SOURCE, bronze_root=ctx.bronze_root)
    stats["years"] = years
    if not years:
        raise FileNotFoundError(f"no FARS years under {ctx.bronze_root / SOURCE}")

    partitions: list[c.Partition] = []
    for year in years:
        parts = ctx.partitions(SOURCE, year)
        for p in parts:
            ctx.manifest.add_input(p)
        partitions.extend(parts)
    stats["partitions"] = {
        y: [p.load_ts for p in partitions if p.dataset == y] for y in years
    }

    for member in ("accident", "vehicle", "person"):
        c.bronze_view(con, f"fars_{member}_raw", partitions, pattern=f"{member}.parquet")
        stats[f"bronze_rows_{member}"] = con.execute(
            f"SELECT COUNT(*) FROM fars_{member}_raw"
        ).fetchone()[0]

    _codebook(ctx, stats)
    _typed_accident(ctx, sentinels, stats)
    _typed_vehicle(ctx, sentinels, stats)
    _typed_person(ctx, sentinels, stats)
    _histories(ctx, stats)
    _crosschecks(ctx, stats)
    return stats


# A `<COL>NAME` label column exists beside continuous fields too (MILEPTNAME
# labels a milepost, TWAY_IDNAME a road name), and those are not code
# dictionaries -- MILEPT alone contributes ~10^5 distinct "codes". A code
# dictionary is bounded; this is where the line is drawn, and the columns it
# excludes are reported rather than silently dropped.
CODEBOOK_MAX_DISTINCT_CODES = 300


def _codebook(ctx: c.BuildContext, stats: dict[str, Any]) -> None:
    """Extract every `<COL>NAME` label column into one long codebook table.

    The labels are NHTSA's own, read out of the files rather than transcribed
    from the manual, so the codebook is evidence rather than assertion. It is
    what the severity crosswalk's INJ_SEV rows are checked against in
    `_crosschecks` -- and INJ_SEV's labels there read "Injured, Severity
    Unknown" (5) and "Died Prior to Crash*" (6), which is why neither maps to a
    severity.

    `first_year`/`last_year` come from `_bronze_dataset` (the partition's year),
    so a code that appeared or disappeared mid-range is visible.
    """
    con = ctx.con
    parts: list[str] = []
    excluded: list[str] = []
    for member in ("accident", "vehicle", "person"):
        cols = [
            r[0] for r in con.execute(
                f"DESCRIBE SELECT * FROM fars_{member}_raw"
            ).fetchall()
        ]
        colset = set(cols)
        pairs = [(col[:-4], col) for col in cols
                 if col.endswith("NAME") and col[:-4] in colset]
        kept = 0
        for code_col, name_col in pairs:
            n = con.execute(
                f"SELECT COUNT(DISTINCT {c.quote_ident(code_col)}) "
                f"FROM fars_{member}_raw"
            ).fetchone()[0]
            if n > CODEBOOK_MAX_DISTINCT_CODES:
                excluded.append(f"{member}.{code_col} ({n} distinct)")
                continue
            kept += 1
            parts.append(
                f"""SELECT '{member}' AS tbl, '{code_col}' AS col,
                           {c.quote_ident(code_col)} AS code,
                           {c.quote_ident(name_col)} AS label,
                           _bronze_dataset AS year
                    FROM fars_{member}_raw"""
            )
        stats[f"codebook_columns_{member}"] = kept

    if not parts:
        con.execute(
            "CREATE OR REPLACE TABLE fars_codebook (tbl VARCHAR, col VARCHAR, "
            "code VARCHAR, label VARCHAR, first_year VARCHAR, last_year VARCHAR, "
            "row_count BIGINT)"
        )
        stats["codebook_rows"] = 0
        return

    con.execute(
        f"""
        CREATE OR REPLACE TABLE fars_codebook AS
        SELECT tbl, col, code, label,
               MIN(year) AS first_year, MAX(year) AS last_year,
               COUNT(*)  AS row_count
        FROM ({" UNION ALL ".join(parts)})
        WHERE code IS NOT NULL
        GROUP BY 1, 2, 3, 4
        """
    )
    stats["codebook_rows"] = con.execute(
        "SELECT COUNT(*) FROM fars_codebook"
    ).fetchone()[0]
    stats["codebook_excluded_continuous_columns"] = sorted(excluded)


def _typed_accident(ctx: c.BuildContext, sentinels, stats: dict[str, Any]) -> None:
    con = ctx.con
    lat_sent = sentinel_predicate("accident", "LATITUDE", "LATITUDE", sentinels)
    lon_sent = sentinel_predicate("accident", "LONGITUD", "LONGITUD", sentinels)
    lat = "TRY_CAST(LATITUDE AS DOUBLE)"
    lon = "TRY_CAST(LONGITUD AS DOUBLE)"
    # Belt and braces on top of the sentinel list: a value outside the valid
    # range of the coordinate system itself is not a coordinate, whatever it is.
    # This is what stops a NEW sentinel format -- one nobody has added to the
    # CSV yet -- from putting a crash in the Arctic Ocean.
    out_of_range = f"({lat} NOT BETWEEN -90 AND 90 OR {lon} NOT BETWEEN -180 AND 180)"
    sentinel_any = f"({lat_sent} OR {lon_sent})"
    valid = (
        f"(NOT {sentinel_any} AND NOT {out_of_range} "
        f"AND {c.envelope_sql(SOURCE, lat, lon)})"
    )

    hour = nulled("accident", "HOUR", "HOUR", "INTEGER", sentinels)
    minute = nulled("accident", "MINUTE", "MINUTE", "INTEGER", sentinels)

    con.execute(
        f"""
        CREATE OR REPLACE VIEW fars_accident_typed AS
        SELECT
            _bronze_dataset                             AS year,
            ST_CASE                                     AS st_case,
            STATE                                       AS state_fips,
            {_state_case()}                             AS jurisdiction,
            -- FARS COUNTY is the county FIPS *within* the state, unpadded.
            CASE WHEN TRY_CAST(COUNTY AS INTEGER) BETWEEN 1 AND 998
                 THEN lpad(COUNTY, 3, '0') END          AS county_fips,
            TRY_CAST(CITY AS INTEGER)                   AS city,
            make_date(TRY_CAST(YEAR AS INTEGER),
                      TRY_CAST(MONTH AS INTEGER),
                      TRY_CAST(DAY AS INTEGER))         AS crash_date,
            -- HOUR/MINUTE = 99 is "unknown", so the TIMESTAMP is NULL while the
            -- DATE stays populated: the crash has a known date and an unknown
            -- time, and collapsing that into a midnight timestamp would invent
            -- 244-305 crashes per year at 00:00. Naive local wall clock; FARS
            -- publishes local time and localisation is Phase 4.
            CASE WHEN {hour} IS NULL OR {minute} IS NULL THEN NULL
                 ELSE make_timestamp(TRY_CAST(YEAR AS INTEGER),
                                     TRY_CAST(MONTH AS INTEGER),
                                     TRY_CAST(DAY AS INTEGER),
                                     {hour}, {minute}, 0)
            END                                         AS crash_datetime_local,
            {hour}                                      AS crash_hour,
            {minute}                                    AS crash_minute,
            CASE WHEN {valid} THEN {lat} END            AS latitude,
            CASE WHEN {valid} THEN {lon} END            AS longitude,
            {lat}                                       AS lat_raw,
            {lon}                                       AS lon_raw,
            CASE WHEN {lat} IS NULL OR {lon} IS NULL THEN '{c.GEO_MISSING}'
                 WHEN {sentinel_any} OR {out_of_range}  THEN '{c.GEO_SENTINEL}'
                 WHEN {c.envelope_sql(SOURCE, lat, lon)} THEN '{c.GEO_OK}'
                 ELSE '{c.GEO_OUT_OF_ENVELOPE}' END     AS geo_quality,
            {sentinel_any}                              AS coordinate_sentinel,
            TRY_CAST(FATALS AS INTEGER)                 AS fatals,
            TRY_CAST(PERSONS AS INTEGER)                AS persons,
            TRY_CAST(PERMVIT AS INTEGER)                AS permvit,
            TRY_CAST(PERNOTMVIT AS INTEGER)             AS pernotmvit,
            TRY_CAST(PEDS AS INTEGER)                   AS peds,
            TRY_CAST(VE_TOTAL AS INTEGER)               AS ve_total,
            TRY_CAST(VE_FORMS AS INTEGER)               AS ve_forms,
            TRY_CAST(PVH_INVL AS INTEGER)               AS pvh_invl,
            TRY_CAST(HARM_EV AS INTEGER)                AS harm_ev,
            TRY_CAST(MAN_COLL AS INTEGER)               AS man_coll,
            TRY_CAST(RELJCT1 AS INTEGER)                AS reljct1,
            TRY_CAST(RELJCT2 AS INTEGER)                AS reljct2,
            TRY_CAST(TYP_INT AS INTEGER)                AS typ_int,
            TRY_CAST(WRK_ZONE AS INTEGER)               AS wrk_zone,
            TRY_CAST(REL_ROAD AS INTEGER)               AS rel_road,
            TRY_CAST(LGT_COND AS INTEGER)               AS lgt_cond,
            TRY_CAST(WEATHER AS INTEGER)                AS weather,
            TRY_CAST(SCH_BUS AS INTEGER)                AS sch_bus,
            TRY_CAST(RAIL AS VARCHAR)                   AS rail,
            TRY_CAST(ROUTE AS INTEGER)                  AS route,
            TRY_CAST(RUR_URB AS INTEGER)                AS rur_urb,
            TRY_CAST(FUNC_SYS AS INTEGER)               AS func_sys,
            TRY_CAST(RD_OWNER AS INTEGER)               AS rd_owner,
            TRY_CAST(NHS AS INTEGER)                    AS nhs,
            TRY_CAST(SP_JUR AS INTEGER)                 AS sp_jur,
            {nulled("accident", "MILEPT", "MILEPT", "INTEGER", sentinels)} AS milept,
            TWAY_ID                                     AS tway_id,
            TWAY_ID2                                    AS tway_id2,
            TRY_CAST(DAY_WEEK AS INTEGER)               AS day_week,
            {nulled("accident", "NOT_HOUR", "NOT_HOUR", "INTEGER", sentinels)} AS not_hour,
            {nulled("accident", "NOT_MIN", "NOT_MIN", "INTEGER", sentinels)}   AS not_min,
            {nulled("accident", "ARR_HOUR", "ARR_HOUR", "INTEGER", sentinels)} AS arr_hour,
            {nulled("accident", "ARR_MIN", "ARR_MIN", "INTEGER", sentinels)}   AS arr_min,
            {nulled("accident", "HOSP_HR", "HOSP_HR", "INTEGER", sentinels)}   AS hosp_hr,
            {nulled("accident", "HOSP_MN", "HOSP_MN", "INTEGER", sentinels)}   AS hosp_mn,
            _bronze_load_ts,
            -- FARS bronze carries _bronze_member and _bronze_zip_sha256 rather
            -- than the _bronze_page/_bronze_raw_path/_bronze_row_sha256 trio the
            -- HTTP sources write: the unit of raw preservation is the annual
            -- zip, not a page. The zip hash IS the row's provenance, so it
            -- stands in for the row hash and the member name for the path.
            _bronze_zip_sha256                          AS _bronze_row_sha256,
            _bronze_member                              AS _bronze_raw_path
        FROM fars_accident_raw
        """
    )

    stats["accident_sentinel_coords"] = con.execute(
        "SELECT COUNT(*) FROM fars_accident_typed WHERE coordinate_sentinel"
    ).fetchone()[0]
    stats["accident_sentinel_coords_by_year"] = dict(
        con.execute(
            "SELECT year, COUNT(*) FROM fars_accident_typed "
            "WHERE coordinate_sentinel GROUP BY 1 ORDER BY 1"
        ).fetchall()
    )
    stats["accident_sentinel_string_match_77_7777"] = dict(
        con.execute(
            "SELECT _bronze_dataset, SUM(CASE WHEN LATITUDE = '77.7777' THEN 1 ELSE 0 END) "
            "FROM fars_accident_raw GROUP BY 1 ORDER BY 1"
        ).fetchall()
    )
    stats["accident_geo_quality"] = dict(
        con.execute(
            "SELECT geo_quality, COUNT(*) FROM fars_accident_typed GROUP BY 1 ORDER BY 2 DESC"
        ).fetchall()
    )
    stats["accident_hour_unknown"] = dict(
        con.execute(
            "SELECT year, COUNT(*) FROM fars_accident_typed "
            "WHERE crash_hour IS NULL GROUP BY 1 ORDER BY 1"
        ).fetchall()
    )
    stats["accident_datetime_null_date_present"] = con.execute(
        "SELECT COUNT(*) FROM fars_accident_typed "
        "WHERE crash_datetime_local IS NULL AND crash_date IS NOT NULL"
    ).fetchone()[0]
    stats["accident_jurisdiction_null"] = con.execute(
        "SELECT COUNT(*) FROM fars_accident_typed WHERE jurisdiction IS NULL"
    ).fetchone()[0]
    stats["accident_max_latitude_outside_alaska"] = con.execute(
        "SELECT MAX(latitude) FROM fars_accident_typed WHERE jurisdiction <> 'AK'"
    ).fetchone()[0]


def _typed_vehicle(ctx: c.BuildContext, sentinels, stats: dict[str, Any]) -> None:
    ctx.con.execute(
        f"""
        CREATE OR REPLACE VIEW fars_vehicle_typed AS
        SELECT
            v._bronze_dataset                           AS year,
            v.ST_CASE                                   AS st_case,
            TRY_CAST(v.VEH_NO AS INTEGER)               AS veh_no,
            v.STATE                                     AS state_fips,
            a.jurisdiction                              AS jurisdiction,
            a.crash_date                                AS crash_date,
            TRY_CAST(v.NUMOCCS AS INTEGER)              AS numoccs,
            TRY_CAST(v.MAKE AS INTEGER)                 AS make,
            TRY_CAST(v.MODEL AS INTEGER)                AS model,
            {nulled("vehicle", "MOD_YEAR", "v.MOD_YEAR", "INTEGER", sentinels)} AS mod_year,
            TRY_CAST(v.BODY_TYP AS INTEGER)             AS body_typ,
            v.VIN                                       AS vin,
            {nulled("vehicle", "TRAV_SP", "v.TRAV_SP", "INTEGER", sentinels)}   AS trav_sp,
            TRY_CAST(v.DEFORMED AS INTEGER)             AS deformed,
            TRY_CAST(v.TOW_VEH AS INTEGER)              AS tow_veh,
            TRY_CAST(v.SPEC_USE AS INTEGER)             AS spec_use,
            TRY_CAST(v.EMER_USE AS INTEGER)             AS emer_use,
            TRY_CAST(v.IMPACT1 AS INTEGER)              AS impact1,
            TRY_CAST(v.VEH_SC1 AS INTEGER)              AS veh_sc1,
            TRY_CAST(v.VEH_SC2 AS INTEGER)              AS veh_sc2,
            TRY_CAST(v.DR_PRES AS INTEGER)              AS dr_pres,
            TRY_CAST(v.L_STATE AS INTEGER)              AS l_state,
            v.DR_ZIP                                    AS dr_zip,
            TRY_CAST(v.L_STATUS AS INTEGER)             AS l_status,
            TRY_CAST(v.HIT_RUN AS INTEGER)              AS hit_run,
            TRY_CAST(v.REG_STAT AS INTEGER)             AS reg_stat,
            TRY_CAST(v.OWNER AS INTEGER)                AS owner,
            TRY_CAST(v.DEATHS AS INTEGER)               AS deaths,
            TRY_CAST(v.DR_DRINK AS INTEGER)             AS dr_drink,
            v._bronze_load_ts,
            v._bronze_zip_sha256                        AS _bronze_row_sha256,
            v._bronze_member                            AS _bronze_raw_path
        FROM fars_vehicle_raw v
        LEFT JOIN fars_accident_typed a
               ON a.year = v._bronze_dataset AND a.st_case = v.ST_CASE
        """
    )
    stats["vehicle_without_accident"] = ctx.con.execute(
        "SELECT COUNT(*) FROM fars_vehicle_typed WHERE crash_date IS NULL"
    ).fetchone()[0]


def _typed_person(ctx: c.BuildContext, sentinels, stats: dict[str, Any]) -> None:
    con = ctx.con
    con.execute(
        f"""
        CREATE OR REPLACE VIEW fars_person_typed AS
        SELECT
            p._bronze_dataset                           AS year,
            p.ST_CASE                                   AS st_case,
            TRY_CAST(p.VEH_NO AS INTEGER)               AS veh_no,
            TRY_CAST(p.PER_NO AS INTEGER)               AS per_no,
            p.STATE                                     AS state_fips,
            a.jurisdiction                              AS jurisdiction,
            a.crash_date                                AS crash_date,
            {nulled("person", "AGE", "p.AGE", "INTEGER", sentinels)}  AS age,
            TRY_CAST(p.SEX AS INTEGER)                  AS sex,
            TRY_CAST(p.PER_TYP AS INTEGER)              AS per_typ,
            TRY_CAST(p.SEAT_POS AS INTEGER)             AS seat_pos,
            TRY_CAST(p.REST_USE AS INTEGER)             AS rest_use,
            TRY_CAST(p.AIR_BAG AS INTEGER)              AS air_bag,
            TRY_CAST(p.EJECTION AS INTEGER)             AS ejection,
            TRY_CAST(p.DRINKING AS INTEGER)             AS drinking,
            TRY_CAST(p.DRUGS AS INTEGER)                AS drugs,
            {nulled("person", "DOA", "p.DOA", "INTEGER", sentinels)}  AS doa,
            TRY_CAST(p.DEATH_DA AS INTEGER)             AS death_da,
            TRY_CAST(p.DEATH_MO AS INTEGER)             AS death_mo,
            TRY_CAST(p.DEATH_YR AS INTEGER)             AS death_yr,
            TRY_CAST(p.HOSPITAL AS INTEGER)             AS hospital,
            TRY_CAST(p.LOCATION AS INTEGER)             AS location,
            -- inj_sev is sentinel-nulled (9 = unknown whether injured);
            -- inj_sev_raw keeps every code so the crosswalk can distinguish
            -- 5 "injured, severity unknown" from 6 "died prior to crash" from
            -- 9 "unknown", which map to the same ordinal for three different
            -- reasons. Mapping happens BEFORE any MAX() -- a MAX(INJ_SEV) that
            -- has not had 9 removed returns 9.
            {nulled("person", "INJ_SEV", "p.INJ_SEV", "INTEGER", sentinels)} AS inj_sev,
            TRY_CAST(p.INJ_SEV AS INTEGER)              AS inj_sev_raw,
            CAST(x.severity_ordinal AS INTEGER)         AS severity_ordinal,
            x.kabco                                     AS severity_kabco,
            CASE WHEN p.INJ_SEV = '5' THEN 'INJURED_UNKNOWN'
                 WHEN p.INJ_SEV = '6' THEN 'DIED_PRIOR_TO_CRASH'
                 WHEN p.INJ_SEV = '9' THEN 'UNKNOWN'
            END                                         AS severity_note,
            p._bronze_load_ts,
            p._bronze_zip_sha256                        AS _bronze_row_sha256,
            p._bronze_member                            AS _bronze_raw_path
        FROM fars_person_raw p
        LEFT JOIN fars_accident_typed a
               ON a.year = p._bronze_dataset AND a.st_case = p.ST_CASE
        LEFT JOIN severity_crosswalk x
               ON x.source_system = '{SOURCE_SYSTEM}'
              AND x.source_column = 'INJ_SEV'
              AND x.source_value = p.INJ_SEV
        """
    )
    assert_all_mapped(
        con, table="fars_person_typed", source_system=SOURCE_SYSTEM,
        source_column="INJ_SEV",
        value_expr="coalesce(CAST(inj_sev_raw AS VARCHAR), '__NULL__')",
    )
    stats["person_inj_sev_counts"] = dict(
        con.execute(
            "SELECT inj_sev_raw, COUNT(*) FROM fars_person_typed GROUP BY 1 ORDER BY 1"
        ).fetchall()
    )
    stats["person_age_sentinel"] = con.execute(
        "SELECT COUNT(*) FROM fars_person_typed WHERE age IS NULL"
    ).fetchone()[0]
    stats["person_without_accident"] = con.execute(
        "SELECT COUNT(*) FROM fars_person_typed WHERE crash_date IS NULL"
    ).fetchone()[0]


def _histories(ctx: c.BuildContext, stats: dict[str, Any]) -> None:
    """SCD2 all three members, snapshot-scoped to the year."""
    for member, key, attrs in (
        ("accident", ACCIDENT_KEY, ACCIDENT_ATTRIBUTES),
        ("vehicle", VEHICLE_KEY, VEHICLE_ATTRIBUTES),
        ("person", PERSON_KEY, PERSON_ATTRIBUTES),
    ):
        sql = c.scd2_sql(
            source_relation=f"fars_{member}_typed",
            natural_key=key,
            attributes=attrs,
            valid_from_expr="_bronze_load_ts",
            mode=c.MODE_SNAPSHOT,
            # A FARS partition is a complete snapshot of exactly one year.
            snapshot_scope="year",
        )
        ctx.con.execute(f"CREATE OR REPLACE TABLE fars_{member}_history AS {sql}")
        stats[f"silver_rows_{member}_history"] = ctx.con.execute(
            f"SELECT COUNT(*) FROM fars_{member}_history"
        ).fetchone()[0]
        stats[f"silver_rows_{member}_current"] = ctx.con.execute(
            f"SELECT COUNT(*) FROM fars_{member}_history WHERE is_current"
        ).fetchone()[0]
        stats[f"silver_rows_{member}_deleted"] = ctx.con.execute(
            f"SELECT COUNT(*) FROM fars_{member}_history "
            "WHERE deleted_in_load_ts IS NOT NULL"
        ).fetchone()[0]


def _crosschecks(ctx: c.BuildContext, stats: dict[str, Any]) -> None:
    """FARS is a fatality census, so assert that -- and report where it fails.

    Crash-level severity for FARS is 5 by definition. The check is whether the
    person file agrees: every accident should have at least one person with
    INJ_SEV=4. Accidents that do not are flagged rather than corrected -- FATALS
    is NHTSA's own count and disagreeing with it is a finding, not a bug to
    paper over.
    """
    con = ctx.con
    con.execute(
        """
        CREATE OR REPLACE TABLE fars_accident_severity AS
        SELECT a.year, a.st_case,
               MAX(p.severity_ordinal)                                AS max_person_ordinal,
               SUM(CASE WHEN p.inj_sev_raw = 4 THEN 1 ELSE 0 END)     AS fatal_persons,
               SUM(CASE WHEN p.inj_sev_raw = 5 THEN 1 ELSE 0 END)     AS injured_unknown_persons,
               SUM(CASE WHEN p.inj_sev_raw = 6 THEN 1 ELSE 0 END)     AS died_prior_persons,
               COUNT(p.per_no)                                        AS person_rows
        FROM fars_accident_history a
        LEFT JOIN fars_person_history p
               ON p.year = a.year AND p.st_case = a.st_case AND p.is_current
        WHERE a.is_current
        GROUP BY 1, 2
        """
    )
    row = con.execute(
        """SELECT COUNT(*),
                  SUM(CASE WHEN fatal_persons = 0 THEN 1 ELSE 0 END),
                  SUM(CASE WHEN person_rows = 0 THEN 1 ELSE 0 END),
                  SUM(injured_unknown_persons), SUM(died_prior_persons)
           FROM fars_accident_severity"""
    ).fetchone()
    (stats["accidents"], stats["accidents_without_fatal_person"],
     stats["accidents_without_person_rows"], stats["persons_injured_unknown"],
     stats["persons_died_prior_to_crash"]) = row

    stats["fatals_vs_person_count_mismatch"] = con.execute(
        """SELECT COUNT(*) FROM fars_accident_history a
           JOIN fars_accident_severity s ON s.year = a.year AND s.st_case = a.st_case
           WHERE a.is_current AND a.fatals <> s.fatal_persons"""
    ).fetchone()[0]

    # The codebook is the evidence for the crosswalk's INJ_SEV rows: NHTSA's own
    # labels, out of NHTSA's own files.
    stats["inj_sev_labels"] = dict(
        con.execute(
            "SELECT code, MAX(label) FROM fars_codebook "
            "WHERE tbl = 'person' AND col = 'INJ_SEV' GROUP BY 1 ORDER BY 1"
        ).fetchall()
    )
    stats["in_scope_states_by_year"] = [
        dict(zip(["year", "TX", "FL", "MD", "MONTGOMERY_MD"], r))
        for r in con.execute(
            """SELECT year,
                      SUM(CASE WHEN jurisdiction = 'TX' THEN 1 ELSE 0 END),
                      SUM(CASE WHEN jurisdiction = 'FL' THEN 1 ELSE 0 END),
                      SUM(CASE WHEN jurisdiction = 'MD' THEN 1 ELSE 0 END),
                      SUM(CASE WHEN jurisdiction = 'MD' AND county_fips = '031'
                               THEN 1 ELSE 0 END)
               FROM fars_accident_history WHERE is_current GROUP BY 1 ORDER BY 1"""
        ).fetchall()
    ]
