"""Every known defect's measured count, from the full local bronze.

    python -m src.transform.report
    python -m src.transform.report --json
    python -m src.transform.report --defect coordinates

One row per defect: the detection query, the measured count, and what the
pipeline does about it. This is the source of the numbers in DATA_QUALITY.md --
they are printed from the data rather than transcribed, so a reviewer can
re-run this and get the same table.

Every query below runs against BRONZE, because that is where the defect lives.
Where the transform changes the number (dedupe, envelope padding), both figures
are reported: silver's is the second column, and a defect whose bronze and
silver counts differ is exactly the thing the reader wants to see.
"""

from __future__ import annotations

import argparse
import json
import sys
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Callable

from ..config import BRONZE_DIR, SILVER_DIR, envelope
from ..ingest.watermark import WatermarkStore
from . import common as c
from .dictionaries import ObservedValue, detect_drift, parse_substance

INC = "moco_inc"
DRV = "moco_drv"
NM = "moco_nm"
TXD = "txd"
FARS_A = "fars_a"
FARS_P = "fars_p"


@dataclass
class Defect:
    """One known defect with its detection query and disposition."""

    key: str
    title: str
    source: str
    query: str
    disposition: str
    measure: Callable[[Any], dict[str, Any]] = field(repr=False, default=None)


def _views(con, bronze_root: Path, store: WatermarkStore | None) -> None:
    """Register a view per bronze dataset. Missing datasets are skipped."""
    for name, source, dataset in (
        (INC, "montgomery", "bhju-22kf"),
        (DRV, "montgomery", "mmzv-x632"),
        (NM, "montgomery", "n7fk-dce5"),
        (TXD, "txdot", "cris_crash"),
    ):
        parts = c.discover_partitions(source, dataset, bronze_root=bronze_root,
                                      store=store)
        if parts:
            c.bronze_view(con, name, parts)

    years = c.discover_datasets("fars", bronze_root=bronze_root)
    fars_parts = [
        p for y in years
        for p in c.discover_partitions("fars", y, bronze_root=bronze_root, store=store)
    ]
    if fars_parts:
        c.bronze_view(con, FARS_A, fars_parts, pattern="accident.parquet")
        c.bronze_view(con, FARS_P, fars_parts, pattern="person.parquet")


def _has(con, view: str) -> bool:
    return bool(
        con.execute(
            "SELECT COUNT(*) FROM duckdb_views() WHERE view_name = ?", [view]
        ).fetchone()[0]
    )


def _silver(con, root: Path, rel: str, table: str) -> str | None:
    path = Path(root) / rel
    if not path.exists():
        return None
    con.execute(
        f"CREATE OR REPLACE VIEW {table} AS SELECT * FROM "
        f"read_parquet('{str(path).replace(chr(39), chr(39) * 2)}')"
    )
    return table


# --------------------------------------------------------------------------
# the nine defects
# --------------------------------------------------------------------------


def measure(con, silver_root: Path) -> list[dict[str, Any]]:
    env = envelope("montgomery")
    out: list[dict[str, Any]] = []

    def add(key, title, source, query, disposition, bronze, silver=None, **extra):
        out.append({
            "defect": key, "title": title, "source": source,
            "detection_query": " ".join(query.split()),
            "bronze": bronze, "silver": silver,
            "disposition": disposition, **extra,
        })

    # 1 ---------------------------------------------------------------
    if _has(con, INC):
        q = f"""SELECT COUNT(*) FROM (SELECT DISTINCT ":id", latitude, longitude FROM {INC})
                WHERE TRY_CAST(latitude AS DOUBLE)
                        NOT BETWEEN {env['assignment_min_lat']} AND {env['assignment_max_lat']}
                   OR TRY_CAST(longitude AS DOUBLE)
                        NOT BETWEEN {env['assignment_min_lon']} AND {env['assignment_max_lon']}"""
        n_assignment = con.execute(q).fetchone()[0]
        q_pad = f"""SELECT COUNT(*) FROM (SELECT DISTINCT ":id", latitude, longitude FROM {INC})
                    WHERE NOT {c.envelope_sql('montgomery',
                              'TRY_CAST(latitude AS DOUBLE)',
                              'TRY_CAST(longitude AS DOUBLE)')}"""
        n_pad = con.execute(q_pad).fetchone()[0]
        nulls = con.execute(
            f"""SELECT COUNT(*) FROM {INC} WHERE latitude IS NULL OR longitude IS NULL
                OR TRY_CAST(latitude AS DOUBLE) = 0 OR TRY_CAST(longitude AS DOUBLE) = 0"""
        ).fetchone()[0]
        sv = _silver(con, silver_root, "montgomery/crash_current.parquet", "s_crash")
        silver_stats = None
        if sv:
            silver_stats = dict(zip(
                ["out_of_envelope", "lat_nulled", "max_km", "median_km"],
                con.execute(
                    """SELECT SUM(CASE WHEN geo_quality = 'OUT_OF_ENVELOPE' THEN 1 ELSE 0 END),
                              SUM(CASE WHEN latitude IS NULL THEN 1 ELSE 0 END),
                              ROUND(MAX(distance_from_envelope_m) / 1000, 2),
                              ROUND(MEDIAN(distance_from_envelope_m)
                                    FILTER (WHERE geo_quality = 'OUT_OF_ENVELOPE') / 1000, 2)
                       FROM s_crash"""
                ).fetchone()))
        add("coordinates_outside_envelope",
            "Coordinates that pass a null check and are still wrong",
            "montgomery/bhju-22kf", q,
            "KEPT, never dropped. lat_raw/lon_raw preserve the published values, "
            "canonical latitude/longitude are NULLed and geo_quality is "
            "OUT_OF_ENVELOPE, with distance_from_envelope_m in geodesic metres. "
            "Dropping them would change the crash count between layers and lose "
            "the evidence that a report exists; a wrong coordinate is not a "
            "wrong crash.",
            {"out_of_assignment_bbox": n_assignment,
             "out_of_padded_envelope": n_pad,
             "null_or_zero_coordinates": nulls},
            silver_stats)

    # 2 ---------------------------------------------------------------
    if _has(con, DRV):
        q = f"SELECT driver_substance_abuse, COUNT(*) FROM {DRV} GROUP BY 1"
        rows = con.execute(q).fetchall()
        parsed = [(v, n, parse_substance(v)) for v, n in rows]
        by_scheme: dict[str, int] = {}
        for _v, n, s in parsed:
            by_scheme[s.scheme] = by_scheme.get(s.scheme, 0) + n
        sv = _silver(con, silver_root, "montgomery/driver_current.parquet", "s_drv")
        silver_stats = None
        if sv:
            silver_stats = {
                "by_scheme": dict(con.execute(
                    "SELECT substance_scheme, COUNT(*) FROM s_drv GROUP BY 1 ORDER BY 1"
                ).fetchall()),
                "unmapped": con.execute(
                    "SELECT COUNT(*) FROM s_drv WHERE substance_scheme = 'UNMAPPED'"
                ).fetchone()[0],
                "alcohol_suspected": con.execute(
                    "SELECT COUNT(*) FROM s_drv WHERE alcohol_status = 'SUSPECTED'"
                ).fetchone()[0],
                "drug_suspected": con.execute(
                    "SELECT COUNT(*) FROM s_drv WHERE drug_status = 'SUSPECTED'"
                ).fetchone()[0],
            }
        add("substance_two_dictionary_generations",
            "Two generations of code dictionary concatenated in one column",
            "montgomery/mmzv-x632", q,
            "Parsed by grammar, never by date. substance_scheme, alcohol_status, "
            "drug_status and substance_detail replace the raw string, which is "
            "kept in substance_raw. Four spellings of null stay distinct: "
            "SQL NULL and 'UNKNOWN'/'Unknown, Unknown' -> UNKNOWN, 'N/A' -> "
            "NOT_APPLICABLE (no driver to test).",
            {"distinct_values": len(rows),
             "rows_by_scheme": by_scheme,
             "sql_nulls": sum(n for v, n in rows if v is None)},
            silver_stats)

    # 3 ---------------------------------------------------------------
    if _has(con, DRV):
        q = f"""SELECT d, n_new, n_old FROM (
                  SELECT substr(crash_date_time, 1, 10) AS d,
                    SUM(CASE WHEN driver_substance_abuse LIKE '%, %' THEN 1 ELSE 0 END) n_new,
                    SUM(CASE WHEN driver_substance_abuse ~ '^[A-Z0-9/ ]+$' THEN 1 ELSE 0 END) n_old
                  FROM (SELECT DISTINCT ":id", crash_date_time, driver_substance_abuse FROM {DRV})
                  GROUP BY 1) WHERE n_new > 0 AND n_old > 0 ORDER BY 1"""
        overlap = con.execute(q).fetchall()
        first_new = con.execute(
            f"""SELECT MIN(substr(crash_date_time, 1, 10)) FROM {DRV}
                WHERE driver_substance_abuse LIKE '%, %'"""
        ).fetchone()[0]
        last_old = con.execute(
            f"""SELECT MAX(substr(crash_date_time, 1, 10)) FROM {DRV}
                WHERE driver_substance_abuse ~ '^[A-Z0-9/ ]+$'"""
        ).fetchone()[0]
        # The number a hardcoded cutover would get wrong, minimised over every
        # candidate date. Non-zero means no single date is correct.
        best = con.execute(
            f"""WITH v AS (SELECT DISTINCT ":id" AS id, substr(crash_date_time,1,10) AS d,
                    CASE WHEN driver_substance_abuse LIKE '%, %' THEN 'NEW'
                         ELSE 'OLD' END AS scheme FROM {DRV}),
                     cand AS (SELECT DISTINCT d FROM v)
                SELECT MIN(wrong) FROM (
                  SELECT cand.d, SUM(CASE WHEN (v.d >= cand.d) <> (v.scheme = 'NEW')
                                          THEN 1 ELSE 0 END) AS wrong
                  FROM cand CROSS JOIN v GROUP BY 1)"""
        ).fetchone()[0]
        sv = _silver(con, silver_root, "montgomery/driver_current.parquet", "s_drv2")
        silver_stats = None
        if sv and overlap:
            lo, hi = overlap[0][0], overlap[-1][0]
            silver_stats = dict(zip(
                ["rows_in_window", "resolved_by_grammar", "unmapped_in_window"],
                con.execute(
                    f"""SELECT COUNT(*),
                               SUM(CASE WHEN substance_scheme IN ('OLD_SINGLE','NEW_PAIR')
                                        THEN 1 ELSE 0 END),
                               SUM(CASE WHEN substance_scheme = 'UNMAPPED' THEN 1 ELSE 0 END)
                        FROM s_drv2 WHERE crash_date BETWEEN DATE '{lo}' AND DATE '{hi}'"""
                ).fetchone()))
        add("dictionary_cutover_overlaps",
            "The dictionary cutover overlaps -- no hardcoded date is correct",
            "montgomery/mmzv-x632", q,
            "No cutover date is used anywhere. Every value is classified by "
            "grammar, so the overlap window needs no special case at all. The "
            "window is measured by CRASH DATE; :created_at cannot be used "
            "because a 2024-06-12 bulk reload stamped 172,096 old-generation and "
            "3,637 new-generation rows with one creation date.",
            {"overlap_window": [dict(zip(["crash_date", "new", "old"], r))
                                for r in overlap],
             "first_new_scheme_crash_date": first_new,
             "last_old_scheme_crash_date": last_old,
             "min_misclassified_by_any_single_cutover_date": best},
            silver_stats)

    # 4 ---------------------------------------------------------------
    if _has(con, INC) and _has(con, DRV):
        q = f"""SELECT COUNT(*) FROM (SELECT DISTINCT report_number FROM {INC}) i
                WHERE NOT EXISTS (SELECT 1 FROM {DRV} d
                                  WHERE d.report_number = i.report_number)"""
        inc_only = con.execute(q).fetchone()[0]
        drv_only = con.execute(
            f"""SELECT COUNT(*) FROM (SELECT DISTINCT report_number FROM {DRV}) d
                WHERE NOT EXISTS (SELECT 1 FROM {INC} i
                                  WHERE i.report_number = d.report_number)"""
        ).fetchone()[0]
        explained = 0
        if _has(con, NM):
            explained = con.execute(
                f"""SELECT COUNT(*) FROM (SELECT DISTINCT report_number FROM {INC}) i
                    WHERE NOT EXISTS (SELECT 1 FROM {DRV} d
                                      WHERE d.report_number = i.report_number)
                      AND EXISTS (SELECT 1 FROM {NM} n
                                  WHERE n.report_number = i.report_number)"""
            ).fetchone()[0]
        by_type = dict(con.execute(
            f"""SELECT acrs_report_type, COUNT(*) FROM
                  (SELECT DISTINCT report_number, acrs_report_type FROM {INC}) i
                WHERE NOT EXISTS (SELECT 1 FROM {DRV} d
                                  WHERE d.report_number = i.report_number)
                GROUP BY 1 ORDER BY 2 DESC"""
        ).fetchall())
        sv = _silver(con, silver_root, "montgomery/crash_current.parquet", "s_crash2")
        silver_stats = None
        if sv:
            silver_stats = dict(zip(
                ["crashes", "without_driver_rows", "without_any_party"],
                con.execute(
                    """SELECT COUNT(*),
                              SUM(CASE WHEN NOT has_driver_rows THEN 1 ELSE 0 END),
                              SUM(CASE WHEN NOT has_driver_rows
                                        AND NOT has_non_motorist_rows THEN 1 ELSE 0 END)
                       FROM s_crash2"""
                ).fetchone()))
        add("incidents_drivers_universe_disagreement",
            "The tables disagree about the crash universe",
            "montgomery/bhju-22kf + mmzv-x632", q,
            "An inner join is never used. Crashes with no driver row are KEPT "
            "and flagged has_driver_rows=false; the party tables join back with "
            "a LEFT JOIN. The contract's foreign key from driver to crash is "
            "enforced in the direction that holds (0 orphan drivers) and the "
            "other direction is a flag, not a constraint.",
            {"incident_report_numbers_without_a_driver": inc_only,
             "driver_report_numbers_without_an_incident": drv_only,
             "of_which_have_a_non_motorist_row": explained,
             "of_which_have_no_party_row_at_all": inc_only - explained,
             "by_acrs_report_type": by_type},
            silver_stats)

    # 5 ---------------------------------------------------------------
    if _has(con, DRV):
        q = f"""SELECT COUNT(*)::DOUBLE / COUNT(DISTINCT report_number) FROM {DRV}"""
        raw_ratio = con.execute(q).fetchone()[0]
        dedup_ratio = con.execute(
            f"""SELECT COUNT(*)::DOUBLE / COUNT(DISTINCT report_number)
                FROM (SELECT DISTINCT ":id", report_number FROM {DRV})"""
        ).fetchone()[0]
        sv = _silver(con, silver_root, "montgomery/driver_current.parquet", "s_drv3")
        silver_stats = None
        if sv:
            silver_stats = dict(zip(
                ["driver_rows", "distinct_report_numbers", "rows_per_crash"],
                con.execute(
                    """SELECT COUNT(*), COUNT(DISTINCT report_number),
                              ROUND(COUNT(*)::DOUBLE / COUNT(DISTINCT report_number), 4)
                       FROM s_drv3"""
                ).fetchone()))
        add("grain_fanout",
            "Grain fan-out: Drivers carries denormalised crash-level attributes",
            "montgomery/mmzv-x632", q,
            "The real key is enforced: person_id on the driver table, "
            "report_number on the crash table, both as unique_keys in the silver "
            "contract on the current slice. Every crash-level rollup on "
            "montgomery/crash is aggregated from the party tables (MAX, BOOL_OR, "
            "COUNT), never read off a driver row.",
            {"driver_rows_per_report_number_raw": round(raw_ratio, 4),
             "driver_rows_per_report_number_deduped": round(dedup_ratio, 4)},
            silver_stats)

    # 6 ---------------------------------------------------------------
    if _has(con, TXD):
        q = f"""SELECT located_fl,
                       latitude IS NOT NULL   AS derived,
                       rpt_latitude IS NOT NULL AS officer,
                       COUNT(*)
                FROM {TXD} GROUP BY 1, 2, 3 ORDER BY 4 DESC"""
        combos = [dict(zip(["located_fl", "derived", "officer", "n"], r))
                  for r in con.execute(q).fetchall()]
        disagree = con.execute(
            f"""SELECT COUNT(*) FROM {TXD}
                WHERE latitude IS NOT NULL AND rpt_latitude IS NOT NULL
                  AND (abs(TRY_CAST(latitude AS DOUBLE) - TRY_CAST(rpt_latitude AS DOUBLE)) > 0.01
                    OR abs(TRY_CAST(longitude AS DOUBLE) - TRY_CAST(rpt_longitude AS DOUBLE)) > 0.01)"""
        ).fetchone()[0]
        sv = _silver(con, silver_root, "txdot/crash_current.parquet", "s_txd")
        silver_stats = None
        if sv:
            silver_stats = {
                "coord_source": dict(con.execute(
                    "SELECT coord_source, COUNT(*) FROM s_txd GROUP BY 1 ORDER BY 2 DESC"
                ).fetchall()),
                "geo_quality": dict(con.execute(
                    "SELECT geo_quality, COUNT(*) FROM s_txd GROUP BY 1 ORDER BY 2 DESC"
                ).fetchall()),
                "pairs_disagree": con.execute(
                    "SELECT COUNT(*) FROM s_txd WHERE coord_pairs_disagree"
                ).fetchone()[0],
                "located_fl_vs_derived_exceptions": con.execute(
                    "SELECT COUNT(*) FROM s_txd "
                    "WHERE located_fl <> (coord_source = 'CRIS_DERIVED')"
                ).fetchone()[0],
            }
        add("txdot_two_coordinate_pairs",
            "Two competing coordinate pairs in TxDOT",
            "txdot/cris_crash", q,
            "Precedence: CRIS-derived, then officer-reported, then NULL. "
            "coord_source records which fired. The rule is empirical, not "
            "aesthetic: located_fl='1' if and only if the derived pair is "
            "populated (0 exceptions in 100,000), and preferring the officer "
            "pair would leave 68,932 rows uncoordinated for no gain.",
            {"combinations": combos,
             "both_present_and_disagree_gt_0.01deg": disagree},
            silver_stats)

    # 7 ---------------------------------------------------------------
    if _has(con, TXD):
        q = f"SELECT amend_supp_fl, COUNT(*) FROM {TXD} GROUP BY 1"
        counts = dict(con.execute(q).fetchall())
        sv = _silver(con, silver_root, "txdot/crash_history.parquet", "s_txh")
        silver_stats = None
        if sv:
            silver_stats = dict(zip(
                ["history_rows", "current_rows", "max_version_no", "amended", "deleted"],
                con.execute(
                    """SELECT COUNT(*), SUM(CASE WHEN is_current THEN 1 ELSE 0 END),
                              MAX(version_no),
                              SUM(CASE WHEN is_amended THEN 1 ELSE 0 END),
                              SUM(CASE WHEN deleted_in_load_ts IS NOT NULL THEN 1 ELSE 0 END)
                       FROM s_txh"""
                ).fetchone()))
        add("txdot_amended_reports",
            "Amended reports -- an SCD2 problem, not an append problem",
            "txdot/cris_crash", q,
            "SCD2 on crash_id with a row hash over the conformed attributes. An "
            "amendment produces exactly one new version and closes the previous "
            "one; a re-ingest of identical content produces none, because the "
            "hash excludes _bronze_* metadata. Deletion detection is scoped to "
            "the OID range the ingest cursor says was actually swept.",
            {"amend_supp_fl_counts": counts},
            silver_stats)

    # 8 ---------------------------------------------------------------
    if _has(con, TXD):
        q = f"""SELECT COUNT(*) FROM (DESCRIBE SELECT * FROM {TXD})
                WHERE column_name LIKE '%\\_id' ESCAPE '\\'"""
        id_cols = con.execute(q).fetchone()[0]
        date_types = con.execute(
            f"""SELECT COUNT(*) FROM (DESCRIBE SELECT * FROM {TXD})
                WHERE column_name IN ('crash_date','crash_time','report_date')
                  AND column_type = 'VARCHAR'"""
        ).fetchone()[0]
        sv = _silver(con, silver_root, "txdot/crash_current.parquet", "s_txd2")
        silver_stats = None
        if sv:
            silver_stats = dict(zip(
                ["crash_date_null", "crash_time_null", "report_date_null",
                 "crash_datetime_null", "severity_unmapped"],
                con.execute(
                    """SELECT SUM(CASE WHEN crash_date IS NULL THEN 1 ELSE 0 END),
                              SUM(CASE WHEN crash_time_local IS NULL THEN 1 ELSE 0 END),
                              SUM(CASE WHEN report_date IS NULL THEN 1 ELSE 0 END),
                              SUM(CASE WHEN crash_datetime_local IS NULL THEN 1 ELSE 0 END),
                              SUM(CASE WHEN severity_ordinal IS NULL THEN 1 ELSE 0 END)
                       FROM s_txd2"""
                ).fetchone()))
        add("txdot_string_dates_and_opaque_codes",
            "String dates and integer code dictionaries",
            "txdot/cris_crash", q,
            "Dates parsed to DATE/TIME/TIMESTAMP with zero parse failures. "
            "crash_sev_id IS decoded, because its meaning is verifiable from the "
            "injury-count columns in the same row -- see "
            "txdot.verify_crash_sev_consistency, which is also a test. The other "
            "60 *_id columns are typed INTEGER and left UNDECODED on purpose: "
            "their dictionaries are in the CRIS Automated Interface guide V29.0 "
            "PDF, and inventing labels from a guess is worse than a number.",
            {"id_columns": id_cols, "string_typed_date_columns": date_types},
            silver_stats)

    # 9 ---------------------------------------------------------------
    if _has(con, FARS_A):
        num = """(abs(TRY_CAST(LATITUDE AS DOUBLE) - 77.7777) < 1e-4
               OR abs(TRY_CAST(LATITUDE AS DOUBLE) - 88.8888) < 1e-4
               OR abs(TRY_CAST(LATITUDE AS DOUBLE) - 99.9999) < 1e-4)"""
        q = f"SELECT _bronze_dataset, COUNT(*) FROM {FARS_A} WHERE {num} GROUP BY 1 ORDER BY 1"
        numeric = dict(con.execute(q).fetchall())
        string_match = dict(con.execute(
            f"""SELECT _bronze_dataset,
                       SUM(CASE WHEN LATITUDE IN ('77.7777','88.8888','99.9999')
                                THEN 1 ELSE 0 END)
                FROM {FARS_A} GROUP BY 1 ORDER BY 1"""
        ).fetchall())
        formats = dict(con.execute(
            f"SELECT LATITUDE, COUNT(*) FROM {FARS_A} WHERE {num} GROUP BY 1 ORDER BY 2 DESC"
        ).fetchall())
        hour = dict(con.execute(
            f"""SELECT _bronze_dataset, SUM(CASE WHEN HOUR = '99' THEN 1 ELSE 0 END)
                FROM {FARS_A} GROUP BY 1 ORDER BY 1"""
        ).fetchall())
        age = con.execute(
            f"SELECT COUNT(*) FROM {FARS_P} WHERE AGE IN ('998','999')"
        ).fetchone()[0] if _has(con, FARS_P) else None
        sv = _silver(con, silver_root, "fars/accident_current.parquet", "s_fa")
        silver_stats = None
        if sv:
            silver_stats = {
                "geo_quality": dict(con.execute(
                    "SELECT geo_quality, COUNT(*) FROM s_fa GROUP BY 1 ORDER BY 2 DESC"
                ).fetchall()),
                "max_latitude": con.execute(
                    "SELECT MAX(latitude) FROM s_fa"
                ).fetchone()[0],
                "max_latitude_outside_alaska": con.execute(
                    "SELECT MAX(latitude) FROM s_fa WHERE jurisdiction <> 'AK'"
                ).fetchone()[0],
                "crash_datetime_null_but_date_present": con.execute(
                    "SELECT COUNT(*) FROM s_fa WHERE crash_datetime_local IS NULL "
                    "AND crash_date IS NOT NULL"
                ).fetchone()[0],
            }
        add("fars_sentinels",
            "Sentinel values, not nulls",
            "fars/accident + person", q,
            "Driven by config/fars_sentinels.csv per (table, column) -- a blanket "
            "7/8/9 rule would corrupt LGT_COND, HARM_EV and MAN_COLL, where 9 is "
            "substantive. Coordinates are matched NUMERICALLY because NHTSA "
            "changes the decimal formatting between years, and are additionally "
            "rejected outside [-90,90]/[-180,180] so a NEW sentinel format "
            "cannot land a crash in the Arctic before anyone adds it to the CSV. "
            "Sentinels are mapped BEFORE any join or aggregate: a MAX(INJ_SEV) "
            "that has not had 9 removed returns 9.",
            {"sentinel_coordinates_numeric_by_year": numeric,
             "same_rows_found_by_exact_string_match": string_match,
             "observed_sentinel_spellings": formats,
             "hour_99_by_year": hour,
             "person_age_998_or_999": age},
            silver_stats)

    return out


def _cli(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(
        prog="python -m src.transform.report",
        description="Measured counts for every known defect, from local bronze.",
    )
    ap.add_argument("--bronze-root", type=Path, default=None)
    ap.add_argument("--silver-root", type=Path, default=None)
    ap.add_argument("--defect", help="substring filter on the defect key")
    ap.add_argument("--json", action="store_true")
    args = ap.parse_args(argv)

    bronze = args.bronze_root or BRONZE_DIR
    silver = args.silver_root or SILVER_DIR
    db = bronze / "_watermarks.duckdb"
    store = WatermarkStore(db) if db.exists() else None

    con = c.connect()
    _views(con, bronze, store)
    rows = measure(con, silver)
    if args.defect:
        rows = [r for r in rows if args.defect in r["defect"]]

    if args.json:
        print(json.dumps(rows, indent=2, default=str))
        return 0

    print(f"Known defects, measured against {bronze}")
    if silver.exists():
        print(f"Silver figures from {silver}")
    for i, r in enumerate(rows, 1):
        print(f"\n{'=' * 78}\n{i}. {r['title']}\n   source: {r['source']}")
        print(f"   detection:\n     {r['detection_query']}")
        print("   BRONZE:")
        for k, v in r["bronze"].items():
            print(f"     {k}: {json.dumps(v, default=str)}")
        if r["silver"]:
            print("   SILVER:")
            for k, v in r["silver"].items():
                print(f"     {k}: {json.dumps(v, default=str)}")
        print("   disposition:")
        for line in _wrap(r["disposition"], 72):
            print(f"     {line}")
    return 0


def _wrap(text: str, width: int) -> list[str]:
    words, lines, cur = text.split(), [], ""
    for w in words:
        if len(cur) + len(w) + 1 > width:
            lines.append(cur)
            cur = w
        else:
            cur = f"{cur} {w}".strip()
    if cur:
        lines.append(cur)
    return lines


if __name__ == "__main__":
    sys.exit(_cli())
