"""Build the committed bronze test extracts from local bronze.

    python -m tests.fixtures.make_extracts
    python -m tests.fixtures.make_extracts --bronze-root data/bronze --out tests/fixtures/bronze

Run once, output committed. Re-run it only to widen coverage; the extracts are
the fixtures and regenerating them casually would make the test suite's
expectations move underneath it.

The extracts are REAL bronze rows -- same all-string parquet, same `_bronze_*`
columns, same `{source}/{dataset}/{load_ts}/page_*.parquet` tree -- sampled to
exhibit each defect. That matters: a synthetic fixture tests the transform
against my idea of the data, and the whole point of this exercise is that my
idea of the data was wrong in several places.

These are public crash records. There is no PII in them: Montgomery publishes
`person_id` as an opaque GUID with no name, address, licence number or DOB
anywhere in the three datasets; TxDOT's `investigator_narrative` is empty in
this slice and is dropped here regardless; FARS is a de-identified national
census. A few hundred rows per table keeps the repo small and the tests fast.

Two rows are SYNTHESISED rather than sampled, and they are marked as such below,
because the defect they exercise does not exist in a single-partition local
bronze:

  * a TxDOT amendment -- the same crash_id in two partitions with a changed
    attribute. There is only one TxDOT sweep locally, so restatement has never
    happened here. Synthesising it is the only way to test the SCD2 path that
    matters most.
  * a FARS reissue -- a second partition for one year in which one ST_CASE is
    revised and one is removed.

Both are built by copying real rows and changing one field, so every other
column keeps its real shape.
"""

from __future__ import annotations

import argparse
import json
import shutil
import sys
from pathlib import Path

import duckdb

REPO = Path(__file__).resolve().parents[2]
DEFAULT_BRONZE = REPO / "data" / "bronze"
DEFAULT_OUT = REPO / "tests" / "fixtures" / "bronze"

# Two Montgomery partitions, so the same :id appears twice -- the overlapping
# read the real bronze has and the SCD2 dedupe path needs.
MOCO_P1 = "20260101T000000000Z"
MOCO_P2 = "20260102T000000000Z"
TXD_P1 = "20260101T000000000Z"
TXD_P2 = "20260102T000000000Z"   # synthetic: carries the amendment
FARS_P1 = "20260101T000000000Z"
FARS_P2 = "20260102T000000000Z"  # synthetic: carries the reissue

FARS_YEARS = ("2019", "2024")    # oldest and newest: both sentinel formats


def _con(bronze: Path) -> duckdb.DuckDBPyConnection:
    con = duckdb.connect()
    con.execute("SET preserve_insertion_order=true")
    for name, source, dataset in (
        ("inc", "montgomery", "bhju-22kf"),
        ("drv", "montgomery", "mmzv-x632"),
        ("nmo", "montgomery", "n7fk-dce5"),
        ("txd", "txdot", "cris_crash"),
    ):
        glob = str(bronze / source / dataset / "*" / "*.parquet")
        con.execute(
            f"CREATE OR REPLACE VIEW {name} AS SELECT * FROM "
            f"read_parquet('{glob}', union_by_name=true)"
        )
    for year in FARS_YEARS:
        for member in ("accident", "vehicle", "person"):
            glob = str(bronze / "fars" / year / "*" / f"{member}.parquet")
            con.execute(
                f"CREATE OR REPLACE VIEW fars_{member}_{year} AS SELECT * FROM "
                f"read_parquet('{glob}', union_by_name=true)"
            )
    return con


def _write(con, sql: str, dest: Path, *, load_ts: str) -> int:
    """Write one page, with `_bronze_load_ts` rewritten to the fixture partition."""
    dest.parent.mkdir(parents=True, exist_ok=True)
    con.execute(
        f"CREATE OR REPLACE TEMP TABLE _page AS "
        f"SELECT * REPLACE ('{load_ts}' AS _bronze_load_ts) FROM ({sql})"
    )
    n = con.execute("SELECT COUNT(*) FROM _page").fetchone()[0]
    con.execute(
        f"COPY (SELECT * FROM _page) TO '{dest}' (FORMAT PARQUET, COMPRESSION ZSTD)"
    )
    return n


def build(bronze: Path, out: Path) -> dict[str, int]:
    con = _con(bronze)
    if out.exists():
        shutil.rmtree(out)
    counts: dict[str, int] = {}

    # ---------------------------------------------------------------
    # Montgomery incidents. The union of every defect class, so the
    # fixture reproduces each one rather than merely being small.
    # ---------------------------------------------------------------
    con.execute(
        """
        CREATE OR REPLACE TEMP TABLE inc_pick AS
        WITH d AS (SELECT DISTINCT ON (":id") * FROM inc ORDER BY ":id"),
        -- every out-of-envelope crash: 105 rows, the whole defect population
        outside AS (
            SELECT ":id" AS id FROM d
            WHERE TRY_CAST(latitude AS DOUBLE) NOT BETWEEN 38.88 AND 39.38
               OR TRY_CAST(longitude AS DOUBLE) NOT BETWEEN -77.56 AND -76.85
        ),
        -- crashes with no driver row at all (the 785), a sample
        orphan AS (
            SELECT ":id" AS id FROM d
            WHERE NOT EXISTS (SELECT 1 FROM drv WHERE drv.report_number = d.report_number)
            ORDER BY ":id" LIMIT 40
        ),
        -- one crash per distinct crash-level substance concatenation: this is
        -- what makes the grammar test meaningful, including the mixed-generation
        -- strings and the 3+ driver joins
        joins AS (
            SELECT DISTINCT ON (driver_substance_abuse) ":id" AS id FROM d
            ORDER BY driver_substance_abuse, ":id"
        ),
        -- crashes inside the dictionary overlap window
        cutover AS (
            SELECT ":id" AS id FROM d
            WHERE substr(crash_date_time, 1, 10) BETWEEN '2023-12-26' AND '2024-01-05'
            ORDER BY ":id" LIMIT 60
        ),
        -- multi-driver crashes, both generations
        multi AS (
            SELECT ":id" AS id FROM d WHERE d.report_number IN (
                SELECT report_number FROM (SELECT DISTINCT ":id" i, report_number FROM drv)
                GROUP BY 1 HAVING COUNT(*) >= 3 ORDER BY 1 LIMIT 15)
        ),
        -- comma-joined number_of_lanes ("2, 3")
        lanes AS (
            SELECT ":id" AS id FROM d
            WHERE number_of_lanes IS NOT NULL
              AND TRY_CAST(number_of_lanes AS INTEGER) IS NULL
            ORDER BY ":id" LIMIT 10
        ),
        -- one of each report type, for the severity crosswalk
        types AS (
            SELECT DISTINCT ON (acrs_report_type) ":id" AS id FROM d
            ORDER BY acrs_report_type, ":id"
        )
        SELECT DISTINCT id FROM (
            SELECT id FROM outside UNION ALL SELECT id FROM orphan
            UNION ALL SELECT id FROM joins UNION ALL SELECT id FROM cutover
            UNION ALL SELECT id FROM multi UNION ALL SELECT id FROM lanes
            UNION ALL SELECT id FROM types)
        """
    )
    con.execute(
        """CREATE OR REPLACE TEMP TABLE inc_rows AS
           SELECT DISTINCT ON (":id") * FROM inc
           WHERE ":id" IN (SELECT id FROM inc_pick) ORDER BY ":id" """
    )

    # Parties for exactly those crashes, so the anti-join numbers in the fixture
    # are a property of the fixture rather than an artefact of sampling.
    con.execute(
        """CREATE OR REPLACE TEMP TABLE drv_rows AS
           SELECT DISTINCT ON (":id") * FROM drv
           WHERE report_number IN (SELECT report_number FROM inc_rows)
           ORDER BY ":id" """
    )
    con.execute(
        """CREATE OR REPLACE TEMP TABLE nmo_rows AS
           SELECT DISTINCT ON (":id") * FROM nmo
           WHERE report_number IN (SELECT report_number FROM inc_rows)
           ORDER BY ":id" """
    )
    # Plus the duplicate-person_id non-motorists, wherever their crash landed.
    con.execute(
        """INSERT INTO nmo_rows
           SELECT DISTINCT ON (":id") * FROM nmo WHERE person_id IN (
               SELECT person_id FROM nmo GROUP BY 1 HAVING COUNT(DISTINCT ":id") > 1)
             AND ":id" NOT IN (SELECT ":id" FROM nmo_rows) ORDER BY ":id" """
    )
    con.execute(
        """INSERT INTO inc_rows
           SELECT DISTINCT ON (":id") * FROM inc
           WHERE report_number IN (SELECT report_number FROM nmo_rows)
             AND ":id" NOT IN (SELECT ":id" FROM inc_rows) ORDER BY ":id" """
    )
    con.execute(
        """INSERT INTO drv_rows
           SELECT DISTINCT ON (":id") * FROM drv
           WHERE report_number IN (SELECT report_number FROM nmo_rows)
             AND ":id" NOT IN (SELECT ":id" FROM drv_rows) ORDER BY ":id" """
    )

    moco = out / "montgomery"
    for dataset, table in (("bhju-22kf", "inc_rows"), ("mmzv-x632", "drv_rows"),
                           ("n7fk-dce5", "nmo_rows")):
        # Partition 1 is a partial earlier read; partition 2 is the full one --
        # exactly the overlap the real bronze has, so the dedupe path is
        # exercised and `same :id in two partitions` is true by construction.
        counts[f"montgomery/{dataset}/{MOCO_P1}"] = _write(
            con, f'SELECT * FROM {table} WHERE hash(":id") % 3 = 0',
            moco / dataset / MOCO_P1 / "page_00001.parquet", load_ts=MOCO_P1)
        counts[f"montgomery/{dataset}/{MOCO_P2}"] = _write(
            con, f"SELECT * FROM {table}",
            moco / dataset / MOCO_P2 / "page_00001.parquet", load_ts=MOCO_P2)

    # ---------------------------------------------------------------
    # TxDOT. Every coordinate combination, both amend_supp_fl values,
    # every crash_sev_id including the undocumented 95.
    # ---------------------------------------------------------------
    con.execute(
        """
        CREATE OR REPLACE TEMP TABLE txd_rows AS
        WITH combos AS (
            SELECT crash_id FROM (
                SELECT crash_id, row_number() OVER (
                    PARTITION BY located_fl, latitude IS NOT NULL,
                                 rpt_latitude IS NOT NULL
                    ORDER BY crash_id) rn
                FROM txd) WHERE rn <= 25
        ),
        sev AS (
            SELECT crash_id FROM (
                SELECT crash_id, row_number() OVER (
                    PARTITION BY crash_sev_id ORDER BY crash_id) rn FROM txd)
            WHERE rn <= 10
        ),
        amended AS (
            SELECT crash_id FROM (
                SELECT crash_id, row_number() OVER (
                    PARTITION BY amend_supp_fl ORDER BY crash_id) rn FROM txd)
            WHERE rn <= 15
        ),
        disagree AS (
            SELECT crash_id FROM txd
            WHERE latitude IS NOT NULL AND rpt_latitude IS NOT NULL
              AND (abs(TRY_CAST(latitude AS DOUBLE)
                       - TRY_CAST(rpt_latitude AS DOUBLE)) > 0.01
                OR abs(TRY_CAST(longitude AS DOUBLE)
                       - TRY_CAST(rpt_longitude AS DOUBLE)) > 0.01)
            ORDER BY crash_id LIMIT 20
        )
        SELECT * EXCLUDE (investigator_narrative) FROM txd WHERE crash_id IN (
            SELECT crash_id FROM combos UNION SELECT crash_id FROM sev
            UNION SELECT crash_id FROM amended UNION SELECT crash_id FROM disagree)
        """
    )
    txd = out / "txdot" / "cris_crash"
    counts[f"txdot/cris_crash/{TXD_P1}"] = _write(
        con, "SELECT * FROM txd_rows ORDER BY crash_id",
        txd / TXD_P1 / "page_00001.parquet", load_ts=TXD_P1)

    # SYNTHETIC. Partition 2 is a re-sweep of the same OID range in which
    # exactly one crash has been amended: crash_speed_limit changed and
    # amend_supp_fl flipped to '1'. Everything else is byte-for-byte the
    # partition-1 row. No TxDOT restatement has happened in local bronze, so
    # this is the only way to exercise the path the assignment cares most about.
    amended_id = con.execute(
        "SELECT crash_id FROM txd_rows WHERE amend_supp_fl = '0' "
        "AND crash_speed_limit IS NOT NULL ORDER BY crash_id LIMIT 1"
    ).fetchone()[0]
    counts[f"txdot/cris_crash/{TXD_P2}"] = _write(
        con,
        f"""SELECT * REPLACE (
              CASE WHEN crash_id = '{amended_id}' THEN '1' ELSE amend_supp_fl END
                   AS amend_supp_fl,
              CASE WHEN crash_id = '{amended_id}'
                   THEN CAST(TRY_CAST(crash_speed_limit AS INTEGER) + 5 AS VARCHAR)
                   ELSE crash_speed_limit END AS crash_speed_limit)
            FROM txd_rows ORDER BY crash_id""",
        txd / TXD_P2 / "page_00001.parquet", load_ts=TXD_P2)

    # ---------------------------------------------------------------
    # FARS. 2019 and 2024 for both sentinel formats.
    # ---------------------------------------------------------------
    for year in FARS_YEARS:
        con.execute(
            f"""
            CREATE OR REPLACE TEMP TABLE fars_pick_{year} AS
            WITH sentinel AS (
                SELECT ST_CASE FROM fars_accident_{year}
                WHERE abs(TRY_CAST(LATITUDE AS DOUBLE) - 77.7777) < 1e-4
                   OR abs(TRY_CAST(LATITUDE AS DOUBLE) - 88.8888) < 1e-4
                   OR abs(TRY_CAST(LATITUDE AS DOUBLE) - 99.9999) < 1e-4
                ORDER BY ST_CASE LIMIT 25
            ),
            unknown_hour AS (
                SELECT ST_CASE FROM fars_accident_{year} WHERE HOUR = '99'
                ORDER BY ST_CASE LIMIT 15
            ),
            in_scope AS (
                SELECT ST_CASE FROM fars_accident_{year}
                WHERE STATE IN ('24', '48', '12') ORDER BY ST_CASE LIMIT 40
            ),
            normal AS (
                SELECT ST_CASE FROM fars_accident_{year} ORDER BY ST_CASE LIMIT 25
            )
            SELECT DISTINCT ST_CASE FROM (
                SELECT ST_CASE FROM sentinel UNION ALL SELECT ST_CASE FROM unknown_hour
                UNION ALL SELECT ST_CASE FROM in_scope UNION ALL SELECT ST_CASE FROM normal)
            """
        )
        base = out / "fars" / year
        for member in ("accident", "vehicle", "person"):
            counts[f"fars/{year}/{FARS_P1}/{member}"] = _write(
                con,
                f"""SELECT * FROM fars_{member}_{year}
                    WHERE ST_CASE IN (SELECT ST_CASE FROM fars_pick_{year})
                    ORDER BY ST_CASE""",
                base / FARS_P1 / f"{member}.parquet", load_ts=FARS_P1)

    # SYNTHETIC. A 2019 reissue: one ST_CASE has its FATALS revised, one is
    # removed entirely, the rest are unchanged. This is the FARS restatement
    # the assignment describes ("prior-year files are silently revised in
    # place") and no second partition exists locally to exercise it.
    revised, removed = [
        r[0] for r in con.execute(
            "SELECT ST_CASE FROM fars_pick_2019 ORDER BY ST_CASE LIMIT 2"
        ).fetchall()
    ]
    base = out / "fars" / "2019"
    for member in ("accident", "vehicle", "person"):
        if member == "accident":
            sql = f"""SELECT * REPLACE (
                        CASE WHEN ST_CASE = '{revised}'
                             THEN CAST(TRY_CAST(FATALS AS INTEGER) + 1 AS VARCHAR)
                             ELSE FATALS END AS FATALS)
                      FROM fars_accident_2019
                      WHERE ST_CASE IN (SELECT ST_CASE FROM fars_pick_2019)
                        AND ST_CASE <> '{removed}' ORDER BY ST_CASE"""
        else:
            sql = f"""SELECT * FROM fars_{member}_2019
                      WHERE ST_CASE IN (SELECT ST_CASE FROM fars_pick_2019)
                        AND ST_CASE <> '{removed}' ORDER BY ST_CASE"""
        counts[f"fars/2019/{FARS_P2}/{member}"] = _write(
            con, sql, base / FARS_P2 / f"{member}.parquet", load_ts=FARS_P2)

    manifest = {
        "generated_by": "python -m tests.fixtures.make_extracts",
        "source_bronze": str(bronze),
        "partitions": {
            "montgomery": [MOCO_P1, MOCO_P2],
            "txdot": [TXD_P1, TXD_P2],
            "fars": {y: ([FARS_P1, FARS_P2] if y == "2019" else [FARS_P1])
                     for y in FARS_YEARS},
        },
        "synthetic": {
            "txdot_amended_crash_id": amended_id,
            "fars_2019_revised_st_case": revised,
            "fars_2019_removed_st_case": removed,
        },
        "row_counts": counts,
    }
    (out / "MANIFEST.json").write_text(json.dumps(manifest, indent=2) + "\n")
    con.close()
    return counts


def _cli(argv=None) -> int:
    ap = argparse.ArgumentParser(prog="python -m tests.fixtures.make_extracts")
    ap.add_argument("--bronze-root", type=Path, default=DEFAULT_BRONZE)
    ap.add_argument("--out", type=Path, default=DEFAULT_OUT)
    args = ap.parse_args(argv)
    if not args.bronze_root.exists():
        print(f"no bronze at {args.bronze_root}", file=sys.stderr)
        return 2
    counts = build(args.bronze_root, args.out)
    total = sum(counts.values())
    for k, v in sorted(counts.items()):
        print(f"  {k:<50} {v:>6}")
    print(f"  {'TOTAL':<50} {total:>6}")
    return 0


if __name__ == "__main__":
    sys.exit(_cli())
