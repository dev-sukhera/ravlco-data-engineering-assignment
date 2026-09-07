"""silver.crash -- one row per crash, per source, across all three sources.

Thin by design. This is a CONFORMED GRAIN, not an entity-resolved fact: it
answers "how many distinct crashes has this pipeline seen, and where and how bad
were they" with one row per (source, source record). It does NOT attempt to
decide that FARS accident 48/123456 and TxDOT crash 19876543 are the same
event -- that is cross-source entity resolution and it is Phase 3's job, on top
of this table rather than inside it.

Keeping the two apart matters for a reason the assignment names directly: FARS
is a national fatality census, TxDOT is Texas-wide all-severity and Montgomery
is county-wide all-severity, so a Texas fatality genuinely appears twice.
Resolving it here would make the row count depend on the match rule, and every
downstream count would silently move whenever the rule was tuned. Here the
count is a fact about the sources; there it becomes a claim about identity.

`crash_uid` is deterministic -- `source_system + ':' + source_record_id` -- and
never random. A UUID would break the byte-identity guarantee on the first
rebuild and would make a lead's id unstable across runs, which for a record that
carries an eligibility decision is a compliance problem, not just an annoyance.

Columns match `contracts/lead_output.schema.json` where they overlap:
`source_system` is that contract's enum, `jurisdiction` is its two-letter
pattern, `severity_ordinal` its 0-5 integer.
"""

from __future__ import annotations

from typing import Any

from . import common as c

TABLE = "crash_current"

COLUMNS = [
    "crash_uid",
    "source_system",
    "source_record_id",
    "jurisdiction",
    "crash_date",
    "crash_datetime_local",
    "latitude",
    "longitude",
    "geo_quality",
    "severity_ordinal",
    "severity_source_value",
    "severity_grain",
    "is_amended",
    "version_no",
    "_bronze_load_ts",
]

# Which source tables feed it. A source absent from this build contributes
# nothing rather than failing: `--source txdot` must produce a usable
# crash_current for TxDOT alone.
FEEDS = {
    "montgomery": "moco_crash_final",
    "txdot": "txd_crash_history",
    "fars": "fars_accident_history",
}


def _montgomery_sql(relation: str) -> str:
    return f"""
    SELECT
        'MONTGOMERY_MD:' || report_number      AS crash_uid,
        'MONTGOMERY_MD'                        AS source_system,
        report_number                          AS source_record_id,
        'MD'                                   AS jurisdiction,
        crash_date,
        crash_datetime_local,
        latitude, longitude, geo_quality,
        severity_ordinal,
        severity_source_value,
        severity_grain,
        -- Montgomery publishes no amendment flag. NULL means "this source does
        -- not say", which is the honest value; FALSE would assert that no crash
        -- report here was ever amended, and the county's :updated_at history
        -- shows plenty were.
        CAST(NULL AS BOOLEAN)                  AS is_amended,
        version_no,
        _bronze_load_ts
    FROM {relation} WHERE is_current
    """


def _txdot_sql(relation: str) -> str:
    return f"""
    SELECT
        'TXDOT_CRIS:' || crash_id              AS crash_uid,
        'TXDOT_CRIS'                           AS source_system,
        crash_id                               AS source_record_id,
        'TX'                                   AS jurisdiction,
        crash_date,
        crash_datetime_local,
        latitude, longitude, geo_quality,
        severity_ordinal,
        CAST(crash_sev_id AS VARCHAR)          AS severity_source_value,
        -- CRIS publishes crash_sev_id at the CRASH grain already -- it is
        -- TxDOT's own max over the crash's persons, not ours.
        'SOURCE_CRASH_LEVEL'                   AS severity_grain,
        is_amended,
        version_no,
        _bronze_load_ts
    FROM {relation} WHERE is_current
    """


def _fars_sql(accident: str, severity: str) -> str:
    return f"""
    SELECT
        'NHTSA_FARS:' || a.year || '-' || a.st_case  AS crash_uid,
        'NHTSA_FARS'                                 AS source_system,
        a.year || '-' || a.st_case                   AS source_record_id,
        a.jurisdiction,
        a.crash_date,
        a.crash_datetime_local,
        a.latitude, a.longitude, a.geo_quality,
        -- FARS is a fatality census, so the crash ordinal is 5 by definition.
        -- The max over the person file is computed anyway and any disagreement
        -- is reported (measured: 0 accidents of 222,695 lack a person with
        -- INJ_SEV=4). Taking the person max INSTEAD would be wrong in the other
        -- direction: an accident whose only fatality has no person row would
        -- silently stop being fatal.
        5                                            AS severity_ordinal,
        CAST(coalesce(s.max_person_ordinal, 0) AS VARCHAR) AS severity_source_value,
        'FATALITY_CENSUS'                            AS severity_grain,
        -- FARS restates whole years rather than flagging amended records, so
        -- there is no per-record amendment flag to carry. version_no > 1 is the
        -- equivalent signal and it is already a column.
        CAST(NULL AS BOOLEAN)                        AS is_amended,
        a.version_no,
        a._bronze_load_ts
    FROM {accident} a
    LEFT JOIN {severity} s ON s.year = a.year AND s.st_case = a.st_case
    WHERE a.is_current
    """


def build(ctx: c.BuildContext, sources: list[str]) -> dict[str, Any]:
    con = ctx.con
    selects: list[str] = []
    if "montgomery" in sources:
        selects.append(_montgomery_sql(FEEDS["montgomery"]))
    if "txdot" in sources:
        selects.append(_txdot_sql(FEEDS["txdot"]))
    if "fars" in sources:
        selects.append(_fars_sql(FEEDS["fars"], "fars_accident_severity"))
    if not selects:
        raise ValueError("unified.build called with no sources")

    con.execute(
        "CREATE OR REPLACE TABLE silver_crash AS "
        + "\nUNION ALL BY NAME\n".join(f"({s})" for s in selects)
    )

    stats: dict[str, Any] = {}
    stats["rows"] = con.execute("SELECT COUNT(*) FROM silver_crash").fetchone()[0]
    stats["distinct_crash_uid"] = con.execute(
        "SELECT COUNT(DISTINCT crash_uid) FROM silver_crash"
    ).fetchone()[0]
    stats["by_source"] = dict(
        con.execute(
            "SELECT source_system, COUNT(*) FROM silver_crash GROUP BY 1 ORDER BY 1"
        ).fetchall()
    )
    stats["by_jurisdiction"] = dict(
        con.execute(
            "SELECT jurisdiction, COUNT(*) FROM silver_crash GROUP BY 1 ORDER BY 2 DESC"
        ).fetchall()
    )
    stats["severity_ordinal_distribution"] = dict(
        con.execute(
            "SELECT severity_ordinal, COUNT(*) FROM silver_crash GROUP BY 1 ORDER BY 1"
        ).fetchall()
    )
    stats["geo_quality_distribution"] = dict(
        con.execute(
            "SELECT geo_quality, COUNT(*) FROM silver_crash GROUP BY 1 ORDER BY 2 DESC"
        ).fetchall()
    )
    return stats
