"""The single severity ordinal, and the seed CSV that defines it.

Three sources, three injury scales, one ordinal:

    0  unknown / not reported
    1  no apparent injury          KABCO O
    2  possible injury             KABCO C
    3  suspected minor injury      KABCO B
    4  suspected serious injury    KABCO A
    5  fatal injury                KABCO K

The mapping lives in `config/severity_crosswalk.csv`, not in this file. A
crosswalk is a claim about what two vocabularies mean to each other; it wants
to be reviewable by someone who reads the CRIS guide and the FARS manual but
not Python, and it wants a `notes` column wide enough to say where it is lossy.
Code that hardcodes it makes both of those harder for no benefit.

Ordinal direction is ascending in severity, which is NOT the direction two of
the three sources use. TxDOT's `crash_sev_id` runs 5 = not injured, 4 = fatal:
ordering by the raw id sorts the scale backwards. FARS `INJ_SEV` is ascending
to 4 and then uses 5, 6, 9 for three different kinds of not-a-severity. Neither
raw code is safe to compare or MAX() directly, which is the entire reason this
table exists.

Grain differs too, and silver keeps that visible rather than papering over it:

    MONTGOMERY_MD  injury_severity   PERSON-level (drivers, non-motorists)
    NHTSA_FARS     INJ_SEV           PERSON-level
    TXDOT_CRIS     crash_sev_id      CRASH-level (already a max over persons)

So the Montgomery and FARS crash-level ordinal is `MAX(party ordinal)` and
TxDOT's is read straight off the crash. `severity_grain` on silver.crash records
which of those produced the value, so a downstream consumer cannot mistake a
max-over-parties for a source-published crash severity.

An unmapped source value RAISES. It does not silently become 0. A code that
appears in the data and not in the CSV is drift -- exactly the same event as a
new dictionary token -- and the correct response is to fail the build and make
somebody decide what it means. (`crash_sev_id = 95` is the live instance: it is
in the CSV, mapped to 0, with a note saying it is undocumented in CRIS V29.0.)
"""

from __future__ import annotations

import csv
import functools
from dataclasses import dataclass
from pathlib import Path

from ..config import CONFIG_DIR

CROSSWALK_PATH = CONFIG_DIR / "severity_crosswalk.csv"

# The sentinel the CSV uses for "the source column is SQL NULL here". A literal
# empty string in a CSV cell is indistinguishable from a missing cell, and
# Montgomery's injury_severity genuinely is NULL for 4,618 driver rows, so the
# null case needs a spelling that survives a round trip through a spreadsheet.
NULL_KEY = "__NULL__"

MIN_ORDINAL = 0
MAX_ORDINAL = 5

# Ordinal -> the label a human reads. Kept here rather than in the CSV because
# it is the definition of the ordinal itself, not a per-source mapping.
ORDINAL_LABELS: dict[int, str] = {
    0: "UNKNOWN",
    1: "NO_APPARENT_INJURY",
    2: "POSSIBLE_INJURY",
    3: "SUSPECTED_MINOR_INJURY",
    4: "SUSPECTED_SERIOUS_INJURY",
    5: "FATAL_INJURY",
}


class UnmappedSeverity(KeyError):
    """A source severity value with no row in the crosswalk.

    Raised, never defaulted. See the module docstring.
    """


@dataclass(frozen=True)
class SeverityMapping:
    source_system: str
    source_column: str
    source_value: str
    kabco: str | None
    severity_ordinal: int
    notes: str

    @property
    def label(self) -> str:
        return ORDINAL_LABELS[self.severity_ordinal]


@functools.cache
def _load(path: str | None = None) -> tuple[SeverityMapping, ...]:
    p = Path(path) if path else CROSSWALK_PATH
    out: list[SeverityMapping] = []
    with p.open(newline="", encoding="utf-8") as fh:
        for row in csv.DictReader(fh):
            if not row.get("source_system"):
                continue
            ordinal = int(row["severity_ordinal"])
            if not MIN_ORDINAL <= ordinal <= MAX_ORDINAL:
                raise ValueError(
                    f"{p}: severity_ordinal {ordinal} outside "
                    f"[{MIN_ORDINAL},{MAX_ORDINAL}] for "
                    f"{row['source_system']}.{row['source_column']}="
                    f"{row['source_value']}"
                )
            out.append(
                SeverityMapping(
                    source_system=row["source_system"].strip(),
                    source_column=row["source_column"].strip(),
                    source_value=row["source_value"],
                    kabco=(row["kabco"].strip() or None),
                    severity_ordinal=ordinal,
                    notes=row["notes"],
                )
            )
    if not out:
        raise ValueError(f"{p}: crosswalk is empty")
    return tuple(out)


def mappings(path: str | None = None) -> tuple[SeverityMapping, ...]:
    """Every row of the crosswalk."""
    return _load(path)


@functools.cache
def _index(path: str | None = None) -> dict[tuple[str, str, str], SeverityMapping]:
    idx: dict[tuple[str, str, str], SeverityMapping] = {}
    for m in _load(path):
        key = (m.source_system, m.source_column, m.source_value)
        if key in idx:
            raise ValueError(f"duplicate crosswalk row for {key}")
        idx[key] = m
    return idx


def to_ordinal(
    source_system: str, source_column: str, source_value: str | None, *, path: str | None = None
) -> int:
    """Map one source severity value to the ordinal, or raise.

    `None` is looked up as NULL_KEY, so a source that publishes SQL NULL gets a
    reviewed decision from the CSV rather than an implicit one from this code.
    """
    key = (source_system, source_column, NULL_KEY if source_value is None else source_value)
    try:
        return _index(path)[key].severity_ordinal
    except KeyError:
        raise UnmappedSeverity(
            f"no severity crosswalk row for {source_system}.{source_column}="
            f"{source_value!r}. This is drift: add a row to "
            f"{CROSSWALK_PATH.name} with a decision and a note, or fix upstream."
        ) from None


def lookup(
    source_system: str, source_column: str, source_value: str | None, *, path: str | None = None
) -> SeverityMapping:
    key = (source_system, source_column, NULL_KEY if source_value is None else source_value)
    try:
        return _index(path)[key]
    except KeyError:
        raise UnmappedSeverity(
            f"no severity crosswalk row for {source_system}.{source_column}={source_value!r}"
        ) from None


def register(con, *, table: str = "severity_crosswalk", path: str | None = None) -> str:
    """Materialise the crosswalk as a DuckDB table and return its name.

    The transforms JOIN against this rather than calling `to_ordinal()` per row:
    a 220,000-row Python round trip to answer a 29-row lookup is the kind of
    pandas-shaped mistake this pipeline is meant not to make. The LEFT JOIN also
    makes the unmapped case a *set* -- `WHERE severity_ordinal IS NULL` names
    every offending value at once -- instead of an exception on the first one.
    """
    con.execute(
        f"""
        CREATE OR REPLACE TABLE {table} (
            source_system    VARCHAR NOT NULL,
            source_column    VARCHAR NOT NULL,
            source_value     VARCHAR NOT NULL,
            kabco            VARCHAR,
            severity_ordinal INTEGER NOT NULL,
            severity_label   VARCHAR NOT NULL,
            notes            VARCHAR
        )
        """
    )
    con.executemany(
        f"INSERT INTO {table} VALUES (?,?,?,?,?,?,?)",
        [
            (
                m.source_system,
                m.source_column,
                m.source_value,
                m.kabco,
                m.severity_ordinal,
                m.label,
                m.notes,
            )
            for m in _load(path)
        ],
    )
    return table


def assert_all_mapped(con, *, table: str, source_system: str, source_column: str,
                      value_expr: str, crosswalk: str = "severity_crosswalk") -> None:
    """Raise naming every value in `table.value_expr` the crosswalk misses.

    `value_expr` is SQL, so a caller can pass `coalesce(injury_severity,
    '__NULL__')` and have the null case checked too.
    """
    rows = con.execute(
        f"""
        SELECT v, COUNT(*) AS n FROM (SELECT {value_expr} AS v FROM {table}) t
        WHERE NOT EXISTS (
            SELECT 1 FROM {crosswalk} c
            WHERE c.source_system = ? AND c.source_column = ? AND c.source_value = t.v
        )
        GROUP BY 1 ORDER BY 2 DESC
        """,
        [source_system, source_column],
    ).fetchall()
    if rows:
        detail = ", ".join(f"{v!r} ({n} rows)" for v, n in rows)
        raise UnmappedSeverity(
            f"{source_system}.{source_column}: {len(rows)} value(s) missing from "
            f"{CROSSWALK_PATH.name}: {detail}"
        )
