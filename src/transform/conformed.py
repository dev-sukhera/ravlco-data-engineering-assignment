"""Conformed vocabularies and the seed-CSV crosswalks that feed them.

Three dimensions -- road class, officer-reported weather, non-motorist type --
have the same shape as the severity crosswalk: each source speaks its own
dialect (Montgomery two generations of strings, FARS integer codes with labels
in the codebook, TxDOT integer codes with no labels at all), and gold needs one
member per concept. So they share one loader, one DuckDB registration and one
drift check, and differ only in the vocabulary and the CSV path.

Why the vocabulary is code and the mapping is data
--------------------------------------------------
The set of conformed members is a MODELLING decision: it fixes the surrogate
keys, the column order of the dim, and what a downstream analyst may GROUP BY.
It changes when the model changes, which is a code review. The mapping from a
source value to a member is an OBSERVATION about a source: it changes when a
source publishes a new token, which is drift, and drift is handled by adding a
CSV row with a note -- never by editing Python (Phase 2's severity crosswalk
established the pattern; this module generalises it).

Surrogate keys for these dimensions are the member's position in the vocabulary
tuple, and UNKNOWN is always -1. Position, not insertion order, so the key is a
function of this file and nothing else -- adding a member appends; reordering is
a breaking change and the contract's pinned key test catches it.

Lossiness is a property of the mapping and lives in the CSV `notes` column, one
sentence per row where it applies. The report aggregates those rows so the
memo's "where the mapping is lossy" answer is generated from the same file the
build reads, not written separately and left to drift.
"""

from __future__ import annotations

import csv
import functools
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from ..config import CONFIG_DIR

NULL_KEY = "__NULL__"
UNKNOWN_SK = -1


class UnmappedValue(KeyError):
    """A source value with no crosswalk row. Drift, and the build fails on it."""


@dataclass(frozen=True)
class Member:
    code: str
    label: str
    attrs: dict[str, Any]

    def __init__(self, code: str, label: str, **attrs: Any):
        object.__setattr__(self, "code", code)
        object.__setattr__(self, "label", label)
        object.__setattr__(self, "attrs", dict(attrs))


@dataclass(frozen=True)
class Vocabulary:
    """One conformed dimension: its members in key order and its seed CSV."""

    name: str                      # dim_<name>, <name>_sk
    csv_path: Path
    members: tuple[Member, ...]    # UNKNOWN is added by `rows()`; do not list it
    attr_columns: tuple[str, ...]  # extra dim attributes carried from Member.attrs

    def sk(self, code: str) -> int:
        if code == "UNKNOWN":
            return UNKNOWN_SK
        for i, m in enumerate(self.members):
            if m.code == code:
                return i + 1
        raise KeyError(f"{self.name}: no member {code!r}")

    def codes(self) -> frozenset[str]:
        return frozenset(m.code for m in self.members) | {"UNKNOWN"}

    def rows(self) -> list[tuple[Any, ...]]:
        """(sk, code, label, *attrs) for every member including UNKNOWN at -1."""
        unknown_attrs = tuple(None if c not in ("functional_class_known",) else False
                              for c in self.attr_columns)
        out = [(UNKNOWN_SK, "UNKNOWN", "Unknown / not reported", *unknown_attrs)]
        for i, m in enumerate(self.members):
            out.append((i + 1, m.code, m.label,
                        *(m.attrs.get(c) for c in self.attr_columns)))
        return out


# ---------------------------------------------------------------------------
# the three vocabularies
# ---------------------------------------------------------------------------

ROAD_CLASS = Vocabulary(
    name="road_class",
    csv_path=CONFIG_DIR / "road_class_crosswalk.csv",
    attr_columns=("functional_class_known", "fhwa_class"),
    members=(
        # FHWA functional classes first, in FHWA order, because FARS FUNC_SYS
        # already speaks this scale and it is the only one of the three sources
        # that publishes function rather than ownership.
        Member("INTERSTATE", "Interstate", functional_class_known=True, fhwa_class=1),
        Member("FREEWAY_EXPRESSWAY", "Other freeway or expressway",
               functional_class_known=True, fhwa_class=2),
        Member("PRINCIPAL_ARTERIAL", "Other principal arterial",
               functional_class_known=True, fhwa_class=3),
        Member("MINOR_ARTERIAL", "Minor arterial", functional_class_known=True, fhwa_class=4),
        Member("MAJOR_COLLECTOR", "Major collector", functional_class_known=True, fhwa_class=5),
        Member("MINOR_COLLECTOR", "Minor collector", functional_class_known=True, fhwa_class=6),
        Member("LOCAL", "Local road", functional_class_known=True, fhwa_class=7),
        Member("RAMP", "Ramp", functional_class_known=False, fhwa_class=None),
        Member("PRIVATE_OR_NOT_IN_INVENTORY", "Private or not in state inventory",
               functional_class_known=False, fhwa_class=None),
        # Bridging members for sources that publish OWNERSHIP, not function.
        # Montgomery says "Maryland (State)" and "County"; that is who maintains
        # the road, and a county road can be a six-lane arterial. Forcing those
        # into an FHWA class would be inventing data; leaving them UNKNOWN would
        # throw away the one thing the source does say. So they get their own
        # members, flagged functional_class_known = false, and the memo's
        # answer to "which roads" for Maryland is honest about the resolution.
        Member("STATE_HIGHWAY_UNCLASSIFIED", "State-system highway, functional class not published",
               functional_class_known=False, fhwa_class=None),
        Member("COUNTY_MUNICIPAL_UNCLASSIFIED", "County or municipal road, functional class not published",
               functional_class_known=False, fhwa_class=None),
        Member("OTHER", "Other roadway", functional_class_known=False, fhwa_class=None),
    ),
)

WEATHER = Vocabulary(
    name="weather_condition",
    csv_path=CONFIG_DIR / "weather_crosswalk.csv",
    attr_columns=("is_precipitation", "is_adverse"),
    members=(
        Member("CLEAR", "Clear", is_precipitation=False, is_adverse=False),
        Member("CLOUDY", "Cloudy", is_precipitation=False, is_adverse=False),
        Member("RAIN", "Rain", is_precipitation=True, is_adverse=True),
        Member("SNOW", "Snow", is_precipitation=True, is_adverse=True),
        Member("SLEET_HAIL", "Sleet or hail", is_precipitation=True, is_adverse=True),
        Member("FREEZING_RAIN", "Freezing rain or drizzle", is_precipitation=True, is_adverse=True),
        Member("WINTRY_MIX", "Wintry mix", is_precipitation=True, is_adverse=True),
        Member("BLOWING_SNOW", "Blowing snow", is_precipitation=True, is_adverse=True),
        Member("FOG_SMOKE", "Fog, smog or smoke", is_precipitation=False, is_adverse=True),
        Member("SEVERE_WIND", "Severe crosswinds", is_precipitation=False, is_adverse=True),
        Member("BLOWING_SAND", "Blowing sand, soil or dirt", is_precipitation=False, is_adverse=True),
        Member("OTHER", "Other", is_precipitation=None, is_adverse=None),
        # NOT_REPORTED is a member, not UNKNOWN: "the officer left it blank"
        # (FARS 98, Montgomery N/A) and "the officer could not tell" (FARS 99,
        # Montgomery UNKNOWN) are different facts, and the same reasoning made
        # severity 0 distinct from O in Phase 2.
        Member("NOT_REPORTED", "Not reported", is_precipitation=None, is_adverse=None),
    ),
)

NON_MOTORIST_TYPE = Vocabulary(
    name="non_motorist_type",
    csv_path=CONFIG_DIR / "non_motorist_type_crosswalk.csv",
    attr_columns=(),
    members=(
        Member("PEDESTRIAN", "Pedestrian"),
        Member("BICYCLIST", "Bicyclist"),
        Member("OTHER_CYCLIST", "Other cyclist / pedalcyclist"),
        Member("PERSONAL_CONVEYANCE", "Person on a personal conveyance"),
        Member("OTHER", "Other non-motorist"),
    ),
)

VOCABULARIES = {v.name: v for v in (ROAD_CLASS, WEATHER, NON_MOTORIST_TYPE)}


# ---------------------------------------------------------------------------
# crosswalk loading
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class Mapping:
    source_system: str
    source_column: str
    source_value: str
    conformed_code: str
    notes: str


@functools.cache
def load(vocab_name: str, path: str | None = None) -> tuple[Mapping, ...]:
    """Every row of a vocabulary's crosswalk, validated against the vocabulary.

    A `conformed_code` the vocabulary does not know is a hard error at load,
    not at join: a typo in the CSV must fail the build before it silently
    becomes a NULL foreign key.
    """
    vocab = VOCABULARIES[vocab_name]
    p = Path(path) if path else vocab.csv_path
    out: list[Mapping] = []
    seen: set[tuple[str, str, str]] = set()
    with p.open(newline="", encoding="utf-8") as fh:
        for row in csv.DictReader(fh):
            if not row.get("source_system"):
                continue
            code = row["conformed_code"].strip()
            if code not in vocab.codes():
                raise ValueError(
                    f"{p.name}: conformed_code {code!r} is not a member of "
                    f"{vocab.name} (members: {sorted(vocab.codes())})"
                )
            key = (row["source_system"].strip(), row["source_column"].strip(),
                   row["source_value"])
            if key in seen:
                raise ValueError(f"{p.name}: duplicate crosswalk row for {key}")
            seen.add(key)
            out.append(Mapping(*key, code, row.get("notes", "")))
    if not out:
        raise ValueError(f"{p}: crosswalk is empty")
    return tuple(out)


def register(con, vocab_name: str, *, table: str | None = None,
             path: str | None = None) -> str:
    """Materialise one crosswalk as `map_<name>_source` with the sk resolved.

    Joined in SQL, like the severity crosswalk: the unmapped case becomes
    `WHERE <name>_sk IS NULL`, a set that names every offending value at once.
    """
    vocab = VOCABULARIES[vocab_name]
    table = table or f"map_{vocab_name}_source"
    con.execute(
        f"""
        CREATE OR REPLACE TABLE {table} (
            source_system   VARCHAR NOT NULL,
            source_column   VARCHAR NOT NULL,
            source_value    VARCHAR NOT NULL,
            conformed_code  VARCHAR NOT NULL,
            {vocab.name}_sk INTEGER NOT NULL,
            is_lossy        BOOLEAN NOT NULL,
            notes           VARCHAR
        )
        """
    )
    con.executemany(
        f"INSERT INTO {table} VALUES (?,?,?,?,?,?,?)",
        [
            (m.source_system, m.source_column, m.source_value, m.conformed_code,
             vocab.sk(m.conformed_code),
             m.notes.upper().startswith("LOSSY") or "UNDECODED" in m.notes.upper(),
             m.notes)
            for m in load(vocab_name, path)
        ],
    )
    return table


def register_dim(con, vocab_name: str, *, table: str | None = None) -> str:
    """Materialise the dimension itself: one row per member plus UNKNOWN."""
    vocab = VOCABULARIES[vocab_name]
    table = table or f"dim_{vocab_name}"
    attr_types = {
        "functional_class_known": "BOOLEAN", "fhwa_class": "INTEGER",
        "is_precipitation": "BOOLEAN", "is_adverse": "BOOLEAN",
    }
    extra = "".join(f", {c} {attr_types[c]}" for c in vocab.attr_columns)
    con.execute(
        f"""CREATE OR REPLACE TABLE {table} (
                {vocab.name}_sk INTEGER NOT NULL,
                {vocab.name}_code VARCHAR NOT NULL,
                {vocab.name}_label VARCHAR NOT NULL{extra})"""
    )
    n = 3 + len(vocab.attr_columns)
    con.executemany(
        f"INSERT INTO {table} VALUES ({','.join('?' * n)})", vocab.rows()
    )
    return table


def assert_all_mapped(con, vocab_name: str, *, table: str, source_system: str,
                      source_column: str, value_expr: str,
                      crosswalk: str | None = None) -> None:
    """Raise naming every value of `table.value_expr` the crosswalk misses.

    `value_expr` is SQL; pass `coalesce(CAST(x AS VARCHAR), '__NULL__')` so the
    null case is a reviewed CSV row too.
    """
    crosswalk = crosswalk or f"map_{vocab_name}_source"
    rows = con.execute(
        f"""
        SELECT v, COUNT(*) AS n FROM (SELECT {value_expr} AS v FROM {table}) t
        WHERE NOT EXISTS (
            SELECT 1 FROM {crosswalk} c
            WHERE c.source_system = ? AND c.source_column = ? AND c.source_value = t.v)
        GROUP BY 1 ORDER BY 2 DESC
        """,
        [source_system, source_column],
    ).fetchall()
    if rows:
        detail = ", ".join(f"{v!r} ({n} rows)" for v, n in rows)
        raise UnmappedValue(
            f"{source_system}.{source_column}: {len(rows)} value(s) missing from "
            f"{VOCABULARIES[vocab_name].csv_path.name}: {detail}. This is drift: "
            "add a row with a decision and a note, or fix upstream."
        )


def lossy_rows(vocab_name: str) -> list[Mapping]:
    """The crosswalk rows whose notes declare lossiness -- for the report."""
    return [m for m in load(vocab_name)
            if m.notes.upper().startswith("LOSSY") or "UNDECODED" in m.notes.upper()]
