"""Code-dictionary normalisation and the value-set drift detector.

Two things live here, and they are deliberately the same code path.

**The grammar.** Montgomery's `driver_substance_abuse` carries two generations
of code dictionary in one column, concatenated. `parse_substance()` is a pure
function over a single raw string; `parse_substance_list()` is the same parser
run over the comma-joined *crash-level* string that Incidents publishes. Both
are pure Python over a string, with no database and no clock, which is what
makes the ambiguity cases unit-testable and what lets SQL use them: the
transform materialises `parse_substance()` over the distinct-value set into a
small lookup table and joins it, rather than trying to express the grammar in
SQL.

**The detector.** `detect_drift()` takes the values a column actually contains
and the vocabulary that column is *supposed* to contain, and reports the
difference with first-seen/last-seen evidence. It is generic over
(dataset, column) -- it has to be, because the same 2024 dictionary generation
change hit `injury_severity` in exactly the same week, and a detector
special-cased to `driver_substance_abuse` would have caught one and slept
through the other.


Why grammar and not date
------------------------
The obvious implementation is "before 2024-01-01 it is the old scheme, after it
is the new one". That is wrong on this data and the failure is measurable:
the two generations coexist by crash date over 2023-12-28 .. 2024-01-03.
2023-12-28 has 2 new-scheme rows against 39 old; 2024-01-03 has 37 new against
2 old. **No single date classifies every row correctly** -- a cutover at
2024-01-01 misfiles four rows, and every other candidate date misfiles more.
`test_dictionary_cutover_overlap_handled` asserts exactly that non-existence
on bronze, and asserts on silver that every row in the window resolved through
the grammar instead.

`:created_at` is worse, not better, as a cutover proxy: a 2024-06-12 bulk
reload stamped 172,096 old-scheme and 3,637 new-scheme rows with the same
creation date. The transform classifies per value. The date is evidence in the
drift report, never an input to the decision.


The grammar itself
------------------
Old generation: one UPPERCASE token per party, e.g. `NONE DETECTED`.
New generation: an ordered PAIR of Title Case tokens per party,
`<alcohol>, <drug>`, e.g. `Not Suspect of Alcohol Use, Suspect of Drug Use`.

The embedded ", " is what breaks naive splitting -- a split on comma turns one
new-generation driver into two phantom parties. The parse is therefore a
left-to-right consume: at each position, a new-generation alcohol token claims
the *next* token as its drug half; an old-generation token stands alone. The
two generations' token sets are disjoint (uppercase vs Title Case), so the
consume never has to guess, and a crash string mixing both generations across
its parties -- which Incidents genuinely contains -- parses correctly.

Case is used to *route* between vocabularies, never to decide meaning: both
vocabularies are enumerated below and a token outside both is UNMAPPED, not
guessed at from its shape.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Iterable, Sequence

# --------------------------------------------------------------------------
# vocabularies
# --------------------------------------------------------------------------
# Every token below was observed in local bronze on 2026-09-07 across all
# partitions of mmzv-x632 (Drivers, 21 distinct values), n7fk-dce5
# (Non-Motorists, 20 distinct) and bhju-22kf (Incidents, 121 distinct
# *concatenations* of the same tokens). Counts are in the build report.

NOT_SUSPECTED = "NOT_SUSPECTED"
SUSPECTED = "SUSPECTED"
UNKNOWN = "UNKNOWN"
NOT_APPLICABLE = "NOT_APPLICABLE"
UNMAPPED = "UNMAPPED"

STATUSES = (NOT_SUSPECTED, SUSPECTED, UNKNOWN, NOT_APPLICABLE, UNMAPPED)

SCHEME_OLD = "OLD_SINGLE"
SCHEME_NEW = "NEW_PAIR"
SCHEME_NULL = "NULL"
SCHEME_UNMAPPED = "UNMAPPED"
SCHEME_MIXED = "MIXED"  # crash-level strings only: parties from both generations

# Old generation, one token per party. (alcohol_status, drug_status, detail).
#
# The PRESENT / CONTRIBUTED distinction is a causation claim, not a detection
# claim: PRESENT says the substance was found, CONTRIBUTED says the officer
# judged it causal. Both are "suspected" for the purpose of a single harmonised
# flag -- an officer who records CONTRIBUTED has necessarily also detected --
# so both map to SUSPECTED and the distinction is preserved verbatim in
# `substance_detail`. Collapsing them into one status and throwing the detail
# away would make "alcohol contributed" unrecoverable downstream; keeping two
# statuses would make the flag incomparable with the new generation, which has
# no causation concept at all. Detail is the only place the information can
# live without distorting the harmonised field.
#
# OTHER: a substance outside the enumerated categories was detected. It is a
# positive finding, so SUSPECTED on the drug side (the column's own name is
# "substance abuse" and alcohol has its own tokens); detail keeps OTHER.
# COMBINED SUBSTANCE PRESENT / COMBINATION CONTRIBUTED: by definition more than
# one substance class, so both sides are SUSPECTED.
_OLD: dict[str, tuple[str, str, str | None]] = {
    "NONE DETECTED":               (NOT_SUSPECTED,  NOT_SUSPECTED,  "NONE_DETECTED"),
    "ALCOHOL PRESENT":             (SUSPECTED,      NOT_SUSPECTED,  "ALCOHOL_PRESENT"),
    "ALCOHOL CONTRIBUTED":         (SUSPECTED,      NOT_SUSPECTED,  "ALCOHOL_CONTRIBUTED"),
    "ILLEGAL DRUG PRESENT":        (NOT_SUSPECTED,  SUSPECTED,      "ILLEGAL_DRUG_PRESENT"),
    "ILLEGAL DRUG CONTRIBUTED":    (NOT_SUSPECTED,  SUSPECTED,      "ILLEGAL_DRUG_CONTRIBUTED"),
    "MEDICATION PRESENT":          (NOT_SUSPECTED,  SUSPECTED,      "MEDICATION_PRESENT"),
    "MEDICATION CONTRIBUTED":      (NOT_SUSPECTED,  SUSPECTED,      "MEDICATION_CONTRIBUTED"),
    "COMBINED SUBSTANCE PRESENT":  (SUSPECTED,      SUSPECTED,      "COMBINED_SUBSTANCE_PRESENT"),
    "COMBINATION CONTRIBUTED":     (SUSPECTED,      SUSPECTED,      "COMBINATION_CONTRIBUTED"),
    "OTHER":                       (UNKNOWN,        SUSPECTED,      "OTHER"),
    "UNKNOWN":                     (UNKNOWN,        UNKNOWN,        None),
    "N/A":                         (NOT_APPLICABLE, NOT_APPLICABLE, None),
}

# New generation, an ordered pair per party. Position 1 is alcohol, position 2
# is drug -- the two halves share the token "Unknown", so position is the only
# thing that distinguishes them and the parse must not reorder.
_NEW_ALCOHOL: dict[str, str] = {
    "Not Suspect of Alcohol Use": NOT_SUSPECTED,
    "Suspect of Alcohol Use":     SUSPECTED,
    "Unknown":                    UNKNOWN,
}
_NEW_DRUG: dict[str, str] = {
    "Not Suspect of Drug Use": NOT_SUSPECTED,
    "Suspect of Drug Use":     SUSPECTED,
    "Unknown":                 UNKNOWN,
}

# The three spellings of null, plus SQL NULL itself. The assignment says "at
# least three distinct spellings of null across the two schemes" and that is
# what these are: SQL NULL, "N/A" (old), "UNKNOWN" (old), "Unknown, Unknown"
# (new). They are NOT interchangeable and silver keeps them apart:
#   N/A            -> NOT_APPLICABLE  (no driver to test: parked, driverless)
#   UNKNOWN        -> UNKNOWN         (a driver, not tested or not recorded)
#   Unknown,Unknown-> UNKNOWN         (same, new generation)
#   SQL NULL       -> scheme NULL, both statuses UNKNOWN
# Flattening all four to NULL is the lossy move the assignment is testing for.
NULL_SPELLINGS = (None, "", "N/A", "UNKNOWN", "Unknown, Unknown")

SUBSTANCE_COLUMNS = ("driver_substance_abuse", "non_motorist_substance_abuse")


@dataclass(frozen=True)
class Substance:
    """One party's normalised substance record."""

    scheme: str
    alcohol_status: str
    drug_status: str
    substance_detail: str | None
    raw: str | None

    @property
    def any_suspected(self) -> bool:
        return SUSPECTED in (self.alcohol_status, self.drug_status)

    @property
    def is_unmapped(self) -> bool:
        return self.scheme == SCHEME_UNMAPPED


_NULL_SUBSTANCE = Substance(SCHEME_NULL, UNKNOWN, UNKNOWN, None, None)


def _tokens(raw: str) -> list[str]:
    """Split the concatenation. `, ` is the join separator Socrata uses.

    Splitting on `,` alone would also split inside a token if one ever contained
    a bare comma; none does today, and the strip() keeps the parse tolerant of
    `,` vs `, ` without inventing tokens.
    """
    return [t.strip() for t in raw.split(",") if t.strip()]


def parse_substance(raw: str | None) -> Substance:
    """Normalise ONE party's raw substance value.

    Pure: no clock, no database, no cutover date. Returns UNMAPPED rather than
    guessing, so an unrecognised token reaches the drift detector instead of
    being silently absorbed into UNKNOWN.
    """
    if raw is None or not raw.strip():
        return _NULL_SUBSTANCE

    value = raw.strip()
    if value in _OLD:
        alcohol, drug, detail = _OLD[value]
        return Substance(SCHEME_OLD, alcohol, drug, detail, value)

    toks = _tokens(value)
    if len(toks) == 2 and toks[0] in _NEW_ALCOHOL and toks[1] in _NEW_DRUG:
        return Substance(
            SCHEME_NEW, _NEW_ALCOHOL[toks[0]], _NEW_DRUG[toks[1]], None, value
        )

    # A single-party column holding more than one party's worth of tokens is
    # itself a defect worth surfacing, so it does not silently become a list
    # here -- parse_substance_list is the explicit entry point for that.
    return Substance(SCHEME_UNMAPPED, UNMAPPED, UNMAPPED, None, value)


def parse_substance_list(raw: str | None) -> list[Substance]:
    """Normalise the CRASH-LEVEL concatenation Incidents publishes.

    `bhju-22kf.driver_substance_abuse` is the per-crash `, `-join of every
    driver's value -- the worst form of the embedded-comma defect, because a
    new-generation party contributes two comma-separated tokens and an
    old-generation party contributes one, in the same string, in any mixture.
    121 distinct concatenations exist locally.

    Left-to-right consume with a two-token lookahead. Returns one Substance per
    party recovered. A token that starts a new-generation pair but is not
    followed by a valid drug token yields UNMAPPED for that party and the parse
    continues, so one bad party does not destroy the rest of the crash.

    This is a CROSS-CHECK, not the source of truth. The crash-level flags on
    silver.montgomery_crash are aggregated from the Drivers table, which has one
    unambiguous row per driver. The disagreement count between the two is
    reported; see src/transform/montgomery.py.
    """
    if raw is None or not raw.strip():
        return []

    toks = _tokens(raw)
    out: list[Substance] = []
    i = 0
    while i < len(toks):
        tok = toks[i]
        if tok in _OLD:
            alcohol, drug, detail = _OLD[tok]
            out.append(Substance(SCHEME_OLD, alcohol, drug, detail, tok))
            i += 1
        elif tok in _NEW_ALCOHOL:
            nxt = toks[i + 1] if i + 1 < len(toks) else None
            if nxt is not None and nxt in _NEW_DRUG:
                out.append(
                    Substance(
                        SCHEME_NEW,
                        _NEW_ALCOHOL[tok],
                        _NEW_DRUG[nxt],
                        None,
                        f"{tok}, {nxt}",
                    )
                )
                i += 2
            else:
                out.append(Substance(SCHEME_UNMAPPED, UNMAPPED, UNMAPPED, None, tok))
                i += 1
        else:
            out.append(Substance(SCHEME_UNMAPPED, UNMAPPED, UNMAPPED, None, tok))
            i += 1
    return out


def crash_scheme(parties: Sequence[Substance]) -> str:
    """Which dictionary generation a whole crash's parties are written in."""
    schemes = {p.scheme for p in parties}
    if not schemes:
        return SCHEME_NULL
    if SCHEME_UNMAPPED in schemes:
        return SCHEME_UNMAPPED
    real = schemes - {SCHEME_NULL}
    if len(real) > 1:
        return SCHEME_MIXED
    return real.pop() if real else SCHEME_NULL


# --------------------------------------------------------------------------
# the accepted vocabulary, per (dataset, column)
# --------------------------------------------------------------------------
# What each column is ALLOWED to contain. Anything else is drift.
#
# `injury_severity` is here for the reason given at the top of the file: the
# same 2024 dictionary generation change re-cased it (NO APPARENT INJURY ->
# No Apparent Injury, 11 distinct values across both cases in Drivers, 10 in
# Non-Motorists) and a detector that only watched the substance column would
# have passed it silently.

_MOCO_SUBSTANCE = tuple(_OLD) + tuple(
    f"{a}, {d}" for a in _NEW_ALCOHOL for d in _NEW_DRUG
)

_MOCO_INJURY_OLD = (
    "NO APPARENT INJURY",
    "POSSIBLE INJURY",
    "SUSPECTED MINOR INJURY",
    "SUSPECTED SERIOUS INJURY",
    "FATAL INJURY",
)
_MOCO_INJURY_NEW = (
    "No Apparent Injury",
    "Possible Injury",
    "Suspected Minor Injury",
    "Suspected Serious Injury",
    "Fatal Injury",
)

VOCABULARIES: dict[tuple[str, str], tuple[str, ...]] = {
    ("mmzv-x632", "driver_substance_abuse"): _MOCO_SUBSTANCE,
    ("mmzv-x632", "non_motorist_substance_abuse"): _MOCO_SUBSTANCE,
    ("n7fk-dce5", "driver_substance_abuse"): _MOCO_SUBSTANCE,
    ("n7fk-dce5", "non_motorist_substance_abuse"): _MOCO_SUBSTANCE,
    ("mmzv-x632", "injury_severity"): _MOCO_INJURY_OLD + _MOCO_INJURY_NEW,
    ("n7fk-dce5", "injury_severity"): _MOCO_INJURY_OLD + _MOCO_INJURY_NEW,
    ("bhju-22kf", "acrs_report_type"): (
        "Property Damage Crash",
        "Injury Crash",
        "Fatal Crash",
    ),
    # Incidents' substance column is a per-crash concatenation, so its
    # vocabulary is the TOKEN set, not the value set: the detector is told to
    # tokenise this one. See `tokenised` in detect_drift().
    ("bhju-22kf", "driver_substance_abuse"): _MOCO_SUBSTANCE,
    ("bhju-22kf", "non_motorist_substance_abuse"): _MOCO_SUBSTANCE,
}

# Columns whose values are `, `-joined concatenations of several parties'
# tokens rather than single values. The detector parses these with the grammar
# and reports unmapped *tokens*, not unmapped concatenations -- otherwise every
# new multi-driver combination would look like drift.
#
# WHICH columns those are is wider than the assignment says, and it is a
# property of GRAIN, not of dataset. A substance column is single-valued only on
# the table whose grain matches it. Everywhere else it is the denormalised
# crash-level roll-up of the *other* party type, and it concatenates:
#
#   mmzv-x632.driver_substance_abuse        one driver        SINGLE
#   n7fk-dce5.non_motorist_substance_abuse  one non-motorist  SINGLE
#   mmzv-x632.non_motorist_substance_abuse  all non-motorists CONCATENATED (29 distinct)
#   n7fk-dce5.driver_substance_abuse        all drivers       CONCATENATED (33 distinct)
#   bhju-22kf.driver_substance_abuse        all drivers       CONCATENATED (121 distinct)
#   bhju-22kf.non_motorist_substance_abuse  all non-motorists CONCATENATED (29 distinct)
#
# Measured 2026-09-07: the grammar leaves ZERO unmapped tokens across all six,
# while a naive single-value parse leaves 8 / 16 / 99 / 8 unmapped values on the
# four concatenated ones. The assignment names only mmzv-x632 and the Phase 1
# report found bhju-22kf; the other two are new here.
TOKENISED_COLUMNS: frozenset[tuple[str, str]] = frozenset(
    {
        ("bhju-22kf", "driver_substance_abuse"),
        ("bhju-22kf", "non_motorist_substance_abuse"),
        ("mmzv-x632", "non_motorist_substance_abuse"),
        ("n7fk-dce5", "driver_substance_abuse"),
    }
)


def vocabulary(dataset: str, column: str) -> tuple[str, ...]:
    """The accepted value set, or a raise naming what is registered.

    Raising on an unregistered column is the point: a column nobody has written
    a vocabulary for has not been reviewed, and "no vocabulary" must not read as
    "everything is fine".
    """
    key = (dataset, column)
    if key not in VOCABULARIES:
        raise KeyError(
            f"no vocabulary registered for {dataset}.{column} "
            f"(registered: {sorted(VOCABULARIES)})"
        )
    return VOCABULARIES[key]


# --------------------------------------------------------------------------
# drift detection
# --------------------------------------------------------------------------


@dataclass
class DriftToken:
    """One value (or token) the vocabulary does not accept, with evidence."""

    value: str
    row_count: int
    first_crash_date_time: str | None = None
    last_crash_date_time: str | None = None
    first_created_at: str | None = None
    last_created_at: str | None = None

    def line(self) -> str:
        return (
            f"  {self.value!r:<52} rows={self.row_count:<8} "
            f"crash_date_time {self.first_crash_date_time} .. {self.last_crash_date_time}  "
            f":created_at {self.first_created_at} .. {self.last_created_at}"
        )


@dataclass
class DriftReport:
    """The result of one (dataset, column) check."""

    dataset: str
    column: str
    accepted: tuple[str, ...]
    unmapped: list[DriftToken] = field(default_factory=list)
    rows_checked: int = 0
    tokenised: bool = False

    @property
    def drifted(self) -> bool:
        return bool(self.unmapped)

    @property
    def unmapped_rows(self) -> int:
        return sum(t.row_count for t in self.unmapped)

    def render(self) -> str:
        head = (
            f"[{'DRIFT' if self.drifted else 'ok'}] {self.dataset}.{self.column}  "
            f"rows={self.rows_checked}  accepted_vocabulary={len(self.accepted)}"
            + ("  (tokenised)" if self.tokenised else "")
        )
        if not self.drifted:
            return head
        lines = [
            head,
            f"  {len(self.unmapped)} value(s) outside the accepted vocabulary, "
            f"{self.unmapped_rows} row(s):",
        ]
        lines += [t.line() for t in sorted(self.unmapped, key=lambda t: -t.row_count)]
        return "\n".join(lines)


@dataclass(frozen=True)
class ObservedValue:
    """One distinct value as seen in the data, with its evidence window.

    The transform builds these with a single GROUP BY; the detector never reads
    the database itself, which is what makes it testable on a literal list.
    """

    value: str | None
    row_count: int
    first_crash_date_time: str | None = None
    last_crash_date_time: str | None = None
    first_created_at: str | None = None
    last_created_at: str | None = None


def detect_drift(
    dataset: str,
    column: str,
    values: Iterable[ObservedValue],
    *,
    accepted: Sequence[str] | None = None,
    tokenised: bool | None = None,
) -> DriftReport:
    """Compare observed values against the accepted vocabulary.

    `accepted` overrides the registered vocabulary -- that is what
    `--vocabulary-asof` uses to replay history with only the pre-cutover tokens
    accepted, which is the Part 6 "show it firing against historical data"
    evidence.

    NULL is never drift. A null is an absence of a value, and the four spellings
    of null in NULL_SPELLINGS are enumerated in the vocabulary as themselves.
    """
    accepted_set = tuple(accepted) if accepted is not None else vocabulary(dataset, column)
    ok = set(accepted_set)
    is_tokenised = (
        tokenised if tokenised is not None else (dataset, column) in TOKENISED_COLUMNS
    )

    # value -> accumulated evidence
    acc: dict[str, DriftToken] = {}
    rows = 0
    for obs in values:
        rows += obs.row_count
        if obs.value is None or not obs.value.strip():
            continue
        if is_tokenised:
            bad = [
                p.raw
                for p in parse_substance_list(obs.value)
                if p.is_unmapped and p.raw is not None
            ]
            # A concatenation whose parties all parse is fine even if the whole
            # string has never been seen before; a token nobody has a mapping
            # for is drift regardless of what it is concatenated with.
            offenders = [b for b in bad if b not in ok]
        else:
            offenders = [] if obs.value in ok else [obs.value]

        for value in offenders:
            tok = acc.get(value)
            if tok is None:
                acc[value] = DriftToken(
                    value,
                    obs.row_count,
                    obs.first_crash_date_time,
                    obs.last_crash_date_time,
                    obs.first_created_at,
                    obs.last_created_at,
                )
            else:
                tok.row_count += obs.row_count
                tok.first_crash_date_time = _min(
                    tok.first_crash_date_time, obs.first_crash_date_time
                )
                tok.last_crash_date_time = _max(
                    tok.last_crash_date_time, obs.last_crash_date_time
                )
                tok.first_created_at = _min(tok.first_created_at, obs.first_created_at)
                tok.last_created_at = _max(tok.last_created_at, obs.last_created_at)

    return DriftReport(
        dataset=dataset,
        column=column,
        accepted=accepted_set,
        unmapped=list(acc.values()),
        rows_checked=rows,
        tokenised=is_tokenised,
    )


def _min(a: str | None, b: str | None) -> str | None:
    return b if a is None else (a if b is None else min(a, b))


def _max(a: str | None, b: str | None) -> str | None:
    return b if a is None else (a if b is None else max(a, b))


def vocabulary_asof(dataset: str, column: str, values: Iterable[ObservedValue],
                    asof_crash_date: str) -> tuple[str, ...]:
    """The vocabulary as it would have looked on a given date.

    Replays the observed values and keeps only those first seen on or before
    `asof_crash_date`. This is how `python -m src.transform.drift
    --vocabulary-asof 2023-12-27` reconstructs the pre-cutover vocabulary from
    the data itself rather than from a hardcoded list -- so the demonstration
    cannot be accused of having been rigged by choosing which tokens to omit.
    """
    keep: list[str] = []
    for obs in values:
        if obs.value is None:
            continue
        first = obs.first_crash_date_time
        if first is not None and first[:10] <= asof_crash_date:
            keep.append(obs.value)
    return tuple(dict.fromkeys(keep))
