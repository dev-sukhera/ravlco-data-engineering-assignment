"""Load `rules.yaml` and `blackout_windows.csv`, and refuse a stale one.

Added to the scaffold because `EligibilityEngine` must not also be a YAML
parser: the engine's job is to run every gate and compose a decision, and the
loader's job is to make sure the law it runs is the law the build asked for.
Splitting them is what lets `tests/test_compliance.py` point the loader at a
`tmp_path` copy of the ruleset -- which is how the Ohio live-defence demo and
the effective-dating tests prove their claims without touching `src/`.


The three refusals, and why each one exists
-------------------------------------------
1. **Declared version != the version the build expects.** `ruleset_version` is
   stamped on every decision and every lineage row. A decision that says
   `1.0.0` but was produced by a different file is an unreadable audit record,
   which is worse than no audit record because it looks like one.

2. **Content hash != `content_sha256_prefix`.** This catches BOTH halves of
   the same mistake: a rule edited without a version bump (the version now
   means nothing) and a version bumped with no rule change (the version now
   means nothing in the other direction). The hash is taken over the whole
   document with `content_sha256_prefix` itself removed, canonicalised as
   sorted JSON, so reflowing a comment or a YAML block scalar does not trip
   it but changing a number does. The error message prints the value to paste.

3. **A null-window blackout row that does not name its carrier.** The Maryland
   row has no `days_from`: Maryland's constraint is a channel bar, not a
   clock. A time-window table row with no window is either that -- in which
   case it must say which rule actually carries the constraint -- or it is an
   unfinished row, and the difference has to be enforced rather than trusted.
   `config/blackout_windows.csv`'s MD row names
   MD_MVA_TELEPHONE_SOLICITATION_BAR and LIVE_SOLICITATION_PROHIBITED, and
   this loader checks both exist.


Effective dating
----------------
`in_force(rule, as_of)` is the only place a rule's dates are read. A rule with
`effective_from > effective_to` is never in force at any date -- which is not
a degenerate case to guard against but a MODELLING DEVICE: it is how the
vacated one-to-one consent order is represented (ASSIGNMENT.md 5d(1)), present
in the file and provably inert.


The matcher
-----------
`when` is an AND over fields; each value is a membership list. `unless`
suppresses the rule when ALL of its fields match. Two special members:

    null   matches a missing or None value
    "*"    matches any value that is not None

Deliberately not an expression language. A YAML rule file that can express
arbitrary predicates is a second programming language with no tests, no type
checker and no reviewer; everything Part 5 actually requires is a conjunction
of set memberships, and keeping it to that is what makes the file readable by
someone who is checking the law rather than the code.
"""

from __future__ import annotations

import csv
import functools
import hashlib
import json
from dataclasses import dataclass, field
from datetime import date, datetime
from pathlib import Path
from typing import Any, Iterable, Mapping, Sequence

import yaml

from .. import config

# Sentinels used inside a `when` / `unless` value list.
ANY_VALUE = "*"

# Dispositions. Mirrored in rules.yaml `dispositions`, which carries the prose.
BAR = "BAR"
HOLD_UNTIL_DATE = "HOLD_UNTIL_DATE"
HOLD_UNTIL_REFRESH = "HOLD_UNTIL_REFRESH"
NOTE = "NOTE"
AFFIRMATIVE = "AFFIRMATIVE"


class RulesetError(ValueError):
    """The ruleset cannot be trusted to produce a readable decision.

    Raised, never warned. Every message names the file and what to do about
    it, because the person who sees it is mid-build and the fix is always
    either "bump the version" or "paste this hash".
    """


# ---------------------------------------------------------------------------
# blackout windows
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class BlackoutRow:
    """One `(jurisdiction, record_type) -> earliest_contact_date` rule.

    `days_from` is None for a row that declares NO time gate. That is not a
    missing value: Maryland's constraint is a channel bar rather than a clock,
    and the table stays a time-window table by saying so explicitly and naming
    the rules that do carry it.
    """

    jurisdiction: str
    record_type: str
    days_from: int | None
    anchor_field: str | None
    legal_basis: str
    notes: str

    @property
    def has_window(self) -> bool:
        return self.days_from is not None and bool(self.anchor_field)


def load_blackout(path: str | Path, *, known_codes: Iterable[str]) -> list[BlackoutRow]:
    """Parse the blackout CSV, or raise naming the row that is wrong."""
    p = Path(path)
    if not p.exists():
        raise RulesetError(f"no blackout window table at {p}")
    known = set(known_codes)
    rows: list[BlackoutRow] = []
    with p.open(newline="", encoding="utf-8") as fh:
        for lineno, raw in enumerate(csv.DictReader(fh), start=2):
            jurisdiction = (raw.get("jurisdiction") or "").strip()
            record_type = (raw.get("record_type") or "").strip()
            if not jurisdiction:
                continue
            days_raw = (raw.get("days_from") or "").strip()
            anchor = (raw.get("anchor_field") or "").strip() or None
            basis = (raw.get("legal_basis") or "").strip()
            notes = (raw.get("notes") or "").strip()

            if not record_type:
                raise RulesetError(
                    f"{p}:{lineno}: jurisdiction {jurisdiction!r} has no record_type. "
                    "The table's key is (jurisdiction, record_type); a row without "
                    "one cannot be looked up."
                )
            days = int(days_raw) if days_raw else None
            if days is not None and days < 0:
                raise RulesetError(f"{p}:{lineno}: days_from {days} is negative")
            if days is not None and not anchor:
                raise RulesetError(
                    f"{p}:{lineno}: {jurisdiction}/{record_type} declares "
                    f"{days} days with no anchor_field to count from"
                )
            if not basis:
                raise RulesetError(
                    f"{p}:{lineno}: {jurisdiction}/{record_type} has no legal_basis. "
                    "A window without a citation is a number somebody remembered."
                )
            if days is None:
                # A null-window row MUST name the rule that carries the
                # constraint instead. See the module docstring.
                named = [c for c in known if c in notes]
                if not named:
                    raise RulesetError(
                        f"{p}:{lineno}: {jurisdiction}/{record_type} declares no time "
                        "window, so it must name the reason code(s) of the rule that "
                        "carries the constraint in its notes column. None of the known "
                        "reason codes appears there. If this row is simply unfinished, "
                        "finish it; if the jurisdiction genuinely has no waiting "
                        "period, say which rule bars the contact instead."
                    )
            rows.append(
                BlackoutRow(jurisdiction, record_type, days, anchor, basis, notes)
            )
    if not rows:
        raise RulesetError(f"{p} contains no rows")
    return rows


# ---------------------------------------------------------------------------
# the ruleset
# ---------------------------------------------------------------------------


def _canonical(doc: Mapping[str, Any]) -> str:
    """The document minus its own hash field, as sorted JSON.

    Sorted so key order in the YAML does not change the hash; `default=str` so
    the YAML loader's `date` objects serialise stably. Comments are not part
    of the parsed document and therefore not part of the hash -- reflowing a
    note does not force a version bump, changing a number does.
    """
    body = {k: v for k, v in doc.items() if k != "content_sha256_prefix"}
    return json.dumps(body, sort_keys=True, default=str, ensure_ascii=False)


def content_sha256(doc: Mapping[str, Any]) -> str:
    return hashlib.sha256(_canonical(doc).encode("utf-8")).hexdigest()


def _as_date(value: Any) -> date | None:
    if value is None:
        return None
    if isinstance(value, datetime):
        return value.date()
    if isinstance(value, date):
        return value
    return date.fromisoformat(str(value))


@dataclass(frozen=True)
class Rule:
    """One effective-dated rule, as read from the file.

    `raw` keeps the whole mapping so a lineage record can store the params
    exactly as they were evaluated -- reconstructing them from the typed
    fields would drop anything this dataclass does not know about, which is
    precisely the information a future reader will want.
    """

    id: str
    reason_code: str
    legal_basis: str
    params: dict[str, Any]
    when: dict[str, list[Any]]
    unless: list[dict[str, list[Any]]]
    effective_from: date | None
    effective_to: date | None
    family: str
    raw: dict[str, Any] = field(default_factory=dict, repr=False)

    def in_force(self, as_of: date) -> bool:
        """Is this rule the law on `as_of`?

        Inclusive on both ends. A rule whose `effective_from` is AFTER its
        `effective_to` can never satisfy both and is therefore never in force
        -- which is how a vacated rule is represented rather than deleted.
        """
        if self.effective_from is not None and as_of < self.effective_from:
            return False
        if self.effective_to is not None and as_of > self.effective_to:
            return False
        return True

    def matches(self, record: Mapping[str, Any]) -> bool:
        """Does this rule fire on this record?

        `when` is one conjunction. `unless` is a list of conjunctions and ANY
        of them suppresses the rule -- because a legal bar typically has
        several independent exceptions, and modelling them as a single AND
        would require every exception to hold at once before lifting it. The
        catch-all live-solicitation rule is the live case: it is lifted by a
        jurisdiction having its own rule row OR by a valid consumer-direct
        consent, and requiring both barred every consented record in a new
        jurisdiction.
        """
        if not _matches(self.when, record):
            return False
        return not any(_matches(clause, record) for clause in self.unless)


def _matches(clause: Mapping[str, Sequence[Any]], record: Mapping[str, Any]) -> bool:
    """AND over fields, membership within each. Empty clause matches."""
    for field_name, allowed in clause.items():
        value = record.get(field_name)
        if not _member(value, allowed):
            return False
    return True


def _member(value: Any, allowed: Sequence[Any]) -> bool:
    for candidate in allowed:
        if candidate is None:
            if value is None:
                return True
        elif candidate == ANY_VALUE:
            if value is not None:
                return True
        elif isinstance(candidate, bool) or isinstance(value, bool):
            # `True == 1` in Python and enum members compare equal to their
            # string value; compare booleans by identity of type so a `1` in a
            # record cannot satisfy a rule written for `true`.
            if isinstance(candidate, bool) and isinstance(value, bool) and candidate is value:
                return True
        elif value is not None and str(value) == str(candidate):
            return True
    return False


def _unless_clauses(value: Any) -> list[dict[str, list[Any]]]:
    """Normalise `unless` to a list. A bare mapping is a one-element list.

    Both spellings are accepted so a rule with a single exception stays
    readable as a mapping and only the rules that genuinely have several pay
    for the list syntax.
    """
    if not value:
        return []
    if isinstance(value, Mapping):
        return [dict(value)]
    return [dict(clause) for clause in value]


def _rules_from(entries: Any, family: str) -> list[Rule]:
    out: list[Rule] = []
    for entry in entries or []:
        out.append(
            Rule(
                id=entry["id"],
                reason_code=entry.get("reason_code"),
                legal_basis=" ".join(str(entry.get("legal_basis") or "").split()),
                params=dict(entry.get("params") or {}),
                when=dict(entry.get("when") or {}),
                unless=_unless_clauses(entry.get("unless")),
                effective_from=_as_date(entry.get("effective_from")),
                effective_to=_as_date(entry.get("effective_to")),
                family=family,
                raw=dict(entry),
            )
        )
    return out


@dataclass(frozen=True)
class Ruleset:
    """The parsed, verified ruleset. Read-only for the life of a build."""

    version: str
    path: Path
    sha256: str
    doc: dict[str, Any]
    source_gates: list[Rule]
    live_solicitation: list[Rule]
    dnc: list[Rule]
    internal_dnc: list[Rule]
    ebr: list[Rule]
    rnd: list[Rule]
    line_type_freshness: list[Rule]
    outside_calling_window: list[Rule]
    consent_rules: list[Rule]
    consent_affirmative: list[Rule]
    data_quality: list[Rule]

    # -- lookups the gates and the engine need ---------------------------
    @property
    def severity_order(self) -> list[str]:
        return list(self.doc["reason_code_severity_order"])

    @property
    def code_dispositions(self) -> dict[str, str]:
        return dict(self.doc["code_dispositions"])

    @property
    def status_precedence(self) -> list[dict[str, Any]]:
        return list(self.doc["status_precedence"])

    @property
    def blackout_codes(self) -> dict[str, Any]:
        return dict(self.doc["blackout_reason_codes"])

    @property
    def line_type_routing(self) -> dict[str, Any]:
        return dict(self.doc["channel_gates"]["line_type_routing"])

    @property
    def calling_window(self) -> dict[str, Any]:
        return dict(self.doc["channel_gates"]["calling_window"])

    @property
    def consent_spec(self) -> dict[str, Any]:
        return dict(self.doc["consent"])

    @property
    def retention(self) -> dict[str, Any]:
        return dict(self.doc["retention"])

    @property
    def monitoring(self) -> dict[str, Any]:
        return dict(self.doc["monitoring"])

    def circuit(self, jurisdiction: str | None) -> str | None:
        return self.doc["jurisdiction_circuit"].get(jurisdiction)

    def severity_index(self, code: str) -> int:
        """Position in the declared order; the derived-time-window slot for a
        code built from a blackout row this file does not name."""
        order = self.severity_order
        try:
            return order.index(code)
        except ValueError:
            marker = self.blackout_codes["derived_severity_marker"]
            return order.index(marker)

    def disposition(self, code: str) -> str:
        dispositions = self.code_dispositions
        if code in dispositions:
            return dispositions[code]
        # A derived time-window code is a time window by construction: it came
        # from a row in the blackout table, which is the time-window table.
        return dispositions[self.blackout_codes["derived_severity_marker"]]

    def in_force(self, rules: Sequence[Rule], as_of: date) -> list[Rule]:
        return [r for r in rules if r.in_force(as_of)]


def load_ruleset(
    path: str | Path | None = None,
    *,
    expect_version: str | None = None,
    verify_content_hash: bool = True,
) -> Ruleset:
    """Parse and verify `rules.yaml`. Raises `RulesetError` on any mismatch.

    `verify_content_hash=False` exists for exactly one caller: the helper that
    PRINTS the hash to paste after a deliberate rule change
    (`python -m src.compliance.ruleset --rehash`). Nothing in the build or in
    the test suite passes it.
    """
    cfg = config.compliance()
    p = Path(path) if path is not None else config.compliance_path("ruleset_path")
    if not p.exists():
        raise RulesetError(f"no ruleset at {p}")
    doc = yaml.safe_load(p.read_text(encoding="utf-8"))
    if not isinstance(doc, dict):
        raise RulesetError(f"{p} did not parse to a mapping")

    version = str(doc.get("version", ""))
    expected = expect_version if expect_version is not None else str(cfg["ruleset_version"])
    if version != expected:
        raise RulesetError(
            f"{p} declares version {version!r} but this build expects {expected!r} "
            f"(config/compliance.toml [compliance] ruleset_version). Every decision "
            f"is stamped with the version, so a mismatch would produce an audit "
            f"record that names a ruleset it was not produced by."
        )

    digest = content_sha256(doc)
    declared = str(doc.get("content_sha256_prefix", "") or "")
    if verify_content_hash and not (declared and digest.startswith(declared.lower())):
        raise RulesetError(
            f"{p}: content hash {digest[:16]} does not match the declared "
            f"content_sha256_prefix {declared!r}. Either a rule changed without a "
            f"version bump, or the version was bumped with no rule change -- both "
            f"make `ruleset_version` on a decision meaningless. If the change was "
            f"intended, set:\n\n    content_sha256_prefix: {digest[:16]}\n\n"
            f"and bump `version` if any rule actually moved."
        )

    # Cross-checks against config/compliance.toml. The duplication is
    # deliberate (see the header of config/compliance.toml); it is only a
    # guard if disagreeing is an error.
    if str(doc.get("window_arithmetic")) != str(cfg["window_arithmetic"]):
        raise RulesetError(
            f"{p} says window_arithmetic={doc.get('window_arithmetic')!r} but "
            f"config/compliance.toml says {cfg['window_arithmetic']!r}"
        )
    channel = doc["channel_gates"]
    dnc_param = next(
        r["params"]["max_age_days"] for r in channel["dnc"] if "max_age_days" in r["params"]
    )
    if int(dnc_param) != int(cfg["dnc_max_age_days"]):
        raise RulesetError(
            f"{p} says the DNC scrub ages out at {dnc_param} days but "
            f"config/compliance.toml says {cfg['dnc_max_age_days']}"
        )
    honour = doc["consent"]["revocation"]["honour_business_days"]
    if int(honour) != int(cfg["revocation_honour_business_days"]):
        raise RulesetError(
            f"{p} honours a revocation in {honour} business days but "
            f"config/compliance.toml says {cfg['revocation_honour_business_days']}"
        )

    # Every code that can be emitted must have a place in the severity order
    # and a disposition, or the output bytes stop being reproducible.
    order = set(doc["reason_code_severity_order"])
    dispositions = set(doc["code_dispositions"])
    if order != dispositions:
        raise RulesetError(
            f"{p}: reason_code_severity_order and code_dispositions disagree "
            f"(only in order: {sorted(order - dispositions)}; only in "
            f"dispositions: {sorted(dispositions - order)})"
        )

    consent = doc["consent"]
    return Ruleset(
        version=version,
        path=p,
        sha256=hashlib.sha256(p.read_bytes()).hexdigest(),
        doc=doc,
        source_gates=_rules_from(doc["source_gates"], "source"),
        live_solicitation=_rules_from(doc["live_solicitation"], "live_solicitation"),
        dnc=_rules_from(channel["dnc"], "dnc"),
        internal_dnc=_rules_from(channel["internal_dnc"], "internal_dnc"),
        ebr=_rules_from(channel["ebr"], "ebr"),
        rnd=_rules_from(channel["rnd"], "rnd"),
        line_type_freshness=_rules_from(channel["line_type_freshness"], "line_type"),
        outside_calling_window=_rules_from(
            channel["outside_calling_window"], "calling_window"
        ),
        consent_rules=_rules_from(consent["rules"], "consent"),
        consent_affirmative=_rules_from(consent["affirmative"], "consent"),
        data_quality=_rules_from(doc["data_quality"], "data_quality"),
    )


@functools.cache
def default_ruleset() -> Ruleset:
    return load_ruleset()


def _cli(argv: list[str] | None = None) -> int:
    """`python -m src.compliance.ruleset --rehash` after a deliberate edit."""
    import argparse
    import sys

    ap = argparse.ArgumentParser(prog="python -m src.compliance.ruleset")
    ap.add_argument("--rehash", action="store_true",
                    help="print the content_sha256_prefix to paste into rules.yaml")
    ap.add_argument("--path", type=Path, default=None)
    args = ap.parse_args(argv)
    p = args.path or config.compliance_path("ruleset_path")
    doc = yaml.safe_load(Path(p).read_text(encoding="utf-8"))
    digest = content_sha256(doc)
    if args.rehash:
        print(f"content_sha256_prefix: {digest[:16]}")
        return 0
    rs = load_ruleset(p)
    print(f"{rs.path} v{rs.version} sha {rs.sha256[:16]} content {digest[:16]}")
    print(f"  {len(rs.severity_order)} codes ordered, "
          f"{len(rs.source_gates)} source gates, "
          f"{len(rs.consent_rules)} consent rules")
    return 0


if __name__ == "__main__":
    import sys

    sys.exit(_cli())
