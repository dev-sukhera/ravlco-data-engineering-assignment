"""Enforced data contracts at both layer boundaries.

Two JSON Schema 2020-12 documents -- `contracts/bronze.schema.json` and
`contracts/silver.schema.json` -- describe one ROW of each table: types,
nullability, enums and ranges. They are the same dialect as the output contract
the downstream contact centre already consumes, so a reviewer reads one grammar
for all three.

JSON Schema cannot express the things that actually break a warehouse, so each
table also carries an `x-table-constraints` block:

    unique_keys      [["report_number"], ...]      -- grain, enforced
    foreign_keys     [{columns, references, ...}]  -- referential expectations
    row_count_min    an integer                    -- "did the build produce
                                                      anything at all"
    row_rules        [{name, predicate, ...}]      -- cross-column invariants

`row_rules` (added for Phase 5's analysis tables) is the one thing above that
JSON Schema genuinely cannot express even in principle: it describes one column
at a time, so it has no way to say "`significant` is true exactly where `p_sim`
is at or under `p_fdr`". A statistical output table is mostly made of
invariants of that shape -- a class column that must agree with the z-score
that produced it, a rate that must be null wherever its denominator was refused
-- and those are the errors that survive a type check and land in a published
map. Each rule is a NULL-safe SQL predicate that must hold for every row.

`orphans_allowed_when` on a foreign key names the boolean column that licenses a
missing parent. That is not a loophole: Montgomery's 785 driverless crashes are
a real, measured property of the source, and the contract's job is to say
"orphans are expected here and `has_driver_rows` marks them" rather than either
failing every build or saying nothing.


Why a hand-rolled validator
---------------------------
Great Expectations and Soda both do this well and both bring a large dependency
tree, a config format and a results store for what is, against a DuckDB relation,
a `DESCRIBE` plus a dozen generated `SELECT COUNT(*) WHERE NOT (...)` queries.
The rules live in JSON either way; this way they are enforced by 300 lines with
no new dependency, and every violation is reported as a COUNT plus up to three
example key values rather than as a boolean.

Types are checked against `DESCRIBE`, everything else against generated SQL, and
ALL failures are collected before raising -- a validator that stops at the first
error makes fixing a schema a game of whack-a-mole.
"""

from __future__ import annotations

import json
import functools
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Iterable, Sequence

from .config import REPO_ROOT

CONTRACTS_DIR = REPO_ROOT / "contracts"
BRONZE_CONTRACT = CONTRACTS_DIR / "bronze.schema.json"
SILVER_CONTRACT = CONTRACTS_DIR / "silver.schema.json"
# Phase 6. Named here so a caller says `contracts.COMPLIANCE_CONTRACT` rather
# than rebuilding the path, which is how the gold and analysis contracts
# already reach their builds.
COMPLIANCE_CONTRACT = CONTRACTS_DIR / "compliance.schema.json"
SCORING_CONTRACT = CONTRACTS_DIR / "scoring.schema.json"
LEAD_OUTPUT_CONTRACT = CONTRACTS_DIR / "lead_output.schema.json"

# JSON Schema type -> the DuckDB logical types that satisfy it. Deliberately
# permissive within a family (an INTEGER column satisfies "integer" whether it
# is INTEGER or BIGINT) and strict across families (VARCHAR does not satisfy
# "number"), because the failure this catches is a column that was never cast,
# not a width choice.
_TYPE_FAMILIES: dict[str, tuple[str, ...]] = {
    "string": ("VARCHAR", "UUID", "BLOB"),
    "integer": ("TINYINT", "SMALLINT", "INTEGER", "BIGINT", "HUGEINT",
                "UTINYINT", "USMALLINT", "UINTEGER", "UBIGINT"),
    "number": ("FLOAT", "DOUBLE", "DECIMAL", "REAL",
               "TINYINT", "SMALLINT", "INTEGER", "BIGINT", "HUGEINT"),
    "boolean": ("BOOLEAN",),
    "date": ("DATE",),
    "timestamp": ("TIMESTAMP", "TIMESTAMP WITH TIME ZONE", "TIMESTAMP_NS",
                  "TIMESTAMP_MS", "TIMESTAMP_S"),
    "time": ("TIME",),
    # Added in Phase 6. `contracts/compliance.schema.json` describes a table
    # whose grain is one LEAD, and a lead's `geo`, `contact` and `consent` are
    # nested objects in the output contract the downstream contact centre
    # already consumes -- so the parquet carries them as STRUCT and the two
    # decision arrays as LIST. Without these two families the validator would
    # report every one of them as a type violation, and flattening them purely
    # to satisfy the validator would mean the parquet no longer had the shape
    # `contracts/lead_output.schema.json` specifies.
    "object": ("STRUCT",),
    "array": ("LIST",),
}


class ContractViolation(AssertionError):
    """One or more contract violations. The message lists every one."""


@dataclass
class Violation:
    table: str
    kind: str
    detail: str
    count: int | None = None
    examples: list[Any] = field(default_factory=list)

    def render(self) -> str:
        head = f"  [{self.kind}] {self.table}: {self.detail}"
        if self.count is not None:
            head += f"  ({self.count} row(s))"
        if self.examples:
            head += f"  e.g. {self.examples}"
        return head


@functools.cache
def load_contract(path: str | Path) -> dict[str, Any]:
    p = Path(path)
    if not p.exists() or not p.stat().st_size:
        raise FileNotFoundError(f"contract {p} is missing or empty")
    with p.open(encoding="utf-8") as fh:
        doc = json.load(fh)
    if "tables" not in doc:
        raise ValueError(f"contract {p} has no 'tables' object")
    return doc


def table_contract(contract: dict[str, Any], table: str) -> dict[str, Any]:
    tables = contract["tables"]
    if table not in tables:
        raise KeyError(
            f"no contract for table {table!r} (have: {sorted(tables)}). "
            "A table with no contract has not been reviewed; add one."
        )
    return tables[table]


def _duck_family(duck_type: str) -> str:
    """The contract type family a DuckDB logical type satisfies.

    Two normalisations, both added in Phase 6 for the compliance tables:
    DuckDB renders a list as `<element>[]` and a struct as `STRUCT(...)`, and
    the contract cares about the CONTAINER, not the element -- `reason_codes`
    is an array of strings whether the element type prints as VARCHAR or as
    something else. The element type is checked by the row rules instead
    (`len(legal_basis) = len(reason_codes)`, `list_contains(...)`), which is
    where a container's contents can actually be asserted.
    """
    t = duck_type.upper().strip()
    if t.endswith("[]"):
        return "array"
    base = t.split("(")[0].strip()
    for family, members in _TYPE_FAMILIES.items():
        if base in members:
            return family
    return base


def _quote(name: str) -> str:
    return '"' + name.replace('"', '""') + '"'


def _sql_literal(value: Any) -> str:
    if value is None:
        return "NULL"
    if isinstance(value, bool):
        return "TRUE" if value else "FALSE"
    if isinstance(value, (int, float)):
        return repr(value)
    return "'" + str(value).replace("'", "''") + "'"


def validate_relation(
    con,
    relation: str,
    contract: dict[str, Any],
    table: str,
    *,
    key_columns: Sequence[str] | None = None,
    unique_keys: Sequence[Sequence[str]] | None = None,
    check_row_count_min: bool = True,
    max_examples: int = 3,
) -> list[Violation]:
    """Check one DuckDB relation against one table's contract entry.

    Returns every violation found; raising is the caller's decision so that a
    report tool can print them and a build can refuse on them.

    `unique_keys` overrides the contract's. The contract's key is the grain of
    the CURRENT slice; a history table is deliberately not unique on it -- that
    is what SCD2 means -- so the build passes (natural_key, valid_from,
    _bronze_load_ts) there, which is also the total order the parquet is written
    in. The two are the same assertion: the writer refuses a non-total order and
    the contract refuses a non-unique key.

    `check_row_count_min` is off for fixture-scale builds. The floor is a
    production-corpus assertion -- it catches a truncated or empty build -- and
    a few hundred committed test rows are not evidence of one.
    """
    spec = table_contract(contract, table)
    props: dict[str, Any] = spec.get("properties", {})
    required: set[str] = set(spec.get("required", []))
    constraints: dict[str, Any] = spec.get("x-table-constraints", {})
    out: list[Violation] = []

    described = con.execute(f"DESCRIBE SELECT * FROM {relation}").fetchall()
    actual = {row[0]: row[1] for row in described}
    # Example values for a violation are pulled from the table's key, so a
    # failure names the offending rows. A table with no declared key (bronze,
    # where a multi-partition read is legitimately not unique on anything)
    # simply reports counts.
    declared = constraints.get("unique_keys") or []
    key_columns = list(key_columns if key_columns is not None
                       else (declared[0] if declared else []))

    # -- presence -------------------------------------------------------
    missing = [col for col in props if col not in actual]
    if missing:
        out.append(Violation(table, "missing-columns", f"absent: {missing}"))
    extra = [col for col in actual if col not in props]
    if extra and not spec.get("additionalProperties", False):
        out.append(Violation(table, "unexpected-columns", f"not in contract: {extra}"))

    # -- column order ---------------------------------------------------
    # Column order is part of this contract, because it is part of the
    # determinism guarantee: the writer emits the contract's order, so a
    # reordered contract must fail loudly rather than silently rewriting every
    # parquet file with new bytes and the same content.
    if spec.get("x-column-order-is-contract", True) and not missing and not extra:
        if list(actual) != list(props):
            out.append(
                Violation(table, "column-order",
                          f"expected {list(props)}, got {list(actual)}")
            )

    checks: list[tuple[str, str, str]] = []  # (kind, detail, predicate-that-must-hold)

    for col, rule in props.items():
        if col not in actual:
            continue
        q = _quote(col)
        types = rule.get("type")
        types = [types] if isinstance(types, str) else list(types or [])
        nullable = "null" in types
        concrete = [t for t in types if t != "null"]

        if concrete:
            got = _duck_family(actual[col])
            if got not in concrete:
                out.append(
                    Violation(table, "type",
                              f"{col}: contract {concrete}, relation {actual[col]}")
                )
                # Do not also range- or enum-check a column of the wrong type.
                # `WHERE varchar_col >= 0` is not a failing check, it is a
                # BinderException -- and the type violation already says
                # everything there is to say about the column.
                continue

        if col in required and not nullable:
            checks.append(("null", f"{col} must not be null", f"{q} IS NOT NULL"))

        if "enum" in rule:
            allowed = [v for v in rule["enum"] if v is not None]
            lits = ", ".join(_sql_literal(v) for v in allowed)
            null_ok = "" if (None in rule["enum"] or nullable) else ""
            pred = f"({q} IS NULL OR {q} IN ({lits}))" if (nullable or None in rule["enum"]) \
                else f"{q} IN ({lits})"
            checks.append(("enum", f"{col} outside {allowed}", pred + null_ok))

        if "minimum" in rule:
            checks.append(("range", f"{col} < {rule['minimum']}",
                           f"({q} IS NULL OR {q} >= {rule['minimum']})"))
        if "maximum" in rule:
            checks.append(("range", f"{col} > {rule['maximum']}",
                           f"({q} IS NULL OR {q} <= {rule['maximum']})"))
        if "pattern" in rule:
            checks.append(("pattern", f"{col} !~ {rule['pattern']}",
                           f"({q} IS NULL OR regexp_matches({q}, "
                           f"{_sql_literal(rule['pattern'])}))"))

    for kind, detail, predicate in checks:
        out.extend(
            _count_failures(con, relation, table, kind, detail, predicate,
                            key_columns, max_examples)
        )

    # -- row rules ------------------------------------------------------
    # Added in Phase 5. JSON Schema describes ONE COLUMN at a time, so it
    # cannot say "significant is true only where p_sim <= p_fdr" -- a
    # cross-column invariant that is exactly the kind of thing a statistical
    # output table gets wrong. `row_rules` is a list of
    # {name, predicate, description}: a SQL expression that must hold for every
    # row, checked by the same COUNT-plus-examples machinery as everything
    # else, so a failure names the table, the rule and up to three keys.
    #
    # Predicates must be NULL-SAFE (write `col IS NULL OR ...`): `NOT (NULL)`
    # is NULL, not TRUE, so a rule that evaluates to NULL on a row passes it.
    # That is the same convention the range checks above use.
    for rule in constraints.get("row_rules", []):
        predicate = rule["predicate"]
        detail = rule.get("description") or rule["name"]
        out.extend(
            _count_failures(con, relation, table, f"row-rule:{rule['name']}",
                            detail, predicate, key_columns, max_examples)
        )

    # -- unique keys ----------------------------------------------------
    for key in (unique_keys if unique_keys is not None
                else constraints.get("unique_keys", [])):
        if not key or any(k not in actual for k in key):
            continue
        cols = ", ".join(_quote(k) for k in key)
        n = con.execute(
            f"SELECT COUNT(*) FROM (SELECT {cols} FROM {relation} "
            f"GROUP BY {cols} HAVING COUNT(*) > 1)"
        ).fetchone()[0]
        if n:
            examples = con.execute(
                f"SELECT {cols} FROM {relation} GROUP BY {cols} "
                f"HAVING COUNT(*) > 1 ORDER BY {cols} LIMIT {max_examples}"
            ).fetchall()
            out.append(
                Violation(table, "unique-key", f"{key} is not unique", n,
                          [list(e) for e in examples])
            )

    # -- row count floor -------------------------------------------------
    minimum = constraints.get("row_count_min") if check_row_count_min else None
    if minimum is not None:
        n = con.execute(f"SELECT COUNT(*) FROM {relation}").fetchone()[0]
        if n < minimum:
            out.append(
                Violation(table, "row-count", f"{n} rows < row_count_min {minimum}")
            )

    return out


def validate_foreign_keys(
    con,
    contract: dict[str, Any],
    table: str,
    relation: str,
    resolve: dict[str, str],
    *,
    max_examples: int = 3,
) -> list[Violation]:
    """Check `x-table-constraints.foreign_keys` for one table.

    Separate from `validate_relation` because it needs every OTHER table's
    relation name, which only the build knows. `resolve` maps a contract table
    name to the relation holding it.

    `orphans_allowed_when` names a boolean column on the child that is FALSE
    exactly where a parent is legitimately absent. Montgomery's 785 crashes with
    no driver row are the live case: the orphan is real, it is measured, and the
    contract says so instead of pretending otherwise.
    """
    spec = table_contract(contract, table)
    out: list[Violation] = []
    for fk in spec.get("x-table-constraints", {}).get("foreign_keys", []):
        parent_table = fk["references"]["table"]
        parent_rel = resolve.get(parent_table)
        if parent_rel is None:
            continue
        child_cols = fk["columns"]
        parent_cols = fk["references"]["columns"]
        on = " AND ".join(
            f"p.{_quote(pc)} = ch.{_quote(cc)}"
            for cc, pc in zip(child_cols, parent_cols)
        )
        # `orphans_allowed_when` names a boolean column on the CHILD that is
        # TRUE exactly where a missing parent is legitimate and measured. Those
        # rows are excluded from the check; every other row must have a parent.
        # A NULL in that column is not a licence -- coalesce to false, so
        # "unknown" fails closed.
        guard = ""
        allowed = fk.get("orphans_allowed_when")
        if allowed:
            guard = f" AND NOT coalesce(ch.{_quote(allowed)}, false)"
        where_parent = fk["references"].get("where", "TRUE")
        where_child = fk.get("where", "TRUE")
        sql = (
            f"SELECT COUNT(*) FROM {relation} ch WHERE {where_child}{guard} "
            f"AND NOT EXISTS (SELECT 1 FROM {parent_rel} p "
            f"WHERE {on} AND ({where_parent}))"
        )
        n = con.execute(sql).fetchone()[0]
        if n:
            cols = ", ".join(f"ch.{_quote(cc)}" for cc in child_cols)
            examples = con.execute(
                f"SELECT {cols} FROM {relation} ch WHERE {where_child}{guard} "
                f"AND NOT EXISTS (SELECT 1 FROM {parent_rel} p "
                f"WHERE {on} AND ({where_parent})) LIMIT {max_examples}"
            ).fetchall()
            out.append(
                Violation(
                    table, "foreign-key",
                    f"{child_cols} -> {parent_table}.{parent_cols}"
                    + (f" (orphans_allowed_when {allowed})" if allowed else ""),
                    n, [list(e) for e in examples],
                )
            )
    return out


def _count_failures(con, relation: str, table: str, kind: str, detail: str,
                    predicate: str, key_columns: Sequence[str],
                    max_examples: int) -> list[Violation]:
    n = con.execute(
        f"SELECT COUNT(*) FROM {relation} WHERE NOT ({predicate})"
    ).fetchone()[0]
    if not n:
        return []
    examples: list[Any] = []
    if key_columns:
        cols = ", ".join(_quote(k) for k in key_columns)
        examples = [
            list(r) for r in con.execute(
                f"SELECT {cols} FROM {relation} WHERE NOT ({predicate}) "
                f"LIMIT {max_examples}"
            ).fetchall()
        ]
    return [Violation(table, kind, detail, n, examples)]


def raise_for(violations: Iterable[Violation], *, context: str = "") -> None:
    """Raise a single ContractViolation listing every violation, or return."""
    violations = list(violations)
    if not violations:
        return
    head = f"{len(violations)} contract violation(s)"
    if context:
        head += f" in {context}"
    raise ContractViolation(head + ":\n" + "\n".join(v.render() for v in violations))


def validate(
    con,
    relation: str,
    table: str,
    *,
    contract_path: str | Path = SILVER_CONTRACT,
    resolve: dict[str, str] | None = None,
    key_columns: Sequence[str] | None = None,
    unique_keys: Sequence[Sequence[str]] | None = None,
    check_row_count_min: bool = True,
    raise_on_violation: bool = True,
) -> list[Violation]:
    """Validate one relation and, unless told otherwise, raise on any violation."""
    contract = load_contract(contract_path)
    violations = validate_relation(
        con, relation, contract, table, key_columns=key_columns,
        unique_keys=unique_keys, check_row_count_min=check_row_count_min,
    )
    if resolve:
        violations += validate_foreign_keys(con, contract, table, relation, resolve)
    if raise_on_violation:
        raise_for(violations, context=f"{Path(contract_path).name}:{table}")
    return violations
