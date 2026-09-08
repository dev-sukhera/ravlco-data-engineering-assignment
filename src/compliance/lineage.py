"""Immutable decision lineage. Append-only, content-addressed, idempotent.

The scaffold's own design note in `engine.py` is the requirement:

    "Every decision emits an immutable lineage record. If you cannot
     reconstruct why a record was released eighteen months from now, you have
     not built a compliance control, you have built a filter."

Eighteen months from now `rules.yaml` will say something else, the blackout
table will have more rows, and the DNC scrub that was fresh will be ancient.
So a lineage row does not reference the ruleset -- it CONTAINS the state that
produced the decision: the ordered findings with their params as evaluated,
their citations as they read on the day, the tokenised input snapshot, the
`as_of`, the ruleset semver AND its file hash, the blackout table's hash, and
the engine build sha.


The id is a hash, not a sequence
--------------------------------
    decision_lineage_id = sha256(tokenised input, ruleset sha, blackout sha, as_of)

Three properties fall out, and they are the ones that make "append-only" a
guarantee rather than a hope:

  * **A re-run is a no-op.** Same inputs, same rules, same date -> same id,
    already present with identical content -> nothing appended. Two builds
    over unchanged inputs add zero rows, which is what ASSIGNMENT.md Part 6
    asks to be proven.
  * **A change makes a NEW row, never an update.** Change one input field or
    one rule and the id changes; the old row is untouched and still says what
    the decision was and why. There is no code path in this module that
    rewrites a stored payload.
  * **Locality is visible.** Changing one rule changes the ids of exactly the
    decisions that rule touched, because the ruleset sha is in every id but
    the input snapshot is not shared. That claim is tested on the Ohio row:
    adding Ohio to the blackout table changes the blackout sha and therefore
    every id -- which is correct and is why the test asserts on the DECISIONS
    (no existing disposition moves), not on the ids.

A sequence number was rejected: it needs a writer that coordinates, it makes
two builds of the same data produce different lineage, and it cannot tell you
whether a row you are looking at is the same decision you made last week.


Refusing a conflicting write
----------------------------
`add()` raises `LineageConflict` if an id is already present with DIFFERENT
content. That is not a defensive nicety -- it is the one thing that could
falsify the whole design. If two different decisions could ever land on one
id, the audit record would be silently wrong, so the writer treats it as a
build-stopping error and names the id and the differing keys.
"""

from __future__ import annotations

import hashlib
import json
import logging
from dataclasses import dataclass, field
from datetime import date, datetime
from pathlib import Path
from typing import Any, Iterable, Mapping, Sequence

import pandas as pd

log = logging.getLogger("compliance.lineage")

# Columns of `decision_lineage`, in contract order.
LINEAGE_COLUMNS = [
    "decision_lineage_id",
    "lead_id",
    "party_token",
    "as_of",
    "evaluated_at",
    "eligibility_status",
    "blocked_until_date",
    "reason_codes",
    "legal_basis",
    "findings_json",
    "input_snapshot_json",
    "ruleset_version",
    "ruleset_sha256",
    "blackout_sha256",
    "_compliance_build_sha",
]

# Never written to a lineage row, whatever a caller passes. The vault's
# projection should already have removed them; this is the second lock on the
# same door, because a lineage table is the one table nobody re-reads until
# they are already in trouble.
FORBIDDEN_KEYS = frozenset({
    "full_name", "street_address", "city", "phone_e164",
    "party_latitude", "party_longitude", "latitude", "longitude",
})


class LineageConflict(RuntimeError):
    """An existing id would be overwritten with different content."""


def _jsonable(value: Any) -> Any:
    """Canonicalise for hashing and for storage.

    Dataclasses, pydantic models and dates all appear in an engine record, and
    all three have to serialise the SAME WAY on every run or the id moves. The
    order is deliberate: named conversions first, `str()` only as the last
    resort, so nothing silently becomes a repr with a memory address in it.
    """
    # NaN FIRST, before the scalar branch. A null in a string column comes
    # back from parquet through pandas as float NaN, not None, so a stored row
    # and a freshly-computed identical row would hash differently and the
    # append-only writer would report a conflict on a row that had not
    # changed. `json.dumps(float("nan"))` also emits a bare `NaN`, which is
    # not valid JSON and would poison `input_snapshot_json`.
    if isinstance(value, float) and value != value:
        return None
    if value is None or isinstance(value, (str, int, float, bool)):
        return value
    if isinstance(value, (datetime, date)):
        return value.isoformat()
    if isinstance(value, Mapping):
        return {str(k): _jsonable(v) for k, v in sorted(value.items(), key=lambda kv: str(kv[0]))}
    if hasattr(value, "model_dump"):          # pydantic v2
        return _jsonable(value.model_dump(mode="json"))
    if hasattr(value, "__dataclass_fields__"):
        return _jsonable({f: getattr(value, f) for f in value.__dataclass_fields__})
    if isinstance(value, (list, tuple, set, frozenset)):
        items = [_jsonable(v) for v in value]
        return sorted(items, key=str) if isinstance(value, (set, frozenset)) else items
    if value is pd.NaT or value is pd.NA:
        return None
    return str(value)


def tokenised_snapshot(record: Mapping[str, Any]) -> dict[str, Any]:
    """The engine input, canonicalised and stripped of direct identifiers."""
    out = {k: _jsonable(v) for k, v in record.items() if k not in FORBIDDEN_KEYS}
    return dict(sorted(out.items()))


def _canonical(payload: Mapping[str, Any]) -> str:
    return json.dumps(payload, sort_keys=True, separators=(",", ":"), default=str)


def decision_lineage_id(
    *, snapshot: Mapping[str, Any], ruleset_sha: str, blackout_sha: str, as_of: date
) -> str:
    """sha256 over the four things that can change a decision.

    NOT over the findings: the findings are a FUNCTION of these four, so
    hashing them too would be hashing the same information twice and would
    hide a genuine engine bug -- two different outputs for one input would
    quietly get two different ids instead of colliding and raising.
    """
    material = _canonical({
        "snapshot": snapshot,
        "ruleset_sha256": ruleset_sha,
        "blackout_sha256": blackout_sha,
        "as_of": as_of.isoformat(),
    })
    return hashlib.sha256(material.encode("utf-8")).hexdigest()


@dataclass
class LineageStore:
    """Append-only store for decision lineage rows.

    Loads whatever is already on disk so that "a second build appends
    nothing" is a property of the store rather than of the caller remembering
    to check.
    """

    path: Path
    _rows: dict[str, dict[str, Any]] = field(default_factory=dict, repr=False)
    _existing: set[str] = field(default_factory=set, repr=False)
    appended: int = 0
    reused: int = 0

    @classmethod
    def open(cls, path: Path | str) -> "LineageStore":
        p = Path(path)
        store = cls(path=p)
        if p.exists():
            frame = pd.read_parquet(p)
            for row in frame.to_dict("records"):
                store._rows[str(row["decision_lineage_id"])] = dict(row)
            store._existing = set(store._rows)
            log.debug("lineage: loaded %d existing rows from %s", len(store._rows), p)
        return store

    def add(self, row: Mapping[str, Any]) -> str:
        lineage_id = str(row["decision_lineage_id"])
        payload = {k: row.get(k) for k in LINEAGE_COLUMNS}
        if lineage_id in self._rows:
            stored = self._rows[lineage_id]
            if _content_sha(stored) != _content_sha(payload):
                differing = sorted(
                    k for k in LINEAGE_COLUMNS
                    if k not in _CONTENT_EXCLUDED
                    and _canonical({k: _jsonable(stored.get(k))})
                    != _canonical({k: _jsonable(payload.get(k))})
                )
                raise LineageConflict(
                    f"decision_lineage_id {lineage_id} already exists with different "
                    f"content (differs in {differing}). A lineage row is immutable: "
                    "if the decision changed, its inputs changed, and a changed input "
                    "must produce a NEW id. This is an engine bug, not a data problem."
                )
            self.reused += 1
            return lineage_id
        self._rows[lineage_id] = payload
        self.appended += 1
        return lineage_id

    def frame(self) -> pd.DataFrame:
        """Every row, ordered by id -- the table's total sort order."""
        if not self._rows:
            return pd.DataFrame(columns=LINEAGE_COLUMNS)
        frame = pd.DataFrame(list(self._rows.values()), columns=LINEAGE_COLUMNS)
        return frame.sort_values("decision_lineage_id", kind="stable").reset_index(drop=True)

    @property
    def total(self) -> int:
        return len(self._rows)


# Excluded from the content comparison. `decision_lineage_id` because it IS
# the key. `_compliance_build_sha` because it is metadata about WHICH BUILD
# first reached this decision, not part of the decision: it hashes the whole
# `[compliance]` config block, so adding an unrelated setting -- an output
# path, a sample size -- moves it while every decision stays identical.
# Comparing it would make an append-only store raise on a build that changed
# nothing a decision depends on. The stored value therefore means "the
# earliest build that produced this decision", which is the more useful fact
# anyway; the CURRENT build's sha is on every `leads` row and in the manifest.
_CONTENT_EXCLUDED = frozenset({"decision_lineage_id", "_compliance_build_sha"})


def _content_sha(payload: Mapping[str, Any]) -> str:
    body = {k: _jsonable(v) for k, v in payload.items() if k not in _CONTENT_EXCLUDED}
    return hashlib.sha256(_canonical(body).encode("utf-8")).hexdigest()


def build_row(
    *,
    lead_id: str,
    party_token: str | None,
    snapshot: Mapping[str, Any],
    findings: Sequence[Any],
    status: str,
    blocked_until: date | None,
    reason_codes: Sequence[str],
    legal_basis: Sequence[str],
    as_of: date,
    evaluated_at: datetime,
    ruleset_version: str,
    ruleset_sha: str,
    blackout_sha: str,
    build_sha: str,
) -> dict[str, Any]:
    """One lineage row, ready for the store."""
    snap = tokenised_snapshot(snapshot)
    return {
        "decision_lineage_id": decision_lineage_id(
            snapshot=snap, ruleset_sha=ruleset_sha,
            blackout_sha=blackout_sha, as_of=as_of,
        ),
        "lead_id": lead_id,
        "party_token": party_token,
        "as_of": as_of.isoformat(),
        "evaluated_at": evaluated_at.isoformat(),
        "eligibility_status": status,
        "blocked_until_date": blocked_until.isoformat() if blocked_until else None,
        "reason_codes": "|".join(reason_codes),
        "legal_basis": json.dumps(list(legal_basis), ensure_ascii=False),
        "findings_json": json.dumps(
            [f.as_lineage() for f in findings], ensure_ascii=False, sort_keys=False
        ),
        "input_snapshot_json": json.dumps(snap, ensure_ascii=False, sort_keys=True),
        "ruleset_version": ruleset_version,
        "ruleset_sha256": ruleset_sha,
        "blackout_sha256": blackout_sha,
        "_compliance_build_sha": build_sha,
    }
