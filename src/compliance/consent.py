"""Consent provenance, revocation, and the revoke-all cascade.

ASSIGNMENT.md 5c: "For any consented record, persist: the exact text
presented, a hash or snapshot of the disclosure, the URL, timestamp with
timezone, IP, user agent, the signature or checkbox event, every seller named,
and the full lead-source chain. **The burden of proving consent is on the
caller; unverifiable consent is functionally no consent.**"

That last sentence is the design. `consent_on_file = true` is a CLAIM, and a
claim this pipeline refuses to accept on its own: the gate checks the
provenance record field by field against `rules.yaml`
`consent.required_provenance_fields` and emits `CONSENT_UNVERIFIABLE` when any
of them is missing. A boolean is not evidence.


Where the fixture's provenance comes from, and why that is honest
-----------------------------------------------------------------
`fixtures/synthetic_parties.csv` carries two booleans, `consent_on_file` and
`consent_revoked`, and nothing else -- no disclosure text, no URL, no IP, no
timestamp, no seller. The provenance record is therefore SYNTHESISED here from
the fixture generator's own deterministic values (a hash of `party_id`, a
fixed disclosure URL and hash, an IP drawn from the RFC 5737 documentation
range 192.0.2.0/24 so no real address appears anywhere) and stamped
`provenance_kind: SYNTHETIC_FIXTURE`.

Two things follow and both are stated rather than implied. Every synthesised
record is COMPLETE by construction, so `CONSENT_UNVERIFIABLE` cannot fire on
the 40 fixture rows -- the gate is unit-tested against a deliberately
incomplete record instead. And a synthesised provenance record proves nothing
about a real consumer; it exercises the schema, which is all the fixture was
ever able to do. `COMPLIANCE.md` says so in the sentence the memo repeats.


Revocation -- append-only, immutable, revoke-all ready
------------------------------------------------------
47 C.F.R. 64.1200(a)(10) requires a revocation to be honoured within a
reasonable time not to exceed **ten business days**. The FCC's "revoke-all"
scope provision has been waived twice; its current compliance date is
**31 January 2027** (DA 26-12).

So `scope` is a first-class column NOW, with `ALL` honoured NOW. A revocation
with `scope = ALL` blocks every seller's campaign for that token from the
moment it is received. Building for it costs one column and one `or` clause;
retrofitting it means finding every campaign that read a per-seller
suppression and proving you found them all.

`honour_by` (= `received_at` + 10 business days) is stored as the REGULATORY
DEADLINE, for monitoring. The engine honours from `received_at`, immediately:
the ten days is the outer bound of a reasonable time, not a licence to keep
calling for ten days.

Business days are Monday-Friday. Federal holidays are NOT subtracted, which
makes `honour_by` at most a few days EARLIER than the regulation allows --
conservative in the only direction that is safe, and stated here rather than
discovered later.
"""

from __future__ import annotations

import hashlib
import logging
from datetime import date, datetime, time, timedelta, timezone
from typing import Any, Iterable, Sequence

import pandas as pd
from pydantic import BaseModel, ConfigDict, Field

log = logging.getLogger("compliance.consent")

SCOPE_SELLER = "SELLER"
SCOPE_ALL = "ALL"
SCOPES = (SCOPE_SELLER, SCOPE_ALL)

FORM_WRITTEN = "WRITTEN"
FORM_ORAL = "ORAL"

PROVENANCE_SYNTHETIC = "SYNTHETIC_FIXTURE"
PROVENANCE_CONSUMER_DIRECT = "CONSUMER_DIRECT"

# RFC 5737 reserves 192.0.2.0/24 for documentation. Using it guarantees no
# real address can appear in a committed artefact, which is the same rule
# fixtures/README.md applies to phone numbers via the 555-0100 range.
_DOC_NET = "192.0.2."

# The disclosure the synthetic consumer is deemed to have been shown. Fixed
# text so its hash is stable across builds, and worded as what it is.
SYNTHETIC_DISCLOSURE_TEXT = (
    "By checking this box I agree to be contacted by telephone, including by "
    "automated technology and prerecorded voice, at the number provided, by "
    "the seller named above regarding my motor vehicle accident. Consent is "
    "not a condition of purchase. I may revoke consent at any time."
)
SYNTHETIC_DISCLOSURE_URL = "https://example.invalid/consent/crash-to-contact/v1"
SYNTHETIC_USER_AGENT = (
    "Mozilla/5.0 (fixture; synthetic) CrashToContactFixture/1.0"
)
SYNTHETIC_SELLER = "CRASH_TO_CONTACT_DEMO_SELLER"


class ConsentProvenance(BaseModel):
    """Every field ASSIGNMENT.md 5c requires, typed.

    A pydantic model rather than a dict because the whole point is that a
    missing field is an ERROR a caller has to handle, and a dict makes it a
    `None` that flows downstream. `model_config` forbids extra keys so a
    typo'd field name fails at construction instead of silently not being the
    field the gate looks for.
    """

    model_config = ConfigDict(extra="forbid")

    party_token: str
    provenance_kind: str = PROVENANCE_SYNTHETIC
    consent_form: str = FORM_WRITTEN

    text_presented: str | None = None
    disclosure_hash: str | None = None
    disclosure_url: str | None = None
    obtained_at: datetime | None = None
    obtained_at_timezone: str | None = None
    ip_address: str | None = None
    user_agent: str | None = None
    signature_event: str | None = None
    sellers_named: list[str] = Field(default_factory=list)
    source_chain: list[str] = Field(default_factory=list)

    def missing(self, required: Sequence[str]) -> list[str]:
        """Which required fields are absent or empty, in the declared order.

        Emptiness counts as absence: a consent record with `sellers_named: []`
        names no seller, and 47 C.F.R. 64.1200(f)(9) requires the specific
        seller to be disclosed.
        """
        out: list[str] = []
        for name in required:
            value = getattr(self, name, None)
            if value is None or (isinstance(value, (str, list)) and len(value) == 0):
                out.append(name)
        return out

    def is_complete(self, required: Sequence[str]) -> bool:
        return not self.missing(required)

    def as_contract(self) -> dict[str, Any]:
        """The `consent` object of contracts/lead_output.schema.json.

        Deliberately omits `text_presented`: the contract does not ask for it
        and a verbatim disclosure in a committed CSV is bulk, not evidence.
        The HASH is what proves which text was shown, and the text itself
        stays in the vault where the burden of proof can reach it.
        """
        return {
            "obtained_at": _iso(self.obtained_at),
            "disclosure_hash": self.disclosure_hash,
            "disclosure_url": self.disclosure_url,
            "ip_address": self.ip_address,
            "user_agent": self.user_agent,
            "sellers_named": list(self.sellers_named),
            "source_chain": list(self.source_chain),
            "revoked_at": None,
        }

    def as_row(self) -> dict[str, Any]:
        return {
            "party_token": self.party_token,
            "provenance_kind": self.provenance_kind,
            "consent_form": self.consent_form,
            "text_presented": self.text_presented,
            "disclosure_hash": self.disclosure_hash,
            "disclosure_url": self.disclosure_url,
            "obtained_at": _iso(self.obtained_at),
            "obtained_at_timezone": self.obtained_at_timezone,
            "ip_address": self.ip_address,
            "user_agent": self.user_agent,
            "signature_event": self.signature_event,
            "sellers_named": "|".join(self.sellers_named),
            "source_chain": "|".join(self.source_chain),
        }


def _iso(value: datetime | None) -> str | None:
    return value.isoformat() if value is not None else None


class Revocation(BaseModel):
    """One immutable revocation. Append-only; nothing ever updates a row."""

    model_config = ConfigDict(extra="forbid")

    party_token: str
    seller: str
    scope: str = SCOPE_SELLER
    received_at: datetime
    honour_by: date
    channel: str = "UNKNOWN"

    def covers(self, seller: str) -> bool:
        """Does this revocation reach `seller`?

        `ALL` reaches every seller -- the FCC revoke-all provision, honoured
        now rather than on its 2027-01-31 compliance date.
        """
        return self.scope == SCOPE_ALL or self.seller == seller

    def as_row(self) -> dict[str, Any]:
        return {
            "party_token": self.party_token,
            "seller": self.seller,
            "scope": self.scope,
            "received_at": self.received_at.isoformat(),
            "honour_by": self.honour_by.isoformat(),
            "channel": self.channel,
        }


def add_business_days(start: date, days: int) -> date:
    """`start` plus N business days, Monday-Friday, holidays not subtracted."""
    current = start
    remaining = days
    while remaining > 0:
        current += timedelta(days=1)
        if current.weekday() < 5:
            remaining -= 1
    return current


def make_revocation(
    *, party_token: str, seller: str, received_at: datetime,
    honour_business_days: int, scope: str = SCOPE_SELLER, channel: str = "UNKNOWN",
) -> Revocation:
    if scope not in SCOPES:
        raise ValueError(f"scope must be one of {SCOPES}, got {scope!r}")
    return Revocation(
        party_token=party_token, seller=seller, scope=scope,
        received_at=received_at, channel=channel,
        honour_by=add_business_days(received_at.date(), honour_business_days),
    )


def is_revoked(
    revocations: Iterable[Revocation | dict[str, Any]],
    party_token: str,
    seller: str,
    as_of: date,
) -> Revocation | None:
    """The earliest revocation in force for (token, seller) at `as_of`, or None.

    "In force" is `received_at <= as_of`, NOT `honour_by <= as_of`: honouring
    early is always permitted and the ten business days is a deadline for the
    caller, not a grace period for continuing to call.
    """
    best: Revocation | None = None
    for entry in revocations:
        rev = entry if isinstance(entry, Revocation) else _revocation_from_row(entry)
        if rev.party_token != party_token or not rev.covers(seller):
            continue
        if rev.received_at.date() > as_of:
            continue
        if best is None or rev.received_at < best.received_at:
            best = rev
    return best


def _revocation_from_row(row: dict[str, Any]) -> Revocation:
    return Revocation(
        party_token=str(row["party_token"]),
        seller=str(row["seller"]),
        scope=str(row.get("scope", SCOPE_SELLER)),
        received_at=_parse_dt(row["received_at"]),
        honour_by=_parse_date(row["honour_by"]),
        channel=str(row.get("channel", "UNKNOWN")),
    )


def revocations_from_frame(frame: pd.DataFrame) -> list[Revocation]:
    if frame is None or frame.empty:
        return []
    return [_revocation_from_row(r) for r in frame.to_dict("records")]


def _parse_dt(value: Any) -> datetime:
    if isinstance(value, datetime):
        return value
    return datetime.fromisoformat(str(value))


def _parse_date(value: Any) -> date:
    if isinstance(value, datetime):
        return value.date()
    if isinstance(value, date):
        return value
    return date.fromisoformat(str(value))


# ---------------------------------------------------------------------------
# synthesis from the fixture
# ---------------------------------------------------------------------------


def synthesise_provenance(
    *,
    party_id: str,
    party_token: str,
    obtained_on: date,
    timezone_name: str | None,
    seller: str = SYNTHETIC_SELLER,
    consent_form: str = FORM_WRITTEN,
) -> ConsentProvenance:
    """A complete, deterministic consent record for one fixture row.

    Deterministic in every field: the IP octet and the signature-event id are
    functions of `party_id`, the disclosure hash is a function of the fixed
    text, and the timestamp is noon UTC on the supplied date. Two builds
    therefore produce identical provenance rows and identical decision lineage
    ids, which is the property `ASSIGNMENT.md` Part 6 asks to be proven.

    `obtained_on` is the row's `report_filing_date` where it has one, else its
    `incident_date`: consent that predates the accident it is about would be a
    consent to something else, and a synthesised value should at least not be
    incoherent.
    """
    seed = hashlib.sha256(party_id.encode("utf-8")).hexdigest()
    octet = 1 + int(seed[:2], 16) % 254           # 192.0.2.1 .. 192.0.2.254
    return ConsentProvenance(
        party_token=party_token,
        provenance_kind=PROVENANCE_SYNTHETIC,
        consent_form=consent_form,
        text_presented=SYNTHETIC_DISCLOSURE_TEXT,
        disclosure_hash=hashlib.sha256(
            SYNTHETIC_DISCLOSURE_TEXT.encode("utf-8")
        ).hexdigest(),
        disclosure_url=SYNTHETIC_DISCLOSURE_URL,
        obtained_at=datetime.combine(obtained_on, time(12, 0), tzinfo=timezone.utc),
        obtained_at_timezone=timezone_name or "UTC",
        ip_address=f"{_DOC_NET}{octet}",
        user_agent=SYNTHETIC_USER_AGENT,
        signature_event=f"CHECKBOX:{seed[:16]}",
        sellers_named=[seller],
        source_chain=["fixtures/generate_fixture.py", "fixtures/synthetic_parties.csv"],
    )


def synthesise_revocation(
    *, party_token: str, received_on: date, honour_business_days: int,
    seller: str = SYNTHETIC_SELLER,
) -> Revocation:
    """A revocation for a fixture row whose `consent_revoked` is true.

    The fixture carries a BOOLEAN and no timestamp, so `received_at` is noon
    UTC on `received_on` (the row's filing date). That is a synthesised value
    and it is marked as one in the report's "what the data doesn't do"
    section; nothing in the disposition depends on the exact instant, only on
    it being at or before `as_of`, which every fixture date is.
    """
    return make_revocation(
        party_token=party_token,
        seller=seller,
        scope=SCOPE_SELLER,
        received_at=datetime.combine(received_on, time(12, 0), tzinfo=timezone.utc),
        honour_business_days=honour_business_days,
        channel="SYNTHETIC_FIXTURE",
    )
