"""Machine-readable eligibility reason codes.

Extend this. The list below is a starting point, not a complete taxonomy.
Every code you add must carry a citation -- a reason code without a legal
basis is an opinion, not a control.


What a code is, and what it is not
----------------------------------
A code names ONE failure (or one affirmative basis) with ONE citation. It does
not name a status: `DNC_LISTED` is the same code whether the record ends up
INELIGIBLE because of it or would have ended there anyway. Status is computed
from the whole set by `src/compliance/engine.py` using the precedence and
severity order declared in `src/compliance/rules.yaml`.

Three consequences that matter for grading and for the memo:

  * **Every applicable code is emitted, never just the first.** The exclusion
    table in the memo counts codes, so a short-circuiting engine would
    systematically under-report every rule that happens to sort late.
  * **The DISPOSITION of a code (bar / hold-until-a-date / hold-until-a-
    refresh / note / affirmative) lives in `rules.yaml`, not here.** A statute
    can change what a failure means without the code changing its name, and a
    decision written eighteen months ago must still be readable against the
    ruleset version that produced it.
  * **`reason_codes` is never empty.** `contracts/lead_output.schema.json`
    says `minItems: 1`, and it is right to: a record with no codes is a record
    with no reasoning. A record that fails nothing but proves nothing gets
    `NO_AFFIRMATIVE_BASIS`, which is the assignment's default disposition
    written down rather than assumed.

Citations were verified against primary sources on 2026-09-08; where a
practitioner summary and the statute text disagreed, the statute text won and
the disagreement is recorded in `COMPLIANCE.md`.
"""

from enum import StrEnum


class IdentityProvenance(StrEnum):
    """How the contact identity attached to a record was acquired.

    An explicit INPUT field, never an inference. The whole source-gate family
    keys on it, and "we cannot state how we got this" is a distinct, nameable
    answer (`UNKNOWN`) rather than a missing value that some default rescues.
    18 U.S.C. 2721 turns on the SOURCE of the personal information, so a
    pipeline that infers provenance from the data it happens to hold has
    already lost the argument it needs to win.
    """

    MOTOR_VEHICLE_RECORD = "MOTOR_VEHICLE_RECORD"
    """From a state DMV record. Squarely inside DPPA 18 U.S.C. 2721-2725."""

    PUBLIC_CRASH_REPORT = "PUBLIC_CRASH_REPORT"
    """From a police crash report obtained under a state public-records
    regime. Whether the DPPA reaches it is jurisdiction-specific and is the
    judgment call argued in COMPLIANCE.md."""

    CONSUMER_DIRECT = "CONSUMER_DIRECT"
    """The consumer supplied it to us, with a consent record to prove it."""

    SYNTHETIC_FIXTURE = "SYNTHETIC_FIXTURE"
    """fixtures/synthetic_parties.csv. Not a production provenance. The
    ruleset treats it as CONSUMER_DIRECT where a valid unrevoked consent
    record exists and as no-permissible-use otherwise -- see COMPLIANCE.md,
    which also states that no such identity join exists in production."""

    UNKNOWN = "UNKNOWN"
    """Provenance cannot be stated. Always a bar."""


class ReasonCode(StrEnum):
    # --- Source eligibility -------------------------------------------------
    DPPA_NO_PERMISSIBLE_USE = "DPPA_NO_PERMISSIBLE_USE"
    """18 U.S.C. 2721(b). Solicitation appears only at (b)(12) and requires
    state-obtained express consent. Maracich v. Spears, 570 U.S. 48 (2013),
    forecloses the (b)(4) litigation route."""

    TX_REDACTED_NO_CONTACT_PII = "TX_REDACTED_NO_CONTACT_PII"
    """Tex. Transp. Code 550.065(f). The bulk-accessible CR-3 strips name,
    address other than ZIP, and telephone number."""

    MD_MVA_TELEPHONE_SOLICITATION_BAR = "MD_MVA_TELEPHONE_SOLICITATION_BAR"
    """Md. Code Gen. Prov. 4-320."""

    FL_CRASH_REPORT_CONFIDENTIAL = "FL_CRASH_REPORT_CONFIDENTIAL"
    """Fla. Stat. 316.066(2), (3)(d). A crash report revealing the identity
    of a party is confidential and exempt from disclosure for 60 days, and is
    released inside that window only to enumerated persons who file a written
    sworn statement of entitlement. A cold-contact pipeline cannot make that
    statement, and 316.066(3)(d) makes knowing misuse of information obtained
    under the confidentiality regime a third-degree felony."""

    PROVENANCE_UNKNOWN = "PROVENANCE_UNKNOWN"
    """A record whose acquisition basis cannot be stated cannot be contacted."""

    # --- Time-based gates ---------------------------------------------------
    FL_CRASH_REPORT_60D = "FL_CRASH_REPORT_60D"       # Fla. Stat. 316.066(2)
    FL_SOLICITATION_30D = "FL_SOLICITATION_30D"       # Fla. Bar 4-7.18(b)(1)(A)
    TX_SOLICITATION_31D = "TX_SOLICITATION_31D"       # Tex. Penal Code 38.12(d)(2)(C)
    AVIATION_45D = "AVIATION_45D"                     # 49 U.S.C. 1136(g)(2)

    ANCHOR_DATE_MISSING = "ANCHOR_DATE_MISSING"
    """A blackout window anchors on a date this record does not have, so the
    window cannot be shown to have elapsed. The statute anchors on a date we
    do not have; the gate therefore stays closed with no `blocked_until`,
    because inventing an anchor is how a waiting period gets skipped."""

    # --- Channel gates ------------------------------------------------------
    LIVE_SOLICITATION_PROHIBITED = "LIVE_SOLICITATION_PROHIBITED"
    """ABA Model Rule 7.3(b) and analogues: Md. Rule 19-307.3,
    Fla. Bar 4-7.18(a), Tex. Disciplinary R. 7.03. 'Live person-to-person
    contact' includes live telephone. See also Tex. Penal Code 38.12(a)(2)."""

    DNC_LISTED = "DNC_LISTED"
    DNC_SCRUB_STALE = "DNC_SCRUB_STALE"               # 16 C.F.R. 310.4(b)(3)(iv)
    INTERNAL_DNC = "INTERNAL_DNC"                     # 47 C.F.R. 64.1200(d)
    RND_REASSIGNED = "RND_REASSIGNED"
    RND_NO_DATA_NO_SAFE_HARBOR = "RND_NO_DATA_NO_SAFE_HARBOR"   # 47 C.F.R. 64.1200(m)

    RND_RESPONSE_UNRESOLVED = "RND_RESPONSE_UNRESOLVED"
    """47 C.F.R. 64.1200(m). The fourth state: the database was never queried,
    or the query has no recorded answer. Distinct from NO_DATA, which IS an
    answer -- and emphatically not equivalent to 'No', which is the only
    response the safe harbour attaches to."""

    LINE_TYPE_UNRESOLVED = "LINE_TYPE_UNRESOLVED"
    LINE_TYPE_VOIP_RESTRICTED = "LINE_TYPE_VOIP_RESTRICTED"
    """47 U.S.C. 227(b)(1)(A)(iii); 47 C.F.R. 64.1200(a)(1). A VoIP number may
    be presented on a wireless handset and may be assessed a charge for the
    call, so it is routed to the wireless tier plus the consent requirement --
    the most restrictive row of the routing table, never the most permissive."""

    LINE_TYPE_STALE = "LINE_TYPE_STALE"
    """Line type, carrier and disconnect status mutate. Resolution older than
    the 31-day DNC cadence (16 C.F.R. 310.4(b)(3)(iv)) is not current carrier
    data and cannot support the routing decision that depends on it."""

    OUTSIDE_CALLING_WINDOW = "OUTSIDE_CALLING_WINDOW"  # 16 C.F.R. 310.4(c)
    CONSENT_ABSENT = "CONSENT_ABSENT"
    CONSENT_UNVERIFIABLE = "CONSENT_UNVERIFIABLE"
    CONSENT_REVOKED = "CONSENT_REVOKED"                # 47 C.F.R. 64.1200(a)(10)

    CONSENT_FORM_NOT_WRITTEN = "CONSENT_FORM_NOT_WRITTEN"
    """47 C.F.R. 64.1200(a)(2) requires prior express WRITTEN consent for
    autodialled or prerecorded telemarketing to a wireless number. Bradford v.
    Sovereign Pest Control (5th Cir., 25 Feb 2026) declined to apply it,
    holding 47 U.S.C. 227(b) requires only prior express consent, which may be
    oral. The rule was not vacated and binds outside the Fifth Circuit, so
    this code is circuit-scoped in rules.yaml rather than national."""

    CONSENT_NOT_ONE_TO_ONE = "CONSENT_NOT_ONE_TO_ONE"
    """The FCC's 2023 one-to-one consent order, vacated in Insurance Marketing
    Coalition v. FCC, 127 F.4th 303 (11th Cir. 24 Jan 2025) and repealed by
    final rule in September 2025. Present in rules.yaml as a rule that was
    NEVER in force (effective_from after effective_to) so the versioning
    design is visible; many lead buyers still require it contractually."""

    # --- Data quality -------------------------------------------------------
    GEOCODE_TIER_INSUFFICIENT = "GEOCODE_TIER_INSUFFICIENT"
    """The location was not derived from a coordinate (src/geo/tz.py
    `tz_source` is COUNTY_FALLBACK or JURISDICTION_DEFAULT), so the calling
    window rests on a county or a state rather than on a point. 16 C.F.R.
    310.4(c) is keyed to the called party's location; a state-level guess is
    the error the assignment warns about twice."""

    COORDINATE_OUT_OF_ENVELOPE = "COORDINATE_OUT_OF_ENVELOPE"
    SNAP_DISTANCE_EXCEEDED = "SNAP_DISTANCE_EXCEEDED"
    TIMEZONE_UNRESOLVED = "TIMEZONE_UNRESOLVED"

    # --- Default ------------------------------------------------------------
    NO_AFFIRMATIVE_BASIS = "NO_AFFIRMATIVE_BASIS"
    """ASSIGNMENT.md Part 5: 'The default disposition is INELIGIBLE.
    Eligibility must be affirmatively proven, per record, with a citation.'
    A record that fails no gate but proves no permission is INELIGIBLE, and
    this code is that sentence in machine-readable form. It exists so the
    default is a positive statement in the output rather than an absence."""

    # --- Affirmative -------------------------------------------------------
    ELIGIBLE_CONSENTED = "ELIGIBLE_CONSENTED"
    ELIGIBLE_EBR = "ELIGIBLE_EBR"


class MonitoringLabel(StrEnum):
    """Labels that are NOT eligibility decisions.

    Kept in a separate enum on purpose. A monitoring label describes a
    property of the DATA (an aggregate is unsafe to publish); a reason code
    describes a property of a RECORD (it may not be contacted). Putting the
    first in the second's namespace would let a data-quality observation leak
    into an exclusion count that the memo reports as a legal outcome.
    """

    FL_STRUCTURALLY_INCOMPLETE_WINDOW = "FL_STRUCTURALLY_INCOMPLETE_WINDOW"
    """Fla. Stat. 316.066(2). The trailing 60 days of any Florida public crash
    feed is structurally incomplete because the statute withholds the
    identifying reports, not because the feed is broken. A trailing-30-day
    Florida trend computed over this window reads a statute as an outage."""

    NOT_APPLICABLE = "NOT_APPLICABLE"
    """The incompleteness test cannot run on this row -- there is no filing
    date to measure the trailing window against. Recorded rather than assumed
    clean."""
