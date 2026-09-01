"""Machine-readable eligibility reason codes.

Extend this. The list below is a starting point, not a complete taxonomy.
Every code you add must carry a citation — a reason code without a legal
basis is an opinion, not a control.
"""

from enum import StrEnum


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

    PROVENANCE_UNKNOWN = "PROVENANCE_UNKNOWN"
    """A record whose acquisition basis cannot be stated cannot be contacted."""

    # --- Time-based gates ---------------------------------------------------
    FL_CRASH_REPORT_60D = "FL_CRASH_REPORT_60D"       # Fla. Stat. 316.066(2)
    FL_SOLICITATION_30D = "FL_SOLICITATION_30D"       # Fla. Bar 4-7.18(b)(1)(A)
    TX_SOLICITATION_31D = "TX_SOLICITATION_31D"       # Tex. Penal Code 38.12(d)(2)(C)
    AVIATION_45D = "AVIATION_45D"                     # 49 U.S.C. 1136(g)(2)

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
    LINE_TYPE_UNRESOLVED = "LINE_TYPE_UNRESOLVED"
    OUTSIDE_CALLING_WINDOW = "OUTSIDE_CALLING_WINDOW"  # 16 C.F.R. 310.4(c)
    CONSENT_ABSENT = "CONSENT_ABSENT"
    CONSENT_UNVERIFIABLE = "CONSENT_UNVERIFIABLE"
    CONSENT_REVOKED = "CONSENT_REVOKED"                # 47 C.F.R. 64.1200(a)(10)

    # --- Data quality -------------------------------------------------------
    GEOCODE_TIER_INSUFFICIENT = "GEOCODE_TIER_INSUFFICIENT"
    COORDINATE_OUT_OF_ENVELOPE = "COORDINATE_OUT_OF_ENVELOPE"
    SNAP_DISTANCE_EXCEEDED = "SNAP_DISTANCE_EXCEEDED"
    TIMEZONE_UNRESOLVED = "TIMEZONE_UNRESOLVED"

    # --- Affirmative -------------------------------------------------------
    ELIGIBLE_CONSENTED = "ELIGIBLE_CONSENTED"
    ELIGIBLE_EBR = "ELIGIBLE_EBR"
