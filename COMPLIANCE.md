# Contact eligibility: the ruleset, in sentences

The machine-readable ruleset is `src/compliance/rules.yaml` (v1.0.0, sha256
`e4321de0…`) and `config/blackout_windows.csv`; this document is those two files in
prose, for a reader checking the law rather than the code. Every number below is a
parameter in one of them with a citation attached. Nothing in `src/compliance/` states
a legal requirement and no branch in it names a state.

Citations were checked against primary sources on 2026-09-08; where a practitioner
summary and the statute text disagreed, the statute text won. This is an engineering
reading, not legal advice, and the judgement calls are flagged as such.

---

## What a decision is

Every record leaving the pipeline carries seven fields:

| field | meaning |
|---|---|
| `eligibility_status` | `ELIGIBLE`, `INELIGIBLE` or `BLOCKED_UNTIL` |
| `blocked_until_date` | the date a time gate opens, or null |
| `reason_codes` | every applicable code, ordered by severity, never empty |
| `legal_basis` | one citation per code, **parallel array, same order** |
| `decision_lineage_id` | FK to an immutable audit record |
| `evaluated_at` | timestamptz |
| `ruleset_version` | semver |

**The default is INELIGIBLE.** Eligibility is proven per record, with a citation, or
it does not exist; a record that fails nothing and proves nothing still gets a code,
`NO_AFFIRMATIVE_BASIS`, so the default is a positive statement rather than an absence.
**Every gate runs on every record** — a short-circuiting engine would under-report
every rule that happens to sort late, and the exclusion table counts codes.

**Precedence.** Each code carries a *disposition*: **BAR** (non-curable as the
record stands), **HOLD_UNTIL_DATE** (a waiting period with a computable end),
**HOLD_UNTIL_REFRESH** (curable by re-running a lookup, not by waiting — a stale DNC
scrub, an unresolved line type, an RND "No Data", a missing anchor date), **NOTE**
(recorded, enforced elsewhere) and **AFFIRMATIVE**.

Any BAR or HOLD_UNTIL_REFRESH ⇒ **INELIGIBLE**. Otherwise any HOLD_UNTIL_DATE ⇒
**BLOCKED_UNTIL**, dated at the *maximum* over every time gate that binds. Otherwise
an affirmative basis ⇒ **ELIGIBLE**. Otherwise INELIGIBLE.

Two consequences. A record that is both DNC-listed *and* inside the Texas 31-day
window is INELIGIBLE, not BLOCKED_UNTIL — both codes are emitted, only the status
collapses, because publishing a date would say it becomes contactable then, which is
false. And the two holds are different answers: "not yet" has a date and belongs in
the queue; "we do not know" has none, and giving it one is how a control becomes a
filter.

**Severity order**, which orders `reason_codes` and its parallel citations: source
bars → provenance → consent → DNC/RND → time windows → channel, line type and window →
data quality → the default → affirmative.

---

## Source eligibility

**DPPA — 18 U.S.C. §§2721–2725.** Motor-vehicle-record personal information may be
disclosed only for fourteen enumerated permissible uses. Solicitation appears only at
§2721(b)(12), and only where *the State* obtained affirmative express consent — no
state runs such a programme at scale. *Maracich v. Spears*, 570 U.S. 48 (2013),
forecloses the workaround by name: an attorney's solicitation of clients is **not**
within the §2721(b)(4) litigation exception. §2724 exposure is actual damages with a
**$2,500 statutory floor per record**, plus punitive damages and fees. Any record
whose `identity_provenance` is `MOTOR_VEHICLE_RECORD` is barred everywhere.

**Texas — Tex. Transp. Code §550.065.** The only bulk-accessible product is the
redacted CR-3 required by §550.065(c-1), and §550.065(f) strips name, licence number,
date of birth other than year, address other than ZIP, telephone number, plate number
and insurer details. Plainly: **the fields a contact pipeline needs are exactly the
fields the statute removes.** That is arithmetic, not policy, and it makes the Texas
count zero before any other gate runs.

**Maryland — Md. Code Gen. Prov. §4-320**, the DPPA analogue. The custodian may not
disclose MVA personal information without the written consent of the person in
interest, may permit its use only for an MVA-approved purpose, and **may not disclose
it for use in telephone solicitations.**

*The judgement call.* §4-320 sits in the Public Information Act's required-denials
subtitle and binds the **MVA custodian's disclosure**; the Montgomery feed is a
*police crash report* released under a different provision, so reading §4-320 onto it
is a step. **The step is taken here**: Maryland has stated a public policy that
motor-vehicle personal information is not to be used for telephone solicitation, and
the county's own person table ships titled "(Anonymized)" because the same policy
reaches it. **The rejected reading** — that §4-320 binds only the MVA — is recorded
rather than dismissed, and would not change the outcome: such a record is still
barred by Md. Rule 19-307.3 whenever the caller acts for a lawyer.

**Florida — Fla. Stat. §316.066(2), (3)(d).** A crash report revealing a party's
identity is confidential and exempt for 60 days, released inside that window only to
enumerated persons who file a **written sworn statement** of entitlement. A
cold-contact pipeline cannot make that statement, and §316.066(3)(d) makes knowing
misuse of information obtained under that regime a **third-degree felony**. This is a
separate bar from the 60-day *time* window below; only the time window can lapse.

**Provenance unknown.** A record whose acquisition basis cannot be stated cannot be
contacted. `identity_provenance` is an explicit *input*, never an inference; a record
that never carried one matches the rule's `null` member, so forgetting to populate it
is an exclusion rather than a silent pass.

---

## Blackout windows

`config/blackout_windows.csv` is a `(jurisdiction, record_type) → earliest_contact_date`
table; a new jurisdiction is a row.

| Jurisdiction | Rule | Window | Anchor |
|---|---|---|---|
| FL | Fla. Stat. §316.066(2) | 60 days | filing date |
| FL | R. Reg. Fla. Bar 4-7.18(b)(1)(A) | 30 days | incident date |
| TX | Tex. Penal Code §38.12(d)(2)(C) | 31 days | incident date |
| **MD** | **Md. Code Gen. Prov. §4-320; Md. Rule 19-307.3** | **none — a channel bar, not a clock** | — |
| US | 49 U.S.C. §1136(g)(2) | 45 days | incident date (aviation only) |

**The Maryland row** has a null `days_from` because Maryland's constraint is a
channel bar, not a clock. That is not an unfinished row: the loader **refuses to
start** unless a null-window row names, in its notes, the codes of the rules that
carry the constraint instead — here `MD_MVA_TELEPHONE_SOLICITATION_BAR` and
`LIVE_SOLICITATION_PROHIBITED`. A blank in a time-window table is otherwise
indistinguishable from a jurisdiction nobody finished researching.

**The arithmetic is exclusive:** `earliest_contact_date = anchor + days + 1`. The
statutes do not pin down the boundary day and the two readings differ by one;
exclusive is chosen because a 31-day window opening on day 31 has waited thirty, and
because the downside is asymmetric — Tex. Penal Code §38.12 is criminal and Fla.
Stat. §316.066(3)(d) is a felony.

**The Florida interaction.** Both rows are evaluated and both emit their own code.
They anchor on *different dates* with *different lengths*, so neither subsumes the
other — and the data gate is longer, so it binds. A record 40 days past filing and 45
past the incident shows `FL_CRASH_REPORT_60D` **alone**: the bar rule has lapsed, and
listing it anyway would inflate the exclusion table with a dead constraint.

**A missing anchor** cannot be shown to have elapsed: the gate stays closed with
`ANCHOR_DATE_MISSING` and **no** date. Defaulting the anchor to the incident date, or
to today, is how a criminal waiting period gets skipped by a null.

**Adding Ohio** is one row in the CSV plus a version bump; the code
(`OH_WRITTEN_SOLICITATION_45D`) and the citation come from the row itself. A test
hashes `src/compliance/` before and after and asserts nothing under `src/` changed.

---

## Channel rules

**The calling window comes from geography.** 16 C.F.R. §310.4(c) and 47 C.F.R.
§64.1200(c)(1) key it to the called party's *location*. The IANA zone is resolved
from the party's coordinate; the area code — parsed from the E.164 number with the
country code stripped, never sliced off the front of a string — supplies a *set* of
zones as a fallback; when they disagree the per-zone windows are **intersected**,
which can only narrow. The stricter of federal and state hours then applies.

Verified state hours:

| | window | citation |
|---|---|---|
| Federal | 08:00–21:00 | 16 C.F.R. §310.4(c); 47 C.F.R. §64.1200(c)(1) |
| Florida | 08:00–20:00 | Fla. Stat. §501.616(6) |
| Maryland | 08:00–20:00 | Md. Code Com. Law §14-4502(c)(1) (Stop the Spam Calls Act of 2023, eff. 2024-01-01) |
| **Texas** | **09:00–21:00**, noon–21:00 Sunday | **Tex. Bus. & Com. Code §301.051** |

Texas opens an hour **later** than the federal floor. Several secondary sources
attribute that curfew to §302.101, which is in fact the registration-certificate
requirement; the statute text puts it at §301.051. No pipeline defaulting to 8 a.m.
would discover this.

The two worked examples the fixture exists to catch:

- **P007, El Paso (31.85, −106.53), NPA 915** → `America/Denver`. The *Texas default
  is Central*, so a state-derived window is an hour wrong in the direction that
  places a call after the permitted end. The NPA agrees. **09:00–21:00 Mountain**.
- **P008, Pensacola (30.42, −87.22), NPA 850** → `America/Chicago`. The Florida
  default is Eastern *and most of 850 is Eastern*, so both naive methods agree on the
  wrong answer. 850 maps to **{America/New_York, America/Chicago}**, and honouring
  both narrows the Central-clock window by an hour at the top: **08:00–19:00**.

**DNC is a freshness SLA, not a load.** 16 C.F.R. §310.4(b)(3)(iv) requires a
registry version obtained no more than **31 days** before the call, so
`DNC_SCRUB_STALE` fires when the scrub ages out **even if the number is not listed** —
what the 31 days buys is the safe harbour, and an aged scrub loses it whatever the
last answer was. A *missing* scrub date is stale too.

**Internal DNC** per 47 C.F.R. §64.1200(d)(3), (d)(6): honoured within 30 days,
retained 5 years, append-only. **EBR** per 16 C.F.R. §310.2(o) and 47 C.F.R.
§64.1200(f)(5): 18 months from a transaction, 3 from an inquiry. Both tables start
empty — there is no source of either here — so `ELIGIBLE_EBR` never fires on the
forty; both are unit-tested instead, because an untested affirmative path is where an
unlawful call comes from.

**The RND has three states and a silence.** The 47 C.F.R. §64.1200(m) safe harbour
attaches **only to "No"**. "Yes" is `RND_REASSIGNED`; **"No Data" is not a green
light** — it is the database saying it cannot answer — and yields
`RND_NO_DATA_NO_SAFE_HARBOR`; and no recorded response is a fourth, distinct fact
(`RND_RESPONSE_UNRESOLVED`), kept separate because collapsing it into "No Data" would
hide a broken integration.

**Line type routes to the most restrictive row.** `wireless` takes the 47 U.S.C.
§227(b)(1)(A)(iii) strict-liability tier, `landline` the §227(c) DNC regime, and
`voip`, `unknown` **and any value nobody anticipated** take the wireless tier plus the
consent requirement. A test asserts the VoIP routing *outcome* equals the wireless one
and is not the landline one.

**Consent is evidence, not a boolean.** The pipeline persists the exact text, a
disclosure hash, the URL, a timestamp with zone, IP, user agent, the signature event,
every seller named and the full lead-source chain. A record claiming consent with any
of those missing is `CONSENT_UNVERIFIABLE`: the burden of proof is on the caller.
Revocation trumps consent on file, is append-only and immutable, and is honoured from
`received_at` — the **10 business days** of 47 C.F.R. §64.1200(a)(10) is stored as
`honour_by`, the regulatory deadline, not a grace period. `scope ∈ {SELLER, ALL}` is a
first-class column **now** and `ALL` cascades across every seller **now**, not on the
FCC's current 2027-01-31 compliance date (DA 26-12): one column and one `or` clause
today, versus a search for every campaign that read a per-seller suppression later.

**Live solicitation.** Md. Rule 19-307.3, R. Reg. Fla. Bar 4-7.18(a) and Tex.
Disciplinary R. 7.03 bar solicitation by live person-to-person contact, which ABA Model
Rule 7.3 cmt. [2] defines to include live telephone; Tex. Penal Code §38.12(a)(2) makes
it barratry, a **third-degree felony reaching the caller** and not only the firm. The
configured actor is `ATTORNEY_OR_AGENT` — the product is accident leads for a contact
centre acting for lawyers.

*The assumption.* The bar is lifted by one path only: a valid, unrevoked
consumer-direct consent, on the Rule 7.3(b)(2) / 4-7.18(a)(1) analogue for a person who
has initiated contact with the lawyer. **This is an assumption about what the fixture's
`consent_on_file` represents**, and a real stretch — a consent to be contacted by a
lead buyer is not automatically a consumer-initiated approach to a specific lawyer. If
the principal is *not* a lawyer or their agent these rules do not bind; change `actor`
in `config/compliance.toml` and the answer moves with it, with no code change.

---

## The two unsettled areas (§5d)

A circuit split is **two data rows**, not a rewrite. Rules are effective-dated, the
engine evaluates only those in force at `as_of`, and a legal change is a **new row
plus a version bump**, never an edit — so replaying an old `as_of` replays the old law.

**One-to-one consent is dead.** Vacated in *Insurance Marketing Coalition v. FCC*,
127 F.4th 303 (11th Cir., 24 Jan 2025) and repealed by final rule in September 2025.
Present in the ruleset and **provably inert**:

```yaml
- id: CONSENT_ONE_TO_ONE
  status: VACATED
  contractual: true
  effective_from: 2025-01-27      # AFTER effective_to, so never in force
  effective_to:   2025-01-24
```

Kept rather than deleted: many lead buyers still require it *contractually*
(`contractual: true`) and a per-buyer ruleset switches it on by changing two dates,
and deleting a vacated rule destroys the record that we considered it. A test asserts
it fires at no date.

**Written consent is circuit-dependent.** *Bradford v. Sovereign Pest Control*
(5th Cir., 25 Feb 2026) held that 47 U.S.C. §227(b) requires only prior express
consent, which may be oral. The rule was not vacated and binds outside the Fifth
Circuit. That is one closed row and one open one:

```yaml
- id: CONSENT_WRITTEN_REQUIRED_NATIONAL
  scope: national
  effective_from: 2013-10-16
  effective_to:   2026-02-24
- id: CONSENT_WRITTEN_REQUIRED_EX_5TH
  scope: circuit
  excluded_circuits: ["5th"]
  effective_from: 2026-02-25
  effective_to:   null
```

A Texas record with oral consent **fails** at `as_of = 2026-02-01` and **passes** at
`as_of = 2026-09-01`; a Maryland one fails at both. No code knows Texas is the Fifth
Circuit — `jurisdiction_circuit` is a mapping in the ruleset. Practitioner guidance is
uniformly to keep obtaining written consent, which is why this is a carve-out rather
than a repeal.

**The loader refuses** a ruleset whose declared version is not the one the build
expects, or whose content hash does not match its declared prefix — catching both a
rule edited without a version bump and a version bumped with no rule change. Either
makes `ruleset_version` on a decision meaningless.

---

## Data protection

**The vault boundary is the schema, not a policy.** 18 U.S.C. §2725(3) excludes the
**5-digit ZIP code** from "personal information", and that exclusion is the only
lawful path to geographic aggregation for an MVR-sourced record. It is drawn as a
projection: `full_name`, `street_address`, `city`, `phone_e164` and the raw
coordinates never leave `data/vault/`; `party_token`, `phone_token`, `zip5`,
jurisdiction and values *derived* from the coordinate do. A test greps every byte
under `data/gold/` and `output/` for every fixture name, street and phone number and
finds none.

*One place this is softer than §2725(3) alone would draw it*: a census block group is
**finer** than a 5-digit ZIP, so on a genuinely MVR-sourced record the carve-out
justifies the ZIP and not the block group. They are emitted because
`contracts/lead_output.schema.json` has those fields and this pipeline does not get
to redefine its consumer's contract, and because the fixture rows are not
MVR-sourced. A production MVR-sourced feed would have to drop them.

**Tokens** are keyed HMAC-SHA256, not bare digests: an unkeyed hash of a ten-digit
NANP number is reversible in milliseconds, so an unkeyed "token" is the phone number
with extra steps. They are deterministic rather than random UUIDs because the token
*is* the join key, and a random surrogate needs a mapping table — a second vault.

**Access attribution.** Every vault read appends a row naming the reader, the table,
the purpose, the row count and the time — 18 U.S.C. §2721(c)'s five-year redisclosure
record. There is no default `purpose` argument: a read nobody had to justify is a read
nobody will be able to justify.

**Retention TTLs** are in the ruleset with citations: consent 5y, revocations 5y,
internal DNC 5y (47 C.F.R. §64.1200(d)(6)), access log 5y (§2721(c)), decision lineage
7y, vault identities 5y from last lawful contact. `retention.py` **reports what would
be deleted and deletes nothing** — deletion that propagates to backups and derived
tables is an operational control with a runbook and a rollback story, and a `delete()`
here would make the repo look compliant while leaving the copies. Lineage deliberately
outlives identity: it is the evidence a decision was lawful, and its token is
unresolvable once the vault row is gone.

**Encryption and key rotation** — not built. In production: TLS in transit; envelope
encryption at rest with a KMS-held key-encryption key and per-table data keys, rotated
annually and on suspicion. The HMAC vault key is harder, because rotating a *join key*
re-keys every derived table: a scheduled migration with a dual-write window, not a
rotation.

**California Delete Act** (Cal. Civ. Code §1798.99.80 et seq.) — not built. An
accident-lead vendor meets the data-broker definition almost exactly. Registration is
due annually by **31 January**, and DROP deletion-request processing has been mandatory
since **1 August 2026**, at least every 45 days, at **$200/day** per unprocessed
request. A registration and an operational SLA, not a code change.

**Recording consent** — not built. All-party-consent states include CA, FL, IL, MD, MA,
MT, NV, NH, PA and WA, and two of the three jurisdictions in scope are on that list. It
is a dialler configuration and a disclosure script, and it touches this pipeline only
in that both it and the calling window key on the called party's *location*.

---

## The fixture harness versus production

**This is the sentence the memo repeats.** `fixtures/synthetic_parties.csv` supplies
an identity layer that the three real sources deliberately do not.
**No such identity join exists in production.** Montgomery exposes an opaque GUID
with no name or address; the Texas layer is the redacted CR-3; Maryland's person
table ships titled "(Anonymized)"; FARS carries no names. That is the statutory
design, not an oversight in the data.

The ruleset treats a fixture row as consumer-direct **where a valid, unrevoked
consent record exists**, and as having no permissible use otherwise. The forty rows
therefore exercise tokenisation, routing, windows, DNC, RND and consent against
something concrete, and prove nothing about whether the join is available.

To make that concrete rather than rhetorical, the same engine is fed **every gold
crash with no identity layer attached** — the same `evaluate`,
`identity_provenance = PUBLIC_CRASH_REPORT`, no contact block. All **268,493** rows,
in 56 seconds:

| jurisdiction | crashes | ELIGIBLE | BLOCKED_UNTIL | INELIGIBLE |
|---|---:|---:|---:|---:|
| Maryland | 128,026 | **0** | 0 | 128,026 |
| Texas | 121,556 | **0** | 0 | 121,556 |
| Florida | 18,911 | **0** | 0 | 18,911 |
| **Total** | **268,493** | **0** | **0** | **268,493** |

**Zero deliverable leads for cold contact, in every jurisdiction in scope.** That is
the honest answer and it falls out of the data rather than being asserted.

---

## Exclusion table by reason code

**Crash-only run** (production truth, all 268,493 rows). Every row fails on several
grounds at once, which is why the counts exceed the corpus:

| jurisdiction | reason code | disposition | records |
|---|---|---|---:|
| MD | CONSENT_ABSENT | BAR | 128,026 |
| MD | LIVE_SOLICITATION_PROHIBITED | BAR | 128,026 |
| MD | MD_MVA_TELEPHONE_SOLICITATION_BAR | BAR | 128,026 |
| MD | DNC_SCRUB_STALE | HOLD_UNTIL_REFRESH | 128,026 |
| MD | LINE_TYPE_UNRESOLVED | HOLD_UNTIL_REFRESH | 128,026 |
| MD | RND_RESPONSE_UNRESOLVED | HOLD_UNTIL_REFRESH | 128,026 |
| MD | SNAP_DISTANCE_EXCEEDED | BAR | 476 |
| MD | GEOCODE_TIER_INSUFFICIENT | BAR | 112 |
| MD | COORDINATE_OUT_OF_ENVELOPE | BAR | 105 |
| TX | CONSENT_ABSENT | BAR | 121,556 |
| TX | LIVE_SOLICITATION_PROHIBITED | BAR | 121,556 |
| TX | TX_REDACTED_NO_CONTACT_PII | BAR | 121,556 |
| TX | DNC_SCRUB_STALE | HOLD_UNTIL_REFRESH | 121,556 |
| TX | LINE_TYPE_UNRESOLVED | HOLD_UNTIL_REFRESH | 121,556 |
| TX | RND_RESPONSE_UNRESOLVED | HOLD_UNTIL_REFRESH | 121,556 |
| TX | GEOCODE_TIER_INSUFFICIENT | BAR | 7,297 |
| FL | CONSENT_ABSENT | BAR | 18,911 |
| FL | FL_CRASH_REPORT_CONFIDENTIAL | BAR | 18,911 |
| FL | LIVE_SOLICITATION_PROHIBITED | BAR | 18,911 |
| FL | ANCHOR_DATE_MISSING | HOLD_UNTIL_REFRESH | 18,911 |
| FL | DNC_SCRUB_STALE | HOLD_UNTIL_REFRESH | 18,911 |
| FL | LINE_TYPE_UNRESOLVED | HOLD_UNTIL_REFRESH | 18,911 |
| FL | RND_RESPONSE_UNRESOLVED | HOLD_UNTIL_REFRESH | 18,911 |
| FL | GEOCODE_TIER_INSUFFICIENT | BAR | 20 |

Florida's `ANCHOR_DATE_MISSING` deserves its own sentence: the 60-day gate anchors on
a *report filing date*, and no crash feed in scope publishes one, so the window cannot
be shown to have elapsed and stays closed. That is part of the honest answer, not a
gap in it.

**Fixture run** (40 rows: 21 ELIGIBLE, 4 BLOCKED_UNTIL, 15 INELIGIBLE):

| jurisdiction | status | reason code | disposition | records |
|---|---|---|---|---:|
| MD | ELIGIBLE | ELIGIBLE_CONSENTED | AFFIRMATIVE | 13 |
| MD | ELIGIBLE | LINE_TYPE_VOIP_RESTRICTED | NOTE | 2 |
| MD | INELIGIBLE | DPPA_NO_PERMISSIBLE_USE | BAR | 5 |
| MD | INELIGIBLE | LIVE_SOLICITATION_PROHIBITED | BAR | 5 |
| MD | INELIGIBLE | SNAP_DISTANCE_EXCEEDED | BAR | 5 |
| MD | INELIGIBLE | CONSENT_ABSENT | BAR | 3 |
| MD | INELIGIBLE | CONSENT_REVOKED | BAR | 2 |
| MD | INELIGIBLE | DNC_LISTED | BAR | 2 |
| MD | INELIGIBLE | COORDINATE_OUT_OF_ENVELOPE | BAR | 1 |
| MD | INELIGIBLE | DNC_SCRUB_STALE | HOLD_UNTIL_REFRESH | 1 |
| MD | INELIGIBLE | LINE_TYPE_UNRESOLVED | HOLD_UNTIL_REFRESH | 1 |
| MD | INELIGIBLE | RND_NO_DATA_NO_SAFE_HARBOR | HOLD_UNTIL_REFRESH | 1 |
| MD | INELIGIBLE | RND_REASSIGNED | BAR | 1 |
| MD | INELIGIBLE | ELIGIBLE_CONSENTED | AFFIRMATIVE | 10 |
| TX | ELIGIBLE | ELIGIBLE_CONSENTED | AFFIRMATIVE | 4 |
| TX | BLOCKED_UNTIL | TX_SOLICITATION_31D | HOLD_UNTIL_DATE | 2 |
| FL | ELIGIBLE | ELIGIBLE_CONSENTED | AFFIRMATIVE | 4 |
| FL | BLOCKED_UNTIL | FL_CRASH_REPORT_60D | HOLD_UNTIL_DATE | 2 |
| FL | BLOCKED_UNTIL | FL_SOLICITATION_30D | HOLD_UNTIL_DATE | 1 |

Ten Maryland rows carry `ELIGIBLE_CONSENTED` **and** are INELIGIBLE. Not a
contradiction: the engine is saying "a valid consent exists *and* something else bars
the call", which is what a business reader needs in order to know whether a record is
recoverable.

Reproduce all of it with `python -m src.compliance.build --json`; the counts are in
`_compliance_manifest.json` under `stats.fixture` and `stats.crash_only`, and the
table is `data/gold/compliance/exclusion_by_code`.
