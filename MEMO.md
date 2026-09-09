# Crash-to-contact: decision memo

**To:** Contact-center leadership<br>
**Date:** 9 September 2026<br>
**Recommendation:** Do not launch a live outbound crash-lead product from these public
records. The lawful daily contactable volume is zero. Retain the pipeline as an audited
analytics and consented-inbound foundation while counsel and product select a viable use.

## 1. What I built, and what I deliberately did not build

**Built**

- A restartable daily acquisition path for Montgomery County, a bounded but full-scale
  pagination demonstration for Texas, and six annual federal fatal-crash files. Raw source
  evidence is retained and later revisions do not overwrite history.
- A reconciled crash model with parties at their proper grain, a severity vocabulary,
  duplicate-crash resolution, county and neighborhood context, road proximity, local time,
  weather context, and three interpreted spatial analyses.
- A default-closed eligibility decision for every record, with every applicable reason,
  citation, rule version and immutable audit reference; fabricated parties exercise
  consent, revocation, suppression, telephone routing and waiting periods without real PII.
- An explainable priority score that runs only after eligibility, plus contracts, tests,
  monitored daily orchestration, recovery proofs and a 10× operating estimate.

**Deliberately not built**

- CRSS was excluded because it is a weighted probability sample that cannot produce state
  estimates; combining it with census-like sources would make totals wrong.
- Drive-time isochrones were deferred because corridor and compliance controls carried more
  decision value; straight-line access would overstate road access.
- Texas road matching was not run statewide. The local demonstration establishes the
  method, while the large extract and multi-zone projection work add hours without changing
  the legal conclusion.
- Weather was limited to recent Montgomery history. Full-history calls would exceed the
  free daily request budget; station observations were rejected because rural missingness
  would introduce a geographic bias.
- Federal fatal records before 2019 were not loaded; six years are enough to prove annual
  revision handling and source overlap without processing fifty years.
- A fitted machine-learning score was rejected because there is no defensible outcome for
  training or independent calibration. The transparent score cannot outrank injury severity
  with weak context.
- No production identity purchase, skip trace, call delivery, encryption/key rotation,
  deletion execution, hosted operations, or California broker workflow was built. Each
  requires a lawful purpose, accountable owner, credentials and an operational rollback.

*Where to look: `README.md`, `DECISIONS.md`, `OPERABILITY.md`, and `SCORING.md`.*

## 2. Daily contactable volume and exclusions

The daily pipeline produces **zero contactable records per day in Maryland, Texas and
Florida**. This is not a low-volume forecast: the complete current decision run evaluated
268,493 crashes and made every one ineligible. The public sources have no lawful identity
and telephone layer. Montgomery exposes opaque identifiers, Texas removes contact fields,
and the federal source has no names. No production join to the fabricated party file
exists.

| Jurisdiction | records evaluated | contactable per daily run | ineligible |
|---|---:|---:|---:|
| Maryland | 128,026 | **0** | 128,026 |
| Texas | 121,556 | **0** | 121,556 |
| Florida | 18,911 | **0** | 18,911 |
| **Total** | **268,493** | **0** | **268,493** |

Each record can have several simultaneous exclusions, so code counts exceed records. The
largest production exclusions are:

| Jurisdiction | reason code | records |
|---|---|---:|
| MD | consent absent / live solicitation prohibited / Maryland telephone bar | 128,026 each |
| MD | suppression refresh / line type unresolved / reassignment check unresolved | 128,026 each |
| MD | road distance / coarse geography / invalid coordinate | 476 / 112 / 105 |
| TX | consent absent / live solicitation prohibited / redacted contact data | 121,556 each |
| TX | suppression refresh / line type unresolved / reassignment check unresolved | 121,556 each |
| TX | coarse geography | 7,297 |
| FL | consent absent / confidential crash report / live solicitation prohibited | 18,911 each |
| FL | missing filing date / suppression refresh / line type / reassignment unresolved | 18,911 each |
| FL | coarse geography | 20 |

The fabricated 40-row harness demonstrates machinery, not daily volume: 21 eligible, four
waiting, and 15 ineligible. It produces these code counts across statuses: consent proven
35; live-solicitation bar 5; no permissible motor-record use 5; road-distance failure 5;
consent absent 3; consent revoked 2; suppression listed 2; Florida 60-day hold 2; Texas
31-day hold 2; restricted internet telephone line 2; and one each for Florida 30-day hold,
invalid coordinate, stale suppression check, unresolved line type, missing reassignment
answer and reassigned number. Counts reconcile to the committed exclusion table; codes
overlap by design.

*Where to look: `COMPLIANCE.md` §The fixture harness versus production and §Exclusion
table; the authoritative fields are in the compliance manifest.*

## 3. Can these records lawfully go to a live outbound call center?

The federal baseline is restrictive. The Driver's Privacy Protection Act, 18 U.S.C.
§§2721–2725, permits personal information only for enumerated uses. Solicitation appears
at §2721(b)(12) only where the state obtained affirmative express consent; §2725 defines
the protected information. Litigation support is not a prospecting exception. ABA Model
Rule 7.3(b) also bars a lawyer or agent from live person-to-person solicitation, including
live telephone, absent a valid exception.

**Maryland: zero — no compliant path for a live outbound call center.** Md. Gen. Prov.
§4-320 prohibits use of motor-vehicle personal information for telephone solicitation,
and Md. Rule 19-307.3 bars the lawyer or agent's live approach. Applying §4-320 beyond the
MVA custodian to this use of a police feed is the principal judgment call; even the narrower
reading does not overcome the professional-conduct bar.

**Texas: zero — no compliant path for a live outbound call center.** Tex. Transp. Code
§550.065 makes the bulk product a redacted report without the name, full address or phone
needed to call. Tex. Penal Code §38.12 separately makes telephone solicitation for economic
benefit barratry and reaches the caller. The public Texas layer is plainly mislabeled, but
it is the agency's unauthenticated publication of the redacted crash table identified in
the CRIS guide. I use it for crash analytics, record the label as a provenance risk, and do
not treat public access as permission to contact.

**Florida: zero — no compliant path for a live outbound call center.** Fla. Stat.
§316.066 makes identifying crash reports confidential for 60 days, requires a sworn basis
for early access, and criminalizes knowing misuse. Bar Rule 4-7.18 adds a 30-day targeted
written-contact wait and bars live solicitation. There is no Florida crash-report feed in
this build, but the rules and structural-incompleteness monitor are in scope.

*Where to look: `COMPLIANCE.md` §Source eligibility, §Blackout windows and §Channel rules.*

## 4. Viable product shapes

**Aggregate safety analytics.** The system can sell agencies, fleets, insurers and road
operators trend, corridor and quality analysis without identifying or contacting a crash
party. The live example shows raw crash clustering is much stronger than per-resident risk;
all 55 statistically significant raw-volume hot cells disappear after population
normalization. That is useful site-planning evidence and a warning against treating resident
demographics as exposure.

**Consented inbound.** A consumer can initiate contact and give purpose-specific,
seller-specific, revocable consent. The system already records disclosure evidence,
source chain and revocation and can suppress downstream use. Product launch still needs a
real consent origin, contract controls, recording rules and counsel approval; the fabricated
harness proves behavior, not acquisition.

**Enumerated DPPA work.** Section 2721(b) can support government or court functions,
insurer claims investigation and anti-fraud work when the customer, purpose and minimum
data actually fit the exception. Section 2721(b)(5) can support statistical research only
when personal information is not published or used to contact people. Litigation services
under §2721(b)(4) do not plausibly cover client solicitation, and §2721(b)(12) is unavailable
without state-obtained express consent.

**B2B safety partnerships.** Insurer or fleet customers can receive de-identified crash-risk
signals for prevention, routing and claims operations under their own lawful relationship.
The deliverable should be an area/corridor alert or a customer-supplied policy/vehicle match,
not a list of unrelated crash parties.

*Where to look: `ANALYSIS.md`, `SCORING.md` §ACS and protected proxies, and
`COMPLIANCE.md` §Consent.*

## 5. What I would build next quarter

1. Obtain written counsel approval for one product, customer type, permissible purpose,
   contact actor and jurisdiction. Until that dependency clears, no identity source or
   dialer integration begins.
2. Persist the complete rule body once per version so an 18-month-old decision can be
   reconstructed without relying on repository history; add legal-change ownership and
   approval workflow.
3. Add a lawful Florida source if one exists, then use the existing recent-window monitor
   as an acceptance sensor so statutory withholding cannot masquerade as an outage.
4. Contract a real identity/consent source only if its provenance proves the approved use;
   exercise revoke-all, key rotation, deletion propagation and backup recovery before load.
5. Complete statewide Texas road matching and remeasure state-zone thresholds, then add
   trauma-center drive-time only for aggregate safety or claims uses.
6. Host orchestration with alerts, catalog ownership and service-level objectives; obtain
   commercial weather terms and rerun the 10× sizing measurement in that environment.
7. Commission an independent audit of every census and geographic feature, including
   correlated proxies, before any prioritization experiment.

*Where to look: `OPERABILITY.md`, `DECISIONS.md`, and `COMPLIANCE.md` §Data protection.*

## 6. Where this pipeline is most likely wrong, and how I would find out

- The fabricated record claiming a roughly 400 m road miss is only 9.12 m from the current
  road extract. A source-version change or bad note is likely; the snap manifest, golden
  fixture and reference hash expose it.
- Nine Texas county mappings were initially wrong because two agencies sort names
  differently; a polygon-versus-source county monitor caught 1,336 affected records. Keep
  that comparison blocking on every reference refresh.
- Reanalysis weather is nearly always present and spatially smooth. Availability monitoring
  will look healthy while conditions are wrong locally; compare sampled hours with officer
  reports and independent stations, and alert on agreement shifts.
- Hot-spot significance depends on simulation count. The planned 999 runs could not resolve
  the correction threshold; a permutation sweep and stored resolution-limit flag now catch
  this, but should rerun when the number of cells changes.
- Fifty-nine fall-back timestamps occur twice. Choosing the earlier occurrence is arbitrary;
  the ambiguity flag should block time-sensitive operations and be audited against source
  narrative or dispatch time where available.
- The Maryland conclusion extends a custodian-focused statute to the proposed use. Counsel
  must sign that interpretation and review it on rule change; the narrower interpretation
  remains visible in the rule memo rather than erased.
- The 1 September 2026 frozen clock makes results reproducible but is unsafe if copied into
  production. A deployment check must require an explicit run date and alert when it differs
  from the scheduler partition.
- Six synthetic crash/party proximity matches are close to the 4.19 expected by chance.
  A lawful deterministic identity key—not proximity—must be required before any match can
  affect a person.

*Where to look: `DATA_QUALITY.md`, the geo/analysis/compliance manifests,
`IMPLEMENTATION_GUIDE.md` §10 Q6, and `tests/`.*

**Word count:** 1,822 (`wc -w MEMO.md`; headings and tables included).
