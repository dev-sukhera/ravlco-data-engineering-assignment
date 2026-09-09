# Phase 6 — Compliance engine: implementation brief

You are implementing Phase 6 of the Crash-to-Contact take-home in this repo. Phases 1–5
are complete and committed on `feature/dimensional-modeling` (Phase 5 = commits
`e9c5bc4` … `3745491`: `src/analysis/`, `ANALYSIS.md`, `output/figures/`). Your job is
ASSIGNMENT.md §5 — **the section that decides the outcome.** Every record leaving the
pipeline must carry a defensible, cited, auditable contact-eligibility decision. The
default is INELIGIBLE; eligibility is proven per record, never assumed. A submission that
emits contactable records without a per-record decision fails regardless of everything
else, and *"zero deliverable leads for jurisdiction X, with citations"* is a passing
answer. Build the machinery so that the honest answer falls out of data, and then write
the honest answer down.

Read these before writing code, in this order:

1. `ASSIGNMENT.md` §5 in full (5a–5e; every sentence is a rubric line), §0 ("What we
   are actually measuring"), §7 (the memo questions the engine must be able to answer —
   the per-jurisdiction lawfulness question and the exclusion table by reason code),
   §9 (the live defence: "add Ohio 45 days" must be a data change you can demo in under
   a minute), Appendix B (citations — every reason code carries one).
2. `src/compliance/engine.py` (the scaffold's design notes are grading criteria — keep
   the `EligibilityDecision` dataclass fields and the `evaluate(record: dict)` signature;
   the input stays a plain dict so the fixture harness and the pipeline share one code
   path), `src/compliance/reason_codes.py` (extend it; every code you add carries a
   citation docstring), `contracts/lead_output.schema.json` (**do not modify**; the
   output must validate against it row by row), `config/blackout_windows.csv` (the MD
   row is `INCOMPLETE` — you complete it, §3.1), `fixtures/README.md` and
   `fixtures/synthetic_parties.csv` (**do not modify**; joining against it is mandatory;
   treat every value as real PII: tokenise, never log, never commit an intermediate
   containing it).
3. `ai docs/implementation/phase4-geo-report.md` §Timezone and §"For DECISIONS.md" —
   the coordinate-derived timezone machinery you reuse (`src/geo/tz.py`: `ZoneFinder`,
   `resolve_zone`, `localise`, `jurisdiction_default`), the DST policy already decided
   (gap → shift forward + flag; ambiguous → fold=0 + flag), and the house style for
   decisions. `ai docs/implementation/phase5-analysis-report.md` §"Open items for
   Phases 6–7" and §9 of its "Things the spec said" (the `.gitignore` working-tree
   change — see Environment below).
4. `src/config.py` (`settings()`, `run_setting()`, `key()`, `GOLD_DIR`; add a
   `compliance()` accessor), `src/transform/common.py` (`write_parquet` validate-then-
   write with a total sort order, `BuildManifest`, `connect`, `hash_files`),
   `src/geo/build.py` and `src/analysis/build.py` (the CLI + manifest + lineage-sha
   pattern you copy), `src/contracts.py` + `contracts/gold.schema.json` (the dialect;
   add `contracts/compliance.schema.json`), `src/geo/envelope.py` /
   `src/geo/census_join.py` (`point_in_polygon`) / `src/geo/h3_index.py` /
   `src/geo/snap.py` (`snap_points`, `snap_crs_for`) — the party coordinates get the
   same enrichment a crash gets.
5. `tests/conftest.py`, `tests/test_geo.py`, `tests/test_analysis.py` — how gold is built
   into `tmp_path` from the committed bronze extract; no mocks; no network in tests.
   **Note `tests/test_engine.py` is already taken** (it holds the silver grammar and
   crosswalk unit tests). Engine tests go in a new `tests/test_compliance.py`.

Environment: Python venv at `.venv`; DuckDB 1.5.5 (spatial), geopandas 1.1.4, shapely
2.1.2, h3 4.5.0, timezonefinder 8.x, **pydantic 2.13.5, PyYAML 6.0.3, jsonschema 4.26.0**
(all installed), pandas 3.0, pyarrow 25, pytest 9. `phonenumbers` is **not** installed;
if you want it for the NPA-NXX → timezone fallback, add it to `requirements.txt` pinned
with a one-line reason (alternative: a small committed `config/npa_timezone.csv` covering
NANP area codes — either is fine, but the fallback must return a *set* of zones for an
area code that spans two, e.g. 850). Baseline before you touch anything:
`.venv/bin/python -m pytest -q` → **322 passed, 4 xfailed**. Full local gold exists under
`data/gold/` (gitignored); `crash_geo` covers Montgomery to 2026-09-02, TxDOT is a
bounded 100k slice, FARS is 2019–2024 — so only MD fixture rows can plausibly match a
real crash (§2).

**`git status` at start:** `.gitignore` is modified in the working tree and the diff
*removes* the `IMPLEMENTATION_GUIDE.md` and `ai docs/` ignore lines (Phase 5's report
item 9 describes this). Run `git checkout -- .gitignore` first so those stay ignored.
`src/compliance/{lineage.py,vault.py,rules.yaml}` are empty untracked scaffold files
you fill in this phase. `src/scoring/`, `orchestration/`, `notebooks/`, `ai docs/`,
`IMPLEMENTATION_GUIDE.md` stay untracked/ignored — never commit them.

---

## 1. Scope

**In scope (build fully):**

- **`src/compliance/rules.yaml`** — the ruleset as data, `version: 1.0.0` (semver), with
  every rule **effective-dated** (`effective_from`, `effective_to: null`) so a legal
  change is a new row + version bump, never an edit. Sections: `source_gates`
  (per `source_system` × `identity_provenance`: DPPA coverage and permissible use,
  TX §550.065 redaction, MD §4-320, provenance-unknown), `channel_gates` (DNC staleness
  31d, internal DNC 30d honour / 5y retain, EBR 18mo/3mo, RND tri-state, line-type
  routing table with `voip`/`unknown` → most restrictive, calling windows: federal
  08:00–21:00, FL 08:00–20:00 with citations, MD/TX researched and cited), `consent`
  (required provenance fields, revocation honoured ≤10 business days, revoke-all scope
  with its 2027-01-31 compliance date noted as effective-dated data), `live_solicitation`
  (Rule 7.3 analogues per state; the actor assumption — see §3.4), `status_precedence`,
  `reason_code_severity_order`. Every rule: `id`, `reason_code`, `legal_basis`,
  `params`, `effective_from/to`, `notes`. The engine reads the file and nothing else
  says what the law is.
- **`config/blackout_windows.csv`** — complete the MD row (§3.1). The engine looks up
  `(jurisdiction, record_type)` and computes `earliest_contact_date` from
  `anchor_field + days_from`; no branch in code names a state. Add an `Ohio 45-day`
  row **only in a test** (copy the CSV to `tmp_path`, append one row, bump version) and
  prove the new jurisdiction blocks with zero code change — that is the live-defence demo.
- **`config/compliance.toml`** + `src/config.py::compliance()` — `as_of_date =
  "2026-09-01"` (the fixture anchor; the frozen clock), `ruleset_path`, `ruleset_version`
  expectation, `window_arithmetic = "exclusive"` (contactable strictly after
  `anchor + days`; §3.2), `dnc_max_age_days = 31`, `revocation_honour_business_days =
  10`, vault key name, output paths. Every number in the engine comes from here or
  from `rules.yaml`, never from a literal.
- **`src/compliance/engine.py`** — `EligibilityEngine(ruleset_path, ruleset_version,
  blackout_path, as_of)`; `evaluate(record: dict) -> EligibilityDecision`. Starts
  INELIGIBLE, runs **every** gate (never short-circuits), accumulates **all** reason
  codes, orders them by the severity order in `rules.yaml`, resolves status by the
  precedence in §3.3, sets `blocked_until_date = max()` over time gates, attaches one
  `legal_basis` string per reason code (parallel arrays, same order), stamps
  `ruleset_version` and the ruleset file sha, and emits a lineage record. Refuse to
  construct if `rules.yaml`'s `version` ≠ the version passed in (a stale-config guard).
- **`src/compliance/gates/`** (or one module per gate family) — provenance, source,
  time-window, channel (DNC / internal DNC / EBR / RND / line type / calling window /
  consent + revocation), data-quality (envelope, geocode tier, snap distance, timezone
  unresolved). Each gate is a pure function `(record, rules, as_of) -> list[Finding]`
  where a `Finding` is `(reason_code, legal_basis, blocked_until: date|None, detail)`.
- **`src/compliance/window.py`** — the calling window: zone(s) from **coordinates**
  (`src/geo/tz.py`), NPA-NXX zone set as **fallback only**, intersection of the
  per-zone windows when the two disagree, then the stricter of federal and state hours.
  Output the contract's `calling_window_local {earliest, latest, basis}` where `basis`
  names the derivation (`coords:America/Denver ∩ npa:915→{America/Denver}`; state rule
  applied). `grep -rn "phone\[:3\]\|SUBSTR(phone" src/` must be empty — the area code
  is parsed from E.164 with the country code stripped, and only ever as a fallback.
- **`src/compliance/vault.py`** — the token vault. `vault.parties` (fixture PII +
  `party_token` = HMAC-SHA256 over `party_id` with a key from `settings.toml
  [keys].vault_hmac_key`, default a documented dev key so tests run) lives under
  `data/vault/` (gitignored, separate from `data/gold/`). The analytic side carries only
  `party_token`, `phone_token`, `zip5` (the §2725(3) carve-out **is** the schema
  boundary — say so in the module docstring), `jurisdiction`, and derived geo/temporal
  fields; no name, street, phone, or raw coordinates cross. Every read of the vault
  appends to `vault.access_log` (who/what/purpose/when — the §2721(c) 5-year
  redisclosure record). A `ZIP5-only` view is the only thing the join exposes.
- **`src/compliance/lineage.py`** — append-only decision lineage: one record per
  decision with `decision_lineage_id`, `party_token`, `lead_id`, `as_of`,
  `ruleset_version`, `ruleset_sha256`, `blackout_csv_sha256`, `engine_sha`
  (`_compliance_build_sha`), the full ordered findings with params as evaluated, the
  input snapshot **with identifiers tokenised**, `evaluated_at`. `decision_lineage_id`
  is a deterministic hash of (tokenised input, ruleset sha, blackout sha, as_of) so a
  re-run is idempotent and a changed input or rule produces a *new* id — never an
  update. Writer refuses to overwrite an existing id with different content.
- **`src/compliance/consent.py`** — the consent-provenance schema (all §5c fields:
  text presented, disclosure hash, URL, timestamp with tz, IP, UA, signature event,
  sellers named, lead-source chain) as a pydantic model; an append-only `revocations`
  table (`party_token`, `seller`, `scope ∈ {SELLER, ALL}`, `received_at`,
  `honour_by` = received + 10 business days, `channel`); `is_revoked(party_token,
  seller, as_of)` honouring `ALL` scope (revoke-all-ready). Fixture rows carry only
  `consent_on_file` / `consent_revoked` booleans: synthesise the provenance record
  **from the fixture generator's deterministic values** (hash of party_id, a fixed
  disclosure URL/hash, no real IPs — use documentation ranges 192.0.2.0/24) and mark it
  `provenance_kind: SYNTHETIC_FIXTURE`; a record with `consent_on_file=true` but a
  provenance record missing any required field → `CONSENT_UNVERIFIABLE`. Revoked
  trumps on-file → `CONSENT_REVOKED`.
- **`src/compliance/leads.py`** — the fixture join and lead assembly (§2): party
  enrichment (envelope, PIP tract/BG, H3 r8, tz, MD snapping), best-effort crash match,
  engine evaluation, lineage write, output row assembly in the contract's shape.
- **`src/compliance/build.py`** — the CLI: `python -m src.compliance.build
  [--as-of 2026-09-01] [--gold-root …] [--out-root …] [--ruleset …] [--blackout …]
  [--json]`. Writes `data/gold/compliance/{leads,decision_lineage,exclusion_by_code}`
  parquet, `data/vault/*`, `output/sample_leads.csv`, `_compliance_manifest.json`.
  Validates every table against `contracts/compliance.schema.json` **and** every output
  row against `contracts/lead_output.schema.json` (jsonschema, format checks on) before
  the first write.
- **`src/compliance/fl_incompleteness.py`** — the §5a second-order effect: given a feed
  with `jurisdiction`, `report_filing_date` and an `as_of`, label rows inside the
  trailing 60 days `FL_STRUCTURALLY_INCOMPLETE_WINDOW` and refuse/annotate any
  trailing-window aggregate that overlaps it. There is no FL crash feed in scope, so
  this runs against the FL fixture rows and FARS FL (no filing date → labelled
  `NOT_APPLICABLE` with the reason), and the test proves the label fires. It is a
  monitoring design, not a lead gate — say so.
- **`contracts/compliance.schema.json`** (gold dialect: `x-column-order-is-contract`,
  typed properties, enums for status/line type/RND, `unique_keys` on `lead_id` and
  `decision_lineage_id`, a row rule that `eligibility_status = BLOCKED_UNTIL ⇒
  blocked_until_date IS NOT NULL AND > as_of`, `ELIGIBLE ⇒ reason_codes contains an
  affirmative code`, `reason_codes` non-empty). Wire into `src/contracts.py`.
- **`tests/test_compliance.py`** (§6) and a committed golden table
  `tests/fixtures/compliance/fixture_dispositions.csv` (`party_id, status,
  blocked_until_date, reason_codes` — no PII, party_id is a fixture label).
- **`output/sample_leads.csv`** — committed, 40 rows, contract-valid, fixture-derived.
- **The prose**: `COMPLIANCE.md` (the graded artefact — currently a 3-line stub) and
  `ai docs/implementation/phase6-compliance-report.md` in the shape of the Phase 4/5
  reports.

**Out of scope (do not build; leave hooks):** `priority_score` / `score_components`
(Phase 7 — emit `null` and leave a `score.py` hook with the input the engine hands it:
eligible/blocked records only); Dagster assets, backfill CLI, drift detector wiring
(Phase 8 — but make `build.py` importable and side-effect-free at import so an asset
can wrap it); real DNC/RND/carrier lookups (no network; the fixture *is* the lookup
result — model the join key and staleness, not the HTTP call); encryption-at-rest /
key rotation beyond the HMAC key living in gitignored settings (design paragraph in
`COMPLIANCE.md`, not code); California Delete Act / recording-consent (one paragraph
each); any change to `src/geo/`, `src/transform/`, `src/analysis/`, gold column sets,
the fixture, or `lead_output.schema.json`. If a read-only helper is genuinely missing
from `src/geo/tz.py` or `src/transform/common.py`, add it and say so in the report.

---

## 2. The fixture join — what the identity layer is and is not

The fixture has no `report_number`, `crash_sk` or any key into gold. It has
`party_latitude/longitude`, `incident_date`, `report_filing_date`, `state`, `zip5`,
phone, line type, RND, DNC, consent flags and `fixture_note` on two rows. So:

- **The party is the record.** Each fixture row becomes one lead. Enrich the party
  coordinates exactly as a crash is enriched: jurisdiction envelope (`config/geo.toml`
  `[envelope.*]`) → `COORDINATE_OUT_OF_ENVELOPE` (P019, Cumberland, is 100+ km outside
  Montgomery — the quarantine-not-drop decision from Phase 4 carries over), county
  polygon, tract/BG PIP, H3 r8, coordinate-derived `tz_iana` (+ `TIMEZONE_UNRESOLVED`
  when null), and for MD rows the Montgomery road snap with `snap_distance_m` →
  `SNAP_DISTANCE_EXCEEDED` past the Phase 4 rejection threshold (P020, "nearest road is
  ~400 m away" — `GEOCODE_TIER_INSUFFICIENT` is the companion code; decide whether both
  fire and say why). TX/FL rows: no snapping in scope → `snap_status = NOT_IN_SCOPE`,
  `snap_distance_m = null`, and that is *not* a data-quality failure — do not let an
  out-of-scope enrichment turn into a reason code.
- **Best-effort crash match, recorded honestly.** Match each party to a gold crash by
  (`jurisdiction`, `crash_date` within ±1 day of `incident_date`, geodesic distance
  ≤ a config threshold computed in EPSG:26985 for MD / the Phase 4 `[crs.snap]` code
  per state — comment the CRS). Where a match exists, `source_system` /
  `source_record_id` / `severity_ordinal` / `ingested_at` come from the crash; where
  none exists (expected for every TX and FL row, and for most MD rows since the fixture
  is synthetic), `source_system = "OTHER"`, `source_record_id = party_token`,
  `ingested_at` = the fixture file's git commit time or the build time (say which —
  it must be stable across two builds for byte identity). Report the match count per
  jurisdiction; do not fabricate a crash.
- **Identity provenance is an explicit input field**, not an inference. Add
  `identity_provenance ∈ {MOTOR_VEHICLE_RECORD, PUBLIC_CRASH_REPORT, CONSUMER_DIRECT,
  SYNTHETIC_FIXTURE, UNKNOWN}` to the engine's record dict; `rules.yaml` maps each to a
  DPPA/§4-320/§550.065 outcome with a citation. The fixture rows are
  `SYNTHETIC_FIXTURE`, which the ruleset treats as *consumer-direct where a valid,
  unrevoked consent record exists and as no-permissible-use otherwise* — write this
  assumption in `COMPLIANCE.md` and in the report as the thing the memo must repeat:
  **no such identity join exists in production**; the production sources are redacted
  by statute, so on production data every record fails the source gate and the
  engine's honest output for MD/TX/FL cold contact is zero. The engine must produce
  that answer when fed a gold crash without a party: build `evaluate_crash_only()` (or
  feed the same `evaluate` a record with `identity_provenance = PUBLIC_CRASH_REPORT` and
  no contact block) and put the per-jurisdiction counts in the manifest — that table is
  the memo's answer to §7's lawfulness question.
- **PII discipline on the output.** `output/sample_leads.csv` is committed. It carries
  `lead_id` (surrogate), `phone_token`, `zip5`, tract/BG/H3/tz, and the decision. It
  does **not** carry name, street, city, E.164 phone, or party coordinates (the contract
  has no lat/lon field for exactly this reason). Nested contract objects (`geo`,
  `contact`, `consent`, `reason_codes`, `legal_basis`) are JSON-encoded in their CSV
  cells; the validator re-parses each row into the contract's object shape before
  `jsonschema.validate`. State the encoding in `output/README.md`'s existing text
  (append; do not rewrite it).

---

## 3. Method requirements

**3.1 The Maryland row.** Research and complete it. The correct shape is: *no
accident-specific waiting period; the constraint is a channel bar, not a clock.*
Md. Code Gen. Prov. §4-320 bars telephone-solicitation use of MVA personal
information; Md. Rule 19-307.3 bars live person-to-person solicitation by lawyers.
Encode it so the CSV stays a time-window table: `MD,telephone_solicitation,,,"Md. Code
Gen. Prov. 4-320; Md. Rule 19-307.3","No waiting period. Channel bar — enforced by
rules.yaml source_gates/live_solicitation, codes MD_MVA_TELEPHONE_SOLICITATION_BAR /
LIVE_SOLICITATION_PROHIBITED"` (a null `days_from` means "no time gate", and the loader
asserts that a null-window row names the rule that carries it). If you prefer a
`rule_kind` column, add it at the end so the scaffold's columns stay in place, and say
why in the report. The MD disposition for cold contact is the "conclusion the business
will not like": write it as the honest default and the reasoning, both sides, in
`COMPLIANCE.md` (Open Question 6 in the guide: the MoCo feed is a police crash report,
not an MVA record; whether DPPA/§4-320 reach it is a judgment call — make it, cite it,
and state the rejected reading).

**3.2 Window arithmetic.** Exclusive: `earliest_contact_date = anchor + days + 1 day`;
contactable when `as_of >= earliest_contact_date`; `blocked_until_date =
earliest_contact_date` otherwise. FL: both rows apply; the engine emits **both** codes
when both bind and `blocked_until = max` (a candidate who implements only 4-7.18 has
implemented the wrong constraint — the test in §6 checks the 60-day filing gate wins
when the 30-day incident gate has already lapsed). `record_type` per fixture row:
`crash_report` for the FL data gate on every FL row, `written_solicitation` for the bar
rules, `telephone_solicitation` for MD. A row missing its anchor field (null
`report_filing_date`) cannot be cleared → the time gate stays closed with
`blocked_until = null` and an explicit code (`ANCHOR_DATE_MISSING`, add it with the
basis "the statute anchors on a date we do not have").

**3.3 Status precedence and code order** (put both in `rules.yaml`, mirror in
`COMPLIANCE.md`): any non-curable failure (source, provenance, consent-revoked,
DNC-listed, internal DNC, RND reassigned, data-quality) ⇒ `INELIGIBLE` even if a time
window also applies; only curable failures (time windows; DNC scrub stale; RND no-data;
line type unresolved — these are curable by a refresh, not by a date, so they yield
`INELIGIBLE` with the code, and the report explains the difference) ⇒ time windows
alone give `BLOCKED_UNTIL` with the latest date; no failures **and** an affirmative
basis (`ELIGIBLE_CONSENTED` or `ELIGIBLE_EBR`) ⇒ `ELIGIBLE`. Severity order: source
bars > provenance > consent > DNC/RND > time windows > channel/line/window > data
quality > affirmative. `reason_codes` is never empty (the contract says `minItems: 1`;
an ELIGIBLE record's affirmative code is its proof).

**3.4 Channel stack, per fixture field.**
- `on_national_dnc=true` → `DNC_LISTED` (16 C.F.R. 310.4(b)(1)(iii)(B)).
  `dnc_scrub_age_days > 31` → `DNC_SCRUB_STALE` auto-hold **even when not listed**
  (P016, 45 days). Internal DNC: an empty append-only `internal_dnc` table with the
  30-day honour / 5-year retention params in rules; the test adds a token and proves
  `INTERNAL_DNC`. EBR: `ebr` table (`party_token`, `kind ∈ {TRANSACTION, INQUIRY}`,
  `occurred_at`), 18-month / 3-month windows from rules; fixture rows have none, so
  `ELIGIBLE_EBR` never fires on the fixture — the test proves it can.
- RND: `NO` → safe harbour; `YES` → `RND_REASSIGNED` (INELIGIBLE); `NO_DATA` →
  `RND_NO_DATA_NO_SAFE_HARBOR` (not eligible; P013). Three states, three outcomes; a
  null response is a fourth (`LINE_TYPE_UNRESOLVED`-style unresolved, not `NO`).
- Line type: `wireless` → TCPA §227(b)(1)(A)(iii) strict-liability tier (manual dial
  only unless prior express consent for autodial); `landline` → §227(c) DNC regime;
  `voip`, `unknown` → **treated as wireless plus** the consent requirement, i.e. the
  most restrictive row of the routing table, with `LINE_TYPE_UNRESOLVED` on `unknown`
  (P006) and a `LINE_TYPE_VOIP_RESTRICTED` (add it) on `voip` (P005, P029). Also
  `line_type_asof` older than the 31-day cadence → unresolved (fixture has no asof;
  derive it from `dnc_scrub_age_days` and say so, or leave null and let the rule fire
  only when present — choose, document).
- Calling window (§1 `window.py`). The two traps: **P007** (El Paso, 915,
  31.85 / −106.53 → `America/Denver`; a state-default or naive NPA table says Central)
  and **P008** (Pensacola, 850, 30.42 / −87.22 → `America/Chicago`; the state default
  and most of area code 850 is Eastern). Coordinates win as primary; the fallback set
  is intersected; the resulting window is expressed in the **coordinate zone** with the
  intersection narrowing it (Central-vs-Mountain disagreement narrows the window by an
  hour at each end when both are honoured); FL applies 08:00–20:00. `basis` states all
  of it. `OUTSIDE_CALLING_WINDOW` is not a fixture-time code (no dial-time in the
  fixture): emit the window; add a `dial_at` optional input so the test can prove the
  code fires at 21:30 local.
- Consent: §1 `consent.py`. `consent_on_file=false` → `CONSENT_ABSENT` (P002, P017,
  P025); `consent_revoked=true` → `CONSENT_REVOKED` regardless of on-file (P003,
  P018). Affirmative basis for ELIGIBLE on the fixture is `ELIGIBLE_CONSENTED` only.
- Live solicitation: `rules.yaml` carries `actor` (`ATTORNEY_OR_AGENT` default — the
  product is accident leads for a contact centre acting for lawyers; the rules 7.3
  analogues bar live telephone solicitation of a stranger) and `contact_kind`. With a
  valid consumer-direct consent the contact is not an unsolicited live solicitation
  (Rule 7.3(a)(2)-style exceptions for a person who has contacted the lawyer) — encode
  this as the one path that lifts `LIVE_SOLICITATION_PROHIBITED`, cite it, and flag the
  assumption in `COMPLIANCE.md` so the memo can say what changes if the actor is not a
  lawyer.

**3.5 Ruleset versioning for Part 5d.** Show the design, not a paragraph: one-to-one
consent is present in `rules.yaml` as a rule with `effective_from: 2025-01-27,
effective_to: 2025-01-24` — i.e. **never in force**, `status: VACATED`, citation
*Insurance Marketing Coalition v. FCC*, and a `contractual: true` note; written-consent
requirement present with `scope: circuit`, `excluded_circuits: [5th]`, citation
*Bradford*, `effective_to: null`, and a `params.consent_form: [WRITTEN, ORAL]` that the
consent gate reads. The engine evaluates only rules in force at `as_of`; the test in §6
flips `as_of` across an effective date and shows the decision move with **no code
change**. Bumping `version` without changing a rule is refused by the loader
(`ruleset_version` in a decision must mean something).

**3.6 Data protection (§5e), in code where it is cheap.** Vault split + access log +
ZIP5 boundary (§1). Retention TTL per record class as a `retention` block in
`rules.yaml` (`consent: 5y`, `revocation: 5y`, `internal_dnc: 5y`, `lineage: 7y`,
`vault.parties: TTL from last lawful contact`) read by a `retention.py` that *reports*
what would be deleted at `as_of` (it does not delete — say why: deletion that reaches
backups and derived tables is an operational control, Phase 8). Everything else in
§5e (envelope encryption, key rotation, Delete Act registration, recording-consent
states) is one cited paragraph each in `COMPLIANCE.md`.

**3.7 CRS / geo discipline** carries over: the crash-match distance and the party snap
are computed in the Phase 4 projected CRS per state with the comment; storage 4326;
`grep -rn 3857 src/compliance` empty. Timezone from coordinates, never from state or
area code as primary — the `basis` string is the proof.

---

## 4. Outputs

Under `data/gold/compliance/` (gitignored) unless stated:

- **`leads`** — one row per lead, the contract's flat + nested shape (nested as
  STRUCT columns in parquet, JSON in the CSV), plus lineage columns
  (`party_token`, `crash_sk` or null, `match_method`, `identity_provenance`,
  `_compliance_build_sha`, `_geo_build_sha`, `ruleset_sha256`, `blackout_sha256`).
- **`decision_lineage`** — append-only (§1 `lineage.py`). A second build with unchanged
  inputs appends **nothing** (every id already present with identical content).
- **`crash_only_decisions`** — the production-truth table: every gold crash (or a
  stratified sample per jurisdiction if 268k rows is slow — say which) evaluated with
  no identity layer, and **`exclusion_by_code`** — counts by `jurisdiction ×
  eligibility_status × reason_code` for both the fixture leads and the crash-only run.
  This is the memo's exclusion table.
- **`data/vault/parties`, `access_log`, `consent_provenance`, `revocations`,
  `internal_dnc`, `ebr`** — the PII side (`data/` is gitignored; assert in a test that
  no file under `data/gold/` or `output/` contains any fixture `full_name`, street, or
  E.164 string).
- **`output/sample_leads.csv`** — committed; 40 rows; contract-valid; plus
  `output/sample_leads.schema_check.json` (the validator's per-row result, committed —
  cheap evidence).
- **`_compliance_manifest.json`** — `as_of`, ruleset version + sha, blackout sha,
  input hashes (`crash_geo`, `fact_crash`, fixture file), fixture-row counts by status,
  counts by reason code, crash-only counts by jurisdiction × status, the two trap
  rows' derivations, the match table, vault access count, output hashes, warnings.

---

## 5. Determinism, idempotency

- Two consecutive `python -m src.compliance.build` runs on unchanged inputs produce
  byte-identical parquet, a byte-identical `sample_leads.csv`, identical manifest
  values except `built_at`, and zero new lineage rows. `evaluated_at` is therefore
  **`as_of` at 00:00 in UTC**, not wall-clock — say so in the report (the contract
  wants a timestamptz; the frozen clock is the reproducibility decision the fixture
  README demands, and the real wall-clock lives in the manifest's `built_at`).
- `_compliance_build_sha` = hash(inputs, ruleset sha, blackout sha, `[compliance]`
  config); moves exactly when one of those moves. Changing one rule changes the sha,
  every `decision_lineage_id`, and only the decisions that rule touches — test the
  locality claim on the Ohio row (no existing decision changes).
- `write_parquet` gets a total sort order (`lead_id`; `decision_lineage_id`;
  `jurisdiction, eligibility_status, reason_code`).

---

## 6. Tests (`tests/test_compliance.py`) — real behaviour, no mocks, no network

Engine unit tests, each a minimal record dict at `as_of = 2026-09-01`:

- TX, incident 10 days ago, otherwise clean → `BLOCKED_UNTIL` incident+31(+1),
  `TX_SOLICITATION_31D` present.
- FL, filed 40 days ago, incident 45 days ago → blocked by `FL_CRASH_REPORT_60D`, and
  `FL_SOLICITATION_30D` **absent** (already lapsed); `blocked_until` = filing+60(+1).
- FL, filed 5 days ago, incident 6 days ago → **both** codes, `blocked_until = max`.
- FL row with null `report_filing_date` → `ANCHOR_DATE_MISSING`, status INELIGIBLE.
- MD, consent on file, fresh DNC, wireless → exactly the disposition your §3.1 analysis
  concludes, **with MD codes attached where they apply**, never silently eligible.
- No `identity_provenance` / `UNKNOWN` → `INELIGIBLE`, `PROVENANCE_UNKNOWN`.
- `PUBLIC_CRASH_REPORT` provenance with no contact block (a real gold row) →
  `INELIGIBLE` with the source code for its jurisdiction and `CONSENT_ABSENT`.
- `rnd_response=NO_DATA` → `RND_NO_DATA_NO_SAFE_HARBOR`; `YES` → `RND_REASSIGNED`;
  `NO` → neither.
- `dnc_scrub_age_days=45`, not on DNC → `DNC_SCRUB_STALE`; 31 → not; 32 → yes.
- `line_type=voip` and `unknown` → the most-restrictive row: assert the routing
  outcome equals the wireless-plus-consent outcome and is *not* the landline outcome.
- consent on file + revoked → `CONSENT_REVOKED`, no `ELIGIBLE_CONSENTED`; a revocation
  with `scope=ALL` for one seller blocks every seller's campaign for that token.
- Consent provenance missing a required field → `CONSENT_UNVERIFIABLE`.
- Record failing 3 gates → all 3 codes, in the severity order from `rules.yaml`; the
  `legal_basis` array is parallel and the same length.
- Calling window: P007 coordinates → zone `America/Denver`; NPA 915 fallback agrees;
  P008 → `America/Chicago` with NPA 850 spanning two zones → intersection; a synthetic
  disagreement (Eastern coords, Pacific area code) → the intersected window is
  narrower than either; FL → 08:00–20:00; a `dial_at` of 21:30 local →
  `OUTSIDE_CALLING_WINDOW`. Assert `basis` names `coords:` first.
- **Ruleset add-a-row**: copy `blackout_windows.csv` to `tmp_path`, append
  `OH,written_solicitation,45,incident_date,"Ohio R. Prof. Cond. 7.3(b)(3)",`, bump
  `version` to `1.1.0` in a copied `rules.yaml`, evaluate an OH record 10 days old →
  `BLOCKED_UNTIL`, and assert **no file under `src/` changed** (hash `src/compliance/`
  before and after). Also: version bump with no rule change → loader refuses; rule
  change without version bump → loader refuses.
- Effective dating: the written-consent rule evaluated at an `as_of` before and after
  an `effective_from` moves the decision; the vacated one-to-one rule never fires.
- Lineage: two evaluations of the same record → same `decision_lineage_id`, one
  stored row; change one input field → new id, old row untouched; attempting to write
  a different payload under an existing id raises.
- Vault: tokens are stable across runs with the same key and differ with a different
  key; the analytic `leads` table has no column whose values match any fixture
  `full_name`/`street_address`/`phone_e164`; every vault read appends an access-log row.
- FL incompleteness: rows filed within 60 days of `as_of` are labelled; a trailing-30-
  day aggregate over FL refuses/annotates.
- **Fixture golden**: run all 40 rows end-to-end (enrichment on the committed bronze
  extract's gold — Montgomery snapping needs the OSM cache; if unavailable in CI, snap
  status `UNAVAILABLE` is *not* a reason code and the test says so) and compare
  `(party_id → status, blocked_until_date, reason_codes)` to
  `tests/fixtures/compliance/fixture_dispositions.csv`. The two trap rows and the two
  `fixture_note` rows are asserted individually with the codes you expect.
- Contract: every `sample_leads.csv` row validates against `lead_output.schema.json`
  with format checking; a row with `eligibility_status=ELIGIBLE` and no affirmative
  code fails the compliance contract with table and column named.
- Idempotency (§5): two builds into `tmp_path`, identical hashes, zero appended lineage.
- CRS/phone: `grep 3857` empty in `src/compliance`; `grep "phone\[:3\]|SUBSTR(phone"`
  empty across `src/`.

---

## 7. Conventions

- Match Phases 1–5: module docstrings explain **why**; a comment at every CRS choice;
  every legal number (31, 60, 10 business days, 08:00–21:00) is a `rules.yaml` param
  with a citation, never a literal; numbers in prose are measured, dated and
  reproducible by a named command.
- Commit as you go, **one-line conventional commits, no body, no co-author trailer**:
  `feat(compliance): …`, `feat(contracts): …`, `test(compliance): …`,
  `chore(config): …`, `docs(compliance): …`. Commit only files you fill in this phase.
  Never commit `data/`, `config/settings.toml`, `*.parquet` outside `tests/fixtures/`,
  `ai docs/`, `IMPLEMENTATION_GUIDE.md`, `notebooks/`, `src/scoring/`, `orchestration/`.
  `output/sample_leads.csv` and `output/sample_leads.schema_check.json` **are**
  committed (they are the deliverable).
- Logs carry counts and codes, never a name, street, E.164 number or party coordinate
  at any level. The vault module is the only place that touches raw fixture columns.
- When the spec, this brief and the law-as-you-read-it disagree, cite the primary
  source, choose, and put the disagreement in the report and `COMPLIANCE.md`. This
  brief's legal readings are a starting point, not authority — verify each citation
  you rely on against the statute text in Appendix B.

---

## 8. Deliverable: the prose

**`COMPLIANCE.md`** (committed, ~1,500–2,500 words, the ruleset as cited prose — a
mirror of `rules.yaml` + the blackout CSV in sentences, readable by counsel). Structure:
what a decision is (the seven fields, the default, the precedence rule, code severity
order); **Source eligibility** per jurisdiction — DPPA + *Maracich*, TX §550.065(f) and
what redaction means for a contact pipeline (stated plainly), MD §4-320 + Rule 19-307.3
and the judgment call on whether the police feed is a motor-vehicle record, provenance-
unknown; **Blackout windows** (the table, the FL interaction and why the data gate
binds, the exclusive day arithmetic, the Ohio demo); **Channel rules** (calling window
derivation with the intersection rule and the two trap rows as worked examples, DNC as a
31-day SLA, internal DNC, EBR, RND three states, line-type routing table, consent
provenance and revocation with revoke-all); **The unsettled areas** (5d — how the
effective-dated, versioned ruleset absorbs *IMC v. FCC* and *Bradford* without a
rewrite, with the actual YAML rows quoted); **Data protection** (vault boundary at
ZIP5, access log, retention TTLs, the paragraphs on encryption/rotation, Delete Act,
recording consent); **The fixture harness vs production** — the sentence the memo must
repeat, and the crash-only counts per jurisdiction; **Exclusion table by reason code**
(from `exclusion_by_code`, both runs).

**`ai docs/implementation/phase6-compliance-report.md`**: What I built (per module);
Things the spec, the brief or the law said that the data doesn't do (e.g. the fixture
has no `line_type_asof`, no revocation timestamp, no dial time; FL has no crash feed;
what the MD row really is); What I bounded and why (crash-only sample vs full, snapping
availability, no live lookups); Bugs the tests caught; **Verification** (two-build byte
identity, zero appended lineage, the Ohio no-code-change hash proof, PII grep over
`data/gold` + `output`, test counts fixture vs full); **The measurements** (40-row
disposition table by status and by code; the two trap derivations verbatim; crash
match counts; crash-only counts per jurisdiction × status × top codes; vault access
count; every number in `COMPLIANCE.md` with its command); **For DATA_QUALITY.md**
(P019/P020 as the fixture's geo defects; FL structural incompleteness as a monitoring
label); **For DECISIONS.md** (MD row reading; exclusive arithmetic; status precedence;
identity-provenance field; fixture-as-consumer-direct assumption; HMAC vault vs UUID;
deterministic lineage id vs sequence; `evaluated_at` = frozen as_of; NPA fallback source;
voip-as-wireless; live-solicitation actor assumption; each with the rejected
alternative); **For MEMO.md** (the per-jurisdiction lawfulness answer in one sentence
each, the production-join sentence, the exclusion table, the three §5d/§5e items a
business reader needs); **For AI_USE.md** (what this brief and the agent did); Open
items for Phases 7–8 (score input hook; Dagster asset boundaries; the backfill's
byte-identity already proven here; the FL-window monitor as a Dagster sensor).

---

## 9. Definition of done

- `python -m src.compliance.build` runs end-to-end from local gold twice with
  byte-identical outputs and zero appended lineage; the manifest records as_of, ruleset
  version + sha, every input hash, the disposition counts, the trap derivations and
  the crash-only table.
- `EligibilityEngine.evaluate` starts INELIGIBLE, runs every gate, accumulates every
  applicable code in the declared severity order with a parallel `legal_basis`,
  computes `blocked_until = max`, stamps the semver, writes an immutable lineage row,
  and reads the law only from `rules.yaml` + `blackout_windows.csv`.
- The MD row is complete and justified; the Ohio demo passes with `src/` untouched;
  effective dating moves a decision with `as_of`; the loader refuses a version/rule
  mismatch.
- All 40 fixture rows are dispositioned and golden-tested; P007 and P008 resolve to
  `America/Denver` / `America/Chicago` from coordinates with the intersection in
  `basis`; P016 holds on staleness; P013 is not eligible on `NO_DATA`; P003/P018 are
  revoked; P005/P029/P006 route most-restrictively; P019/P020 carry data-quality codes.
- `output/sample_leads.csv` has 40 contract-valid rows, no PII, and is committed with
  its schema-check file; `tests` prove no fixture name/street/phone exists under
  `data/gold/` or `output/`.
- `COMPLIANCE.md` exists as cited prose with the exclusion table and the per-
  jurisdiction answer, including "zero" wherever that is the honest answer.
- `pytest -q` green; the 322 Phase 1–5 tests pass unchanged; no test touches the
  network; `grep -rn 3857 src/compliance` and the `SUBSTR(phone`/`phone[:3]` greps
  are empty.
- `git log` shows small one-line commits; `git status` shows no data, settings,
  `ai docs/`, guide or scoring/orchestration files staged; the report written with
  the measurements above.
