# Phase 9 — Documents: implementation brief

You are implementing Phase 9 of the Crash-to-Contact take-home in this repo. Phases 0–8
are complete and committed on `main` (Phase 8 = commits `0a9e458` … `8f6a367`:
`orchestration/`, `OPERABILITY.md`, `contracts/bronze.schema.json` enforcement,
`output/drift_firing.log`). Your job is ASSIGNMENT.md §7 and §8 **in full**: the six
required documents, written so a grader who opens only the repo — and a 60-minute live
defense (§9) run on it — finds every claim backed by a file, a manifest number, or a
command they can run.

**This phase writes no new pipeline code.** Everything the documents describe already
exists and is proven. The graded gap is that four of the six deliverable documents are
still the scaffold stubs (`MEMO.md`, `DATA_QUALITY.md`, `DECISIONS.md`, `AI_USE.md` — each
is 12 words), `README.md` has a quickstart nobody has run from a clean checkout, and the
evidence for all of them is scattered across eight per-phase build reports and five
manifests. Phase 9 consolidates, verifies, and commits. If you find yourself editing a
file under `src/`, stop and ask whether the document should describe the code as it is
instead.

## Read first, in this order

1. `ASSIGNMENT.md` §7 (the six memo questions — each is a rubric line, including the
   ≤2,000-word limit and "business audience"), §8 (the deliverable tree), §9 (the four
   things the live defense will do), §0 (the hard cap and the "zero is a passing answer"
   paragraph), and §1 "Known defects" (the nine bullets `DATA_QUALITY.md` must answer,
   each with *what, how detected, what the pipeline does*).
2. Every file in `ai docs/implementation/`. Each phase report has sections headed
   **"For DATA_QUALITY.md"**, **"For DECISIONS.md"**, **"For MEMO.md"** and (Phase 6)
   **"For AI_USE.md"**. Those sections were written *for you*; they are the raw material.
   Phase 8's "Open items for Phase 9" names five timestamped operational decisions and the
   cost/right-sizing conclusion to lift. Phase 2 §"The drift detector, firing on historical
   data" and §"Bonus: three defects the assignment does not list" belong in
   `DATA_QUALITY.md`. Phase 3 §"The entity-resolution census" and Phase 6 §"The crash-only
   run — the memo's answer" belong in the memo.
3. The documents that already exist and are final or near-final, so you do not
   duplicate them and do not contradict them: `COMPLIANCE.md` (the ruleset as cited
   prose, with the per-jurisdiction answer and the exclusion table — the memo *summarises*
   this, it does not restate it), `ANALYSIS.md`, `SCORING.md`, `OPERABILITY.md`,
   `output/README.md`, `fixtures/README.md`.
4. The manifests, which are the only legitimate source of any number you write:
   `data/gold/_build_manifest.json`, `data/gold/_geo_manifest.json`,
   `data/gold/analysis/_analysis_manifest.json`, the compliance and scoring manifests under
   `data/gold/compliance/` and `data/gold/scoring/`, `data/reference/_reference_manifest.json`,
   `data/bronze/_watermarks.duckdb`, `output/sample_leads.schema_check.json`,
   `output/drift_firing.log`. If a number is not in one of these, or reproducible by a
   command you ran and wrote down, it does not go in a document.
5. `src/compliance/rules.yaml`, `config/blackout_windows.csv`, `src/compliance/reason_codes.py`
   — every citation the memo makes must already appear in one of these or in
   `COMPLIANCE.md`. The memo introduces no new law.
6. `git log --format='%h %ad %s' --date=iso` — `DECISIONS.md` timestamps are taken from
   the commit that landed each decision, not invented.

## Deliverables

### `MEMO.md` — ≤2,000 words, business audience, six numbered sections

Budget the most care here; it is 10% of the score and "where we learn the most about you."

- **Audience discipline.** A contact-center VP reads this. No module names, no column
  names, no CRS codes in the body; those go in a one-line "where to look" pointer per
  section (`COMPLIANCE.md §…`, `ANALYSIS.md §…`) so the engineer reader can follow.
  Statute citations *do* belong in the body — the business reader needs to see that the
  "zero" answer is law, not caution.
- **Q1 — built / deliberately not built.** Two short lists. The "not built" list is graded
  as *answers*: CRSS (with the one-paragraph reason it is a sample and cannot union),
  isochrones, statewide TX snapping, full-history weather, GHCNh, FARS pre-2019, ML
  scoring — plus whatever the phase reports' "What I bounded" sections add. Each cut gets
  one sentence of *why*, not an apology.
- **Q2 — daily contactable volume by jurisdiction, and the exclusion table by reason
  code.** The honest production number is what Phase 6's crash-only run says it is (every
  record `INELIGIBLE`; state the count per jurisdiction and the top codes). Then the
  fixture table: 40 rows → status counts → code counts, pulled from
  `data/gold/compliance/exclusion_by_code` and reconciled against `COMPLIANCE.md`'s table
  (they must agree to the row, or you fix the doc that is wrong and say which). State
  plainly that the fixture join does not exist in production (fixtures/README.md makes
  this mandatory). Express "per day" honestly: the pipeline is daily; the contactable
  count per day is zero; say what the fixture demonstrates instead.
- **Q3 — the direct lawfulness question, per jurisdiction: MD, TX, FL** (FL has no crash
  source in scope but has rules in scope — answer it anyway, since the memo asks "each
  jurisdiction in scope"). Cite DPPA §2721/§2725, Md. Gen. Prov. §4-320 and Rule
  19-307.3, Tex. Penal Code §38.12 and Transp. Code §550.065, Fla. Stat. §316.066 and Bar
  Rule 4-7.18, ABA Model Rule 7.3(b). Where the honest answer is "zero — no compliant path
  for a live outbound call center", write exactly that sentence and then the reasoning.
  Include the TxDOT "should you be using it at all" answer here or in Q6 (ASSIGNMENT §1b
  requires it in the memo) — the repo's position is *use it, with the mislabeling
  documented as a provenance risk*; check Phase 1/2 reports for the wording and the
  CRIS-guide citation.
- **Q4 — viable product shapes.** One paragraph each: aggregate analytics (the Phase 5
  analyses are the live example — name one interpreted finding), consented inbound
  (the consent-provenance schema and revocation cascade already exist), enumerated DPPA
  exceptions (§2721(b) — say which ones plausibly apply and which do not), and at least
  one more (insurer/fleet B2B, safety-program partnership, or research under §2721(b)(5)).
- **Q5 — next quarter.** Draw from every report's "Open items": ruleset bodies persisted
  by version, Florida feed + the structural-incompleteness monitor as a sensor, statewide
  TX snapping, hosted orchestration, a real identity source *if a lawful one exists*, and
  the ACS-proxy audit. Ordered, with the dependency on the legal answer stated first.
- **Q6 — where the pipeline is most likely wrong, and how you would find out.** Be
  specific and unflattering: the P020 snap mismatch, the nine mis-decoded TX counties the
  polygon test caught (Phase 4), the ERA5 "never null" property, the FDR sensitivity to
  permutation count (Phase 5), the fold=0 DST choice, the Maryland judgment call
  (IMPLEMENTATION_GUIDE §10 Q6), the 2026-09-01 frozen clock. For each: the detector that
  would catch it (a test, a manifest field, a monitor).
- **Word count.** Enforce with `wc -w MEMO.md` and state the count at the bottom of the
  file in a single line. Tables and headings count; if you need to argue they don't, you
  are over budget. Target 1,800 so a grader's counter cannot disagree.

### `DATA_QUALITY.md` — every defect: what, detection method, disposition, count

- One section per ASSIGNMENT §1 defect, in the assignment's order (nine), then the bonus
  defects the reports found (Phase 2 §"Bonus", Phase 4 §2 "nine Texas counties", §3
  "five crashes not in the United States", Phase 5 and 6 "Things the data doesn't do").
- Each section has the same four sub-headings: **What** / **How detected** (the actual
  query, test name, or manifest field — `tests/test_known_defects.py::…` where one exists)
  / **What the pipeline does** (disposition, with the design reason: D7 "keep the row,
  quarantine the geometry" etc.) / **Count** (from a manifest, with the manifest path).
- The cutover-overlap section must give the measured window (dates) and the count of
  rows in it. The `driver_substance_abuse` section must point at `output/drift_firing.log`
  and quote the firing lines.
- The Incidents/Drivers anti-join section reports *both directions* with counts.
- Close with a CRSS paragraph (why it is not ingested; §1d) and the FL structural
  incompleteness note (no FL feed, but the monitor exists — say where).

### `DECISIONS.md` — timestamped, with the rejected options

- One entry per decision, format: `### YYYY-MM-DD — <decision title>` then **Decision /
  Rejected / Why / Evidence** (evidence = commit hash, test name, or manifest field).
- Timestamp = the date of the commit that landed it (`git log`). Do not backdate to
  when the guide was written; the guide is planning, the commit is the decision.
- Seed list: IMPLEMENTATION_GUIDE.md D1–D15, then every "For DECISIONS.md" section in the
  eight phase reports, then Phase 8's five operational decisions. Include the deliberate
  scope cuts as decisions (each names the rejected "build it" option and its cost).
  Include the Spark/Sedona sentence verbatim as a rejected option under the stack decision.
- Include the decisions that changed mid-build (e.g. the RND `YES` YAML fix, the
  consent exception on the catch-all bar, the party snap threshold split, corpus vs
  sample evaluation) — a decision log that only shows first choices reads as fiction.

### `AI_USE.md` — honest and specific

- The assignment says the omission costs a lot and disclosure is free; the guide's §1
  note says this file must disclose `IMPLEMENTATION_GUIDE.md` and `ai docs/` or those
  must be gitignored. **Decision point for the owner** (see below): the working tree's
  `.gitignore` currently *un-ignores* both. Write `AI_USE.md` so that it is truthful under
  whichever choice is made, and name the choice.
- Structure: what was AI-assisted per phase (planning briefs, implementation, tests,
  docs), what was human-decided (the legal judgment calls, the scope cuts, the Maryland
  disposition, the TxDOT use-it-at-all answer, the coordinate precedence), and what was
  verified by hand against primary sources (COMPLIANCE.md says citations were checked on
  2026-09-08 — say how). Name the tooling (Claude Code) and the pattern (one brief and one
  report per phase). Phase 6's "For AI_USE.md" section has specifics; use them.
- Include the mistakes AI made that tests caught (each report's "Bugs the tests caught")
  — that is the most credible line in the file.

### `README.md` — a 5-minute quickstart that actually works

- Keep the existing layout, asset list, and 10× cost section. Replace the quickstart
  with two paths and label them: **(a) no network, no data, under five minutes** — venv,
  `pip install -r requirements.txt`, `pytest` (state the expected count and the xfail
  behaviour of the four bronze tests), `python -m src.compliance.build` against the
  fixture, the Ohio demo, the single-record walkthrough (a command that prints one
  party's decision, reason codes, citations and lineage id — if no such command exists,
  the closest existing command plus the manifest path; do not write a new one unless it
  is under twenty lines and you note it in your report); **(b) full pipeline** — the
  ingest → transform → geo → analysis → compliance → scoring → Dagster sequence with
  rough wall-clock per step from the reports and the Census-key prerequisite.
- **Then run path (a) from a clean venv in a fresh clone** (`git clone . /tmp/…` or the
  scratchpad) and paste the trimmed output into your report. If anything fails, fix the
  README, not the code, unless the failure is a genuine packaging bug — then fix it in
  its own commit and say so.
- Add a "Documents" section listing all six deliverables plus `ANALYSIS.md`, `SCORING.md`,
  `OPERABILITY.md`, one line each, so the grader's first click lands.

### `COMPLIANCE.md` — finalise, do not rewrite

- Verify the `rules.yaml` sha256 in its header still matches (`sha256sum`), the
  exclusion table matches `data/gold/compliance/exclusion_by_code`, and every reason code
  in `reason_codes.py` is mentioned. Fix drift; leave prose alone.

## Rules

- **No new PII, anywhere.** Fixture names, streets and phones must not appear in any
  document; the memo's exclusion table uses party ids and counts only. The existing PII
  grep test must stay green.
- **No new law.** Every citation in the memo already exists in `rules.yaml`,
  `blackout_windows.csv` or `COMPLIANCE.md`. If you believe one is missing, write it down
  in your report as an open item; do not add it.
- **No number without a source.** Manifest path or reproducing command, in the doc or in
  your report.
- **Do not modify** `fixtures/`, `contracts/lead_output.schema.json`,
  `output/sample_leads.csv`.
- **Commits:** one commit per document, in this order — `DATA_QUALITY.md`,
  `DECISIONS.md`, `COMPLIANCE.md` (if changed), `README.md`, `AI_USE.md`, `MEMO.md` last —
  plus one for the `.gitignore` decision if it is made. One-line conventional-commit
  messages in the repo's existing style (`docs(memo): …`, `docs(quality): …`). **Never
  mention an AI tool in a commit message and never add a co-author trailer.** Commit dates
  are the real dates; do not rewrite history.
- Write in the repo's existing voice (see `COMPLIANCE.md`, `ANALYSIS.md`): plain,
  declarative, no hedging filler, numbers in tables.

## Decision point to surface, not decide

The working tree has an uncommitted `.gitignore` change that removes the ignore rules for
`IMPLEMENTATION_GUIDE.md` and `ai docs/`. Phase 5 and 6 reports both flagged this. Two
consistent options: **(1)** restore the ignore lines and describe both artifacts in
`AI_USE.md` as local working notes not in the repo; **(2)** commit them and describe them
in `AI_USE.md` as the disclosed planning and build record. Option (2) is more transparent
and the assignment rewards disclosure; option (1) keeps the repo tree at the §8 shape.
State which one you assumed, make `AI_USE.md` correct for it, and put it first in your
report so the owner can flip it in one commit. `notebooks/eda_bronze_silver.ipynb` is
also untracked — same question, same treatment.

## Verification you must run and paste

```
wc -w MEMO.md                                   # ≤ 2000
pytest -q                                       # green; bronze xfails as designed
grep -rn "3857" src/ | grep -v "rendering\|never\|not "     # empty or justified comments only
python -m src.compliance.build && git status --short output/ # sample unchanged
sha256sum src/compliance/rules.yaml             # matches COMPLIANCE.md header
```
plus the clean-venv quickstart transcript, and a table mapping each memo number and each
`DATA_QUALITY.md` count to its manifest field.

## Report

Write `ai docs/implementation/phase9-docs-report.md` in the same shape as the earlier
reports: what was written, where the reports and the manifests disagreed and which won,
what you bounded (anything in the memo you cut to fit 2,000 words, and where it lives
instead), the quickstart transcript, the verification table, the `.gitignore` decision
you assumed, and open items for the live defense (a one-page "walk one record" script
and the Ohio demo command belong here, not in the memo).
