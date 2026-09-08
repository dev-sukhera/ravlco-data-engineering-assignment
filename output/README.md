`sample_leads.csv` goes here. Maximum 100 rows.

**No real personal information.** Synthetic or fully redacted only.

If your pipeline's honest output for a jurisdiction is an empty file, submit the
empty file with its header row and explain it in MEMO.md. That is a valid result.

---

## `sample_leads.csv` — what is in it and how it is encoded

40 rows, one per row of `fixtures/synthetic_parties.csv`, in the column order of
`contracts/lead_output.schema.json`. All three statuses are present, not just
`ELIGIBLE`: the contract requires a decision on every record and the memo wants the
exclusion table, so a mixed file is the more informative one. Regenerate with
`python -m src.compliance.build`.

**No personal information.** The file carries `lead_id` (a surrogate), `phone_token`,
`zip5`, census tract / block group / H3 / IANA zone, and the decision. It carries no
name, street, city, E.164 number or coordinate — the contract has no latitude or
longitude field for exactly that reason, and the boundary is drawn in
`src/compliance/vault.py` around the 18 U.S.C. §2725(3) ZIP5 carve-out. A test greps
every byte of this directory and of `data/gold/` for every fixture name, street and
phone number.

**Nested objects are JSON-encoded in their cells.** `geo`, `contact`, `consent`,
`reason_codes`, `legal_basis` and `score_components` are objects and arrays in the contract, so each is
written as a JSON document inside one CSV field (with `"` doubled, per RFC 4180 —
any spreadsheet or `csv` reader handles it). Key order inside each object follows the
contract rather than being sorted, so a cell is readable. `src/compliance/build.py`
re-parses every cell back into the contract's object shape before
`jsonschema.validate`, so what is validated is exactly what is written.

**`sample_leads.schema_check.json`** is that validator's per-row result, committed
beside the CSV as evidence: 40 rows, 40 valid, 0 invalid, checked against the
unmodified contract with `jsonschema` **format checking on** (six fields declare
`format: date` or `date-time`, and jsonschema ignores both by default).
