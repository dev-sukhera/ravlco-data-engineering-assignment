# Synthetic party fixture

`synthetic_parties.csv` — 40 fabricated identity records. **Joining against this
file is mandatory.** Your `output/sample_leads.csv` must be populated from it.

## Why this exists

None of the three assignment sources contain callable personal information.
Montgomery County exposes an opaque `person_id` GUID with no name or address.
The public Texas layer is the redacted CR-3 — Tex. Transp. Code §550.065(f)
strips name, driver's licence number, date of birth other than year, address
other than ZIP, and telephone number. Maryland's person table is titled
"(Anonymized)". FARS carries no names.

That is not an oversight in the data. It is the statutory design.

So this fixture supplies the identity layer the real sources deliberately do
not, purely so you can exercise tokenisation, line-type routing, calling-window
derivation, DNC and RND handling, and consent provenance against something
concrete.

**Your memo must still state that no such join is available in production, and
what that means for the product.** The fixture is a test harness, not a
demonstration that the join exists.

## Rules

- Do **not** substitute real personal information for any of this. Not scraped,
  not appended, not skip-traced, not purchased. A submission containing real
  third-party PII is an automatic fail — see Part 00 of the assignment.
- Do not "improve" the fixture by adding records. Grading diffs against it.
- Treat these values as though they were real: tokenise them, do not print them
  to logs, and do not commit intermediate files containing them.

## What is in it

Every value is fabricated. Phone line numbers use the NANP fictional range
(555-0100 – 555-0199), which is permanently unassignable. Names and street
addresses do not correspond to real people or deliverable addresses.

Real **area codes** and real **coordinates** are used, because the calling-window
logic has to be exercised against genuine geography. Two records in particular
exist to catch a specific mistake; you will find them if your timezone
derivation is correct and miss them if it is not.

The fixture contains records that should clear every gate, records that should
be blocked on timing, and records that should be excluded outright. Roughly half
are unremarkable filler so the output has volume. Work out which is which — that
is the exercise. `fixture_note` is populated on exactly two rows, and only for
defects you could not otherwise infer from the row itself.

`incident_date` and `report_filing_date` are anchored to **2026-09-01** so the
fixture is reproducible. If you are working later than that, either freeze
"today" to that date in config or state your handling in `DECISIONS.md` — do not
silently let the windows drift.

## Provenance

`generate_fixture.py` produced this file and is committed so you can see there
is nothing hidden in it. You do not need to run it.
