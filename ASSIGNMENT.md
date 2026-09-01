# Take-Home: Crash-to-Contact

### A compliance-gated geospatial lead pipeline

**Role:** Senior Data Engineer
**Format:** Take-home build + 60-minute live defense of your own code
**Suggested effort:** This is deliberately unbounded. Most strong submissions land between 10 and 16 hours. We would rather see a narrower scope built properly than the whole thing built badly. Tell us what you cut and why — that is a graded answer, not an apology.
**Submission:** A private Git repository with full commit history, plus the written memo in Part 7.

---

## 0. Read this part first

We operate an outbound contact center. The business wants a daily feed of motor-vehicle-crash-derived leads that agents can act on.

Your job is to build the pipeline that produces that feed — **and to determine what may lawfully be in it.**

That second clause is not a footnote. It is 25% of your score, and it is the only section with a hard cap: a submission that emits contactable records without a defensible eligibility decision attached to each one cannot pass this exercise, no matter how good the engineering is. We have shipped enough of these to know that the pipeline is the easy part.

Some of the sources below will lead you to conclusions the business will not like. Write them down anyway. **"Here is why this jurisdiction produces zero deliverable leads, with citations" is a passing answer. Quietly producing them is not.**

### Ground rules

- Use any language, stack, and tooling you like. Justify the choice.
- AI assistance is allowed and expected. Keep an `AI_USE.md` noting where you leaned on it. The live defense will make this obvious anyway, so the honest note costs you nothing and the omission costs you a lot.
- Commit as you work. A repo with one commit reads as a dump; we grade the process too.
- Everything below is public data reachable without a paid subscription. Where a source requires a free key or a registration, that is noted. Where a source is **deliberately closed**, that is the point of the exercise — see Part 5.
- Do not scrape anything behind a login, a paywall, a CAPTCHA, or a Terms of Service prohibition. If a path is closed, document that it is closed. Finding the wall and stopping at it is the correct behavior and we score it as such.

### What we are actually measuring

| We are testing | Not |
|---|---|
| Whether your pipeline is correct when the data is wrong | Whether you can call an API |
| Whether you understand what a coordinate *means* | Whether you can import GeoPandas |
| Whether you can defend a design under questioning | Whether your README is pretty |
| Whether you know what you are not allowed to do | Whether you can move rows quickly |

---

## 1. Ingestion — three sources, three access shapes

Ingest **all three**. They are chosen because each fails differently.

### 1a. Montgomery County, MD — Socrata / SODA API

Your primary high-resolution source. Open, no key, current to within about a week, roughly 350k rows across three tables at three different grains.

| Table | Dataset ID | Rows | Coverage |
|---|---|---|---|
| Crash Reporting – Incidents Data | `bhju-22kf` | ~124,770 | 2015-01-01 → present |
| Crash Reporting – Drivers Data | `mmzv-x632` | ~219,644 | 2015-01-01 → present |
| Crash Reporting – Non-Motorists Data | `n7fk-dce5` | ~7,498 | 2015-01-01 → present |

- Endpoint: `https://data.montgomerycountymd.gov/resource/{id}.json`
- Full SoQL is supported: `$select`, `$where`, `$group`, `$order`, `$limit`, `$offset`
- Bulk CSV: `https://data.montgomerycountymd.gov/api/views/{id}/rows.csv?accessType=DOWNLOAD`
- No key required. A free app token lifts anonymous throttling. Handle throttling regardless.

**Requirements**

- Incremental ingestion with a durable watermark. Re-running must not duplicate, and must pick up records that appeared for dates you have already loaded.
- Paginate correctly. `$offset` without a deterministic `$order` will silently skip and repeat rows under concurrent writes — we will check for this specifically.
- Preserve the raw payload byte-for-byte in your bronze layer alongside the parsed form.

### 1b. Texas — TxDOT CRIS via the public ArcGIS FeatureServer

Your volume and pagination source: roughly **3.09 million rows, ~190 columns**, no registration.

```
https://services.arcgis.com/KTcxiTD9dsQw4r7Z/arcgis/rest/services/
  TXDOT_Statewide_Bicyclist_Involved_Crashes/FeatureServer/0
```

Note the name. The layer is titled and described as bicyclist-involved crashes, but `where=1=1` returns 3,088,450 records against 14,535 with `bicyclist_involved_fl=1`. Its own service description string is `txdot_ph_2.automated_sw.cris_crash`. **It is publishing the full statewide CRIS crash table.**

Two things we want from this:

1. Ingest it correctly. `maxRecordCount` is **2,000**, so you are looking at ~1,545 paged requests. Naive `resultOffset` paging against a moving table is wrong; unindexed `WHERE` clauses time out server-side. Solve both.
2. **Tell us in your memo whether you should be using it, and why you concluded that.** It is genuinely public and unauthenticated. It is also plainly mislabeled. There is a defensible answer in either direction. We want to see you notice the question exists.

Reference for what TxDOT intends to release: the CRIS Automated Interface guide, `https://www.txdot.gov/content/dam/docs/division/trf/crash-records/cris-guide.pdf` (V29.0, June 2025), and Tex. Transp. Code §550.065.

### 1c. NHTSA FARS — annual bulk files

Your full-refresh-with-restatement source.

```
https://static.nhtsa.gov/nhtsa/downloads/FARS/{YEAR}/National/FARS{YEAR}NationalCSV.zip
```

Years 1975–2024. Browse at `https://www.nhtsa.gov/file-downloads?p=nhtsa/downloads/FARS/`.

The 2023 file carries a last-modified of **2026-04-01**. Prior-year files are silently revised in place. Design for that.

There is also a REST API at `https://crashviewer.nhtsa.dot.gov/CrashAPI` (no key, 5,000-record cap, 2010+). Use it or don't; the bulk files are more reliable.

### 1d. Optional — and a trap

**NHTSA CRSS** (`https://static.nhtsa.gov/nhtsa/downloads/CRSS/{YEAR}/CRSS{YEAR}CSV.zip`) is publicly downloadable and looks like more of the same. It is not. It is a probability sample of ~50–60k police-reported crashes carrying `WEIGHT` and PSU/stratum design variables, and it **cannot produce state-level estimates at all.**

If you use it, use it correctly. If you `UNION ALL` it with FARS or Montgomery County, you have produced a number that is wrong by orders of magnitude and we will find it.

### Known defects — report these back to us

Each source below contains real, verifiable defects. Your submission must include a `DATA_QUALITY.md` reporting what you found, how you detected it, and what your pipeline does about it. We know the answers. Some of these are the entire point:

- **Coordinates that pass a null check and are still wrong.** `bhju-22kf` has zero null and zero zero-valued lat/long. It also has records outside Montgomery County's envelope, some of them well over a hundred miles away. Only a geometric sanity check catches these — and "drop them" is not automatically the right answer. Tell us what you did and why.
- **Two generations of code dictionary concatenated in one column.** `driver_substance_abuse` in `mmzv-x632` mixes an old uppercase single-value scheme (`NONE DETECTED`, `ALCOHOL PRESENT`) with a newer comma-joined pair (`Not Suspect of Alcohol Use, Not Suspect of Drug Use`). There are at least three distinct spellings of null across the two schemes, and the embedded comma breaks naive splitting.
- **The dictionary cutover overlaps.** The old scheme and the new scheme coexist for several days around the new year of 2024. A hardcoded cutover date is wrong. Find the window; handle it.
- **The tables disagree about the crash universe.** Incidents and Drivers do not contain the same set of `report_number` values. An inner join silently drops crashes. Quantify the disagreement with an anti-join and say what you did about it.
- **Grain fan-out.** Drivers is one row per driver but carries denormalized crash-level attributes (weather, light, lat/long). Aggregating crash-level fields off Drivers overcounts multi-vehicle crashes by roughly 1.8×. Establish and enforce the real key.
- **Two competing coordinate pairs in TxDOT.** `rpt_latitude`/`rpt_longitude` (officer-reported) and `latitude`/`longitude` (CRIS-derived) are both present, and the officer-reported pair is frequently null where the derived pair is populated. There is also a `located_fl` flag. Pick a precedence rule and defend it.
- **Amended reports.** TxDOT exposes `amend_supp_fl`. Late amendment is a first-class concept in this schema, which means re-pulls change history. Your pipeline must be idempotent under restatement — this is a slowly-changing-dimension problem, not an append problem.
- **String dates and integer code dictionaries.** TxDOT dates are `esriFieldTypeString`; roughly 60 columns are meaningless `*_id` integers without the CRIS lookups.
- **Sentinel values, not nulls.** FARS encodes unknown coordinates as `77.7777` / `88.8888` / `99.9999`, and uses 7/8/9-fill in coded fields (`AGE` 998/999, `HOUR` 99). Loaded naively, you will place crashes in the Arctic Ocean.

---

## 2. Modeling and entity resolution

Produce a dimensional model. We are not prescriptive about the shape, but we expect you to be explicit about grain and to defend it.

**Required:**

- A crash-level fact at exactly one row per crash, with a documented natural key and a stable surrogate key.
- Party-level facts (driver, non-motorist) at their own grain, correctly related.
- Conformed dimensions for date, time, geography, road class, weather condition, severity.
- **Cross-source entity resolution.** The same crash can appear in more than one source. FARS is a fatality census; Montgomery County is all-severity; TxDOT is Texas-wide. Where universes overlap, resolve or explicitly scope. If you decide the universes are disjoint enough not to need resolution, prove it rather than asserting it.
- **Severity harmonization.** The three sources use different injury scales. Build a documented crosswalk to a single ordinal. Note where the mapping is lossy — it is.
- **Late-arriving and restated records.** Both TxDOT amendments and FARS annual reissues change history. Show us your strategy (SCD2, snapshot partitions, event-sourced — your call) and show us a test that proves a re-run is idempotent.

---

## 3. Geospatial

This section carries the most weight of any technical section, and it is where mid-level and senior submissions separate most cleanly.

### 3a. Coordinate reference systems — non-negotiable

Store canonical geometry in **EPSG:4326**. Reproject before any distance, length, area, or buffer operation.

- Tri-state analysis: **EPSG:5070** (NAD83 / CONUS Albers, equal-area) so that per-km² rates across three states are honest.
- State-local work: **EPSG:26985** (Maryland, single zone), **EPSG:32139** / 32137 / 32138 / 32140 / 32141 (Texas zones), **EPSG:26958** / 26959 / 26960 (Florida East / West / North). Statewide single-CRS alternatives: **EPSG:3083** (Texas Albers), **EPSG:3086** (Florida GDL Albers).
- **EPSG:3857 is for rendering tiles and nothing else.** If a distance, buffer, or area calculation in your submission is performed in Web Mercator, that section scores zero. Scale error is `1/cos(lat)`: a 500 m buffer drawn in 3857 near Baltimore is about 387 m on the ground.

State your chosen CRS per operation, in code, with a comment saying why.

### 3b. Required enrichments

**Census geography.** Point-in-polygon join each crash to its census tract and block group.

- TIGER/Line 2025: `https://www2.census.gov/geo/tiger/TIGER2025/BG/tl_2025_{FIPS}_bg.zip` and `.../TRACT/tl_2025_{FIPS}_tract.zip`. FIPS: MD=24, TX=48, FL=12.
- Roads: `.../ROADS/tl_2025_{state}{county}_roads.zip`, `.../PRISECROADS/tl_2025_{FIPS}_prisecroads.zip`

**ACS socioeconomic context.** Block-group level, ACS 5-year.

- `https://api.census.gov/data/2023/acs/acs5?get=...&for=block%20group:*&in=state:24%20county:003%20tract:*&key=YOUR_KEY`
- **An API key is now required** for data endpoints — free at `https://api.census.gov/data/key_signup.html`. Note that the Census's own guidance page still advertises 500 unkeyed queries per day; that page is stale. Trust the live service over the docs, and say so if you hit it.
- Useful variables: `B01003_001E` population, `B19013_001E` median household income, `B25044` vehicles available by tenure, `B08301` means of transportation to work, `B08303` travel time to work.
- **Read Part 4 before you decide how to use these.**

**Road network snapping.** Attach each crash to its nearest road segment and inherit `highway` class, `maxspeed`, `lanes`, `name`, `ref`.

- Bulk extracts (do **not** use Overpass for statewide pulls — it will blow the fair-use quota): `https://download.geofabrik.de/north-america/us/{maryland|texas|florida}-latest.osm.pbf`
- Overpass (`https://overpass-api.de/api/interpreter`) is fine for small ad-hoc POI queries only. Its documented etiquette limit for a regular application is on the order of 100 queries/day.
- **Record the snap distance as a first-class quality attribute** and set a rejection threshold. A crash snapped 400 m to a road is not enrichment, it is fiction. Expect `maxspeed` and `lanes` to be sparsely tagged outside major corridors; report the null rate rather than silently imputing.

**Linear referencing.** Express each snapped crash as an offset along its segment (`ST_LineLocatePoint` / `shapely` `line.project`). This is what makes corridor-level analysis possible rather than point-level.

**H3 aggregation.** Index at resolution 8 (~0.74 km², ~530 m edge) with resolution 9 (~0.10 km², ~200 m edge) as the finest defensible grain. Use `cell_to_parent` for multi-resolution rollups and `grid_disk` for neighborhood smoothing. Note: the `h3` Python library is at 4.x and v3's `geo_to_h3`-style API no longer exists — code written against v3 will not run.

**Weather.** Join crash timestamp and location to conditions.

- `https://archive-api.open-meteo.com/v1/archive?latitude=&longitude=&start_date=&end_date=&hourly=...` — free, no key for non-commercial use, 1940–present, gap-free. Limits are 600/min, 10,000/day.
- Be aware of what you are joining to: Open-Meteo is **ERA5 reanalysis on a ~9–25 km grid** — spatially smooth and never missing. Station observation (NOAA GHCNh) is accurate at a point and frequently absent. A good answer uses reanalysis as the backbone and says why. A very good answer notes that NOAA's ISD was superseded and relocated to S3 in mid-2026, so any tutorial pointing at `ncei.noaa.gov/data/global-hourly/` is stale.

**Timezone — and read the next sentence twice.** Derive an IANA timezone for every crash **from its coordinates**, not from its area code, not from its state.

- `timezonefinder` (8.x) offline, or a spatial join against Timezone Boundary Builder 2026b.
- Texas spans Central **and** Mountain (El Paso and Hudspeth counties). Florida spans Eastern **and** Central (the western Panhandle). Maryland is entirely Eastern.
- Store timestamps as UTC `TIMESTAMPTZ`; derive local wall-clock at query time. Note that several of these feeds publish naive local time already, so you must *localize*, not *convert*. Handle the spring-forward gap and the fall-back ambiguity explicitly.

This is not a geography exercise. Part 5 explains why it is a compliance control.

### 3c. Required analysis

Pick **at least three**, implement them properly, and interpret the output in prose. An unlabeled heatmap is not an analysis.

| Technique | What we are looking for |
|---|---|
| **Getis-Ord Gi\*** (`esda.G_Local`) | Statistically significant hot/cold clusters with an FDR correction. Contrast against raw counts and show that population normalization changes the answer. |
| **Moran's I / LISA** (`esda.Moran`, `Moran_Local`) | Whether crash rates cluster spatially at all, before you assert that they do. |
| **ST-DBSCAN** | Clusters tight in space *and* time — a recurring Friday-night corridor, not a year-long smear. |
| **KDE** | Continuous intensity surface. Bandwidth selection is the whole exercise; justify it. |
| **Isochrone / drive-time** | Crash-to-trauma-center access. Free Valhalla endpoint: `https://valhalla1.openstreetmap.de/isochrone` (no key, fair use, send an `X-Client-Id`). Note that network distance exceeds Euclidean by roughly 1.2–1.4× in US road grids. |
| **Spatial cross-validation** | If you build any model: random splits leak across spatially autocorrelated neighbors and inflate your score. Block by county or H3 cell. |

---

## 4. Lead scoring

Produce a ranked, explainable priority score per eligible record.

**Required:**

- Every score decomposes into named contributions. No opaque blob.
- Every feature carries provenance back to its source field.
- A documented, reproducible backtest of whatever you claim the score does.

**Constraint, and it is graded:** ACS variables are available at block-group level, and several of them are close proxies for protected characteristics. Using block-group median income, or anything correlated with it, as a *prioritization* feature for who gets contacted is a redlining-adjacent design, and in several of the jurisdictions in scope it creates real exposure.

We are not telling you the answer. We are telling you the question is live, and we expect your memo to address it. A submission that quietly ranks leads by neighborhood income has told us a great deal.

---

## 5. The compliance gate

**This is the section that decides the outcome.**

Build a `contact_eligibility` decision layer. Every record leaving your pipeline carries:

```
eligibility_status   : ELIGIBLE | INELIGIBLE | BLOCKED_UNTIL
blocked_until_date   : date or null
reason_codes         : array, machine-readable, ordered by severity
legal_basis          : citation string per reason code
decision_lineage_id  : FK to an immutable audit record
evaluated_at         : timestamptz
ruleset_version      : semver
```

The default disposition is **INELIGIBLE**. Eligibility must be affirmatively proven, per record, with a citation. A record whose provenance you cannot state is a record that cannot be contacted.

### 5a. Jurisdictional blackout windows

Implement as a data-driven `(jurisdiction, record_type) → earliest_contact_date` table, not as hardcoded branches. At minimum:

| Jurisdiction | Rule | Window |
|---|---|---|
| **Florida** | Fla. Stat. §316.066(2) — crash reports revealing personal information are exempt from public disclosure | **filing date + 60 days** |
| **Florida** | R. Reg. Fla. Bar 4-7.18(b)(1)(A) — targeted written communication re: an accident | **incident + 30 days** |
| **Texas** | Tex. Penal Code §38.12(d)(2)(C) — written solicitation re: an accident | **incident + 31 days** |
| **Maryland** | No accident-specific waiting period. Md. Rule 19-307.3 governs the contact method instead — see 5c. | — |
| *(edge case)* | 49 U.S.C. §1136(g)(2) — aviation accidents | incident + 45 days |

Note the Florida interaction: the 60-day data gate is **longer** than the 30-day solicitation gate, so the data gate binds. A candidate who implements only the bar rule has implemented the wrong constraint.

Note also the second-order effect: because of §316.066(2), **the most recent 60 days of any Florida public feed is structurally incomplete.** If you compute a trailing-30-day Florida trend, you will read a statute as an outage. Detect this and label it.

### 5b. Source-eligibility gates

**DPPA — 18 U.S.C. §§2721–2725.** Personal information from state motor vehicle records may be disclosed only for 14 enumerated permissible uses. Solicitation appears only at **§2721(b)(12)**, and only where *the State* has obtained affirmative express consent. No state runs such a program at scale.

The obvious workaround is foreclosed by name: **Maracich v. Spears, 570 U.S. 48 (2013)** holds that an attorney's solicitation of clients is *not* covered by the §2721(b)(4) litigation exception. Penalties under §2724 are actual damages with a **$2,500 statutory floor per record**, plus punitive damages and fees.

Note the one genuine carve-out: §2725(3) excludes the **5-digit ZIP code** from the definition of "personal information." That is your legitimate path to geographic aggregation. Build the boundary into your schema, not into a policy document.

**Texas §550.065.** The only bulk-accessible product is the **redacted** CR-3 required by §550.065(c-1). Section 550.065(f) strips name, driver's license number, date of birth other than year, address other than ZIP, telephone number, plate number, and insurer details.

Work out what that means for a contact pipeline and state it plainly in your memo.

**Maryland Gen. Prov. §4-320** is Maryland's DPPA analogue. It requires written consent for marketing lists and **expressly bars use of the personal information for telephone solicitation.**

### 5c. Contact-channel rules

**Calling window — derived from geography, not from the phone number.** 16 C.F.R. §310.4(c) and 47 C.F.R. §64.1200(c)(1) restrict outbound calls to **8:00 a.m.–9:00 p.m. local time at the called party's location.** Several states are stricter: Florida §501.616(6) is **8 a.m.–8 p.m.**, as are Oklahoma and Washington.

Because of number portability and VoIP, NPA-NXX is not reliable evidence of physical location. Derive the window from the lead's address where known, fall back to NPA-NXX, and **take the intersection when they disagree.** This is why Part 3b required a coordinate-derived timezone.

A pipeline that computes a calling window from `SUBSTR(phone,1,3)` fails this section outright.

**DNC — a freshness SLA, not a one-time load.** The safe harbor at 16 C.F.R. §310.4(b)(3)(iv) requires a registry version obtained **no more than 31 days** before the call. Model it as a staleness constraint on a join key, with monitoring and an automatic hold when the scrub ages out. Registry access is at telemarketing.donotcall.gov; the first five area codes are free.

Also implement: an internal company-specific DNC list per 47 C.F.R. §64.1200(d) (honored within 30 days, retained 5 years), and the **established business relationship** exemption windows (18 months from a transaction, 3 months from an inquiry).

**Reassigned Numbers Database.** The §64.1200(m) safe harbor attaches only to a **"No"** response. A **"No Data"** response is *not* a green light — this is the single most common engineering misreading of the RND. Model the three states distinctly.

**Line type.** Resolve every number to `wireless | landline | voip | unknown` against current carrier data, not against the NPA-NXX block's original assignment. The distinction is legally load-bearing: §227(b)(1)(A)(iii) attaches strict liability to autodialed and prerecorded calls to wireless numbers, while landlines fall under the §227(c) DNC regime which requires more than one call in twelve months to be actionable. **Route `voip` and `unknown` to the most restrictive treatment, never the most permissive.** Line type, carrier, and disconnect status all mutate — pair the refresh with the 31-day DNC cadence.

**Consent provenance.** For any consented record, persist: the exact text presented, a hash or snapshot of the disclosure, the URL, timestamp with timezone, IP, user agent, the signature or checkbox event, every seller named, and the full lead-source chain. The burden of proving consent is on the caller; unverifiable consent is functionally no consent. Retain at least through the 4-year TCPA limitations period — 5 years aligns with the TSR and DPPA record rules.

**Revocation.** Append-only, immutable, honored within **10 business days** per 47 C.F.R. §64.1200(a)(10). Design the schema so a single revocation can cascade across all campaigns for a seller: the FCC's "revoke-all" scope provision has been waived twice and its current compliance date is **January 31, 2027** (DA 26-12). Building for it now is cheap; retrofitting it is not.

### 5d. Two areas where the law is genuinely unsettled

We include these because a senior engineer should be able to build against a moving target and say so.

1. **One-to-one consent is dead.** The FCC's 2023 order was vacated in *Insurance Marketing Coalition v. FCC*, 127 F.4th 303 (11th Cir. Jan. 24, 2025) and formally repealed by final rule in September 2025. Do **not** build it as binding federal law. Do note that many lead buyers still require it contractually.
2. **Written consent is now circuit-dependent.** *Bradford v. Sovereign Pest Control* (5th Cir., Feb. 25, 2026) held that §227(b) requires only prior express consent, which may be oral, and declined to apply 47 C.F.R. §64.1200(a)(2)'s written requirement. The rule was not vacated and binds everywhere outside the Fifth Circuit. Practitioner guidance is uniformly to keep obtaining written consent.

Tell us how you would design a ruleset that survives a change like this without a rewrite. This is a versioning and configuration question as much as a legal one.

### 5e. Data protection

- Tokenize direct identifiers into a segregated vault; the analytic warehouse joins on surrogates. Recall the §2725(3) ZIP5 carve-out when you draw the boundary.
- Encryption at rest and in transit, envelope encryption with documented key rotation.
- Per-query access attribution on any table containing direct identifiers. DPPA §2721(c) independently requires **5-year redisclosure records** naming each recipient and permitted purpose.
- Retention TTLs by record class, with deletion that actually propagates to backups and derived tables. This is where most implementations quietly fail.
- **California Delete Act.** An accident-lead vendor meets the statutory definition of a data broker almost exactly: a business that knowingly collects and sells personal information of consumers with whom it has no direct relationship. Registration is due annually by January 31; DROP deletion-request processing has been mandatory since **August 1, 2026**, at least once every 45 days, with **$200/day** penalties per unprocessed request.
- Note the recording-consent states if calls are recorded (CA, FL, IL, MD, MA, MT, NV, NH, PA, WA and others).

---

## 6. Operability

- **Orchestration.** Dagster, Airflow, or Prefect. Show the dependency graph, retry semantics, and how a failed partition is recovered without a full rebuild.
- **Idempotent backfill.** A documented command that rebuilds any date range and produces byte-identical output. Prove it.
- **Schema-drift detection.** When the `driver_substance_abuse` dictionary changed, the correct behavior was an alert, not a silent pass. Build the detector that would have caught it, and show it firing against the historical data.
- **Data contracts** at every layer boundary, with enforced types, nullability, ranges, and referential expectations.
- **Tests.** dbt tests, Great Expectations, Soda, or hand-rolled — we do not care which. We care that the four Montgomery County defects listed in Part 1 each have a test that fails on the raw data and passes after your transform.
- **Storage.** GeoParquet **1.1.0** (the current stable spec — target it, and use the `bbox` covering column for row-group pruning; 2.0 is still a release candidate and GeoParquet remains an incubating OGC standard). Sensible partitioning. Justify your file sizes.
- **Right-sizing, and a trap.** At three states and ~10⁵–10⁷ rows this is single-node territory. DuckDB with the `spatial` extension plus GeoPandas is the correct answer, or PostGIS if you need a concurrent serving layer. Reaching for Spark or Sedona here is over-engineering, and we will read it as such — **unless you name it as the escape hatch for national multi-year scale and explain why you did not use it.** That answer scores well.
- **Cost.** Estimate the monthly cost of running this daily at 10× the current volume.

---

## 7. The memo

A `MEMO.md` of no more than 2,000 words, written for a business audience rather than an engineering one. This is 10% of the score and it is where we learn the most about you.

Address:

1. **What did you build, and what did you deliberately not build?**
2. **What is the volume of contactable leads your pipeline produces per day, by jurisdiction — and what is the volume of records you excluded, by reason code?** We want the exclusion table. It is more informative than the inclusion table.
3. **The direct question: for each jurisdiction in scope, can the records your pipeline produces lawfully be delivered to a live outbound call center? Show your reasoning.**

   Consider carefully. Note that ABA Model Rule 7.3(b) and its state analogues — Md. Rule 19-307.3, Fla. Bar 4-7.18(a), Tex. Disciplinary R. 7.03 — bar solicitation by **live person-to-person contact**, which the ABA comment defines to include live telephone. Note that Tex. Penal Code §38.12(a)(2) criminalizes in-person or telephone solicitation for economic benefit as barratry, a **third-degree felony**, and that it reaches the caller, not only the firm. Note that Florida §316.066(3)(d) makes knowing misuse of confidentially-obtained crash data a **third-degree felony**.

   If your honest answer for one or more jurisdictions is *"zero — there is no compliant path for this product as described,"* **write that.** It is a correct answer and we will score it as one. What we will not score well is a memo that produces a lead count while stepping around the question.

4. **What product shapes remain viable given your findings?** Aggregate analytics? Consented inbound? Non-solicitation uses under an enumerated DPPA exception? Something else? One paragraph each.
5. **What would you build next, given a quarter?**
6. **Where is your pipeline most likely to be wrong, and how would you find out?**

---

## 8. Deliverables

```
repo/
├── README.md            # setup, run, and a 5-minute quickstart that actually works
├── MEMO.md              # Part 7
├── DATA_QUALITY.md      # findings from Part 1, with detection method per defect
├── DECISIONS.md         # timestamped design decisions and what you rejected
├── AI_USE.md            # where you used AI assistance
├── COMPLIANCE.md        # the ruleset, with citations, as prose
├── src/
├── tests/
├── contracts/           # data contracts / schemas
├── orchestration/
└── output/
    └── sample_leads.csv # ≤100 rows, synthetic or fully redacted PII
```

**`output/sample_leads.csv` must not contain real personal information.** If your pipeline's honest output for a jurisdiction is an empty file, submit the empty file with its header and explain it in the memo.

---

## 9. The live defense

60 minutes, on your own code. Bring your repo. We will:

- Ask you to walk a single record from source through to eligibility decision.
- Change a requirement and ask what breaks. (Example: *"Ohio just enacted a 45-day accident solicitation window, effective in three weeks."*)
- Ask you to defend one thing you did not do.
- Ask you where you are wrong.

There is no trick. We are checking that the person in the room is the person who wrote the code, and that they can think under mild pressure.

---

## Appendix A — Sources at a glance

| Source | Endpoint | Key | Notes |
|---|---|---|---|
| MoCo Crashes | `data.montgomerycountymd.gov/resource/{bhju-22kf,mmzv-x632,n7fk-dce5}.json` | none | SoQL, ~daily |
| TxDOT CRIS | `services.arcgis.com/KTcxiTD9dsQw4r7Z/.../FeatureServer/0` | none | 3.09M rows, cap 2000/page |
| NHTSA FARS | `static.nhtsa.gov/nhtsa/downloads/FARS/{yr}/National/FARS{yr}NationalCSV.zip` | none | annual, restated in place |
| NHTSA CRSS | `static.nhtsa.gov/nhtsa/downloads/CRSS/{yr}/CRSS{yr}CSV.zip` | none | **survey sample — weights required** |
| TIGER/Line 2025 | `www2.census.gov/geo/tiger/TIGER2025/{BG,TRACT,ROADS}/` | none | vintage 2025 |
| Census Geocoder | `geocoding.geo.census.gov/geocoder/geographies/{onelineaddress,addressbatch}` | none | 10k/batch, no header row |
| ACS 5-year | `api.census.gov/data/2023/acs/acs5` | **required** | free signup |
| OSM extracts | `download.geofabrik.de/north-america/us/{state}-latest.osm.pbf` | none | MD 203MB, TX 683MB, FL 625MB |
| Overpass | `overpass-api.de/api/interpreter` | none | small queries only |
| Open-Meteo | `archive-api.open-meteo.com/v1/archive` | none | ERA5 reanalysis, 1940– |
| NOAA GHCNh | `ncei.noaa.gov/oa/global-historical-climatology-network/index.html#hourly/` | none | ISD's successor |
| Valhalla | `valhalla1.openstreetmap.de/isochrone` | none | fair use |
| Timezones | `github.com/evansiroky/timezone-boundary-builder` (2026b) | none | or `timezonefinder` 8.x |
| HRSA facilities | `gisportal.hrsa.gov/server/rest/services/HealthCareFacilities/CMSApprovedFacilities_FS/MapServer` | none | HIFLD Open was retired Aug 2025 |

## Appendix B — Legal citations referenced

18 U.S.C. §§2721–2725 (DPPA) · *Maracich v. Spears*, 570 U.S. 48 (2013) · Fla. Stat. §316.066(2)–(3) · Fla. Stat. §501.059 (FTSA) · Fla. Stat. §501.616(6)–(7) · R. Reg. Fla. Bar 4-7.18 · Tex. Transp. Code §550.065 · Tex. Penal Code §38.12 · Tex. Disciplinary R. Prof. Conduct 7.03 · Md. Code Gen. Prov. §4-320 · Md. Rule 19-307.3 · ABA Model Rule 7.3 · 47 U.S.C. §227 (TCPA) · 47 C.F.R. §64.1200 · 16 C.F.R. Part 310 (TSR) · *Facebook v. Duguid*, 592 U.S. 395 (2021) · *Insurance Marketing Coalition v. FCC*, 127 F.4th 303 (11th Cir. 2025) · *Bradford v. Sovereign Pest Control* (5th Cir. 2026) · FCC DA 26-12 · Cal. Civ. Code §1798.99.80 et seq. (Delete Act) · 49 U.S.C. §1136(g)(2)

*Citations are provided so candidates can research primary sources. They are not legal advice, and this document is not a substitute for counsel.*
