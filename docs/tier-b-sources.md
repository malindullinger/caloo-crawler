# Source Tier Classification

> **This document is normative.** Claude must treat its rules as hard constraints, not suggestions.

This document classifies every event source by **extraction reliability** and
tracks sources approved for text-based parsing.

**Tier = extraction reliability, not organizer quality.** A municipal portal
run by a trusted Gemeinde can still be Tier B if it only provides unstructured
text dates. A commercial platform with JSON-LD is Tier A regardless of content
quality.

**Terminology note**: Human-facing documentation uses "text-based parsing". System field uses `extraction_method = "text_heuristic"`.

---

## Source Classification System (MANDATORY)

Every source MUST be explicitly classified into exactly one of the following buckets **before implementation**:

### Tier A — Structured Sources
- Machine-readable datetime via JSON-LD, API, ICS, or equivalent
- No text heuristics required
- Fully scalable and preferred
- **Default tier for new sources**

### Tier B — Explicit Text-Based Exceptions
- No structured datetime available
- Explicit, consistent human-readable datetime text
- Text parsing allowed **ONLY with explicit approval**
- Logic must be source-specific and quarantined
- **Requires documented decision with constraints**

### Tier C — Not Viable / Date-Only
- Vague or inconsistent datetime information
- Requires inference or guessing
- Either date-only ingestion or skipped entirely
- **Never attempt to "make it work"**

> **Tier A is defined by source quality, not parser cleverness.**
> Claude must never downgrade data integrity to "make a source work".

### DB field: `source_happenings.source_tier`

| Tier | DB value | `source_priority` in `happening_sources` |
|------|----------|------------------------------------------|
| A    | `'A'`    | 300 (highest)                            |
| B    | `'B'`    | 200                                      |
| C    | `'C'`    | 100 (lowest)                             |

When `best_source` CTE selects image/URL for the feed, higher priority wins.
Migration 022 corrects historical tier mismatches in the DB.

---

## Tier A Sources

### 1. Eventbrite Zurich

**Source ID**: `eventbrite_zurich`

**Listing URL**: `https://www.eventbrite.com/d/switzerland--zurich/events/`

**Classification**: Tier A — Structured (JSON-LD primary)

**Decision Date**: 2026-02-12

---

#### Structured Data Assessment

| Feature | Status |
|---------|--------|
| JSON-LD `@type: Event` | Yes (primary extraction) |
| Schema.org Event subtypes | Yes (`SocialEvent`, `EducationEvent`, etc. — any type ending with `Event`) |
| HTML `<time datetime>` | Available (used as text-heuristic fallback) |
| API endpoint | Not used (JSON-LD sufficient) |

**Conclusion**: Fully structured source. JSON-LD provides `startDate`/`endDate` in ISO 8601.

---

#### Extraction strategy

| Step | Method | Fallback |
|------|--------|----------|
| Listing page | Non-JS HTML scrape | JS render (Playwright) if < 3 URLs extracted |
| Detail page | Non-JS HTML + `extract_jsonld_event()` | JS render if JSON-LD not found in SSR |
| Datetime | JSON-LD `startDate`/`endDate` | Text heuristic from `.date-info` selectors |

---

#### Constraints

1. **Domain allowlist** — Only fetch detail pages from:
   `.com`, `.ch`, `.de`, `.co.uk`, `.at`, `.ca`
   (rejects regional domains like `.com.mx`, `.es` that never contain Zurich events)

2. **Geographic preflight guard** — SSR JSON-LD is checked for
   `addressCountry` and `addressLocality`. Events proven to be
   outside Zurich/Switzerland are skipped before JS fallback.
   If unsure, the event is kept (errs on side of inclusion).

3. **Online events skipped** — Events with `VirtualLocation` /
   `OnlineEventAttendanceMode` or "Online" in title are excluded.

4. **Image URL canonicalization** — Handles Eventbrite's Next.js
   `_next/image` proxy pattern by extracting underlying CDN URL.

---

#### Implementation

| File | Purpose |
|------|---------|
| `src/sources/adapters/eventbrite.py` | Full adapter with JSON-LD extraction, domain allowlist, preflight guard |
| `src/jobs/bridge_eventbrite_to_canonical.py` | Bridge from legacy `events` table to canonical schema |

---

## Tier B Sources

### 1. Maennedorf Portal (Municipal Exception)

**Source ID**: `maennedorf_portal`

**URL**: https://www.maennedorf.ch/anlaesseaktuelles

**Classification**: Tier B — Municipal Exception (text-only datetime source)

**Decision Date**: 2026-02-09

**Approval**: Explicitly approved for text-based parsing

---

#### Structured Data Assessment

| Feature | Status |
|---------|--------|
| JSON-LD `@type: Event` | ❌ Not found |
| HTML `<time datetime>` | ❌ Not found |
| Schema.org microdata | ❌ Not found |
| RSS/ICS feeds | ❌ Not found |
| API endpoint | ❌ Not found |

**Conclusion**: No structured datetime available from this source.

---

#### Approved Text Patterns (EXHAUSTIVE LIST)

**Allowed patterns:**
| Pattern | Example | Result |
|---------|---------|--------|
| `D. Mon. YYYY, HH.MM Uhr - HH.MM Uhr` | `"27. Feb. 2026, 19.30 Uhr - 21.00 Uhr"` | start=19:30, end=21:00, precision=datetime |
| `D. Mon. YYYY, HH.MM Uhr` | `"27. Feb. 2026, 19.30 Uhr"` | start=19:30, end=NULL, precision=datetime |
| `D. Mon. YYYY` | `"21. März 2026"` | date only, precision=date, times=NULL |

**NOT allowed (must reject, preserve unknown-time semantics):**
| Pattern Type | Example | Why Rejected |
|--------------|---------|--------------|
| Relative dates | `"heute"`, `"morgen"` | Requires inference |
| Vague times | `"abends"`, `"nachmittags"` | Not explicit |
| Open-ended ranges | `"ab 19.00 Uhr"` | No end time inferable |
| Multi-day spans without times | `"22. - 24. März"` | Ambiguous time handling |

**Do NOT expand this list** without explicit approval. Creative regex is prohibited.

---

#### Constraints (STRICTLY ENFORCED)

1. **Text parsing allowed ONLY for this source**
   - Do NOT generalize this logic to shared utilities
   - Do NOT reuse this approach for other sources without explicit approval

2. **Parsing limited to explicit, fully specified patterns**
   - Must match exact pattern with day, month, year, and optionally time
   - No partial matches, no inference

3. **No inference, no defaults, no guessing**
   - If pattern doesn't match exactly → unknown time
   - If time component missing → `date_precision='date'`
   - If ambiguous → preserve unknown-time semantics

4. **End time requires explicit source text**
   - End time may ONLY be set if explicitly present in the source text
   - Never infer duration or default end times
   - `"19.30 Uhr"` alone → `end_at = NULL` (not "assume 2 hours")

5. **Failure handling**
   - If parsing fails or is ambiguous:
     - `date_precision = 'date'`
     - `start_time_local = NULL`
     - `end_time_local = NULL`
   - Never invent times, never use midnight placeholders

6. **Extraction method must be explicit**
   - `extraction_method: "text_heuristic"` in extra dict
   - Bridge notes include `tier_b:text_heuristic` marker

---

#### Implementation

| File | Purpose |
|------|---------|
| `src/sources/adapters/maennedorf_portal.py` | Source-specific adapter with quarantined text parsing |
| `src/jobs/bridge_maennedorf_to_canonical.py` | Bridge to canonical schema |

**Quarantine Location**: Lines 169-196 in adapter (text heuristic fallback)

### 2. Elternverein Uetikon (FairGate SPA Exception)

**Source ID**: `elternverein_uetikon`

**URL**: https://elternverein-uetikon.ch/veranstaltungen

**Classification**: Tier B — FairGate SPA Exception (text-only datetime, JS required)

**Decision Date**: 2026-02-16

**Approval**: Explicitly approved for text-based parsing

---

#### Structured Data Assessment

| Feature | Status |
|---------|--------|
| JSON-LD `@type: Event` | Not found |
| HTML `<time datetime>` | Not found |
| Schema.org microdata | Not found |
| RSS/ICS feeds | Not found |
| API endpoint | Not found (FairGate CMS, no public API) |

**Conclusion**: No structured datetime available. FairGate renders events
client-side as `div.columnBox` elements with uppercase German date text.

---

#### Approved Text Patterns (EXHAUSTIVE LIST)

**Allowed patterns:**
| Pattern | Example | Result |
|---------|---------|--------|
| `D. MONTH YYYY` | `"11. JANUAR 2026"` | date only, precision=date, times=NULL |
| `D.-D. MONTH YYYY` (compact range) | `"9.-14. NOVEMBER 2026"` | expanded to start+end date, precision=date |
| `D. MONTH - D. MONTH YYYY` | `"4. MAI - 28. SEPTEMBER 2026"` | start+end date, precision=date |
| `D. MONTH YYYY - D. MONTH YYYY` | `"19. OKT. 2026 - 19. APRIL 2027"` | start+end date, precision=date |

**Month forms**: Full German (`JANUAR`, `FEBRUAR`, `MÄRZ`, ...) and abbreviated (`JAN.`, `FEB.`, `MÄR.`, ...). Both uppercase from source; normalized to title-case.

**NOT allowed:**
| Pattern Type | Example | Why Rejected |
|--------------|---------|--------------|
| Times | Source does not provide times | No times exist to parse |
| Relative dates | `"heute"`, `"nächste Woche"` | Requires inference |
| Vague periods | `"Herbst 2026"` | Not explicit |

**Do NOT expand this list** without explicit approval.

---

#### Constraints (STRICTLY ENFORCED)

1. **Text parsing quarantined to this adapter**
   - All regex patterns in `src/sources/adapters/elternverein_uetikon.py`
   - Do NOT reuse for other sources

2. **Date-only extraction (no times)**
   - This source never provides event times
   - All events are `date_precision='date'`, times=NULL
   - Never infer or default times

3. **JS rendering required**
   - FairGate is a SPA; non-JS fetch returns empty content
   - Adapter uses `http_get(url, render_js=True)`

4. **Location hardcoded**
   - `location_raw = "Uetikon am See"` (all events are local)

5. **No description extraction**
   - Source does not provide event descriptions in parseable form
   - `description_raw = None`

6. **Extraction method must be explicit**
   - `extraction_method: "text_heuristic"` in extra dict

---

#### Implementation

| File | Purpose |
|------|---------|
| `src/sources/adapters/elternverein_uetikon.py` | Source-specific adapter with quarantined text parsing |

**No bridge job** — flows through pipeline → merge_loop directly.

---

## Adding New Tier B Sources

**STOP.** Before adding a source as Tier B:

1. **Complete Source Reconnaissance** — Document absence of all structured data options

2. **Request Explicit Approval** — Tier B requires human decision, not automatic classification

3. **Document the Decision** — Include:
   - Decision date
   - Rationale for exception
   - Exact text pattern approved
   - Constraints specific to source

4. **Implement with Quarantine**:
   - All parsing logic in source-specific adapter
   - No shared utilities
   - Clear `extraction_method` tracking

5. **Update This Document** — Add entry following the template above

---

## Classification Checklist (For New Sources)

Before implementing any source, answer these questions:

| Question | Answer Required |
|----------|-----------------|
| Does source provide JSON-LD Event? | Yes → Tier A |
| Does source provide `<time datetime>`? | Yes → Tier A |
| Does source provide API/ICS? | Yes → Tier A |
| Is datetime text explicit and consistent? | Yes → Request Tier B approval |
| Is datetime vague or requires guessing? | Yes → Tier C (date-only or skip) |

If all structured options are "No" and text is not explicit → **Do not proceed. Source is Tier C.**
