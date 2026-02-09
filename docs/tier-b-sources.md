# Tier B Sources (Explicit Text-Based Exceptions)

> **This document is normative.** Claude must treat its rules as hard constraints, not suggestions.

This document tracks sources that cannot provide structured time data and have been **explicitly approved** for text-based parsing.

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
