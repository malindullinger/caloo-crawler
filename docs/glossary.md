# Glossary — Caloo Domain Terminology

> **This document is normative.** All code, documentation, and communication should use these terms consistently.

---

## Core Entities

### Happening
A canonical entity representing one potential feed card in the **Happenings** tab.

- Covers both "events" (one-time) and "activities" (recurring)
- One happening = one idea a parent might act on
- Stored in `happening` table (singular)
- May have multiple source records contributing to it

### Course
A canonical entity shown in the **Courses** tab.

- Structurally similar to happenings but behaviorally distinct
- NOT shown in Happenings tab
- May have dated sessions (course_sessions) or be session-less
- Stored in `courses` table

### Occurrence
A specific dated instance when a happening takes place.

- Linked to an offering (happening → offering → occurrence)
- Has concrete `start_at` and `end_at` timestamps
- Stored in `occurrence` table

### Offering
Time-based configuration of a happening.

- Types: `one_off`, `series`, `recurring`
- Contains schedule rules (timezone, date range, recurrence)
- Stored in `offering` table

### Organizer
The entity responsible for running a happening or course.

- May be a business, non-profit, public institution, or individual
- Has `organizer_type` (for_profit, non_profit, public)
- Has `legal_form` (AG, GmbH, Verein, etc.)
- Stored in `organizer` table (singular)

---

## Source Records

### Source Happening
A raw input record before canonicalization.

- One source happening may contribute to one canonical happening
- Multiple source happenings may merge into one canonical happening
- Tracks `source_tier` (A, B, C) and `extraction_method`
- Stored in `source_happenings` table

### Source Course
A raw course input record before canonicalization.

- Same relationship to `courses` as source_happenings to `happening`
- Stored in `source_courses` table

---

## Source Classification

### Tier A — Structured Sources
- Machine-readable datetime (JSON-LD, API, ICS, `<time>`)
- No text heuristics required
- Preferred and scalable

### Tier B — Explicit Text Exceptions
- No structured datetime available
- Text parsing allowed **only with explicit approval**
- Logic must be source-specific and quarantined
- See `docs/tier-b-sources.md` for approved sources

### Tier C — Not Viable
- Vague or inconsistent datetime information
- Either date-only ingestion or skipped entirely

---

## Time Concepts

### date_precision
Field indicating whether time information is known.

| Value | Meaning |
|-------|---------|
| `datetime` | Full date and time known |
| `date` | Only date known, time unknown |

**Contract:** When `date_precision = 'date'`, time fields MUST be NULL. Never use `00:00` as placeholder.

### Extraction Method
How datetime was obtained from source.

| Value | Meaning |
|-------|---------|
| `jsonld` | Extracted from JSON-LD `startDate`/`endDate` |
| `time_element` | Extracted from HTML `<time datetime>` |
| `text_heuristic` | Parsed from human-readable text (Tier B only) |

---

## Provenance

### Happening Sources
Links canonical happenings to their contributing source records.

- Tracks which sources contributed to a happening
- Identifies primary source (highest trust)
- Stored in `happening_sources` table

### Field Provenance
Tracks which source provided each field value.

- Enables audit trail
- Supports conflict resolution
- Stored in `happening_field_provenance` table

---

## Status Values

### visibility_status
Whether a happening/course appears in the feed.

| Value | Meaning |
|-------|---------|
| `draft` | Not visible in feed |
| `published` | Visible in feed (if eligible) |
| `archived` | Historically preserved, not visible |

### occurrence/session status
State of a specific instance.

| Value | Meaning |
|-------|---------|
| `scheduled` | Planned to happen |
| `cancelled` | Will not happen |
| `completed` | Already happened |
