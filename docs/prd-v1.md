# PRD — Caloo Happenings & Courses Ingestion System (v1)

## Purpose
Build a robust ingestion → canonicalization → eligibility pipeline that:
- Aggregates happenings (events + activities) and courses from multiple sources
- Preserves strict data contracts (esp. time handling)
- Supports manual enrichment and deduplication
- Produces reliable feed cards for Lovable
- Scales to additional sources without hacks

This PRD defines WHAT to build and HOW to validate it.

---

## Product Definitions (LOCKED)

### Domain Objects
- **Happening**: Canonical entity representing one feed card in the *Happenings* tab
  (covers events + activities)
- **Course**: Canonical entity shown in the *Courses* tab (NOT a happening)
- **Course Session / Occurrence**: Optional dated instances belonging to a course

### Source Records
All inputs are treated as raw source records:
- `source_happening`
- `source_course`

Inputs may come from:
1. External web crawler
2. Internal manual admin input
3. Partner feeds (future)
4. User-generated (future)

---

## Tier System (Source-only)
- **Tier A**: Structured (JSON-LD, API, ICS)
- **Tier B**: Explicit text exception (approved + quarantined)
- **Tier C**: Not viable / date-only or skipped

Tier applies ONLY to sources, never to canonical records.

---

## Non-Negotiable Contracts
- Never invent times
- Never default to `00:00`
- `date_precision='date'` ⇒ no start/end time fields
- End time only allowed if explicitly present
- Text parsing only for approved Tier B sources
- All decisions must be testable

---

## Core System Responsibilities

### 1. Canonicalization
- Merge multiple source records into one canonical happening/course
- Track provenance per field
- Preserve auditability

### 2. Eligibility
- Determine whether a canonical happening can appear in the feed
- Centralized, deterministic logic

### 3. Enrichment
- Optional metadata improves cards but never blocks eligibility

### 4. Admin Control
- Visibility into coverage & gaps
- Manual enrichment and manual creation
- Duplicate resolution

---

## Mandatory Feed Eligibility (v1)

A happening is eligible iff:
- title present
- start_date present (or start_at if datetime)
- location present OR online=true
- status != cancelled
- unknown-time contract respected
- audience constraints respected (newborn-only excluded in v1)

Eligibility must return reasons when false.

---

## Data Model (v1)

### Happenings
- `source_happenings`
- `happenings`
- `happening_sources`
- `happening_field_provenance` (recommended)

### Courses
- `source_courses`
- `courses`
- `course_sources`
- `course_sessions` (optional)

### Organizers
- legal_form
- organizer_type (for-profit / non-profit / public)
- locality
- priority_score

---

## Merge Strategy (v1)

Matching:
- Prefer stable external IDs
- Fallback fingerprint: normalized title + date (+ venue)

Field precedence:
partner_feed > internal_manual > Tier A > Tier B

Never infer missing data.

---

## Testing Strategy (MANDATORY)

Three layers:
1. Adapter tests (per source)
2. Canonicalization & contract tests (global)
3. Feed eligibility + view model tests

---

## Milestones & Tasks

### Milestone 1 — Foundation & Naming (NO CRAWLING)
**Goal:** Lock domain language + schema.

Tasks:
- Create glossary doc (happening vs course)
- Create DB tables (happenings, courses, source_* tables)
- Add enums: `happening_type`, `date_precision`
- Add organizer priority fields
- Migration tests

Deliverable:
- Schema ready
- No ingestion yet

---

### Milestone 2 — Canonicalization & Provenance
**Goal:** Merge safely and audibly.

Tasks:
- Implement canonical merge function
- Implement field-level precedence rules
- Implement provenance tracking
- Unit tests for dedupe + merge

Deliverable:
- Two sources → one canonical record
- Provenance visible

---

### Milestone 3 — Feed Eligibility Gate
**Goal:** One truth for "can this be shown?"

Tasks:
- Implement `is_feed_eligible(happening)`
- Add reasons array
- Write unit tests for all failure modes
- Add DB contract tests (no 00:00, date_precision rules)

Deliverable:
- Deterministic eligibility + tests

---

### Milestone 4 — Tier B Exception (Maennedorf)
**Goal:** Prove exception model works without contaminating system.

Tasks:
- Implement Maennedorf adapter (already approved)
- Ensure quarantine
- Add adapter-specific tests
- Verify eligibility + contract tests still pass

Deliverable:
- Tier B works without breaking rules

---

### Milestone 5 — Admin Coverage & Gaps View
**Goal:** Human-in-the-loop control.

Tasks:
- Create SQL view:
  - organizer → raw → canonical → eligible counts
  - missing mandatory fields
  - tier distribution
- Expose as spreadsheet-style UI
- Add manual patch flow:
  - create internal_manual source record
  - re-run merge

Deliverable:
- Admin can see gaps + fix them

---

### Milestone 6 — Manual Creation (No Source)
**Goal:** Support physical-only info.

Tasks:
- Admin UI to create happening/course manually
- Creates `source_*` with `source_type='internal_manual'`
- Highest merge precedence
- Eligible for feed immediately if requirements met

Deliverable:
- Manual happenings & courses fully supported

---

## Ralph Loop (WHEN TO USE)
Do NOT use autonomous loops until:
- Milestones 1–3 pass end-to-end manually
- Tests are green

When ready:
- Introduce a lightweight Ralph loop:
  - prd.md required
  - progress.txt updated per feature
  - each feature must add tests
  - lint + test before next loop

Automation accelerates correctness — it does not replace it.

---

## Success Criteria (v1)
- No invalid cards in feed
- Clear audit trail for every field
- Manual enrichment works
- Tier discipline enforced
- Claude can implement features without ambiguity
