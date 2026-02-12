# ARCHITECTURE — Caloo Canonicalization System

## Purpose
This document defines the **canonical data architecture** for Caloo’s Happenings & Courses ingestion system.

Its goals are to:
- make the system **deterministic, auditable, and testable**
- prevent silent data corruption (especially around time handling)
- provide a stable mental model for future development (Phase 2+)
- act as the architectural source of truth alongside `DECISIONS.md`

This document focuses on **how the system is structured and why**, not on UI or implementation details.

---

## Core Principles (Non-Negotiable)

1. **Never invent data**
   - Times are never guessed.
   - No default `00:00`.
   - No inferred end times.
2. **Precision must match evidence**
   - `date_precision='date'` ⇒ no time fields allowed.
   - `date_precision='datetime'` ⇒ explicit raw datetime required.
3. **Canonical objects are stable**
   - Canonicals change only through deterministic merges or explicit admin action.
4. **Provenance is mandatory**
   - Every canonical record must be traceable to its source records.
5. **Ambiguity is allowed**
   - The system must prefer `needs_review` over incorrect certainty.

---

## Domain Model (Locked)

### Canonical Objects

#### Happening
A **Happening** represents one feed card in the *Happenings* UI.

It is an **identity object**, not a schedule.

Responsibilities:
- Title
- Description
- Audience
- Organizer
- Visibility status

Explicitly does **not** contain:
- dates
- times
- recurrence
- venues

> **Invariant:**  
> Time and date must never live on `happening`.

---

#### Offering
An **Offering** represents a *schedule container* for a Happening.

Responsibilities:
- Date range (`start_date`, `end_date`)
- Timezone
- Recurrence type (e.g. `one_off`, `series`)

An offering may produce one or more occurrences.

---

### Series vs Program (important distinction)
- **`series` (offering_type)** describes a **schedule shape** (e.g. a multi-date range or recurring pattern).
- **Program / umbrella concepts** describe a **semantic container** (Phase 2) that may group multiple happenings under one theme (e.g. “CoolDay”, “Kino für die Chline”).

Phase 1 supports `series` as a scheduling construct only. Program-level grouping is intentionally deferred to Phase 2.

---

#### Occurrence
An **Occurrence** is a concrete, dated instance of an offering.

Responsibilities:
- `start_at`
- `end_at` (only if explicitly present)
- Venue (optional but preferred)

Uniqueness:
- `(offering_id, start_at)` must be unique

---

### Source Objects

#### source_happenings
Raw, immutable inputs from ingestion.

Properties:
- `title_raw`
- `datetime_raw`
- `location_raw`
- `date_precision`
- extracted `start_at` / `end_at` (if valid)
- extraction metadata and error messages

These records:
- are never mutated after ingestion
- may be invalid, incomplete, or noisy
- drive all canonicalization decisions

---

#### happening_sources
Join table linking:
- one `source_happening`
- to one canonical `happening`

Responsibilities:
- provenance
- merge traceability
- confidence explanation

---

#### happening_field_provenance (optional, Phase 2+)
Tracks per-field origin and precedence.
Not required for Phase 1 correctness.

---

## Courses (Separate Track)

Courses are **not** Happenings.

- Courses live in their own tables.
- Courses may have sessions/occurrences.
- Courses never appear in the Happenings feed.

This separation is intentional and enforced at the schema level.

---

## Canonicalization Flow (Phase 1)

1. **Ingest raw source records**
   - Apply strict contracts.
   - Reject obvious noise early where possible.
2. **Validate contracts**
   - Time precision rules
   - No missing required fields without review status
3. **Match candidates**
   - Prefer stable external IDs
   - Fallback fingerprint: normalized title + date (+ venue)
4. **Score candidates**
   - Deterministic scoring only
   - Near-tie guard enforced
5. **Decide**
   - `merge` → link provenance only
   - `create` → new canonical
   - `needs_review` → stop, do nothing

> Phase 1 allows **no inference and no field mutation** during auto-resolve.

---

## Review Lifecycle

### Status values (Phase 1)
- `processed`
- `needs_review`

> A non-zero `needs_review` set is expected and healthy.

### Meaning of `needs_review`
A record is in `needs_review` **only if**:
- multiple real-world interpretations exist, or
- required information is missing, or
- the source is semantically ambiguous (e.g. series/program concepts)

Noise, headers, and invalid rows **must not remain** in `needs_review`.

---

## Archived Canonicals

- Archived happenings **must never** be used as match candidates.
- Archived canonicals must have:
  - zero offerings
  - zero source links

This prevents deadlocks and infinite review loops.

---

## Phase 1 Completion Definition

Phase 1 is considered complete when:
- All technical duplicates are eliminated.
- Archived canonicals are fully excluded from matching.
- `needs_review` contains only **real-world ambiguity**.
- No auto-merge performs inference or mutation.

Phase 1 success ≠ zero reviews  
Phase 1 success = **meaningful reviews only**

---

## Phase Boundaries

### Phase 1 (Completed)
- Deterministic canonicalization
- Strict time contracts
- Provenance
- Review hygiene

### Phase 2 (Planned)
- Explicit modeling of series / program containers
- Improved PDF extraction
- Richer admin tooling
- Eligibility reasoning surfaced to UI

No Phase-2 work may violate Phase-1 invariants.

---

## Testing Expectations

Every architectural invariant must be:
- enforced by code
- covered by tests
- reproducible from Git

Test layers:
1. Adapter tests (per source)
2. Canonicalization & contract tests
3. Eligibility & view-model tests

---

## Final Note

This architecture intentionally prefers **correct uncertainty** over false certainty.

Ambiguity is not a failure state — it is a first-class outcome.
