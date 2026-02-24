# Project Decisions (Source of Truth)

## Decision: GitHub is the source of truth
**All behavior changes must be committed to this repo.**

### What lives where
- **GitHub (this repo):** pipeline logic (scrape → normalize → schedules → write to DB)
- **Supabase:** data storage + *thin* views for consumption (no “business logic” that only exists in SQL)
- **Lovable:** UI only (no hidden rules that change what data means)

### Rules of thumb
1. If it changes *what users see* → it must be reproducible from Git.
2. If a Supabase view is updated → its SQL must be copied into this repo (e.g. `sql/views/...`).
3. Lovable should read from a stable view name (e.g. `this_weekend_events`) so the UI doesn’t churn.


---

## Decision: Canonicalization Phase 1 is complete (deterministic, no inference)

**Phase 1 goal:**  
Build a deterministic, auditable canonicalization pipeline that converts messy real-world inputs into canonical Happenings **without guessing times**, **without false merges**, and with **clear provenance**.

### Phase 1 success criteria
- No false auto-merges
- No inferred or guessed times (no placeholders)
- No hidden coupling between raw and canonical layers
- `needs_review` shrinks over time and becomes **meaningful** (only real ambiguity remains)

### Current state at Phase 1 close-out
- Technical duplicate canonicals eliminated (e.g. *Playgrounds*, *Familientanzen*)
- Archived canonicals are excluded from all matching candidate paths
- Ingestion noise and header artifacts removed from the review set (e.g. `title_raw = "Kopfzeile"`)
- Remaining needs_review rows intentionally represent semantic ambiguity (program/umbrella concepts; Phase 2). “Series” remains a schedule shape on offering.
  (series / umbrella / program-level concepts)

### Locked model rule (Phase 1 invariant)
**Time and date must never live on `happening`.**

Canonical model responsibilities:
- **`happening`** — identity + feed card  
  (title, description, audience, organizer, visibility)
- **`offering`** — schedule container  
  (date range, recurrence, timezone)
- **`occurrence`** — concrete dated instance  
  (`start_at`, `end_at`, venue)
- **`source_happenings`** — raw scraped inputs with strict contracts
- **`happening_sources`** — provenance join table (source → canonical)

### Rule of thumb
**Phase 1 success ≠ zero reviews**  
**Phase 1 success = reviews exist only for real-world ambiguity, not technical bugs**

---

## Decision: Phase 1 hardening rules for ingestion, matching, and review hygiene

To keep canonicalization deterministic and to prevent review noise from accumulating, the following rules are enforced.

### Raw ingestion contract rules (non-negotiable)
1. **No time guessing**
   - Never invent or default times (e.g. midnight placeholders are forbidden).
2. **Precision must match evidence**
   - If `date_precision = 'datetime'`, then `datetime_raw` must be present and consistent with `start_at` / `end_at`.
3. **Noise titles are not events**
   - Known header or table artifacts (e.g. `title_raw = "Kopfzeile"`) must not remain in `needs_review`.
   - Such rows are auto-rejected (marked processed with a clear rejection reason).
4. **Invalid or incomplete rows**
   - Rows with missing required fields are either:
     - sent to `needs_review` with an explicit error reason, or
     - auto-rejected if clearly non-events.

### Matching candidate hygiene
- **Archived canonicals must never be used as match candidates** (hard exclusion).
- Deduping canonicals is a first-class cleanup step to:
  - shrink candidate pools
  - enable safe auto-resolve
  - prevent review deadlocks

### Review loop behavior (Phase 1)
- Auto-resolve is allowed **only** when the decision is confidently `merge`.
- Auto-resolve may perform **only**:
  - provenance linking (`happening_sources`)
  - status transitions (`needs_review → processed`)
- Auto-resolve must **not**:
  - mutate canonical fields
  - create new canonicals

### Follow-up implementation notes
- Maintain a small banlist for known PDF/header noise titles.
- Ensure merge loops explicitly process `needs_review` rows (not only "pending/new"),
  so the system remains self-healing over time.

---

## Decision: Phase 1 — ingestion defaults to `visibility_status = 'published'`

**Date:** 2026-02-16

**Context:**
`create_happening_schedule_occurrence()` creates new canonical happenings when
the merge loop decides `kind = "create"`. The feed (`feed_cards_view`) requires
`visibility_status = 'published'` — draft happenings are invisible.

Previously the default was `"draft"`, which silently orphaned all happenings
created by the merge loop for sources without a pre-existing legacy bridge
(eventbrite_zurich, elternverein_uetikon). Migration 021 published the
existing drafts; the code now defaults to `"published"`.

**Policy (Phase 1):**
- All happenings created by the merge loop are immediately **published**.
- No moderation UI exists yet; the feed shows all future scheduled events.
- Eligibility filtering (`published` + `scheduled` + `start_at IS NOT NULL` +
  not in the past) is the only gate.

**Rationale:**
- Phase 1 product rule: show parents everything that's upcoming — no curation.
- Source data has already passed pipeline validation, dedupe-key computation,
  and merge-loop scoring before reaching the CREATE path.
- A `"draft"` default with no publishing mechanism is a silent data loss bug,
  not a safety feature.

**Future (when moderation exists):**
- Revisit default when an Admin UI or moderation workflow is built.
- Possible options: default to `"draft"` for Tier B/C sources, source-tier
  gating, or manual approval queues.
- Until then, `"published"` is the only correct default.

**Files:**
- `src/canonicalize/merge_loop.py` line 498 — sets the default
- `migrations/021_publish_draft_happenings.sql` — one-time fix for existing drafts
- `tests/test_canonical_field_invariants.py` — `test_create_sets_visibility_published`
- `tests/test_source_to_canonical_chain.py` — regression tests

---

## Decision — Minimum Trust Standard (MTS-v1.2)

Caloo defines a Minimum Trust Standard (MTS) that determines:

- Feed eligibility
- trust_status state
- Review triggers
- Confidence scoring baseline

MTS separates:

Eligibility → structural + time-based visibility
trust_status → governance clarity
confidence_score → ranking modifier

This prevents silent data drift and enforces responsible recommendation standards.

This decision locks the Feed Integrity Layer contract before implementation.

---
