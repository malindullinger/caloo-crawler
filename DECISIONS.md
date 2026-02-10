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
- Remaining `needs_review` rows intentionally represent **semantic ambiguity**
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
- Ensure merge loops explicitly process `needs_review` rows (not only “pending/new”),
  so the system remains self-healing over time.

---

