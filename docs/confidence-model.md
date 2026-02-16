# Confidence Scoring Model

> Last updated: 2026-02
>
> Describes how the merge loop decides whether a source row matches
> an existing canonical happening.

Source code: `src/canonicalize/matching.py` (scoring),
`src/canonicalize/merge_loop.py` (decision logic).
For telemetry see [ops_runbook_merge_loop.md](ops_runbook_merge_loop.md).

---

## Overview

When a new source row arrives, the merge loop must decide:

| Decision | Meaning |
|----------|---------|
| **create** | No match found; create a new canonical happening |
| **merge** | High-confidence match; link source to existing happening |
| **review** | Ambiguous match; queue for human review |

The decision is based on a **confidence score** (0.0–1.0) computed
for each candidate happening.

---

## Scoring function

`confidence_score(happening, offering, source_row) → float`

### Signals and base weights

| Signal | Base weight | Comparison method |
|--------|------------|-------------------|
| **Title** | 0.50 | Jaccard token similarity (normalized, lowercased, punctuation-stripped) |
| **Date** | 0.30 | Binary: 1.0 if source date falls within offering date range, else 0.0 |
| **Venue** | 0.20 | Jaccard token similarity (normalized, Swiss street abbreviations expanded) |

### Dynamic weight renormalization

Not all signals are always available:
- Venue is often unavailable (canonical happenings may not have a
  venue name yet)
- If a signal is unavailable, its weight is set to 0 and the
  remaining weights are renormalized to sum to 1.0

**Example:** If venue is unavailable (common case):
- Title weight: `0.50 / 0.80 = 0.625`
- Date weight: `0.30 / 0.80 = 0.375`
- Max possible score: 1.0 (still achievable)

This prevents the common situation where "max score < threshold"
just because venue data doesn't exist yet.

---

## Decision thresholds

| Constant | Value | Defined in |
|----------|-------|------------|
| `CONFIDENCE_THRESHOLD` | **0.85** | `matching.py` |
| `NEAR_TIE_DELTA` | **0.03** | `merge_loop.py` |
| `PERFECT_CONFIDENCE` | **1.0** | `merge_loop.py` |
| `PERFECT_TIE_EPS` | **1e-9** | `merge_loop.py` |

---

## Decision flowchart

```
score all candidate happenings
       │
       ▼
   top score < 0.85?  ──yes──►  CREATE (no match)
       │ no
       ▼
   ≥ 2 candidates at 1.0?  ──yes──►  REVIEW (perfect tie)
       │ no
       ▼
   top − second < 0.03?  ──yes──►  REVIEW (near tie)
       │ no
       ▼
   MERGE (confident match)
```

### In words

1. **Below threshold → create.** If the best candidate scores below
   0.85, no match exists. A new canonical happening is created.

2. **Perfect tie → review.** If two or more candidates both score 1.0
   (within float epsilon), the system cannot safely choose. Sent to
   human review to prevent duplicate canonicals or wrong merges.

3. **Near tie → review.** If the top two candidates are within 0.03
   of each other (and both above threshold), the match is ambiguous.
   Sent to review.

4. **Clear winner → merge.** The top candidate scores above 0.85 and
   is clearly ahead of the second candidate. Source row is linked to
   the existing happening.

---

## What triggers reviews (common causes)

| Cause | Example | Resolution |
|-------|---------|------------|
| Similar titles, same date | "Kinderyoga" and "Yoga für Kinder" on same date | Admin merges or marks distinct |
| Same organizer, adjacent dates | Weekly market appearing as two candidates | Admin confirms series or distinct |
| Missing venue data | Two happenings match on title + date but have different venues not yet in the system | Add venue data to improve discrimination |

---

## Telemetry

Confidence scores are recorded per merge run in `merge_run_stats`:

| Column | Content |
|--------|---------|
| `confidence_min` | Lowest score across all scored rows |
| `confidence_avg` | Mean score |
| `confidence_max` | Highest score |
| `confidence_histogram` | `{"0_50": N, "50_70": N, "70_85": N, "85_90": N, "90_95": N, "95_100": N}` |
| `source_confidence` | Per-source breakdown: `{"source_id": {"min": F, "avg": F, "max": F, "count": N, "hist": {...}}}` |

See `src/db/confidence_telemetry.py` for bucket definitions.

---

## Invariants

1. **No inference.** The scoring function only compares existing data.
   It never invents missing values or guesses.
2. **Deterministic.** Same inputs always produce the same score.
3. **Archived happenings excluded.** Candidates with
   `visibility_status = 'archived'` are never scored.
4. **Phase 3 contract.** Only rows with `dedupe_key` starting with
   `"v1|"` are processed. Legacy rows are permanently quarantined.

---

## Data-Quality Confidence Score (v1)

> Added: 2026-02 (migration 026)

A **separate, unrelated** concept from the match confidence above.

**Purpose:** Measures how complete and reliable a happening's source
metadata is. Used for admin review prioritization, ops monitoring,
and future source weighting.

**Module:** `src/canonicalize/confidence.py` → `compute_confidence_score()`

**Range:** 0–100 (integer). Column: `happening.confidence_score`.

**CRITICAL:** This score **NEVER filters feed visibility**. It is a
quality signal for ranking and review only.

### v1 Formula

Starts at 100 and applies penalties:

| Condition | Penalty |
|-----------|---------|
| `date_precision = 'date'` (no time info) | -20 |
| `image_url` missing or empty | -20 |
| `description` missing or empty | -15 |
| `source_tier = 'B'` | -10 |
| `extraction_method` is not `'jsonld'` | -15 |
| `timezone` missing or empty | -30 |
| `canonical_url` missing or empty | -40 |

Clamped to `[0, 100]`.

### When computed

- **CREATE:** Set on the new happening payload.
- **MERGE:** Recomputed after field/tag updates. Only written if changed.
- **Backfill:** `python -m scripts.recompute_confidence_scores` (dry-run default).

### Querying

```sql
-- Low-confidence happenings needing review
SELECT id, title, confidence_score
FROM happening
WHERE visibility_status = 'published'
  AND confidence_score < 50
ORDER BY confidence_score;
```

### Naming disambiguation

| Term | Type | Module | Column |
|------|------|--------|--------|
| Match confidence | float 0.0–1.0 | `matching.py` | `merge_run_stats.confidence_*` |
| Data-quality confidence | int 0–100 | `confidence.py` | `happening.confidence_score` |
