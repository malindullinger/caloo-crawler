# System Integrity Dashboard

> Last updated: 2026-02
>
> Migrations: `025`, `027`, `029_system_integrity_severity.sql`

## Overview

`public.system_integrity_view` is a read-only diagnostic view that runs
11 health checks against the data model. Each check returns exactly one
row. It does not modify any data, views, or pipeline logic.

**Expected steady state:** all rows have `status = 'OK'`.

A `FAIL` status with `severity = 'WARN'` is informational — it flags
data quality issues that do NOT indicate a system error.

---

## Usage

```sql
SELECT * FROM system_integrity_view;
```

Returns 11 rows with columns:

| Column | Type | Description |
|--------|------|-------------|
| `check_name` | TEXT | Unique identifier for the check |
| `status` | TEXT | `'OK'` or `'FAIL'` (whether the condition was detected) |
| `severity` | TEXT | `'FAIL'` (action needed) or `'WARN'` (informational) |
| `metric_value` | INT | Count of problematic items (0 = healthy) |
| `details` | TEXT | Human-readable explanation or diagnostic counters |

### Quick check queries

```sql
-- Critical issues only (severity = FAIL)
SELECT * FROM system_integrity_view
WHERE status = 'FAIL' AND severity = 'FAIL';

-- Warnings only
SELECT * FROM system_integrity_view
WHERE status = 'FAIL' AND severity = 'WARN';
```

---

## Checks

### A) `orphan_occurrences` — severity: FAIL

Occurrence rows where `offering_id` does not match any row in `offering`.

**Cause:** Manual deletion of an offering without cascading, or a
migration error.

**Fix:** Investigate and either delete the orphan or re-link it.

### B) `orphan_offerings` — severity: FAIL

Offering rows where `happening_id` does not match any row in `happening`.

**Cause:** Same as above — manual deletion or migration error.

**Fix:** Investigate and either delete the orphan or re-link it.

### C) `unpublished_future_happenings` — severity: FAIL

> Rewritten: migration 029

Happenings with `visibility_status != 'published'` that have **both**:
1. Future scheduled occurrences
2. At least one `happening_sources` link

These happenings meet the Phase-1 publish policy (sources + future
occurrences) but are not published. This is a genuine error — the
happening should be published or its occurrences cancelled.

**Cause:** Merge loop skipped the publish step, or the happening was
manually un-published after being linked to sources.

**Fix:** Publish the happening, or investigate why it was un-published.

### D) `negative_duration_occurrences` — severity: FAIL

Occurrences where `end_at < start_at`.

**Cause:** Source data error or normalization bug. Should never happen
with correct source adapters.

**Fix:** Correct the occurrence times, or check the source adapter.

### E) `missing_timezone_occurrences` — severity: FAIL

Scheduled occurrences whose offering has `timezone IS NULL`.

**Cause:** Offering created without a timezone. All offerings should
default to `'Europe/Zurich'`.

**Fix:** Set the timezone on the offering.

### F) `happenings_without_sources` — severity: FAIL

Published happenings with no rows in `happening_sources`. These
happenings have no provenance trail — they exist in the canonical
model but can't be traced back to any source.

**Cause:** Manual creation without linking a source, or a merge loop
bug that published but didn't link.

**Fix:** Investigate whether the happening should exist. If from a
source, re-run the merge. If manually created, add a source link or
leave as-is (with awareness).

### G) `feed_vs_occurrence_count_drift` — severity: FAIL

Compares `count(feed_cards_view)` with the count of eligible
occurrences (published + scheduled + start_at IS NOT NULL).

The feed is a time-windowed subset of eligible occurrences, so
`feed_count <= eligible_count` is always expected. If feed exceeds
eligible, a structural bug exists in the feed view.

**metric_value:** Excess feed rows (0 when healthy). Both counts
are shown in `details`.

**Cause of FAIL:** Feed view is returning rows that don't meet
the eligibility criteria. Check for missing WHERE clauses.

### H) `detail_vs_feed_visibility_mismatch` — severity: FAIL

Rows in `occurrence_detail_view` where `visibility_status != 'published'`.

The detail view has `WHERE h.visibility_status = 'published'`, so
this should always return 0. If it doesn't, the detail view filter
is broken.

**Cause of FAIL:** Someone modified the detail view and removed or
weakened the visibility filter.

**Fix:** Restore the filter in the view definition.

### I) `low_confidence_happenings` — severity: WARN

> Downgraded to WARN: migration 029

Published happenings where `confidence_score < 50`.

A high count indicates many happenings have poor source metadata
(missing images, descriptions, URLs, etc.). This is a data-quality
signal — it does NOT block the feed or hide any happenings.

**metric_value:** Count of low-confidence published happenings.

**details:** Shows count and the minimum confidence score found.

**Cause of FAIL status:** Source adapters are producing incomplete
metadata. Check the [confidence model formula](confidence-model.md)
and the `recompute_confidence_scores` script.

**Action:** Improve source adapters, add missing metadata, or accept
the current quality level. Low confidence is not a system error.

### J) `tier_b_without_image_ratio` — severity: FAIL

Percentage of published Tier B happenings whose primary source has
no image. Threshold: **FAIL if > 20%** (more than 1 in 5).

Tier B sources use text heuristics for extraction. Image coverage
is expected to be lower, but not absent. A 20% threshold flags
systemic image gaps without triggering on occasional misses.

**metric_value:** Percentage as integer (e.g., 25 = 25%).

**details:** Shows tier_b_total, without_image count, and precise
ratio.

**Cause of FAIL:** Tier B source adapters are not extracting images,
or the sources genuinely don't provide images.

**Fix:** Check source adapter image extraction. If the source has
no images, the ratio is structural and can be accepted.

### K) `draft_future_without_sources` — severity: WARN

> Added: migration 029

Non-published happenings that have future scheduled occurrences but
**no** `happening_sources` link. These don't meet the Phase-1 publish
policy, so they are expected to remain unpublished.

This is informational — it shows draft happenings that exist in the
data model but have no provenance and therefore cannot be published
under the current policy.

**metric_value:** Count of such draft happenings.

**Cause of FAIL status:** Happenings were created (possibly manually
or by a partial merge) without linking sources.

**Action:** If the happening should be published, add a source link.
If it's an incomplete draft, no action needed — the WARN is expected.

### Integrity guardrail: editorial priority out of range

`editorial_priority_out_of_range` is a WARN if any happening has
`editorial_priority` outside `[-100, 100]`.

WARN does not block deploys, but should be corrected.

See [ranking.md](ranking.md) for the editorial priority contract.

---

## Severity model

| Severity | Meaning | Action |
|----------|---------|--------|
| `FAIL` | Structural or policy violation | Investigate and fix |
| `WARN` | Data quality signal or expected draft state | Review, may accept |

An integrity `FAIL` does NOT block the feed. It indicates a condition
that should be investigated. WARN checks are purely informational.

---

## Note on confidence_score

`happening.confidence_score` (migration 026) is a data-quality signal,
not a feed filter. Checks I and K use confidence/source data for
diagnostic purposes only.

See [confidence-model.md](confidence-model.md) for the formula and
query examples.

---

## When to run

- After applying any migration
- After a full pipeline run (crawl + merge)
- As a pre-deploy sanity check
- When investigating data anomalies

---

## Interpreting results

| Scenario | Action |
|----------|--------|
| All `OK` | Healthy. No action needed. |
| `FAIL` with `severity = 'FAIL'` | Structural issue. Investigate per check guidance above. |
| `FAIL` with `severity = 'WARN'` | Data quality issue or expected draft. Review and accept or improve sources. |
| `feed_vs_occurrence_count_drift` shows large gap (but OK) | Normal — the feed is windowed. The gap grows as more past events accumulate. |

---

## Relationship to other views

| View | Purpose | Modified by migration 029? |
|------|---------|----------------------------|
| `feed_cards_view` | Feed contract (LOCKED) | No |
| `occurrence_detail_view` | Detail page enrichment | No |
| `system_integrity_view` | Diagnostics (this view) | Refactored (11 checks, severity column) |
| `low_confidence_dashboard_view` | Admin diagnostics (migration 028) | No |
