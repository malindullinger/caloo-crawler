# System Integrity Dashboard

> Last updated: 2026-02
>
> Migrations: `025_system_integrity_view.sql`, `027_system_integrity_confidence_extension.sql`

## Overview

`public.system_integrity_view` is a read-only diagnostic view that runs
10 health checks against the data model. Each check returns exactly one
row. It does not modify any data, views, or pipeline logic.

**Expected steady state:** all rows have `status = 'OK'`.

---

## Usage

```sql
SELECT * FROM system_integrity_view;
```

Returns 10 rows with columns:

| Column | Type | Description |
|--------|------|-------------|
| `check_name` | TEXT | Unique identifier for the check |
| `status` | TEXT | `'OK'` (metric_value = 0) or `'FAIL'` (metric_value > 0) |
| `metric_value` | INT | Count of problematic items (0 = healthy) |
| `details` | TEXT | Human-readable explanation or diagnostic counters |

---

## Checks

### A) `orphan_occurrences`

Occurrence rows where `offering_id` does not match any row in `offering`.

**Cause:** Manual deletion of an offering without cascading, or a
migration error.

**Fix:** Investigate and either delete the orphan or re-link it.

### B) `orphan_offerings`

Offering rows where `happening_id` does not match any row in `happening`.

**Cause:** Same as above — manual deletion or migration error.

**Fix:** Investigate and either delete the orphan or re-link it.

### C) `unpublished_future_happenings`

Happenings with `visibility_status != 'published'` that have future
scheduled occurrences. These events exist in the data but are invisible
to the feed and detail pages.

**Cause:** Happening was drafted or archived but its occurrences were
not cancelled. Or the publish step in the merge loop was skipped.

**Fix:** Either publish the happening or cancel its occurrences.

### D) `negative_duration_occurrences`

Occurrences where `end_at < start_at`.

**Cause:** Source data error or normalization bug. Should never happen
with correct source adapters.

**Fix:** Correct the occurrence times, or check the source adapter.

### E) `missing_timezone_occurrences`

Scheduled occurrences whose offering has `timezone IS NULL`.

**Cause:** Offering created without a timezone. All offerings should
default to `'Europe/Zurich'`.

**Fix:** Set the timezone on the offering.

### F) `happenings_without_sources`

Published happenings with no rows in `happening_sources`. These
happenings have no provenance trail — they exist in the canonical
model but can't be traced back to any source.

**Cause:** Manual creation without linking a source, or a merge loop
bug that published but didn't link.

**Fix:** Investigate whether the happening should exist. If from a
source, re-run the merge. If manually created, add a source link or
leave as-is (with awareness).

### G) `feed_vs_occurrence_count_drift`

Compares `count(feed_cards_view)` with the count of eligible
occurrences (published + scheduled + start_at IS NOT NULL).

The feed is a time-windowed subset of eligible occurrences, so
`feed_count <= eligible_count` is always expected. If feed exceeds
eligible, a structural bug exists in the feed view.

**metric_value:** Excess feed rows (0 when healthy). Both counts
are shown in `details`.

**Cause of FAIL:** Feed view is returning rows that don't meet
the eligibility criteria. Check for missing WHERE clauses.

### H) `detail_vs_feed_visibility_mismatch`

Rows in `occurrence_detail_view` where `visibility_status != 'published'`.

The detail view has `WHERE h.visibility_status = 'published'`, so
this should always return 0. If it doesn't, the detail view filter
is broken.

**Cause of FAIL:** Someone modified the detail view and removed or
weakened the visibility filter.

**Fix:** Restore the filter in the view definition.

### I) `low_confidence_happenings`

> Added: migration 027

Published happenings where `confidence_score < 50`.

A high count indicates many happenings have poor source metadata
(missing images, descriptions, URLs, etc.). This is a data-quality
signal — it does NOT block the feed or hide any happenings.

**metric_value:** Count of low-confidence published happenings.

**details:** Shows count and the minimum confidence score found.

**Cause of FAIL:** Source adapters are producing incomplete metadata.
Check the [confidence model formula](confidence-model.md) and the
`recompute_confidence_scores` script.

**Fix:** Improve source adapters, add missing metadata, or accept
the current quality level (low confidence is not a system error).

### J) `tier_b_without_image_ratio`

> Added: migration 027

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

---

## Note on confidence_score thresholds

`happening.confidence_score` (migration 026) is a data-quality signal,
not a feed filter. Checks I and J use confidence data for diagnostic
purposes only. A `FAIL` on these checks means "review your source
quality" — it does NOT mean "the system is broken".

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
| One or more `FAIL` | Check `metric_value` for severity and `details` for context. Investigate per check guidance above. |
| `feed_vs_occurrence_count_drift` shows large gap (but OK) | Normal — the feed is windowed. The gap grows as more past events accumulate. |
| `low_confidence_happenings` FAIL | Data quality issue, not system error. Review source adapters or accept current quality. |
| `tier_b_without_image_ratio` FAIL | > 20% of Tier B happenings lack images. Check source adapters or accept if sources lack images. |

---

## Relationship to other views

| View | Purpose | Modified by migration 027? |
|------|---------|----------------------------|
| `feed_cards_view` | Feed contract (LOCKED) | No |
| `occurrence_detail_view` | Detail page enrichment | No |
| `system_integrity_view` | Diagnostics (this view) | Extended (10 checks) |
| `low_confidence_dashboard_view` | Admin diagnostics (migration 028) | No (separate migration) |
