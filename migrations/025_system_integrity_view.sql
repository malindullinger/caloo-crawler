-- Migration 025: System integrity dashboard (read-only diagnostics)
--
-- Creates a view that runs 8 health checks against the data model.
-- Each check returns exactly one row with: check_name, status, metric_value, details.
-- Status is 'OK' when metric_value = 0 (no problems), 'FAIL' otherwise.
--
-- Usage:  SELECT * FROM system_integrity_view;
--
-- This is a pure read-only diagnostic view. It does NOT modify any data,
-- views, or pipeline logic.

CREATE OR REPLACE VIEW public.system_integrity_view AS

-- A) Orphan occurrences: occurrence rows with no matching offering
SELECT
    'orphan_occurrences'::text              AS check_name,
    CASE WHEN count(*) = 0
         THEN 'OK' ELSE 'FAIL'
    END::text                               AS status,
    count(*)::int                           AS metric_value,
    'Occurrence rows with no matching offering'::text AS details
FROM occurrence o
WHERE NOT EXISTS (
    SELECT 1 FROM offering off WHERE off.id = o.offering_id
)

UNION ALL

-- B) Orphan offerings: offering rows with no matching happening
SELECT
    'orphan_offerings',
    CASE WHEN count(*) = 0 THEN 'OK' ELSE 'FAIL' END,
    count(*)::int,
    'Offering rows with no matching happening'
FROM offering off
WHERE NOT EXISTS (
    SELECT 1 FROM happening h WHERE h.id = off.happening_id
)

UNION ALL

-- C) Unpublished happenings that have future scheduled occurrences
SELECT
    'unpublished_future_happenings',
    CASE WHEN count(*) = 0 THEN 'OK' ELSE 'FAIL' END,
    count(*)::int,
    'Non-published happenings with future scheduled occurrences'
FROM happening h
WHERE h.visibility_status != 'published'
  AND EXISTS (
      SELECT 1
      FROM offering off
      JOIN occurrence o ON o.offering_id = off.id
      WHERE off.happening_id = h.id
        AND o.status = 'scheduled'
        AND o.start_at > now()
  )

UNION ALL

-- D) Negative duration occurrences: end_at < start_at
SELECT
    'negative_duration_occurrences',
    CASE WHEN count(*) = 0 THEN 'OK' ELSE 'FAIL' END,
    count(*)::int,
    'Occurrences where end_at < start_at'
FROM occurrence o
WHERE o.end_at IS NOT NULL
  AND o.start_at IS NOT NULL
  AND o.end_at < o.start_at

UNION ALL

-- E) Missing timezone: scheduled occurrences whose offering has NULL timezone
SELECT
    'missing_timezone_occurrences',
    CASE WHEN count(*) = 0 THEN 'OK' ELSE 'FAIL' END,
    count(*)::int,
    'Scheduled occurrences whose offering has NULL timezone'
FROM occurrence o
JOIN offering off ON off.id = o.offering_id
WHERE off.timezone IS NULL
  AND o.status = 'scheduled'

UNION ALL

-- F) Published happenings with no source provenance
SELECT
    'happenings_without_sources',
    CASE WHEN count(*) = 0 THEN 'OK' ELSE 'FAIL' END,
    count(*)::int,
    'Published happenings with no happening_sources rows'
FROM happening h
WHERE h.visibility_status = 'published'
  AND NOT EXISTS (
      SELECT 1 FROM happening_sources hs WHERE hs.happening_id = h.id
  )

UNION ALL

-- G) Feed count vs eligible occurrence count drift
--    feed_cards_view is a filtered subset of eligible occurrences.
--    If feed count exceeds eligible count, a structural bug exists.
SELECT
    'feed_vs_occurrence_count_drift',
    CASE WHEN f.cnt <= e.cnt THEN 'OK' ELSE 'FAIL' END,
    GREATEST(f.cnt - e.cnt, 0)::int,
    format('feed_cards=%s eligible_occurrences=%s', f.cnt, e.cnt)
FROM
    (SELECT count(*) AS cnt FROM feed_cards_view) f,
    (SELECT count(*) AS cnt
     FROM occurrence o
     JOIN offering off ON off.id = o.offering_id
     JOIN happening h  ON h.id   = off.happening_id
     WHERE h.visibility_status = 'published'
       AND o.status = 'scheduled'
       AND o.start_at IS NOT NULL
    ) e

UNION ALL

-- H) Detail view visibility mismatch
--    occurrence_detail_view filters for published only.
--    If any non-published rows appear, the view filter is broken.
SELECT
    'detail_vs_feed_visibility_mismatch',
    CASE WHEN count(*) = 0 THEN 'OK' ELSE 'FAIL' END,
    count(*)::int,
    'Detail view rows where visibility_status is not published'
FROM occurrence_detail_view
WHERE visibility_status != 'published';
