-- 022_fix_source_tiers.sql
-- Fix source_tier metadata to match documented tiering.
--
-- Context:
--   storage.py hardcodes source_tier='A' for all sources (pipeline bug).
--   The documented tiers (docs/tier-b-sources.md) are:
--     eventbrite_zurich     → A  (JSON-LD structured extraction)
--     maennedorf_portal     → B  (text_heuristic, approved municipal exception)
--     elternverein_uetikon  → B  (text_heuristic, FairGate SPA regex parsing)
--
-- This migration corrects the DB to match documentation.
-- Tier = extraction reliability, not organizer quality.
--
-- No behavior change: source_tier is metadata used for source_priority
-- in happening_sources (Tier A = 300, B = 200) but does not affect
-- feed eligibility or visibility.
--
-- Safe to run multiple times (idempotent — WHERE clause prevents no-op updates).

-- ============================================================
-- UP
-- ============================================================

-- maennedorf_portal: documented Tier B (text_heuristic, municipal exception)
UPDATE source_happenings
SET    source_tier = 'B',
       updated_at  = now()
WHERE  source_id   = 'maennedorf_portal'
  AND  source_tier IS DISTINCT FROM 'B';

-- elternverein_uetikon: classified Tier B (text_heuristic, FairGate SPA)
UPDATE source_happenings
SET    source_tier = 'B',
       updated_at  = now()
WHERE  source_id   = 'elternverein_uetikon'
  AND  source_tier IS DISTINCT FROM 'B';

-- eventbrite_zurich: already correct (Tier A, JSON-LD), but enforce for safety
UPDATE source_happenings
SET    source_tier = 'A',
       updated_at  = now()
WHERE  source_id   = 'eventbrite_zurich'
  AND  source_tier IS DISTINCT FROM 'A';

-- ============================================================
-- Verification (run after migration to confirm)
-- ============================================================
-- SELECT source_id, source_tier, count(*)
-- FROM source_happenings
-- GROUP BY source_id, source_tier
-- ORDER BY source_id;
--
-- Expected:
--   eventbrite_zurich     | A | ...
--   elternverein_uetikon  | B | ...
--   maennedorf_portal     | B | ...

-- ============================================================
-- DOWN (rollback) — revert to previous state
-- ============================================================
-- UPDATE source_happenings SET source_tier = 'A'
-- WHERE source_id IN ('maennedorf_portal', 'elternverein_uetikon')
--   AND source_tier = 'B';
