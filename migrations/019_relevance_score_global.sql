-- 019_relevance_score_global.sql
-- Adds deterministic relevance scoring column to public.happening.
-- Used by feed_cards_view ORDER BY for within-section ranking.
-- Safe to run multiple times (ADD COLUMN IF NOT EXISTS).
-- Does NOT backfill — use scripts/recompute_relevance_scores.py after applying.

-- ============================================================
-- UP
-- ============================================================

ALTER TABLE public.happening
  ADD COLUMN IF NOT EXISTS relevance_score_global INT NOT NULL DEFAULT 0;

COMMENT ON COLUMN public.happening.relevance_score_global IS
  'Deterministic ranking score computed from audience_tags + topic_tags. '
  'Recomputed on create, merge, and via recompute script. '
  'Higher = ranked earlier within a section. editorial_priority is a separate, higher-priority sort key.';

-- ============================================================
-- DOWN (rollback) — run separately if you need to undo
-- ============================================================
-- ALTER TABLE public.happening DROP COLUMN IF EXISTS relevance_score_global;
