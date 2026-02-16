-- 017_happening_classification_columns.sql
-- Adds lightweight classification columns to public.happening.
-- Safe to run multiple times (ADD COLUMN IF NOT EXISTS).
-- Does NOT touch source tables. Does NOT backfill.

-- ============================================================
-- UP
-- ============================================================

ALTER TABLE public.happening
  ADD COLUMN IF NOT EXISTS audience_tags text[] NOT NULL DEFAULT '{}'::text[];

ALTER TABLE public.happening
  ADD COLUMN IF NOT EXISTS topic_tags text[] NOT NULL DEFAULT '{}'::text[];

ALTER TABLE public.happening
  ADD COLUMN IF NOT EXISTS editorial_priority int NOT NULL DEFAULT 0;


-- ============================================================
-- DOWN (rollback) — run separately if you need to undo
-- ============================================================
-- ALTER TABLE public.happening DROP COLUMN IF EXISTS audience_tags;
-- ALTER TABLE public.happening DROP COLUMN IF EXISTS topic_tags;
-- ALTER TABLE public.happening DROP COLUMN IF EXISTS editorial_priority;
