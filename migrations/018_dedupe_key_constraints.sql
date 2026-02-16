-- 018_dedupe_key_constraints.sql
-- Finalize the dedupe_key contract with proper unique constraints and indexes.
--
-- Context:
--   - storage.py upserts with ON CONFLICT (source_id, dedupe_key)
--   - Migration 011 dropped the old (source_id, external_id) unique index
--   - But no migration ever created a UNIQUE index on (source_id, dedupe_key)
--   - PostgREST was silently accepting ON CONFLICT without a backing constraint
--   - This migration fixes that gap
--
-- Safe to run multiple times (IF NOT EXISTS / guarded).

-- ============================================================
-- STEP 1: Backfill — handle any NULL dedupe_key rows
-- ============================================================
-- The pipeline always computes dedupe_key in Python before upsert.
-- Any NULL rows are legacy/orphan rows that predate the v1 contract.
-- We give them a stable fallback key derived from their DB id so the
-- NOT NULL constraint can be applied without data loss.
-- These rows will NOT match future upserts (which use content-based keys),
-- so they won't interfere with deduplication.

UPDATE source_happenings
SET dedupe_key = 'v1|legacy-' || id::text
WHERE dedupe_key IS NULL;

UPDATE source_courses
SET dedupe_key = 'v1|legacy-' || id::text
WHERE dedupe_key IS NULL;


-- ============================================================
-- STEP 2: NOT NULL constraint
-- ============================================================
-- After backfill, all rows have dedupe_key. Make it required.
-- ALTER COLUMN SET NOT NULL is idempotent in effect (Postgres will
-- succeed if already NOT NULL, or apply if nullable).

ALTER TABLE source_happenings
  ALTER COLUMN dedupe_key SET NOT NULL;

ALTER TABLE source_courses
  ALTER COLUMN dedupe_key SET NOT NULL;


-- ============================================================
-- STEP 3: Unique constraint on (source_id, dedupe_key)
-- ============================================================
-- This backs the ON CONFLICT (source_id, dedupe_key) used by upserts.
-- Without this, PostgREST ON CONFLICT was silently doing nothing useful
-- when rows had the same pair but no DB constraint to detect it.

CREATE UNIQUE INDEX IF NOT EXISTS idx_sh_source_dedupe_unique
  ON source_happenings (source_id, dedupe_key);

CREATE UNIQUE INDEX IF NOT EXISTS idx_sc_source_dedupe_unique
  ON source_courses (source_id, dedupe_key);


-- ============================================================
-- STEP 4: Drop old non-unique dedupe index (replaced by unique)
-- ============================================================
-- Migration 001 created a partial non-unique index. The new unique
-- index on (source_id, dedupe_key) subsumes its use cases.

DROP INDEX IF EXISTS idx_source_happenings_dedupe;


-- ============================================================
-- DOWN (rollback) — run separately if you need to undo
-- ============================================================
-- DROP INDEX IF EXISTS idx_sh_source_dedupe_unique;
-- DROP INDEX IF EXISTS idx_sc_source_dedupe_unique;
-- ALTER TABLE source_happenings ALTER COLUMN dedupe_key DROP NOT NULL;
-- ALTER TABLE source_courses ALTER COLUMN dedupe_key DROP NOT NULL;
-- CREATE INDEX IF NOT EXISTS idx_source_happenings_dedupe
--   ON source_happenings (dedupe_key) WHERE dedupe_key IS NOT NULL;
