-- Migration 011: Drop legacy partial unique index on (source_id, external_id)
--
-- Rationale:
--   dedupe_key is now the canonical uniqueness contract.
--   All upserts use on_conflict="source_id,dedupe_key".
--   Keeping both unique constraints causes 23505 during migration.
--
-- The UNIQUE(source_id, dedupe_key) index remains as the single conflict target.

DROP INDEX IF EXISTS idx_source_happenings_source_external_unique_partial;
DROP INDEX IF EXISTS idx_source_happenings_external_unique;
