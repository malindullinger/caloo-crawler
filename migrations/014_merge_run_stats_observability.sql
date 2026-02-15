-- Migration 014: Observability expansion for merge_run_stats
--
-- Rationale:
--   Phase 7 makes each merge run fully explainable by adding:
--     1. canonical_updates_count — count of canonical field updates performed
--     2. history_rows_created — count of canonical_field_history rows inserted
--     3. source_breakdown — JSONB per-source breakdown of outcomes
--     4. stage_timings_ms — JSONB timing data for key stages
--   Also adds a DB-side RPC function for safe, deterministic counting
--   of actual history inserts (INSERT ON CONFLICT DO NOTHING with ROW_COUNT).
--
-- Contract:
--   1. New columns have safe defaults (0 for ints, NULL for JSONB)
--   2. insert_field_history_batch() returns actual insert count, not attempts
--   3. change_key uniqueness prevents duplicate history rows on re-runs
--   4. Existing merge_run_stats rows remain valid (additive change)

-- Part A: Expand merge_run_stats with new observability columns
ALTER TABLE merge_run_stats
  ADD COLUMN IF NOT EXISTS canonical_updates_count INT DEFAULT 0,
  ADD COLUMN IF NOT EXISTS history_rows_created INT DEFAULT 0,
  ADD COLUMN IF NOT EXISTS source_breakdown JSONB,
  ADD COLUMN IF NOT EXISTS stage_timings_ms JSONB;

COMMENT ON COLUMN merge_run_stats.canonical_updates_count IS 'Count of canonical happening field updates performed this run';
COMMENT ON COLUMN merge_run_stats.history_rows_created IS 'Count of canonical_field_history rows actually inserted (not attempts)';
COMMENT ON COLUMN merge_run_stats.source_breakdown IS 'Per-source JSON: {source_id: {created, merged, review, field_updates, errors}}';
COMMENT ON COLUMN merge_run_stats.stage_timings_ms IS 'Timing data in milliseconds: {total_processing_ms, ...}';

-- Part B: RPC function for safe batch insert of field history
-- Returns actual insert count via GET DIAGNOSTICS (not attempted count).
-- ON CONFLICT (change_key) DO NOTHING guarantees idempotency.
CREATE OR REPLACE FUNCTION public.insert_field_history_batch(changes JSONB)
RETURNS INT
LANGUAGE plpgsql
AS $$
DECLARE
  actual_inserts INT;
BEGIN
  INSERT INTO canonical_field_history
    (happening_id, source_happening_id, field_name, old_value, new_value, change_key)
  SELECT
    (c->>'happening_id')::UUID,
    NULLIF(c->>'source_happening_id', '')::UUID,
    c->>'field_name',
    c->>'old_value',
    c->>'new_value',
    c->>'change_key'
  FROM jsonb_array_elements(changes) AS c
  ON CONFLICT (change_key) DO NOTHING;

  GET DIAGNOSTICS actual_inserts = ROW_COUNT;
  RETURN actual_inserts;
END;
$$;

COMMENT ON FUNCTION public.insert_field_history_batch(JSONB) IS 'Batch-insert canonical_field_history rows. Returns count of rows actually inserted (ON CONFLICT DO NOTHING).';

-- Rollback:
-- ALTER TABLE merge_run_stats
--   DROP COLUMN IF EXISTS canonical_updates_count,
--   DROP COLUMN IF EXISTS history_rows_created,
--   DROP COLUMN IF EXISTS source_breakdown,
--   DROP COLUMN IF EXISTS stage_timings_ms;
-- DROP FUNCTION IF EXISTS public.insert_field_history_batch(JSONB);

-- Verification:
-- 1. New columns exist:
--    SELECT canonical_updates_count, history_rows_created, source_breakdown, stage_timings_ms
--    FROM merge_run_stats LIMIT 1;
--
-- 2. RPC function works (empty batch):
--    SELECT public.insert_field_history_batch('[]'::JSONB);
--    -- Returns 0
--
-- 3. RPC idempotency (duplicate change_key):
--    -- First call inserts, second call returns 0 (ON CONFLICT DO NOTHING)
