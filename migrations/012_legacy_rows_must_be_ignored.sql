-- Migration 012: Prevent legacy (non-v1) source_happenings from becoming processable
--
-- Rationale:
--   Phase 3 introduced content-based dedupe keys with "v1|" prefix.
--   Legacy rows (URL-based keys, no prefix) are quarantined as status='ignored'.
--   This constraint ensures no code regression can ever requeue them.
--
-- Contract:
--   IF dedupe_key NOT LIKE 'v1|%' THEN status MUST be 'ignored'
--   v1| rows have no status restriction (queued, processing, processed, needs_review, ignored all valid)

ALTER TABLE source_happenings
ADD CONSTRAINT legacy_rows_must_be_ignored
CHECK (
    dedupe_key LIKE 'v1|%' OR status = 'ignored'
);

-- Rollback:
-- ALTER TABLE source_happenings DROP CONSTRAINT legacy_rows_must_be_ignored;

-- Verification:
-- 1. This should FAIL:
--    UPDATE source_happenings SET status = 'queued'
--    WHERE dedupe_key NOT LIKE 'v1|%' LIMIT 1;
--
-- 2. This should SUCCEED:
--    UPDATE source_happenings SET status = 'ignored'
--    WHERE dedupe_key NOT LIKE 'v1|%' LIMIT 1;
--
-- 3. This should SUCCEED (v1| rows are unrestricted):
--    UPDATE source_happenings SET status = 'queued'
--    WHERE dedupe_key LIKE 'v1|%' LIMIT 1;
