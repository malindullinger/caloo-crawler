-- Migration 008: invariants_time_contract
-- DB-level enforcement of the unknown-time contract
--
-- PREREQUISITE: Migrations 001-007 must be applied first
--
-- Contract:
--   date_precision = 'date'     => start_at IS NULL AND end_at IS NULL
--   date_precision = 'datetime' => start_at IS NOT NULL
--
-- This prevents:
--   - Storing 00:00 placeholder times when only date is known
--   - Storing datetime precision without an actual start time

-- ============================================================
-- source_happenings: time contract constraint
-- ============================================================

ALTER TABLE source_happenings
DROP CONSTRAINT IF EXISTS source_happenings_time_contract;

ALTER TABLE source_happenings
ADD CONSTRAINT source_happenings_time_contract CHECK (
  (date_precision = 'date' AND start_at IS NULL AND end_at IS NULL)
  OR
  (date_precision = 'datetime' AND start_at IS NOT NULL)
);

COMMENT ON CONSTRAINT source_happenings_time_contract ON source_happenings IS
  'Enforces unknown-time contract: date => no times, datetime => start required';

-- ============================================================
-- course_sessions: time contract constraint
-- ============================================================

ALTER TABLE course_sessions
DROP CONSTRAINT IF EXISTS course_sessions_time_contract;

ALTER TABLE course_sessions
ADD CONSTRAINT course_sessions_time_contract CHECK (
  (date_precision = 'date' AND start_at IS NULL AND end_at IS NULL)
  OR
  (date_precision = 'datetime' AND start_at IS NOT NULL)
);

COMMENT ON CONSTRAINT course_sessions_time_contract ON course_sessions IS
  'Enforces unknown-time contract: date => no times, datetime => start required';
