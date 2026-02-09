-- Migration 009: start_date_local / end_date_local for source_happenings
-- Purpose:
--   - Provide a DATE field for BOTH datetime and date-only records
--   - Required for offering.start_date/end_date and for deterministic dedupe_key/fingerprints
-- Constraints:
--   - No guessing: if we cannot derive a date, start_date_local remains NULL
--   - Canonicalization must only process rows where status='pending' AND start_date_local IS NOT NULL

ALTER TABLE source_happenings
  ADD COLUMN IF NOT EXISTS start_date_local DATE;

ALTER TABLE source_happenings
  ADD COLUMN IF NOT EXISTS end_date_local DATE;

COMMENT ON COLUMN source_happenings.start_date_local IS
  'Local DATE for the happening (Europe/Zurich). Derived from start_at if datetime precision; otherwise parsed from datetime_raw. Never inferred from title.';

COMMENT ON COLUMN source_happenings.end_date_local IS
  'Local DATE end (Europe/Zurich). Optional. Derived from end_at if present; otherwise parsed from datetime_raw if explicit.';

-- Ensure that canonicalizable records always have a date.
-- NOTE: We allow NULL only for truly broken source rows, which must be moved to needs_review/error and excluded from canonicalization.
DO $$
BEGIN
  IF NOT EXISTS (
    SELECT 1
    FROM pg_constraint
    WHERE conname = 'source_happenings_start_date_local_required'
  ) THEN
    ALTER TABLE source_happenings
      ADD CONSTRAINT source_happenings_start_date_local_required
      CHECK (
        -- For valid time-precision states, start_date_local must exist
        (date_precision IN ('date','datetime') AND start_date_local IS NOT NULL)
        OR
        -- Anything else is allowed only if it is NOT pending (i.e., needs_review/error)
        (date_precision NOT IN ('date','datetime') OR status <> 'pending')
      );
  END IF;
END $$;

COMMENT ON CONSTRAINT source_happenings_start_date_local_required ON source_happenings IS
  'If date_precision is date/datetime and status is pending, start_date_local must be non-null. Records missing a date must not remain pending.';

-- Optional: helper index for canonicalization filters
CREATE INDEX IF NOT EXISTS idx_source_happenings_pending_with_date
  ON source_happenings(status, start_date_local)
  WHERE status = 'pending' AND start_date_local IS NOT NULL;
