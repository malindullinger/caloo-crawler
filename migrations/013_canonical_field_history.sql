-- Migration 013: Canonical field history — historical change log
--
-- Rationale:
--   Phase 6 adds canonical field updates on the merge path. When a source
--   row merges into an existing happening and a tracked field value differs,
--   the happening is updated and the old→new transition is recorded here.
--
--   This is distinct from happening_field_provenance (migration 003) which
--   tracks CURRENT state (which source provides each field). This table
--   tracks HISTORICAL transitions (old value → new value).
--
-- Contract:
--   1. change_key = sha256(happening_id|field_name|old_value|new_value)
--      source_happening_id is excluded so the same logical change from
--      any source produces the same key.
--   2. UNIQUE INDEX on change_key → INSERT ON CONFLICT DO NOTHING
--      guarantees idempotent logging (re-running merge produces no duplicates).
--   3. History is logged ONLY on the merge path, never on creation.

CREATE TABLE IF NOT EXISTS canonical_field_history (
  id UUID PRIMARY KEY DEFAULT gen_random_uuid(),

  -- Links
  happening_id UUID NOT NULL REFERENCES happening(id) ON DELETE CASCADE,
  source_happening_id UUID REFERENCES source_happenings(id) ON DELETE SET NULL,

  -- Change tracking
  field_name TEXT NOT NULL,
  old_value TEXT,
  new_value TEXT,
  change_key TEXT NOT NULL,

  -- Timestamp
  changed_at TIMESTAMPTZ DEFAULT now()
);

-- Deterministic idempotency: same logical change → same key → no duplicate
CREATE UNIQUE INDEX IF NOT EXISTS idx_field_history_change_key
  ON canonical_field_history (change_key);

-- Performance indexes
CREATE INDEX IF NOT EXISTS idx_field_history_happening
  ON canonical_field_history (happening_id);

-- Comments
COMMENT ON TABLE canonical_field_history IS 'Historical field-level change log for canonical happenings (Phase 6)';
COMMENT ON COLUMN canonical_field_history.change_key IS 'sha256(happening_id|field_name|old_value|new_value) — deterministic, source-agnostic';
COMMENT ON COLUMN canonical_field_history.field_name IS 'Canonical field name: title, description';

-- Rollback:
-- DROP TABLE IF EXISTS canonical_field_history;

-- Verification:
-- 1. Table exists:
--    SELECT count(*) FROM canonical_field_history;
--
-- 2. Unique index prevents duplicates:
--    INSERT INTO canonical_field_history (happening_id, field_name, old_value, new_value, change_key)
--    VALUES ('...', 'title', 'old', 'new', 'test-key');
--    -- Second insert with same change_key should be silently ignored:
--    INSERT INTO canonical_field_history (happening_id, field_name, old_value, new_value, change_key)
--    VALUES ('...', 'title', 'old', 'new', 'test-key')
--    ON CONFLICT (change_key) DO NOTHING;
