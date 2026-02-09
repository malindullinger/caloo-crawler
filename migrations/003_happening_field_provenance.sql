-- Migration 003: happening_field_provenance
-- Field-level audit trail: tracks which source provided each field value
-- Enables conflict resolution and auditability per PRD

CREATE TABLE IF NOT EXISTS happening_field_provenance (
  id UUID PRIMARY KEY DEFAULT gen_random_uuid(),

  -- Links
  happening_id UUID NOT NULL REFERENCES happening(id) ON DELETE CASCADE,
  source_happening_id UUID REFERENCES source_happenings(id) ON DELETE SET NULL,

  -- Field tracking
  field_name TEXT NOT NULL,           -- title, start_at, location_name, description, etc.
  value_hash TEXT,                    -- hash of current value for change detection
  precedence_rank INT NOT NULL DEFAULT 0,

  -- Timestamps
  updated_at TIMESTAMPTZ DEFAULT now()
);

-- One provenance record per field per happening
CREATE UNIQUE INDEX IF NOT EXISTS idx_field_provenance_unique
  ON happening_field_provenance (happening_id, field_name);

-- Performance indexes
CREATE INDEX IF NOT EXISTS idx_field_provenance_happening
  ON happening_field_provenance (happening_id);
CREATE INDEX IF NOT EXISTS idx_field_provenance_source
  ON happening_field_provenance (source_happening_id)
  WHERE source_happening_id IS NOT NULL;

-- Comments
COMMENT ON TABLE happening_field_provenance IS 'Field-level provenance: tracks which source provided each field value';
COMMENT ON COLUMN happening_field_provenance.field_name IS 'Field name on happening table: title, start_at, end_at, location_name, description, etc.';
COMMENT ON COLUMN happening_field_provenance.value_hash IS 'Hash of current value for detecting changes';
COMMENT ON COLUMN happening_field_provenance.precedence_rank IS 'Priority rank of the source that provided this value';
