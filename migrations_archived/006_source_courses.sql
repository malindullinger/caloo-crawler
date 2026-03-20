-- Migration 006: source_courses
-- Raw course records from sources before canonicalization
-- Mirrors source_happenings structure for courses

CREATE TABLE IF NOT EXISTS source_courses (
  id UUID PRIMARY KEY DEFAULT gen_random_uuid(),

  -- Source identification
  source_id TEXT NOT NULL,
  source_type TEXT NOT NULL DEFAULT 'crawler'
    CHECK (source_type IN ('crawler', 'internal_manual', 'partner_feed')),
  source_tier TEXT NOT NULL DEFAULT 'A'
    CHECK (source_tier IN ('A', 'B', 'C')),
  external_id TEXT,  -- NULL allowed for internal_manual records

  -- Raw fields
  title_raw TEXT,
  description_raw TEXT,
  schedule_raw TEXT,
  location_raw TEXT,

  -- Extraction metadata
  extraction_method TEXT
    CHECK (extraction_method IS NULL OR extraction_method IN ('jsonld', 'time_element', 'text_heuristic')),
  item_url TEXT,
  content_hash TEXT,

  -- Dedupe helpers
  dedupe_key TEXT,

  -- Status
  status TEXT DEFAULT 'pending',
  error_message TEXT,

  -- Timestamps
  fetched_at TIMESTAMPTZ,
  created_at TIMESTAMPTZ DEFAULT now(),
  updated_at TIMESTAMPTZ DEFAULT now()
);

-- Partial unique index for records with external_id
CREATE UNIQUE INDEX IF NOT EXISTS idx_source_courses_external_unique
  ON source_courses (source_id, external_id)
  WHERE external_id IS NOT NULL;

-- Unique index for manual records
CREATE UNIQUE INDEX IF NOT EXISTS idx_source_courses_manual_unique
  ON source_courses (source_id, content_hash)
  WHERE source_type = 'internal_manual' AND content_hash IS NOT NULL;

-- Performance indexes
CREATE INDEX IF NOT EXISTS idx_source_courses_source_id
  ON source_courses (source_id);
CREATE INDEX IF NOT EXISTS idx_source_courses_status
  ON source_courses (status);
CREATE INDEX IF NOT EXISTS idx_source_courses_content_hash
  ON source_courses (content_hash)
  WHERE content_hash IS NOT NULL;

-- Comments
COMMENT ON TABLE source_courses IS 'Raw course records from sources before canonicalization';
