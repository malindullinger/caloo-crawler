-- Migration 001: source_happenings
-- Raw happening records from crawlers, manual input, or partner feeds
-- Tracks tier classification and extraction method per PRD

CREATE TABLE IF NOT EXISTS source_happenings (
  id UUID PRIMARY KEY DEFAULT gen_random_uuid(),

  -- Source identification
  source_id TEXT NOT NULL,
  source_type TEXT NOT NULL DEFAULT 'crawler'
    CHECK (source_type IN ('crawler', 'internal_manual', 'partner_feed')),
  source_tier TEXT NOT NULL DEFAULT 'A'
    CHECK (source_tier IN ('A', 'B', 'C')),
  external_id TEXT,  -- NULL allowed for internal_manual records

  -- Raw extracted fields
  title_raw TEXT,
  datetime_raw TEXT,
  location_raw TEXT,
  description_raw TEXT,

  -- Time contract fields (CRITICAL)
  date_precision TEXT DEFAULT 'date'
    CHECK (date_precision IN ('datetime', 'date')),
  start_at TIMESTAMPTZ,
  end_at TIMESTAMPTZ,
  timezone TEXT DEFAULT 'Europe/Zurich',

  -- Extraction metadata
  extraction_method TEXT
    CHECK (extraction_method IS NULL OR extraction_method IN ('jsonld', 'time_element', 'text_heuristic')),
  item_url TEXT,
  content_hash TEXT,

  -- Dedupe helpers
  dedupe_key TEXT,  -- Generated fingerprint: normalized(title + start_date + venue)

  -- Status
  status TEXT DEFAULT 'pending',
  error_message TEXT,

  -- Timestamps
  fetched_at TIMESTAMPTZ,
  created_at TIMESTAMPTZ DEFAULT now(),
  updated_at TIMESTAMPTZ DEFAULT now()
);

-- Partial unique index for records with external_id (crawlers, partners)
CREATE UNIQUE INDEX IF NOT EXISTS idx_source_happenings_external_unique
  ON source_happenings (source_id, external_id)
  WHERE external_id IS NOT NULL;

-- Unique index for manual records (by content_hash)
CREATE UNIQUE INDEX IF NOT EXISTS idx_source_happenings_manual_unique
  ON source_happenings (source_id, content_hash)
  WHERE source_type = 'internal_manual' AND content_hash IS NOT NULL;

-- Performance indexes
CREATE INDEX IF NOT EXISTS idx_source_happenings_source_id
  ON source_happenings (source_id);
CREATE INDEX IF NOT EXISTS idx_source_happenings_status
  ON source_happenings (status);
CREATE INDEX IF NOT EXISTS idx_source_happenings_fetched
  ON source_happenings (source_id, fetched_at DESC);
CREATE INDEX IF NOT EXISTS idx_source_happenings_tier
  ON source_happenings (source_tier);
CREATE INDEX IF NOT EXISTS idx_source_happenings_content_hash
  ON source_happenings (content_hash)
  WHERE content_hash IS NOT NULL;
CREATE INDEX IF NOT EXISTS idx_source_happenings_item_url
  ON source_happenings (item_url)
  WHERE item_url IS NOT NULL;
CREATE INDEX IF NOT EXISTS idx_source_happenings_dedupe
  ON source_happenings (dedupe_key)
  WHERE dedupe_key IS NOT NULL;

-- Comment
COMMENT ON TABLE source_happenings IS 'Raw happening records from sources before canonicalization';
COMMENT ON COLUMN source_happenings.date_precision IS 'datetime = full time known, date = only date known (never use 00:00 placeholder)';
COMMENT ON COLUMN source_happenings.dedupe_key IS 'Generated fingerprint for matching: normalized(title + start_date + venue)';
