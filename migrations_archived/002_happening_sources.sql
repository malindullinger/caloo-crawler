-- Migration 002: happening_sources
-- Provenance tracking: links canonical happenings to contributing source records
-- Supports multi-source canonicalization per PRD merge strategy

CREATE TABLE IF NOT EXISTS happening_sources (
  id UUID PRIMARY KEY DEFAULT gen_random_uuid(),

  -- Links
  happening_id UUID NOT NULL REFERENCES happening(id) ON DELETE CASCADE,
  source_happening_id UUID NOT NULL REFERENCES source_happenings(id) ON DELETE CASCADE,

  -- Merge metadata
  source_priority INT NOT NULL DEFAULT 0,  -- higher = more trusted (partner_feed > internal_manual > Tier A > Tier B)
  is_primary BOOLEAN DEFAULT false,        -- which source "owns" this happening

  -- Timestamps
  merged_at TIMESTAMPTZ DEFAULT now()
);

-- Unique constraint: one link per (happening, source) pair
CREATE UNIQUE INDEX IF NOT EXISTS idx_happening_sources_unique
  ON happening_sources (happening_id, source_happening_id);

-- Performance indexes
CREATE INDEX IF NOT EXISTS idx_happening_sources_happening
  ON happening_sources (happening_id);
CREATE INDEX IF NOT EXISTS idx_happening_sources_source
  ON happening_sources (source_happening_id);
CREATE INDEX IF NOT EXISTS idx_happening_sources_primary
  ON happening_sources (happening_id)
  WHERE is_primary = true;

-- Comments
COMMENT ON TABLE happening_sources IS 'Provenance: tracks which source records contributed to each canonical happening';
COMMENT ON COLUMN happening_sources.source_priority IS 'Higher = more trusted. partner_feed(100) > internal_manual(50) > Tier A(20) > Tier B(10)';
COMMENT ON COLUMN happening_sources.is_primary IS 'True for the source that "owns" this happening (highest priority at merge time)';
