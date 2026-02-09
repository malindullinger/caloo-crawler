-- Migration 010: ambiguous_match_log
-- Purpose:
--   Persist low-confidence dedupe/match decisions for manual review.
-- Rules:
--   - Only created when match score is below threshold
--   - resolution NULL means unresolved

CREATE TABLE IF NOT EXISTS ambiguous_match_log (
  id UUID PRIMARY KEY DEFAULT gen_random_uuid(),

  source_happening_id UUID NOT NULL REFERENCES source_happenings(id) ON DELETE CASCADE,
  candidate_happening_id UUID NOT NULL REFERENCES happening(id) ON DELETE CASCADE,

  confidence_score NUMERIC(4,3) NOT NULL,

  source_title TEXT,
  candidate_title TEXT,

  resolution TEXT CHECK (
    resolution IS NULL OR resolution IN ('merged', 'kept_separate', 'dismissed')
  ),

  resolved_at TIMESTAMPTZ,
  resolved_by TEXT,

  created_at TIMESTAMPTZ DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_ambiguous_unresolved
  ON ambiguous_match_log (created_at DESC)
  WHERE resolution IS NULL;

CREATE INDEX IF NOT EXISTS idx_ambiguous_source
  ON ambiguous_match_log (source_happening_id);

CREATE INDEX IF NOT EXISTS idx_ambiguous_candidate
  ON ambiguous_match_log (candidate_happening_id);
