-- Migration 016: Confidence telemetry + review outcome audit trail
--
-- Phase 9 — observability + analytics only. No merge behavior changes.

-- ---------------------------------------------------------------------------
-- A) Expand merge_run_stats with confidence telemetry columns
-- ---------------------------------------------------------------------------

ALTER TABLE merge_run_stats
  ADD COLUMN IF NOT EXISTS confidence_min DOUBLE PRECISION,
  ADD COLUMN IF NOT EXISTS confidence_avg DOUBLE PRECISION,
  ADD COLUMN IF NOT EXISTS confidence_max DOUBLE PRECISION,
  ADD COLUMN IF NOT EXISTS confidence_histogram JSONB,
  ADD COLUMN IF NOT EXISTS source_confidence JSONB;

COMMENT ON COLUMN merge_run_stats.confidence_min IS
  'Minimum confidence score across all scored rows in this run (NULL if no rows scored).';
COMMENT ON COLUMN merge_run_stats.confidence_avg IS
  'Mean confidence score across all scored rows in this run (NULL if no rows scored).';
COMMENT ON COLUMN merge_run_stats.confidence_max IS
  'Maximum confidence score across all scored rows in this run (NULL if no rows scored).';
COMMENT ON COLUMN merge_run_stats.confidence_histogram IS
  'Fixed-bucket histogram of confidence scores: {"0_50":N, "50_70":N, "70_85":N, "85_95":N, "95_99":N, "99_100":N}.';
COMMENT ON COLUMN merge_run_stats.source_confidence IS
  'Per-source confidence telemetry: {"source_id": {"min":F, "avg":F, "max":F, "hist":{...}}}.';

-- ---------------------------------------------------------------------------
-- B) Create canonical_review_outcomes table
--    Audit trail for human review resolutions (feedback loop).
--    Analytics only — does not affect merge behavior.
-- ---------------------------------------------------------------------------

CREATE TABLE IF NOT EXISTS canonical_review_outcomes (
  id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  review_id UUID NOT NULL,
  source_happening_id UUID NOT NULL,
  happening_id UUID,
  decision TEXT NOT NULL CHECK (decision IN ('merge', 'create', 'ignore')),
  selected_candidate_happening_id UUID,
  confidence_score DOUBLE PRECISION,
  confidence_breakdown JSONB,
  resolved_by TEXT,
  resolved_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- Unique on review_id for idempotent upsert
CREATE UNIQUE INDEX IF NOT EXISTS idx_review_outcomes_review_id
  ON canonical_review_outcomes (review_id);

-- Lookup by source happening
CREATE INDEX IF NOT EXISTS idx_review_outcomes_source
  ON canonical_review_outcomes (source_happening_id);

-- Recency ordering for dashboards
CREATE INDEX IF NOT EXISTS idx_review_outcomes_resolved
  ON canonical_review_outcomes (resolved_at DESC);

COMMENT ON TABLE canonical_review_outcomes IS
  'Audit trail of human review resolutions. Analytics only — does not affect merge behavior.';
