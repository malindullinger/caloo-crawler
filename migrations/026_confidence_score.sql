-- Migration 026: Add confidence_score to happening
--
-- A deterministic data-quality signal computed from source metadata.
-- NOT a feed filter — used for admin review prioritization,
-- ops monitoring, and future source weighting only.
--
-- Range: 0–100 (integer). Default 100 (no penalties).
-- Recomputed on CREATE and MERGE by the merge loop.
-- Can be batch-recomputed via scripts/recompute_confidence_scores.py.

ALTER TABLE happening
  ADD COLUMN IF NOT EXISTS confidence_score INT NOT NULL DEFAULT 100;

COMMENT ON COLUMN happening.confidence_score IS
  'Data-quality signal (0–100). Computed from source metadata. '
  'NOT a feed filter. Used for review prioritization and ops only.';

-- Index for ordering by quality (admin dashboards, review queues)
CREATE INDEX IF NOT EXISTS idx_happening_confidence_score
  ON happening (confidence_score);
