-- Migration 004: organizer_fields
-- Add missing organizer columns per PRD
-- Additive only - does not modify existing columns
-- Table name confirmed: organizer (singular)

DO $$
BEGIN
  -- Add legal_form if not exists
  -- Swiss legal forms: AG, GmbH, Verein, Stiftung, Genossenschaft, Einzelunternehmen, public, other
  IF NOT EXISTS (
    SELECT 1 FROM information_schema.columns
    WHERE table_schema = 'public'
      AND table_name = 'organizer'
      AND column_name = 'legal_form'
  ) THEN
    ALTER TABLE organizer ADD COLUMN legal_form TEXT;
    COMMENT ON COLUMN organizer.legal_form IS 'Swiss legal form: AG, GmbH, Verein, Stiftung, Genossenschaft, Einzelunternehmen, public, other';
  END IF;

  -- Add priority_score if not exists
  IF NOT EXISTS (
    SELECT 1 FROM information_schema.columns
    WHERE table_schema = 'public'
      AND table_name = 'organizer'
      AND column_name = 'priority_score'
  ) THEN
    ALTER TABLE organizer ADD COLUMN priority_score INT DEFAULT 0;
    COMMENT ON COLUMN organizer.priority_score IS 'Feed ordering priority: higher = more prominent in results';
  END IF;

  -- Add locality if not exists
  IF NOT EXISTS (
    SELECT 1 FROM information_schema.columns
    WHERE table_schema = 'public'
      AND table_name = 'organizer'
      AND column_name = 'locality'
  ) THEN
    ALTER TABLE organizer ADD COLUMN locality TEXT;
    COMMENT ON COLUMN organizer.locality IS 'Geographic region (e.g., Männedorf, Zürichsee, Zürich)';
  END IF;
END $$;

-- Indexes for new columns (idempotent)
CREATE INDEX IF NOT EXISTS idx_organizer_priority
  ON organizer (priority_score DESC NULLS LAST);
CREATE INDEX IF NOT EXISTS idx_organizer_locality
  ON organizer (locality)
  WHERE locality IS NOT NULL;
CREATE INDEX IF NOT EXISTS idx_organizer_legal_form
  ON organizer (legal_form)
  WHERE legal_form IS NOT NULL;
