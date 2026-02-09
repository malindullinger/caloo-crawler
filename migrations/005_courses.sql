-- Migration 005: courses
-- Canonical course records (separate from happenings per PRD)
-- Shown in Courses tab, NOT in Happenings tab

CREATE TABLE IF NOT EXISTS courses (
  id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  public_id TEXT UNIQUE,

  -- Core fields
  title TEXT NOT NULL,
  description TEXT,

  -- Organizer
  organizer_id UUID REFERENCES organizer(id),

  -- Audience
  audience_type TEXT,
  age_min INT,
  age_max INT,

  -- Default location (can be overridden per session)
  venue_id UUID,
  location_name TEXT,

  -- Schedule fields (for courses without explicit sessions)
  start_date DATE,                            -- Course start date
  end_date DATE,                              -- Course end date
  timezone TEXT DEFAULT 'Europe/Zurich',

  -- Status
  visibility_status TEXT DEFAULT 'draft'
    CHECK (visibility_status IN ('draft', 'published', 'archived')),

  -- Timestamps
  created_at TIMESTAMPTZ DEFAULT now(),
  updated_at TIMESTAMPTZ DEFAULT now()
);

-- Performance indexes
CREATE INDEX IF NOT EXISTS idx_courses_organizer
  ON courses (organizer_id)
  WHERE organizer_id IS NOT NULL;
CREATE INDEX IF NOT EXISTS idx_courses_visibility
  ON courses (visibility_status);
CREATE INDEX IF NOT EXISTS idx_courses_public_id
  ON courses (public_id)
  WHERE public_id IS NOT NULL;
CREATE INDEX IF NOT EXISTS idx_courses_start_date
  ON courses (start_date)
  WHERE start_date IS NOT NULL;

-- Comments
COMMENT ON TABLE courses IS 'Canonical course records - shown in Courses tab, NOT in Happenings tab';
COMMENT ON COLUMN courses.start_date IS 'Course start date (for courses without explicit sessions)';
COMMENT ON COLUMN courses.end_date IS 'Course end date (for courses without explicit sessions)';
