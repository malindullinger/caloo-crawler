-- Migration 007: course_sessions
-- Specific dated instances of a course
-- Optional - courses may not have explicit sessions

CREATE TABLE IF NOT EXISTS course_sessions (
  id UUID PRIMARY KEY DEFAULT gen_random_uuid(),

  -- Parent course
  course_id UUID NOT NULL REFERENCES courses(id) ON DELETE CASCADE,

  -- Session ordering
  session_index INT,  -- 1, 2, 3... for ordered sessions

  -- Session timing
  start_at TIMESTAMPTZ,
  end_at TIMESTAMPTZ,

  -- Time contract (CRITICAL - same as happenings)
  date_precision TEXT DEFAULT 'datetime'
    CHECK (date_precision IN ('datetime', 'date')),
  -- When date_precision = 'date', time fields may be NULL or date boundaries
  -- Never use 00:00 as placeholder for unknown time

  -- Location (can override course default)
  venue_id UUID,
  location_name TEXT,

  -- Status
  status TEXT DEFAULT 'scheduled'
    CHECK (status IN ('scheduled', 'cancelled', 'completed')),
  notes TEXT,

  -- Timestamps
  created_at TIMESTAMPTZ DEFAULT now(),
  updated_at TIMESTAMPTZ DEFAULT now()
);

-- Performance indexes
CREATE INDEX IF NOT EXISTS idx_course_sessions_course
  ON course_sessions (course_id);
CREATE INDEX IF NOT EXISTS idx_course_sessions_start
  ON course_sessions (start_at)
  WHERE start_at IS NOT NULL;
CREATE INDEX IF NOT EXISTS idx_course_sessions_status
  ON course_sessions (status);
CREATE INDEX IF NOT EXISTS idx_course_sessions_index
  ON course_sessions (course_id, session_index)
  WHERE session_index IS NOT NULL;

-- Comments
COMMENT ON TABLE course_sessions IS 'Specific dated instances of a course - optional, courses may not have explicit sessions';
COMMENT ON COLUMN course_sessions.date_precision IS 'datetime = full time known, date = only date known (follows same no-00:00 contract as happenings)';
COMMENT ON COLUMN course_sessions.session_index IS 'Ordering for multi-session courses: 1, 2, 3, etc.';
