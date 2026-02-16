-- 020_next_weekend_with_events.sql
-- SQL function: find the next weekend (Fri 00:01 → Mon 00:00 Europe/Zurich)
-- that has at least one eligible event.
--
-- Uses the SAME eligibility rules as feed_cards_view:
--   - happening.visibility_status = 'published'
--   - occurrence.status = 'scheduled'
--   - occurrence.start_at IS NOT NULL
--   - end_at is NULL or >= start_at (valid range)
--   - COALESCE(end_at, start_at) >= after_ts (not in the past relative to query time)
--   - occurrence.start_at falls within the weekend window (Zurich local)
--
-- Does NOT replace or duplicate feed_cards_view.
-- Frontend still reads ONLY feed_cards_view for the feed.
-- This is a lightweight lookup helper for "show next weekend with events" UX.
--
-- Safe to run multiple times (CREATE OR REPLACE).

-- ============================================================
-- UP
-- ============================================================

CREATE OR REPLACE FUNCTION public.next_weekend_with_events(
  after_ts  timestamptz DEFAULT now(),
  max_weeks_ahead int     DEFAULT 26
)
RETURNS TABLE (
  weekend_start_zh timestamptz,
  weekend_end_zh   timestamptz,
  event_count      int
)
LANGUAGE plpgsql
STABLE
AS $$
DECLARE
  _local_ts    timestamp;    -- after_ts in Zurich local time
  _week_monday timestamp;    -- Monday 00:00 of current ISO week (local)
  _ws_local    timestamp;    -- weekend start (Zurich local, no tz)
  _we_local    timestamp;    -- weekend end   (Zurich local, no tz)
  _ws_tz       timestamptz;  -- weekend start (UTC-aware)
  _we_tz       timestamptz;  -- weekend end   (UTC-aware)
  _first_ws_tz timestamptz;  -- saved first candidate for fallback
  _first_we_tz timestamptz;
  _count       int;
  _i           int;
BEGIN
  -- 1. Convert to Zurich local time and find ISO week start (Monday)
  _local_ts    := after_ts AT TIME ZONE 'Europe/Zurich';
  _week_monday := date_trunc('week', _local_ts);

  -- 2. Compute weekend window for this week
  --    Matches feed_cards_view params CTE exactly:
  --      weekend_start = Monday + 4d + 00:01 = Friday 00:01
  --      weekend_end   = Monday + 7d         = next Monday 00:00
  _ws_local := _week_monday + interval '4 days' + interval '00:01';
  _we_local := _week_monday + interval '7 days';
  _ws_tz    := _ws_local AT TIME ZONE 'Europe/Zurich';
  _we_tz    := _we_local AT TIME ZONE 'Europe/Zurich';

  -- 3. If after_ts is already past this weekend's end, start from next week
  IF after_ts >= _we_tz THEN
    _week_monday := _week_monday + interval '7 days';
    _ws_local    := _week_monday + interval '4 days' + interval '00:01';
    _we_local    := _week_monday + interval '7 days';
    _ws_tz       := _ws_local AT TIME ZONE 'Europe/Zurich';
    _we_tz       := _we_local AT TIME ZONE 'Europe/Zurich';
  END IF;

  -- Save the first candidate weekend for the no-events fallback
  _first_ws_tz := _ws_tz;
  _first_we_tz := _we_tz;

  -- 4. Search forward week by week
  FOR _i IN 0..max_weeks_ahead - 1 LOOP

    -- Count eligible events in this weekend window.
    -- WHERE clause matches feed_cards_view base CTE, except the
    -- "not in the past" check uses after_ts instead of now() so the
    -- function answers relative to the supplied query time.
    SELECT count(*)::int INTO _count
    FROM occurrence o
      JOIN offering off ON off.id = o.offering_id
      JOIN happening h  ON h.id  = off.happening_id
    WHERE h.visibility_status = 'published'
      AND o.status            = 'scheduled'
      AND o.start_at IS NOT NULL
      AND (o.end_at IS NULL OR o.end_at >= o.start_at)
      AND COALESCE(o.end_at, o.start_at) >= after_ts
      -- Weekend window check (same as feed_cards_view computed CTE):
      AND (o.start_at AT TIME ZONE 'Europe/Zurich') >= _ws_local
      AND (o.start_at AT TIME ZONE 'Europe/Zurich') <  _we_local;

    IF _count > 0 THEN
      weekend_start_zh := _ws_tz;
      weekend_end_zh   := _we_tz;
      event_count      := _count;
      RETURN NEXT;
      RETURN;
    END IF;

    -- Advance to next week
    _week_monday := _week_monday + interval '7 days';
    _ws_local    := _week_monday + interval '4 days' + interval '00:01';
    _we_local    := _week_monday + interval '7 days';
    _ws_tz       := _ws_local AT TIME ZONE 'Europe/Zurich';
    _we_tz       := _we_local AT TIME ZONE 'Europe/Zurich';
  END LOOP;

  -- 5. No events found in the entire horizon — return first candidate with 0
  weekend_start_zh := _first_ws_tz;
  weekend_end_zh   := _first_we_tz;
  event_count      := 0;
  RETURN NEXT;
  RETURN;
END;
$$;

COMMENT ON FUNCTION public.next_weekend_with_events IS
  'Find the next weekend (Fri 00:01 – Mon 00:00 Europe/Zurich) with eligible events. '
  'Eligibility matches feed_cards_view, except "not in the past" uses after_ts (not now()). '
  'When called with default after_ts=now(), behavior is identical to feed. '
  'Returns one row: weekend range + event count (0 if none found in horizon).';

-- ============================================================
-- DOWN (rollback) — run separately if you need to undo
-- ============================================================
-- DROP FUNCTION IF EXISTS public.next_weekend_with_events;
