# Feed Health Verification Pack

## Purpose

This document provides a lightweight, read-only verification pack to validate `feed_cards_view` behavior. It supports the CLAUDE.md work method: "Verify with checklist (SQL + UI)".

**This is documentation only.** All queries are SELECT statements against the existing view. Nothing is changed.

---

## Verification Checklist

- [ ] **Feed is queryable** — `feed_cards_view` returns rows without error
- [ ] **Section keys are valid** — only `weekend` and `coming_up` values exist
- [ ] **Weekend window is correct (DOW)** — `weekend` rows fall on Thu/Fri/Sat/Sun
- [ ] **Weekend window is correct (date range)** — `weekend` rows fall within computed Thursday→Sunday window
- [ ] **Unknown time handling** — rows with `date_precision = 'date'` have NULL `start_time_local` and `end_time_local`
- [ ] **No midnight placeholders** — no `00:00` times when `date_precision = 'date'`
- [ ] **Ongoing logic is strict** — `is_happening_now` rows have both `start_at` and `end_at`, duration ≤ 12h, and `now()` is between them
- [ ] **No orphaned ongoing** — rows marked `is_happening_now = true` are actually happening now
- [ ] **Dedupe inputs are present** — key fields for frontend dedupe (`title`, `location_name`, `start_at`, `series_label`) are populated where expected
- [ ] **Public access works** — query succeeds without auth (manual check: incognito / anon key)

---

## SQL Queries

### Q1: Basic feed health (row count + section distribution)

```sql
SELECT
  count(*) AS total_rows,
  count(*) FILTER (WHERE section_key = 'weekend') AS weekend_rows,
  count(*) FILTER (WHERE section_key = 'coming_up') AS coming_up_rows,
  count(*) FILTER (WHERE section_key NOT IN ('weekend', 'coming_up')) AS invalid_section_rows
FROM public.feed_cards_view;
```

**Expected:** `invalid_section_rows = 0`

---

### Q2: Section key validation

```sql
SELECT DISTINCT section_key
FROM public.feed_cards_view;
```

**Expected:** Only `weekend` and `coming_up`

---

### Q3a: Weekend window correctness (day of week)

Weekend rows should only fall on Thursday (4), Friday (5), Saturday (6), or Sunday (0).

```sql
SELECT
  external_id,
  title,
  section_key,
  start_at AT TIME ZONE 'Europe/Zurich' AS start_local,
  EXTRACT(DOW FROM start_at AT TIME ZONE 'Europe/Zurich') AS day_of_week
FROM public.feed_cards_view
WHERE section_key = 'weekend'
  AND EXTRACT(DOW FROM start_at AT TIME ZONE 'Europe/Zurich') NOT IN (0, 4, 5, 6)
LIMIT 10;
```

**Expected:** 0 rows

---

### Q3b: Weekend window correctness (computed date range)

Weekend window is Thursday through Sunday of the current week, computed from `date_trunc('week', ...)`.

```sql
WITH weekend_window AS (
  SELECT
    date_trunc('week', now() AT TIME ZONE 'Europe/Zurich')::date + 3 AS thursday,
    date_trunc('week', now() AT TIME ZONE 'Europe/Zurich')::date + 6 AS sunday
)
SELECT
  f.external_id,
  f.title,
  f.section_key,
  (f.start_at AT TIME ZONE 'Europe/Zurich')::date AS start_date,
  w.thursday,
  w.sunday
FROM public.feed_cards_view f
CROSS JOIN weekend_window w
WHERE f.section_key = 'weekend'
  AND (
    (f.start_at AT TIME ZONE 'Europe/Zurich')::date < w.thursday
    OR (f.start_at AT TIME ZONE 'Europe/Zurich')::date > w.sunday
  )
LIMIT 10;
```

**Expected:** 0 rows (all weekend rows fall within Thursday–Sunday of current week)

**Note:** If the feed includes next week's weekend, extend this check to include the next week's Thursday–Sunday window as well. This query validates the current-week window only.

---

### Q4: Unknown time handling — date_precision = 'date' must have NULL times

```sql
SELECT
  external_id,
  title,
  date_precision,
  start_time_local,
  end_time_local
FROM public.feed_cards_view
WHERE date_precision = 'date'
  AND (start_time_local IS NOT NULL OR end_time_local IS NOT NULL)
LIMIT 10;
```

**Expected:** 0 rows

---

### Q5: No midnight placeholders when time is unknown

```sql
SELECT
  external_id,
  title,
  date_precision,
  start_time_local,
  end_time_local
FROM public.feed_cards_view
WHERE date_precision = 'date'
  AND (start_time_local LIKE '00:00%' OR end_time_local LIKE '00:00%')
LIMIT 10;
```

**Expected:** 0 rows

---

### Q6: Ongoing logic validation

`is_happening_now` should only be true if: `start_at` AND `end_at` exist, duration ≤ 12h, and `now()` is between them.

```sql
SELECT
  external_id,
  title,
  is_happening_now,
  start_at,
  end_at,
  EXTRACT(EPOCH FROM (end_at - start_at)) / 3600 AS duration_hours
FROM public.feed_cards_view
WHERE is_happening_now = true
  AND (
    start_at IS NULL
    OR end_at IS NULL
    OR EXTRACT(EPOCH FROM (end_at - start_at)) / 3600 > 12
    OR now() < start_at
    OR now() > end_at
  )
LIMIT 10;
```

**Expected:** 0 rows

---

### Q7: Ongoing sanity check — all ongoing rows are actually happening now

```sql
SELECT
  external_id,
  title,
  is_happening_now,
  start_at,
  end_at,
  now() AS current_time
FROM public.feed_cards_view
WHERE is_happening_now = true
ORDER BY start_at
LIMIT 10;
```

**Expected:** All rows have `now()` between `start_at` and `end_at`

---

### Q8: Dedupe input coverage — check key fields are present

```sql
SELECT
  count(*) AS total,
  count(*) FILTER (WHERE title IS NULL OR title = '') AS missing_title,
  count(*) FILTER (WHERE start_at IS NULL) AS missing_start_at,
  count(*) FILTER (WHERE series_label IS NOT NULL) AS has_series_label
FROM public.feed_cards_view;
```

**Expected:** `missing_title = 0`, `missing_start_at = 0`. `series_label` may be NULL for non-series items.

---

### Q9: Potential duplicate detection (same title + location + date)

```sql
SELECT
  title,
  location_name,
  (start_at AT TIME ZONE 'Europe/Zurich')::date AS event_date,
  count(*) AS occurrences
FROM public.feed_cards_view
WHERE series_label IS NULL
GROUP BY title, location_name, (start_at AT TIME ZONE 'Europe/Zurich')::date
HAVING count(*) > 1
ORDER BY count(*) DESC
LIMIT 10;
```

**Expected:** Review manually — these may be legitimate (different times) or frontend dedupe candidates

---

## Manual Verification Steps

### Public access

Verify that `feed_cards_view` is queryable without authentication:

1. Open the app in incognito mode
2. Confirm the feed loads correctly
3. Or: query Supabase with anon key and verify rows are returned

This cannot be verified by SQL alone — it requires testing the public access contract.

---

## What This Does NOT Change

| Area | Status |
|------|--------|
| `feed_cards_view` schema | No changes |
| `feed_cards_view` SQL logic | No changes |
| Frontend dedupe logic | No changes |
| Any user-facing behavior | No changes |
| Any data in the database | No changes (read-only queries) |
| New views or tables | None created |

---

## When to Run These Checks

- After modifying `feed_cards_view`
- After crawler changes that affect data shape
- Before releases that touch feed logic
- When debugging unexpected frontend behavior
