# Detail Page View — Validation Queries

> Run these in the **Supabase SQL Editor** after applying migration 023.
>
> Each query is self-contained and copy-pasteable.
> Expected result shapes are documented inline.

For the view definition see [../sql/views/occurrence_detail_view.sql](../sql/views/occurrence_detail_view.sql).
For the field mapping see [detail-page-contract.md](detail-page-contract.md).

---

## Q1 — Happy path: fetch by occurrence_id

Pick any published occurrence and verify the detail view returns
exactly one row with all required fields populated.

```sql
-- Pick the most recently updated occurrence visible in the feed
WITH sample AS (
    SELECT external_id AS occurrence_id
    FROM feed_cards_view
    ORDER BY updated_at DESC
    LIMIT 1
)
SELECT
    d.occurrence_id,
    d.happening_id,
    d.happening_title,
    d.canonical_url,
    d.start_at,
    d.end_at,
    d.timezone,
    d.date_precision,
    d.location_name,
    d.image_url,
    d.description        IS NOT NULL AS has_description,
    d.organizer_name     IS NOT NULL AS has_organizer,
    d.other_occurrences,
    d.offering_type,
    d.happening_kind,
    d.visibility_status,
    d.start_date_local,
    d.start_time_local,
    d.end_time_local
FROM occurrence_detail_view d
JOIN sample s ON d.occurrence_id::text = s.occurrence_id;
```

**Expected:** Exactly 1 row. `occurrence_id`, `happening_title`,
`start_at`, `timezone`, `date_precision`, `visibility_status` are
never NULL. `other_occurrences` is `[]` or a JSONB array.

---

## Q2 — best_source determinism: same happening, multiple sources

Verify that when a happening has multiple source links, the detail
view picks the same image/URL as the feed card.

```sql
-- Find happenings with 2+ source links
WITH multi_source_happenings AS (
    SELECT hs.happening_id, count(*) AS source_count
    FROM happening_sources hs
    GROUP BY hs.happening_id
    HAVING count(*) >= 2
    LIMIT 3
)
SELECT
    d.happening_id,
    d.happening_title,
    d.image_url       AS detail_image,
    f.image_url        AS feed_image,
    d.canonical_url    AS detail_url,
    f.canonical_url    AS feed_url,
    m.source_count
FROM multi_source_happenings m
JOIN occurrence_detail_view d ON d.happening_id = m.happening_id
JOIN feed_cards_view f        ON f.external_id  = d.occurrence_id::text
LIMIT 5;
```

**Expected:** For every row, `detail_image = feed_image` and
`detail_url = feed_url`. If they diverge, the `best_source` CTEs
are out of sync (bug).

If no rows are returned, all happenings currently have only one
source link — that's fine, the test is vacuously true.

---

## Q3 — other_occurrences: excludes self, excludes past, ordered

Verify the JSONB array logic for happenings with multiple occurrences.

```sql
-- Find an offering with 2+ scheduled future occurrences
WITH multi_occ AS (
    SELECT o.offering_id, count(*) AS occ_count
    FROM occurrence o
    WHERE o.status = 'scheduled'
      AND o.start_at IS NOT NULL
      AND COALESCE(o.end_at, o.start_at) >= now()
    GROUP BY o.offering_id
    HAVING count(*) >= 2
    LIMIT 1
)
SELECT
    d.occurrence_id,
    d.happening_title,
    d.start_at,
    jsonb_array_length(d.other_occurrences) AS other_count,
    d.other_occurrences
FROM occurrence_detail_view d
JOIN offering off ON off.id = (SELECT offering_id FROM multi_occ)
WHERE d.happening_id = off.happening_id
LIMIT 3;
```

**Expected per row:**
- `other_count` = (total future occurrences in offering) - 1,
  capped at 5
- The current row's `occurrence_id` must NOT appear in
  `other_occurrences`
- `other_occurrences` entries are ordered by `start_at` ascending

**Verify self-exclusion:**

```sql
-- For each row above, check that occurrence_id is not in the array
SELECT d.occurrence_id,
       d.other_occurrences @> jsonb_build_array(
           jsonb_build_object('occurrence_id', d.occurrence_id)
       ) AS self_present_in_others
FROM occurrence_detail_view d
WHERE jsonb_array_length(d.other_occurrences) > 0
LIMIT 5;
```

**Expected:** `self_present_in_others` = `false` for every row.

If no multi-occurrence offerings exist, all `other_occurrences` will
be `[]` — that's correct.

---

## Q4 — NULL handling: unknown-time and missing description

Verify that date-only events show NULL times and that missing
descriptions stay NULL (never empty string).

```sql
SELECT
    d.occurrence_id,
    d.happening_title,
    d.date_precision,
    d.start_time_local,
    d.end_time_local,
    d.description,
    CASE
        WHEN d.date_precision = 'date' AND d.start_time_local IS NOT NULL
            THEN 'BUG: time should be NULL for date-only'
        WHEN d.date_precision = 'date' AND d.end_time_local IS NOT NULL
            THEN 'BUG: end_time should be NULL for date-only'
        WHEN d.description = ''
            THEN 'BUG: description should be NULL, not empty string'
        ELSE 'OK'
    END AS check_result
FROM occurrence_detail_view d
ORDER BY d.date_precision, d.happening_title
LIMIT 20;
```

**Expected:** `check_result` = `'OK'` for every row.

- `date_precision = 'date'` rows: `start_time_local` and
  `end_time_local` MUST be NULL
- `date_precision = 'datetime'` rows: `start_time_local` is a
  `'HH:MM'` string
- `description`: either NULL or a non-empty string (never `''`)

---

## Q5 — Visibility: only published happenings appear

Verify that the view excludes draft and archived happenings.

```sql
-- Count by visibility status (should only have 'published')
SELECT
    d.visibility_status,
    count(*) AS row_count
FROM occurrence_detail_view d
GROUP BY d.visibility_status;
```

**Expected:** Exactly one row with `visibility_status = 'published'`.
No `draft` or `archived` rows should appear.

**Cross-check against unpublished happenings:**

```sql
-- These happenings must NOT appear in the detail view
SELECT h.id, h.title, h.visibility_status
FROM happening h
WHERE h.visibility_status != 'published'
  AND h.id IN (SELECT happening_id FROM occurrence_detail_view);
```

**Expected:** 0 rows. If any appear, the view's WHERE clause is broken.

---

## Summary checklist

| # | Check | Pass criteria |
|---|-------|---------------|
| Q1 | Happy path | 1 row, required fields non-NULL |
| Q2 | best_source determinism | detail image/URL = feed image/URL |
| Q3 | other_occurrences | self excluded, past excluded, ordered, max 5 |
| Q4 | NULL handling | No times for date-only; no empty-string descriptions |
| Q5 | Visibility | Only published happenings visible |
