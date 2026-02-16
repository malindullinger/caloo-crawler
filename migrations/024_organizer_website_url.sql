-- Migration 024: Add website_url to organizer + expose in detail view
--
-- The organizer table already exists (pre-migration, with name, organizer_type).
-- Migration 004 added legal_form, priority_score, locality.
-- This migration adds website_url and refreshes occurrence_detail_view to expose it.
--
-- happening.organizer_id already exists as a FK to organizer.
-- occurrence_detail_view already LEFT JOINs organizer.
-- This is additive only.

-- 1. Add website_url column
ALTER TABLE organizer
  ADD COLUMN IF NOT EXISTS website_url TEXT;

COMMENT ON COLUMN organizer.website_url
  IS 'Public website URL for the organizer (e.g. https://example.ch)';

-- 2. Replace occurrence_detail_view to expose organizer_website_url
--    All existing columns preserved; one column added.
CREATE OR REPLACE VIEW public.occurrence_detail_view AS

WITH best_source AS (
    SELECT DISTINCT ON (hs.happening_id)
        hs.happening_id,
        sh.item_url,
        sh.location_raw,
        sh.image_url,
        sh.description_raw
    FROM happening_sources hs
    JOIN source_happenings sh ON sh.id = hs.source_happening_id
    ORDER BY hs.happening_id,
             hs.is_primary DESC NULLS LAST,
             hs.source_priority,
             hs.merged_at DESC NULLS LAST
)

SELECT
    o.id                                        AS occurrence_id,
    h.id                                        AS happening_id,
    h.title                                     AS happening_title,
    NULLIF(BTRIM(bs.item_url), '')              AS canonical_url,

    o.start_at,
    o.end_at,
    off.timezone,

    CASE
        WHEN COALESCE(o.notes, '') ILIKE '%event time missing%'
            THEN 'date'
        WHEN o.start_at IS NOT NULL
             AND date_trunc('day', o.start_at) = o.start_at
            THEN 'date'
        ELSE 'datetime'
    END                                         AS date_precision,

    COALESCE(
        NULLIF(BTRIM(v.name), ''),
        NULLIF(BTRIM(bs.location_raw), ''),
        CASE
            WHEN bs.item_url ILIKE '%maennedorf.ch/%'
                THEN 'Männedorf'
            ELSE NULL
        END
    )                                           AS location_name,

    NULLIF(BTRIM(bs.image_url), '')             AS image_url,

    COALESCE(
        NULLIF(BTRIM(h.description), ''),
        NULLIF(BTRIM(bs.description_raw), '')
    )                                           AS description,

    org.name                                    AS organizer_name,

    NULL::text                                  AS booking_url,

    (
        SELECT COALESCE(jsonb_agg(sub.row_data ORDER BY sub.sort_at), '[]'::jsonb)
        FROM (
            SELECT
                jsonb_build_object(
                    'occurrence_id', oo.id,
                    'start_at',      oo.start_at,
                    'end_at',        oo.end_at
                ) AS row_data,
                oo.start_at AS sort_at
            FROM occurrence oo
            WHERE oo.offering_id = o.offering_id
              AND oo.id          != o.id
              AND oo.status       = 'scheduled'
              AND oo.start_at    IS NOT NULL
              AND COALESCE(oo.end_at, oo.start_at) >= now()
            ORDER BY oo.start_at
            LIMIT 5
        ) sub
    )                                           AS other_occurrences,

    off.start_date                              AS offering_start_date,
    off.end_date                                AS offering_end_date,
    off.offering_type,
    h.happening_kind,
    h.visibility_status,

    h.audience_tags,
    h.topic_tags,
    h.editorial_priority,

    (o.start_at AT TIME ZONE 'Europe/Zurich')::date  AS start_date_local,
    (o.end_at   AT TIME ZONE 'Europe/Zurich')::date  AS end_date_local,

    CASE
        WHEN COALESCE(o.notes, '') ILIKE '%event time missing%'
            THEN NULL
        WHEN o.start_at IS NOT NULL
             AND date_trunc('day', o.start_at) = o.start_at
            THEN NULL
        ELSE to_char(
            (o.start_at AT TIME ZONE 'Europe/Zurich')::time::interval,
            'HH24:MI'
        )
    END                                         AS start_time_local,

    CASE
        WHEN o.end_at IS NULL
            THEN NULL
        WHEN COALESCE(o.notes, '') ILIKE '%event time missing%'
            THEN NULL
        WHEN o.start_at IS NOT NULL
             AND date_trunc('day', o.start_at) = o.start_at
            THEN NULL
        ELSE to_char(
            (o.end_at AT TIME ZONE 'Europe/Zurich')::time::interval,
            'HH24:MI'
        )
    END                                         AS end_time_local,

    org.organizer_type,
    NULLIF(BTRIM(org.website_url), '')          AS organizer_website_url

FROM occurrence o
JOIN offering off   ON off.id = o.offering_id
JOIN happening h    ON h.id   = off.happening_id
LEFT JOIN venue v   ON v.id   = COALESCE(o.venue_id, h.primary_venue_id)
LEFT JOIN organizer org ON org.id = h.organizer_id
LEFT JOIN best_source bs ON bs.happening_id = h.id

WHERE h.visibility_status = 'published'
  AND o.status = 'scheduled';
