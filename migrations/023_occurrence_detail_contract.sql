-- 023_occurrence_detail_contract.sql
-- Create occurrence_detail_view: canonical detail page data contract.
--
-- Purpose:
--   One-row-per-occurrence enrichment view for the detail page.
--   Provides happening/offering context, best-source image/description,
--   organizer info, and "other dates" for the same offering.
--
-- This is NOT a feed view. The feed contract (feed_cards_view) is unchanged.
-- Frontend queries this view by occurrence_id to render the detail page.
--
-- Query pattern (Supabase JS):
--   supabase.from('occurrence_detail_view')
--     .select('*')
--     .eq('occurrence_id', id)
--     .single()
--
-- Safe to re-run (CREATE OR REPLACE VIEW is idempotent).

-- ============================================================
-- UP
-- ============================================================

CREATE OR REPLACE VIEW public.occurrence_detail_view AS

-- best_source: deterministic selection of source data per happening.
-- Same CTE as feed_cards_view — primary source first, then highest
-- priority, then most recently merged. Ensures image/description/URL
-- selection is consistent between feed card and detail page.
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
    -- ── Required fields ──────────────────────────────────────
    o.id                                        AS occurrence_id,
    h.id                                        AS happening_id,
    h.title                                     AS happening_title,
    NULLIF(BTRIM(bs.item_url), '')              AS canonical_url,

    o.start_at,
    o.end_at,
    off.timezone,

    -- date_precision: same logic as feed_cards_view (no divergence)
    CASE
        WHEN COALESCE(o.notes, '') ILIKE '%event time missing%'
            THEN 'date'
        WHEN o.start_at IS NOT NULL
             AND date_trunc('day', o.start_at) = o.start_at
            THEN 'date'
        ELSE 'datetime'
    END                                         AS date_precision,

    -- location_name: venue → best_source → municipality fallback
    -- Same COALESCE chain as feed_cards_view
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

    -- ── Optional fields (nullable) ───────────────────────────

    -- description: canonical happening first, then best-source fallback
    COALESCE(
        NULLIF(BTRIM(h.description), ''),
        NULLIF(BTRIM(bs.description_raw), '')
    )                                           AS description,

    org.name                                    AS organizer_name,

    -- booking_url: deferred (no source field exists yet)
    NULL::text                                  AS booking_url,

    -- other_occurrences: up to 5 upcoming occurrences in same offering
    -- Ordered by start_at; excludes the current occurrence and past events.
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

    -- ── Context fields ───────────────────────────────────────

    off.start_date                              AS offering_start_date,
    off.end_date                                AS offering_end_date,
    off.offering_type,
    h.happening_kind,
    h.visibility_status,

    h.audience_tags,
    h.topic_tags,
    h.editorial_priority,

    -- ── Time display (same rules as feed_cards_view) ─────────

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

    -- ── Organizer context ────────────────────────────────────

    org.organizer_type

FROM occurrence o
JOIN offering off   ON off.id = o.offering_id
JOIN happening h    ON h.id   = off.happening_id
LEFT JOIN venue v   ON v.id   = COALESCE(o.venue_id, h.primary_venue_id)
LEFT JOIN organizer org ON org.id = h.organizer_id
LEFT JOIN best_source bs ON bs.happening_id = h.id

-- Published only. Cancelled/completed occurrences excluded.
-- No time-based filter: deeplinks to past occurrences still resolve.
WHERE h.visibility_status = 'published'
  AND o.status = 'scheduled';


-- ============================================================
-- Verification (run after migration to confirm)
-- ============================================================
-- SELECT occurrence_id, happening_title, date_precision,
--        start_at, location_name, image_url,
--        jsonb_array_length(other_occurrences) AS other_dates_count
-- FROM occurrence_detail_view
-- LIMIT 5;
--
-- Single-row lookup (expected query pattern):
-- SELECT * FROM occurrence_detail_view
-- WHERE occurrence_id = '<uuid>';

-- ============================================================
-- DOWN (rollback)
-- ============================================================
-- DROP VIEW IF EXISTS public.occurrence_detail_view;
