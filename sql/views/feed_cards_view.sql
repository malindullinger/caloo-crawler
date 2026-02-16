-- Mirrored from Supabase via:
--   select pg_get_viewdef('public.feed_cards_view'::regclass, true) as sql;
-- Date: 2026-02-16 (Europe/Zurich)
-- Frontend reads ONLY public.feed_cards_view.

WITH params AS (
         SELECT (now() AT TIME ZONE 'Europe/Zurich'::text) AS now_zh,
            date_trunc('week'::text, (now() AT TIME ZONE 'Europe/Zurich'::text)) + '4 days'::interval + '00:01:00'::interval AS weekend_start_zh,
            date_trunc('week'::text, (now() AT TIME ZONE 'Europe/Zurich'::text)) + '7 days'::interval AS weekend_end_zh
        ), best_source AS (
         SELECT DISTINCT ON (hs.happening_id) hs.happening_id,
            sh.item_url,
            sh.location_raw,
            sh.image_url
           FROM happening_sources hs
             JOIN source_happenings sh ON sh.id = hs.source_happening_id
          ORDER BY hs.happening_id, hs.is_primary DESC NULLS LAST, hs.source_priority, hs.merged_at DESC NULLS LAST
        ), base AS (
         SELECT o.id::text AS external_id,
            h.title,
            h.audience_tags,
            h.topic_tags,
            h.editorial_priority,
            h.relevance_score_global,
            COALESCE(NULLIF(btrim(v.name), ''::text), NULLIF(btrim(bs.location_raw), ''::text),
                CASE
                    WHEN bs.item_url ~~* '%maennedorf.ch/%'::text THEN 'Männedorf'::text
                    ELSE NULL::text
                END) AS location_name,
            NULLIF(btrim(bs.item_url), ''::text) AS canonical_url,
            o.start_at,
            o.end_at,
            off.timezone,
            h.happening_kind::text AS event_type,
            false AS is_all_day,
                CASE
                    WHEN COALESCE(o.notes, ''::text) ~~* '%event time missing%'::text THEN 'date'::text
                    WHEN o.start_at IS NOT NULL AND date_trunc('day'::text, o.start_at) = o.start_at THEN 'date'::text
                    ELSE 'datetime'::text
                END AS date_precision,
            NULLIF(btrim(bs.image_url), ''::text) AS image_url,
            NULL::text AS price_text,
            off.offering_type::text AS schedule_type,
            (o.start_at AT TIME ZONE 'Europe/Zurich'::text)::date AS start_date_local,
            (o.end_at AT TIME ZONE 'Europe/Zurich'::text)::date AS end_date_local,
                CASE
                    WHEN COALESCE(o.notes, ''::text) ~~* '%event time missing%'::text THEN NULL::text
                    ELSE to_char((o.start_at AT TIME ZONE 'Europe/Zurich'::text)::time without time zone::interval, 'HH24:MI'::text)
                END AS start_time_local,
                CASE
                    WHEN o.end_at IS NULL THEN NULL::text
                    WHEN COALESCE(o.notes, ''::text) ~~* '%event time missing%'::text THEN NULL::text
                    ELSE to_char((o.end_at AT TIME ZONE 'Europe/Zurich'::text)::time without time zone::interval, 'HH24:MI'::text)
                END AS end_time_local,
            h.title AS title_raw,
            'canonical'::text AS title_source,
            GREATEST(h.updated_at, off.updated_at, o.updated_at) AS updated_at,
            o.start_at AS sort_at
           FROM occurrence o
             JOIN offering off ON off.id = o.offering_id
             JOIN happening h ON h.id = off.happening_id
             LEFT JOIN venue v ON v.id = COALESCE(o.venue_id, h.primary_venue_id)
             LEFT JOIN best_source bs ON bs.happening_id = h.id
          WHERE h.visibility_status = 'published'::visibility_status AND o.status = 'scheduled'::occurrence_status AND o.start_at IS NOT NULL AND (o.end_at IS NULL OR o.end_at >= o.start_at) AND COALESCE(o.end_at, o.start_at) >= now()
        ), computed AS (
         SELECT b.external_id,
            b.title,
            b.audience_tags,
            b.topic_tags,
            b.editorial_priority,
            b.relevance_score_global,
            b.location_name,
            b.canonical_url,
            b.start_at,
            b.end_at,
            b.timezone,
            b.event_type,
            b.is_all_day,
            b.date_precision,
            b.image_url,
            b.price_text,
            b.schedule_type,
            b.start_date_local,
            b.end_date_local,
            b.start_time_local,
            b.end_time_local,
            b.title_raw,
            b.title_source,
            b.updated_at,
            b.sort_at,
            p.now_zh,
            p.weekend_start_zh,
            p.weekend_end_zh,
            (b.sort_at AT TIME ZONE 'Europe/Zurich'::text) >= p.weekend_start_zh AND (b.sort_at AT TIME ZONE 'Europe/Zurich'::text) < p.weekend_end_zh AS is_this_weekend,
                CASE
                    WHEN (b.sort_at AT TIME ZONE 'Europe/Zurich'::text) >= p.weekend_start_zh AND (b.sort_at AT TIME ZONE 'Europe/Zurich'::text) < p.weekend_end_zh THEN 'weekend'::text
                    ELSE 'coming_up'::text
                END AS section_key,
                CASE
                    WHEN b.start_at IS NOT NULL AND b.end_at IS NOT NULL AND (b.end_at - b.start_at) <= '12:00:00'::interval THEN p.now_zh >= (b.start_at AT TIME ZONE 'Europe/Zurich'::text) AND p.now_zh < (b.end_at AT TIME ZONE 'Europe/Zurich'::text)
                    ELSE false
                END AS is_happening_now
           FROM base b
             CROSS JOIN params p
        ), labels AS (
         SELECT c.external_id,
            c.title,
            c.audience_tags,
            c.topic_tags,
            c.editorial_priority,
            c.relevance_score_global,
            c.location_name,
            c.canonical_url,
            c.start_at,
            c.end_at,
            c.timezone,
            c.event_type,
            c.is_all_day,
            c.date_precision,
            c.image_url,
            c.price_text,
            c.schedule_type,
            c.start_date_local,
            c.end_date_local,
            c.start_time_local,
            c.end_time_local,
            c.title_raw,
            c.title_source,
            c.updated_at,
            c.sort_at,
            c.now_zh,
            c.weekend_start_zh,
            c.weekend_end_zh,
            c.is_this_weekend,
            c.section_key,
            c.is_happening_now,
                CASE
                    WHEN c.is_happening_now THEN 'Ongoing'::text
                    WHEN c.section_key = 'weekend'::text THEN 'This weekend'::text
                    ELSE 'Coming up'::text
                END AS display_kind,
                CASE
                    WHEN c.is_happening_now AND c.end_at IS NOT NULL THEN 'Until '::text || to_char((c.end_at AT TIME ZONE 'Europe/Zurich'::text)::time without time zone::interval, 'HH24:MI'::text)
                    WHEN c.start_date_local IS NOT NULL AND (c.start_time_local IS NULL OR c.start_time_local = ''::text OR c.date_precision = 'date'::text) THEN to_char(c.start_date_local::timestamp without time zone, 'Dy DD Mon'::text)
                    WHEN c.start_date_local IS NOT NULL AND c.start_time_local IS NOT NULL AND c.start_time_local <> ''::text THEN (to_char(c.start_date_local::timestamp without time zone, 'Dy DD Mon'::text) || ' · '::text) || "left"(c.start_time_local, 5)
                    WHEN c.start_at IS NOT NULL THEN to_char((c.start_at AT TIME ZONE 'Europe/Zurich'::text), 'Dy DD Mon · HH24:MI'::text)
                    ELSE NULL::text
                END AS display_when,
            NULL::text AS series_label,
            NULL::integer AS series_upcoming_index,
            NULL::integer AS series_upcoming_total
           FROM computed c
        )
 SELECT external_id,
    title,
    location_name,
    canonical_url,
    start_at,
    end_at,
    timezone,
    event_type,
    is_all_day,
    date_precision,
    image_url,
    price_text,
    schedule_type,
    start_date_local,
    end_date_local,
    start_time_local,
    end_time_local,
    title_raw,
    title_source,
    updated_at,
    sort_at,
    is_this_weekend,
    section_key,
    display_kind,
    display_when,
    is_happening_now,
    series_label,
    series_upcoming_index,
    series_upcoming_total,
    audience_tags,
    topic_tags,
    editorial_priority,
    relevance_score_global
   FROM labels
  ORDER BY (section_key = 'weekend'::text) DESC, editorial_priority DESC NULLS LAST, relevance_score_global DESC NULLS LAST, sort_at, title;
