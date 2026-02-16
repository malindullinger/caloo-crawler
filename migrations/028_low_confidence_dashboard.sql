-- Migration 028: Low-confidence admin dashboard view
--
-- Creates a read-only diagnostic view that surfaces happenings with
-- low confidence scores alongside their source metadata signals.
--
-- Pure diagnostic. No data changes. No feed changes.
-- Sorted by confidence_score ASC (worst first).

CREATE OR REPLACE VIEW public.low_confidence_dashboard_view AS

WITH best_source AS (
    SELECT DISTINCT ON (hs.happening_id)
        hs.happening_id,
        sh.source_tier,
        sh.date_precision,
        sh.image_url,
        sh.description_raw,
        sh.item_url,
        sh.extraction_method
    FROM happening_sources hs
    JOIN source_happenings sh ON sh.id = hs.source_happening_id
    ORDER BY hs.happening_id,
             hs.is_primary DESC NULLS LAST,
             hs.source_priority,
             hs.merged_at DESC NULLS LAST
)

SELECT
    h.id                                              AS happening_id,
    h.title                                           AS title,
    h.confidence_score                                AS confidence_score,
    bs.source_tier                                    AS source_tier,
    bs.date_precision                                 AS date_precision,
    (bs.image_url IS NOT NULL
     AND btrim(bs.image_url) != '')                   AS has_image,
    (COALESCE(h.description, bs.description_raw) IS NOT NULL
     AND btrim(COALESCE(h.description, bs.description_raw, '')) != '')
                                                      AS has_description,
    (bs.item_url IS NOT NULL
     AND btrim(bs.item_url) != '')                    AS canonical_url_present,
    bs.extraction_method                              AS extraction_method
FROM happening h
LEFT JOIN best_source bs ON bs.happening_id = h.id
WHERE h.visibility_status = 'published'
ORDER BY h.confidence_score ASC;
