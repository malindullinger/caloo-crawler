-- Post-deploy quick check
-- Single deterministic read-only query. No temp tables, no functions.
-- Copy-paste into Supabase SQL editor after applying migrations.

SELECT
    (SELECT count(*)
     FROM feed_cards_view
    )::int                                          AS feed_card_count,

    (SELECT count(*)
     FROM happening
     WHERE visibility_status = 'published'
    )::int                                          AS published_happening_count,

    (SELECT count(*)
     FROM occurrence
     WHERE status = 'scheduled'
    )::int                                          AS scheduled_occurrence_count,

    (SELECT count(*)
     FROM happening
     WHERE visibility_status = 'published'
       AND confidence_score < 50
    )::int                                          AS low_confidence_count,

    (SELECT count(*)
     FROM system_integrity_view
     WHERE status = 'FAIL'
    )::int                                          AS integrity_fail_count;
