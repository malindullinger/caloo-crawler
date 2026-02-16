-- 021_publish_draft_happenings.sql
-- One-time fix: publish draft happenings that were created by the merge loop
-- but never made it to the feed because visibility_status was 'draft'.
--
-- Context:
--   create_happening_schedule_occurrence() previously set visibility_status='draft'.
--   For maennedorf_portal, happenings were pre-created as 'published' by the legacy
--   bridge, so they worked. For eventbrite_zurich and elternverein_uetikon, the
--   merge loop created new draft happenings that never appeared in the feed.
--
-- Scope: Only publishes happenings that are linked via happening_sources
-- (i.e. they went through the merge loop and have provenance). Orphan drafts
-- without source links are left untouched.
--
-- Safe to run multiple times (idempotent).

-- ============================================================
-- UP
-- ============================================================

UPDATE happening
SET    visibility_status = 'published',
       updated_at = now()
WHERE  visibility_status = 'draft'
  AND  id IN (
    SELECT DISTINCT hs.happening_id
    FROM   happening_sources hs
  );

-- ============================================================
-- DOWN (rollback) — not reversible: we don't know which were
-- originally draft. Kept as comment for documentation.
-- ============================================================
-- No safe rollback: publishing is a forward-only operation.
-- If needed, identify affected rows via:
--   SELECT h.id FROM happening h
--   WHERE h.updated_at >= '<migration_run_timestamp>'
--     AND h.visibility_status = 'published';
