-- Migration 015: Performance indexes for merge loop hot paths
--
-- Phase 8 — no behavior changes, no schema changes (indexes only).
--
-- These indexes target the queries executed on every merge_loop iteration.
-- All use CREATE INDEX IF NOT EXISTS so they are safe to re-run.
--
-- NOTE: For production application on large live tables, consider running
-- CREATE INDEX CONCURRENTLY outside a transaction instead. Supabase migration
-- runners execute inside a transaction, which is incompatible with CONCURRENTLY.

-- ---------------------------------------------------------------------------
-- 1. source_happenings: serves fetch_queued_source_happenings
--
--    Query pattern (PostgREST):
--      .like("dedupe_key", "v1|%")
--      .in_("status", ["queued", "needs_review"])
--      .order("created_at", desc=False)
--      .limit(200)
--
--    Translates to:
--      WHERE dedupe_key LIKE 'v1|%'
--        AND status IN ('queued', 'needs_review')
--      ORDER BY created_at ASC
--      LIMIT 200
--
--    Without index: seq scan + sort on full table.
--    With index: index scan on (status, created_at) satisfies both the
--      status IN filter (first column) and ORDER BY created_at (second column).
--      dedupe_key LIKE 'v1|%' is applied as a secondary filter on matching rows.
-- ---------------------------------------------------------------------------
CREATE INDEX IF NOT EXISTS idx_sh_status_created
  ON source_happenings (status, created_at);

-- ---------------------------------------------------------------------------
-- 2. source_happenings: analytical queries per source + date range
--
--    Supports future queries filtering by source_id + start_date_local range
--    (e.g., "show all rows from zurich_gemeinde in March 2026").
--    Also useful for the enqueue upsert path which keys on source_id.
-- ---------------------------------------------------------------------------
CREATE INDEX IF NOT EXISTS idx_sh_source_date
  ON source_happenings (source_id, start_date_local);

-- ---------------------------------------------------------------------------
-- 3. offering: serves fetch_candidate_bundles range scan
--
--    Query pattern (PostgREST):
--      .select("*, happening(*)")
--      .lte("start_date", source_start_date_local)
--      .gte("end_date", source_start_date_local)
--      .limit(200)
--
--    Translates to:
--      WHERE start_date <= X AND end_date >= X
--
--    Without index: seq scan on offering table.
--    With btree on (start_date, end_date): partial improvement.
--      Postgres can use start_date <= X efficiently via the first column,
--      then filters end_date >= X from matching rows.
--
--    NOTE: For optimal range containment queries (start_date <= X <= end_date),
--    a GiST index on a daterange column is the proper solution. That requires
--    adding a computed daterange column — deferred to Phase 8.5 to avoid
--    schema changes in this hardening-only migration.
-- ---------------------------------------------------------------------------
CREATE INDEX IF NOT EXISTS idx_offering_date_range
  ON offering (start_date, end_date);

-- ---------------------------------------------------------------------------
-- 4. occurrence: serves occurrence enrichment in fetch_candidate_bundles
--
--    Query pattern (PostgREST):
--      .select("id,offering_id,venue_id,start_at,end_at,status")
--      .in_("offering_id", offering_ids)
--      .limit(2000)
--
--    Translates to:
--      WHERE offering_id IN (uuid1, uuid2, ...)
--
--    Without index: seq scan on occurrence table per batch.
--    With index: index scan on offering_id.
-- ---------------------------------------------------------------------------
CREATE INDEX IF NOT EXISTS idx_occurrence_offering
  ON occurrence (offering_id);

-- ---------------------------------------------------------------------------
-- 5. merge_run_stats: observability dashboard ordering
--
--    Query pattern:
--      ORDER BY started_at DESC LIMIT N
--
--    Without index: seq scan + sort (small table now, grows with each run).
--    With index: index scan backward on (started_at DESC).
-- ---------------------------------------------------------------------------
CREATE INDEX IF NOT EXISTS idx_merge_run_stats_started
  ON merge_run_stats (started_at DESC);
