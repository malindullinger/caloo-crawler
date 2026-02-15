# EXPLAIN Checklist — Merge Loop Hot Paths

Run these queries manually against the live database after applying
`migrations/015_performance_indexes.sql` to verify indexes are used.

Each query mirrors a real merge loop access pattern. Expected plan
describes what `EXPLAIN (ANALYZE, BUFFERS)` should show.

---

## 1. fetch_queued_source_happenings

**Hot path:** Called once per batch iteration. Scans the full queue.

```sql
EXPLAIN (ANALYZE, BUFFERS)
SELECT *
FROM source_happenings
WHERE dedupe_key LIKE 'v1|%'
  AND status IN ('queued', 'needs_review')
ORDER BY created_at ASC
LIMIT 200;
```

**Expected plan:** Index Scan using `idx_sh_status_created` on
`source_happenings (status, created_at)`. The `status IN` filter uses
the first index column; `ORDER BY created_at ASC` is satisfied by the
second column (no Sort node). `dedupe_key LIKE 'v1|%'` appears as a
Filter condition on the index scan output.

---

## 2. fetch_candidate_bundles (offering range scan)

**Hot path:** Called once per source row. Finds offerings spanning the source date.

```sql
EXPLAIN (ANALYZE, BUFFERS)
SELECT *
FROM offering
WHERE start_date <= '2026-03-15'
  AND end_date >= '2026-03-15'
LIMIT 200;
```

**Expected plan:** Index Scan (or Bitmap Index Scan) using
`idx_offering_date_range` on `offering (start_date, end_date)`. The
btree satisfies `start_date <= X` via the first column; `end_date >= X`
is filtered from matching rows (Index Cond or Filter).

**Note:** This btree is a partial improvement. For optimal range
containment queries, a GiST index on a `daterange` column is
recommended (Phase 8.5).

---

## 3. occurrence enrichment

**Hot path:** Called once per source row (if offerings exist). Fetches occurrences by offering IDs.

```sql
EXPLAIN (ANALYZE, BUFFERS)
SELECT id, offering_id, venue_id, start_at, end_at, status
FROM occurrence
WHERE offering_id IN (
  -- Replace with real UUIDs from the offering table:
  '00000000-0000-0000-0000-000000000001',
  '00000000-0000-0000-0000-000000000002'
);
```

**Expected plan:** Index Scan (or Bitmap Index Scan) using
`idx_occurrence_offering` on `occurrence (offering_id)`. Each UUID in
the IN list triggers an index probe.

---

## 4. canonical_field_history lookup

**Hot path:** Read path for field history (debugging / observability). Also used by the RPC's ON CONFLICT via `idx_field_history_change_key`.

```sql
EXPLAIN (ANALYZE, BUFFERS)
SELECT *
FROM canonical_field_history
WHERE happening_id = '00000000-0000-0000-0000-000000000001';
```

**Expected plan:** Index Scan using `idx_field_history_happening` on
`canonical_field_history (happening_id)`.

---

## 5. merge_run_stats dashboard

**Hot path:** Observability dashboard — latest runs.

```sql
EXPLAIN (ANALYZE, BUFFERS)
SELECT *
FROM merge_run_stats
ORDER BY started_at DESC
LIMIT 10;
```

**Expected plan:** Index Scan Backward using `idx_merge_run_stats_started`
on `merge_run_stats (started_at DESC)`. No Sort node needed.

---

## What to look for

| Good sign | Bad sign |
|-----------|----------|
| `Index Scan` or `Index Only Scan` | `Seq Scan` on large tables |
| No `Sort` node when ORDER BY is present | `Sort` node (index not covering order) |
| Low `Buffers: shared hit` relative to rows | High `Buffers: shared read` (cold cache) |
| `Rows Removed by Filter` is small | `Rows Removed by Filter` is large fraction of scanned rows |
