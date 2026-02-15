# DB Migration Apply Checklist (Phases 7–9)

Three migrations must be applied **in order**. All are additive
(no destructive DDL), so they are safe to re-run thanks to
`IF NOT EXISTS` / `CREATE OR REPLACE` guards.

---

## Apply order

| # | File | What it does |
|---|------|--------------|
| 1 | `014_merge_run_stats_observability.sql` | Adds `canonical_updates_count`, `history_rows_created`, `source_breakdown`, `stage_timings_ms` to `merge_run_stats`. Creates RPC `insert_field_history_batch`. |
| 2 | `015_performance_indexes.sql` | Adds 5 B-tree indexes on hot-path tables. |
| 3 | `016_confidence_telemetry_and_review_outcomes.sql` | Adds confidence columns to `merge_run_stats`. Creates `canonical_review_outcomes` table + indexes. |

---

## How to apply

### Option A — Supabase SQL Editor (recommended for small projects)

1. Open **Supabase Dashboard > SQL Editor**.
2. Paste the contents of each migration file **one at a time, in order**.
3. Click **Run**.

### Option B — Supabase CLI migrations

```bash
supabase migration up
```

(Only if the project uses `supabase/migrations/` directory structure.)

### Important: CREATE INDEX and transactions

Supabase SQL Editor wraps each execution in an implicit transaction.
`CREATE INDEX` (non-concurrent) works fine inside transactions.

If you later need `CREATE INDEX CONCURRENTLY` (for zero-downtime on
large tables), you **must not** run it inside a transaction. In that
case, use `psql` directly or the Supabase SQL Editor with each
`CREATE INDEX CONCURRENTLY` statement run **individually** (not batched
with other DDL).

Migration 015 uses plain `CREATE INDEX IF NOT EXISTS` (not concurrent),
so it is safe to run in the SQL Editor as-is.

---

## Post-apply verification

Run these queries in the SQL Editor to confirm everything landed.

### 1. Verify merge_run_stats columns

```sql
SELECT
  column_name, data_type
FROM information_schema.columns
WHERE table_schema = 'public'
  AND table_name = 'merge_run_stats'
  AND column_name IN (
    'canonical_updates_count',
    'history_rows_created',
    'source_breakdown',
    'stage_timings_ms',
    'confidence_min',
    'confidence_avg',
    'confidence_max',
    'confidence_histogram',
    'source_confidence'
  )
ORDER BY column_name;
```

**Expected**: 9 rows returned.

### 2. Verify RPC function exists

```sql
SELECT routine_name, data_type
FROM information_schema.routines
WHERE routine_schema = 'public'
  AND routine_name = 'insert_field_history_batch';
```

**Expected**: 1 row with `data_type = 'integer'`.

### 3. Verify canonical_review_outcomes table

```sql
SELECT column_name, data_type
FROM information_schema.columns
WHERE table_schema = 'public'
  AND table_name = 'canonical_review_outcomes'
ORDER BY ordinal_position;
```

**Expected**: 11 columns (`id`, `review_id`, `source_happening_id`,
`happening_id`, `decision`, `selected_candidate_happening_id`,
`confidence_score`, `confidence_breakdown`, `resolved_by`,
`resolved_at`, `created_at`).

### 4. Verify indexes

```sql
SELECT indexname
FROM pg_indexes
WHERE schemaname = 'public'
  AND indexname IN (
    'idx_sh_status_created',
    'idx_sh_source_date',
    'idx_offering_date_range',
    'idx_occurrence_offering',
    'idx_merge_run_stats_started',
    'idx_review_outcomes_review_id',
    'idx_review_outcomes_source',
    'idx_review_outcomes_resolved',
    'idx_field_history_change_key',
    'idx_field_history_happening'
  )
ORDER BY indexname;
```

**Expected**: 10 rows.

---

## Rollback / recovery

All migrations use `IF NOT EXISTS` guards:

- **ALTER TABLE ADD COLUMN IF NOT EXISTS** — safe to re-run.
- **CREATE TABLE IF NOT EXISTS** — safe to re-run.
- **CREATE INDEX IF NOT EXISTS** — safe to re-run.
- **CREATE OR REPLACE FUNCTION** — safe to re-run (replaces in place).

If you need to **undo** a migration:

| Migration | Rollback |
|-----------|----------|
| 014 | `ALTER TABLE merge_run_stats DROP COLUMN IF EXISTS canonical_updates_count, DROP COLUMN IF EXISTS history_rows_created, DROP COLUMN IF EXISTS source_breakdown, DROP COLUMN IF EXISTS stage_timings_ms; DROP FUNCTION IF EXISTS insert_field_history_batch(JSONB);` |
| 015 | `DROP INDEX IF EXISTS idx_sh_status_created, idx_sh_source_date, idx_offering_date_range, idx_occurrence_offering, idx_merge_run_stats_started;` |
| 016 | `ALTER TABLE merge_run_stats DROP COLUMN IF EXISTS confidence_min, DROP COLUMN IF EXISTS confidence_avg, DROP COLUMN IF EXISTS confidence_max, DROP COLUMN IF EXISTS confidence_histogram, DROP COLUMN IF EXISTS source_confidence; DROP TABLE IF EXISTS canonical_review_outcomes;` |

Rollback is non-destructive to data in other tables. Dropping columns
on `merge_run_stats` only loses observability data (not merge data).
