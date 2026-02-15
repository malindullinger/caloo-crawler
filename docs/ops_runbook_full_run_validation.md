# Ops Runbook: Full-Scale Pipeline Validation

Validation checklist for running the complete crawl + canonicalize
pipeline at production scale, with focus on `maennedorf_portal`.

---

## Prerequisites

1. Migrations 014–016 applied to Supabase (see [db_migration_apply_checklist.md](db_migration_apply_checklist.md)).
2. DB smoke check passes:
   ```bash
   python -m scripts.db_smoke_verify
   ```
3. Environment variables set: `SUPABASE_URL`, `SUPABASE_SERVICE_ROLE_KEY`.

---

## Step 1 — Adjust source config (if needed)

The pipeline uses DB-driven source configs. To validate at full scale,
ensure `maennedorf_portal.max_items` is set to the real item count
(~164–200) in the `source_configs` table:

```sql
SELECT source_id, config
FROM source_configs
WHERE source_id = 'maennedorf_portal';
```

If `max_items` is artificially low for dev, temporarily update it:

```sql
UPDATE source_configs
SET config = jsonb_set(config, '{max_items}', '200')
WHERE source_id = 'maennedorf_portal';
```

Remember to revert after validation if needed.

---

## Step 2 — Run the pipeline

### Crawl only (fetch + normalize + upsert to source_happenings)

```bash
CALOO_MAENNEDORF_WORKERS=10 python -m src.pipeline 2>&1 | tee /tmp/pipeline.log
```

### Crawl + merge loop (full pipeline)

If the merge loop runs separately:

```bash
# 1. Crawl
CALOO_MAENNEDORF_WORKERS=10 python -m src.pipeline 2>&1 | tee /tmp/pipeline.log

# 2. Merge with stats
python -c "
from scripts.canonicalize_cli import get_supabase_client
from src.canonicalize.merge_loop import run_merge_loop
sb = get_supabase_client()
counts = run_merge_loop(supabase=sb, dry_run=False, persist_run_stats=True)
print(counts)
" 2>&1 | tee /tmp/merge.log
```

### Environment knobs

| Variable | Default | Purpose |
|----------|---------|---------|
| `CALOO_MAENNEDORF_WORKERS` | 10 | Concurrent detail-page fetch threads |
| `CALOO_MAENNEDORF_JS_FALLBACK` | `true` | Enable/disable Playwright JS fallback |

---

## Step 3 — Extract from logs

### Maennedorf timing line

Look for the `[maennedorf][timing]` log line:

```bash
grep '\[maennedorf\]\[timing\]' /tmp/pipeline.log
```

Expected output shape:

```
[maennedorf][timing] listing_s=1.23 details_s=45.67 total_s=46.90 urls=164 workers=10 js_fallback_used=2 avg_detail_s=0.28 avg_fetch_s=0.25
```

Key metrics:

| Metric | Good | Investigate |
|--------|------|-------------|
| `urls` | ~164 (or matches portal) | Much lower = listing page issue |
| `js_fallback_used` | 0–5 | > 20 = non-JS extraction regressed |
| `avg_detail_s` | < 0.5s | > 2s = network or parsing issue |
| `workers` | 10 | Check env var if different |

### Merge run stats

```bash
grep -E 'created|merged|review|errors|canonical_updates|history_rows' /tmp/merge.log
```

Or query Supabase directly:

```sql
SELECT
  started_at,
  finished_at,
  source_rows_processed,
  canonical_created,
  canonical_merged,
  canonical_review,
  errors,
  canonical_updates_count,
  history_rows_created,
  confidence_avg,
  source_breakdown
FROM merge_run_stats
ORDER BY started_at DESC
LIMIT 1;
```

---

## Step 4 — Success criteria

### Crawl

- [ ] `urls` in timing line roughly matches portal count (~164)
- [ ] `js_fallback_used` approaches 0 (ideally < 5)
- [ ] No Python stack traces in pipeline.log
- [ ] `Normalized events written` count is close to `urls` count

### Merge

- [ ] `errors` = 0
- [ ] `source_rows_processed` matches expected queue size
- [ ] `canonical_created` + `canonical_merged` + `canonical_review` = `source_rows_processed`
- [ ] `finished_at` is non-null (run completed)
- [ ] `confidence_avg` is non-null (if any rows were scored)
- [ ] `source_breakdown` JSON has `maennedorf_portal` key

### DB state

```sql
-- Total canonical happenings
SELECT count(*) FROM happening;

-- Happenings from maennedorf
SELECT count(*)
FROM source_happenings
WHERE source_id = 'maennedorf_portal'
  AND status = 'merged';

-- Field history rows (should be > 0 on re-runs)
SELECT count(*) FROM canonical_field_history;
```

---

## Step 5 — Known follow-up optimizations

These are **not** blockers for validation. Capture for future phases.

### Skip JS fallback for non-event detail pages

Some maennedorf detail URLs redirect to `/sitzung/` (municipal meeting
minutes) or similar non-event targets. Currently these fail non-JS
extraction and trigger a JS fallback that also fails.

**Optimization**: Detect known non-event URL patterns before fetching
detail pages and skip them entirely. This would reduce `js_fallback_used`
further and speed up the crawl.

### GiST + daterange index for offering lookups

Migration 015 adds a B-tree index on `offering(start_date, end_date)`.
For optimal range-containment queries (`start_date <= X AND end_date >= X`),
a GiST index on a `daterange` column would be more efficient.
Deferred to Phase 8.5.

### Batch size tuning

The merge loop fetches 200 rows per batch. At scale, consider:
- Adaptive batch sizing based on queue depth
- Backpressure when approaching rate limits
