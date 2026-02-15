# Ops Runbook: Merge Loop

How to run, monitor, and troubleshoot the canonicalization merge loop.

---

## Commands

### Dry-run benchmark (no DB writes)

```bash
python -m scripts.merge_benchmark
```

Outputs JSON:

```json
{
  "wall_clock_ms": 142,
  "queued": 12,
  "created": 8,
  "merged": 3,
  "review": 1,
  "errors": 0,
  "canonical_updates": 4,
  "history_rows": 7
}
```

| Key | Meaning |
|-----|---------|
| `wall_clock_ms` | Total elapsed time for the merge loop (ms) |
| `queued` | Source rows fetched from the queue |
| `created` | New canonical happenings created |
| `merged` | Source rows merged into existing happenings |
| `review` | Rows sent to ambiguous-match review |
| `errors` | Rows that failed processing |
| `canonical_updates` | Field-level updates applied to existing happenings |
| `history_rows` | Rows written to `canonical_field_history` |

### Real merge run (writes to DB)

```bash
# With run stats persisted
python -c "
from scripts.canonicalize_cli import get_supabase_client
from src.canonicalize.merge_loop import run_merge_loop
sb = get_supabase_client()
counts = run_merge_loop(supabase=sb, dry_run=False, persist_run_stats=True)
print(counts)
"
```

### Full pipeline (crawl + canonicalize)

```bash
python -m src.pipeline
```

---

## What to check in merge_run_stats

After a run with `persist_run_stats=True`, query the latest row:

```sql
SELECT *
FROM merge_run_stats
ORDER BY started_at DESC
LIMIT 1;
```

### Key columns

| Column | What "good" looks like |
|--------|----------------------|
| `finished_at` | Non-null (run completed) |
| `source_rows_processed` | Matches `queued` count |
| `canonical_created` | > 0 on first runs |
| `canonical_merged` | > 0 on subsequent runs |
| `canonical_review` | Low (< 5% of processed) |
| `errors` | 0 |
| `canonical_updates_count` | > 0 when merging into existing happenings |
| `history_rows_created` | > 0 when fields changed on merge |
| `stage_timings_ms` | JSON with `total_processing_ms` |
| `confidence_min` | 0.0–1.0 (null if no scored rows) |
| `confidence_avg` | 0.0–1.0 (null if no scored rows) |
| `confidence_max` | 0.0–1.0 (null if no scored rows) |
| `confidence_histogram` | `{"0_50":N, "50_70":N, ...}` |
| `source_breakdown` | `{"source_id": {"created":N, "merged":N, ...}}` |
| `source_confidence` | `{"source_id": {"min":F, "avg":F, "max":F, "hist":{...}}}` |

### Example: check confidence distribution

```sql
SELECT
  confidence_min,
  confidence_avg,
  confidence_max,
  confidence_histogram
FROM merge_run_stats
ORDER BY started_at DESC
LIMIT 5;
```

### Example: per-source breakdown

```sql
SELECT
  started_at,
  source_breakdown
FROM merge_run_stats
WHERE source_breakdown IS NOT NULL
ORDER BY started_at DESC
LIMIT 3;
```

---

## Troubleshooting

### "Why is `history_rows_created` always 0?"

- **First run**: All rows are `created` (not merged), so no field diffs occur.
  History rows only appear when a source row merges into an existing happening
  and at least one field value differs.
- **Subsequent runs**: If the source data hasn't changed, dedupe_key matches
  but field values are identical, so no history rows are written.
- **Check**: Run with a source that has updated data (e.g., changed title).

### "Why is `confidence_*` null?"

- Confidence telemetry is only recorded for rows that go through
  `decide_match()` with at least one candidate happening.
- If all rows are `created` (no existing candidates), no confidence scores
  are produced, and the fields remain null.
- **Check**: Ensure there are existing canonical happenings in the date range
  being processed, so the matcher has candidates to score.

### "Why is `canonical_review` high?"

- Review is triggered when the top confidence score is between
  `CONFIDENCE_THRESHOLD` (0.85) and near-tie conditions, or when
  multiple candidates score within `NEAR_TIE_DELTA` (0.03) of each other.
- A high review rate usually means source data is ambiguous (similar
  titles, overlapping dates, same venue).
- **Action**: Check the `canonical_review_queue` for patterns.

### "What indicates sequential scan risk?"

- See [docs/explain_checklist.md](explain_checklist.md) for the 5 hot-path
  EXPLAIN queries.
- Run each query with `EXPLAIN (ANALYZE, BUFFERS)` and look for:
  - `Seq Scan` on tables with > 1000 rows
  - `Rows Removed by Filter` much larger than `Rows` returned
  - Missing index usage where one is expected
- Migration 015 adds the necessary indexes. Verify they exist:

```sql
SELECT indexname FROM pg_indexes
WHERE schemaname = 'public'
  AND indexname LIKE 'idx_%'
ORDER BY indexname;
```

### "Run completed but `errors` > 0"

- Check application logs for stack traces.
- Errors are counted per source row. A single bad row doesn't stop the loop.
- Common causes: missing required fields (`title_raw`, `start_date_local`),
  malformed dates, or Supabase timeout on individual upsert.
