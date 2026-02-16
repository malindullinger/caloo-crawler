# Schema — Current State

> Last updated: 2026-02

## Purpose

This document explains where the canonical schema definition lives
and how `docs/schema-v1.md` relates to the current database state.

**The canonical schema is defined by the migrations directory.**
This document is an index, not a re-documentation of every column.

---

## Where to find the schema

| Source | Purpose |
|--------|---------|
| `migrations/001_*.sql` – `028_*.sql` | Canonical DDL: tables, columns, constraints, indexes, functions, views. Apply in order. |
| `docs/schema-v1.md` | Original v1 schema design document. Covers the base tables. Does NOT include columns/tables added by migrations 008+. |
| Supabase Dashboard > Database > Tables | Live schema (runtime truth). |

To get the live schema for any table:

```sql
SELECT column_name, data_type, is_nullable, column_default
FROM information_schema.columns
WHERE table_schema = 'public'
  AND table_name = '<table>'
ORDER BY ordinal_position;
```

---

## Core tables (from schema-v1)

These tables are documented in [schema-v1.md](schema-v1.md):

| Table | Role | Origin |
|-------|------|--------|
| `happening` | Canonical identity (title, description, visibility) | Pre-existing + migration 017 |
| `offering` | Schedule container (date range, recurrence) | Pre-existing |
| `occurrence` | Concrete dated instance (start_at, end_at) | Pre-existing |
| `organizer` | Entity running a happening/course | Pre-existing + migrations 004, 024 |
| `source_happenings` | Raw ingestion records | Migration 001 |
| `happening_sources` | Provenance join (source → canonical) | Migration 002 |
| `happening_field_provenance` | Per-field audit trail | Migration 003 |
| `courses` | Canonical course records | Migration 005 |
| `source_courses` | Raw course records | Migration 006 |
| `course_sessions` | Dated course instances | Migration 007 |

---

## Additions beyond schema-v1

These were added by later migrations and are NOT in `schema-v1.md`:

### Columns added to existing tables

| Migration | Table | Columns added |
|-----------|-------|---------------|
| 008 | `source_happenings`, `course_sessions` | CHECK constraint `*_time_contract` (date_precision coherence) |
| 009 | `source_happenings` | `start_date_local DATE`, `end_date_local DATE`, CHECK `start_date_local_required` |
| 014 | `merge_run_stats` | `canonical_updates_count`, `history_rows_created`, `source_breakdown JSONB`, `stage_timings_ms JSONB` |
| 016 | `merge_run_stats` | `confidence_min`, `confidence_avg`, `confidence_max`, `confidence_histogram JSONB`, `source_confidence JSONB` |
| 017 | `happening` | `audience_tags text[]`, `topic_tags text[]`, `editorial_priority int` |
| 018 | `source_happenings`, `source_courses` | `dedupe_key NOT NULL` constraint, UNIQUE index `(source_id, dedupe_key)` |
| 019 | `happening` | `relevance_score_global INT NOT NULL DEFAULT 0` |
| 024 | `organizer` | `website_url TEXT` |
| 026 | `happening` | `confidence_score INT NOT NULL DEFAULT 100` (data-quality signal, not a feed filter) |

### New tables

| Migration | Table | Purpose |
|-----------|-------|---------|
| 010 | `ambiguous_match_log` | Review queue for ambiguous merge decisions |
| 013 | `canonical_field_history` | Field-level change audit trail |
| 014 | `merge_run_stats` (if not pre-existing) | Per-run observability counters |
| 016 | `canonical_review_outcomes` | Review resolution audit trail |

### Indexes (migration 015)

| Index | Table | Columns |
|-------|-------|---------|
| `idx_sh_status_created` | `source_happenings` | `(status, created_at)` |
| `idx_sh_source_date` | `source_happenings` | `(source_id, start_date_local)` |
| `idx_offering_date_range` | `offering` | `(start_date, end_date)` |
| `idx_occurrence_offering` | `occurrence` | `(offering_id)` |
| `idx_merge_run_stats_started` | `merge_run_stats` | `(started_at DESC)` |

### Functions

| Migration | Function | Purpose |
|-----------|----------|---------|
| 014 | `insert_field_history_batch(JSONB)` | Batch insert to `canonical_field_history` with `ON CONFLICT (change_key) DO NOTHING` |
| 020 | `next_weekend_with_events(timestamptz, int)` | Find next weekend with eligible events (same eligibility as feed_cards_view) |

### Data fixes

| Migration | Scope | Purpose |
|-----------|-------|---------|
| 021 | `happening` | Publish draft happenings that have `happening_sources` links (one-time fix for visibility_status bug) |
| 022 | `source_happenings` | Correct `source_tier` to match documented tiering: maennedorf_portal → B, elternverein_uetikon → B, eventbrite_zurich → A |

### Views

| Migration | View | Purpose |
|-----------|------|---------|
| 023 | `occurrence_detail_view` | Detail page data contract: 1-row-per-occurrence enrichment with happening/offering/organizer context, best-source image/description, and up to 5 other upcoming occurrences |
| 024 | `occurrence_detail_view` (updated) | Adds `organizer_website_url` column; also adds `website_url` to `organizer` table |
| 025 | `system_integrity_view` | Read-only diagnostics: 8 health checks (orphans, negative durations, missing timezones, provenance gaps, count drift) |
| 027 | `system_integrity_view` (updated) | Extends to 10 checks: adds `low_confidence_happenings` and `tier_b_without_image_ratio` |
| 028 | `low_confidence_dashboard_view` | Admin diagnostics: published happenings with confidence signals, sorted worst-first |

---

## How to apply all migrations

See [db_migration_apply_checklist.md](db_migration_apply_checklist.md)
for step-by-step instructions and post-apply verification queries.

---

## Updating this document

When a new migration is added:
1. Add its table/column changes to the appropriate section above.
2. Do NOT duplicate full column definitions — the migration SQL is
   the source of truth.
3. Keep this document as an index, not a replacement for the migrations.
