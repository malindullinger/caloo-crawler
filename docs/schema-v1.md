# Schema v1 — Caloo Ingestion System

## Overview

This document describes the target schema for the Caloo ingestion system. Tables are additive to the existing schema.

---

## Table Diagram

```
                     ┌─────────────────┐
                     │   organizer     │
                     │ (existing+cols) │
                     └────────┬────────┘
                              │
         ┌────────────────────┼────────────────────┐
         │                    │                    │
         ▼                    ▼                    ▼
┌─────────────────┐  ┌─────────────────┐  ┌─────────────────┐
│ source_happenings│  │    happening    │  │     courses     │
│     (NEW)       │  │   (existing)    │  │      (NEW)      │
└────────┬────────┘  └────────┬────────┘  └────────┬────────┘
         │                    │                    │
         │  ┌─────────────────┤                    │
         │  │                 │                    │
         ▼  ▼                 ▼                    ▼
┌─────────────────┐  ┌─────────────────┐  ┌─────────────────┐
│happening_sources│  │    offering     │  │ course_sessions │
│     (NEW)       │  │   (existing)    │  │      (NEW)      │
└─────────────────┘  └────────┬────────┘  └─────────────────┘
                              │
                              ▼
                     ┌─────────────────┐
                     │   occurrence    │
                     │   (existing)    │
                     └─────────────────┘
```

---

## New Tables

### source_happenings
Raw happening records from crawlers, manual input, or partner feeds.

| Column | Type | Constraints | Description |
|--------|------|-------------|-------------|
| id | UUID | PK | Primary key |
| source_id | TEXT | NOT NULL | Source identifier (e.g., "maennedorf_portal") |
| source_type | TEXT | NOT NULL, CHECK | crawler, internal_manual, partner_feed |
| source_tier | TEXT | NOT NULL, CHECK | A, B, C |
| external_id | TEXT | nullable | Stable ID from source (NULL for manual) |
| title_raw | TEXT | | Raw title as extracted |
| datetime_raw | TEXT | | Raw datetime string |
| location_raw | TEXT | | Raw location string |
| description_raw | TEXT | | Raw description |
| date_precision | TEXT | CHECK | datetime, date |
| start_at | TIMESTAMPTZ | | Parsed start (UTC) |
| end_at | TIMESTAMPTZ | | Parsed end (UTC) |
| timezone | TEXT | DEFAULT | Europe/Zurich |
| extraction_method | TEXT | CHECK | jsonld, time_element, text_heuristic |
| item_url | TEXT | | Source page URL |
| content_hash | TEXT | | For change detection |
| dedupe_key | TEXT | | Generated fingerprint for matching |
| status | TEXT | | pending, processed, error |
| error_message | TEXT | | Error details if failed |
| fetched_at | TIMESTAMPTZ | | When fetched from source |
| created_at | TIMESTAMPTZ | DEFAULT now() | |
| updated_at | TIMESTAMPTZ | DEFAULT now() | |

**Unique Constraints:**
- `(source_id, external_id)` WHERE external_id IS NOT NULL
- `(source_id, content_hash)` for manual records

### happening_sources
Provenance: links canonical happenings to contributing sources.

| Column | Type | Constraints | Description |
|--------|------|-------------|-------------|
| id | UUID | PK | |
| happening_id | UUID | FK → happening | |
| source_happening_id | UUID | FK → source_happenings | |
| source_priority | INT | NOT NULL DEFAULT 0 | Higher = more trusted |
| is_primary | BOOLEAN | DEFAULT false | Primary source for this happening |
| merged_at | TIMESTAMPTZ | DEFAULT now() | |

### happening_field_provenance
Field-level audit trail.

| Column | Type | Constraints | Description |
|--------|------|-------------|-------------|
| id | UUID | PK | |
| happening_id | UUID | FK → happening | |
| source_happening_id | UUID | FK (nullable) | May be NULL if manually set |
| field_name | TEXT | NOT NULL | title, start_at, location_name, etc. |
| value_hash | TEXT | | Hash for change detection |
| precedence_rank | INT | NOT NULL DEFAULT 0 | |
| updated_at | TIMESTAMPTZ | DEFAULT now() | |

### courses
Canonical course records (separate from happenings).

| Column | Type | Constraints | Description |
|--------|------|-------------|-------------|
| id | UUID | PK | |
| public_id | TEXT | UNIQUE | |
| title | TEXT | NOT NULL | |
| description | TEXT | | |
| organizer_id | UUID | FK → organizer | |
| audience_type | TEXT | | |
| age_min | INT | | |
| age_max | INT | | |
| venue_id | UUID | | Default venue |
| location_name | TEXT | | Default location text |
| start_date | DATE | | Course start date |
| end_date | DATE | | Course end date |
| timezone | TEXT | DEFAULT | Europe/Zurich |
| visibility_status | TEXT | DEFAULT 'draft' | draft, published, archived |
| created_at | TIMESTAMPTZ | DEFAULT now() | |
| updated_at | TIMESTAMPTZ | DEFAULT now() | |

### source_courses
Raw course records from sources.

| Column | Type | Constraints | Description |
|--------|------|-------------|-------------|
| id | UUID | PK | |
| source_id | TEXT | NOT NULL | |
| source_type | TEXT | NOT NULL, CHECK | |
| source_tier | TEXT | NOT NULL, CHECK | |
| external_id | TEXT | nullable | |
| title_raw | TEXT | | |
| description_raw | TEXT | | |
| schedule_raw | TEXT | | |
| location_raw | TEXT | | |
| extraction_method | TEXT | CHECK | |
| item_url | TEXT | | |
| content_hash | TEXT | | |
| dedupe_key | TEXT | | |
| status | TEXT | | |
| error_message | TEXT | | |
| fetched_at | TIMESTAMPTZ | | |
| created_at | TIMESTAMPTZ | DEFAULT now() | |
| updated_at | TIMESTAMPTZ | DEFAULT now() | |

### course_sessions
Specific dated instances of a course.

| Column | Type | Constraints | Description |
|--------|------|-------------|-------------|
| id | UUID | PK | |
| course_id | UUID | FK → courses | NOT NULL |
| session_index | INT | | 1, 2, 3... |
| start_at | TIMESTAMPTZ | | |
| end_at | TIMESTAMPTZ | | |
| date_precision | TEXT | CHECK | datetime, date |
| venue_id | UUID | | Override course venue |
| location_name | TEXT | | Override course location |
| status | TEXT | DEFAULT 'scheduled' | |
| notes | TEXT | | |
| created_at | TIMESTAMPTZ | DEFAULT now() | |
| updated_at | TIMESTAMPTZ | DEFAULT now() | |

---

## Modified Tables

### organizer (existing)
New columns added:

| Column | Type | Description |
|--------|------|-------------|
| legal_form | TEXT | Swiss legal form (see below) |
| priority_score | INT | For feed ordering (higher = preferred) |
| locality | TEXT | Geographic region |

**Expected legal_form values (free text):**
- AG (Aktiengesellschaft)
- GmbH (Gesellschaft mit beschränkter Haftung)
- Verein (Association)
- Stiftung (Foundation)
- Genossenschaft (Cooperative)
- Einzelunternehmen (Sole proprietorship)
- public (Government/public institution)
- other

---

## CHECK Constraints

All enum-like TEXT fields have CHECK constraints:

```sql
source_type IN ('crawler', 'internal_manual', 'partner_feed')
source_tier IN ('A', 'B', 'C')
extraction_method IN ('jsonld', 'time_element', 'text_heuristic')
date_precision IN ('datetime', 'date')
```

---

## Time Contract (CRITICAL)

The `date_precision` field enforces the unknown-time contract:

- `date_precision = 'datetime'` → `start_at` IS NOT NULL (meaningful time)
- `date_precision = 'date'` → `start_at` IS NULL AND `end_at` IS NULL

**Never use `00:00` as a placeholder for unknown time.**

This is enforced at the DB level via CHECK constraints. See `docs/invariants.md` for:
- Constraint definitions
- Verification test cases (failing + passing inserts)

---

## Indexes

Key indexes for performance:

```sql
-- source_happenings
idx_source_happenings_source_id (source_id)
idx_source_happenings_status (status)
idx_source_happenings_fetched (fetched_at DESC)
idx_source_happenings_tier (source_tier)
idx_source_happenings_content_hash (content_hash)
idx_source_happenings_item_url (item_url)
idx_source_happenings_dedupe (dedupe_key)

-- happening_sources
idx_happening_sources_happening (happening_id)
idx_happening_sources_source (source_happening_id)

-- courses
idx_courses_organizer (organizer_id)
idx_courses_visibility (visibility_status)

-- course_sessions
idx_course_sessions_course (course_id)
idx_course_sessions_start (start_at)

-- organizer
idx_organizer_priority (priority_score DESC NULLS LAST)
idx_organizer_locality (locality)
```
