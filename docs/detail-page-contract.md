# Detail Page Data Contract

> Last updated: 2026-02
>
> **This document is normative.** It defines what data the detail page
> requires, how it is fetched, and its relationship to the feed.

For the feed contract see [../CLAUDE.md](../CLAUDE.md).
For domain definitions see [glossary.md](glossary.md).
For UI label mapping see [ui-terminology.md](ui-terminology.md).

---

## Overview

The **detail page** shows the full information for a single occurrence
(session) of a happening (experience). It is reached by tapping a feed
card.

The feed (`feed_cards_view`) is intentionally lightweight. The detail
page joins additional tables via `occurrence_detail_view` to provide
richer context.

---

## Routing identifier

| Identifier | Source | Purpose |
|------------|--------|---------|
| `occurrence.id` | Primary key (UUID) | Canonical routing target for detail page |

The feed card carries the `occurrence.id` (or a stable public_id
derived from it). The detail page fetches by this identifier.

> **Why occurrence, not happening?**
> A happening may have multiple occurrences (different dates/times).
> Each feed card represents one occurrence. The detail page must show
> the specific date/time the user tapped on.

---

## SQL access pattern

The detail page queries `public.occurrence_detail_view` — a dedicated
enrichment view (NOT a feed view). Migration 023 creates it.

**Supabase JS:**

```js
const { data, error } = await supabase
  .from('occurrence_detail_view')
  .select('*')
  .eq('occurrence_id', occurrenceId)
  .single();
```

**Raw SQL equivalent:**

```sql
SELECT * FROM occurrence_detail_view
WHERE occurrence_id = :occurrence_id;
```

The view returns **exactly one row** per published, scheduled occurrence.
If the occurrence doesn't exist or the happening is unpublished, the
query returns no rows — the frontend shows an empty state.

**View SQL source:** `sql/views/occurrence_detail_view.sql`

---

## Field mapping

### Required fields

| View column | Source | Notes |
|-------------|--------|-------|
| `occurrence_id` | `occurrence.id` | Routing key (UUID) |
| `happening_id` | `happening.id` | Canonical identity |
| `happening_title` | `happening.title` | Display title |
| `canonical_url` | `best_source.item_url` | External link to source page |
| `start_at` | `occurrence.start_at` | TIMESTAMPTZ |
| `end_at` | `occurrence.end_at` | TIMESTAMPTZ; NULL if unknown |
| `timezone` | `offering.timezone` | Always `'Europe/Zurich'` |
| `date_precision` | Derived | `'datetime'` or `'date'` (same logic as feed) |
| `location_name` | venue → best_source → municipality fallback | Same COALESCE chain as feed |
| `image_url` | `best_source.image_url` | Best-source image; NULL if none |

### Optional fields (nullable)

| View column | Source | Notes |
|-------------|--------|-------|
| `description` | `happening.description` → `best_source.description_raw` | COALESCE; NULL if neither exists |
| `organizer_name` | `organizer.name` via `happening.organizer_id` | NULL if no organizer linked |
| `organizer_type` | `organizer.organizer_type` | `for_profit` / `non_profit` / `public` |
| `organizer_website_url` | `organizer.website_url` via `happening.organizer_id` | NULL if no organizer or no URL; empty strings normalized to NULL |
| `booking_url` | Deferred | Always NULL (no source field yet) |
| `other_occurrences` | JSONB array | Up to 5 upcoming occurrences in same offering |

### Context fields

| View column | Source | Notes |
|-------------|--------|-------|
| `offering_start_date` | `offering.start_date` | DATE; always present |
| `offering_end_date` | `offering.end_date` | DATE; may be NULL |
| `offering_type` | `offering.offering_type` | `one_off` / `series` / `recurring` |
| `happening_kind` | `happening.happening_kind` | `'event'` etc. |
| `visibility_status` | `happening.visibility_status` | Always `'published'` (view filters) |
| `audience_tags` | `happening.audience_tags` | text[]; may be NULL |
| `topic_tags` | `happening.topic_tags` | text[]; may be NULL |
| `editorial_priority` | `happening.editorial_priority` | INT; may be NULL |

### Time display fields

| View column | Source | Notes |
|-------------|--------|-------|
| `start_date_local` | Derived | DATE in Europe/Zurich |
| `end_date_local` | Derived | DATE in Europe/Zurich; NULL if no end_at |
| `start_time_local` | Derived | `'HH24:MI'` or NULL if date_precision = 'date' |
| `end_time_local` | Derived | `'HH24:MI'` or NULL |

---

## other_occurrences format

JSONB array of up to 5 upcoming occurrences in the same offering
(excluding the current occurrence and past events):

```json
[
  {"occurrence_id": "uuid-1", "start_at": "2026-03-22T10:00:00+01:00", "end_at": "2026-03-22T12:00:00+01:00"},
  {"occurrence_id": "uuid-2", "start_at": "2026-03-29T10:00:00+01:00", "end_at": null}
]
```

Empty array `[]` when there are no other upcoming occurrences.

---

## best_source selection (deterministic)

Both `feed_cards_view` and `occurrence_detail_view` use the same
`best_source` CTE for consistent image/URL/description selection:

```sql
SELECT DISTINCT ON (hs.happening_id)
    hs.happening_id, sh.item_url, sh.location_raw, sh.image_url, sh.description_raw
FROM happening_sources hs
JOIN source_happenings sh ON sh.id = hs.source_happening_id
ORDER BY hs.happening_id,
         hs.is_primary DESC NULLS LAST,
         hs.source_priority,
         hs.merged_at DESC NULLS LAST
```

Priority: primary source first → highest source_priority → most recently merged.

---

## Time eligibility for detail routing

**Published, user-facing detail pages (v1):** `start_at` is required.
Unknown-time occurrences are not eligible for detail routing because
they do not appear in `feed_cards_view` (no occurrence row exists for
date-only items). Users cannot tap a card that doesn't exist.

**Admin / debug tooling:** Detail pages MAY exist for unknown-time
happenings (routed by `happening.public_id` via offering). These MUST
render a date-only state and never invent time. The `offering.start_date`
provides the date; time fields remain NULL.

---

## Relationship to feed_cards_view

| Concern | Feed (card) | Detail page |
|---------|-------------|-------------|
| Data source | `feed_cards_view` only | `occurrence_detail_view` |
| Data density | Lightweight (title, time, location, section) | Full (description, organizer, tags, other dates) |
| New views allowed? | NO — single feed contract (LOCKED) | `occurrence_detail_view` is the detail contract |
| Filtering | Section + weekend window + published + scheduled + future | Single item by occurrence_id; no time filter |
| best_source CTE | Yes | Yes (identical) |

---

## Invariants (apply to detail page too)

1. **No inference / never invent data.**
   If `date_precision = 'date'`, the detail page must NOT show a time.
   Never display `00:00` as a placeholder.

2. **Timezone is `Europe/Zurich`.**
   All displayed times are in Zurich local time.

3. **Published only.**
   The detail page must not render draft or archived happenings.
   If a happening is unpublished between card tap and detail load,
   show an appropriate empty state.

---

## Lovable integration

### Query

The detail page fetches from `occurrence_detail_view`, not from
`feed_cards_view`. The feed remains the sole feed source (LOCKED).

```ts
const { data, error } = await supabase
  .from('occurrence_detail_view')
  .select('*')
  .eq('occurrence_id', occurrenceId)
  .single();
```

The `occurrenceId` comes from the feed card's `external_id` field
(which is `occurrence.id::text`).

### Handling no result (404 / empty state)

When `.single()` returns no data (the occurrence doesn't exist,
or the happening was unpublished between card tap and detail load):

```ts
if (error || !data) {
  // Show friendly empty state:
  // "This event is no longer available."
  // Do NOT retry or show a loading spinner indefinitely.
  return;
}
```

### Rendering optional blocks safely

All optional fields may be NULL. Guard before rendering:

```ts
// Description (may be NULL)
{data.description && <p>{data.description}</p>}

// Organizer (may be NULL)
{data.organizer_name && (
  <span>
    By {data.organizer_website_url
      ? <a href={data.organizer_website_url}>{data.organizer_name}</a>
      : data.organizer_name}
  </span>
)}

// Image (may be NULL)
{data.image_url && <img src={data.image_url} alt={data.happening_title} />}

// Other dates (always an array, may be empty)
{data.other_occurrences.length > 0 && (
  <section>
    <h3>Other dates</h3>
    {data.other_occurrences.map(occ => (
      <a key={occ.occurrence_id} href={`/detail/${occ.occurrence_id}`}>
        {formatDate(occ.start_at)}
      </a>
    ))}
  </section>
)}
```

### Time display rules

Follow the same rules as the feed card:

```ts
if (data.date_precision === 'date') {
  // Show date only (e.g. "Sat 22 Mar")
  // NEVER show "00:00" or any time
} else {
  // Show date + time (e.g. "Sat 22 Mar · 10:00")
  // Use start_time_local / end_time_local (already formatted as "HH:MM")
}
```

### Booking URL (deferred)

`booking_url` is always NULL in the current schema. When it becomes
available, render it as an external link button. Do not add a
placeholder or empty link.

---

## Validation queries

See [detail-page-validation.md](detail-page-validation.md) for 5
copy-pasteable SQL queries to verify the view after applying
migration 023.

---

## Explicitly deferred

| Feature | Status |
|---------|--------|
| Booking / external link | `booking_url` column exists but always NULL |
| Social proof / attendance signals | Deferred (see PRODUCT.md §10) |
| Related happenings (cross-offering) | Not implemented |
| Venue address / coordinates | Deferred; venue table not yet populated |
