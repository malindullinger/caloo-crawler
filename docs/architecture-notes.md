# Caloo — Data Architecture Notes (Canonical)

## 1. Purpose
Caloo is a family-focused web app helping parents discover activities & events for weekends.

## 2. Canonical feed contract (LOCKED)
Frontend is allowed to read from exactly one feed view:
- `public.feed_cards_view`

Support/aux view:
- `public.source_document_latest`

All legacy/experimental/versioned feed views have been removed by design.

## 3. Feed semantics
### 3.1 One row = one card candidate
A row from `feed_cards_view` represents a single “display card candidate” (a user-visible idea).
Raw rows can map to fewer cards due to frontend dedupe.

### 3.2 Display fields
The view provides:
- timezone normalization (`Europe/Zurich`)
- `section_key` (weekend vs coming_up)
- “ongoing” classification (strict rules)
- safe handling of missing times
- display labels (e.g. `display_kind`, `display_when`)

## 4. Time model (LOCKED)
### 4.1 Timezone
All feed logic assumes `Europe/Zurich`.

### 4.2 Weekend window
- Start: Thursday
- End: Sunday
- `section_key ∈ { weekend, coming_up }`

### 4.3 Ongoing logic
An item is "ongoing" only if:
- `start_at` AND `end_at` exist
- duration ≤ 12 hours
- now is between start_at and end_at

## 5. Unknown/missing time handling (CRITICAL)
If time is unknown:
- `date_precision = 'date'`
- `start_time_local` / `end_time_local` are NULL
- UI must show date only
- never show `00:00` placeholders

This is enforced both in SQL view and frontend mapping.

## 6. Deduplication (CURRENT STATE)
Two-layer approach (intentional):

### 6.1 Frontend dedupe (authoritative for now)
Location:
- `useHappeningFeedCards` (alias: `useActivities`)

Card key preference:
1) `series_label` (when present)
2) else: title + location + date (+ time only if meaningful)

Row preference:
- real times > midnight fallback
- newer `updated_at`

Expected debug behavior:
- raw rows > deduped rows = mapped cards

### 6.2 SQL dedupe (non-authoritative experiments)
Some DISTINCT-ON experiments may exist but do not own dedupe until a deliberate decision is made.

## 7. Current verified state
- `count(*) from feed_cards_view` matches expected raw rows
- Section split verified (coming_up vs weekend)
- Frontend cards rendered ~ deduped size
- No duplicates
- Public access works (incognito)

## 8. Weekend-empty UX behavior

> Last updated: 2026-02

### 8.1 Problem

The weekend section (`section_key = 'weekend'`) may return zero rows
when no published happenings have occurrences in the Thu–Sun window.
The feed must handle this gracefully.

### 8.2 Behavior specification (default)

| Condition | Weekend section | Coming-up section |
|-----------|-----------------|-------------------|
| Weekend has cards | Show "Dieses Wochenende" header + cards normally | Show "Demnächst" header + cards normally |
| Weekend empty, coming_up has cards | Show friendly empty-state message + optional CTA ("Schau mal unter Demnächst") | Render as usual — no promotion, no change |
| Both sections empty | Global empty state ("We're gathering new ideas — check back soon") | *(covered by global state)* |

**Key rule:** When weekend is empty, the "Coming up" section renders
exactly as it would if the weekend section had cards. There is no
hidden promotion of coming_up items into the weekend section. The
weekend section shows an empty state; coming_up stays below it
unchanged. This keeps the contract stable and avoids implicit
re-sectioning logic.

### 8.3 Empty-state copy intent

The empty-state message must:
- Be calm, not alarming ("Nothing this weekend yet" not "No events found!")
- Optionally suggest looking at "Coming up" section
- Never pressure the user (aligns with PRODUCT.md principle: reduce cognitive load)

Exact copy is a UI concern. Intent is documented here so backend and
frontend stay aligned.

### 8.4 "Show next weekend with events" — SQL helper

When the current weekend is empty, the UI may offer to show the next
weekend that has events. A lightweight SQL function supports this:

**Function:** `public.next_weekend_with_events(after_ts, max_weeks_ahead)`

- Defined in `migrations/020_next_weekend_with_events.sql`
- **Not a feed view.** Returns one row: `(weekend_start_zh, weekend_end_zh, event_count)`.
- Uses the **same eligibility rules** as `feed_cards_view` (published + scheduled + start_at not null + valid range + not past), except the "not in the past" check uses `after_ts` instead of `now()`. When called with the default `after_ts = now()`, behavior is identical to the feed.
- Uses the **same weekend window** as `feed_cards_view` (Friday 00:01 → Monday 00:00, Europe/Zurich).
- Searches forward week by week until `event_count > 0` or `max_weeks_ahead` reached.
- If no events found in the horizon, returns the first candidate weekend with `event_count = 0`.

**Usage:**

```sql
-- Default: search from now, up to 26 weeks ahead
SELECT * FROM next_weekend_with_events();

-- Custom: search from a specific timestamp, max 8 weeks
SELECT * FROM next_weekend_with_events('2026-03-10T12:00:00+01:00'::timestamptz, 8);
```

**Frontend integration:** The frontend calls this function (via Supabase RPC)
when the weekend section is empty. The returned `weekend_start_zh` /
`weekend_end_zh` are used to display "Next weekend with events: [date]"
in the empty-state CTA. The feed itself still comes from `feed_cards_view`.

### 8.5 Verification SQL

Three cases to verify correct behavior after applying migration 020:

```sql
-- Case A: Current weekend has events
-- Expected: returns current weekend range with event_count > 0
SELECT * FROM next_weekend_with_events();

-- Case B: Current weekend empty, future weekend has events
-- Use a timestamp on a Monday (outside any weekend) to simulate
-- empty current weekend — should skip ahead to next weekend with events
SELECT * FROM next_weekend_with_events(
  (date_trunc('week', now() AT TIME ZONE 'Europe/Zurich')
   + interval '1 day')  -- Tuesday of this week
  AT TIME ZONE 'Europe/Zurich'
);

-- Case C: No events in horizon → returns event_count = 0
-- Use a very short horizon to force this case
SELECT * FROM next_weekend_with_events(now(), 1);
-- If no events this weekend or next: event_count = 0

-- Verify eligibility matches feed_cards_view:
-- The event_count for the current weekend should match:
SELECT count(*) FROM feed_cards_view WHERE section_key = 'weekend';
-- vs:
SELECT event_count FROM next_weekend_with_events();
-- These two counts should be equal when called at the same moment.
```

### 8.6 Ownership

| Concern | Owner |
|---------|-------|
| Section assignment logic | Backend (`feed_cards_view`) |
| Empty-state rendering | Frontend |
| "Next weekend" lookup | Backend (`next_weekend_with_events`) |
| "Next weekend" navigation | Frontend (using RPC result + existing feed data) |
| Empty-state copy / UX | Product / Design |

**Constraint:** No new feed views may be created to solve the
empty-weekend problem. The `feed_cards_view` contract is the only
feed source. `next_weekend_with_events` is a helper function, not a view.

---

## 9. Detail page enrichment

> Added: 2026-02

The detail page uses `public.occurrence_detail_view` (migration 023)
for enrichment beyond the feed card. This is NOT a feed view — the
feed stays on `feed_cards_view` exclusively.

- **Query:** `SELECT * FROM occurrence_detail_view WHERE occurrence_id = :id`
- **Adds:** description, organizer info, image_url, other_occurrences (JSONB), tags
- **Reuses** the same `best_source` CTE as `feed_cards_view` for deterministic
  image/URL/description selection
- **Invariants:** same unknown-time rules, same timezone, published-only filter
- See [detail-page-contract.md](detail-page-contract.md) for field mapping

---

## 10. Guardrails going forward
- No versioned views
- No parallel feed logic
- No quick fixes that bypass architecture
- One canonical source, one mental model, one contract

## 11. Deferred decisions (explicit)
- Series semantics
- Event vs Activity vs Happening taxonomy
- Dedupe migration to SQL
- Commercial fields (price, booking, images)
- Auth/personalization differences

---

## 12. Cross-references

| Topic | Document |
|-------|----------|
| Feed view SQL | [../sql/views/feed_cards_view.sql](../sql/views/feed_cards_view.sql) |
| UI terminology | [ui-terminology.md](ui-terminology.md) |
| Detail page contract | [detail-page-contract.md](detail-page-contract.md) |
| Ranking layer | [ranking.md](ranking.md) |
| Feed health checks | [feed-health-checks.md](feed-health-checks.md) |
| Tagging | [tagging.md](tagging.md) |
| Minimum Trust Standard | [minimum-trust-standard.md](minimum-trust-standard.md) |

---

# Phase 9 — Feed Integrity Layer

## Display Unit

One occurrence per offering (next eligible upcoming OR currently ongoing).

## Time Logic

- upcoming → now < start_at
- ongoing → start_at <= now < end_at
- ending_soon → end_at - now <= 20 minutes
- invisible → now >= end_at

An occurrence disappears the moment:
now >= end_at

## Eligibility Rule

Feed view must filter using:

WHERE trust_status != 'suppressed'
AND end_at > now()

## Series Handling

Only the next eligible occurrence per offering appears in the feed.

Future occurrence count is derived from feed-eligible future occurrences.

---
