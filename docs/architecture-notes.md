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

## 8. Guardrails going forward
- No versioned views
- No parallel feed logic
- No quick fixes that bypass architecture
- One canonical source, one mental model, one contract

## 9. Deferred decisions (explicit)
- Series semantics
- Event vs Activity vs Happening taxonomy
- Dedupe migration to SQL
- Commercial fields (price, booking, images)
- Auth/personalization differences
