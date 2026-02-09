# Caloo — Canonical Project Instructions (Read First)

## Prime directive
- Maintain ONE canonical feed contract.
- Do not add versioned views, parallel feed logic, or "quick fixes".
- If a request conflicts with the canonical architecture or PRODUCT.md, STOP and ask for clarification or propose an architecture-aligned alternative.

## Product authority
PRODUCT.md defines Caloo's product intent, behavioral framing, principles, and accessibility constraints.

All planning, implementation, and verification must align with PRODUCT.md.
If a proposed change conflicts with PRODUCT.md, the agent must stop and ask for clarification before proceeding.

## Product principles as constraints
The product principles in PRODUCT.md are not aspirational.
They are decision constraints and trade-off guardrails.

When multiple valid implementation paths exist, the agent must:
- explicitly consider and reference relevant product principles
- prefer solutions that reduce cognitive load, increase clarity, and support accessibility
- avoid adding complexity unless it is clearly justified by a concrete user need

## Stop conditions
The agent must stop and ask the user for clarification before proceeding if:
- a change could materially affect product intent, behavioral framing, or accessibility
- a decision touches areas marked as "explicitly deferred" in PRODUCT.md
- the correct trade-off between simplicity and completeness is unclear
- a change risks increasing cognitive load or excluding users with accessibility needs
- the change would introduce a new feed view or parallel feed logic (explicit approval required)

## Canonical data source (LOCKED)
Frontend may query ONLY:
- `public.feed_cards_view`

Other views that exist are not feed sources:
- `public.source_document_latest` (crawler/source support only)

Do NOT create or revive any:
- `events_lovable_v*`, `events_lovable_cards_v*`, `this_weekend_*`, `event_feed_cards`, `public_events_v*`, etc.

## What a feed row means
- 1 row in `feed_cards_view` = 1 display card candidate ("one idea" a parent sees)
- Raw rows may exceed visible cards due to frontend dedupe.

## Timezone & time handling (LOCKED)
- Timezone: `Europe/Zurich`
- Weekend window:
  - Start = Thursday (week start + 4 days)
  - End = Sunday (week start + 7 days)
- `section_key ∈ { weekend, coming_up }`
- Ongoing logic:
  - Only if `start_at` AND `end_at` exist
  - duration ≤ 12 hours
  - now between start_at and end_at

## Unknown time handling (CRITICAL)
If crawler cannot determine a real time:
- `date_precision = 'date'`
- `start_time_local` / `end_time_local` = NULL
- UI must show date only
- Never show misleading `00:00`

This is enforced in both SQL view + frontend mapping. If any layer violates this, treat it as a bug to fix (not a UI workaround).

## Deduplication ownership (CURRENT)
- Frontend dedupe is canonical for now.
- Location: `useHappeningFeedCards` (alias: `useActivities`)
- Rules:
  - Prefer `series_label` when present
  - Else key: title + location + date (+ time only if meaningful)
  - Prefer rows with: meaningful times (not midnight fallback), newer `updated_at`

SQL DISTINCT-ON experiments may exist but are NOT authoritative until explicitly decided.

## Public access
- Incognito access to feed is expected and correct (public read contract).

## What is explicitly deferred (do NOT implement yet)
- Final "series" semantics (series_label, upcoming index)
- Parent mental model: Event vs Activity vs Happening
- Moving dedupe fully to SQL
- Pricing / booking links / images
- Auth vs public content differences
- Monetization/business model (must not influence current architecture)

## Work method
Always follow:
1) Plan (write steps + checks and reference relevant PRODUCT principles)
2) Implement minimal changes
3) Verify with checklist (SQL + UI)
4) Summarize what changed + what was NOT changed

When touching SQL/view logic:
- Ensure `feed_cards_view` stays stable and backward-compatible
- Add checks/queries for edge cases (unknown time, ongoing, weekend window)
- Do not introduce new feed views without explicit user approval
