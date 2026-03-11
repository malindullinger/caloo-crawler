# Legacy Migration Scripts (Archived)

These files are **not part of the active runtime**. They were one-time migration
and testing tools used during early Phase 1 (Feb 2026) and have been superseded
by the canonical pipeline and admin tooling.

## Files

| File | Original location | Purpose | Superseded by |
|------|-------------------|---------|---------------|
| `bridge_eventbrite_to_canonical.py` | `src/jobs/` | One-shot migration from old `events` table to `happening`/`offering`/`occurrence` for Eventbrite source | Canonical transform pipeline + Event Control UI mutations |
| `bridge_maennedorf_to_canonical.py` | `src/jobs/` | Same for Maennedorf Tier B source | Same |
| `supabase_test.py` | project root | Minimal smoke test inserting a row into `event_raw` | Validation harness (`npm run validate` in caloo) |

## Why archived (not deleted)

These scripts contain source-specific mapping logic that may be useful as
reference if similar migrations are needed in the future. They should **not**
be re-run against production — they bypass the review workflow by hardcoding
`visibility_status = "published"` and do not create `happening_sources` rows
(provenance violation).

Archived: 2026-03-11
