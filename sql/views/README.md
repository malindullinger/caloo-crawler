# sql/views/ — Supabase View Mirror

> Last updated: 2026-02

## Purpose

This directory contains the SQL definitions of all Supabase views that
affect user-visible behavior. Per `DECISIONS.md` rule 2:

> If a Supabase view is updated, its SQL must be copied into this repo.

The production Supabase database is the runtime source; these files are
the **version-controlled mirror** so that behavior changes are
reproducible from Git.

## Current views

| File | Supabase view | Role |
|------|---------------|------|
| `feed_cards_view.sql` | `public.feed_cards_view` | Single authoritative feed contract (LOCKED) |

## Update checklist

When you change a view in Supabase:

1. **Apply** the new SQL in Supabase SQL Editor (or via migration).
2. **Export** the updated definition:
   ```sql
   SELECT pg_get_viewdef('public.<view_name>', true);
   ```
3. **Paste** the output into the corresponding `.sql` file in this directory.
4. **Add the header comment** (source of truth, last-updated date, purpose note).
5. **Commit** in the same PR as the change that motivated the update.
6. **Verify** `feed_cards_view` still works for anon/incognito users after changes.

## Rules

- Frontend reads ONLY `public.feed_cards_view`. No other feed views allowed.
- Do NOT create versioned views (`_v2`, `_v3`, etc.) — update in place.
- Every view file must start with a header comment noting the source of
  truth and last-updated date.
