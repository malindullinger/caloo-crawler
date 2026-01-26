# Project Decisions (Source of Truth)

## Decision: GitHub is the source of truth
**All behavior changes must be committed to this repo.**

### What lives where
- **GitHub (this repo):** pipeline logic (scrape → normalize → schedules → write to DB)
- **Supabase:** data storage + *thin* views for consumption (no “business logic” that only exists in SQL)
- **Lovable:** UI only (no hidden rules that change what data means)

### Rules of thumb
1. If it changes *what users see* → it must be reproducible from Git.
2. If a Supabase view is updated → its SQL must be copied into this repo (e.g. `sql/views/...`).
3. Lovable should read from a stable view name (e.g. `this_weekend_events`) so the UI doesn’t churn.
