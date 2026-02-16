# UI Terminology Mapping

> Last updated: 2026-02
>
> **This document is normative.** It maps canonical backend terms to
> user-facing UI labels and vice versa. DB column and table names do
> NOT change — this mapping is a presentation-layer concern only.

For canonical domain definitions see [glossary.md](glossary.md).
For the feed data contract see [../CLAUDE.md](../CLAUDE.md) and
[../sql/views/feed_cards_view.sql](../sql/views/feed_cards_view.sql).

---

## Canonical term → UI label

| Canonical term (backend) | DB location | UI label (German default) | UI label (English) | Notes |
|--------------------------|-------------|---------------------------|--------------------|----|
| Happening | `happening` table | *Experience* (planned) | *Experience* | Identity object; never shown raw as "Happening" in UI |
| Occurrence | `occurrence` table | *Session* | *Session* | A single dated instance of an offering |
| Offering | `offering` table | *Series* (when `offering_type = 'series'`) | *Series* | Schedule container; not directly visible to users as "Offering" |
| Feed card | Row from `feed_cards_view` | *(card component)* | *(card component)* | Occurrence projection rendered as a card |
| Course | `courses` table | *Kurs* | *Course* | Separate tab; not in Happenings feed |
| Organizer | `organizer` table | *Veranstalter* | *Organizer* | Shown in detail view, not on card |

---

## UI element → Canonical source

| UI element | Source field | Notes |
|------------|-------------|-------|
| Card title | `feed_cards_view.title` | From `happening.title` |
| Card location | `feed_cards_view.location_name` | From `happening.location_name` |
| Time pill text | Computed from `start_at`, `end_at`, `date_precision`, `is_happening_now` | See PRODUCT.md §9 for display rules |
| Section header "This weekend" | `section_key = 'weekend'` | Thu–Sun window, Europe/Zurich |
| Section header "Coming up" | `section_key = 'coming_up'` | After current weekend |

---

## display_kind mapping

`display_kind` comes from `happening.happening_kind` in the view.

| `display_kind` value | UI meaning | Example |
|----------------------|------------|---------|
| `event` | One-off or infrequent occurrence | Concert, market, festival |
| *(other values TBD)* | Deferred | Activity, workshop — not yet finalized |

> **Note:** Final Event / Activity / Happening taxonomy is explicitly
> deferred (see PRODUCT.md §10). `display_kind` values will evolve.

---

## display_when (time pill)

`display_when` is NOT stored in the database. It is computed at
render time from:

| Input field | Purpose |
|-------------|---------|
| `is_happening_now` | Show "Laufend · Bis HH:MM Uhr" |
| `start_at` + timezone | Today / Tomorrow / future date |
| `end_at` | End time display (when `is_happening_now`) |
| `date_precision` | If `'date'` → show date only, never show time |

Full rules are in PRODUCT.md §9 (Time Pill Display Rules).

---

## section_key → UI section

| `section_key` | UI section header (DE) | UI section header (EN) |
|---------------|------------------------|------------------------|
| `weekend` | *Dieses Wochenende* | *This weekend* |
| `coming_up` | *Demnächst* | *Coming up* |

Section headers are presentation-only. The pill text inside a section
must NOT repeat the section concept (e.g., never show "This weekend"
inside a time pill).

---

## Rules

1. **DB names do not change.** This mapping is UI-only.
2. **One language per UI element.** No mixed-language output.
3. **Default language is German.** English is the alternative.
4. Full i18n framework is explicitly deferred.
