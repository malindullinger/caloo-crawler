# Heuristic Tagging

> Last updated: 2026-02
>
> **This document is normative.** It describes the deterministic
> heuristic tagging system for canonical happenings.

Source code: `src/canonicalize/tagging.py`
For how tags feed into ranking see [ranking.md](ranking.md).

---

## Purpose

Heuristic tagging assigns `audience_tags` and `topic_tags` to
canonical happenings based on keyword matching against title and
description text. Tags are used as inputs to the ranking layer and
for future filtering/personalization.

---

## Columns (on `happening` table)

| Column | Type | Default | Migration |
|--------|------|---------|-----------|
| `audience_tags` | `text[]` | `'{}'` | 017 |
| `topic_tags` | `text[]` | `'{}'` | 017 |
| `editorial_priority` | `int` | `0` | 017 |

---

## Vocabulary

### Audience tags (`AUDIENCE_VOCAB`)

| Tag key | Keywords (casefold'ed) |
|---------|----------------------|
| `adults` | erwachsene |
| `family_kids` | kinder, kind, familie, eltern, spiel, spielplatz, jugend, schule, kita, familienkreis, familientreff |
| `seniors` | senior, 60+, rentner |

### Topic tags (`TOPIC_VOCAB`)

| Tag key | Keywords (casefold'ed) |
|---------|----------------------|
| `civic` | gemeinde, abstimmung, sitzung, versammlung, infoanlass |
| `culture` | konzert, theater, kino, museum, ausstellung, lesung |
| `nature` | wald, wander, natur, see, outdoor, spielplatz |
| `sport` | sport, turnen, fussball, schwimmen, tanz, yoga, bewegung |

---

## Matching semantics

1. **Input:** `happening.title` + `happening.description` (or
   `source_row.title_raw` + `source_row.description_raw` during merge)
2. **Normalization:** `casefold()` + collapse whitespace
   - `casefold()` normalizes `ß → ss` (so keyword `fussball` matches
     input `Fußball`)
   - Umlauts `ä/ö/ü` stay as-is (already lowercase after casefold)
   - Punctuation preserved (needed for `60+` matching)
3. **Matching:** Substring search (`keyword in text`)
   - Handles German compound words naturally
     (e.g., `"Kinderyoga"` contains `"kinder"`)
4. **Output:** Sorted list of matched tag keys (deterministic)
5. **Multiple matches:** A happening can have multiple audience tags
   AND multiple topic tags simultaneously

---

## Write rules (strict)

| Rule | Detail |
|------|--------|
| **Only set when empty** | Tags are written only if the current array is `'{}'` (empty). Non-empty arrays are never overwritten. |
| **Admin edits win** | If an admin has manually set tags, heuristic tagging skips that happening. |
| **Never touch `editorial_priority`** | Automated systems must not modify `editorial_priority` under any circumstances. |
| **Idempotent** | Re-running tagging on the same data produces the same result. Field history uses `ON CONFLICT (change_key) DO NOTHING`. |
| **Logged** | Every tag write is logged to `canonical_field_history` with a deterministic `change_key` for auditability. |

---

## Integration points

Tagging runs at two points in the merge loop:

| Path | When | Behavior |
|------|------|----------|
| **CREATE** | New canonical happening created | Tags computed from source_row text; included in initial INSERT payload (if non-empty) |
| **MERGE** | Source row merges into existing happening | `apply_heuristic_tags()` reads current happening; if tags empty, computes from source_row text (falls back to happening text); updates + logs to field history |

See `src/canonicalize/merge_loop.py` for implementation.

---

## Serialization

`pg_array_literal()` converts tag lists to PostgreSQL `text[]` literal
strings for deterministic `change_key` computation:

| Input | Output |
|-------|--------|
| `[]` | `"{}"` |
| `["sport"]` | `"{sport}"` |
| `["sport", "culture"]` | `"{culture,sport}"` |

Output is always sorted for stability.

---

## Current limitations

| Limitation | Notes |
|------------|-------|
| **Starter vocabulary only** | Current keyword lists are intentionally small and conservative. Expansion requires review. |
| **No negative matching** | Cannot exclude tags (e.g., "this mentions kids but isn't for kids"). |
| **No language detection** | Assumes German text. English-language happenings may not match German keywords. |
| **No weighting** | Title matches and description matches are treated equally. |
| **No admin UI for tag management** | Tags can only be edited via direct DB access. Admin UI is deferred. |

---

## Admin override intent (planned)

When an admin UI exists:
- Admins can set `audience_tags` and `topic_tags` to any values
- Once set by an admin, heuristic tagging will never overwrite them
  (the "only set when empty" rule guarantees this)
- Admins can clear tags back to `'{}'` to re-enable heuristic tagging
- `editorial_priority` is always admin-only (never set by code)
