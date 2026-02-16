# Ranking Layer

> Last updated: 2026-02
>
> **This document is normative.** It defines the intended ranking model
> for feed card ordering within sections.

For the feed contract see [../CLAUDE.md](../CLAUDE.md).
For the view SQL see [../sql/views/feed_cards_view.sql](../sql/views/feed_cards_view.sql).
For tagging (input to ranking) see [tagging.md](tagging.md).

---

## Principle: Ranking only, no exclusion

The ranking layer controls **order** within sections. It MUST NOT
filter, hide, or exclude cards. A card that passes eligibility
(published happening + scheduled occurrence) always appears in the
feed. Ranking determines where it appears relative to other cards.

> **Ranking is not filtering.** Every eligible card is shown.

---

## Current state (Feb 2026): chronological baseline

**Today, ranking is purely chronological within each section.**

All scoring columns (`editorial_priority`, `relevance_score_global`)
either default to `0` or do not yet exist. The effective ORDER BY is
section grouping + start time — nothing else. This is intentional and
correct for the current phase.

### Section-level ordering (LOCKED)

Cards are first grouped by `section_key`:

| Order | Section | Rule |
|-------|---------|------|
| 1 | `weekend` | Thu–Sun of current week |
| 2 | `coming_up` | After current weekend |

Weekend-first ordering is locked and must not change.

### Within-section ordering (current = chronological)

Within each section, cards are ordered **chronologically** by
occurrence start time. No scoring signals are active today:

```sql
ORDER BY
  CASE WHEN section_key = 'weekend' THEN 0 ELSE 1 END,
  COALESCE(occ.start_at, off.start_date::timestamptz) ASC
```

### Transition plan

When ranking signals are activated (future), the ORDER BY will gain a
`relevance_score DESC` step between section grouping and chronological
tiebreaker. The transition is additive — chronological ordering is
preserved as the final tiebreaker, and all scoring remains
deterministic and auditable (no randomness, no opaque ML).

---

## Planned scoring model

### Input columns (on `happening` table)

| Column | Type | Default | Source | Purpose |
|--------|------|---------|--------|---------|
| `editorial_priority` | `int` | `0` | Admin only (never set by heuristics) | Manual boost/suppress by editorial team |
| `audience_tags` | `text[]` | `'{}'` | Heuristic tagging or admin | Audience classification; input to relevance |
| `topic_tags` | `text[]` | `'{}'` | Heuristic tagging or admin | Topic classification; input to relevance |
| `relevance_score_global` | `float` | *(not yet created)* | Computed | Pre-computed base relevance score |

### Scoring formula (planned)

```
relevance_score = base_score + freshness_bonus - diversity_penalty
```

| Component | Source | Range | Description |
|-----------|--------|-------|-------------|
| `base_score` | `relevance_score_global` | 0.0–1.0 | Static relevance from tags + editorial priority |
| `freshness_bonus` | Time proximity to `occurrence.start_at` | 0.0–0.3 | Closer events get a bonus within their section |
| `diversity_penalty` | Same-organizer adjacency | 0.0–0.2 | Prevents one organizer from dominating the feed |

### Freshness bonus

Computed from how close the occurrence is to "now" relative to the
section window:

- Events happening today or tomorrow get maximum bonus
- Events further in the section window get less bonus
- Date-only items (no occurrence) get zero freshness bonus

### Diversity penalty

Applied via `ROW_NUMBER() OVER (PARTITION BY organizer_id ORDER BY ...)`:

- First card per organizer: no penalty
- Second card: small penalty
- Third+: larger penalty

This is a soft ranking signal, not a hard limit. All cards still appear.

### editorial_priority

| Value | Meaning |
|-------|---------|
| `0` | Default (no editorial opinion) |
| `> 0` | Boosted (appears higher in section) |
| `< 0` | Suppressed (appears lower in section) |

**Rule:** `editorial_priority` is NEVER set by heuristic tagging or
automated processes. It is reserved for explicit admin/editorial action.

---

## ORDER BY contract (target)

When the scoring model is implemented, the view ORDER BY becomes:

```sql
ORDER BY
  -- 1. Section grouping (weekend first) — LOCKED
  CASE WHEN section_key = 'weekend' THEN 0 ELSE 1 END,
  -- 2. Relevance score (higher = earlier) — PLANNED
  relevance_score DESC,
  -- 3. Chronological tiebreaker — STABLE
  COALESCE(occ.start_at, off.start_date::timestamptz) ASC
```

Until the scoring model is implemented, steps 1 and 3 are the only
active ordering. Step 2 is a no-op (all scores are 0).

---

## Implementation status

| Component | Status | Location |
|-----------|--------|----------|
| Section ordering | **Implemented** (LOCKED) | `feed_cards_view` |
| Chronological ordering | **Implemented** | `feed_cards_view` |
| `editorial_priority` column | **Created** (migration 017) | `happening` table |
| `audience_tags` / `topic_tags` | **Created + populated** | `happening` table; see [tagging.md](tagging.md) |
| `relevance_score_global` column | **Not yet created** | Planned on `happening` |
| Freshness bonus | **Not yet implemented** | Planned in view |
| Diversity penalty | **Not yet implemented** | Planned in view |
| Scoring in ORDER BY | **Not yet implemented** | Planned in view |

---

## Constraints

1. **No exclusion.** Ranking controls order, never visibility.
2. **Weekend-first is locked.** Section ordering is not negotiable.
3. **editorial_priority is admin-only.** Automated systems must not touch it.
4. **Deterministic.** Given the same data, the same ordering must result.
   No randomness, no time-of-day variation beyond freshness.
5. **No new feed views.** Ranking is implemented inside `feed_cards_view`,
   not as a separate view.
