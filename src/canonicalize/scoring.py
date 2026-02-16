# src/canonicalize/scoring.py
"""
Deterministic relevance scoring for canonical happenings.

Pure utility — no DB access, no side effects.

Computes `relevance_score_global` from audience_tags + topic_tags.
editorial_priority is NOT included here — it is a separate, higher-priority
sort key in the feed_cards_view ORDER BY.

Formula (v1):
  score = 0
  +50  if 'family_kids' in audience_tags
  -30  if 'seniors' in audience_tags
  +10  if any topic_tag in BOOSTED_TOPICS

Mapping from user intent to actual tag vocabulary:
  user 'family' / 'kids' → our 'family_kids' (single tag) → +50
  user 'seniors'          → our 'seniors'                  → -30
  user 'outdoor'          → our 'nature'                   → +10
  user 'culture'          → our 'culture'                  → +10
  user 'sports'           → our 'sport'                    → +10
  user 'creative'         → not in vocabulary (skipped)

Tags not listed receive 0 (neutral):
  'adults' → 0
  'civic'  → 0
"""
from __future__ import annotations


# ---------------------------------------------------------------------------
# Scoring rules (v1)
# ---------------------------------------------------------------------------

AUDIENCE_SCORES: dict[str, int] = {
    "family_kids": 50,
    "seniors": -30,
    # "adults": 0 — neutral, omitted
}

BOOSTED_TOPICS: frozenset[str] = frozenset({"nature", "culture", "sport"})
TOPIC_BOOST: int = 10


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------

def compute_relevance_score(
    audience_tags: list[str] | None = None,
    topic_tags: list[str] | None = None,
) -> int:
    """
    Deterministic relevance score from tags.

    Returns an integer (may be negative). Same inputs always produce same output.
    """
    score = 0

    for tag in (audience_tags or []):
        score += AUDIENCE_SCORES.get(tag, 0)

    if any(t in BOOSTED_TOPICS for t in (topic_tags or [])):
        score += TOPIC_BOOST

    return score
