# src/canonicalize/tagging.py
"""
Heuristic audience/topic tagging for canonical happenings.

Pure utility — deterministic, no DB access, no side effects.

Uses substring matching on casefold'ed text to handle German compound
words (e.g. "Kinderyoga" matches keyword "kinder").

casefold() normalizes:
  - ß → ss  (so keyword "fussball" matches input "Fußball")
  - Upper → lower
  - Umlauts ä/ö/ü stay as-is (already lowercase)
"""
from __future__ import annotations

import re
from typing import Optional


# ---------------------------------------------------------------------------
# Vocabulary (starter, stable strings)
# ---------------------------------------------------------------------------

AUDIENCE_VOCAB: dict[str, list[str]] = {
    "adults": ["erwachsene"],
    "family_kids": [
        "kinder", "kind", "familie", "eltern", "spiel", "spielplatz",
        "jugend", "schule", "kita", "familienkreis", "familientreff",
    ],
    "seniors": ["senior", "60+", "rentner"],
}

TOPIC_VOCAB: dict[str, list[str]] = {
    "civic": [
        "gemeinde", "abstimmung", "sitzung", "versammlung", "infoanlass",
    ],
    "culture": [
        "konzert", "theater", "kino", "museum", "ausstellung", "lesung",
    ],
    "nature": ["wald", "wander", "natur", "see", "outdoor", "spielplatz"],
    "sport": [
        "sport", "turnen", "fussball", "schwimmen", "tanz", "yoga",
        "bewegung",
    ],
}


# ---------------------------------------------------------------------------
# Internals
# ---------------------------------------------------------------------------

def _normalize_for_matching(text: Optional[str]) -> str:
    """casefold + collapse whitespace. Keeps punctuation (e.g. '60+')."""
    if not text:
        return ""
    s = text.casefold()
    s = re.sub(r"\s+", " ", s).strip()
    return s


def _match_vocab(text: str, vocab: dict[str, list[str]]) -> list[str]:
    """Return sorted tag keys whose vocabulary has at least one keyword hit."""
    tags: list[str] = []
    for tag in sorted(vocab):  # sorted keys → deterministic output
        for kw in vocab[tag]:
            if kw in text:
                tags.append(tag)
                break
    return tags


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------

def infer_audience_tags(
    title: Optional[str] = None,
    description: Optional[str] = None,
) -> list[str]:
    """
    Deterministic audience tag inference from title + description text.
    Returns sorted list of matched tag keys, or empty list.
    """
    text = _normalize_for_matching(title) + " " + _normalize_for_matching(description)
    return _match_vocab(text, AUDIENCE_VOCAB)


def infer_topic_tags(
    title: Optional[str] = None,
    description: Optional[str] = None,
) -> list[str]:
    """
    Deterministic topic tag inference from title + description text.
    Returns sorted list of matched tag keys, or empty list.
    """
    text = _normalize_for_matching(title) + " " + _normalize_for_matching(description)
    return _match_vocab(text, TOPIC_VOCAB)


def pg_array_literal(tags: list[str]) -> str:
    """
    Serialize tag list to PostgreSQL text[] literal for deterministic
    change_key computation. Sorts to guarantee stability.

    [] → "{}"
    ["sport", "culture"] → "{culture,sport}"
    """
    if not tags:
        return "{}"
    return "{" + ",".join(sorted(tags)) + "}"
