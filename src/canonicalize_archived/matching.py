# src/canonicalize/matching.py
"""
Pure matching / scoring functions for canonicalization.
No DB access, no crawler imports.
"""
from __future__ import annotations

import re
from hashlib import sha256
from typing import Any, Dict, Optional

CONFIDENCE_THRESHOLD = 0.85


# ---------------------------------------------------------------------------
# Normalization
# ---------------------------------------------------------------------------

def normalize_title(title: Optional[str]) -> str:
    """Lowercase, collapse whitespace, strip punctuation."""
    if not title:
        return ""
    s = title.lower().strip()
    s = re.sub(r"\s+", " ", s)
    s = re.sub(r"[^\w\s]", "", s)
    return s


def normalize_venue(venue: Optional[str]) -> str:
    """Lowercase, collapse whitespace, expand common CH street abbreviations."""
    if not venue:
        return ""
    s = venue.lower().strip()
    s = re.sub(r"\s+", " ", s)
    s = s.replace("str.", "strasse").replace("str ", "strasse ")
    s = s.rstrip(".,;")
    return s


# ---------------------------------------------------------------------------
# Token similarity
# ---------------------------------------------------------------------------

def jaccard_tokens(a: str, b: str) -> float:
    """Jaccard similarity over whitespace-split token sets. Returns 0.0–1.0."""
    sa = set(a.split())
    sb = set(b.split())
    if not sa and not sb:
        return 1.0
    if not sa or not sb:
        return 0.0
    return len(sa & sb) / len(sa | sb)


# ---------------------------------------------------------------------------
# Fingerprint
# ---------------------------------------------------------------------------

def compute_fingerprint(source_row: Dict[str, Any]) -> str:
    """
    Deterministic fingerprint for a source_happenings row.
    Uses start_date_local (DATE, not timestamp) so date-only sources match.

    Expects dict keys: title_raw, start_date_local (str ISO or None), location_raw.
    """
    t = normalize_title(source_row.get("title_raw"))
    d = source_row.get("start_date_local") or ""
    v = normalize_venue(source_row.get("location_raw"))
    base = "|".join(part for part in (t, d, v) if part)
    return sha256(base.encode("utf-8")).hexdigest()[:32]


# ---------------------------------------------------------------------------
# Confidence scoring
# ---------------------------------------------------------------------------

def confidence_score(
    happening_row: Dict[str, Any],
    offering_row: Dict[str, Any],
    source_row: Dict[str, Any],
) -> float:
    """
    Score how likely *source_row* matches an existing happening + offering pair.

    Returns 0.0–1.0.  Compare against CONFIDENCE_THRESHOLD to decide
    auto-merge vs. ambiguous_match_log.

    Weights (sum = 1.0):
      title   0.50
      date    0.30
      venue   0.20
    """
    # --- title ---
    src_title = normalize_title(source_row.get("title_raw"))
    hap_title = normalize_title(happening_row.get("title"))
    title_sim = jaccard_tokens(src_title, hap_title)

    # --- date ---
    src_date = source_row.get("start_date_local") or ""
    off_date = offering_row.get("start_date_local") or ""
    date_sim = 1.0 if (src_date and src_date == off_date) else 0.0

    # --- venue ---
    src_venue = normalize_venue(source_row.get("location_raw"))
    hap_venue = normalize_venue(happening_row.get("location_name"))
    venue_sim = jaccard_tokens(src_venue, hap_venue)

    return 0.50 * title_sim + 0.30 * date_sim + 0.20 * venue_sim
