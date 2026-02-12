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
    offering_row: Optional[Dict[str, Any]],
    source_row: Dict[str, Any],
) -> float:
    """
    Score how likely *source_row* matches an existing happening + offering pair.

    Returns 0.0–1.0. Compare against CONFIDENCE_THRESHOLD to decide
    auto-merge vs. review.

    Base weights (sum = 1.0 when all signals available):
      title   0.50
      date    0.30
      venue   0.20

    IMPORTANT:
    - Your canonical `happening` table does NOT include venue name fields.
      Venue typically lives on `venue` / `occurrence.venue_id`.
    - Therefore venue similarity is often unavailable at this stage.
      To avoid "max score < threshold", we dynamically renormalize weights
      based on which signals are actually available.
    """
    # --- title ---
    src_title = normalize_title(source_row.get("title_raw"))
    hap_title = normalize_title(happening_row.get("title"))
    title_sim = jaccard_tokens(src_title, hap_title)
    title_available = bool(src_title and hap_title)

    # --- date (range inclusion; no inference) ---
    # source_happenings.start_date_local is a DATE (ISO string)
    src_date = source_row.get("start_date_local")
    date_sim = 0.0
    date_available = False
    if offering_row and src_date:
        off_start = offering_row.get("start_date")
        off_end = offering_row.get("end_date") or off_start
        # if off_start/off_end are ISO date strings, lexicographic compare works
        if off_start and off_end:
            date_available = True
            date_sim = 1.0 if (off_start <= src_date <= off_end) else 0.0

    # --- venue (often unavailable in happening_row; keep conservative) ---
    src_venue = normalize_venue(source_row.get("location_raw"))
    # You likely don't have venue name on happening_row yet; leave empty unless you add it later.
    # If you later enrich candidate bundles with venue name, populate e.g. happening_row["__venue_name"].
    hap_venue = normalize_venue(happening_row.get("__venue_name"))
    venue_sim = jaccard_tokens(src_venue, hap_venue) if (src_venue and hap_venue) else 0.0
    venue_available = bool(src_venue and hap_venue)

    # --- dynamic weight renormalization ---
    weights = {
        "title": 0.50 if title_available else 0.0,
        "date": 0.30 if date_available else 0.0,
        "venue": 0.20 if venue_available else 0.0,
    }
    total_w = sum(weights.values())
    if total_w <= 0.0:
        return 0.0

    # normalize to sum=1.0 based on available signals
    for k in list(weights.keys()):
        weights[k] = weights[k] / total_w

    score = (
        weights["title"] * title_sim +
        weights["date"] * date_sim +
        weights["venue"] * venue_sim
    )

    # clamp
    if score < 0.0:
        return 0.0
    if score > 1.0:
        return 1.0
    return float(score)
