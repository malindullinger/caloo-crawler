# src/canonicalize/confidence.py
"""
Data-quality confidence score (v1).

A deterministic, penalty-based score that reflects how complete and
reliable a happening's source metadata is.  Range: 0–100.

This is NOT:
  - the match confidence (src/canonicalize/matching.py)
  - a feed filter (NEVER gates visibility)

Used for: admin review prioritization, ops monitoring, source weighting.
"""
from __future__ import annotations


def _is_empty(value: str | None) -> bool:
    """True if value is None or blank after stripping."""
    return not value or not str(value).strip()


def compute_confidence_score(
    *,
    source_tier: str | None = None,
    date_precision: str | None = None,
    image_url: str | None = None,
    description: str | None = None,
    canonical_url: str | None = None,
    timezone: str | None = None,
    extraction_method: str | None = None,
) -> int:
    """
    Compute a data-quality confidence score (0–100).

    Starts at 100 and applies penalties:
      -20  date_precision = 'date' (no time info)
      -20  image_url missing
      -15  description missing
      -10  source_tier = 'B'
      -15  extraction_method is not 'jsonld'
      -30  timezone missing
      -40  canonical_url missing

    Returns: integer clamped to [0, 100].
    """
    score = 100

    # Date precision penalty
    if date_precision == "date":
        score -= 20

    # Image URL penalty
    if _is_empty(image_url):
        score -= 20

    # Description penalty
    if _is_empty(description):
        score -= 15

    # Source tier penalty (B tier = less reliable extraction)
    tier = (source_tier or "").strip().upper()
    if tier == "B":
        score -= 10

    # Extraction method penalty (non-jsonld = less structured)
    method = (extraction_method or "").strip().lower()
    if method != "jsonld":
        score -= 15

    # Timezone penalty
    if _is_empty(timezone):
        score -= 30

    # Canonical URL penalty
    if _is_empty(canonical_url):
        score -= 40

    # Clamp
    return max(0, min(100, score))
