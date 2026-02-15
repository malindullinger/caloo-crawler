# src/canonicalize/dedupe_key.py
"""
Dedupe-key contract for source_happenings.

Contract (v1):
  - Format: "v1|<sha256_hex>"
  - Primary seed: source_id | normalized_title | start_date_local | normalized_location
  - Time-of-day MUST NOT affect the key (uses DATE only)
  - Deterministic: identical inputs → identical key
  - Non-null: always returns a key (URL/external_id fallback when content insufficient)
"""
from __future__ import annotations

from hashlib import sha256
from typing import Optional

from .matching import normalize_title, normalize_venue

VERSION = "v1"


def _sha256_hex(s: str) -> str:
    return sha256(s.encode("utf-8")).hexdigest()


def compute_dedupe_key(
    *,
    source_id: str,
    title: Optional[str],
    start_date_local: Optional[str],  # ISO date string, e.g. "2026-02-15"
    location: Optional[str],
    item_url: Optional[str] = None,
    external_id: Optional[str] = None,
) -> str:
    """
    Compute a versioned, content-based dedupe key.

    Primary path (content-based):
      When title AND date are available, the key is a hash of
      source_id + normalized_title + start_date_local + normalized_location.
      Time-of-day is intentionally excluded.

    Fallback path (identifier-based):
      When content is insufficient, falls back to external_id or item_url hash.

    Raises ValueError if no key can be derived.
    """
    title_key = normalize_title(title)
    date_key = (start_date_local or "").strip()
    loc_key = normalize_venue(location)

    # Content-based key: requires at least title + date
    if title_key and date_key:
        seed = "|".join([source_id, title_key, date_key, loc_key])
        return f"{VERSION}|{_sha256_hex(seed)}"

    # Fallback: external_id
    if external_id:
        seed = f"{source_id}|ext|{external_id}"
        return f"{VERSION}|{_sha256_hex(seed)}"

    # Fallback: item_url
    if item_url:
        seed = f"{source_id}|url|{item_url}"
        return f"{VERSION}|{_sha256_hex(seed)}"

    raise ValueError(
        f"Cannot compute dedupe_key: missing content and identifiers"
        f" for source {source_id}"
    )
