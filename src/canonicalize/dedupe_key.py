# src/canonicalize/dedupe_key.py
"""
Dedupe-key contract for source_happenings and source_courses.

=== CONTRACT (v1) ===

Format:  "v1|<sha256_hex>"

The dedupe_key uniquely identifies a source record *within* a given source.
Two crawl runs that see the same event produce the same dedupe_key, so the
upsert ON CONFLICT (source_id, dedupe_key) prevents duplicate rows.

--- Seed construction ---

  PRIMARY PATH (content-based):
    Requires: title AND start_date_local (both non-empty after normalization)
    seed = "<source_id>|<normalized_title>|<start_date_local>|<normalized_location>"

  FALLBACK #1 (external_id):
    seed = "<source_id>|ext|<external_id>"

  FALLBACK #2 (item_url):
    seed = "<source_id>|url|<item_url>"

  If none of the above can be formed: raise ValueError.

--- Normalization rules ---

  Title:
    - casefold (Python str.lower)
    - collapse whitespace to single space
    - strip ALL non-word characters except spaces (re.sub(r"[^\\w\\s]", ""))
    - Umlauts/ß are preserved as-is (ä → ä, not ae; ß → ß, not ss)
      because Python \\w matches them

  Location / Venue:
    - casefold
    - collapse whitespace
    - expand common Swiss abbreviations: "str." → "strasse", "str " → "strasse "
    - strip trailing punctuation .,;

  Date (start_date_local):
    - ISO date string only, e.g. "2026-06-15"
    - Time-of-day MUST NOT affect the key (date-only guarantee)
    - If the source has date_precision='date' (time unknown), the key still
      uses the date — it does NOT invent 00:00 or any time placeholder

  Source isolation:
    - source_id is always part of the seed
    - Same event from different sources → different dedupe_keys

--- Date-only vs datetime ---

  Both produce keys using start_date_local only.
  A date-only event on 2026-06-15 and a datetime event starting
  2026-06-15T10:00:00+02:00 at the same venue with the same title
  produce THE SAME dedupe_key — this is intentional (same logical event).

  The date_precision field on source_happenings distinguishes them; the
  dedupe_key is about identity ("is this the same event?"), not time
  precision.

--- Courses (source_courses) ---

  Courses typically lack start_date_local. The fallback chain handles this:
    - If title + date exist → content-based key
    - Else external_id → identifier key
    - Else item_url → URL key
    - Else → ValueError

--- Examples ---

  1. Content-based (typical):
     source_id="maennedorf_portal", title="Kinderyoga im Park",
     start_date_local="2026-06-15", location="Gemeindesaal Männedorf"
     → seed = "maennedorf_portal|kinderyoga im park|2026-06-15|gemeindesaal männedorf"
     → key  = "v1|<sha256>"

  2. Content-based, date-only (no time known):
     source_id="eventbrite", title="Flohmarkt", start_date_local="2026-04-12",
     location="Stadtpark"
     → seed = "eventbrite|flohmarkt|2026-04-12|stadtpark"
     → key  = "v1|<sha256>"
     (Same key regardless of whether start_at is NULL or a real timestamp)

  3. Content-based, no location:
     source_id="eventbrite", title="Online Yoga", start_date_local="2026-03-01",
     location=None
     → seed = "eventbrite|online yoga|2026-03-01|"
     → key  = "v1|<sha256>"

  4. Fallback to external_id:
     source_id="partner_feed", title=None, external_id="partner-evt-42"
     → seed = "partner_feed|ext|partner-evt-42"
     → key  = "v1|<sha256>"

  5. Fallback to URL:
     source_id="eventbrite", title=None, item_url="https://eventbrite.com/e/12345"
     → seed = "eventbrite|url|https://eventbrite.com/e/12345"
     → key  = "v1|<sha256>"
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

    Used by both source_happenings and source_courses upsert paths.
    See module docstring for full contract specification.

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
