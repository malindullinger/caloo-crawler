# src/canonicalize/canonical_dedupe_key.py
"""
Canonical dedupe key — deterministic identity for public.happening rows.

=== CONTRACT (c1) ===

Format: "c1|<sha256_hex>"

Unlike the source-level dedupe_key (v1|...) which is scoped per-source,
the canonical dedupe key is CROSS-SOURCE: it identifies a unique real-world
happening regardless of which source contributed it.

--- Seed construction ---

  seed = "<happening_kind>|<normalized_title>|<date_anchor>|<location_anchor>"

  happening_kind: 'event', 'activity', 'course', 'service' (default 'event')

  normalized_title: lowercase, collapse whitespace, strip non-word chars
                    (same rules as matching.normalize_title)

  date_anchor:
    - start_date (ISO date string) if present
    - else start_at cast to date (Europe/Zurich) if present
    - else 'unknown-date'

  location_anchor:
    - primary_venue_id (UUID string) if present
    - else 'online' if online=True
    - else 'unknown-location'

--- Properties ---

  - Deterministic: same inputs → same key, always
  - IMMUTABLE: depends only on the input fields, not on time or state
  - Cross-source: different sources producing the same happening → same key
  - Matches the SQL function public.compute_canonical_dedupe_key exactly

--- Version prefix ---

  'c1|' distinguishes canonical keys from source-level 'v1|' keys.
"""
from __future__ import annotations

from hashlib import sha256
from typing import Any, Mapping, Optional

from .matching import normalize_title

VERSION = "c1"


def _sha256_hex(s: str) -> str:
    return sha256(s.encode("utf-8")).hexdigest()


def _date_anchor(
    start_date: Optional[str],
    start_at: Optional[str],
) -> str:
    """
    Compute the date anchor for the canonical dedupe key.

    Priority:
      1. start_date (ISO date) if present
      2. start_at parsed to date (date portion only) if present
      3. 'unknown-date'
    """
    if start_date:
        return start_date.strip()

    if start_at:
        # start_at is a timestamptz string like "2026-06-15T10:00:00+02:00"
        # Extract date portion. For consistency with the SQL function which
        # uses AT TIME ZONE 'Europe/Zurich', we parse and convert.
        # However, the date portion of an ISO string is usually correct for
        # Europe/Zurich events. We take the first 10 chars as the date.
        try:
            from datetime import datetime, timezone as tz
            from zoneinfo import ZoneInfo

            dt = datetime.fromisoformat(start_at)
            zurich = ZoneInfo("Europe/Zurich")
            local_date = dt.astimezone(zurich).date()
            return local_date.isoformat()
        except (ValueError, ImportError):
            # Fallback: take first 10 chars if it looks like a date
            date_part = start_at[:10].strip()
            if len(date_part) == 10 and date_part[4] == "-":
                return date_part

    return "unknown-date"


def _location_anchor(
    primary_venue_id: Optional[str],
    online: Optional[bool],
) -> str:
    """
    Compute the location anchor for the canonical dedupe key.

    Priority:
      1. primary_venue_id (UUID as string) if present
      2. 'online' if online flag is True
      3. 'unknown-location'
    """
    if primary_venue_id:
        return str(primary_venue_id)
    if online:
        return "online"
    return "unknown-location"


def compute_canonical_dedupe_key(
    *,
    happening_kind: Optional[str] = None,
    title: Optional[str] = None,
    start_date: Optional[str] = None,
    start_at: Optional[str] = None,
    primary_venue_id: Optional[str] = None,
    online: Optional[bool] = None,
) -> str:
    """
    Compute the canonical dedupe key for a happening.

    Returns: "c1|<sha256_hex>"

    This MUST match the SQL function public.compute_canonical_dedupe_key exactly.
    """
    kind = happening_kind or "event"
    norm_title = normalize_title(title)
    date_anch = _date_anchor(start_date, start_at)
    loc_anch = _location_anchor(primary_venue_id, online)

    seed = "|".join([kind, norm_title, date_anch, loc_anch])
    return f"{VERSION}|{_sha256_hex(seed)}"


def compute_canonical_dedupe_key_from_row(
    row: Mapping[str, Any],
) -> str:
    """
    Compute canonical_dedupe_key from a happening dict (as returned by Supabase).
    """
    return compute_canonical_dedupe_key(
        happening_kind=row.get("happening_kind"),
        title=row.get("title"),
        start_date=row.get("start_date"),
        start_at=row.get("start_at"),
        primary_venue_id=row.get("primary_venue_id"),
        online=row.get("online"),
    )


def compute_canonical_dedupe_key_from_source(
    source_row: Mapping[str, Any],
    *,
    happening_kind: Optional[str] = None,
) -> str:
    """
    Compute canonical_dedupe_key from a source_happenings row.

    Maps source field names to canonical field names:
      title_raw → title
      start_date_local → start_date
      start_at → start_at
      (primary_venue_id and online are not on source rows — default to unknown-location)
    """
    return compute_canonical_dedupe_key(
        happening_kind=happening_kind or "event",
        title=source_row.get("title_raw"),
        start_date=source_row.get("start_date_local"),
        start_at=source_row.get("start_at"),
        primary_venue_id=None,  # not available on source rows
        online=None,
    )
