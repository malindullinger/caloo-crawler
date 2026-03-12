"""
Structured time extraction helpers.

Extract datetime from JSON-LD (Schema.org Event) and HTML <time> elements.
Returns ISO 8601 strings that normalize.py can parse directly.

These helpers are reusable across all adapters.
"""
from __future__ import annotations

import json
import re
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Optional
from zoneinfo import ZoneInfo

from bs4 import BeautifulSoup, Tag


@dataclass
class StructuredTime:
    """Extracted structured datetime info."""

    start_iso: str | None = None
    end_iso: str | None = None
    source: str = "unknown"  # "jsonld", "time_element", "text_heuristic"


def extract_jsonld_event(soup: BeautifulSoup) -> StructuredTime | None:
    """Extract startDate/endDate from JSON-LD Event markup.

    Returns StructuredTime with ISO strings if found, else None.
    Handles:
    - Single Event object
    - Array containing Event
    - @graph containing Event
    - @type as string ("Event") or list (["Event", "Thing"])
    """
    for script in soup.find_all("script", type="application/ld+json"):
        try:
            # Use get_text() instead of .string for robustness
            # (script.string is often None with whitespace/comments)
            data = json.loads(script.get_text() or "")
            events = _find_events_in_jsonld(data)
            for event in events:
                start = event.get("startDate")
                if start and isinstance(start, str):
                    end = event.get("endDate")
                    return StructuredTime(
                        start_iso=start.strip(),
                        end_iso=end.strip() if isinstance(end, str) else None,
                        source="jsonld",
                    )
        except (json.JSONDecodeError, TypeError, AttributeError):
            continue
    return None


def _is_event_type(t) -> bool:
    """Check if @type indicates an Event.

    Handles both string and list formats:
    - "@type": "Event"
    - "@type": ["Event", "Thing"]
    """
    if isinstance(t, str):
        return t == "Event"
    if isinstance(t, list):
        return "Event" in t
    return False


def _find_events_in_jsonld(data) -> list[dict]:
    """Recursively find Event objects in JSON-LD structure."""
    events = []
    if isinstance(data, dict):
        if _is_event_type(data.get("@type")):
            events.append(data)
        if "@graph" in data and isinstance(data["@graph"], list):
            for item in data["@graph"]:
                events.extend(_find_events_in_jsonld(item))
    elif isinstance(data, list):
        for item in data:
            events.extend(_find_events_in_jsonld(item))
    return events


def extract_time_element(
    soup: BeautifulSoup,
    container: Tag | None = None,
    reference_time: datetime | None = None,
) -> StructuredTime | None:
    """Extract datetime from <time datetime="..."> elements.

    Strategy:
    - If container provided, search within it first
    - Collect all candidates with valid datetime attr
    - Prefer candidate closest to now (but in future) if reference_time given
    - Otherwise prefer first candidate in container, then first in page

    Returns StructuredTime with ISO string if found, else None.
    """
    # Search scope: container first, then full soup
    search_scope = container if container else soup

    candidates: list[tuple[Tag, str]] = []
    for time_el in search_scope.find_all("time", datetime=True):
        dt_attr = (time_el.get("datetime") or "").strip()
        if dt_attr and _looks_like_datetime(dt_attr):
            candidates.append((time_el, dt_attr))

    # If nothing in container, try full soup
    if not candidates and container:
        for time_el in soup.find_all("time", datetime=True):
            dt_attr = (time_el.get("datetime") or "").strip()
            if dt_attr and _looks_like_datetime(dt_attr):
                candidates.append((time_el, dt_attr))

    if not candidates:
        return None

    # Pick best candidate
    if reference_time and len(candidates) > 1:
        best = _pick_best_time_candidate(candidates, reference_time)
    else:
        best = candidates[0][1]

    return StructuredTime(start_iso=best, end_iso=None, source="time_element")


def _looks_like_datetime(s: str) -> bool:
    """Check if string looks like ISO 8601 date/datetime."""
    # Basic check: starts with YYYY-MM-DD pattern
    return bool(re.match(r"\d{4}-\d{2}-\d{2}", s))


def _has_time_component(s: str) -> bool:
    """Check if ISO string includes a time component (not just date)."""
    return bool(re.search(r"T\d{2}:\d{2}", s))


def _pick_best_time_candidate(
    candidates: list[tuple[Tag, str]],
    reference_time: datetime,
) -> str:
    """Pick candidate closest to reference_time but in future.

    Prefers candidates with time component over date-only.
    """
    tz = ZoneInfo("Europe/Zurich")
    ref_aware = (
        reference_time
        if reference_time.tzinfo
        else reference_time.replace(tzinfo=tz)
    )

    # Prefer candidates with actual time over date-only
    timed = [(tag, s) for (tag, s) in candidates if _has_time_component(s)]
    pool = timed if timed else candidates

    best_dt: datetime | None = None
    best_str: str = pool[0][1]

    for _, dt_str in pool:
        try:
            parsed = parse_iso_datetime(dt_str, tz)
            if parsed and parsed >= ref_aware:
                if best_dt is None or parsed < best_dt:
                    best_dt = parsed
                    best_str = dt_str
        except Exception:
            continue

    return best_str


# ISO 8601 datetime in page text: "2026-03-12T08:30:00+01:00" or "2026-03-12T15:00"
_ISO_TEXT_RE = re.compile(
    r"\d{4}-\d{2}-\d{2}T\d{2}:\d{2}(?::\d{2})?(?:[+-]\d{2}:\d{2})?"
)


def extract_datetime_structured(
    soup: BeautifulSoup,
    *,
    container: Tag | None = None,
) -> tuple[str | None, str]:
    """Run tiers 1-3 of structured datetime extraction.

    Tier 1: JSON-LD Event startDate/endDate
    Tier 2: <time datetime="..."> elements
    Tier 3: ISO 8601 strings in page text

    Returns (datetime_raw, extraction_method).
    extraction_method is "jsonld", "time_element", "iso_text", or "none".
    Adapters call this first, then fall back to their own heuristic.
    """
    # Tier 1: JSON-LD
    structured = extract_jsonld_event(soup)
    if structured and structured.start_iso:
        if structured.end_iso:
            return f"{structured.start_iso} | {structured.end_iso}", "jsonld"
        return structured.start_iso, "jsonld"

    # Tier 2: <time> elements
    now_utc = datetime.now(timezone.utc)
    structured = extract_time_element(soup, container=container, reference_time=now_utc)
    if structured and structured.start_iso:
        return structured.start_iso, "time_element"

    # Tier 3: ISO 8601 text in page content
    page_text = soup.get_text(" ", strip=True)
    iso_matches = _ISO_TEXT_RE.findall(page_text)
    if iso_matches:
        if len(iso_matches) >= 2:
            return f"{iso_matches[0]} | {iso_matches[1]}", "iso_text"
        return iso_matches[0], "iso_text"

    return None, "none"


def parse_iso_datetime(s: str, default_tz: ZoneInfo) -> datetime | None:
    """Parse ISO 8601 string to timezone-aware datetime.

    - If string has offset/Z, respects it
    - Otherwise interprets as default_tz (Europe/Zurich)

    Does NOT use dateparser to avoid locale/timezone ambiguities.
    """
    if not s:
        return None

    s = s.strip()

    # Handle trailing 'Z' (UTC)
    if s.endswith("Z"):
        s = s[:-1] + "+00:00"

    try:
        dt = datetime.fromisoformat(s)
        # If naive, assume default timezone
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=default_tz)
        return dt
    except ValueError:
        return None
