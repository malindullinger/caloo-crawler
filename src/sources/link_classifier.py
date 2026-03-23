"""
Phase 7C.2 — Link classification.

Classifies external links found on detail pages by intent.
Classification only — no link following.

Rules (deterministic):
  - URL patterns and link text determine classification
  - No inference, no heuristics
  - Unknown links classified as "other"
"""
from __future__ import annotations

import re
from urllib.parse import urlparse


# Registration patterns (URL or text)
_REG_RE = re.compile(
    r"(anmeld|registr|booking|sign.?up|einschreib|teilnahm|buchen)",
    re.IGNORECASE,
)

# Ticket patterns
_TICKET_RE = re.compile(
    r"(ticket|eventbrite|eventfrog|starticket|ticketcorner)",
    re.IGNORECASE,
)

# Organizer patterns (text only)
_ORG_TEXT_RE = re.compile(
    r"(veranstalter|organisator|organizer|organiser|träger)",
    re.IGNORECASE,
)

# Venue/location patterns (text only)
_VENUE_TEXT_RE = re.compile(
    r"(standort|ort|karte|map|wegbeschreibung|anfahrt|location|directions)",
    re.IGNORECASE,
)


def classify_link(url: str, text: str) -> str:
    """Classify a link by intent.

    Returns: "registration" | "organizer" | "venue" | "ticket" | "document" | "other"
    """
    # Document (PDF)
    if url.lower().endswith(".pdf"):
        return "document"

    # Mailto → organizer
    if url.startswith("mailto:"):
        return "organizer"

    # Ticket platforms
    if _TICKET_RE.search(url):
        return "ticket"

    # Registration (URL pattern)
    if _REG_RE.search(url):
        return "registration"

    # Registration (text pattern)
    if text and _REG_RE.search(text):
        return "registration"

    # Organizer (text pattern)
    if text and _ORG_TEXT_RE.search(text):
        return "organizer"

    # Venue (text pattern)
    if text and _VENUE_TEXT_RE.search(text):
        return "venue"

    return "other"


def classify_page_links(links: list[dict]) -> dict:
    """Classify a list of external links and return summary.

    Input: list of {url, text} dicts (from content_surfaces.scan_content_surfaces)
    Returns: {
        link_classifications: dict mapping classification → count,
        registration_url: str | None (first registration link found),
        organizer_url: str | None (first organizer link found),
    }
    """
    classifications: dict[str, int] = {}
    registration_url: str | None = None
    organizer_url: str | None = None

    for link in links:
        cls = classify_link(link.get("url", ""), link.get("text", ""))
        classifications[cls] = classifications.get(cls, 0) + 1

        if cls == "registration" and registration_url is None:
            registration_url = link["url"]
        elif cls == "organizer" and organizer_url is None:
            organizer_url = link["url"]

    result: dict = {}
    if classifications:
        result["link_classifications"] = classifications
    if registration_url:
        result["registration_url_from_links"] = registration_url
    if organizer_url:
        result["organizer_url_from_links"] = organizer_url

    return result
