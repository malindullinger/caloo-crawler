"""
Phase 7C.2 — Shared detail field extraction.

Deterministic extraction of supplementary fields from HTML detail pages.
Called by adapters after primary extraction (title, datetime, description, image).
Each function returns a dict to merge into extra{}.

Rules:
  - No inference, no guessing, no LLMs
  - Pattern mismatch → null (not "unknown")
  - Only extract from explicit statements
  - Each field has documented extraction patterns
"""
from __future__ import annotations

import re
from typing import Optional

from bs4 import BeautifulSoup


# ═══════════════════════════════════════════════════════════════════════
# Price extraction
# ═══════════════════════════════════════════════════════════════════════

# Patterns indicating free events
_FREE_RE = re.compile(
    r"\b(kostenlos|gratis|eintritt\s+frei|unentgeltlich"
    r"|kein[e]?\s+(?:kosten|gebühr)"
    r"|free\s+(?:entry|admission|of\s+charge))\b",
    re.IGNORECASE,
)

# Donation patterns
_DONATION_RE = re.compile(
    r"\b(kollekte|freiwillig(?:er|e)?\s+beitrag|spende)\b",
    re.IGNORECASE,
)

# Extract CHF amount
_CHF_AMOUNT_RE = re.compile(
    r"(?:CHF|Fr\.?)\s*(\d+(?:[.,]\d{1,2})?)",
    re.IGNORECASE,
)


def extract_price(soup: BeautifulSoup, *, dl_fields: dict | None = None) -> dict:
    """Extract price information from a detail page.

    Sources (priority order):
      1. <dl> fields: "Preis"/"Kosten"/"Eintritt" label → value
      2. JSON-LD: offers.price + offers.priceCurrency
      3. Full-page text scan (last resort)

    Returns: {price_type, price_from_chf, price_raw} or empty dict.
    """
    # 1. <dl> fields (most reliable for ICMS)
    if dl_fields:
        for label in ("Preis", "Kosten", "Eintritt", "Preis / Eintritt"):
            field = dl_fields.get(label)
            if field:
                return _parse_price_text(field["text"])

    # 2. JSON-LD offers
    import json
    for script in soup.find_all("script", type="application/ld+json"):
        try:
            data = json.loads(script.get_text() or "")
            items = [data] if isinstance(data, dict) else (data if isinstance(data, list) else [])
            for item in items:
                if not isinstance(item, dict) or item.get("@type") != "Event":
                    continue
                offers = item.get("offers")
                if isinstance(offers, dict):
                    price = offers.get("price")
                    if price is not None:
                        try:
                            amount = float(str(price))
                            return {
                                "price_type": "free" if amount == 0 else "paid",
                                "price_from_chf": amount if amount > 0 else None,
                                "price_raw": f"{offers.get('priceCurrency', 'CHF')} {price}",
                            }
                        except (ValueError, TypeError):
                            pass
                elif isinstance(offers, list):
                    for offer in offers:
                        if isinstance(offer, dict) and offer.get("price") is not None:
                            try:
                                amount = float(str(offer["price"]))
                                return {
                                    "price_type": "free" if amount == 0 else "paid",
                                    "price_from_chf": amount if amount > 0 else None,
                                    "price_raw": f"{offer.get('priceCurrency', 'CHF')} {offer['price']}",
                                }
                            except (ValueError, TypeError):
                                pass
        except (json.JSONDecodeError, TypeError, AttributeError):
            continue

    return {}


def _parse_price_text(text: str) -> dict:
    """Parse a price string into structured fields."""
    result: dict = {"price_raw": text}

    if _FREE_RE.search(text):
        result["price_type"] = "free"
        return result

    if _DONATION_RE.search(text):
        result["price_type"] = "donation"
        return result

    m = _CHF_AMOUNT_RE.search(text)
    if m:
        try:
            amount = float(m.group(1).replace(",", "."))
            result["price_type"] = "paid"
            result["price_from_chf"] = amount
        except ValueError:
            result["price_type"] = "paid"
    else:
        # Has text but no recognizable amount
        result["price_type"] = "paid"

    return result


# ═══════════════════════════════════════════════════════════════════════
# Age extraction
# ═══════════════════════════════════════════════════════════════════════

# "ab X Jahren" / "ab X J."
_AGE_MIN_RE = re.compile(
    r"(?:ab|from)\s+(\d{1,2})\s*(?:Jahren?|J\.|years?)",
    re.IGNORECASE,
)

# "X-Y Jahre" / "X bis Y Jahre"
_AGE_RANGE_RE = re.compile(
    r"(\d{1,2})\s*[-–]\s*(\d{1,2})\s*(?:Jahre|J\.|years?)",
    re.IGNORECASE,
)

# "X bis Y Jahre"
_AGE_BIS_RE = re.compile(
    r"(\d{1,2})\s+bis\s+(\d{1,2})\s*(?:Jahre|J\.|years?)",
    re.IGNORECASE,
)

# "Kinder ab X"
_KINDER_AB_RE = re.compile(
    r"Kinder\s+ab\s+(\d{1,2})",
    re.IGNORECASE,
)


def extract_age(
    soup: BeautifulSoup,
    *,
    dl_fields: dict | None = None,
    title: str | None = None,
    description: str | None = None,
) -> dict:
    """Extract age range from a detail page.

    Sources (priority order):
      1. <dl> fields: "Alter"/"Zielgruppe" label
      2. Title keywords
      3. Description text (first match only)

    Returns: {age_min, age_max, age_raw} or empty dict.
    """
    # 1. <dl> fields
    if dl_fields:
        for label in ("Alter", "Zielgruppe", "Altersstufe", "Age"):
            field = dl_fields.get(label)
            if field:
                result = _parse_age_text(field["text"])
                if result:
                    return result

    # 2. Title
    if title:
        result = _parse_age_text(title)
        if result:
            return result

    # 3. Description (first match only)
    if description:
        result = _parse_age_text(description[:500])
        if result:
            return result

    return {}


def _parse_age_text(text: str) -> dict:
    """Parse age from a text string. Returns dict or empty dict."""
    # Range: "X-Y Jahre"
    m = _AGE_RANGE_RE.search(text)
    if m:
        return {
            "age_min": int(m.group(1)),
            "age_max": int(m.group(2)),
            "age_raw": m.group(0),
        }

    # Range: "X bis Y Jahre"
    m = _AGE_BIS_RE.search(text)
    if m:
        return {
            "age_min": int(m.group(1)),
            "age_max": int(m.group(2)),
            "age_raw": m.group(0),
        }

    # "Kinder ab X"
    m = _KINDER_AB_RE.search(text)
    if m:
        return {
            "age_min": int(m.group(1)),
            "age_raw": m.group(0),
        }

    # Minimum: "ab X Jahren"
    m = _AGE_MIN_RE.search(text)
    if m:
        return {
            "age_min": int(m.group(1)),
            "age_raw": m.group(0),
        }

    return {}


# ═══════════════════════════════════════════════════════════════════════
# Registration extraction
# ═══════════════════════════════════════════════════════════════════════

_REGISTRATION_LABELS = {"Anmeldung", "Anmelden", "Registration", "Booking", "Teilnahme"}

# Link text or URL patterns indicating registration
_REG_LINK_RE = re.compile(
    r"(anmeld|registr|booking|teilnahm|einschreib|sign.?up)",
    re.IGNORECASE,
)


def extract_registration(
    soup: BeautifulSoup,
    *,
    dl_fields: dict | None = None,
) -> dict:
    """Extract registration information from a detail page.

    Sources (priority order):
      1. <dl> fields: "Anmeldung" label → text + URL
      2. Links with registration-related text or URL patterns

    Returns: {registration_raw, registration_url} or empty dict.
    """
    # 1. <dl> fields
    if dl_fields:
        for label in _REGISTRATION_LABELS:
            field = dl_fields.get(label)
            if field:
                result: dict = {"registration_raw": field["text"]}
                if field.get("links"):
                    result["registration_url"] = field["links"][0]
                return result

    # 2. Scan for registration links
    for a in soup.find_all("a", href=True):
        href = (a.get("href") or "").strip()
        text = a.get_text(strip=True)

        if not href or href.startswith(("#", "mailto:", "tel:", "javascript:")):
            continue

        if _REG_LINK_RE.search(href) or _REG_LINK_RE.search(text):
            return {
                "registration_raw": text or "Anmeldung",
                "registration_url": href,
            }

    return {}


# ═══════════════════════════════════════════════════════════════════════
# Category extraction (source signals for taxonomy)
# ═══════════════════════════════════════════════════════════════════════

def extract_category(soup: BeautifulSoup) -> dict:
    """Extract category signals from a detail page.

    Sources:
      1. JSON-LD eventCategory / about
      2. Schema.org Event.eventAttendanceMode (online vs in-person)

    Returns: {category_raw, category_source} or empty dict.
    """
    import json
    for script in soup.find_all("script", type="application/ld+json"):
        try:
            data = json.loads(script.get_text() or "")
            items = [data] if isinstance(data, dict) else (data if isinstance(data, list) else [])
            for item in items:
                if not isinstance(item, dict) or item.get("@type") != "Event":
                    continue

                # eventCategory (non-standard but used by some platforms)
                cat = item.get("eventCategory") or item.get("about")
                if isinstance(cat, str) and cat.strip():
                    return {"category_raw": cat.strip(), "category_source": "jsonld"}
                if isinstance(cat, dict) and cat.get("name"):
                    return {"category_raw": cat["name"].strip(), "category_source": "jsonld"}
                if isinstance(cat, list) and cat:
                    names = [
                        c.get("name", c) if isinstance(c, dict) else str(c)
                        for c in cat if c
                    ]
                    if names:
                        return {"category_raw": ", ".join(str(n).strip() for n in names), "category_source": "jsonld"}
        except (json.JSONDecodeError, TypeError, AttributeError):
            continue

    return {}


# ═══════════════════════════════════════════════════════════════════════
# DL field parser (shared utility)
# ═══════════════════════════════════════════════════════════════════════

def extract_dl_fields(soup: BeautifulSoup) -> dict:
    """Extract key/value pairs from <dl> elements.

    Returns dict mapping label → {text: str, links: list[str]}.
    Works for ICMS <dl>/<dt>/<dd> patterns but is general enough
    for any page using definition lists for structured metadata.
    """
    fields: dict = {}
    for dl in soup.find_all("dl"):
        dts = dl.find_all("dt")
        dds = dl.find_all("dd")
        for dt, dd in zip(dts, dds):
            key = dt.get_text(strip=True)
            val_text = dd.get_text(" ", strip=True)
            val_links = [
                a.get("href", "")
                for a in dd.find_all("a", href=True)
                if not a.get("href", "").startswith("mailto:")
            ]
            if key and val_text:
                fields[key] = {"text": val_text, "links": val_links}
    return fields


# ═══════════════════════════════════════════════════════════════════════
# Unified scanner: call all extractors
# ═══════════════════════════════════════════════════════════════════════

def scan_detail_fields(
    soup: BeautifulSoup,
    *,
    title: str | None = None,
    description: str | None = None,
) -> dict:
    """Run all detail field extractors and return merged results.

    Called by adapters after primary extraction. Returns dict to merge
    into extra{}.
    """
    dl_fields = extract_dl_fields(soup)
    result: dict = {}

    price = extract_price(soup, dl_fields=dl_fields)
    if price:
        result.update(price)

    age = extract_age(soup, dl_fields=dl_fields, title=title, description=description)
    if age:
        result.update(age)

    reg = extract_registration(soup, dl_fields=dl_fields)
    if reg:
        result.update(reg)

    cat = extract_category(soup)
    if cat:
        result.update(cat)

    return result
