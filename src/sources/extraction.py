"""Shared extraction helpers for adapter detail pages.

Reusable building blocks for extracting title, description, and other
fields from HTML detail pages. Used by multiple adapters.
"""
from __future__ import annotations

from bs4 import BeautifulSoup


def extract_title(
    soup: BeautifulSoup,
    *,
    primary_selector: str | None = None,
    strip_title_suffix: str | None = None,
) -> str | None:
    """Extract event title from an HTML page.

    Strategy (first match wins):
    1) primary_selector CSS match (if provided)
    2) <h1> element
    3) og:title meta tag
    4) <title> tag (optionally stripping suffix like " | Site Name")

    Returns stripped title string, or None if nothing found.
    """
    # 1) Custom primary selector
    if primary_selector:
        el = soup.select_one(primary_selector)
        if el and el.get_text(strip=True):
            return el.get_text(" ", strip=True)

    # 2) <h1>
    h1 = soup.find("h1")
    if h1 and h1.get_text(strip=True):
        return h1.get_text(" ", strip=True)

    # 3) og:title
    og = soup.find("meta", property="og:title")
    if og and (og.get("content") or "").strip():
        return og["content"].strip()

    # 4) <title> with optional suffix stripping
    if soup.title:
        raw = soup.title.get_text(" ", strip=True)
        if raw:
            if strip_title_suffix and strip_title_suffix in raw:
                return raw.split(strip_title_suffix)[0].strip()
            return raw

    return None
