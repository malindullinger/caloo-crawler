"""
Eventbrite adapter for Zurich-area events.

Strategy:
- Fetch listing page for Zurich events
- Extract event detail URLs
- Fetch each detail page
- Extract JSON-LD Event data (startDate, endDate with full time info)
- Fallback to text extraction if JSON-LD missing

JSON-LD on Eventbrite detail pages provides:
- startDate: "2026-02-21T22:00:00+01:00" (ISO 8601 with timezone)
- endDate: "2026-02-22T04:00:00+01:00"
- name, location, etc.
"""
from __future__ import annotations

import re
from datetime import datetime, timezone
from typing import List
from urllib.parse import urljoin, urlparse

from bs4 import BeautifulSoup

from ..base import BaseAdapter
from ..http import http_get
from ..structured_time import extract_jsonld_event
from ..types import SourceConfig, ExtractedItem


class EventbriteAdapter(BaseAdapter):
    """
    Eventbrite adapter using JSON-LD for structured datetime extraction.
    """

    def fetch(self, cfg: SourceConfig) -> List[ExtractedItem]:
        # Fetch listing page
        res = http_get(cfg.seed_url)
        html = res.text or ""
        soup = BeautifulSoup(html, "html.parser")

        # Extract event detail URLs from listing
        detail_urls = self._extract_event_urls(soup, cfg.seed_url)
        print(f"EventbriteAdapter: found {len(detail_urls)} event URLs")

        # Limit to max_items
        detail_urls = detail_urls[: cfg.max_items]

        # Fetch each detail page
        items: List[ExtractedItem] = []
        for url in detail_urls:
            try:
                item = self._extract_from_detail(url)
                if item:
                    items.append(item)
            except Exception as e:
                print(f"EventbriteAdapter: detail parse failed: {url} err: {repr(e)}")
                continue

        print(f"EventbriteAdapter: items built: {len(items)}")
        return items

    def _extract_event_urls(self, soup: BeautifulSoup, base_url: str) -> List[str]:
        """Extract event detail URLs from listing page."""
        urls: List[str] = []
        seen = set()

        for a in soup.find_all("a", href=True):
            href = a.get("href", "").strip()
            if not href:
                continue

            # Eventbrite event URLs contain "/e/" and end with ticket ID
            # e.g., https://www.eventbrite.com/e/event-name-tickets-1234567890
            if "/e/" in href and re.search(r"tickets?-\d+", href):
                # Clean URL (remove query params)
                clean_url = href.split("?")[0]

                # Make absolute
                if not clean_url.startswith("http"):
                    clean_url = urljoin(base_url, clean_url)

                if clean_url not in seen:
                    seen.add(clean_url)
                    urls.append(clean_url)

        return urls

    def _extract_from_detail(self, detail_url: str) -> ExtractedItem | None:
        """Extract event data from detail page using JSON-LD."""
        res = http_get(detail_url)
        soup = BeautifulSoup(res.text or "", "html.parser")

        # Try JSON-LD extraction (primary method for Eventbrite)
        structured = extract_jsonld_event(soup)

        if structured and structured.start_iso:
            # JSON-LD found - use it
            extraction_method = "jsonld"

            # Get title from JSON-LD or fallback to page
            title = self._get_title_from_jsonld(soup) or self._get_title_from_page(soup)
            if not title:
                return None

            # Build datetime_raw using pipe separator
            if structured.end_iso:
                datetime_raw = f"{structured.start_iso} | {structured.end_iso}"
            else:
                datetime_raw = structured.start_iso

            # Get location from JSON-LD
            location_raw = self._get_location_from_jsonld(soup)

        else:
            # Fallback to text extraction (rare for Eventbrite)
            extraction_method = "text_heuristic"

            title = self._get_title_from_page(soup)
            if not title:
                return None

            datetime_raw = self._extract_datetime_text(soup)
            if not datetime_raw:
                return None

            location_raw = self._extract_location_text(soup)

        # Get description
        description_raw = self._get_description(soup)

        return ExtractedItem(
            title_raw=title,
            datetime_raw=datetime_raw,
            location_raw=location_raw,
            description_raw=description_raw,
            item_url=detail_url,
            extra={
                "adapter": "eventbrite",
                "detail_parsed": True,
                "extraction_method": extraction_method,
            },
            fetched_at=datetime.now(timezone.utc),
        )

    def _get_title_from_jsonld(self, soup: BeautifulSoup) -> str | None:
        """Extract title from JSON-LD Event."""
        import json

        for script in soup.find_all("script", type="application/ld+json"):
            try:
                data = json.loads(script.get_text() or "")
                if isinstance(data, dict) and data.get("@type") == "Event":
                    name = data.get("name")
                    if name:
                        return str(name).strip()
                elif isinstance(data, list):
                    for item in data:
                        if isinstance(item, dict) and item.get("@type") == "Event":
                            name = item.get("name")
                            if name:
                                return str(name).strip()
            except (json.JSONDecodeError, TypeError):
                continue
        return None

    def _get_title_from_page(self, soup: BeautifulSoup) -> str | None:
        """Extract title from page HTML."""
        # Try h1
        h1 = soup.find("h1")
        if h1:
            text = h1.get_text(" ", strip=True)
            if text:
                return text

        # Try og:title
        og = soup.find("meta", property="og:title")
        if og and og.get("content"):
            return og["content"].strip()

        # Try <title>
        if soup.title:
            return soup.title.get_text(" ", strip=True)

        return None

    def _get_location_from_jsonld(self, soup: BeautifulSoup) -> str | None:
        """Extract location from JSON-LD Event."""
        import json

        for script in soup.find_all("script", type="application/ld+json"):
            try:
                data = json.loads(script.get_text() or "")
                if isinstance(data, dict) and data.get("@type") == "Event":
                    loc = data.get("location", {})
                    if isinstance(loc, dict):
                        name = loc.get("name", "")
                        addr = loc.get("address", {})
                        if isinstance(addr, dict):
                            locality = addr.get("addressLocality", "")
                            if name and locality:
                                return f"{name}, {locality}"
                            return name or locality or None
                        elif isinstance(addr, str):
                            return f"{name}, {addr}" if name else addr
                        return name or None
            except (json.JSONDecodeError, TypeError):
                continue
        return None

    def _extract_datetime_text(self, soup: BeautifulSoup) -> str | None:
        """Fallback: extract datetime from page text."""
        # Look for common date/time patterns in Eventbrite pages
        # This is rarely needed since Eventbrite has excellent JSON-LD
        for selector in [".date-info", "[data-testid='event-date']", ".event-details time"]:
            el = soup.select_one(selector)
            if el:
                text = el.get_text(" ", strip=True)
                if text and re.search(r"\d{4}", text):
                    return text
        return None

    def _extract_location_text(self, soup: BeautifulSoup) -> str | None:
        """Fallback: extract location from page text."""
        for selector in [".location-info", "[data-testid='event-location']", ".event-details .location"]:
            el = soup.select_one(selector)
            if el:
                text = el.get_text(" ", strip=True)
                if text:
                    return text
        return None

    def _get_description(self, soup: BeautifulSoup) -> str | None:
        """Extract description from page."""
        # Try og:description
        og = soup.find("meta", property="og:description")
        if og and og.get("content"):
            desc = og["content"].strip()
            if desc:
                return desc[:2000]

        # Try meta description
        meta = soup.find("meta", attrs={"name": "description"})
        if meta and meta.get("content"):
            desc = meta["content"].strip()
            if desc:
                return desc[:2000]

        return None
