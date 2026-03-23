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

import json
import re
from datetime import datetime, timezone
from typing import List
from urllib.parse import urljoin

from bs4 import BeautifulSoup

from ..base import BaseAdapter
from ..content_surfaces import scan_content_surfaces
from ..detail_fields import scan_detail_fields
from ..extraction import extract_title, extract_image, extract_description
from ..http import http_get
from ..link_classifier import classify_page_links
from ..structured_time import extract_datetime_structured
from ..types import SourceConfig, ExtractedItem


class EventbriteAdapter(BaseAdapter):
    """
    Eventbrite adapter using JSON-LD for structured datetime extraction.
    """

    def fetch(self, cfg: SourceConfig) -> List[ExtractedItem]:
        # Surface tracking: single listing page
        self._surfaces_attempted = 1

        # Fetch listing page
        res = http_get(cfg.seed_url)
        html = res.text or ""
        soup = BeautifulSoup(html, "html.parser")

        self._surfaces_succeeded = 1

        # Extract event detail URLs from listing
        detail_urls = self._extract_event_urls(soup, cfg.seed_url)
        print(f"EventbriteAdapter: found {len(detail_urls)} event URLs")

        self._detail_urls_found = len(detail_urls)

        # Limit to max_items
        detail_urls = detail_urls[: cfg.max_items]

        # Fetch each detail page
        items = self._fetch_detail_pages(
            detail_urls,
            self._extract_from_detail,
            adapter_name="EventbriteAdapter",
        )

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

        # Tiers 1-3 (Eventbrite: tier 1 / JSON-LD expected to always succeed)
        datetime_raw, extraction_method = extract_datetime_structured(soup)

        # Tier 4: Eventbrite CSS selector fallback (rare — JSON-LD almost always present)
        if not datetime_raw:
            datetime_raw = self._extract_datetime_text(soup)
            if datetime_raw:
                extraction_method = "text_heuristic"

        if not datetime_raw:
            return None

        # Title: prefer JSON-LD name when available, fall back to page extraction
        if extraction_method == "jsonld":
            title = self._get_title_from_jsonld(soup) or extract_title(soup)
        else:
            title = extract_title(soup)
        if not title:
            return None

        # Location: prefer JSON-LD when available
        if extraction_method == "jsonld":
            location_raw = self._get_location_from_jsonld(soup)
        else:
            location_raw = self._extract_location_text(soup)

        # Description: prefer full body content, fall back to meta
        description_raw = extract_description(
            soup,
            primary_selector=".structured-content-rich-text",
        ) or self._get_description(soup)

        # Image: og:image → JSON-LD → first content <img>
        image_url = extract_image(soup, page_url=detail_url)

        # Organiser from JSON-LD
        organiser_info = self._get_organiser_from_jsonld(soup) if extraction_method == "jsonld" else None

        # Content surfaces, detail fields, and link classification
        surfaces = scan_content_surfaces(soup, detail_url)
        detail = scan_detail_fields(soup, title=title, description=description_raw)
        link_cls = classify_page_links(surfaces.get("external_links", []))

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
                **(organiser_info or {}),
                **({"image_url": image_url} if image_url else {}),
                **{k: v for k, v in surfaces.items() if v},
                **{k: v for k, v in detail.items() if v},
                **{k: v for k, v in link_cls.items() if v},
            },
            fetched_at=datetime.now(timezone.utc),
        )

    def _get_title_from_jsonld(self, soup: BeautifulSoup) -> str | None:
        """Extract title from JSON-LD Event."""
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

    def _get_location_from_jsonld(self, soup: BeautifulSoup) -> str | None:
        """Extract location from JSON-LD Event."""
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

    def _get_organiser_from_jsonld(self, soup: BeautifulSoup) -> dict | None:
        """Extract organiser info from JSON-LD Event."""
        for script in soup.find_all("script", type="application/ld+json"):
            try:
                data = json.loads(script.get_text() or "")
                events = [data] if isinstance(data, dict) else (data if isinstance(data, list) else [])
                for item in events:
                    if isinstance(item, dict) and item.get("@type") == "Event":
                        org = item.get("organizer")
                        if isinstance(org, dict):
                            name = (org.get("name") or "").strip()
                            url = (org.get("url") or "").strip()
                            if name:
                                return {"organiser": {"name": name, **({"url": url} if url else {})}}
                        elif isinstance(org, str) and org.strip():
                            return {"organiser": {"name": org.strip()}}
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
