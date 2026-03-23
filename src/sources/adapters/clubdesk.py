"""
ClubDesk adapter for Swiss association/club event calendars.

Strategy:
- Fetch agenda page (single page, no pagination)
- Parse event listing: h3 date headers + div.cd-tile-h-box event items
- Extract detail URLs from onclick attributes (signed URLs)
- Fetch each detail page for full event data
- Extract from labeled fields (Wann, Ort, Typ, Text)

Classification: Tier A (structured datetime on detail pages)
Platform: ClubDesk Vereinssoftware (clubdesk.com)
Family: ClubDesk (1/3)

First source: Familienclub Zollikon
"""
from __future__ import annotations

import re
from typing import Dict, List, Optional
from urllib.parse import urljoin

from bs4 import BeautifulSoup

from ..base import BaseAdapter
from ..content_surfaces import scan_content_surfaces
from ..detail_fields import scan_detail_fields
from ..extraction import extract_image
from ..http import http_get
from ..link_classifier import classify_page_links
from ..types import SourceConfig, ExtractedItem

# Extract detail URL from onclick: window.location.href='...'
_ONCLICK_URL_RE = re.compile(r"window\.location\.href='([^']+)'")

# Parse German date: "Mittwoch 08.04.2026" → "08.04.2026"
_DATE_RE = re.compile(r"(\d{2}\.\d{2}\.\d{4})")

# Parse time range: "13:15 - 17:45"
_TIME_RANGE_RE = re.compile(r"(\d{1,2}:\d{2})\s*-\s*(\d{1,2}:\d{2})")


class ClubDeskAdapter(BaseAdapter):
    """
    TIER A SOURCE — CLUBDESK VEREINSSOFTWARE
    =========================================
    Classification: Tier A (structured datetime on detail pages)
    Platform: ClubDesk (clubdesk.com)
    Family: ClubDesk (1/3)
    """

    def fetch(self, cfg: SourceConfig) -> List[ExtractedItem]:
        self._surfaces_attempted = 1

        res = http_get(cfg.seed_url)
        soup = BeautifulSoup(res.text or "", "html.parser")

        self._surfaces_succeeded = 1

        # Extract detail URLs from listing page
        detail_urls = self._extract_detail_urls(soup, cfg.seed_url)
        self._detail_urls_found = len(detail_urls)

        # Limit to max_items
        detail_urls = detail_urls[: cfg.max_items]

        # Fetch detail pages
        items = self._fetch_detail_pages(
            detail_urls,
            lambda url: self._extract_from_detail(url, cfg),
            adapter_name=f"ClubDeskAdapter [{cfg.source_id}]",
            delay_every=3,
            delay_s=1.0,
        )

        print(f"ClubDeskAdapter [{cfg.source_id}]: {len(items)} items extracted")
        return items

    def _extract_detail_urls(self, soup: BeautifulSoup, base_url: str) -> List[str]:
        """Extract detail page URLs from onclick attributes on the listing page."""
        urls: List[str] = []
        seen: set = set()

        for tile in soup.select("div.cd-tile-h-box"):
            onclick = tile.get("onclick", "")
            m = _ONCLICK_URL_RE.search(onclick)
            if m:
                path = m.group(1)
                url = urljoin(base_url, path)
                if url not in seen:
                    seen.add(url)
                    urls.append(url)

        return urls

    def _extract_from_detail(self, detail_url: str, cfg: SourceConfig) -> Optional[ExtractedItem]:
        """Extract event data from a ClubDesk detail page."""
        res = http_get(detail_url)
        soup = BeautifulSoup(res.text or "", "html.parser")

        # Title from h1
        h1 = soup.select_one("div.cd-block-content > h1")
        if not h1:
            h1 = soup.find("h1")
        title = h1.get_text(strip=True) if h1 else None
        if not title:
            return None

        # Extract labeled fields from detail page
        fields = self._extract_labeled_fields(soup)

        # DateTime from "Wann" field
        wann = fields.get("Wann", "")
        datetime_raw = wann if wann else None
        if not datetime_raw:
            return None

        # Location from "Ort" field
        location_raw = fields.get("Ort") or None

        # Description from rich text content
        description_raw = None
        desc_el = soup.select_one(".cd-data-value.cd-data-html.tinyMceContent")
        if desc_el:
            text = desc_el.get_text("\n\n", strip=True)
            if text and len(text) > 10:
                description_raw = text[:4000]

        # Image
        image_url = extract_image(soup, page_url=detail_url)

        # Content surfaces + detail fields + link classification
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
                "adapter": "clubdesk",
                "detail_parsed": True,
                "extraction_method": "clubdesk_labeled_fields",
                **({"image_url": image_url} if image_url else {}),
                **({"event_type": fields.get("Typ")} if fields.get("Typ") else {}),
                **{k: v for k, v in surfaces.items() if v},
                **{k: v for k, v in detail.items() if v},
                **{k: v for k, v in link_cls.items() if v},
            },
            fetched_at=self.now_utc(),
        )

    @staticmethod
    def _extract_labeled_fields(soup: BeautifulSoup) -> Dict[str, str]:
        """Extract label → value pairs from ClubDesk detail page.

        Structure: ul > li > (div.cd-data-label + div.cd-data-value)
        """
        fields: Dict[str, str] = {}
        for li in soup.select("li"):
            label_el = li.select_one(".cd-data-label")
            value_el = li.select_one(".cd-data-value")
            if label_el and value_el:
                label = label_el.get_text(strip=True)
                value = value_el.get_text(" ", strip=True)
                if label and value:
                    fields[label] = value
        return fields
