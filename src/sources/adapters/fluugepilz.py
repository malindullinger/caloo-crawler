"""
Fluugepilz (Familienzentrum Erlenbach) adapter — WordPress Events Manager RSS.

Strategy:
- Single RSS fetch: /events/feed/ returns all events (no pagination)
- Parse date/time/location from <description> CDATA (NOT <pubDate>)
- Fetch each item's detail page to extract description + image
- Scan detail pages for content surfaces (PDFs, external links)

Description CDATA format (fixed):
  DD/MM/YYYY - HH:MM - HH:MM <br />VenueName <br />Street <br />City

Classification: Tier A (structured datetime + location in every RSS item)
Platform: WordPress + Events Manager v7 (Marcus Sykes)
Domain: xn--flgepilz-75aa.ch (punycode for flüügepilz.ch)
Family: WordPress Events Manager (1/3)

Note: <pubDate> is UTC-offset (event_start minus CET/CEST offset) and does NOT
represent the actual local event time. The <description> CDATA is the source of
truth for date, time, and location.
"""
from __future__ import annotations

import re
import xml.etree.ElementTree as ET
from typing import Dict, List, Optional

from bs4 import BeautifulSoup

from ..base import BaseAdapter
from ..content_surfaces import scan_content_surfaces
from ..detail_fields import scan_detail_fields
from ..extraction import extract_description, extract_image
from ..http import http_get
from ..link_classifier import classify_page_links
from ..types import SourceConfig, ExtractedItem

# Parse: DD/MM/YYYY - HH:MM - HH:MM <br />Venue <br />Street <br />City
_DESC_RE = re.compile(
    r"(\d{2}/\d{2}/\d{4})"           # date DD/MM/YYYY
    r"\s*-\s*(\d{2}:\d{2})"          # start time HH:MM
    r"\s*-\s*(\d{2}:\d{2})"          # end time HH:MM
    r"\s*<br\s*/>\s*"                 # separator
    r"(.+?)"                          # venue name
    r"\s*<br\s*/>\s*"                 # separator
    r"(.+?)"                          # street
    r"\s*<br\s*/>\s*"                 # separator
    r"(.+)"                           # city
)


class FluugepilzAdapter(BaseAdapter):
    """
    TIER A SOURCE — WORDPRESS EVENTS MANAGER RSS (xn--flgepilz-75aa.ch)
    =====================================================================
    Classification: Tier A (structured datetime + location in every item)
    Platform: WordPress + Events Manager v7
    Family: WordPress Events Manager (1/3)

    Produces: family events, parent-child meetups, library story hours,
              yoga classes, grief support groups
    """

    def fetch(self, cfg: SourceConfig) -> List[ExtractedItem]:
        # Surface tracking: single RSS feed
        self._surfaces_attempted = 1

        res = http_get(cfg.seed_url)
        items = self._parse_rss(res.text or "", cfg)

        self._surfaces_succeeded = 1 if items else 0
        self._detail_urls_found = len(items)

        # Phase 2: Enrich items with detail page data (description + image)
        items = self._enrich_with_detail_pages(items)

        print(f"FluugepilzAdapter: {len(items)} items extracted from RSS feed")
        return items

    def _enrich_with_detail_pages(self, items: List[ExtractedItem]) -> List[ExtractedItem]:
        """Fetch detail pages to add description, image, and content surfaces."""
        # Collect URLs for items that have a detail page link
        url_to_items: Dict[str, List[int]] = {}
        for i, item in enumerate(items):
            if item.item_url:
                url_to_items.setdefault(item.item_url, []).append(i)

        if not url_to_items:
            self._detail_urls_fetched = 0
            return items

        urls = list(url_to_items.keys())

        # Use _fetch_detail_pages with a lambda that returns detail data
        detail_results = self._fetch_detail_pages(
            urls,
            self._extract_detail_data,
            adapter_name="FluugepilzAdapter",
            delay_every=3,
            delay_s=1.0,
        )

        # Build lookup: url -> detail data
        detail_by_url: Dict[str, ExtractedItem] = {}
        for detail_item in detail_results:
            if detail_item.item_url:
                detail_by_url[detail_item.item_url] = detail_item

        # Merge detail data back into RSS items (don't override RSS fields)
        for item in items:
            if not item.item_url or item.item_url not in detail_by_url:
                continue

            detail = detail_by_url[item.item_url]

            # Add description (RSS has none)
            if detail.description_raw:
                item.description_raw = detail.description_raw

            # Merge extra fields (image_url, content surfaces)
            if detail.extra and item.extra:
                for key in ("image_url", "pdf_urls", "pdf_count", "external_link_count"):
                    if key in detail.extra:
                        item.extra[key] = detail.extra[key]
                item.extra["detail_page_fetched"] = True

        return items

    def _extract_detail_data(self, url: str) -> Optional[ExtractedItem]:
        """Fetch and extract description + image from a detail page."""
        res = http_get(url)
        html = res.text or ""
        if not html:
            return None

        soup = BeautifulSoup(html, "html.parser")

        description = extract_description(soup)
        image_url = extract_image(soup, page_url=url)
        surfaces = scan_content_surfaces(soup, url)
        detail = scan_detail_fields(soup, description=description)
        link_cls = classify_page_links(surfaces.get("external_links", []))

        # Return a lightweight ExtractedItem carrying only enrichment data
        extra: Dict = {
            **({"image_url": image_url} if image_url else {}),
            **{k: v for k, v in surfaces.items() if v},
            **{k: v for k, v in detail.items() if v},
            **{k: v for k, v in link_cls.items() if v},
        }

        return ExtractedItem(
            title_raw="",  # not used — RSS title takes precedence
            datetime_raw=None,
            location_raw=None,
            description_raw=description,
            item_url=url,
            extra=extra,
            fetched_at=self.now_utc(),
        )

    def _parse_rss(self, xml_text: str, cfg: SourceConfig) -> List[ExtractedItem]:
        """Parse RSS XML and extract events from <item> elements."""
        try:
            root = ET.fromstring(xml_text)
        except ET.ParseError as e:
            print(f"FluugepilzAdapter: RSS parse error: {e}")
            return []

        channel = root.find("channel")
        if channel is None:
            print("FluugepilzAdapter: no <channel> element in RSS")
            return []

        items: List[ExtractedItem] = []
        now = self.now_utc()

        for item_el in channel.findall("item"):
            extracted = self._extract_item(item_el, now)
            if extracted:
                items.append(extracted)
                if len(items) >= cfg.max_items:
                    break

        return items

    def _extract_item(self, item_el: ET.Element, now) -> ExtractedItem | None:
        """Extract a single event from an RSS <item> element."""
        title = (item_el.findtext("title") or "").strip()
        if not title:
            return None

        link = (item_el.findtext("link") or "").strip()
        desc_raw = (item_el.findtext("description") or "").strip()

        if not desc_raw:
            return None

        m = _DESC_RE.match(desc_raw)
        if not m:
            print(f"FluugepilzAdapter: description parse failed for '{title}': {desc_raw[:100]}")
            return None

        date_str, start_time, end_time, venue, street, city = (
            g.strip() for g in m.groups()
        )

        # Convert DD/MM/YYYY to ISO date YYYY-MM-DD
        day, month, year = date_str.split("/")
        iso_date = f"{year}-{month}-{day}"
        datetime_raw = f"{iso_date}T{start_time}:00"

        # Location: "Venue, Street, City"
        location_raw = f"{venue}, {street}, {city}"

        return ExtractedItem(
            title_raw=title,
            datetime_raw=datetime_raw,
            location_raw=location_raw,
            description_raw=None,  # enriched later from detail page
            item_url=link or None,
            extra={
                "adapter": "fluugepilz",
                "end_time": f"{iso_date}T{end_time}:00",
                "venue": venue,
                "street": street,
                "city": city,
                "extraction_method": "rss_description_cdata",
            },
            fetched_at=now,
        )
