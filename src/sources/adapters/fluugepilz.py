"""
Fluugepilz (Familienzentrum Erlenbach) adapter — WordPress Events Manager RSS.

Strategy:
- Single RSS fetch: /events/feed/ returns all events (no pagination)
- Parse date/time/location from <description> CDATA (NOT <pubDate>)
- No detail page fetching required — all structured data is in the feed
- No event narrative description available in the feed

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
from typing import List

from ..base import BaseAdapter
from ..http import http_get
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
        res = http_get(cfg.seed_url)
        items = self._parse_rss(res.text or "", cfg)
        print(f"FluugepilzAdapter: {len(items)} items extracted from RSS feed")
        return items

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
            description_raw=None,  # no narrative in RSS feed
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
