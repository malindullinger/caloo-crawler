"""
Frauenverein Männedorf adapter — Contao CMS event module.

Strategy:
- Fetch listing page (single page, no pagination)
- Extract detail URLs from div.mod_eventlist > div.event > a[href]
- Fetch each detail page
- Extract datetime via JSON-LD (date-only ISO: "2026-03-26")
- Extract title via JSON-LD name (h1 unreliable on category sub-pages)
- Extract description from div.ce_text

Classification: Tier A (JSON-LD on every detail page, <time datetime> on listing)
Platform: Contao Open Source CMS (standard cal_events module)
Contao family: 1/3

Note: JSON-LD dates are date-only (no time component). Times appear only
in description text (e.g., "19.30 Uhr"). Location is unstructured (in
description text only). Both are left unparsed for now — conservative approach.

Title extraction: JSON-LD `name` is preferred over `<h1>` because Contao
category sub-pages (e.g., /termine-wandervoegel/) use the section heading
as h1, not the event title.
"""
from __future__ import annotations

import json
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


def _find_events(data) -> list[dict]:
    """Find Event objects in JSON-LD, handling top-level, @graph, and array."""
    events: list[dict] = []
    if isinstance(data, dict):
        if data.get("@type") == "Event":
            events.append(data)
        if "@graph" in data and isinstance(data["@graph"], list):
            for item in data["@graph"]:
                events.extend(_find_events(item))
    elif isinstance(data, list):
        for item in data:
            events.extend(_find_events(item))
    return events


class FrauenvereinMaennedorfAdapter(BaseAdapter):
    """
    TIER A SOURCE — CONTAO CMS (frauenverein-maennedorf.ch)
    ========================================================
    Classification: Tier A (JSON-LD Event on every detail page)
    Platform: Contao Open Source CMS — standard mod_eventlist / mod_eventreader
    Family: Contao (1/3)

    Produces: community events, hiking trips, game nights, children's item fairs,
              cultural meetups, cinema evenings
    """

    def fetch(self, cfg: SourceConfig) -> List[ExtractedItem]:
        # Surface tracking: single listing page
        self._surfaces_attempted = 1

        # Phase 1: fetch listing page and extract detail URLs
        detail_urls = self._discover_detail_urls(cfg)
        print(f"FrauenvereinAdapter: {len(detail_urls)} detail URLs discovered")

        self._surfaces_succeeded = 1 if detail_urls else 0
        self._detail_urls_found = len(detail_urls)

        # Respect max_items
        detail_urls = detail_urls[: cfg.max_items]

        # Phase 2: fetch each detail page
        items = self._fetch_detail_pages(
            detail_urls,
            self._extract_from_detail,
            adapter_name="FrauenvereinAdapter",
            delay_every=5,
            delay_s=0.5,
        )

        print(f"FrauenvereinAdapter: {len(items)} items extracted")
        return items

    def _discover_detail_urls(self, cfg: SourceConfig) -> List[str]:
        """Extract event detail URLs from the single listing page."""
        res = http_get(cfg.seed_url)
        soup = BeautifulSoup(res.text or "", "html.parser")

        # Contao mod_eventlist: each event is div.event containing an <a> link
        event_list = soup.select_one("div.mod_eventlist")
        if not event_list:
            print("FrauenvereinAdapter: WARN — div.mod_eventlist not found")
            return []

        urls: List[str] = []
        seen: set[str] = set()

        for event_div in event_list.select("div.event"):
            a = event_div.find("a", href=True)
            if not a:
                continue
            href = a.get("href", "").strip()
            if not href or href.startswith("#"):
                continue
            abs_url = urljoin(cfg.seed_url, href)
            if abs_url not in seen:
                seen.add(abs_url)
                urls.append(abs_url)

        return urls

    def _extract_from_detail(self, detail_url: str) -> ExtractedItem | None:
        """Extract event data from a Contao event detail page."""
        res = http_get(detail_url)
        soup = BeautifulSoup(res.text or "", "html.parser")

        # Title: JSON-LD name first (h1 unreliable on category sub-pages)
        title = self._extract_jsonld_name(soup) or extract_title(soup)
        if not title:
            return None

        # Datetime: JSON-LD first, then <time> element, then ISO text
        # Contao emits JSON-LD Event with date-only startDate/endDate
        datetime_raw, extraction_method = extract_datetime_structured(soup)
        if not datetime_raw:
            return None

        # Description: div.ce_text (Contao content element)
        description_raw = None
        description_raw = extract_description(
            soup, primary_selector="div.ce_text", max_length=4000,
        )
        if not description_raw:
            desc_el = soup.select_one("div.ce_text")
            if desc_el:
                txt = desc_el.get_text(" ", strip=True)
                description_raw = txt[:4000] if txt else None

        # Location: not structured on this site (embedded in description text)
        # Conservative: leave as None rather than risk incorrect extraction
        location_raw = None

        # Image
        image_url = extract_image(soup, page_url=detail_url)

        # Organiser from JSON-LD @graph
        organiser_info = self._extract_jsonld_organiser(soup)

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
                "adapter": "frauenverein_maennedorf",
                "detail_parsed": True,
                "extraction_method": extraction_method,
                **(organiser_info or {}),
                **({"image_url": image_url} if image_url else {}),
                **{k: v for k, v in surfaces.items() if v},
                **{k: v for k, v in detail.items() if v},
                **{k: v for k, v in link_cls.items() if v},
            },
            fetched_at=self.now_utc(),
        )

    @staticmethod
    def _extract_jsonld_name(soup: BeautifulSoup) -> str | None:
        """Extract event name from JSON-LD Event markup.

        Handles both top-level Event and @graph-wrapped Event (Contao uses @graph).
        """
        for script in soup.find_all("script", type="application/ld+json"):
            try:
                data = json.loads(script.get_text() or "")
                for event in _find_events(data):
                    name = event.get("name", "").strip()
                    if name:
                        return name
            except (json.JSONDecodeError, TypeError, AttributeError):
                continue
        return None

    @staticmethod
    def _extract_jsonld_organiser(soup: BeautifulSoup) -> dict | None:
        """Extract organiser from JSON-LD Event (@graph-aware)."""
        for script in soup.find_all("script", type="application/ld+json"):
            try:
                data = json.loads(script.get_text() or "")
                for event in _find_events(data):
                    org = event.get("organizer")
                    if isinstance(org, dict):
                        name = (org.get("name") or "").strip()
                        url = (org.get("url") or "").strip()
                        if name:
                            return {"organiser": {"name": name, **({"url": url} if url else {})}}
                    elif isinstance(org, str) and org.strip():
                        return {"organiser": {"name": org.strip()}}
            except (json.JSONDecodeError, TypeError, AttributeError):
                continue
        return None
