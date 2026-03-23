"""
Generic GOViS adapter for Swiss municipal event portals.

Strategy:
- Fetch listing page via HTTP (no JS rendering needed)
- Paginate via URL pattern: {seed_url}/eventsjsRequest/0/eventspage/{N}
- Extract detail URLs from li.event-item links
- Fetch each detail page via HTTP
- Extract from structured CSS selectors (consistent across GOViS municipalities)

Classification: Tier A (structured HTML with semantic classes)
Platform: GOViS CMS (Swiss eGovernment, backslash AG)
Family: GOViS (generic — serves any GOViS municipality)

Municipalities: Küsnacht, Hombrechtikon, Stäfa (+ Herrliberg when WAF resolves)
"""
from __future__ import annotations

import re
from typing import List, Optional
from urllib.parse import urljoin

from bs4 import BeautifulSoup

from ..base import BaseAdapter
from ..content_surfaces import scan_content_surfaces
from ..detail_fields import scan_detail_fields
from ..extraction import extract_image
from ..http import http_get
from ..link_classifier import classify_page_links
from ..types import SourceConfig, ExtractedItem

# Detail URL suffix: /event/{ID}/eventdate/{DATE_ID}
_DETAIL_URL_RE = re.compile(r"/event/\d+(?:/eventdate/\d+)?$")

# Max pagination pages (safety limit)
_MAX_PAGES = 30

# Page counter regex: "Seite X von Y"
_PAGE_TOTAL_RE = re.compile(r"Seite\s+\d+\s+von\s+(\d+)")


class GovisAdapter(BaseAdapter):
    """
    TIER A SOURCE — GENERIC GOViS CMS
    ===================================
    Classification: Tier A (structured HTML, semantic CSS classes)
    Platform: GOViS CMS (backslash AG)
    Family: GOViS (generic)

    Serves any GOViS municipality via config-only onboarding.
    No Playwright needed — listing + detail pages are static HTML.
    """

    def fetch(self, cfg: SourceConfig) -> List[ExtractedItem]:
        # Phase 1: Discover all detail URLs via HTTP pagination
        detail_urls, pages_fetched = self._discover_urls(cfg)

        self._surfaces_attempted = pages_fetched
        self._surfaces_succeeded = pages_fetched
        self._detail_urls_found = len(detail_urls)

        print(f"GovisAdapter [{cfg.source_id}]: discovered {len(detail_urls)} detail URLs across {pages_fetched} pages")

        # Phase 2: Fetch detail pages
        detail_urls = detail_urls[: cfg.max_items]
        items = self._fetch_detail_pages(
            detail_urls,
            lambda url: self._extract_from_detail(url),
            adapter_name=f"GovisAdapter [{cfg.source_id}]",
            delay_every=1,   # delay after every fetch (GOViS rate-limits aggressively)
            delay_s=2.0,
            circuit_breaker_threshold=10,
        )

        print(f"GovisAdapter [{cfg.source_id}]: extracted {len(items)} items")
        return items

    def _discover_urls(self, cfg: SourceConfig) -> tuple[List[str], int]:
        """Discover all event detail URLs by paginating through the listing."""
        seen: set[str] = set()
        ordered: List[str] = []

        # Fetch first page
        res = http_get(cfg.seed_url)
        soup = BeautifulSoup(res.text or "", "html.parser")

        # Extract URLs from first page
        new_urls = self._extract_urls_from_soup(soup, cfg.seed_url, seen)
        for u in new_urls:
            seen.add(u)
            ordered.append(u)

        # Determine total pages
        total_pages = 1
        m = _PAGE_TOTAL_RE.search(soup.get_text())
        if m:
            total_pages = min(int(m.group(1)), _MAX_PAGES)

        pages_fetched = 1

        # Fetch remaining pages (with polite delay)
        import time
        for page_num in range(2, total_pages + 1):
            time.sleep(1.0)  # polite delay between pagination requests
            page_url = f"{cfg.seed_url}/eventsjsRequest/0/eventspage/{page_num}"
            try:
                res = http_get(page_url)
                page_soup = BeautifulSoup(res.text or "", "html.parser")
                new_urls = self._extract_urls_from_soup(page_soup, cfg.seed_url, seen)
                for u in new_urls:
                    seen.add(u)
                    ordered.append(u)
                pages_fetched += 1
            except Exception as e:
                print(f"GovisAdapter [{cfg.source_id}]: page {page_num} fetch failed: {e}")
                break

        return ordered, pages_fetched

    @staticmethod
    def _extract_urls_from_soup(soup: BeautifulSoup, base_url: str, already_seen: set[str]) -> List[str]:
        """Extract event detail URLs from a GOViS listing page."""
        urls: List[str] = []
        for a in soup.select("li.event-item h2.event-title a[href]"):
            href = a.get("href", "").strip()
            if not href:
                continue
            abs_url = urljoin(base_url, href)
            if _DETAIL_URL_RE.search(abs_url) and abs_url not in already_seen:
                urls.append(abs_url)
        return urls

    def _extract_from_detail(self, detail_url: str) -> Optional[ExtractedItem]:
        """Extract event data from a GOViS detail page using CSS selectors."""
        res = http_get(detail_url)
        if res.status_code != 200:
            return None

        soup = BeautifulSoup(res.text or "", "html.parser")

        # Title
        title_el = soup.select_one("h1.mod-event__title")
        title = title_el.get_text(strip=True) if title_el else None
        if not title:
            return None

        # Date/time
        date_el = soup.select_one("p.mod-event__date")
        datetime_raw = date_el.get_text(" ", strip=True) if date_el else None
        if not datetime_raw:
            return None

        # Location
        location_raw = None
        loc_el = soup.select_one("div.mod-event__location p.location")
        if loc_el:
            # Get text, clean "Lageplan" suffix
            loc_text = loc_el.get_text(", ", strip=True)
            loc_text = re.sub(r",?\s*Lageplan\s*$", "", loc_text).strip()
            if loc_text:
                location_raw = loc_text

        # Description: lead + content
        description_parts: List[str] = []
        lead_el = soup.select_one("p.mod-event__lead")
        if lead_el:
            text = lead_el.get_text(strip=True)
            if text:
                description_parts.append(text)
        content_el = soup.select_one("div.mod-event__content")
        if content_el:
            text = content_el.get_text("\n\n", strip=True)
            if text:
                description_parts.append(text)
        description_raw = "\n\n".join(description_parts)[:4000] if description_parts else None

        # Organizer
        organizer_raw = None
        org_el = soup.select_one("div.mod-event__organisators .event-organisator-custom")
        if org_el:
            organizer_raw = org_el.get_text(strip=True)

        # Price/cost
        price_raw = None
        cost_el = soup.select_one("div.mod-event__bookinginfo")
        if cost_el:
            # Get text, skip "Kosten" header
            paragraphs = cost_el.find_all("p")
            for p in paragraphs:
                text = p.get_text(strip=True)
                if text and text.lower() not in ("kosten", "tickets"):
                    price_raw = text
                    break

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
                "adapter": "govis",
                "detail_parsed": True,
                "extraction_method": "govis_css_selectors",
                **({"image_url": image_url} if image_url else {}),
                **({"organiser": {"name": organizer_raw}} if organizer_raw else {}),
                **({"price_raw": price_raw} if price_raw else {}),
                **{k: v for k, v in surfaces.items() if v},
                **{k: v for k, v in detail.items() if v},
                **{k: v for k, v in link_cls.items() if v},
            },
            fetched_at=self.now_utc(),
        )
