"""
Kirchenweb.ch adapter for Swiss reformed/catholic church event calendars.

Strategy:
- Fetch listing page to discover month navigation links
- Traverse each month page (agenda format) to collect detail URLs
- Fetch each detail page
- Extract datetime via JSON-LD (extract_datetime_structured)
- Extract title via h1 (extract_title)
- Extract location from span.veranstaltungLeadOrt (CSS) or JSON-LD Place.name (fallback)
- Extract description from div.vinfobeschreibung

Classification: Tier A (JSON-LD on every detail page)
Platform: kirchenweb.ch (TYPO3-based Swiss church CMS)
"""
from __future__ import annotations

import json
import re
import time
from typing import List
from urllib.parse import urljoin

from bs4 import BeautifulSoup

from ..base import BaseAdapter
from ..extraction import extract_title
from ..http import http_get
from ..structured_time import extract_datetime_structured
from ..types import SourceConfig, ExtractedItem

# Detail URL pattern: /veranstaltung/{numeric_id}
_DETAIL_URL_RE = re.compile(r"/veranstaltung/\d+$")

# Pagination config
_PAGE_DELAY_S = 1.0  # polite delay between month page fetches


class KirchenwebAdapter(BaseAdapter):
    """
    TIER A SOURCE — CHURCH CMS (kirchenweb.ch)
    ============================================
    Classification: Tier A (JSON-LD on every detail page)
    Platform: kirchenweb.ch (TYPO3-based, operated by Kirchenweb AG)

    Produces: church services, concerts, community events, youth programs
    Reusable across: reformed and catholic churches using kirchenweb.ch
    """

    def fetch(self, cfg: SourceConfig) -> List[ExtractedItem]:
        # Phase 1: discover month navigation URLs from the listing page
        month_urls = self._discover_month_urls(cfg)
        print(f"KirchenwebAdapter: {len(month_urls)} month pages to traverse")

        # Phase 2: traverse month pages to collect detail URLs
        detail_urls = self._collect_detail_urls(month_urls, cfg.seed_url)
        print(f"KirchenwebAdapter: {len(detail_urls)} unique detail URLs discovered")

        # Respect max_items
        detail_urls = detail_urls[: cfg.max_items]

        # Phase 3: fetch each detail page
        items = self._fetch_detail_pages(
            detail_urls,
            self._extract_from_detail,
            adapter_name="KirchenwebAdapter",
            delay_every=10,
            delay_s=0.5,
        )

        print(f"KirchenwebAdapter: {len(items)} items extracted")
        return items

    def _discover_month_urls(self, cfg: SourceConfig) -> List[str]:
        """Extract month navigation links from the listing page."""
        res = http_get(cfg.seed_url)
        soup = BeautifulSoup(res.text or "", "html.parser")

        month_urls: List[str] = []
        seen: set[str] = set()

        for a in soup.find_all("a", href=True):
            href = a.get("href", "").strip()
            if "sucheDarstellungTitel=" not in href:
                continue
            # Only month pages (März, April, März 2026, Januar 2027, etc.)
            # Current-year months omit the year suffix; next-year months include it
            text = a.get_text(strip=True)
            if not re.match(
                r"(?:Januar|Februar|März|April|Mai|Juni|Juli|August|"
                r"September|Oktober|November|Dezember)(?:\s+\d{4})?$",
                text,
            ):
                continue

            abs_url = urljoin(cfg.seed_url, href)
            if abs_url not in seen:
                seen.add(abs_url)
                month_urls.append(abs_url)

        return month_urls

    def _collect_detail_urls(
        self, month_urls: List[str], seed_url: str
    ) -> List[str]:
        """Traverse month pages and collect unique detail URLs."""
        seen: set[str] = set()
        ordered: List[str] = []

        for i, month_url in enumerate(month_urls):
            try:
                res = http_get(month_url)
                soup = BeautifulSoup(res.text or "", "html.parser")
            except Exception as e:
                print(f"KirchenwebAdapter: month page failed: {repr(e)}")
                continue

            # Extract detail URLs from a.agendaTitel links
            for a in soup.select("a.agendaTitel[href]"):
                href = a.get("href", "").strip()
                if not href:
                    continue
                abs_url = urljoin(seed_url, href.split("?")[0].split("#")[0])
                if _DETAIL_URL_RE.search(abs_url) and abs_url not in seen:
                    seen.add(abs_url)
                    ordered.append(abs_url)

            if i + 1 < len(month_urls):
                time.sleep(_PAGE_DELAY_S)

        return ordered

    def _extract_from_detail(self, detail_url: str) -> ExtractedItem | None:
        """Extract event data from a kirchenweb detail page."""
        res = http_get(detail_url)
        soup = BeautifulSoup(res.text or "", "html.parser")

        title = extract_title(soup)
        if not title:
            return None

        # Tier 1 expected: JSON-LD with full ISO timestamps
        datetime_raw, extraction_method = extract_datetime_structured(soup)
        if not datetime_raw:
            return None

        # Location: CSS first, JSON-LD Place.name fallback
        location_raw = None
        loc_el = soup.select_one("span.veranstaltungLeadOrt")
        if loc_el:
            location_raw = loc_el.get_text(" ", strip=True)

        # JSON-LD: extract location fallback + organiser
        organiser: dict | None = None
        for script in soup.find_all("script", type="application/ld+json"):
            try:
                ld = json.loads(script.get_text() or "")
                if isinstance(ld, dict) and ld.get("@type") == "Event":
                    # Location fallback (only if CSS extraction missed)
                    if not location_raw:
                        loc_obj = ld.get("location")
                        if isinstance(loc_obj, dict):
                            loc_name = (loc_obj.get("name") or "").strip()
                            if loc_name:
                                location_raw = loc_name
                    # Organiser
                    org_obj = ld.get("organizer")
                    if isinstance(org_obj, dict):
                        name = (org_obj.get("name") or "").strip()
                        url = (org_obj.get("url") or "").strip()
                        if name:
                            organiser = {"name": name, **({"url": url} if url else {})}
                    elif isinstance(org_obj, str) and org_obj.strip():
                        organiser = {"name": org_obj.strip()}
                    break
            except (json.JSONDecodeError, TypeError, AttributeError):
                continue

        # Description: div.vinfobeschreibung
        description_raw = None
        desc_el = soup.select_one("div.vinfobeschreibung")
        if desc_el:
            txt = desc_el.get_text(" ", strip=True)
            # Strip leading "Beschreibung" label that kirchenweb prepends
            if txt.startswith("Beschreibung "):
                txt = txt[len("Beschreibung "):].strip()
            description_raw = txt[:2000] if txt else None

        return ExtractedItem(
            title_raw=title,
            datetime_raw=datetime_raw,
            location_raw=location_raw,
            description_raw=description_raw,
            item_url=detail_url,
            extra={
                "adapter": "kirchenweb",
                "detail_parsed": True,
                "extraction_method": extraction_method,
                **({"organiser": organiser} if organiser else {}),
            },
            fetched_at=self.now_utc(),
        )
