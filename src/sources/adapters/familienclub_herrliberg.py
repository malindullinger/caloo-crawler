"""
Familienclub Robinson Herrliberg adapter.

Strategy:
- Traverse the ai1ec paginated /agenda/ using page_offset~{N} navigation
- Each page covers ~3-5 days with ~7-10 event instances
- Extract detail URLs: /Veranstaltung/<slug>/?instance_id=<id>
- Deduplicate URLs across pages
- Fetch each detail page
- Extract datetime:
    1) Try JSON-LD (structured — best case)
    2) Try <time> element (ai1ec sometimes emits these)
    3) Search for ISO 8601 strings near "Repeats" label
    4) Parse German date text near "Wann:" as fallback
- Extract location from "Wo:" section
- Extract categories from linked tags

Classification: Tier A (structured ISO timestamps found on detail pages)
"""
from __future__ import annotations

import json
import re
import time
from typing import List
from urllib.parse import urljoin

from bs4 import BeautifulSoup

from ..base import BaseAdapter
from ..content_surfaces import scan_content_surfaces
from ..detail_fields import scan_detail_fields
from ..extraction import extract_image, extract_description
from ..http import http_get
from ..link_classifier import classify_page_links
from ..structured_time import extract_datetime_structured
from ..types import SourceConfig, ExtractedItem

# Pagination config
_MAX_PAGES = 40        # ~4 months of agenda at ~3-5 days per page
_PAGE_DELAY_S = 1.0    # polite delay between page fetches
_MIN_NEW_URLS = 0      # stop when a page yields 0 new URLs


# Detail page URL pattern: /Veranstaltung/<slug>/?instance_id=<digits>
_DETAIL_URL_RE = re.compile(
    r"/Veranstaltung/[^/]+/\?instance_id=\d+"
)

# German date pattern: "12. März 2026 um 8:30 – 11:30"
_GERMAN_DATE_RE = re.compile(
    r"(\d{1,2})\.\s*"
    r"(Januar|Februar|März|April|Mai|Juni|Juli|August|September|Oktober|November|Dezember)"
    r"\s+(\d{4})"
    r"(?:\s+um\s+(\d{1,2})[.:](\d{2}))?"
    r"(?:\s*[–-]\s*(\d{1,2})[.:](\d{2}))?",
    re.IGNORECASE,
)

_GERMAN_MONTHS = {
    "januar": "01", "februar": "02", "märz": "03", "april": "04",
    "mai": "05", "juni": "06", "juli": "07", "august": "08",
    "september": "09", "oktober": "10", "november": "11", "dezember": "12",
}


class FamilienclubHerrlibergAdapter(BaseAdapter):
    """
    TIER A SOURCE — COMMUNITY ANCHOR
    =================================
    Classification: Tier A (structured ISO timestamps on detail pages)
    Source: Familienclub Robinson Herrliberg
    Platform: WordPress + All in One Event Calendar (ai1ec / Timely)

    Produces: recurring play groups, seasonal family events, courses
    """

    def fetch(self, cfg: SourceConfig) -> List[ExtractedItem]:
        # ── Phase 1: Discover detail URLs across paginated agenda ──────
        detail_urls = self._discover_all_detail_urls(cfg)

        self._detail_urls_found = len(detail_urls)

        # Respect max_items
        detail_urls = detail_urls[: cfg.max_items]

        # ── Phase 2: Fetch each detail page ────────────────────────────
        items = self._fetch_detail_pages(
            detail_urls,
            self._extract_from_detail,
            adapter_name="FamilienclubHerrliberg",
            delay_every=5,
            delay_s=0.5,
        )

        print(f"[herrliberg] Items extracted: {len(items)} from {len(detail_urls)} detail URLs")
        return items

    def _discover_all_detail_urls(self, cfg: SourceConfig) -> List[str]:
        """Traverse ai1ec paginated agenda to collect all detail URLs.

        ai1ec pagination: /agenda/action~agenda/page_offset~{N}/
        Each page covers ~3-5 days. We traverse until:
          - A page yields zero new detail URLs, or
          - We hit _MAX_PAGES, or
          - We've collected enough URLs (2x max_items as buffer)
        """
        seen: set[str] = set()
        ordered: List[str] = []
        url_limit = cfg.max_items * 2  # generous buffer before dedup
        pages_attempted = 0
        pages_succeeded = 0

        for page_num in range(_MAX_PAGES):
            pages_attempted += 1
            # Build page URL
            if page_num == 0:
                page_url = cfg.seed_url
            else:
                # ai1ec pattern: /agenda/action~agenda/page_offset~{N}/
                base = cfg.seed_url.rstrip("/")
                page_url = f"{base}/action~agenda/page_offset~{page_num}/"

            try:
                res = http_get(page_url)
                html = res.text or ""
                pages_succeeded += 1
            except Exception as e:
                print(f"[herrliberg] Page {page_num} fetch failed: {repr(e)}")
                break

            soup = BeautifulSoup(html, "html.parser")
            page_urls = self._extract_detail_urls(soup, page_url)

            new_count = 0
            for u in page_urls:
                if u not in seen:
                    seen.add(u)
                    ordered.append(u)
                    new_count += 1

            print(f"[herrliberg] Page {page_num}: {len(page_urls)} links, {new_count} new (total: {len(ordered)})")

            # Stop conditions
            if new_count == _MIN_NEW_URLS:
                print(f"[herrliberg] No new URLs on page {page_num}, stopping pagination")
                break

            if len(ordered) >= url_limit:
                print(f"[herrliberg] URL limit reached ({url_limit}), stopping pagination")
                break

            # Polite delay between listing pages
            if page_num + 1 < _MAX_PAGES:
                time.sleep(_PAGE_DELAY_S)

        print(f"[herrliberg] Discovery complete: {len(ordered)} unique detail URLs from {min(page_num + 1, _MAX_PAGES)} pages")
        # Surface tracking: listing + pagination as one combined surface
        self._surfaces_attempted = 1 + (1 if pages_attempted > 1 else 0)
        self._surfaces_succeeded = (1 if pages_succeeded > 0 else 0) + (1 if pages_succeeded > 1 else 0)
        return ordered

    def enrich(self, cfg: SourceConfig, item: ExtractedItem) -> ExtractedItem:
        return item  # fetch() already parses detail pages

    def _extract_detail_urls(self, soup: BeautifulSoup, base_url: str) -> List[str]:
        """Extract unique event detail URLs from the agenda page."""
        seen: set[str] = set()
        urls: List[str] = []

        for a in soup.find_all("a", href=True):
            href = (a.get("href") or "").strip()
            if not href:
                continue

            # Match /Veranstaltung/<slug>/?instance_id=<id>
            if _DETAIL_URL_RE.search(href):
                abs_url = urljoin(base_url, href)
                if abs_url not in seen:
                    seen.add(abs_url)
                    urls.append(abs_url)

        return urls

    def _extract_from_detail(self, detail_url: str) -> ExtractedItem | None:
        """Extract event data from a detail page."""
        res = http_get(detail_url)
        soup = BeautifulSoup(res.text or "", "html.parser")

        # ── Title ──────────────────────────────────────────────
        # ai1ec detail pages: event title is in .entry-title, NOT <h1>
        # (h1 contains the site name "Familienclub Robinson")
        title = ""
        entry_title = soup.select_one(".entry-title")
        if entry_title and entry_title.get_text(strip=True):
            title = entry_title.get_text(" ", strip=True)
        if not title:
            # Fallback: og:title often has "Event Name (date)"
            og = soup.find("meta", property="og:title")
            if og and og.get("content"):
                raw = og["content"].strip()
                # Strip trailing " (YYYY-MM-DD)" that ai1ec appends
                title = re.sub(r"\s*\(\d{4}-\d{2}-\d{2}\)$", "", raw)
        if not title:
            # Last resort: <title> tag, take part before " | "
            if soup.title:
                raw = soup.title.get_text(" ", strip=True)
                title = raw.split("|")[0].strip()
        title = (title or "").strip()
        if not title:
            return None

        # ── Datetime extraction (tiered strategy) ─────────────
        # Tiers 1-3: structured extraction (JSON-LD → <time> → ISO text)
        datetime_raw, extraction_method = extract_datetime_structured(soup)

        # Tier 4: Parse German date text near "Wann:" label
        if not datetime_raw:
            datetime_raw = self._extract_german_datetime(soup)
            if datetime_raw:
                extraction_method = "german_text"

        if not datetime_raw:
            return None

        # ── Location ──────────────────────────────────────────
        location_raw = self._extract_location(soup)

        # ── Categories ────────────────────────────────────────
        categories = self._extract_categories(soup)

        # ── Description ───────────────────────────────────────
        description_raw = extract_description(
            soup, primary_selector=".entry-content", max_length=4000,
        )
        if not description_raw:
            article = soup.find("article") or soup.select_one(".entry-content")
            if article:
                txt = article.get_text(" ", strip=True)
                description_raw = txt[:4000] if txt else None

        # ── Image ────────────────────────────────────────────
        image_url = extract_image(soup, page_url=detail_url)

        # ── Organiser from JSON-LD ────────────────────────────
        organiser_info = self._extract_organiser_jsonld(soup)

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
                "adapter": "familienclub_herrliberg",
                "detail_parsed": True,
                "extraction_method": extraction_method,
                "categories": categories,
                **(organiser_info or {}),
                **({"image_url": image_url} if image_url else {}),
                **{k: v for k, v in surfaces.items() if v},
                **{k: v for k, v in detail.items() if v},
                **{k: v for k, v in link_cls.items() if v},
            },
            fetched_at=self.now_utc(),
        )

    def _extract_german_datetime(self, soup: BeautifulSoup) -> str | None:
        """Parse German date text from page content.

        Looks for patterns like "12. März 2026 um 8:30 – 11:30" and
        converts to ISO pipe-separated format for normalize.py.
        """
        page_text = soup.get_text("\n", strip=True)
        m = _GERMAN_DATE_RE.search(page_text)
        if not m:
            return None

        day = m.group(1).zfill(2)
        month = _GERMAN_MONTHS.get(m.group(2).lower())
        year = m.group(3)
        if not month:
            return None

        start_h = m.group(4)
        start_m = m.group(5)
        end_h = m.group(6)
        end_m = m.group(7)

        if start_h:
            start_iso = f"{year}-{month}-{day}T{start_h.zfill(2)}:{start_m}:00+01:00"
            if end_h:
                end_iso = f"{year}-{month}-{day}T{end_h.zfill(2)}:{end_m}:00+01:00"
                return f"{start_iso} | {end_iso}"
            return start_iso
        else:
            # Date only, no time
            return f"{year}-{month}-{day}"

    def _extract_location(self, soup: BeautifulSoup) -> str | None:
        """Extract location from the detail page.

        ai1ec detail pages typically show location as text after a
        "Wo:" label, either in dt/dd pairs or as plain text.
        """
        page_text = soup.get_text("\n", strip=True)

        # Look for "Wo:" section and extract following lines
        lines = [ln.strip() for ln in page_text.split("\n") if ln.strip()]
        for i, line in enumerate(lines):
            if line.strip().startswith("Wo:") or line.strip() == "Wo":
                # Collect location lines until next label or empty
                loc_parts: List[str] = []
                for j in range(i + 1, min(i + 5, len(lines))):
                    next_line = lines[j].strip()
                    # Stop at next label or section
                    if next_line.endswith(":") or next_line.startswith("Kontakt"):
                        break
                    if next_line:
                        loc_parts.append(next_line)
                if loc_parts:
                    return ", ".join(loc_parts)

        return None

    def _extract_categories(self, soup: BeautifulSoup) -> List[str]:
        """Extract event categories from linked tags."""
        categories: List[str] = []

        # ai1ec category links contain "cat_ids" in href or are in a
        # categories section
        for a in soup.find_all("a", href=True):
            href = a.get("href", "")
            if "cat_ids" in href or "/Veranstaltungskategorie/" in href:
                text = a.get_text(strip=True)
                if text and text not in categories:
                    categories.append(text)

        return categories

    @staticmethod
    def _extract_organiser_jsonld(soup: BeautifulSoup) -> dict | None:
        """Extract organiser from JSON-LD Event markup."""
        for script in soup.find_all("script", type="application/ld+json"):
            try:
                data = json.loads(script.get_text() or "")
                items = [data] if isinstance(data, dict) else (data if isinstance(data, list) else [])
                for item in items:
                    if isinstance(item, dict) and item.get("@type") == "Event":
                        org = item.get("organizer")
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
