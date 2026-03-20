from __future__ import annotations

import re
from typing import List
from urllib.parse import urljoin

from bs4 import BeautifulSoup

from ..base import BaseAdapter
from ..extraction import extract_title, extract_image, extract_description
from ..http import http_get
from ..structured_time import extract_datetime_structured
from ..types import SourceConfig, ExtractedItem


# Detail page URL pattern: /leben/freizeit/veranstaltungen.html/288/event/{EVENT_ID}
# Optional: .../eventdate/{DATE_ID}
_DETAIL_URL_RE = re.compile(r"/leben/freizeit/veranstaltungen\.html/288/event/\d+")

# German month names for date extraction
_DE_MONTHS = {
    "januar": 1, "februar": 2, "märz": 3, "april": 4,
    "mai": 5, "juni": 6, "juli": 7, "august": 8,
    "september": 9, "oktober": 10, "november": 11, "dezember": 12,
}

# GOViS German date pattern:
# "29. März 2026, 15:00 Uhr bis 16:30 Uhr"
# "Samstag, 29. März 2026, 15:00 Uhr bis 16:30 Uhr"
# "12. März 2026, 19:30 Uhr bis 20:00 Uhr"
# "25. März 2026" (date only)
_GOVIS_DATE_TIME_RE = re.compile(
    r"(?:(?:Montag|Dienstag|Mittwoch|Donnerstag|Freitag|Samstag|Sonntag),\s*)?"
    r"(\d{1,2})\.\s*"
    r"(Januar|Februar|März|April|Mai|Juni|Juli|August|September|Oktober|November|Dezember)"
    r"\s+(\d{4})"
    r"(?:,\s*(\d{1,2}):(\d{2})\s*Uhr"
    r"(?:\s*(?:bis|-)\s*(\d{1,2}):(\d{2})\s*Uhr)?)?",
    re.IGNORECASE,
)

# Pagination config
_MAX_PAGES = 10         # safety limit
_PAGE_NAV_DELAY_S = 2.0  # wait after clicking next page


class GemeindeHerrlibergAdapter(BaseAdapter):
    """
    TIER B SOURCE — MUNICIPAL GOViS CMS
    =====================================
    Classification: Tier B (Explicit text-based exception)
    Platform: GOViS CMS (Swiss eGovernment, backslash AG)

    Strategy:
    - Discover: Playwright pagination (AJAX-driven click-through)
    - Detail: Standard HTTP (static HTML, no JS rendering needed)
    - Datetime: 4-tier strategy (JSON-LD → <time> → ISO text → German text heuristic)
    - Expected: German text heuristic for all items (QUARANTINED HERE)
    """

    def fetch(self, cfg: SourceConfig) -> List[ExtractedItem]:
        # Phase 1: discover all detail URLs via Playwright pagination
        detail_urls = self._discover_urls_playwright(cfg)
        print(f"GemeindeHerrlibergAdapter: discovered {len(detail_urls)} detail URLs")

        self._detail_urls_found = len(detail_urls)

        # Phase 2: fetch each detail page with standard HTTP
        detail_urls = detail_urls[: cfg.max_items]
        items = self._fetch_detail_pages(
            detail_urls,
            lambda url: self._extract_from_detail(cfg, url),
            adapter_name="GemeindeHerrlibergAdapter",
            delay_every=5,
            delay_s=0.5,
        )

        print(f"GemeindeHerrlibergAdapter: extracted {len(items)} items")
        return items

    def _discover_urls_playwright(self, cfg: SourceConfig) -> List[str]:
        """Navigate listing pages via Playwright, clicking pagination to traverse all pages."""
        from playwright.sync_api import sync_playwright

        seen: set[str] = set()
        ordered: List[str] = []

        pages_attempted = 0
        pages_succeeded = 0

        with sync_playwright() as p:
            browser = p.chromium.launch(headless=True)
            try:
                context = browser.new_context(
                    user_agent=(
                        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                        "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36"
                    ),
                    viewport={"width": 1280, "height": 720},
                    locale="de-CH",
                )
                page = context.new_page()
                resp = page.goto(cfg.seed_url, wait_until="domcontentloaded", timeout=30000)

                # Check for WAF/rate-limit block
                if resp and resp.status >= 400:
                    print(f"GemeindeHerrlibergAdapter: listing page returned HTTP {resp.status} — aborting discovery")
                    return []

                page.wait_for_timeout(3000)  # let AJAX content render

                for page_num in range(_MAX_PAGES):
                    pages_attempted += 1
                    # Extract event URLs from current page
                    new_on_page = self._extract_urls_from_page(page, cfg.seed_url, seen)
                    if new_on_page:
                        pages_succeeded += 1
                    for u in new_on_page:
                        if u not in seen:
                            seen.add(u)
                            ordered.append(u)

                    print(f"  page {page_num + 1}: {len(new_on_page)} new URLs (total: {len(ordered)})")

                    # Debug: if first page yields 0 URLs, dump sample hrefs for selector debugging
                    if page_num == 0 and len(new_on_page) == 0:
                        html = page.content()
                        soup_dbg = BeautifulSoup(html, "html.parser")
                        sample_hrefs = [a.get("href", "") for a in soup_dbg.find_all("a", href=True)][:20]
                        print(f"  DEBUG: page HTML length={len(html)}, title={page.title()}")
                        print(f"  DEBUG: sample hrefs: {sample_hrefs[:10]}")
                        # Also check for pagination elements
                        pag = soup_dbg.select("[class*=pagination], [class*=paging], .page-nav")
                        print(f"  DEBUG: pagination elements found: {len(pag)}")
                        for el in pag[:3]:
                            print(f"    tag={el.name} class={el.get('class')} text={el.get_text(' ', strip=True)[:80]}")

                    # Try to navigate to next page
                    if not self._click_next_page(page):
                        print(f"  no next page found — stopping at page {page_num + 1}")
                        break

                    page.wait_for_timeout(int(_PAGE_NAV_DELAY_S * 1000))

            finally:
                browser.close()

        # Surface tracking: listing + pagination
        self._surfaces_attempted = 1 + (1 if pages_attempted > 1 else 0)
        self._surfaces_succeeded = (1 if pages_succeeded > 0 else 0) + (1 if pages_succeeded > 1 else 0)
        return ordered

    def _extract_urls_from_page(self, page, seed_url: str, already_seen: set[str]) -> List[str]:
        """Extract event detail URLs from the current Playwright page."""
        html = page.content()
        soup = BeautifulSoup(html, "html.parser")

        urls: List[str] = []
        for a in soup.find_all("a", href=True):
            href = (a.get("href") or "").strip()
            if not href:
                continue
            # Normalize relative URLs
            abs_url = urljoin(seed_url, href.split("?")[0].split("#")[0])
            if _DETAIL_URL_RE.search(abs_url) and abs_url not in already_seen:
                urls.append(abs_url)

        return urls

    def _click_next_page(self, page) -> bool:
        """Try to click the next-page button. Returns True if successful."""
        # GOViS pagination: try multiple selector strategies
        selectors = [
            "a[title='nächste Seite']",          # German "next page" title
            "a[title='Nächste Seite']",
            ".pagination .next a",                # common pagination pattern
            "li.next > a",
            ".govis-cms-pagination a.next",
            "a:has-text('»')",                    # Playwright text selector for »
            "a:has-text('›')",                    # single arrow
        ]
        for sel in selectors:
            try:
                el = page.locator(sel).first
                if el.is_visible(timeout=1000):
                    el.click()
                    return True
            except Exception:
                continue

        # Fallback: look for any pagination link with a higher page number
        try:
            # Check if there's a "current" page indicator and a next numbered link
            current = page.locator(".pagination .active, .pagination .current").first
            if current.is_visible(timeout=500):
                current_text = current.inner_text().strip()
                if current_text.isdigit():
                    next_num = int(current_text) + 1
                    next_link = page.locator(f".pagination a:has-text('{next_num}')").first
                    if next_link.is_visible(timeout=500):
                        next_link.click()
                        return True
        except Exception:
            pass

        return False

    def _extract_from_detail(self, cfg: SourceConfig, detail_url: str) -> ExtractedItem | None:
        """Fetch and extract data from a single GOViS event detail page."""
        res = http_get(detail_url)
        if res.status_code != 200:
            return None

        soup = BeautifulSoup(res.text or "", "html.parser")

        title = extract_title(soup, strip_title_suffix="|")
        if not title:
            return None

        # Tiers 1-3: structured extraction (JSON-LD → <time> → ISO text)
        datetime_raw, extraction_method = extract_datetime_structured(soup)

        location_raw = None
        organizer_raw = None

        # Tier 4: GOViS German text heuristic (TIER B QUARANTINE)
        if not datetime_raw:
            extraction_method = "govis_german_text"
            datetime_raw, location_raw, organizer_raw = self._extract_govis_fields(soup)

        # If still no location, try extracting it anyway
        if not location_raw:
            _, location_raw, organizer_raw = self._extract_govis_fields(soup)

        if not datetime_raw:
            return None

        # Description: shared helper → fallback to main content block
        description_raw = extract_description(soup, max_length=4000)
        if not description_raw:
            main = soup.select_one("main") or soup.select_one(".content") or soup.select_one("article")
            if main:
                txt = main.get_text(" ", strip=True)
                description_raw = txt[:4000] if txt else None

        # Image
        image_url = extract_image(soup, page_url=detail_url)

        return ExtractedItem(
            title_raw=title,
            datetime_raw=datetime_raw,
            location_raw=location_raw,
            description_raw=description_raw,
            item_url=detail_url,
            extra={
                "adapter": "gemeinde_herrliberg",
                "detail_parsed": True,
                "extraction_method": extraction_method,
                "organizer": organizer_raw,
                **({"image_url": image_url} if image_url else {}),
            },
            fetched_at=self.now_utc(),
        )

    def _extract_govis_fields(self, soup: BeautifulSoup) -> tuple[str | None, str | None, str | None]:
        """Extract datetime, location, and organizer from GOViS detail page text.

        GOViS detail pages display event metadata as labeled fields.
        Returns (datetime_raw, location_raw, organizer_raw).
        """
        page_text = soup.get_text("\n", strip=True)
        lines = [ln.strip() for ln in page_text.split("\n") if ln.strip()]

        datetime_raw = None
        location_raw = None
        organizer_raw = None

        # Strategy 1: Look for German date pattern anywhere in the text
        for line in lines:
            m = _GOVIS_DATE_TIME_RE.search(line)
            if m:
                # Reconstruct the datetime_raw string from the match
                datetime_raw = m.group(0).strip()
                break

        # Strategy 2: Look for labeled fields (GOViS convention)
        for i, line in enumerate(lines):
            lower = line.lower().strip()

            # Location: "Ort" or "Veranstaltungsort" label
            if lower in ("ort", "veranstaltungsort", "ort:") and i + 1 < len(lines):
                # Collect non-empty lines until next label or blank
                loc_parts = []
                for j in range(i + 1, min(i + 4, len(lines))):
                    next_line = lines[j].strip()
                    if not next_line or next_line.lower().rstrip(":") in (
                        "datum", "zeit", "veranstalter", "kontakt", "organisator",
                        "beschreibung", "ort", "kategorie", "kosten",
                    ):
                        break
                    loc_parts.append(next_line)
                if loc_parts:
                    location_raw = ", ".join(loc_parts)

            # Organizer: "Veranstalter" or "Organisator" label
            if lower in ("veranstalter", "organisator", "veranstalter:", "organisator:") and i + 1 < len(lines):
                next_line = lines[i + 1].strip()
                if next_line and next_line.lower().rstrip(":") not in (
                    "datum", "zeit", "ort", "kontakt", "beschreibung", "kategorie",
                ):
                    organizer_raw = next_line

        return datetime_raw, location_raw, organizer_raw
