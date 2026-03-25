"""
Forum-Magazin.ch adapter for Catholic parish event calendars.

Strategy:
- Fetch listing pages with category prefilters (kinder-und-familien, jugend)
- Parse listing table rows for date/time/title/location (canonical source)
- Follow HTMX pagination via /agenda/after/YYYYMMDD/ endpoint
- Deduplicate by numeric event ID across overlapping category filters
- Fetch detail pages for enrichment only (description, image, organizer)

Classification: Tier B (HTML selectors only, no JSON-LD)
  Note: Listing <time> elements provide clean ISO date/time attributes,
  making datetime extraction deterministic (no text heuristics required).
Platform: Django + HTMX (forum-magazin.ch, operated by Forum Pfarrblatt)
"""
from __future__ import annotations

import re
import time
from typing import List
from urllib.parse import urlencode, urljoin, urlparse

from bs4 import BeautifulSoup

from ..base import BaseAdapter
from ..content_surfaces import scan_content_surfaces
from ..detail_fields import scan_detail_fields
from ..extraction import extract_description, extract_image
from ..http import http_get
from ..link_classifier import classify_page_links
from ..types import SourceConfig, ExtractedItem

# Numeric event ID at end of URL path: /agenda/YYYYMMDD-slug-123456/
_EVENT_ID_RE = re.compile(r"-(\d+)/?$")

# Date from event URL path: /agenda/YYYYMMDD-slug/
_URL_DATE_RE = re.compile(r"/agenda/(\d{4})(\d{2})(\d{2})-")

# Default family-relevant categories
_DEFAULT_CATEGORIES = ["kinder-und-familien", "jugend"]

# Pagination config
_PAGE_DELAY_S = 1.0

# Detail fetch config
_DETAIL_DELAY_EVERY = 5
_DETAIL_DELAY_S = 1.0
_CIRCUIT_BREAKER_THRESHOLD = 5


class ForumMagazinAdapter(BaseAdapter):
    """
    TIER B SOURCE — CATHOLIC PARISH EVENTS (forum-magazin.ch)
    ==========================================================
    Classification: Tier B (HTML selectors, no JSON-LD)
      Datetime: deterministic from listing <time> elements (no heuristics)
    Platform: Django + HTMX (Forum Pfarrblatt)

    Produces: church services, concerts, community events, family programs
    Reusable across: Catholic parishes via region/parish filters in extra{}
    """

    def fetch(self, cfg: SourceConfig) -> List[ExtractedItem]:
        categories = (cfg.extra or {}).get("categories", _DEFAULT_CATEGORIES)
        region = (cfg.extra or {}).get("region")

        # Phase 1: collect listing rows across categories, deduping by event ID
        seen_ids: set[str] = set()
        listing_rows: list[dict] = []

        for cat in categories:
            remaining = cfg.max_items - len(listing_rows)
            if remaining <= 0:
                break
            rows = self._paginate_category(
                cfg.seed_url, cat, region, seen_ids, remaining,
            )
            listing_rows.extend(rows)

        listing_rows = listing_rows[: cfg.max_items]

        self._dom_items_visible = len(listing_rows)
        self._detail_urls_found = len(listing_rows)

        # Phase 2: create items from listing data, enrich from detail pages
        items = self._build_enriched_items(listing_rows)

        print(
            f"ForumMagazinAdapter [{cfg.source_id}]: "
            f"{len(items)} items from {len(listing_rows)} listing rows"
        )
        return items

    # ── Listing collection ──────────────────────────────────────────

    def _paginate_category(
        self,
        seed_url: str,
        category: str,
        region: str | None,
        seen_ids: set[str],
        remaining: int,
    ) -> list[dict]:
        """Fetch listing rows for one category, following HTMX pagination.

        Stops when:
        - no new (unseen) rows returned
        - no pagination button in response
        - remaining quota reached
        """
        all_rows: list[dict] = []
        params: dict[str, str] = {"category": category}
        if region:
            params["region"] = region

        url = f"{seed_url}?{urlencode(params)}"

        while remaining > 0:
            self._surfaces_attempted += 1
            try:
                res = http_get(url)
            except Exception as e:
                print(f"ForumMagazinAdapter: listing page failed: {repr(e)}")
                break
            soup = BeautifulSoup(res.text or "", "html.parser")
            self._surfaces_succeeded += 1

            rows, next_path = self._parse_listing_rows(soup, seed_url, seen_ids)

            if not rows:
                # No new rows — either empty page or all duplicates
                break

            all_rows.extend(rows)
            remaining -= len(rows)

            if not next_path or remaining <= 0:
                break

            # Build next page URL from server-provided path + our filter params
            url = urljoin(seed_url, next_path)
            separator = "&" if "?" in url else "?"
            url += separator + urlencode(params)

            time.sleep(_PAGE_DELAY_S)

        return all_rows

    def _parse_listing_rows(
        self,
        soup: BeautifulSoup,
        base_url: str,
        seen_ids: set[str],
    ) -> tuple[list[dict], str | None]:
        """Parse event rows from listing HTML.

        Handles date propagation across rows (th.agenda__day only appears
        on first event of each day due to rowspan). Falls back to extracting
        date from event URL if no header is available.

        Returns (rows, next_path):
        - rows: list of dicts with listing data for unseen events only
        - next_path: hx-get path for next page, or None
        """
        current_date: str | None = None
        rows: list[dict] = []
        next_path: str | None = None

        for tr in soup.select("tr.row--result"):
            classes = tr.get("class", [])

            # Pagination button row
            if "load-more" in classes:
                btn = tr.select_one("button[hx-get]")
                if btn:
                    next_path = (btn.get("hx-get") or "").strip()
                continue

            # Date header (only on first event of each day, via rowspan)
            th = tr.select_one("th.agenda__day")
            if th:
                time_el = th.select_one("time[datetime]")
                if time_el:
                    dt_val = (time_el.get("datetime") or "").strip()
                    if dt_val:
                        current_date = dt_val

            # Detail link (required)
            a = tr.select_one("a.agenda__event[href]")
            if not a:
                continue
            href = (a.get("href") or "").strip()
            if not href:
                continue
            detail_url = urljoin(base_url, href)

            # Dedup: numeric event ID preferred, canonical URL fallback
            event_id = self._extract_event_id(detail_url)
            dedup_key = event_id or detail_url
            if dedup_key in seen_ids:
                continue
            seen_ids.add(dedup_key)

            # Title (required)
            title_el = a.select_one("h1.agenda__event-title")
            title = (
                title_el.get_text(strip=True)
                if title_el
                else (a.get("title") or "").strip()
            )
            if not title:
                continue

            # Date: from header or URL fallback
            row_date = current_date or self._extract_date_from_url(detail_url)
            if not row_date:
                continue

            # Time: clean HH:MM:SS from listing (canonical, no synthesis)
            time_el = tr.select_one("time.agenda__time[datetime]")
            time_str = (
                (time_el.get("datetime") or "").strip() if time_el else ""
            )

            # Build datetime_raw — date-only when no time present
            if time_str:
                datetime_raw = f"{row_date}T{time_str}"
            else:
                datetime_raw = row_date

            # Location (optional)
            loc_el = a.select_one("p.agenda__event-location")
            location = loc_el.get_text(strip=True) if loc_el else None

            # Category (optional)
            cat_el = a.select_one("ul.agenda__event-category > li")
            category = cat_el.get_text(strip=True) if cat_el else None

            rows.append({
                "detail_url": detail_url,
                "event_id": event_id,
                "title": title,
                "datetime_raw": datetime_raw,
                "location": location,
                "category": category,
            })

        return rows, next_path

    # ── Detail enrichment ───────────────────────────────────────────

    def _build_enriched_items(
        self, listing_rows: list[dict],
    ) -> list[ExtractedItem]:
        """Create ExtractedItems from listing data, enriching via detail pages.

        Items are always created from listing data. Detail page enrichment
        is best-effort: HTTP failures trigger circuit breaker but do not
        drop the item.
        """
        items: list[ExtractedItem] = []
        consecutive_failures = 0
        detail_fetched = 0

        for i, row in enumerate(listing_rows):
            extra: dict = {
                "adapter": "forum_magazin",
                "extraction_method": "listing_time_element",
            }
            if row.get("event_id"):
                extra["event_id"] = row["event_id"]
            if row.get("category"):
                extra["category_raw"] = row["category"]

            description_raw: str | None = None

            # Attempt detail enrichment (skip if circuit breaker tripped)
            if consecutive_failures < _CIRCUIT_BREAKER_THRESHOLD:
                try:
                    res = http_get(row["detail_url"])
                    soup = BeautifulSoup(res.text or "", "html.parser")
                    detail_fetched += 1
                    consecutive_failures = 0
                    extra["detail_parsed"] = True

                    detail = self._extract_detail_data(
                        soup, row["detail_url"], row["title"],
                    )
                    description_raw = detail.pop("description", None)
                    for k, v in detail.items():
                        if v:
                            extra[k] = v

                except Exception as e:
                    consecutive_failures += 1
                    detail_fetched += 1
                    print(
                        f"ForumMagazinAdapter: detail failed: "
                        f"{row['detail_url']} err: {repr(e)}"
                    )
                    if consecutive_failures >= _CIRCUIT_BREAKER_THRESHOLD:
                        remaining = len(listing_rows) - i - 1
                        print(
                            f"ForumMagazinAdapter: CIRCUIT BREAKER — "
                            f"{consecutive_failures} consecutive failures, "
                            f"skipping {remaining} remaining detail fetches"
                        )
                        self._circuit_breaker_triggered = True

                # Polite delay between detail fetches
                if (
                    detail_fetched > 0
                    and detail_fetched % _DETAIL_DELAY_EVERY == 0
                    and i + 1 < len(listing_rows)
                ):
                    time.sleep(_DETAIL_DELAY_S)

            items.append(
                ExtractedItem(
                    title_raw=row["title"],
                    datetime_raw=row["datetime_raw"],
                    location_raw=row.get("location"),
                    description_raw=description_raw,
                    item_url=row["detail_url"],
                    extra=extra,
                    fetched_at=self.now_utc(),
                )
            )

        self._detail_urls_fetched = detail_fetched
        return items

    @staticmethod
    def _extract_detail_data(
        soup: BeautifulSoup, detail_url: str, title: str,
    ) -> dict:
        """Extract enrichment data from a detail page.

        Returns dict with optional keys: description, image_url, organiser,
        plus surface/detail/link-classification fields from shared modules.
        """
        data: dict = {}

        # Description
        desc = extract_description(
            soup, primary_selector=".event__content .prose.flow",
        )
        if desc:
            data["description"] = desc

        # Image
        image_url = extract_image(soup, page_url=detail_url)
        if image_url:
            data["image_url"] = image_url

        # Organizer: only extract when explicitly labeled "Veranstalter:"
        org_el = soup.select_one(".event__location__detail")
        if org_el:
            spans = org_el.find_all("span")
            found_label = False
            for span in spans:
                text = span.get_text(strip=True)
                if text.startswith("Veranstalter"):
                    found_label = True
                    continue
                if found_label and text:
                    data["organiser"] = {"name": text}
                    break

        # Shared modules: content surfaces, detail fields, link classification
        surfaces = scan_content_surfaces(soup, detail_url)
        detail_fields = scan_detail_fields(
            soup, title=title, description=data.get("description"),
        )
        link_cls = classify_page_links(
            surfaces.get("external_links", []),
        )

        for source in (surfaces, detail_fields, link_cls):
            for k, v in source.items():
                if v:
                    data[k] = v

        return data

    # ── Helpers ─────────────────────────────────────────────────────

    @staticmethod
    def _extract_event_id(url: str) -> str | None:
        """Extract trailing numeric event ID from URL path.

        URL pattern: /agenda/YYYYMMDD-slug-NUMERICID/
        Falls back to None if pattern doesn't match.
        """
        path = urlparse(url).path
        m = _EVENT_ID_RE.search(path)
        return m.group(1) if m else None

    @staticmethod
    def _extract_date_from_url(url: str) -> str | None:
        """Extract ISO date from event URL path (YYYYMMDD → YYYY-MM-DD).

        Fallback when listing row lacks a th.agenda__day header
        (e.g. pagination splits mid-day).
        """
        path = urlparse(url).path
        m = _URL_DATE_RE.search(path)
        if m:
            return f"{m.group(1)}-{m.group(2)}-{m.group(3)}"
        return None
