from __future__ import annotations

import json
import re
import time
from datetime import datetime, timezone
from typing import Dict, List, Iterator, Any
from urllib.parse import urljoin, urlparse, parse_qs, unquote

from bs4 import BeautifulSoup

from ..base import BaseAdapter
from ..http import http_get
from ..structured_time import extract_jsonld_event
from ..types import SourceConfig, ExtractedItem


# ---------------------------------------
# Regexes
# ---------------------------------------

_EVENT_PATH_RE = re.compile(r"/e/", re.IGNORECASE)
_EVENT_ID_RE = re.compile(r"-(\d{6,})", re.IGNORECASE)
_ZH_HINT_RE = re.compile(r"(zurich|zürich|zuerich)", re.IGNORECASE)

# Minimum listing URLs before we consider the non-JS listing a failure
_LISTING_MIN_URLS = 3


def resolve_eventbrite_image_url(raw_url: str | None, page_url: str | None = None) -> str | None:
    """
    Resolve Eventbrite image URLs to canonical absolute CDN URLs.

    Eventbrite returns relative paths like:
      /e/_next/image?url=https%3A%2F%2Fimg.evbuc.com%2Fhttps%253A%252F%252Fcdn.evbuc.com%252Fimages%252F...&w=940&q=75

    Strategy:
    1. If URL contains '_next/image' with a 'url' query param, extract and double-decode it
       to get the underlying CDN URL (https://cdn.evbuc.com/images/...).
    2. If URL starts with '/', make it absolute using page_url domain.
    3. Otherwise return as-is.
    """
    if not raw_url:
        return None

    raw_url = raw_url.strip()
    if not raw_url:
        return None

    # Check for _next/image proxy pattern
    if "_next/image" in raw_url:
        # Make absolute for urlparse if needed
        abs_url = raw_url
        if abs_url.startswith("/"):
            domain = "https://www.eventbrite.com"
            if page_url:
                parsed = urlparse(page_url)
                domain = f"{parsed.scheme}://{parsed.netloc}"
            abs_url = domain + abs_url

        try:
            parsed = urlparse(abs_url)
            qs = parse_qs(parsed.query)
            url_param = qs.get("url", [None])[0]
            if url_param:
                # Double-decode: first decode from query string, then the inner encoding
                decoded = unquote(unquote(url_param))
                if decoded.startswith("http"):
                    return decoded
        except Exception:
            pass

        # Fallback: return the absolute _next/image URL
        if raw_url.startswith("/"):
            domain = "https://www.eventbrite.com"
            if page_url:
                p = urlparse(page_url)
                domain = f"{p.scheme}://{p.netloc}"
            return domain + raw_url
        return raw_url

    # Make relative URLs absolute
    if raw_url.startswith("/"):
        domain = "https://www.eventbrite.com"
        if page_url:
            p = urlparse(page_url)
            domain = f"{p.scheme}://{p.netloc}"
        return domain + raw_url

    return raw_url


class EventbriteAdapter(BaseAdapter):
    """
    Eventbrite adapter using JSON-LD for structured datetime extraction.

    Performance: non-JS first for both listing and detail pages;
    JS fallback only when extraction fails.
    """

    # ------------------------------------------------------------------
    # Entry
    # ------------------------------------------------------------------

    def fetch(self, cfg: SourceConfig) -> List[ExtractedItem]:
        t0 = time.perf_counter()

        stats: Dict[str, int] = {
            "ok": 0,
            "skip_no_title": 0,
            "skip_online": 0,
            "skip_not_zurich": 0,
            "skip_no_datetime": 0,
            "detail_parse_failed": 0,
            "listing_js_fallback_used": 0,
            "detail_nonjs_ok": 0,
            "detail_js_fallback_used": 0,
        }

        # --- Listing: try non-JS first ---
        t_list0 = time.perf_counter()
        print("[eventbrite] fetching listing (non-JS):", cfg.seed_url)
        res = http_get(cfg.seed_url, render_js=False)
        html = res.text or ""
        soup = BeautifulSoup(html, "html.parser")

        detail_urls = self._extract_event_urls(soup, cfg.seed_url)
        print(f"[eventbrite] listing URLs found (non-JS): {len(detail_urls)}")

        if len(detail_urls) < _LISTING_MIN_URLS:
            print(f"[eventbrite] non-JS listing yielded <{_LISTING_MIN_URLS} URLs, retrying with JS")
            stats["listing_js_fallback_used"] = 1
            res = http_get(cfg.seed_url, render_js=True)
            html = res.text or ""
            soup = BeautifulSoup(html, "html.parser")
            detail_urls = self._extract_event_urls(soup, cfg.seed_url)
            print(f"[eventbrite] listing URLs found (JS): {len(detail_urls)}")

        t_list1 = time.perf_counter()

        detail_urls = detail_urls[: cfg.max_items]

        items: List[ExtractedItem] = []
        total_detail_fetch_s = 0.0

        for url in detail_urls:
            try:
                t_d0 = time.perf_counter()
                item, detail_stats = self._extract_from_detail(url)
                t_d1 = time.perf_counter()
                total_detail_fetch_s += (t_d1 - t_d0)

                for k, v in detail_stats.items():
                    stats[k] = stats.get(k, 0) + v

                if item:
                    items.append(item)
                    stats["ok"] += 1
            except Exception as e:
                stats["detail_parse_failed"] += 1
                print(f"[eventbrite] detail parse failed: {url} | err: {repr(e)}")
                continue

        t1 = time.perf_counter()
        listing_s = t_list1 - t_list0
        total_s = t1 - t0

        print(f"[eventbrite] items built: {len(items)}")
        print(f"[eventbrite] stats: {stats}")
        print(
            f"[eventbrite][timing]"
            f" listing_s={listing_s:.2f}"
            f" details_s={total_detail_fetch_s:.2f}"
            f" total_s={total_s:.2f}"
            f" urls={len(detail_urls)}"
            f" listing_js_fallback={stats['listing_js_fallback_used']}"
            f" detail_nonjs_ok={stats['detail_nonjs_ok']}"
            f" detail_js_fallback={stats['detail_js_fallback_used']}"
        )

        return items

    # ------------------------------------------------------------------
    # Listing extraction
    # ------------------------------------------------------------------

    def _extract_event_urls(self, soup: BeautifulSoup, base_url: str) -> List[str]:
        urls: List[str] = []
        seen = set()

        for a in soup.find_all("a", href=True):
            href = (a.get("href") or "").strip()
            if not href:
                continue

            if not _EVENT_PATH_RE.search(href):
                continue

            clean = href.split("?")[0].split("#")[0]
            if not clean.startswith("http"):
                clean = urljoin(base_url, clean)

            # Must contain numeric event ID
            if not _EVENT_ID_RE.search(clean):
                continue

            if clean not in seen:
                seen.add(clean)
                urls.append(clean)

        return urls

    # ------------------------------------------------------------------
    # Detail extraction (non-JS first, JS fallback)
    # ------------------------------------------------------------------

    def _extract_from_detail(self, detail_url: str) -> tuple[ExtractedItem | None, Dict[str, int]]:
        detail_stats: Dict[str, int] = {}

        # Attempt 1: non-JS
        print("[eventbrite] parsing detail (non-JS):", detail_url)
        res1 = http_get(detail_url, render_js=False)
        html1 = res1.text or ""
        soup1 = BeautifulSoup(html1, "html.parser")

        item = self._try_extract(soup1, html1, detail_url)
        if item:
            detail_stats["detail_nonjs_ok"] = 1
            return item, detail_stats

        # Attempt 2: JS fallback
        print("[eventbrite] non-JS extraction failed, retrying with JS:", detail_url)
        detail_stats["detail_js_fallback_used"] = 1
        res2 = http_get(detail_url, render_js=True)
        html2 = res2.text or ""
        soup2 = BeautifulSoup(html2, "html.parser")

        item = self._try_extract(soup2, html2, detail_url)
        return item, detail_stats

    def _try_extract(self, soup: BeautifulSoup, html: str, detail_url: str) -> ExtractedItem | None:
        """
        Attempt full extraction from parsed HTML. Returns item or None.
        Does NOT print skip reasons (caller handles logging).
        """
        structured = extract_jsonld_event(soup)

        title = self._get_title_from_jsonld(soup) or self._get_title_from_page(soup)
        if not title:
            return None

        location_raw = (
            self._get_location_from_jsonld(soup)
            or self._extract_location_text(soup)
        )

        if self._is_online_event(soup, location_raw):
            return None

        if location_raw:
            if not self._looks_like_zurich(detail_url, location_raw):
                return None
        else:
            if not _ZH_HINT_RE.search(detail_url.lower()):
                return None

        if structured and structured.start_iso:
            extraction_method = "jsonld"
            if structured.end_iso:
                datetime_raw = f"{structured.start_iso} | {structured.end_iso}"
            else:
                datetime_raw = structured.start_iso
        else:
            extraction_method = "text_heuristic"
            datetime_raw = self._extract_datetime_text(soup)
            if not datetime_raw:
                return None

        raw_image_url = self._extract_image_url(soup)
        image_url = resolve_eventbrite_image_url(raw_image_url, detail_url)
        description_raw = self._get_description(soup)

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
                "image_url": image_url,
            },
            fetched_at=datetime.now(timezone.utc),
        )

    # ------------------------------------------------------------------
    # JSON-LD helpers (robust)
    # ------------------------------------------------------------------

    def _iter_jsonld_nodes(self, data: Any) -> Iterator[dict]:
        if isinstance(data, dict):
            if "@graph" in data and isinstance(data["@graph"], list):
                for n in data["@graph"]:
                    yield from self._iter_jsonld_nodes(n)
            yield data
            for v in data.values():
                yield from self._iter_jsonld_nodes(v)
        elif isinstance(data, list):
            for x in data:
                yield from self._iter_jsonld_nodes(x)

    def _get_title_from_jsonld(self, soup: BeautifulSoup) -> str | None:
        for script in soup.find_all("script", type="application/ld+json"):
            try:
                data = json.loads(script.get_text() or "")
            except Exception:
                continue

            for node in self._iter_jsonld_nodes(data):
                t = node.get("@type")
                types = t if isinstance(t, list) else [t]
                if "Event" in types:
                    name = node.get("name")
                    if name:
                        return str(name).strip()
        return None

    def _get_location_from_jsonld(self, soup: BeautifulSoup) -> str | None:
        for script in soup.find_all("script", type="application/ld+json"):
            try:
                data = json.loads(script.get_text() or "")
            except Exception:
                continue

            for node in self._iter_jsonld_nodes(data):
                t = node.get("@type")
                types = t if isinstance(t, list) else [t]
                if "Event" not in types:
                    continue

                loc = node.get("location")
                if isinstance(loc, list) and loc:
                    loc = loc[0]

                if isinstance(loc, dict):
                    if loc.get("@type") == "VirtualLocation":
                        return "Online"

                    name = str(loc.get("name") or "").strip()
                    addr = loc.get("address")

                    if isinstance(addr, dict):
                        locality = str(addr.get("addressLocality") or "").strip()
                        region = str(addr.get("addressRegion") or "").strip()
                        postal = str(addr.get("postalCode") or "").strip()
                        parts = [p for p in [name, locality, region, postal] if p]
                        return ", ".join(parts) if parts else (name or None)

                    if isinstance(addr, str):
                        addr = addr.strip()
                        if name and addr:
                            return f"{name}, {addr}"
                        return addr or name or None

                    return name or None

                if isinstance(loc, str):
                    return loc.strip() or None

        return None

    # ------------------------------------------------------------------
    # Fallback extraction
    # ------------------------------------------------------------------

    def _get_title_from_page(self, soup: BeautifulSoup) -> str | None:
        h1 = soup.find("h1")
        if h1:
            txt = h1.get_text(" ", strip=True)
            if txt:
                return txt

        og = soup.find("meta", property="og:title")
        if og and og.get("content"):
            return og["content"].strip()

        if soup.title:
            return soup.title.get_text(" ", strip=True)

        return None

    def _extract_datetime_text(self, soup: BeautifulSoup) -> str | None:
        for selector in [
            ".date-info",
            "[data-testid='event-date']",
            "time",
            ".event-details time",
        ]:
            el = soup.select_one(selector)
            if el:
                txt = el.get_text(" ", strip=True)
                if txt and re.search(r"\d{4}", txt):
                    return txt
        return None

    def _extract_location_text(self, soup: BeautifulSoup) -> str | None:
        for selector in [
            ".location-info",
            "[data-testid='event-location']",
            ".event-details .location",
        ]:
            el = soup.select_one(selector)
            if el:
                txt = el.get_text(" ", strip=True)
                if txt:
                    return txt
        return None

    def _get_description(self, soup: BeautifulSoup) -> str | None:
        og = soup.find("meta", property="og:description")
        if og and og.get("content"):
            d = og["content"].strip()
            return d[:2000] if d else None

        meta = soup.find("meta", attrs={"name": "description"})
        if meta and meta.get("content"):
            d = meta["content"].strip()
            return d[:2000] if d else None

        return None

    def _extract_image_url(self, soup: BeautifulSoup) -> str | None:
        og = soup.find("meta", property="og:image")
        if og and og.get("content"):
            u = og["content"].strip()
            return u or None

        tw = soup.find("meta", attrs={"name": "twitter:image"})
        if tw and tw.get("content"):
            u = tw["content"].strip()
            return u or None

        return None

    # ------------------------------------------------------------------
    # Filters
    # ------------------------------------------------------------------

    def _is_online_event(self, soup: BeautifulSoup, location_raw: str | None) -> bool:
        loc = (location_raw or "").strip().lower()
        if loc.startswith("online"):
            return True

        # Check JSON-LD for online attendance mode or VirtualLocation
        for script in soup.find_all("script", type="application/ld+json"):
            try:
                data = json.loads(script.get_text() or "")
            except Exception:
                continue
            for node in self._iter_jsonld_nodes(data):
                t = node.get("@type")
                types = t if isinstance(t, list) else [t]
                if "Event" not in types:
                    continue
                mode = node.get("eventAttendanceMode") or ""
                if "OnlineEventAttendanceMode" in str(mode):
                    return True
                loc_node = node.get("location")
                if isinstance(loc_node, dict) and loc_node.get("@type") == "VirtualLocation":
                    return True
                if isinstance(loc_node, list):
                    for ln in loc_node:
                        if isinstance(ln, dict) and ln.get("@type") == "VirtualLocation":
                            return True

        return False

    def _looks_like_zurich(self, url: str, location_raw: str | None) -> bool:
        u = (url or "").lower()
        if _ZH_HINT_RE.search(u):
            return True

        t = (location_raw or "").lower()
        if _ZH_HINT_RE.search(t):
            return True

        if re.search(r"\b(80|81)\d{2}\b", t):
            return True

        allow = [
            "küsnacht", "kuesnacht", "zollikon", "thalwil", "horgen",
            "meilen", "stäfa", "staefa", "uetikon", "männedorf", "maennedorf",
            "herrliberg", "kilchberg", "rüschlikon", "rueschlikon",
        ]
        return any(a in t for a in allow)
