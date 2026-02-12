from __future__ import annotations

import json
import re
from datetime import datetime, timezone
from typing import List
from urllib.parse import urljoin

from bs4 import BeautifulSoup

from ..base import BaseAdapter
from ..http import http_get
from ..structured_time import extract_jsonld_event
from ..types import SourceConfig, ExtractedItem

_TICKET_URL_RE = re.compile(r"tickets?-\d+", re.IGNORECASE)
_EVENT_PATH_RE = re.compile(r"/e/", re.IGNORECASE)
_ZH_HINT_RE = re.compile(r"(zurich|zürich|zuerich)", re.IGNORECASE)


class EventbriteAdapter(BaseAdapter):
    """
    Eventbrite adapter using JSON-LD for structured datetime extraction.

    IMPORTANT:
    Eventbrite pages often require JS rendering to expose JSON-LD / title / location reliably.
    So we use render_js=True for listing + detail.
    """

    def fetch(self, cfg: SourceConfig) -> List[ExtractedItem]:
        res = http_get(cfg.seed_url, render_js=True)
        html = res.text or ""
        soup = BeautifulSoup(html, "html.parser")

        detail_urls = self._extract_event_urls(soup, cfg.seed_url)
        print(f"EventbriteAdapter: found {len(detail_urls)} event URLs")

        detail_urls = detail_urls[: cfg.max_items]

        items: List[ExtractedItem] = []
        for url in detail_urls:
            try:
                item = self._extract_from_detail(url)
                if item:
                    items.append(item)
            except Exception as e:
                print(f"[eventbrite] detail parse failed: {url} err: {repr(e)}")
                continue

        print(f"EventbriteAdapter: items built: {len(items)}")
        return items

    def _extract_event_urls(self, soup: BeautifulSoup, base_url: str) -> List[str]:
        urls: List[str] = []
        seen = set()

        for a in soup.find_all("a", href=True):
            href = (a.get("href") or "").strip()
            if not href:
                continue

            # Must look like an Eventbrite event URL
            if not _EVENT_PATH_RE.search(href):
                continue
            if not _TICKET_URL_RE.search(href):
                continue

            clean = href.split("?")[0].split("#")[0]
            if not clean.startswith("http"):
                clean = urljoin(base_url, clean)

            if clean not in seen:
                seen.add(clean)
                urls.append(clean)

        return urls

    def _extract_from_detail(self, detail_url: str) -> ExtractedItem | None:
        # JS render is key for Eventbrite
        res = http_get(detail_url, render_js=True)
        html = res.text or ""
        soup = BeautifulSoup(html, "html.parser")

        structured = extract_jsonld_event(soup)

        # title
        title = self._get_title_from_jsonld(soup) or self._get_title_from_page(soup)
        if not title:
            print("[eventbrite] skip (no title):", detail_url)
            return None

        # location (best effort)
        loc_jsonld = self._get_location_from_jsonld(soup)
        loc_text = self._extract_location_text(soup)
        location_raw = loc_jsonld or loc_text

        # Skip obvious online events
        if self._is_online_event(soup, location_raw):
            print("[eventbrite] skip (online):", detail_url, "| loc:", (location_raw or "")[:120])
            return None

        # Zurich filter: keep if URL or location signals Zurich.
        if not self._looks_like_zurich(detail_url, location_raw):
            print("[eventbrite] skip (not zurich):", detail_url, "| loc:", (location_raw or "")[:120])
            return None

        # datetime_raw
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
                print("[eventbrite] skip (no datetime):", detail_url)
                return None

        image_url = self._extract_image_url(soup)
        description_raw = self._get_description(soup)

        print("[eventbrite] KEEP:", title[:80], "|", detail_url)

        return ExtractedItem(
            title_raw=title,
            datetime_raw=datetime_raw,
            location_raw=location_raw,
            description_raw=description_raw,
            item_url=detail_url,
            extra={
                "adapter": "eventbrite-zurich",
                "detail_parsed": True,
                "extraction_method": extraction_method,
                "image_url": image_url,
            },
            fetched_at=datetime.now(timezone.utc),
        )

    # ----------------------------
    # Filters / heuristics
    # ----------------------------
    def _is_online_event(self, soup: BeautifulSoup, location_raw: str | None) -> bool:
        loc = (location_raw or "").strip().lower()
        if loc.startswith("online"):
            return True
        txt = soup.get_text(" ", strip=True).lower()
        return ("online event" in txt) or ("this is an online event" in txt)

    def _looks_like_zurich(self, url: str, location_raw: str | None) -> bool:
        u = (url or "").lower()
        if _ZH_HINT_RE.search(u):
            return True

        t = (location_raw or "").lower()
        if _ZH_HINT_RE.search(t):
            return True

        # Zurich-ish zip heuristic (broad but useful)
        if re.search(r"\b(80|81)\d{2}\b", t):
            return True

        # Add Goldküste towns as acceptable Zurich-area hits
        allow = [
            "küsnacht", "kuesnacht", "zollikon", "thalwil", "horgen",
            "meilen", "stäfa", "staefa", "uetikon", "männedorf", "maennedorf",
            "herrliberg", "kilchberg", "rüschlikon", "rueschlikon",
        ]
        return any(a in t for a in allow)

    # ----------------------------
    # JSON-LD helpers
    # ----------------------------
    def _get_title_from_jsonld(self, soup: BeautifulSoup) -> str | None:
        for script in soup.find_all("script", type="application/ld+json"):
            try:
                data = json.loads(script.get_text() or "")
            except Exception:
                continue

            nodes = data if isinstance(data, list) else [data]
            for node in nodes:
                if isinstance(node, dict) and node.get("@type") == "Event":
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

            nodes = data if isinstance(data, list) else [data]
            for node in nodes:
                if not (isinstance(node, dict) and node.get("@type") == "Event"):
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

    # ----------------------------
    # Page fallbacks
    # ----------------------------
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
        for selector in [".date-info", "[data-testid='event-date']", "time", ".event-details time"]:
            el = soup.select_one(selector)
            if el:
                txt = el.get_text(" ", strip=True)
                if txt and re.search(r"\d{4}", txt):
                    return txt
        return None

    def _extract_location_text(self, soup: BeautifulSoup) -> str | None:
        for selector in [".location-info", "[data-testid='event-location']", ".event-details .location"]:
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
