from __future__ import annotations

import os
import re
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Dict, List, Optional, Tuple
from urllib.parse import urljoin, urlparse

from bs4 import BeautifulSoup

from ..base import BaseAdapter
from ..http import http_get
from ..structured_time import extract_jsonld_event, extract_time_element
from ..types import SourceConfig, ExtractedItem

_DETAIL_PATH_RE = re.compile(r"^/(?:_rte/anlass|anlaesseaktuelles)/(\d+)$")
_ESCAPED_DETAIL_RE = re.compile(r"\\/(?:_rte\\/anlass|anlaesseaktuelles)\\/\d+")

_IWEB_IMG_RE = re.compile(
    r"(https?:\/\/api\.i-web\.ch\/public\/guest\/getImageString\/[^\s\"'>]+)",
    re.IGNORECASE,
)


def _normalize_datetime_text(raw: str) -> str:
    """
    Normalize only dot-time tokens like 8.00 -> 08:00,
    while preserving original German format including 'Uhr'
    and en-dash so normalize.py regex still matches.
    """
    s = (raw or "").strip()
    if not s:
        return s

    # Only act if we detect dot-time pattern
    if not re.search(r"\b\d{1,2}\.\d{2}\b", s):
        return s

    def _time_repl(m: re.Match[str]) -> str:
        hh = int(m.group(1))
        mm = int(m.group(2))
        return f"{hh:02d}:{mm:02d}"

    s = re.sub(r"\b(\d{1,2})\.(\d{2})\b", _time_repl, s)
    return s


def _is_junk_title(title: str) -> bool:
    t = (title or "").strip().lower()
    if not t:
        return True
    return t in ("kopfzeile", "fusszeile") or t.startswith("kopfzeile") or t.startswith("fusszeile")


@dataclass(frozen=True)
class _DetailResult:
    item: Optional[ExtractedItem]
    stats_delta: Dict[str, int]
    used_js: bool
    fetch_seconds: float


class MaennedorfPortalAdapter(BaseAdapter):
    """
    Performance optimizations (contract-safe):
    - Listing page: render_js=False (links are in static HTML).
    - Detail pages: try render_js=False first; fallback to render_js=True only if extraction fails.
    - Detail pages fetched concurrently with bounded workers.
    """

    def fetch(self, cfg: SourceConfig) -> List[ExtractedItem]:
        t0 = time.perf_counter()

        # Listing: no JS needed (links are in static HTML)
        t_list0 = time.perf_counter()
        res = http_get(cfg.seed_url, render_js=False)
        html = res.text or ""
        soup = BeautifulSoup(html, "html.parser")
        t_list1 = time.perf_counter()

        href_paths: List[str] = []
        for a in soup.find_all("a", href=True):
            h = (a.get("href") or "").strip()
            if not h:
                continue
            path = h.split("?")[0].split("#")[0]
            if _DETAIL_PATH_RE.match(path):
                href_paths.append(path)

        escaped_hits = _ESCAPED_DETAIL_RE.findall(html)
        escaped_paths = [h.replace("\\/", "/") for h in escaped_hits]
        escaped_paths = [p for p in escaped_paths if _DETAIL_PATH_RE.match(p)]

        candidates = href_paths + escaped_paths

        parsed: List[tuple[int, str]] = []
        for p in candidates:
            m = _DETAIL_PATH_RE.match(p)
            if not m:
                continue
            parsed.append((int(m.group(1)), p))

        parsed.sort(key=lambda t: t[0], reverse=True)

        seen = set()
        detail_urls: List[str] = []
        for _, path in parsed:
            abs_url = urljoin(cfg.seed_url, path)
            if abs_url in seen:
                continue
            seen.add(abs_url)
            detail_urls.append(abs_url)

        detail_urls = detail_urls[: cfg.max_items]

        print("MaennedorfPortalAdapter: detail_urls:", len(detail_urls))
        if detail_urls:
            print("MaennedorfPortalAdapter: first detail url:", detail_urls[0])

        # ---- Detail fetching concurrency ----
        workers = int(os.getenv("CALOO_MAENNEDORF_WORKERS", "10").strip() or "10")
        workers = max(1, min(workers, 32))  # sensible cap
        allow_js_fallback = os.getenv("CALOO_MAENNEDORF_JS_FALLBACK", "true").strip().lower() in ("1", "true", "yes", "y")

        stats: Dict[str, int] = {
            "ok": 0,
            "skip_no_title": 0,
            "skip_junk_title": 0,
            "skip_no_datetime": 0,
            "skip_sitzung": 0,
            "detail_parse_failed": 0,
            "js_fallback_used": 0,
        }

        items: List[ExtractedItem] = []

        t_det0 = time.perf_counter()
        total_fetch_seconds = 0.0

        # We keep deterministic input order; completion order doesn't matter for storage/idempotency.
        with ThreadPoolExecutor(max_workers=workers) as pool:
            futures = [pool.submit(self._extract_from_detail_fast, cfg, url, allow_js_fallback) for url in detail_urls]

            for fut in as_completed(futures):
                try:
                    r: _DetailResult = fut.result()
                    total_fetch_seconds += r.fetch_seconds
                    if r.used_js:
                        stats["js_fallback_used"] += 1
                    for k, v in r.stats_delta.items():
                        stats[k] = stats.get(k, 0) + v
                    if r.item:
                        items.append(r.item)
                        stats["ok"] += 1
                except Exception as e:
                    stats["detail_parse_failed"] += 1
                    print("MaennedorfPortalAdapter: detail parse failed (future):", repr(e))

        t_det1 = time.perf_counter()

        # Note: items list is in completion order; that’s fine.
        # If you ever need stable ordering for debugging, sort by item_url or title here.

        print("MaennedorfPortalAdapter: items built:", len(items))
        print("MaennedorfPortalAdapter: stats:", stats)

        t1 = time.perf_counter()
        listing_s = t_list1 - t_list0
        details_s = t_det1 - t_det0
        total_s = t1 - t0
        avg_detail_s = (details_s / max(len(detail_urls), 1)) if detail_urls else 0.0
        avg_fetch_s = (total_fetch_seconds / max(len(detail_urls), 1)) if detail_urls else 0.0

        print(
            "[maennedorf][timing]"
            f" listing_s={listing_s:.2f}"
            f" details_s={details_s:.2f}"
            f" total_s={total_s:.2f}"
            f" urls={len(detail_urls)}"
            f" workers={workers}"
            f" js_fallback_used={stats.get('js_fallback_used', 0)}"
            f" avg_detail_s={avg_detail_s:.2f}"
            f" avg_fetch_s={avg_fetch_s:.2f}"
        )

        return items

    def enrich(self, cfg: SourceConfig, item: ExtractedItem) -> ExtractedItem:
        return item

    def _extract_from_detail_fast(self, cfg: SourceConfig, detail_url: str, allow_js_fallback: bool) -> _DetailResult:
        """
        Try detail fetch without JS first. If extraction yields no usable item (missing title/datetime),
        optionally retry with JS rendering once.
        """
        # Attempt 1: non-JS
        t0 = time.perf_counter()
        res1 = http_get(detail_url, render_js=False)
        html1 = res1.text or ""
        final_url1 = str(getattr(res1, "final_url", "") or detail_url)
        item1, delta1 = self._extract_from_detail_html(cfg, detail_url, final_url1, html1)
        t1 = time.perf_counter()

        if item1:
            return _DetailResult(item=item1, stats_delta=delta1, used_js=False, fetch_seconds=(t1 - t0))

        # If it was a sitzung skip, don't retry with JS.
        if delta1.get("skip_sitzung", 0) > 0:
            return _DetailResult(item=None, stats_delta=delta1, used_js=False, fetch_seconds=(t1 - t0))

        # Optional JS fallback if extraction failed
        if not allow_js_fallback:
            return _DetailResult(item=None, stats_delta=delta1, used_js=False, fetch_seconds=(t1 - t0))

        t2 = time.perf_counter()
        res2 = http_get(detail_url, render_js=True)
        html2 = res2.text or ""
        final_url2 = str(getattr(res2, "final_url", "") or detail_url)
        item2, delta2 = self._extract_from_detail_html(cfg, detail_url, final_url2, html2)
        t3 = time.perf_counter()

        # For stats, use the JS attempt outcome (it’s the “real” attempt)
        return _DetailResult(item=item2, stats_delta=delta2, used_js=True, fetch_seconds=(t3 - t2) + (t1 - t0))

    def _extract_from_detail_html(
        self,
        cfg: SourceConfig,
        detail_url: str,
        final_url: str,
        html: str,
    ) -> Tuple[Optional[ExtractedItem], Dict[str, int]]:
        """
        Pure extraction from HTML. Returns (ExtractedItem|None, stats_delta).
        """
        delta: Dict[str, int] = {
            "skip_no_title": 0,
            "skip_junk_title": 0,
            "skip_no_datetime": 0,
            "skip_sitzung": 0,
            "detail_parse_failed": 0,
        }

        soup = BeautifulSoup(html or "", "html.parser")

        # If platform redirects to /sitzung/, skip entirely
        try:
            path = urlparse(final_url).path or ""
            if "/sitzung/" in path:
                delta["skip_sitzung"] += 1
                return None, delta
        except Exception:
            pass

        # ---- Title extraction ----
        title = ""

        h1_nodes = []
        main = soup.select_one("main") or soup.select_one("article")
        if main:
            h1_nodes = main.select("h1")
        if not h1_nodes:
            h1_nodes = soup.select("h1")

        h1_texts = [h.get_text(" ", strip=True) for h in h1_nodes if h.get_text(strip=True)]
        h1_texts = [t.strip() for t in h1_texts if t and not _is_junk_title(t)]

        if h1_texts:
            title = h1_texts[-1]
        elif soup.title:
            tt = soup.title.get_text(" ", strip=True).strip()
            if tt and not _is_junk_title(tt):
                title = tt

        title = (title or "").strip()
        if not title:
            delta["skip_no_title"] += 1
            return None, delta
        if _is_junk_title(title):
            delta["skip_junk_title"] += 1
            return None, delta

        # ---- Image extraction (optional enrichment) ----
        def _extract_image_url() -> str | None:
            og = soup.select_one('meta[property="og:image"]')
            if og and og.get("content"):
                return urljoin(final_url, og["content"].strip())

            tw = soup.select_one('meta[name="twitter:image"]')
            if tw and tw.get("content"):
                return urljoin(final_url, tw["content"].strip())

            m = _IWEB_IMG_RE.search(html or "")
            if m:
                return m.group(1).strip()

            container = soup.select_one("main") or soup.select_one("article") or soup
            for img in container.select("img"):
                for attr in ("src", "data-src", "data-original", "data-lazy-src"):
                    v = (img.get(attr) or "").strip()
                    if not v:
                        continue
                    if re.search(r"(logo|icon|sprite|favicon)", v, re.I):
                        continue
                    return urljoin(final_url, v)

                srcset = (img.get("srcset") or "").strip()
                if srcset:
                    first = srcset.split(",")[0].strip().split(" ")[0]
                    if first:
                        return urljoin(final_url, first)

            for src in (soup.select("source[srcset]") or []):
                srcset = (src.get("srcset") or "").strip()
                if not srcset:
                    continue
                first = srcset.split(",")[0].strip().split(" ")[0]
                if first and not re.search(r"(logo|icon|sprite|favicon)", first, re.I):
                    return urljoin(final_url, first)

            for el in (soup.select("[style]") or []):
                style = (el.get("style") or "")
                m2 = re.search(r"background-image\s*:\s*url\(['\"]?([^'\")]+)", style, re.I)
                if m2:
                    u = m2.group(1).strip()
                    if u and not re.search(r"(logo|icon|sprite|favicon)", u, re.I):
                        return urljoin(final_url, u)

            return None

        image_url = _extract_image_url()

        # ---- Datetime / location extraction ----
        lead = soup.select_one(".icms-lead-container")

        datetime_raw: Optional[str] = None
        location_raw: Optional[str] = None
        extraction_method = "text_heuristic"

        structured = extract_jsonld_event(soup)
        if structured and structured.start_iso:
            extraction_method = "jsonld"
            datetime_raw = f"{structured.start_iso} | {structured.end_iso}" if structured.end_iso else structured.start_iso

        if not datetime_raw:
            now_utc = datetime.now(timezone.utc)
            structured2 = extract_time_element(soup, container=lead, reference_time=now_utc)
            if structured2 and structured2.start_iso:
                extraction_method = "time_element"
                datetime_raw = structured2.start_iso

        if not datetime_raw:
            lead_lines: List[str] = []
            if lead:
                lead_text = lead.get_text("\n", strip=True)
                lead_lines = [ln.strip() for ln in lead_text.split("\n") if ln.strip()]

            if lead_lines:
                dt_idx = None
                for i in range(len(lead_lines) - 1, -1, -1):
                    if "Uhr" in lead_lines[i]:
                        dt_idx = i
                        break
                if dt_idx is None:
                    for i in range(len(lead_lines) - 1, -1, -1):
                        if re.search(r"\d{4}", lead_lines[i]) or re.search(r"\d{1,2}\.\d{1,2}\.\d{4}", lead_lines[i]):
                            dt_idx = i
                            break

                if dt_idx is not None:
                    datetime_raw = lead_lines[dt_idx]
                    if dt_idx > 0:
                        location_raw = ", ".join(lead_lines[:dt_idx])

        if not location_raw and lead:
            lead_lines2 = [ln.strip() for ln in lead.get_text("\n", strip=True).split("\n") if ln.strip()]
            if lead_lines2 and extraction_method != "text_heuristic":
                loc_lines = [ln for ln in lead_lines2 if not re.search(r"\d{4}", ln)]
                if loc_lines:
                    location_raw = ", ".join(loc_lines)

        if not datetime_raw:
            delta["skip_no_datetime"] += 1
            return None, delta

        if extraction_method == "text_heuristic" and isinstance(datetime_raw, str):
            before = datetime_raw
            after = _normalize_datetime_text(datetime_raw)
            if after != before:
                print(f"[maennedorf] datetime_raw_normalized before={before!r} after={after!r} url={final_url}")
            datetime_raw = after

        # ---- Description (optional) ----
        description_raw = None
        main2 = soup.select_one("main") or soup.select_one(".content") or soup.select_one("article")
        if main2:
            txt = main2.get_text(" ", strip=True)
            description_raw = txt[:2000] if txt else None

        item = ExtractedItem(
            title_raw=title,
            datetime_raw=datetime_raw,
            location_raw=location_raw,
            description_raw=description_raw,
            item_url=final_url,
            extra={
                "adapter": "maennedorf_portal",
                "detail_parsed": True,
                "extraction_method": extraction_method,
                "image_url": image_url,
            },
            fetched_at=getattr(cfg, "now_utc", None),
        )
        return item, delta
