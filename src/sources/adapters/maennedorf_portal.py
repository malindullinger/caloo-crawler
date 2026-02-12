from __future__ import annotations

import re
from datetime import datetime, timezone
from typing import List
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


def _is_junk_title(title: str) -> bool:
    t = (title or "").strip().lower()
    if not t:
        return True
    return t in ("kopfzeile", "fusszeile") or t.startswith("kopfzeile") or t.startswith("fusszeile")


class MaennedorfPortalAdapter(BaseAdapter):
    def fetch(self, cfg: SourceConfig) -> List[ExtractedItem]:
        res = http_get(cfg.seed_url, render_js=True)
        html = res.text or ""
        soup = BeautifulSoup(html, "html.parser")

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

        items: List[ExtractedItem] = []

        stats = {
            "ok": 0,
            "skip_no_title": 0,
            "skip_junk_title": 0,
            "skip_no_datetime": 0,
            "skip_sitzung": 0,
            "detail_parse_failed": 0,
        }

        for url in detail_urls:
            try:
                item = self._extract_from_detail(cfg, url, stats=stats)
                if item:
                    items.append(item)
                    stats["ok"] += 1
            except Exception as e:
                stats["detail_parse_failed"] += 1
                print("MaennedorfPortalAdapter: detail parse failed:", url, "err:", repr(e))
                continue

        print("MaennedorfPortalAdapter: items built:", len(items))
        print("MaennedorfPortalAdapter: stats:", stats)
        return items

    def enrich(self, cfg: SourceConfig, item: ExtractedItem) -> ExtractedItem:
        return item

    def _extract_from_detail(self, cfg: SourceConfig, detail_url: str, stats: dict) -> ExtractedItem | None:
        res = http_get(detail_url, render_js=True)
        html = res.text or ""
        soup = BeautifulSoup(html, "html.parser")

        # If the platform redirects some "anlass" ids to /sitzung/..., skip them entirely
        final_url = str(getattr(res, "final_url", "") or detail_url)
        try:
            path = urlparse(final_url).path or ""
            if "/sitzung/" in path:
                stats["skip_sitzung"] += 1
                return None
        except Exception:
            pass

        # ----------------------------
        # Title extraction:
        # pages often contain an early H1 "Kopfzeile"; use LAST non-junk H1.
        # ----------------------------
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
            stats["skip_no_title"] += 1
            return None
        if _is_junk_title(title):
            stats["skip_junk_title"] += 1
            return None

        # ----------------------------
        # Image extraction (robust)
        # ----------------------------
        def _extract_image_url() -> str | None:
            og = soup.select_one('meta[property="og:image"]')
            if og and og.get("content"):
                return urljoin(final_url, og["content"].strip())

            tw = soup.select_one('meta[name="twitter:image"]')
            if tw and tw.get("content"):
                return urljoin(final_url, tw["content"].strip())

            m = _IWEB_IMG_RE.search(html)
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

        lead = soup.select_one(".icms-lead-container")

        datetime_raw = None
        location_raw = None
        extraction_method = "text_heuristic"

        structured = extract_jsonld_event(soup)
        if structured and structured.start_iso:
            extraction_method = "jsonld"
            datetime_raw = f"{structured.start_iso} | {structured.end_iso}" if structured.end_iso else structured.start_iso

        if not datetime_raw:
            now_utc = datetime.now(timezone.utc)
            structured = extract_time_element(soup, container=lead, reference_time=now_utc)
            if structured and structured.start_iso:
                extraction_method = "time_element"
                datetime_raw = structured.start_iso

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
            lead_lines = [ln.strip() for ln in lead.get_text("\n", strip=True).split("\n") if ln.strip()]
            if lead_lines and extraction_method != "text_heuristic":
                loc_lines = [ln for ln in lead_lines if not re.search(r"\d{4}", ln)]
                if loc_lines:
                    location_raw = ", ".join(loc_lines)

        if not datetime_raw:
            stats["skip_no_datetime"] += 1
            return None

        description_raw = None
        main = soup.select_one("main") or soup.select_one(".content") or soup.select_one("article")
        if main:
            txt = main.get_text(" ", strip=True)
            description_raw = txt[:2000] if txt else None

        return ExtractedItem(
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
