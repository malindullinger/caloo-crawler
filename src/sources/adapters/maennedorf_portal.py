from __future__ import annotations

import re
from typing import List
from urllib.parse import urljoin

from bs4 import BeautifulSoup

from ..base import BaseAdapter
from ..extraction import extract_title
from ..http import http_get
from ..structured_time import extract_datetime_structured
from ..types import SourceConfig, ExtractedItem


# Detail pages we want:
# - /_rte/anlass/7060739
# - /anlaesseaktuelles/7081397
_DETAIL_PATH_RE = re.compile(r"^/(?:_rte/anlass|anlaesseaktuelles)/(\d+)$")

# Escaped JSON-style paths we saw in rendered HTML:
# "\/_rte\/anlass\/6615883"
_ESCAPED_DETAIL_RE = re.compile(r"\\/(?:_rte\\/anlass|anlaesseaktuelles)\\/\d+")


class MaennedorfPortalAdapter(BaseAdapter):
    """
    TIER B SOURCE — MUNICIPAL EXCEPTION
    ===================================
    Classification: Tier B (Explicit text-based exception)
    Decision: 2026-02-09
    Approval: Explicitly approved for text-based datetime parsing

    This source does NOT provide structured datetime (no JSON-LD, no <time>).
    Text heuristics are QUARANTINED in this adapter only.
    See docs/tier-b-sources.md for constraints.

    Strategy:
    - Fetch list page with JS rendering (Playwright)
    - Extract detail URLs from href and escaped JSON strings
    - Sort by numeric id DESC (newest first)
    - Parse each detail page:
        1) Try JSON-LD (never found)
        2) Try <time> element (never found)
        3) Fall back to text heuristic (QUARANTINED HERE)
    """

    def fetch(self, cfg: SourceConfig) -> List[ExtractedItem]:
        res = http_get(cfg.seed_url, render_js=True)
        html = res.text or ""

        soup = BeautifulSoup(html, "html.parser")

        # ----------------------------
        # 1) Collect normal href paths
        # ----------------------------
        href_paths: List[str] = []
        for a in soup.find_all("a", href=True):
            h = (a.get("href") or "").strip()
            if not h:
                continue

            # drop query/hash for filtering
            path = h.split("?")[0].split("#")[0]

            # keep only matching detail paths
            if _DETAIL_PATH_RE.match(path):
                href_paths.append(path)

        # ----------------------------
        # 2) Collect escaped paths from raw HTML
        # Example: "\\/_rte\\/anlass\\/6615883"
        # ----------------------------
        escaped_hits = _ESCAPED_DETAIL_RE.findall(html)
        escaped_paths = [h.replace("\\/", "/") for h in escaped_hits]
        escaped_paths = [p for p in escaped_paths if _DETAIL_PATH_RE.match(p)]

        # ----------------------------
        # 3) Combine candidates (paths)
        # ----------------------------
        candidates = href_paths + escaped_paths

        # ----------------------------
        # 4) Extract numeric id, then sort by id DESC (newest first)
        # ----------------------------
        parsed: List[tuple[int, str]] = []
        for p in candidates:
            m = _DETAIL_PATH_RE.match(p)
            if not m:
                continue
            parsed.append((int(m.group(1)), p))

        parsed.sort(key=lambda t: t[0], reverse=True)

        # ----------------------------
        # 5) Dedupe while keeping sorted order + build absolute URLs
        # ----------------------------
        seen = set()
        detail_urls: List[str] = []
        for _, path in parsed:
            abs_url = urljoin(cfg.seed_url, path)
            if abs_url in seen:
                continue
            seen.add(abs_url)
            detail_urls.append(abs_url)

        # Respect max_items
        detail_urls = detail_urls[: cfg.max_items]

        print("MaennedorfPortalAdapter: detail_urls:", len(detail_urls))
        if detail_urls:
            print("MaennedorfPortalAdapter: first detail url:", detail_urls[0])

        # ----------------------------
        # 6) Fetch detail pages
        # ----------------------------
        items = self._fetch_detail_pages(
            detail_urls,
            lambda url: self._extract_from_detail(cfg, url),
            adapter_name="MaennedorfPortalAdapter",
        )

        print("MaennedorfPortalAdapter: items built:", len(items))
        return items

    def enrich(self, cfg: SourceConfig, item: ExtractedItem) -> ExtractedItem:
        # fetch() already parses detail pages
        return item

    def _extract_from_detail(self, cfg: SourceConfig, detail_url: str) -> ExtractedItem | None:
        res = http_get(detail_url)
        soup = BeautifulSoup(res.text or "", "html.parser")

        # ICMS puts event title in og:title, not h1 (h1 = site nav element)
        title = None
        og = soup.find("meta", property="og:title")
        if og and (og.get("content") or "").strip():
            title = og["content"].strip()
        if not title:
            title = extract_title(soup, strip_title_suffix=" - ")
        if not title:
            return None

        # Lead container for location + fallback datetime extraction
        lead = soup.select_one(".icms-lead-container")

        # Tiers 1-3: structured extraction (JSON-LD → <time> → ISO text)
        datetime_raw, extraction_method = extract_datetime_structured(soup, container=lead)

        location_raw = None

        # Tier 4: text heuristic (TIER B QUARANTINE)
        # This text parsing is ONLY allowed for this source.
        # Pattern: "D. Mon. YYYY, HH.MM Uhr - HH.MM Uhr"
        # No inference, no defaults — if ambiguous, preserve unknown-time semantics.
        if not datetime_raw:
            extraction_method = "text_heuristic"
            lead_lines: List[str] = []
            if lead:
                lead_text = lead.get_text("\n", strip=True)
                lead_lines = [ln.strip() for ln in lead_text.split("\n") if ln.strip()]

            if lead_lines:
                # Pick the best datetime line:
                # 1) Prefer a line containing "Uhr" (has time window)
                # 2) Else last line that contains a year/date-like pattern
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

        # Extract location from lead if not already set (for structured paths)
        if not location_raw and lead:
            lead_lines = [ln.strip() for ln in lead.get_text("\n", strip=True).split("\n") if ln.strip()]
            # For structured extraction, location is typically all lines before datetime
            # If we used structured extraction, try to find location in lead_lines
            if lead_lines and extraction_method != "text_heuristic":
                # Use all non-date lines as location
                loc_lines = [ln for ln in lead_lines if not re.search(r"\d{4}", ln)]
                if loc_lines:
                    location_raw = ", ".join(loc_lines)

        # DEBUG: print the extraction for specific pages
        if (
            "7060739" in detail_url
            or "7060691" in detail_url
            or "7060775" in detail_url
            or "7060772" in detail_url
        ):
            print("\n--- DEBUG extraction for", detail_url)
            print("extraction_method =", extraction_method)
            print("datetime_raw =", datetime_raw)
            print("location_raw =", location_raw)
            print("---\n")

        # RawEvent requires datetime_raw to be a string
        if not datetime_raw:
            return None

        # Optional description: keep it short
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
            item_url=detail_url,
            extra={
                "adapter": "maennedorf_portal",
                "detail_parsed": True,
                "extraction_method": extraction_method,
            },
            fetched_at=getattr(cfg, "now_utc", None),
        )
