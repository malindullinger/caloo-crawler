from __future__ import annotations

import re
from datetime import datetime, timezone
from typing import List, Optional
from urllib.parse import urljoin

from bs4 import BeautifulSoup

from ..base import BaseAdapter
from ..http import http_get
from ..types import SourceConfig, ExtractedItem


# German month names (title-case) for date normalization
_MONTHS_DE = {
    "JANUAR": "Januar", "FEBRUAR": "Februar", "MÄRZ": "März",
    "APRIL": "April", "MAI": "Mai", "JUNI": "Juni",
    "JULI": "Juli", "AUGUST": "August", "SEPTEMBER": "September",
    "OKTOBER": "Oktober", "NOVEMBER": "November", "DEZEMBER": "Dezember",
    "JAN.": "Jan.", "FEB.": "Feb.", "MÄR.": "Mär.",
    "APR.": "Apr.", "MAI.": "Mai", "JUN.": "Jun.",
    "JUL.": "Jul.", "AUG.": "Aug.", "SEPT.": "Sept.",
    "OKT.": "Okt.", "NOV.": "Nov.", "DEZ.": "Dez.",
}

# Compact range: "9.-14. NOVEMBER 2026"
_COMPACT_RANGE_RE = re.compile(
    r"(\d{1,2})\.\s*-\s*(\d{1,2})\.\s*([A-ZÄÖÜa-zäöü.]+)\s+(\d{4})"
)

# Full range without start year: "4. MAI - 28. SEPTEMBER 2026"
_RANGE_NO_START_YEAR_RE = re.compile(
    r"(\d{1,2})\.\s*([A-ZÄÖÜa-zäöü.]+)\s+-\s+(\d{1,2})\.\s*([A-ZÄÖÜa-zäöü.]+)\s+(\d{4})"
)

# Full range with both years: "19. OKT. 2026 - 19. APRIL 2027"
_RANGE_BOTH_YEARS_RE = re.compile(
    r"(\d{1,2})\.\s*([A-ZÄÖÜa-zäöü.]+)\s+(\d{4})\s+-\s+(\d{1,2})\.\s*([A-ZÄÖÜa-zäöü.]+)\s+(\d{4})"
)


def _normalize_month(word: str) -> str:
    """Convert uppercase German month to title-case."""
    return _MONTHS_DE.get(word.strip(), word)


def _normalize_date_text(raw: str) -> str:
    """
    Normalize German date text into a format that normalize.py can parse.

    Handles:
      "11. JANUAR 2026"                    -> "11. Januar 2026"
      "9.-14. NOVEMBER 2026"              -> "9. November 2026 - 14. November 2026"
      "4. MAI  - 28. SEPTEMBER 2026"      -> "4. Mai 2026 - 28. September 2026"
      "19. OKT. 2026 - 19. APRIL 2027"   -> "19. Okt. 2026 - 19. April 2027"
    """
    s = re.sub(r"\s+", " ", raw.strip())

    # 1) Compact range: "9.-14. NOVEMBER 2026"
    m = _COMPACT_RANGE_RE.search(s)
    if m:
        d1, d2, mon, year = m.group(1), m.group(2), m.group(3), m.group(4)
        mon_norm = _normalize_month(mon)
        return f"{d1}. {mon_norm} {year} - {d2}. {mon_norm} {year}"

    # 2) Full range with both years: "19. OKT. 2026 - 19. APRIL 2027"
    m = _RANGE_BOTH_YEARS_RE.search(s)
    if m:
        d1, m1, y1, d2, m2, y2 = m.groups()
        return f"{d1}. {_normalize_month(m1)} {y1} - {d2}. {_normalize_month(m2)} {y2}"

    # 3) Range without start year: "4. MAI - 28. SEPTEMBER 2026"
    m = _RANGE_NO_START_YEAR_RE.search(s)
    if m:
        d1, m1, d2, m2, year = m.groups()
        return f"{d1}. {_normalize_month(m1)} {year} - {d2}. {_normalize_month(m2)} {year}"

    # 4) Single date: normalize month case
    for upper, title in _MONTHS_DE.items():
        if upper in s:
            s = s.replace(upper, title)
            break

    return s


class ElternvereinUetikonAdapter(BaseAdapter):
    """
    Adapter for Elternverein Uetikon am See.

    Events are listed on a single page (/veranstaltungen) rendered by FairGate CMS.
    Each event is a columnBox div with title, date, image, and a FairGate registration link.
    No detail pages exist — all data is extracted from the listing page.

    JS rendering is REQUIRED (FairGate is a SPA).
    """

    def fetch(self, cfg: SourceConfig) -> List[ExtractedItem]:
        url = cfg.seed_url
        print(f"[elternverein_uetikon] fetching: {url}")

        res = http_get(url, render_js=True)
        html = res.text or ""
        soup = BeautifulSoup(html, "html.parser")

        items: List[ExtractedItem] = []

        boxes = soup.find_all("div", class_="columnBox")
        print(f"[elternverein_uetikon] columnBox elements: {len(boxes)}")

        for box in boxes:
            box_id = box.get("box-id")
            if not box_id:
                continue

            text = box.get_text(" ", strip=True)
            if len(text) < 5:
                continue

            title = self._extract_title(box)
            if not title:
                continue

            date_text = self._extract_date_text(box, title)
            if not date_text:
                print(f"[elternverein_uetikon] skip (no date): {title[:60]}")
                continue

            datetime_raw = _normalize_date_text(date_text)

            image_url = self._extract_image_url(box, cfg.seed_url)
            booking_url = self._extract_booking_url(box)

            # item_url: stable constructed URL using box-id
            item_url = f"{cfg.seed_url.rstrip('/')}#box-{box_id}"

            extra = {
                "adapter": "elternverein_uetikon",
                "extraction_method": "text_heuristic",
            }
            if image_url:
                extra["image_url"] = image_url
            if booking_url:
                extra["booking_url"] = booking_url

            items.append(
                ExtractedItem(
                    title_raw=title,
                    datetime_raw=datetime_raw,
                    location_raw="Uetikon am See",
                    description_raw=None,
                    item_url=item_url,
                    extra=extra,
                    fetched_at=datetime.now(timezone.utc),
                )
            )

            if len(items) >= cfg.max_items:
                break

        print(
            f"[elternverein_uetikon] items built: {len(items)}",
            f"| stats: boxes={len(boxes)} kept={len(items)}",
        )
        return items

    # ------------------------------------------------------------------
    # Extraction helpers
    # ------------------------------------------------------------------

    def _extract_title(self, box) -> Optional[str]:
        """Extract event title from the columnBox text content."""
        # Title is typically in <strong> within the text widget
        strong = box.find("strong")
        if strong:
            title = strong.get_text(" ", strip=True)
            if title and len(title) > 2:
                return title

        # Fallback: first non-empty text in a <p>
        for p in box.find_all("p"):
            txt = p.get_text(" ", strip=True)
            if txt and len(txt) > 2 and not re.match(r"^\d", txt):
                return txt

        return None

    def _extract_date_text(self, box, title: str) -> Optional[str]:
        """Extract date string from the box text, excluding the title."""
        full_text = box.get_text(" ", strip=True)

        # Date comes after the title in the text
        idx = full_text.find(title)
        if idx >= 0:
            after_title = full_text[idx + len(title) :].strip()
        else:
            after_title = full_text

        # Look for date patterns: "DD. MONTH YYYY" or ranges
        date_match = re.search(
            r"\d{1,2}\..*?\d{4}",
            after_title,
        )
        if date_match:
            return date_match.group(0).strip()

        return None

    def _extract_image_url(self, box, base_url: str) -> Optional[str]:
        """Extract image URL from the columnBox."""
        img = box.find("img")
        if not img:
            return None
        src = (img.get("src") or "").strip()
        if not src:
            return None
        if not src.startswith("http"):
            src = urljoin(base_url, src)
        return src

    def _extract_booking_url(self, box) -> Optional[str]:
        """Extract FairGate booking/registration link."""
        for a in box.find_all("a", href=True):
            href = (a.get("href") or "").strip()
            if "fairgate" in href or "my-events" in href:
                return href
        return None
