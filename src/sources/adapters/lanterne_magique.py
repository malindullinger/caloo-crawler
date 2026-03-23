"""
Lanterne Magique / Zauberlaterne adapter — national children's cinema club.

Strategy:
- Fetch club-specific page (e.g., /de/clubs/mannedorf/)
- Extract venue info from header (fixed per club)
- Extract all screening dates from season list (li.liste_dates)
- Extract current film details (title, synopsis, image, duration)
- Generate one event per screening date

Classification: Tier A (explicit dates + times on club page)
Platform: lanterne-magique.org (national platform, custom CMS)
Family: Lanterne Magique (1/3)

First source: Zauberlaterne Männedorf (club_id=76)
"""
from __future__ import annotations

import re
from typing import List, Optional

from bs4 import BeautifulSoup

from ..base import BaseAdapter
from ..http import http_get
from ..types import SourceConfig, ExtractedItem

# German months for date parsing
_MONTHS_DE = {
    "januar": "01", "februar": "02", "marz": "03", "märz": "03",
    "april": "04", "mai": "05", "juni": "06",
    "juli": "07", "august": "08", "september": "09",
    "oktober": "10", "november": "11", "dezember": "12",
}

# Parse: "25. März 2026"
_DATE_RE = re.compile(
    r"(\d{1,2})\.\s*(\w+)\s+(\d{4})",
    re.IGNORECASE,
)

# Parse time from venue info: "Mittwoch : 13:30/15:30"
_TIMES_RE = re.compile(r"(\d{1,2}:\d{2})\s*/\s*(\d{1,2}:\d{2})")

# Parse duration: "dauert 1:35"
_DURATION_RE = re.compile(r"dauert\s+(\d+:\d{2})")


class LanterneMagiqueAdapter(BaseAdapter):
    """
    TIER A SOURCE — LANTERNE MAGIQUE / ZAUBERLATERNE
    =================================================
    Classification: Tier A (explicit dates + fixed venue per club)
    Platform: lanterne-magique.org (national children's cinema network)
    Family: Lanterne Magique (1/3)

    Produces: monthly children's film screenings (Oct-Jun)
    """

    def fetch(self, cfg: SourceConfig) -> List[ExtractedItem]:
        self._surfaces_attempted = 1

        res = http_get(cfg.seed_url)
        soup = BeautifulSoup(res.text or "", "html.parser")

        self._surfaces_succeeded = 1

        items = self._extract_screenings(soup, cfg)
        self._detail_urls_found = len(items)
        self._detail_urls_fetched = len(items)

        print(f"LanterneMagiqueAdapter [{cfg.source_id}]: {len(items)} screenings extracted")
        return items

    def _extract_screenings(self, soup: BeautifulSoup, cfg: SourceConfig) -> List[ExtractedItem]:
        """Extract all screening events from the club page."""
        now = self.now_utc()

        # Extract venue info from header
        venue_info = self._extract_venue(soup)
        location_raw = venue_info.get("location")
        times = venue_info.get("times", [])  # e.g., ["13:30", "15:30"]

        # Extract current film details
        film = self._extract_film(soup)

        # Extract all screening dates
        dates = self._extract_dates(soup)

        # Generate one event per screening date
        items: List[ExtractedItem] = []
        for date_iso in dates:
            if len(items) >= cfg.max_items:
                break

            # Use first showtime for datetime_raw
            time_str = times[0] if times else "14:00"
            datetime_raw = f"{date_iso}T{time_str}:00"

            title = f"Zauberlaterne: {film.get('title', 'Kinderfilm')}"

            items.append(ExtractedItem(
                title_raw=title,
                datetime_raw=datetime_raw,
                location_raw=location_raw,
                description_raw=film.get("description"),
                item_url=cfg.seed_url,
                extra={
                    "adapter": "lanterne_magique",
                    "extraction_method": "club_page_dates",
                    **({"image_url": film.get("image_url")} if film.get("image_url") else {}),
                    **({"film_title": film.get("title")} if film.get("title") else {}),
                    **({"duration": film.get("duration")} if film.get("duration") else {}),
                    **({"end_time": f"{date_iso}T{times[1]}:00"} if len(times) > 1 else {}),
                    **({"showtimes": times} if times else {}),
                },
                fetched_at=now,
            ))

        return items

    @staticmethod
    def _extract_venue(soup: BeautifulSoup) -> dict:
        """Extract venue info from the club page header."""
        result: dict = {}

        # Venue info is in div.sur_titre_section_date within the title area
        venue_block = soup.select_one("div.colonne_centrale_titre div.sur_titre_section_date")
        if not venue_block:
            return result

        text = venue_block.get_text("\n", strip=True)
        lines = [ln.strip() for ln in text.split("\n") if ln.strip()]

        # Parse times: look for "HH:MM/HH:MM" in any line
        for line in lines:
            m = _TIMES_RE.search(line)
            if m:
                result["times"] = [m.group(1), m.group(2)]
                break

        # Venue name + address from remaining lines
        venue_parts = []
        for line in lines[1:]:
            # Skip phone numbers, email, and time patterns
            if re.match(r"^[\d\s+]+$", line) or "@" in line:
                continue
            if re.match(r"^\d{1,2}:\d{2}", line):
                continue
            venue_parts.append(line)

        if venue_parts:
            result["location"] = ", ".join(venue_parts)

        return result

    @staticmethod
    def _extract_film(soup: BeautifulSoup) -> dict:
        """Extract current film details from the club page."""
        result: dict = {}

        # Film title — try multiple selectors
        for sel in [
            "h2.titre_paragraphe_une span.text_span_surlignage_blanc",
            "h2.titre_paragraphe_une",
            "div.colonne_gauche h2",
        ]:
            title_el = soup.select_one(sel)
            if title_el:
                text = title_el.get_text(strip=True).strip("«» ")
                # Skip section headers
                if text and text.lower() not in ("nächste vorstellung", "warum dieser film?", ""):
                    result["title"] = text
                    break

        # Description
        desc_el = soup.select_one("div.colonne_gauche.has-paragraphes > p")
        if desc_el:
            text = desc_el.get_text(strip=True)
            if text and len(text) > 10:
                result["description"] = text[:4000]

        # Image
        img = soup.select_one("img.image_asterix[src*='superpro.lanterne.ch']")
        if img and img.get("src"):
            result["image_url"] = img["src"]

        # Duration
        for li in soup.select("li.liste_prochaine_seance"):
            text = li.get_text(strip=True)
            m = _DURATION_RE.search(text)
            if m:
                result["duration"] = m.group(1)
                break

        return result

    @staticmethod
    def _extract_dates(soup: BeautifulSoup) -> List[str]:
        """Extract all screening dates from the season list.

        Returns list of ISO date strings (YYYY-MM-DD).
        """
        dates: List[str] = []

        for li in soup.select("li.liste_dates"):
            text = li.get_text(strip=True)
            m = _DATE_RE.search(text)
            if m:
                day = m.group(1).zfill(2)
                month_name = m.group(2).lower()
                year = m.group(3)
                month = _MONTHS_DE.get(month_name)
                if month:
                    dates.append(f"{year}-{month}-{day}")

        return dates
