from __future__ import annotations

from typing import List

from ..base import BaseAdapter
from ..http import http_get
from ..types import SourceConfig, ExtractedItem


class ChurchHubAdapter(BaseAdapter):
    """
    Strategy:
    - Fetch hub homepage
    - Find the calendar link (e.g. "Die nächsten 30 Tage" -> kirchestaefa.ch/...)
    - Then parse that calendar page like a list page.
    - Enrich with detail pages if available.
    """

    def fetch(self, cfg: SourceConfig) -> List[ExtractedItem]:
        hub = http_get(cfg.seed_url).text

        # TODO: find the calendar URL in the hub HTML
        calendar_url = self._find_calendar_url(hub) or cfg.seed_url

        cal_html = http_get(calendar_url).text

        items: List[ExtractedItem] = []
        # TODO: parse calendar listing: title, datetime_raw, item_url, location
        return items[: cfg.max_items]

    def _find_calendar_url(self, html: str) -> str | None:
        # TODO: implement: find anchor text or href pattern
        return None
