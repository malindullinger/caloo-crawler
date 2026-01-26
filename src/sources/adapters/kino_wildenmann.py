from __future__ import annotations

from typing import List

from ..base import BaseAdapter
from ..http import http_get
from ..types import SourceConfig, ExtractedItem


class KinoWildenmannAdapter(BaseAdapter):
    """
    Strategy:
    - Fetch homepage/program page
    - Parse showtimes directly from list
    - item_url may be present for each film/event -> use it as detail page if needed
    """

    def fetch(self, cfg: SourceConfig) -> List[ExtractedItem]:
        html = http_get(cfg.seed_url).text

        items: List[ExtractedItem] = []
        # TODO parse:
        # title_raw = film/event name
        # datetime_raw = "30. Jan. 20:15" etc
        # item_url = detail link if available
        return items[: cfg.max_items]
