from __future__ import annotations

from typing import List

from ..base import BaseAdapter
from ..http import http_get
from ..types import SourceConfig, ExtractedItem


class VereinsDirectoryAdapter(BaseAdapter):
    """
    Strategy:
    - Crawl the directory to extract Vereins + their website URLs (if present)
    - This adapter can either:
      A) return zero events (and instead write discovered sources into a 'sources_discovered' table), OR
      B) aggressively attempt to find event pages on each org site (more expensive)

    For MVP, I recommend A).
    """

    def fetch(self, cfg: SourceConfig) -> List[ExtractedItem]:
        html = http_get(cfg.seed_url).text

        # TODO: parse Vereins list and extract website links
        discovered_org_sites: List[str] = []

        # MVP: no events returned
        # Optionally: store discovered_org_sites somewhere for later crawling
        return []
