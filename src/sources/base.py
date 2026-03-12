from __future__ import annotations

import time
from abc import ABC, abstractmethod
from datetime import datetime, timezone
from typing import Callable, List, Optional

from .types import SourceConfig, ExtractedItem


class BaseAdapter(ABC):
    @abstractmethod
    def fetch(self, cfg: SourceConfig) -> List[ExtractedItem]:
        """Return extracted items (list stage)."""

    def enrich(self, cfg: SourceConfig, item: ExtractedItem) -> ExtractedItem:
        """
        Optional enrichment: fetch detail page to fill missing fields.
        Default: no-op.
        """
        return item

    def now_utc(self) -> datetime:
        return datetime.now(timezone.utc)

    def _fetch_detail_pages(
        self,
        urls: List[str],
        extract_fn: Callable[[str], Optional[ExtractedItem]],
        *,
        adapter_name: str = "",
        delay_every: int = 0,
        delay_s: float = 0.5,
    ) -> List[ExtractedItem]:
        """Fetch detail pages with error handling and polite delays.

        Args:
            urls: Detail page URLs to fetch.
            extract_fn: Callable(url) -> ExtractedItem | None.
            adapter_name: For log messages.
            delay_every: Sleep every Nth fetch (0 = no delay).
            delay_s: Seconds to sleep.
        """
        items: List[ExtractedItem] = []
        for i, url in enumerate(urls):
            try:
                item = extract_fn(url)
                if item:
                    items.append(item)
            except Exception as e:
                print(f"{adapter_name}: detail parse failed: {url} err: {repr(e)}")
                continue
            if delay_every and (i + 1) % delay_every == 0 and i + 1 < len(urls):
                time.sleep(delay_s)
        return items
