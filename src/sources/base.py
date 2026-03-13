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
        circuit_breaker_threshold: int = 5,
    ) -> List[ExtractedItem]:
        """Fetch detail pages with error handling, polite delays, and circuit breaker.

        Args:
            urls: Detail page URLs to fetch.
            extract_fn: Callable(url) -> ExtractedItem | None.
            adapter_name: For log messages.
            delay_every: Sleep every Nth fetch (0 = no delay).
            delay_s: Seconds to sleep.
            circuit_breaker_threshold: Abort after this many consecutive failures.
                Set to 0 to disable.
        """
        items: List[ExtractedItem] = []
        consecutive_failures = 0
        for i, url in enumerate(urls):
            try:
                item = extract_fn(url)
                if item:
                    items.append(item)
                    consecutive_failures = 0
                else:
                    consecutive_failures += 1
            except Exception as e:
                consecutive_failures += 1
                print(f"{adapter_name}: detail parse failed: {url} err: {repr(e)}")
            if (
                circuit_breaker_threshold > 0
                and consecutive_failures >= circuit_breaker_threshold
            ):
                remaining = len(urls) - i - 1
                print(
                    f"{adapter_name}: CIRCUIT BREAKER — {consecutive_failures} consecutive "
                    f"failures, aborting {remaining} remaining detail fetches"
                )
                break
            if delay_every and (i + 1) % delay_every == 0 and i + 1 < len(urls):
                time.sleep(delay_s)
        return items
