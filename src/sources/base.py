from __future__ import annotations

from abc import ABC, abstractmethod
from datetime import datetime, timezone
from typing import List

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
