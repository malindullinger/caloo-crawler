from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from typing import Any, Dict, Optional


@dataclass(frozen=True)
class SourceConfig:
    source_id: str
    adapter: str
    seed_url: str
    timezone: str = "Europe/Zurich"
    max_items: int = 100
    enabled: bool = True
    extra: Dict[str, Any] | None = None


@dataclass
class ExtractedItem:
    """
    Adapter-neutral extraction result (not yet NormalizedEvent).
    We store list + detail info here, then convert to RawEvent.
    """
    title_raw: str
    datetime_raw: str | None
    location_raw: str | None
    description_raw: str | None
    item_url: str | None

    # optional: store raw HTML snippets or parsed fields for debugging
    extra: Dict[str, Any] | None = None
    fetched_at: Optional[datetime] = None
