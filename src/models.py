from __future__ import annotations

from datetime import datetime
from typing import Any, Dict, Optional

from pydantic import BaseModel, Field, HttpUrl


class RawEvent(BaseModel):
    source_id: str
    source_url: HttpUrl
    item_url: Optional[HttpUrl] = None

    title_raw: str
    datetime_raw: str
    location_raw: Optional[str] = None
    description_raw: Optional[str] = None

    extra: Dict[str, Any] = Field(default_factory=dict)
    fetched_at: datetime


class NormalizedEvent(BaseModel):
    external_id: str
    source_id: str

    event_type: str = "single"
    is_all_day: bool = False
    date_precision: str = "datetime"

    title: str
    start_at: datetime
    end_at: Optional[datetime] = None
    timezone: str = "Europe/Zurich"

    location_name: Optional[str] = None
    location_address: Optional[str] = None
    lat: Optional[float] = None
    lng: Optional[float] = None

    age_min: Optional[int] = None
    age_max: Optional[int] = None
    price_text: Optional[str] = None

    description: Optional[str] = None
    canonical_url: str
    last_seen_at: datetime

    extra: Dict[str, Any] = Field(default_factory=dict)
