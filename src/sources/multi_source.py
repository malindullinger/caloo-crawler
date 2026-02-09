from __future__ import annotations

from datetime import datetime, timezone
from typing import List

from .registry import get_adapter
from .types import SourceConfig, ExtractedItem
from ..models import RawEvent


SOURCES: List[SourceConfig] = [
    SourceConfig(
        source_id="maennedorf_portal",
        adapter="maennedorf_portal",
        seed_url="https://www.maennedorf.ch/anlaesseaktuelles?datumVon=22.01.2026&datumBis=30.12.2026",
        timezone="Europe/Zurich",
        max_items=50,
    ),
    SourceConfig(
        source_id="ref-staefa-hombrechtikon",
        adapter="church_hub",
        seed_url="https://www.ref-staefa-hombrechtikon.ch/",
        timezone="Europe/Zurich",
        max_items=50,
    ),
    SourceConfig(
        source_id="uetikon_vereinsliste",
        adapter="vereins_directory",
        seed_url="https://www.uetikonamsee.ch/vereinsliste",
        timezone="Europe/Zurich",
        max_items=200,
    ),
    SourceConfig(
        source_id="kino-wildenmann",
        adapter="kino_wildenmann",
        seed_url="https://www.kino-wildenmann.ch/",
        timezone="Europe/Zurich",
        max_items=100,
    ),
    SourceConfig(
        source_id="eventbrite-zurich",
        adapter="eventbrite",
        seed_url="https://www.eventbrite.ch/d/switzerland--zurich/events/",
        timezone="Europe/Zurich",
        max_items=20,  # Limited for testing
    ),
]


def fetch_and_extract() -> List[RawEvent]:
    now = datetime.now(timezone.utc)
    out: List[RawEvent] = []

    for cfg in SOURCES:
        if not cfg.enabled:
            continue

        adapter = get_adapter(cfg.adapter)
        items: List[ExtractedItem] = adapter.fetch(cfg)

        # Enrich each item (detail fetch fallback)
        enriched: List[ExtractedItem] = []
        for it in items:
            it.fetched_at = it.fetched_at or now
            enriched.append(adapter.enrich(cfg, it))

        # Convert to RawEvent
        for it in enriched:
            out.append(
                RawEvent(
                    source_id=cfg.source_id,
                    source_url=cfg.seed_url,
                    item_url=it.item_url,
                    title_raw=it.title_raw,
                    datetime_raw=it.datetime_raw,
                    location_raw=it.location_raw,
                    description_raw=it.description_raw,
                    extra=it.extra or {},
                    fetched_at=it.fetched_at or now,
                )
            )

    return out
