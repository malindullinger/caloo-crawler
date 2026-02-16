from __future__ import annotations

import os
from datetime import datetime, timezone
from typing import List

from ..db.db_sources import DbSourcesLoader
from ..models import RawEvent
from .registry import get_adapter
from .types import SourceConfig, ExtractedItem


def _hardcoded_sources_fallback() -> List[SourceConfig]:
    return [
        SourceConfig(
            source_id="maennedorf_portal",
            adapter="maennedorf_portal",
            seed_url="https://www.maennedorf.ch/anlaesseaktuelles?datumVon=22.01.2026&datumBis=30.12.2026",
            timezone="Europe/Zurich",
            max_items=50,
            source_tier="B",
        ),
        SourceConfig(
            source_id="eventbrite_zurich",
            adapter="eventbrite",
            seed_url="https://www.eventbrite.com/d/switzerland--zurich/events/",
            timezone="Europe/Zurich",
            max_items=50,
            source_tier="A",
        ),
        SourceConfig(
            source_id="elternverein_uetikon",
            adapter="elternverein_uetikon",
            seed_url="https://elternverein-uetikon.ch/veranstaltungen",
            timezone="Europe/Zurich",
            max_items=50,
            source_tier="B",
        ),
    ]


def _log_sources(mode: str, sources: List[SourceConfig]) -> None:
    print(f"[sources] mode={mode} count={len(sources)}")
    for s in sources:
        print(
            f"[sources] - source_id={s.source_id} adapter={s.adapter} "
            f"max_items={s.max_items} seed_url={s.seed_url}"
        )


def load_sources() -> List[SourceConfig]:
    """
    DB-first source loading.
    Falls back to hardcoded sources if DB is missing/unreachable/empty.

    Toggle:
      CALOO_SOURCES_FROM_DB=true/false (default true)
    """
    use_db = os.getenv("CALOO_SOURCES_FROM_DB", "true").strip().lower() in ("1", "true", "yes", "y")
    if not use_db:
        sources = _hardcoded_sources_fallback()
        _log_sources("HARDCODED (CALOO_SOURCES_FROM_DB=false)", sources)
        return sources

    try:
        loader = DbSourcesLoader.from_env()
        rows = loader.load_enabled_sources()

        if not rows:
            sources = _hardcoded_sources_fallback()
            _log_sources("FALLBACK (DB empty / no enabled sources)", sources)
            return sources

        sources = [
            SourceConfig(
                source_id=r.source_id,
                adapter=r.adapter,
                seed_url=r.seed_url,
                max_items=r.max_items,
                timezone=r.timezone or "Europe/Zurich",
                source_tier=r.source_tier,
            )
            for r in rows
        ]
        _log_sources("DB", sources)
        return sources

    except Exception as e:
        sources = _hardcoded_sources_fallback()
        _log_sources(f"FALLBACK (DB load failed: {type(e).__name__}: {e})", sources)
        return sources


def fetch_and_extract() -> tuple[List[RawEvent], int]:
    """
    Entry point used by pipeline.py.

    Runs enabled sources sequentially. A single failing source does not crash the full run.

    Returns (raws, sources_run) where sources_run is the number of source
    configs that were attempted (including ones that errored).
    """
    sources = load_sources()
    now = datetime.now(timezone.utc)
    out: List[RawEvent] = []
    sources_run = 0

    for cfg in sources:
        sources_run += 1
        print(f"[source] start source_id={cfg.source_id} adapter={cfg.adapter} max_items={cfg.max_items}")
        try:
            adapter = get_adapter(cfg.adapter)
        except Exception as e:
            print(
                f"[source] ERROR get_adapter failed source_id={cfg.source_id} "
                f"adapter={cfg.adapter}: {type(e).__name__}: {e}"
            )
            continue

        try:
            items: List[ExtractedItem] = adapter.fetch(cfg)
        except Exception as e:
            print(f"[source] ERROR fetch failed source_id={cfg.source_id}: {type(e).__name__}: {e}")
            continue

        # Enrich each item (detail fetch fallback)
        enriched: List[ExtractedItem] = []
        for it in items:
            it.fetched_at = it.fetched_at or now
            enriched.append(adapter.enrich(cfg, it))

        # Convert to RawEvent
        for idx, it in enumerate(enriched):
            if idx < 25:
                img = (it.extra or {}).get("image_url")
                if isinstance(img, str):
                    img = img.strip() or None
                print(
                    f"[DEBUG {cfg.source_id}] image_url:",
                    img,
                    "| item_url:",
                    getattr(it, "item_url", None),
                    "| keys:",
                    list((it.extra or {}).keys())[:10],
                )

            raw_extra = dict(it.extra or {})
            raw_extra["source_tier"] = cfg.source_tier

            out.append(
                RawEvent(
                    source_id=cfg.source_id,
                    source_url=cfg.seed_url,
                    item_url=it.item_url,
                    title_raw=it.title_raw,
                    datetime_raw=it.datetime_raw,
                    location_raw=it.location_raw,
                    description_raw=it.description_raw,
                    extra=raw_extra,
                    fetched_at=it.fetched_at or now,
                )
            )

        print(f"[source] done source_id={cfg.source_id} items={len(enriched)}")

    print(f"[sources] total_raws={len(out)}")
    return out, sources_run
