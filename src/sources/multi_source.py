from __future__ import annotations

import time
import traceback
from concurrent.futures import ThreadPoolExecutor, as_completed, TimeoutError
from datetime import datetime, timezone
from typing import List

from .registry import get_adapter
from .types import SourceConfig, ExtractedItem
from ..models import RawEvent

# Parallelization config
MAX_WORKERS = 4           # concurrent source threads (each source = different domain)
TOTAL_TIMEOUT_S = 900     # 15 min — total time fetch_and_extract() will wait for results
# After TOTAL_TIMEOUT_S, the main thread stops waiting. Pending (not-yet-started)
# futures are cancelled. Running worker threads continue in the background until
# their current HTTP request completes (bounded by 30s per-request timeouts in
# http.py). The Python process may linger ~30s after fetch_and_extract() returns.
# This is a practical total bound, not a per-source kill.


SOURCES: List[SourceConfig] = [
    SourceConfig(
        source_id="maennedorf_portal",
        adapter="maennedorf_portal",
        seed_url="https://www.maennedorf.ch/anlaesseaktuelles?datumVon=22.01.2026&datumBis=30.12.2026",
        timezone="Europe/Zurich",
        max_items=50,
        municipality="maennedorf",
        platform="icms",
    ),
    SourceConfig(
        source_id="eventbrite-zurich",
        adapter="eventbrite",
        seed_url="https://www.eventbrite.ch/d/switzerland--zurich/events/",
        timezone="Europe/Zurich",
        max_items=20,  # Limited for testing
        municipality="zurich",
        platform="eventbrite",
    ),
    SourceConfig(
        source_id="familienclub-herrliberg",
        adapter="familienclub_herrliberg",
        seed_url="https://familienclub-herrliberg.ch/agenda/",
        timezone="Europe/Zurich",
        max_items=200,
        municipality="herrliberg",
        platform="ai1ec",
    ),
    SourceConfig(
        source_id="gemeinde-herrliberg",
        adapter="gemeinde_herrliberg",
        seed_url="https://www.herrliberg.ch/leben/freizeit/veranstaltungen.html/235",
        timezone="Europe/Zurich",
        max_items=200,
        enabled=False,  # WAF-blocked (403) — re-enable when access restored
        municipality="herrliberg",
        platform="govis",
    ),
    SourceConfig(
        source_id="meilen-portal",
        adapter="maennedorf_portal",  # identical ICMS platform
        seed_url="https://www.meilen.ch/anlaesseaktuelles",
        timezone="Europe/Zurich",
        max_items=50,
        municipality="meilen",
        platform="icms",
    ),
    SourceConfig(
        source_id="ref-kirche-herrliberg",
        adapter="kirchenweb",
        seed_url="https://www.ref-herrliberg.ch/agenda?page=agenda&sucheDarstellung=agenda&sucheTyp=veranstaltungen&sucheZeitPunkt=today&sucheZeitFenster=365",
        timezone="Europe/Zurich",
        max_items=200,
        municipality="herrliberg",
        platform="kirchenweb",
    ),
    SourceConfig(
        source_id="kath-kirche-herrliberg",
        adapter="kirchenweb",  # identical kirchenweb.ch platform
        seed_url="https://www.kath-herrliberg.ch/?page=agenda&sucheDarstellung=agenda&sucheTyp=veranstaltungen&sucheZeitPunkt=today&sucheZeitFenster=365",
        timezone="Europe/Zurich",
        max_items=100,
        municipality="herrliberg",
        platform="kirchenweb",
    ),
    SourceConfig(
        source_id="ref-kirche-meilen",
        adapter="kirchenweb",  # identical kirchenweb.ch platform
        seed_url="https://www.ref-meilen.ch/agenda?page=agenda&sucheDarstellung=agenda&sucheTyp=veranstaltungen&sucheZeitPunkt=today&sucheZeitFenster=365",
        timezone="Europe/Zurich",
        max_items=200,
        municipality="meilen",
        platform="kirchenweb",
    ),
]


def _validate_sources(sources: List[SourceConfig]) -> None:
    """Pre-crawl validation. Catches common config errors before the crawl starts."""
    from .registry import ADAPTERS

    print(f"[crawl] validating {len(sources)} source configs")

    # Duplicate source_id check (single-pass)
    seen: set[str] = set()
    dupes: set[str] = set()
    for cfg in sources:
        if cfg.source_id in seen:
            dupes.add(cfg.source_id)
        seen.add(cfg.source_id)
    if dupes:
        raise ValueError(f"[crawl] FATAL: duplicate source_ids: {dupes}")

    for cfg in sources:
        if not cfg.enabled:
            continue
        if cfg.adapter not in ADAPTERS:
            raise ValueError(
                f"[crawl] FATAL: source {cfg.source_id!r} references "
                f"unregistered adapter {cfg.adapter!r}"
            )
        if not cfg.municipality:
            print(
                f"[crawl] WARN: source {cfg.source_id!r} has no municipality — "
                f"downstream canonicalization may group events incorrectly"
            )

    print("[crawl] source manifest validation passed")


def _process_source(cfg: SourceConfig, now: datetime) -> List[RawEvent]:
    """Process a single source: fetch, enrich, convert to RawEvent.

    Runs in a worker thread. Each source targets a different domain,
    so concurrent execution does not violate per-domain politeness.
    """
    t0 = time.monotonic()
    print(f"[crawl] {cfg.source_id} — started")

    adapter = get_adapter(cfg.adapter)
    items: List[ExtractedItem] = adapter.fetch(cfg)

    # Enrich each item (detail fetch fallback)
    enriched: List[ExtractedItem] = []
    for it in items:
        it.fetched_at = it.fetched_at or now
        enriched.append(adapter.enrich(cfg, it))

    # Convert to RawEvent
    result: List[RawEvent] = []
    for it in enriched:
        result.append(
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

    elapsed = time.monotonic() - t0
    print(f"[crawl] {cfg.source_id} — completed: {len(result)} items in {elapsed:.1f}s")
    return result


def fetch_and_extract() -> List[RawEvent]:
    _validate_sources(SOURCES)
    now = datetime.now(timezone.utc)
    out: List[RawEvent] = []
    t_start = time.monotonic()

    # Log disabled sources
    for cfg in SOURCES:
        if not cfg.enabled:
            print(f"[crawl] {cfg.source_id} — skipped (disabled)")

    enabled = [cfg for cfg in SOURCES if cfg.enabled]
    print(f"[crawl] starting {len(enabled)} sources with {MAX_WORKERS} workers")

    # Do NOT use context manager — its __exit__ calls shutdown(wait=True),
    # which blocks until all threads finish. We need shutdown(wait=False)
    # to let the main thread proceed past straggler threads.
    pool = ThreadPoolExecutor(max_workers=MAX_WORKERS)
    futures = {}
    for cfg in enabled:
        future = pool.submit(_process_source, cfg, now)
        futures[future] = cfg

    try:
        for future in as_completed(futures, timeout=TOTAL_TIMEOUT_S):
            cfg = futures[future]
            try:
                result = future.result()  # already complete — returns immediately
                out.extend(result)
            except Exception:
                print(f"[crawl] {cfg.source_id} — FAILED")
                traceback.print_exc()
    except TimeoutError:
        elapsed = time.monotonic() - t_start
        not_done = [futures[f].source_id for f in futures if not f.done()]
        print(f"[crawl] TOTAL TIMEOUT after {elapsed:.1f}s — sources not completed: {not_done}")
    finally:
        # Cancel pending (not-yet-started) futures. Running threads continue
        # in background until their current HTTP request finishes (~30s max).
        pool.shutdown(wait=False, cancel_futures=True)

    elapsed = time.monotonic() - t_start
    print(f"[crawl] all sources done: {len(out)} total items in {elapsed:.1f}s")
    return out
