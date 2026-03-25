from __future__ import annotations

import time
import traceback
from concurrent.futures import ThreadPoolExecutor, as_completed, TimeoutError
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Dict, List, Optional

from .registry import get_adapter
from .types import SourceConfig, ExtractedItem
from ..models import RawEvent
from ..storage import insert_crawl_run, finish_crawl_run


@dataclass
class SourceCrawlMetrics:
    surfaces_attempted: int = 0
    surfaces_succeeded: int = 0
    dom_items_visible: int = 0
    detail_urls_found: int = 0
    detail_urls_fetched: int = 0
    items_extracted: int = 0
    items_failed: int = 0
    items_skipped: int = 0
    circuit_breaker_triggered: bool = False


@dataclass
class SourceCrawlResult:
    source_id: str
    raw_events: List[RawEvent]
    crawl_run_id: Optional[str]
    metrics: SourceCrawlMetrics


@dataclass
class CrawlBatchResult:
    all_raw_events: List[RawEvent]
    source_results: Dict[str, SourceCrawlResult] = field(default_factory=dict)

# Parallelization config
MAX_WORKERS = 4           # concurrent source threads (each source = different domain)
TOTAL_TIMEOUT_S = 1800    # 30 min — total time fetch_and_extract() will wait for results
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
        max_items=200,
        municipality="maennedorf",
        platform="icms",
    ),
    SourceConfig(
        source_id="eventbrite-zurich",
        adapter="eventbrite",
        seed_url="https://www.eventbrite.ch/b/switzerland--z%C3%BCrich/family-and-education/",
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
        max_items=400,
        municipality="meilen",
        platform="icms",
    ),
    SourceConfig(
        source_id="zollikon-portal",
        adapter="maennedorf_portal",  # identical ICMS platform
        seed_url="https://www.zollikon.ch/anlaesseaktuelles",
        timezone="Europe/Zurich",
        max_items=300,
        municipality="zollikon",
        platform="icms",
    ),
    SourceConfig(
        source_id="uetikon-portal",
        adapter="maennedorf_portal",  # identical ICMS platform
        seed_url="https://www.uetikonamsee.ch/anlaesseaktuelles",
        timezone="Europe/Zurich",
        max_items=150,
        municipality="uetikon",
        platform="icms",
    ),
    SourceConfig(
        source_id="rapperswil-jona-portal",
        adapter="maennedorf_portal",  # identical ICMS platform
        seed_url="https://www.rapperswil-jona.ch/anlaesseaktuelles",
        timezone="Europe/Zurich",
        max_items=300,
        municipality="rapperswil-jona",
        platform="icms",
    ),
    SourceConfig(
        source_id="ref-kirche-herrliberg",
        adapter="kirchenweb",
        seed_url="https://www.ref-herrliberg.ch/agenda?page=agenda&sucheDarstellung=agenda&sucheTyp=veranstaltungen&sucheZeitPunkt=today&sucheZeitFenster=365&sucheZielgruppe=Kinder",
        timezone="Europe/Zurich",
        max_items=200,
        municipality="herrliberg",
        platform="kirchenweb",
    ),
    SourceConfig(
        source_id="kath-kirche-herrliberg",
        adapter="kirchenweb",  # identical kirchenweb.ch platform
        seed_url="https://www.kath-herrliberg.ch/?page=agenda&sucheDarstellung=agenda&sucheTyp=veranstaltungen&sucheZeitPunkt=today&sucheZeitFenster=365&sucheZielgruppe=Kinder",
        timezone="Europe/Zurich",
        max_items=100,
        municipality="herrliberg",
        platform="kirchenweb",
    ),
    SourceConfig(
        source_id="ref-kirche-meilen",
        adapter="kirchenweb",  # identical kirchenweb.ch platform
        seed_url="https://www.ref-meilen.ch/agenda?page=agenda&sucheDarstellung=agenda&sucheTyp=veranstaltungen&sucheZeitPunkt=today&sucheZeitFenster=365&sucheZielgruppe=Kinder",
        timezone="Europe/Zurich",
        max_items=200,
        municipality="meilen",
        platform="kirchenweb",
    ),
    SourceConfig(
        source_id="ref-staefa-hombrechtikon",
        adapter="kirchenweb",  # identical kirchenweb.ch platform
        seed_url="https://www.ref-staefa-hombrechtikon.ch/?page=agenda&sucheDarstellung=agenda&sucheTyp=veranstaltungen&sucheZielgruppe=Kinder",
        timezone="Europe/Zurich",
        max_items=200,
        municipality="staefa",
        platform="kirchenweb",
    ),
    SourceConfig(
        source_id="ref-kirche-zollikon-zumikon",
        adapter="kirchenweb",
        seed_url="https://www.ref-zozu.ch/?page=agenda&sucheDarstellung=agenda&sucheTyp=veranstaltungen&sucheZeitPunkt=today&sucheZeitFenster=365&sucheZielgruppe=Kinder",
        timezone="Europe/Zurich",
        max_items=200,
        municipality="zollikon",
        platform="kirchenweb",
    ),
    SourceConfig(
        source_id="kath-kirche-zollikon-zumikon",
        adapter="kirchenweb",
        seed_url="https://www.kath-zollikon-zumikon.ch/?page=agenda&sucheDarstellung=agenda&sucheTyp=veranstaltungen&sucheZeitPunkt=today&sucheZeitFenster=365&sucheZielgruppe=Kinder",
        timezone="Europe/Zurich",
        max_items=200,
        municipality="zollikon",
        platform="kirchenweb",
    ),
    SourceConfig(
        source_id="ref-kirche-kuesnacht",
        adapter="kirchenweb",
        seed_url="https://www.rkk.ch/?page=agenda&sucheDarstellung=agenda&sucheTyp=veranstaltungen&sucheZeitPunkt=today&sucheZeitFenster=365&sucheZielgruppe=Kinder",
        timezone="Europe/Zurich",
        max_items=200,
        municipality="kuesnacht",
        platform="kirchenweb",
    ),
    SourceConfig(
        source_id="ref-kirche-erlenbach",
        adapter="kirchenweb",
        seed_url="https://www.ref-erlenbach.ch/?page=agenda&sucheDarstellung=agenda&sucheTyp=veranstaltungen&sucheZeitPunkt=today&sucheZeitFenster=365&sucheZielgruppe=Kinder",
        timezone="Europe/Zurich",
        max_items=200,
        municipality="erlenbach",
        platform="kirchenweb",
    ),
    SourceConfig(
        source_id="ref-kirche-uetikon",
        adapter="kirchenweb",
        seed_url="https://www.ref-uetikon.ch/?page=agenda&sucheDarstellung=agenda&sucheTyp=veranstaltungen&sucheZeitPunkt=today&sucheZeitFenster=365&sucheZielgruppe=Kinder",
        timezone="Europe/Zurich",
        max_items=200,
        municipality="uetikon",
        platform="kirchenweb",
    ),
    SourceConfig(
        source_id="kath-pfarrei-staefa",
        adapter="kirchenweb",
        seed_url="https://www.pfarreistaefa.ch/?page=agenda&sucheDarstellung=agenda&sucheTyp=veranstaltungen&sucheZeitPunkt=today&sucheZeitFenster=365&sucheZielgruppe=Kinder",
        timezone="Europe/Zurich",
        max_items=200,
        municipality="staefa",
        platform="kirchenweb",
    ),
    SourceConfig(
        source_id="kath-pfarrei-hombrechtikon",
        adapter="kirchenweb",
        seed_url="https://www.pfarreistniklaus.ch/?page=agenda&sucheDarstellung=agenda&sucheTyp=veranstaltungen&sucheZeitPunkt=today&sucheZeitFenster=365&sucheZielgruppe=Kinder",
        timezone="Europe/Zurich",
        max_items=200,
        municipality="hombrechtikon",
        platform="kirchenweb",
    ),
    SourceConfig(
        source_id="frauenverein-maennedorf",
        adapter="frauenverein_maennedorf",
        seed_url="https://www.frauenverein-maennedorf.ch/agenda.html",
        timezone="Europe/Zurich",
        max_items=50,
        municipality="maennedorf",
        platform="contao",
    ),
    SourceConfig(
        source_id="ref-kirche-maennedorf",
        adapter="ref_kirche_maennedorf",
        seed_url="https://www.ref-maennedorf.ch/agenda/",
        timezone="Europe/Zurich",
        max_items=200,
        municipality="maennedorf",
        platform="typo3_kool",
    ),
    SourceConfig(
        source_id="fluugepilz-erlenbach",
        adapter="fluugepilz",
        seed_url="https://www.xn--flgepilz-75aa.ch/events/feed/",
        timezone="Europe/Zurich",
        max_items=50,
        municipality="erlenbach",
        platform="wp_events_manager",
    ),
    # ── ClubDesk sources ─────────────────────────────────────────────
    SourceConfig(
        source_id="familienclub-zollikon",
        adapter="clubdesk",
        seed_url="https://www.familienclubzollikon.ch/clubdesk/www/familienclub?p=1000053",
        timezone="Europe/Zurich",
        max_items=50,
        municipality="zollikon",
        platform="clubdesk",
    ),
    # ── Lanterne Magique (Zauberlaterne) ─────────────────────────────
    SourceConfig(
        source_id="zauberlaterne-maennedorf",
        adapter="lanterne_magique",
        seed_url="https://www.lanterne-magique.org/de/clubs/mannedorf/",
        timezone="Europe/Zurich",
        max_items=20,
        municipality="maennedorf",
        platform="lanterne_magique",
    ),
    # ── GOViS portals ────────────────────────────────────────────────
    SourceConfig(
        source_id="kuesnacht-portal",
        adapter="govis",
        seed_url="https://www.kuesnacht.ch/leben-freizeit/veranstaltungen.page/843",
        timezone="Europe/Zurich",
        max_items=200,
        municipality="kuesnacht",
        platform="govis",
    ),
    SourceConfig(
        source_id="hombrechtikon-portal",
        adapter="govis",
        seed_url="https://www.hombrechtikon.ch/leben/freizeit/veranstaltungskalender.html/228",
        timezone="Europe/Zurich",
        max_items=200,
        municipality="hombrechtikon",
        platform="govis",
    ),
    # ── GOViS family sub-pages ──────────────────────────────────────
    SourceConfig(
        source_id="familienzentrum-kuesnacht",
        adapter="govis",
        seed_url="https://www.kuesnacht.ch/leben-freizeit/kinder-und-familien/familienzentrum.page/841",
        timezone="Europe/Zurich",
        max_items=50,
        municipality="kuesnacht",
        platform="govis",
    ),
    # ── Forum Magazin (Catholic parish events) ─────────────────────
    SourceConfig(
        source_id="forum-magazin-pfannenstiel",
        adapter="forum_magazin",
        seed_url="https://www.forum-magazin.ch/agenda/",
        timezone="Europe/Zurich",
        max_items=100,
        extra={"categories": ["kinder-und-familien", "jugend"], "region": "3"},
        # municipality intentionally empty: region=3 (Pfannenstiel) covers
        # multiple municipalities; downstream maps events via venue/location
        platform="forum_magazin",
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


def _process_source(cfg: SourceConfig, now: datetime) -> SourceCrawlResult:
    """Process a single source: fetch, enrich, convert to RawEvent.

    Runs in a worker thread. Each source targets a different domain,
    so concurrent execution does not violate per-domain politeness.
    Returns SourceCrawlResult (never raises — failures are captured in metrics).
    """
    t0 = time.monotonic()
    print(f"[crawl] {cfg.source_id} — started")

    # Start crawl run
    crawl_run_id: Optional[str] = None
    try:
        crawl_run_id = insert_crawl_run(cfg.source_id)
    except Exception as e:
        print(f"[crawl] {cfg.source_id} — insert_crawl_run failed: {repr(e)}")

    metrics = SourceCrawlMetrics()
    status = "completed"
    error_msg: Optional[str] = None
    raw_events: List[RawEvent] = []

    try:
        adapter = get_adapter(cfg.adapter)
        items: List[ExtractedItem] = adapter.fetch(cfg)

        # Read metrics from adapter instance
        metrics.surfaces_attempted = adapter._surfaces_attempted
        metrics.surfaces_succeeded = adapter._surfaces_succeeded
        metrics.dom_items_visible = adapter._dom_items_visible
        metrics.detail_urls_found = adapter._detail_urls_found
        metrics.detail_urls_fetched = adapter._detail_urls_fetched
        metrics.circuit_breaker_triggered = adapter._circuit_breaker_triggered

        # Enrich each item (detail fetch fallback)
        enriched: List[ExtractedItem] = []
        for it in items:
            it.fetched_at = it.fetched_at or now
            enriched.append(adapter.enrich(cfg, it))

        # Convert to RawEvent
        for it in enriched:
            raw_events.append(
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

        metrics.items_extracted = len(raw_events)
    except Exception as e:
        status = "failed"
        error_msg = repr(e)[:500]
        print(f"[crawl] {cfg.source_id} — FAILED: {error_msg}")
        traceback.print_exc()
    finally:
        # Always finish crawl run (even on failure)
        if crawl_run_id:
            try:
                finish_crawl_run(
                    crawl_run_id,
                    status=status,
                    surfaces_attempted=metrics.surfaces_attempted,
                    surfaces_succeeded=metrics.surfaces_succeeded,
                    dom_items_visible=metrics.dom_items_visible,
                    detail_urls_found=metrics.detail_urls_found,
                    detail_urls_fetched=metrics.detail_urls_fetched,
                    items_extracted=metrics.items_extracted,
                    items_failed=metrics.items_failed,
                    items_skipped=metrics.items_skipped,
                    circuit_breaker_triggered=metrics.circuit_breaker_triggered,
                    error_message=error_msg,
                )
            except Exception as fin_err:
                print(f"[crawl] {cfg.source_id} — finish_crawl_run failed: {repr(fin_err)}")

    elapsed = time.monotonic() - t0
    print(f"[crawl] {cfg.source_id} — done: {len(raw_events)} items in {elapsed:.1f}s")
    return SourceCrawlResult(
        source_id=cfg.source_id,
        raw_events=raw_events,
        crawl_run_id=crawl_run_id,
        metrics=metrics,
    )


def fetch_and_extract() -> CrawlBatchResult:
    _validate_sources(SOURCES)
    now = datetime.now(timezone.utc)
    out: List[RawEvent] = []
    source_results: Dict[str, SourceCrawlResult] = {}
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

    source_counts: dict[str, int] = {}
    try:
        for future in as_completed(futures, timeout=TOTAL_TIMEOUT_S):
            cfg = futures[future]
            try:
                result: SourceCrawlResult = future.result()
                source_results[cfg.source_id] = result
                source_counts[cfg.source_id] = len(result.raw_events)
                out.extend(result.raw_events)
            except Exception:
                source_counts[cfg.source_id] = -1  # -1 = failed
                print(f"[crawl] {cfg.source_id} — future.result() raised unexpectedly")
                traceback.print_exc()
    except TimeoutError:
        elapsed = time.monotonic() - t_start
        not_done = [futures[f].source_id for f in futures if not f.done()]
        print(f"[crawl] TOTAL TIMEOUT after {elapsed:.1f}s — sources not completed: {not_done}")
    finally:
        # Cancel pending (not-yet-started) futures. Running threads continue
        # in background until their current HTTP request finishes (~30s max).
        pool.shutdown(wait=False, cancel_futures=True)

    # Post-crawl output validation: detect silent adapter failures
    for cfg in enabled:
        count = source_counts.get(cfg.source_id)
        if count is None:
            print(f"[crawl] WARN: source {cfg.source_id!r} did not complete (timeout or cancelled)")
        elif count == 0:
            print(
                f"[crawl] WARN: source {cfg.source_id!r} returned 0 items — "
                f"possible causes: adapter broken, site structure changed, anti-bot block"
            )

    elapsed = time.monotonic() - t_start
    print(f"[crawl] all sources done: {len(out)} total items in {elapsed:.1f}s")
    return CrawlBatchResult(all_raw_events=out, source_results=source_results)
