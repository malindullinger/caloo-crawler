from __future__ import annotations

import hashlib
import json
import time
from datetime import datetime, timezone
from typing import Any, Callable, Dict, List, Optional, TypeVar

from supabase import create_client

from .config import SUPABASE_SERVICE_ROLE_KEY, SUPABASE_URL
from .models import RawEvent

supabase = create_client(SUPABASE_URL, SUPABASE_SERVICE_ROLE_KEY)

T = TypeVar("T")

_MAX_RETRIES = 3
_RETRY_DELAY_S = 2.0


def _with_retry(fn: Callable[[], T], label: str) -> T:
    """Execute a Supabase call with retry on transient failures."""
    last_err: Exception | None = None
    for attempt in range(_MAX_RETRIES + 1):
        try:
            return fn()
        except Exception as e:
            last_err = e
            err_str = str(e).lower()
            is_transient = isinstance(e, (ConnectionError, OSError)) or any(
                kw in err_str
                for kw in (
                    "timeout", "connection", "502", "503", "504", "rate",
                    "reset", "broken pipe", "eof", "temporary failure",
                    "network", "unreachable",
                )
            )
            if not is_transient or attempt >= _MAX_RETRIES:
                raise
            delay = _RETRY_DELAY_S * (2 ** attempt)
            print(f"[storage] {label} transient error (attempt {attempt + 1}), retrying in {delay}s: {e}")
            time.sleep(delay)
    raise last_err  # unreachable, but satisfies type checker


def _sha256_hex(s: str) -> str:
    return hashlib.sha256(s.encode("utf-8")).hexdigest()


def _stable_json(payload: Dict[str, Any]) -> str:
    return json.dumps(payload, sort_keys=True, ensure_ascii=False)


# ----------------------------
# RAW EVENTS
# ----------------------------
def store_raw(raw: RawEvent) -> None:
    raw_payload = {
        "title_raw": raw.title_raw,
        "datetime_raw": raw.datetime_raw,
        "location_raw": raw.location_raw,
        "description_raw": raw.description_raw,
        "item_url": str(raw.item_url) if raw.item_url else None,
        "extra": raw.extra,
    }

    content_hash = _sha256_hex(_stable_json(raw_payload))

    row = {
        "source_id": raw.source_id,
        "source_url": str(raw.source_url),
        "item_url": str(raw.item_url) if raw.item_url else None,
        "content_hash": content_hash,
        "raw_payload": raw_payload,
        "fetched_at": raw.fetched_at.astimezone(timezone.utc).isoformat(),
        "status": "ok",
        "error": None,
    }

    _with_retry(
        lambda: supabase.table("event_raw").insert(row).execute(),
        f"store_raw({raw.source_id})",
    )



# Legacy upsert_event and insert_schedules removed in Phase 6H.1.
# Pipeline now writes only to event_raw (store_raw).
# Canonical path: event_raw → ingestRaw.ts → source_record → transformCanonical.ts → happening/offering/occurrence.


# ----------------------------
# CRAWL RUNS (PHASE 6G)
# ----------------------------
def insert_crawl_run(source_id: str) -> str:
    """Insert a crawl_runs row with status='running'. Returns the run id (uuid)."""
    row = {
        "source_id": source_id,
        "status": "running",
        "started_at": datetime.now(timezone.utc).isoformat(),
    }
    result = _with_retry(
        lambda: supabase.table("crawl_runs").insert(row).execute(),
        f"insert_crawl_run({source_id})",
    )
    return result.data[0]["id"]


def finish_crawl_run(
    run_id: str,
    *,
    status: str = "completed",
    surfaces_attempted: int = 0,
    surfaces_succeeded: int = 0,
    dom_items_visible: int = 0,
    detail_urls_found: int = 0,
    detail_urls_fetched: int = 0,
    items_extracted: int = 0,
    items_failed: int = 0,
    items_skipped: int = 0,
    circuit_breaker_triggered: bool = False,
    error_message: Optional[str] = None,
) -> None:
    """Update a crawl_runs row with final metrics and status."""
    row = {
        "finished_at": datetime.now(timezone.utc).isoformat(),
        "status": status,
        "surfaces_attempted": surfaces_attempted,
        "surfaces_succeeded": surfaces_succeeded,
        "dom_items_visible": dom_items_visible,
        "detail_urls_found": detail_urls_found,
        "detail_urls_fetched": detail_urls_fetched,
        "items_extracted": items_extracted,
        "items_failed": items_failed,
        "items_skipped": items_skipped,
        "circuit_breaker_triggered": circuit_breaker_triggered,
        "error_message": error_message,
    }
    _with_retry(
        lambda: supabase.table("crawl_runs").update(row).eq("id", run_id).execute(),
        f"finish_crawl_run({run_id[:8]})",
    )


def item_key(r: RawEvent) -> str:
    """Derive a stable item key from a RawEvent for crawl_run_items.

    Uses item_url (detail page URL) as primary key.
    Falls back to hash of source_id + title + datetime for items without URLs.

    This is the single canonical key derivation. All code that computes
    item keys for crawl_run_items must use this function.
    """
    if r.item_url:
        return str(r.item_url)
    sig = f"{r.source_id}|{r.title_raw}|{r.datetime_raw or ''}"
    return f"hash:{hashlib.sha256(sig.encode()).hexdigest()[:24]}"


def insert_crawl_run_items(run_id: str, item_keys: List[str]) -> None:
    """Bulk insert item keys for a crawl run. Deduplicates keys."""
    if not item_keys:
        return

    unique_keys = list(dict.fromkeys(item_keys))  # preserve order, dedupe
    rows = [{"crawl_run_id": run_id, "item_key": key} for key in unique_keys]

    BATCH_SIZE = 500
    for i in range(0, len(rows), BATCH_SIZE):
        batch = rows[i : i + BATCH_SIZE]
        _with_retry(
            lambda b=batch: supabase.table("crawl_run_items")
                .upsert(b, on_conflict="crawl_run_id,item_key")
                .execute(),
            f"insert_crawl_run_items({run_id[:8]}, batch {i // BATCH_SIZE + 1})",
        )
