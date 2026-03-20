from __future__ import annotations

import hashlib
import traceback

from .sources.multi_source import fetch_and_extract
from .storage import store_raw, insert_crawl_run_items
from .models import RawEvent


def _item_key(r: RawEvent) -> str:
    """Derive a stable item key from a RawEvent for crawl_run_items.

    Uses item_url (detail page URL) as primary key.
    Falls back to hash of source_id + title + datetime for items without URLs.
    """
    if r.item_url:
        return str(r.item_url)
    sig = f"{r.source_id}|{r.title_raw}|{r.datetime_raw or ''}"
    return f"hash:{hashlib.sha256(sig.encode()).hexdigest()[:24]}"


def main() -> None:
    print("PIPELINE: start")

    print("PIPELINE: about to fetch")
    batch = fetch_and_extract()
    print("PIPELINE: fetch done")

    raws = batch.all_raw_events
    print(f"Raw events: {len(raws)}")

    # Collect item keys per source from extracted RawEvent batch
    # (before downstream processing — reflects extraction truth)
    source_item_keys: dict[str, list[str]] = {}
    for r in raws:
        source_item_keys.setdefault(r.source_id, []).append(_item_key(r))

    # Bulk insert crawl_run_items for each source that has a crawl_run_id
    for source_id, sr in batch.source_results.items():
        if sr.crawl_run_id and source_id in source_item_keys:
            try:
                insert_crawl_run_items(sr.crawl_run_id, source_item_keys[source_id])
            except Exception:
                print(f"[pipeline] insert_crawl_run_items FAILED for {source_id}")
                traceback.print_exc()

    failed_raw = 0

    for r in raws:
        try:
            store_raw(r)
        except Exception:
            failed_raw += 1
            print(f"[pipeline] store_raw FAILED for {r.source_id} — {r.title_raw[:60]}")
            traceback.print_exc()

    stored = len(raws) - failed_raw
    print(f"Raw events stored: {stored}")
    if failed_raw > 0:
        print(f"[pipeline] FAILURES: raw={failed_raw}")


if __name__ == "__main__":
    main()
