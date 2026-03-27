from __future__ import annotations

import traceback

from .sources.multi_source import fetch_and_extract
from .storage import store_raw


def main() -> None:
    print("PIPELINE: start")

    print("PIPELINE: about to fetch")
    batch = fetch_and_extract()
    print("PIPELINE: fetch done")

    raws = batch.all_raw_events
    print(f"Raw events: {len(raws)}")

    # crawl_run_items are now persisted inside _process_source() before
    # finish_crawl_run(). No item-key recording needed here.

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
