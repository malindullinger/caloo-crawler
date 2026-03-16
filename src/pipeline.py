from __future__ import annotations

import traceback
from datetime import datetime, timezone

from .sources.multi_source import fetch_and_extract
from .normalize import raw_to_normalized
from .storage import store_raw, upsert_event, insert_schedules


def main() -> None:
    print("PIPELINE: start")
    now_utc = datetime.now(timezone.utc)

    print("PIPELINE: about to fetch")
    raws = fetch_and_extract()
    print("PIPELINE: fetch done")

    print(f"Raw events: {len(raws)}")

    normalized = []
    failed_raw = 0
    failed_normalize = 0
    failed_store = 0

    for r in raws:
        # 1) Always store raw evidence
        try:
            store_raw(r)
        except Exception:
            failed_raw += 1
            print(f"[pipeline] store_raw FAILED for {r.source_id} — {r.title_raw[:60]}")
            traceback.print_exc()
            # Continue — normalization can still proceed even if raw storage failed

        # 2) Normalize
        try:
            n = raw_to_normalized(r, now_utc=now_utc)
        except Exception:
            failed_normalize += 1
            print(f"[pipeline] normalize FAILED for {r.source_id} — {r.title_raw[:60]}")
            traceback.print_exc()
            continue

        if not n:
            continue

        # 3) Upsert normalized event + insert schedules
        try:
            upsert_event(n)

            insert_schedules(
                event_external_id=n.external_id,
                raw_datetime=r.datetime_raw,
                event_type=n.event_type,
                event_start_at_utc=n.start_at,
                event_end_at_utc=n.end_at,
                event_tz=n.timezone,
            )

            normalized.append(n)
        except Exception:
            failed_store += 1
            print(f"[pipeline] upsert/schedule FAILED for {r.source_id} — {n.title[:60]}")
            traceback.print_exc()

    # Summary
    total_failed = failed_raw + failed_normalize + failed_store
    print(f"Normalized events written: {len(normalized)}")
    if total_failed > 0:
        print(f"[pipeline] FAILURES: raw={failed_raw} normalize={failed_normalize} store={failed_store}")

    if normalized:
        print("Sample normalized event written:")
        print(normalized[0].model_dump())


if __name__ == "__main__":
    main()
