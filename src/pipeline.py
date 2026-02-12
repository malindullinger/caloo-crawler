from __future__ import annotations

from datetime import datetime, timezone

from .sources.multi_source import fetch_and_extract
from .normalize import raw_to_normalized
from .storage import insert_schedules, store_raw, upsert_event


def main() -> None:
    print("PIPELINE: start")
    now_utc = datetime.now(timezone.utc)

    print("PIPELINE: about to fetch")
    raws = fetch_and_extract()
    print("PIPELINE: fetch done")

    print(f"Raw events: {len(raws)}")

    # ✅ DEBUG: prove whether image_url exists on RawEvent BEFORE storing
    sample_with_images = 0
    for r in raws[:25]:
        img = (r.extra or {}).get("image_url")
        if isinstance(img, str):
            img = img.strip() or None
        if img:
            sample_with_images += 1
        print("DEBUG raw.image_url:", img, "| item_url:", str(r.item_url) if r.item_url else None)

    print(f"DEBUG: raws with image_url in first 25 = {sample_with_images}")

    normalized = []

    for r in raws:
        # 1) Always store raw evidence
        store_raw(r)

        # 2) Normalize
        n = raw_to_normalized(r, now_utc=now_utc)
        if not n:
            continue

        # 3) Upsert normalized event
        upsert_event(n)

        # 4) Insert schedules
        insert_schedules(
            event_external_id=n.external_id,
            raw_datetime=r.datetime_raw,
            event_type=n.event_type,
            event_start_at_utc=n.start_at,
            event_end_at_utc=n.end_at,  # optional; insert_schedules should accept this
            event_tz=n.timezone,
        )

        normalized.append(n)

    print(f"Normalized events written: {len(normalized)}")

    if normalized:
        print("Sample normalized event written:")
        print(normalized[0].model_dump())


if __name__ == "__main__":
    main()
