from __future__ import annotations

from datetime import datetime, timezone

from .sources.multi_source import fetch_and_extract
from .normalize import raw_to_normalized
from .storage import insert_schedules, store_raw, upsert_event


def main() -> None:
    print("PIPELINE: start")
    now_utc = datetime.now(timezone.utc)

    print("PIPELINE: about to fetch")
    raws, sources_run = fetch_and_extract()
    print("PIPELINE: fetch done")

    print(f"Raw events: {len(raws)}")

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
    normalize_failed = 0
    upserted = 0
    upsert_errors = 0

    for r in raws:
        # 1) Always store raw evidence
        store_raw(r)

        # 2) Normalize
        n = raw_to_normalized(r, now_utc=now_utc)
        if not n:
            normalize_failed += 1
            print(
                f"[pipeline] NORMALIZE_FAILED source_id={r.source_id}"
                f" | title={r.title_raw!r}"
                f" | datetime_raw={r.datetime_raw!r}"
                f" | item_url={r.item_url}"
            )
            continue

        # 3) Upsert normalized event
        try:
            upsert_event(n)
            upserted += 1
        except Exception as e:
            upsert_errors += 1
            print(f"[pipeline] UPSERT_ERROR source_id={n.source_id} | {type(e).__name__}: {e}")
            continue

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

    from .storage import _DEDUPE_CONTENT, _DEDUPE_FALLBACK, _DEDUPE_ERROR

    print(
        f"[dedupe_key] content_based={_DEDUPE_CONTENT} "
        f"fallback={_DEDUPE_FALLBACK} "
        f"error={_DEDUPE_ERROR}"
    )

    # ---------------------------------------------------------------
    # Deterministic, grep-friendly summary line.
    # grep '[pipeline][summary]' /tmp/pipeline.log
    # ---------------------------------------------------------------
    errors = normalize_failed + upsert_errors + _DEDUPE_ERROR
    print(
        f"[pipeline][summary]"
        f" sources_run={sources_run}"
        f" extracted={len(raws)}"
        f" normalized_written={len(normalized)}"
        f" source_upserted={upserted}"
        f" normalize_failed={normalize_failed}"
        f" upsert_errors={upsert_errors}"
        f" dedupe_content={_DEDUPE_CONTENT}"
        f" dedupe_fallback={_DEDUPE_FALLBACK}"
        f" dedupe_error={_DEDUPE_ERROR}"
        f" errors={errors}"
    )


if __name__ == "__main__":
    main()
