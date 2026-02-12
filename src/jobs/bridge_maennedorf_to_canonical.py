# src/jobs/bridge_maennedorf_to_source_happenings.py

from __future__ import annotations

import argparse
import hashlib
from datetime import datetime
from typing import Any, Dict, Optional

import pytz

from src.db import get_supabase  # adjust if your helper differs


TZ_NAME = "Europe/Zurich"
TZ = pytz.timezone(TZ_NAME)

SKIP_TITLES_EXACT = {"Kopfzeile"}


def _clean(s: Optional[str]) -> Optional[str]:
    if s is None:
        return None
    s = s.strip()
    return s or None


def should_skip_title(title: Optional[str]) -> bool:
    t = (title or "").strip()
    if not t:
        return True
    if t in SKIP_TITLES_EXACT:
        return True
    return False


def sha32(text: str) -> str:
    return hashlib.sha256(text.encode("utf-8")).hexdigest()[:32]


def ensure_external_id(
    source_id: str,
    events_external_id: Optional[str],
    item_url: Optional[str],
    title_raw: Optional[str],
    start_date_local: Optional[str],
    location_raw: Optional[str],
) -> str:
    """
    Always return a stable, non-empty external_id for upsert idempotency.
    """
    if events_external_id and events_external_id.strip():
        return events_external_id.strip()

    if item_url and item_url.strip():
        return sha32(f"{source_id}|url|{item_url.strip()}")

    # last resort fallback (should be rare — better to fix ingestion to always have item_url)
    return sha32(
        f"{source_id}|fallback|{(title_raw or '').strip()}|{start_date_local or ''}|{(location_raw or '').strip()}"
    )


def to_local_date_iso(dt: Optional[str]) -> Optional[str]:
    """
    dt: ISO string (timestamptz coming out of Supabase is typically ISO).
    Returns YYYY-MM-DD in Europe/Zurich.
    """
    if not dt:
        return None
    # Supabase often returns ISO like "2026-02-11T09:00:00+00:00"
    parsed = datetime.fromisoformat(dt.replace("Z", "+00:00"))
    local = parsed.astimezone(TZ)
    return local.date().isoformat()


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--live", action="store_true")
    ap.add_argument("--limit", type=int, default=500)
    ap.add_argument("--source-id", default="maennedorf_portal")
    args = ap.parse_args()

    sb = get_supabase()

    # Pull from normalized events
    res = (
        sb.table("events")
        .select("*")
        .eq("source_id", args.source_id)
        .limit(args.limit)
        .execute()
    )
    rows = res.data or []

    upserts: list[Dict[str, Any]] = []
    skipped = 0

    for e in rows:
        title_raw = _clean(e.get("title")) or _clean(e.get("title_raw"))
        if should_skip_title(title_raw):
            skipped += 1
            continue

        item_url = _clean(e.get("item_url")) or _clean(e.get("url"))
        location_raw = _clean(e.get("location_raw")) or _clean(e.get("location_text"))
        description_raw = _clean(e.get("description_raw")) or _clean(e.get("description"))

        # These should already be timestamptz in your `events` table if you normalized correctly.
        start_at = e.get("start_at")  # keep as-is (ISO string or None)
        end_at = e.get("end_at")

        # Must satisfy start_date_local_required for queued rows:
        start_date_local = e.get("start_date_local") or to_local_date_iso(start_at)
        end_date_local = e.get("end_date_local") or to_local_date_iso(end_at)  # optional

        if not start_date_local:
            # Hard skip because queued row would violate constraint
            skipped += 1
            continue

        # Set date_precision in a way that won’t violate time_contract
        date_precision = "time" if start_at else "date"
        if date_precision == "date":
            # safest: do not set start_at/end_at unless precision is 'time'
            start_at = None
            end_at = None

        events_external_id = _clean(e.get("external_id"))
        external_id = ensure_external_id(
            source_id=args.source_id,
            events_external_id=events_external_id,
            item_url=item_url,
            title_raw=title_raw,
            start_date_local=start_date_local,
            location_raw=location_raw,
        )

        # A stable content hash can be useful (you also have a unique index on (source_id, content_hash))
        content_hash = sha32(
            "|".join(
                [
                    args.source_id,
                    external_id,
                    title_raw or "",
                    start_date_local or "",
                    item_url or "",
                    location_raw or "",
                ]
            )
        )

        payload: Dict[str, Any] = {
            # Required / identity
            "source_id": args.source_id,
            "source_type": "crawler",     # matches default/check
            "source_tier": "B",           # bridge-normalized (tier B); must be allowed by your check

            "external_id": external_id,

            # Raw-ish fields for provenance / debugging
            "title_raw": title_raw,
            "datetime_raw": _clean(e.get("datetime_raw")),
            "location_raw": location_raw,
            "description_raw": description_raw,
            "item_url": item_url,

            # Time contract fields
            "date_precision": date_precision,
            "start_at": start_at,
            "end_at": end_at,
            "timezone": TZ_NAME,
            "start_date_local": start_date_local,
            "end_date_local": end_date_local,

            # Optional helpers
            "extraction_method": "bridge_events_to_source_happenings",
            "content_hash": content_hash,

            # State
            "status": "queued",
            "error_message": None,
        }

        # remove Nones (keeps DB cleaner and avoids check edge cases on some installs)
        payload = {k: v for k, v in payload.items() if v is not None}
        upserts.append(payload)

    print(
        f"[bridge_maennedorf_to_source_happenings] source={args.source_id} fetched={len(rows)} "
        f"upserts={len(upserts)} skipped={skipped} live={args.live}"
    )

    if not args.live:
        for p in upserts[:5]:
            print(
                "sample:",
                {k: p.get(k) for k in ("source_id", "external_id", "title_raw", "start_date_local", "date_precision", "start_at", "status", "item_url")},
            )
        return

    sb.table("source_happenings").upsert(
        upserts,
        on_conflict="source_id,external_id",
    ).execute()

    print("[bridge_maennedorf_to_source_happenings] upsert complete")


if __name__ == "__main__":
    main()
