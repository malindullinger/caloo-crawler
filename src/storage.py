from __future__ import annotations

import hashlib
import json
import re
from datetime import date as Date
from datetime import datetime, timezone
from typing import Any, Dict, Optional

from zoneinfo import ZoneInfo

import dateparser
from supabase import create_client

from .config import SUPABASE_SERVICE_ROLE_KEY, SUPABASE_URL
from .models import NormalizedEvent, RawEvent

supabase = create_client(SUPABASE_URL, SUPABASE_SERVICE_ROLE_KEY)


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

    supabase.table("event_raw").insert(row).execute()


# ----------------------------
# NORMALIZED EVENTS
# ----------------------------
def upsert_event(ev: NormalizedEvent) -> None:
    now = datetime.now(timezone.utc)

    row = {
        "external_id": ev.external_id,
        "source_id": ev.source_id,
        "title": ev.title,
        "start_at": ev.start_at.astimezone(timezone.utc).isoformat(),
        "end_at": ev.end_at.astimezone(timezone.utc).isoformat() if ev.end_at else None,
        "timezone": ev.timezone,
        "location_name": ev.location_name,
        "description": ev.description,
        "canonical_url": ev.canonical_url,
        "last_seen_at": ev.last_seen_at.astimezone(timezone.utc).isoformat(),
        "updated_at": now.isoformat(),
        "event_type": ev.event_type,
        "is_all_day": ev.is_all_day,
        "date_precision": ev.date_precision,
    }

    supabase.table("events").upsert(row, on_conflict="source_id,canonical_url").execute()


# ----------------------------
# EVENT SCHEDULES (PHASE 3)
# ----------------------------
def insert_schedules(
    *,
    event_external_id: str,
    raw_datetime: Optional[str],
    event_type: str,
    event_start_at_utc: datetime,
    event_tz: str,
    event_end_at_utc: Optional[datetime] = None,  # accepts pipeline arg safely
) -> None:
    """
    Writes ONE schedule row per event:
    - date_range => schedule_type='window'
        start_date_local / end_date_local from raw range,
        start_time_local / end_time_local from raw time window if present
    - single => schedule_type='session'
        start_date_local + start_time_local from normalized start_at,
        end_time_local from raw if present

    IMPORTANT GUARD:
      - If event_type == 'single' BUT normalized time is 00:00 (date-only),
        we SKIP writing a session row to avoid duplicates / bogus sessions.
    """

    raw_s = (raw_datetime or "").strip()
    tz = ZoneInfo(event_tz)

    # Times like: "14.00", "14:00" optionally followed by "Uhr"
    _TIME_RE = re.compile(r"(\d{1,2})[.:](\d{2})(?:\s*Uhr)?", re.IGNORECASE)

    def _extract_time_window(s: str) -> tuple[Optional[str], Optional[str]]:
        hits = _TIME_RE.findall(s or "")
        if not hits:
            return None, None

        sh, sm = hits[0]
        start_t = f"{int(sh):02d}:{int(sm):02d}"

        end_t = None
        if len(hits) >= 2:
            eh, em = hits[1]
            end_t = f"{int(eh):02d}:{int(em):02d}"

        return start_t, end_t

    def _parse_date_any(s: str) -> Optional[Date]:
        """
        Accepts:
          - '06.01.2026'
          - '6. Jan. 2026'
          - '10. Feb. 2026'
        Returns datetime.date
        """
        s = (s or "").strip()
        if not s:
            return None

        dt = dateparser.parse(
            s,
            languages=["de", "en"],
            settings={
                "TIMEZONE": event_tz,
                "RETURN_AS_TIMEZONE_AWARE": True,
                "PREFER_DATES_FROM": "future",
                "DATE_ORDER": "DMY",
            },
        )
        return dt.date() if dt else None

    # -----------------------------------------
    # 1) Date ranges => window
    # -----------------------------------------
    if event_type == "date_range":
        start_date_local: Optional[Date] = None
        end_date_local: Optional[Date] = None

        # Use the part before the first comma as the "date range"
        # e.g. "6. Jan. 2026 - 10. Feb. 2026, 14.00 Uhr - 14.45 Uhr, ..."
        range_part = raw_s.split(",", 1)[0].strip()

        # Split by dash
        if " - " in range_part:
            left, right = range_part.split(" - ", 1)
            start_date_local = _parse_date_any(left)
            end_date_local = _parse_date_any(right)
        else:
            # Single-date strings like "11. Juli 2026, 9.30 Uhr - 16.00 Uhr"
            # We treat them as a 1-day window.
            only_date = _parse_date_any(range_part)
            if only_date:
                start_date_local = only_date
                end_date_local = only_date

        # If dates still missing, fall back to normalized event bounds
        if not start_date_local:
            start_date_local = event_start_at_utc.astimezone(tz).date()
        if not end_date_local:
            if event_end_at_utc:
                end_date_local = event_end_at_utc.astimezone(tz).date()
            else:
                # at least keep it non-null when we can’t parse
                end_date_local = start_date_local

        # Extract time window (if present)
        start_time_local, end_time_local = _extract_time_window(raw_s)

        # If raw has no time but normalized start has a meaningful time, use it
        if not start_time_local:
            start_local = event_start_at_utc.astimezone(tz)
            if not (start_local.hour == 0 and start_local.minute == 0):
                start_time_local = start_local.strftime("%H:%M")

        row = {
            "event_external_id": event_external_id,
            "schedule_type": "window",
            "start_date_local": start_date_local.isoformat() if start_date_local else None,
            "end_date_local": end_date_local.isoformat() if end_date_local else None,
            "start_time_local": start_time_local,
            "end_time_local": end_time_local,
            "notes": f"date_range_raw={raw_s}",
            "updated_at": datetime.now(timezone.utc).isoformat(),
        }

        supabase.table("event_schedules").upsert(
            row,
            on_conflict="event_external_id,schedule_type",
        ).execute()
        return

    # -----------------------------------------
    # 2) Singles => session
    # -----------------------------------------
    if event_type == "single":
        start_local = event_start_at_utc.astimezone(tz)

        # ✅ Guard: avoid generating bogus midnight sessions
        # These were previously created by "backfill_from_events_start_at=true"
        # when the normalized event had date-only precision.
        if start_local.hour == 0 and start_local.minute == 0:
            return

        start_date_local = start_local.date().isoformat()
        start_time_local = start_local.strftime("%H:%M")

        # End time from raw string if present (second time)
        end_time_local = None
        hits = _TIME_RE.findall(raw_s)
        if len(hits) >= 2:
            eh, em = hits[1]
            end_time_local = f"{int(eh):02d}:{int(em):02d}"

        row = {
            "event_external_id": event_external_id,
            "schedule_type": "session",
            "start_date_local": start_date_local,
            "end_date_local": None,
            "start_time_local": start_time_local,
            "end_time_local": end_time_local,
            "notes": f"single_raw={raw_s}",
            "updated_at": datetime.now(timezone.utc).isoformat(),
        }

        supabase.table("event_schedules").upsert(
            row,
            on_conflict="event_external_id,schedule_type",
        ).execute()
        return

    # Other types => do nothing for now
    return
