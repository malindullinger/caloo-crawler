from __future__ import annotations

import os
from datetime import date, datetime, timezone
from typing import Any, Mapping, Optional

from supabase import Client, create_client

from .canonicalize.dedupe_key import compute_dedupe_key
from .models import RawEvent, NormalizedEvent

# ---- Phase 3: Dedupe metrics ----
_DEDUPE_CONTENT = 0
_DEDUPE_FALLBACK = 0
_DEDUPE_ERROR = 0


# -----------------------------------------------------------------------------
# Supabase client helpers
# -----------------------------------------------------------------------------

def _load_dotenv_if_present() -> None:
    for fname in (".env", ".env.local"):
        if not os.path.exists(fname):
            continue
        try:
            with open(fname, "r", encoding="utf-8") as f:
                for line in f:
                    s = line.strip()
                    if not s or s.startswith("#") or "=" not in s:
                        continue
                    k, v = s.split("=", 1)
                    k = k.strip()
                    v = v.strip().strip('"').strip("'")
                    os.environ.setdefault(k, v)
        except Exception:
            continue


def get_supabase() -> Client:
    _load_dotenv_if_present()

    url = os.getenv("SUPABASE_URL") or os.getenv("NEXT_PUBLIC_SUPABASE_URL")
    key = (
        os.getenv("SUPABASE_SERVICE_ROLE_KEY")
        or os.getenv("SUPABASE_SERVICE_KEY")
        or os.getenv("SUPABASE_ANON_KEY")
        or os.getenv("NEXT_PUBLIC_SUPABASE_ANON_KEY")
    )

    if not url or not key:
        raise RuntimeError(
            "Missing SUPABASE env vars. Need SUPABASE_URL and SUPABASE_SERVICE_ROLE_KEY (preferred)."
        )

    return create_client(url, key)


# -----------------------------------------------------------------------------
# Small utilities
# -----------------------------------------------------------------------------

def _extract_image_url(extra: Mapping[str, Any] | None) -> Optional[str]:
    if not extra:
        return None
    v = extra.get("image_url")
    if isinstance(v, str):
        v = v.strip()
        return v or None
    return None


def _dt_iso(dt: Optional[datetime]) -> Optional[str]:
    if not dt:
        return None
    # Ensure timezone-aware ISO string
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.replace(microsecond=0).isoformat()


def _date_iso(d: Optional[date]) -> Optional[str]:
    if not d:
        return None
    return d.isoformat()


def _enforce_time_contract(payload: dict[str, Any]) -> dict[str, Any]:
    """
    Enforces DB check constraint expectations:
      - if date_precision != 'datetime': start_at and end_at MUST be NULL
      - start_date_local must stay populated
    """
    dp = (payload.get("date_precision") or "").lower().strip()
    if dp != "datetime":
        payload["start_at"] = None
        payload["end_at"] = None
    return payload


# -----------------------------------------------------------------------------
# Public API used by pipeline
# -----------------------------------------------------------------------------

def store_raw(raw: RawEvent) -> None:
    """
    Optional: keep raw evidence elsewhere later.
    For now this is a no-op (your canonical pipeline uses source_happenings).
    """
    return


def enqueue_source_happening(ev: NormalizedEvent) -> None:
    """
    Upsert into source_happenings.

    IMPORTANT:
    - Your DB has UNIQUE(source_id, external_id) (idx_source_happenings_external_unique)
      so we must use on_conflict="source_id,external_id" when external_id is present.
    - If external_id is missing (rare), fall back to on_conflict="source_id,dedupe_key"
      (only works if you created that unique index too).
    - Serialize datetimes/dates as ISO strings to avoid JSON serialization errors.
    """
    supabase = get_supabase()

    image_url = _extract_image_url(ev.extra)

    extraction_method = None
    source_tier = "A"
    if isinstance(ev.extra, dict):
        em = ev.extra.get("extraction_method")
        if isinstance(em, str):
            extraction_method = em.strip() or None
        st = ev.extra.get("source_tier")
        if isinstance(st, str) and st.strip().upper() in ("A", "B", "C"):
            source_tier = st.strip().upper()

    item_url = (ev.canonical_url or "").strip() or None
    external_id = (ev.external_id or "").strip() or None

    # Local dates (required for matching + queue + dedupe_key)
    start_date_local = ev.start_at.date() if ev.start_at else None
    end_date_local = ev.end_at.date() if ev.end_at else start_date_local

    location_raw = getattr(ev, "location_name", None) or getattr(ev, "location_address", None)

    try:
        dedupe_key = compute_dedupe_key(
            source_id=ev.source_id,
            title=ev.title,
            start_date_local=_date_iso(start_date_local),
            location=location_raw,
            item_url=item_url,
            external_id=external_id,
        )

        global _DEDUPE_CONTENT, _DEDUPE_FALLBACK

        # Content-based path requires title + date
        if ev.title and start_date_local:
            _DEDUPE_CONTENT += 1
        else:
            _DEDUPE_FALLBACK += 1

    except ValueError:
        global _DEDUPE_ERROR
        _DEDUPE_ERROR += 1

        print(
            f"[storage] SKIP item: cannot derive dedupe_key"
            f" | source_id={ev.source_id}"
        )
        return

    payload: dict[str, Any] = {
        "source_id": ev.source_id,
        "source_type": "crawler",
        "source_tier": source_tier,
        "external_id": external_id,
        "title_raw": ev.title,
        "datetime_raw": None,
        "location_raw": location_raw,
        "description_raw": ev.description,
        "date_precision": ev.date_precision,
        "start_at": _dt_iso(ev.start_at),
        "end_at": _dt_iso(ev.end_at),
        "timezone": ev.timezone,
        "extraction_method": extraction_method,
        "item_url": item_url,
        "content_hash": None,
        "dedupe_key": dedupe_key,
        "status": "queued",
        "error_message": None,
        "fetched_at": datetime.now(timezone.utc).replace(microsecond=0).isoformat(),
        "start_date_local": _date_iso(start_date_local),
        "end_date_local": _date_iso(end_date_local),
        "image_url": image_url,
    }

    payload = _enforce_time_contract(payload)

    try:
        supabase.table("source_happenings").upsert(
            payload,
            on_conflict="source_id,dedupe_key",
        ).execute()

    except Exception as e:
        print("[storage] enqueue_source_happening FAILED:", repr(e))
        print(
            "[storage] payload summary:",
            {
                "source_id": ev.source_id,
                "external_id": external_id,
                "item_url": item_url,
                "dedupe_key": dedupe_key,
                "date_precision": ev.date_precision,
            },
        )
        raise


def upsert_event(ev: NormalizedEvent) -> None:
    """
    For Phase 1, 'upsert_event' = enqueue a source_happening row.
    """
    enqueue_source_happening(ev)


def upsert_source_happening_row(payload: dict[str, Any]) -> bool:
    """
    Upsert a raw dict into source_happenings.

    Derives dedupe_key centrally (external_id -> item_url -> skip+log).
    Returns True if upserted, False if skipped.
    """
    source_id = payload.get("source_id") or ""
    external_id = (payload.get("external_id") or "").strip() or None
    item_url = (payload.get("item_url") or "").strip() or None

    try:
        dedupe_key = compute_dedupe_key(
            source_id=source_id,
            title=payload.get("title_raw"),
            start_date_local=payload.get("start_date_local"),
            location=payload.get("location_raw"),
            item_url=item_url,
            external_id=external_id,
        )
    except ValueError:
        print(
            f"[storage] SKIP item: cannot derive dedupe_key"
            f" | source_id={source_id} external_id={external_id} item_url={item_url}"
        )
        return False

    payload["dedupe_key"] = dedupe_key
    payload = _enforce_time_contract(payload)

    supabase = get_supabase()
    try:
        supabase.table("source_happenings").upsert(
            payload,
            on_conflict="source_id,dedupe_key",
        ).execute()
        return True
    except Exception as e:
        print("[storage] upsert_source_happening_row FAILED:", repr(e))
        print(
            "[storage] payload summary:",
            {
                "source_id": source_id,
                "external_id": external_id,
                "item_url": item_url,
                "dedupe_key": dedupe_key,
            },
        )
        raise


def insert_schedules(
    *,
    event_external_id: str,
    raw_datetime: str,
    event_type: str,
    event_start_at_utc: Any,
    event_end_at_utc: Any,
    event_tz: str,
) -> None:
    """
    If you still use schedules, keep it.
    If not, safely no-op for now.
    """
    return
