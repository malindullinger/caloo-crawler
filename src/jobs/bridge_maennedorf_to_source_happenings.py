# src/jobs/bridge_maennedorf_to_source_happenings.py
from __future__ import annotations

import argparse
import os
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

from supabase import Client, create_client

# Centralized upsert (dedupe_key derivation + on_conflict="source_id,dedupe_key")
from src.storage import upsert_source_happening_row


# ---------------------------------------------------------------------------
# Minimal dotenv loader (no dependency)
# ---------------------------------------------------------------------------

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


def _get_supabase_client() -> Client:
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
            "Missing Supabase env vars. Need SUPABASE_URL and SUPABASE_SERVICE_ROLE_KEY (preferred)."
        )
    return create_client(url, key)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _to_iso(dt: Any) -> Optional[str]:
    # Accept datetime, or ISO string already
    if dt is None:
        return None
    if isinstance(dt, str):
        return dt
    if isinstance(dt, datetime):
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return dt.astimezone(timezone.utc).isoformat()
    return str(dt)


def _extract_image_url(extra: Any) -> Optional[str]:
    if not isinstance(extra, dict):
        return None
    img = extra.get("image_url")
    if isinstance(img, str):
        img = img.strip()
        return img or None
    return None


def _safe_status(s: Any) -> str:
    return (s or "").strip().lower()


# ---------------------------------------------------------------------------
# Bridge logic
# ---------------------------------------------------------------------------

SOURCE_ID = "maennedorf_portal"
SOURCE_TYPE = "crawler"
SOURCE_TIER = "B"

# source_happenings terminal-ish statuses we shouldn’t overwrite by default
DO_NOT_TOUCH = {"processed", "ignored"}


def _fetch_events(supabase: Client, *, limit: int) -> List[Dict[str, Any]]:
    # Reads from normalized events table (your pipeline writes here)
    resp = (
        supabase.table("events")
        .select(
            "external_id,source_id,title,start_at,end_at,timezone,location_name,description,canonical_url,last_seen_at"
        )
        .eq("source_id", SOURCE_ID)
        .order("last_seen_at", desc=True)
        .limit(limit)
        .execute()
    )
    return resp.data or []


def _get_existing_status_by_external_id(
    supabase: Client,
    external_ids: List[str],
) -> Dict[str, str]:
    if not external_ids:
        return {}

    resp = (
        supabase.table("source_happenings")
        .select("external_id,status")
        .eq("source_id", SOURCE_ID)
        .in_("external_id", external_ids)
        .execute()
    )
    rows = resp.data or []
    out: Dict[str, str] = {}
    for r in rows:
        eid = r.get("external_id")
        if not eid:
            continue
        out[str(eid)] = _safe_status(r.get("status"))
    return out


def bridge(*, supabase: Client, live: bool, limit: int) -> None:
    now_utc = datetime.now(timezone.utc)

    events = _fetch_events(supabase, limit=limit)
    print(f"[bridge] fetched {len(events)} events for source_id={SOURCE_ID}")

    external_ids = [str(e.get("external_id")) for e in events if e.get("external_id")]
    existing_status = _get_existing_status_by_external_id(supabase, external_ids)

    rows: List[Dict[str, Any]] = []
    terminal_skipped = 0

    for ev in events:
        external_id = ev.get("external_id")
        if not external_id:
            # Keep this strict for now to minimize scope for the first refactor.
            # (Later we can allow missing external_id if canonical_url exists.)
            continue

        # Respect terminal states
        st = existing_status.get(str(external_id))
        if st in DO_NOT_TOUCH:
            terminal_skipped += 1
            continue

        item_url = ev.get("canonical_url")  # stored as text in source_happenings
        title_raw = ev.get("title")
        description_raw = ev.get("description")
        location_raw = ev.get("location_name")
        tz = ev.get("timezone") or "Europe/Zurich"

        image_url = _extract_image_url(ev.get("extra"))

        start_at = _to_iso(ev.get("start_at"))
        end_at = _to_iso(ev.get("end_at"))
        date_precision = "datetime" if start_at else "date"

        row: Dict[str, Any] = {
            "source_id": SOURCE_ID,
            "source_type": SOURCE_TYPE,
            "source_tier": SOURCE_TIER,
            "external_id": str(external_id),
            "item_url": str(item_url) if item_url else None,
            "title_raw": title_raw,
            # keep datetime_raw as nullable (merge_loop uses start_date_local anyway)
            "datetime_raw": None,
            "location_raw": location_raw,
            "description_raw": description_raw,
            "date_precision": date_precision,
            "timezone": tz,
            "start_at": start_at,
            "end_at": end_at,
            # if your table requires these, keep them null-safe
            "start_date_local": None,
            "end_date_local": None,
            "image_url": image_url,
            "status": "needs_review",  # safe default; merge_loop can process with flag
            "fetched_at": _to_iso(ev.get("last_seen_at")) or now_utc.isoformat(),
            "updated_at": now_utc.isoformat(),
        }

        # Don’t send nulls unnecessarily
        row = {k: v for k, v in row.items() if v is not None}
        rows.append(row)

    if not live:
        print(
            f"[bridge] DRY RUN: would upsert {len(rows)} terminal_skipped={terminal_skipped}"
        )
        return

    upserted = 0
    skipped = 0

    for row in rows:
        ok = upsert_source_happening_row(row)
        if ok:
            upserted += 1
        else:
            skipped += 1

    print(
        f"[bridge] upsert complete upserted={upserted} skipped={skipped} "
        f"terminal_skipped={terminal_skipped} live={live}"
    )


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Bridge maennedorf events -> source_happenings"
    )
    parser.add_argument("--live", action="store_true", help="Write to DB (default is dry run)")
    parser.add_argument("--limit", type=int, default=200, help="Max events to bridge")
    args = parser.parse_args()

    supabase = _get_supabase_client()
    bridge(supabase=supabase, live=bool(args.live), limit=int(args.limit))


if __name__ == "__main__":
    main()
