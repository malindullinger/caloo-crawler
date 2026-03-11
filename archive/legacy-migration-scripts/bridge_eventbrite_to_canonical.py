from __future__ import annotations

from datetime import datetime, timezone
from zoneinfo import ZoneInfo
import hashlib
from typing import Any, Dict, Optional

from src.storage import supabase

SOURCE_ID = "eventbrite-zurich"
TZ = "Europe/Zurich"
ZURICH = ZoneInfo(TZ)

DRY_RUN = False  # auf True setzen, wenn du erst nur loggen willst


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def short_id(s: str, n: int = 24) -> str:
    """Stable short id (helps if public_id has length limits)."""
    return hashlib.sha1(s.encode("utf-8")).hexdigest()[:n]


def make_public_id(kind: str, ext_id: str, extra: str = "") -> str:
    base = f"{SOURCE_ID}:{kind}:{ext_id}{(':' + extra) if extra else ''}"
    # Keep it compact + stable
    return f"{SOURCE_ID}:{kind}:{short_id(base)}"


def safe_upsert(table: str, payload: Dict[str, Any], on_conflict: str) -> Dict[str, Any]:
    if DRY_RUN:
        print(f"[DRY_RUN] UPSERT {table} on {on_conflict}: {payload}")
        return {}
    # Note: supabase-py v2 requires .execute() directly after upsert
    res = supabase.from_(table).upsert(payload, on_conflict=on_conflict).execute()
    if not res.data:
        raise RuntimeError(f"Upsert returned no data for {table}: {payload}")
    return res.data[0]


def safe_select_one(table: str, filters: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    q = supabase.from_(table).select("*")
    for k, v in filters.items():
        q = q.eq(k, v)
    res = q.limit(1).execute()
    return res.data[0] if res.data else None


def zurich_date(dt_iso: str) -> Optional[str]:
    if not dt_iso:
        return None
    dt = datetime.fromisoformat(dt_iso.replace("Z", "+00:00"))
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=ZURICH)
    return dt.astimezone(ZURICH).date().isoformat()


def main() -> None:
    print("=== Bridge Eventbrite -> canonical (happening/offering/occurrence) ===")
    print(f"Source: {SOURCE_ID} | TZ: {TZ} | DRY_RUN: {DRY_RUN}\n")

    ev_res = (
        supabase
        .from_("events")
        .select("external_id,title,start_at,end_at,date_precision,source_id")
        .eq("source_id", SOURCE_ID)
        .execute()
    )
    events = ev_res.data or []
    print(f"Found {len(events)} rows in public.events for {SOURCE_ID}\n")
    if not events:
        return

    now_iso = utc_now_iso()

    for e in events:
        ext_id = e["external_id"]
        title = (e.get("title") or "Untitled").strip()
        start_at = e.get("start_at")
        end_at = e.get("end_at")

        if not start_at:
            print(f"Skip {ext_id[:12]}… (no start_at)")
            continue

        # 1) HAPPENING
        happening_pid = make_public_id("happening", ext_id)
        happening_payload = {
            "public_id": happening_pid,
            "happening_kind": "event",        # ggf. an euer Enum anpassen
            "title": title,
            "visibility_status": "published", # feed_cards_view verlangt published
            "updated_at": now_iso,
        }
        happening = safe_upsert("happening", happening_payload, on_conflict="public_id")
        happening_id = happening["id"]

        # 2) OFFERING (1 default offering pro happening)
        offering_pid = make_public_id("offering", ext_id, "default")
        start_date_local = zurich_date(start_at)
        end_date_local = zurich_date(end_at) if end_at else start_date_local  # default to start if no end

        offering_payload = {
            "public_id": offering_pid,
            "happening_id": happening_id,
            "offering_type": "one_off",       # valid enum: one_off, series
            "timezone": TZ,
            "start_date": start_date_local,
            "end_date": end_date_local,
            "updated_at": now_iso,
        }
        offering = safe_upsert("offering", offering_payload, on_conflict="public_id")
        offering_id = offering["id"]

        # 3) OCCURRENCE (1 occurrence pro event-row)
        # public_id soll pro Startzeit eindeutig sein (falls später mehrere Termine pro Happening kommen)
        occ_pid = make_public_id("occ", ext_id, start_at)
        occurrence_payload = {
            "public_id": occ_pid,
            "offering_id": offering_id,
            "start_at": start_at,
            "end_at": end_at,
            "status": "scheduled",            # feed_cards_view verlangt scheduled
            "notes": f"bridged_from={SOURCE_ID}",
            "updated_at": now_iso,
        }
        occ = safe_upsert("occurrence", occurrence_payload, on_conflict="public_id")

        print(f"✅ Bridged: {title[:60]}")
        print(f"   happening.public_id:  {happening_pid}")
        print(f"   offering.public_id:   {offering_pid}")
        print(f"   occurrence.public_id: {occ_pid}\n")

    print("Done.")


if __name__ == "__main__":
    main()
