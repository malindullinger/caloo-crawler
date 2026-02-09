"""
Bridge Maennedorf events to canonical schema (happening/offering/occurrence).

TIER B SOURCE — MUNICIPAL EXCEPTION
===================================
Classification: Tier B (Explicit text-based exception)
Decision: 2026-02-09
Status: Explicitly approved for text-based parsing

This source does NOT provide structured datetime (no JSON-LD, no <time>, no API).
Text heuristics are allowed ONLY for this source with strict constraints:
- Pattern: "D. Mon. YYYY, HH.MM Uhr - HH.MM Uhr"
- No inference, no defaults, no guessing
- If parsing fails → date_precision='date', times=NULL

See docs/tier-b-sources.md for full constraints.
"""
from __future__ import annotations

from datetime import datetime, timezone
from zoneinfo import ZoneInfo
import hashlib
from typing import Any, Dict, Optional

from src.storage import supabase

SOURCE_ID = "maennedorf_portal"
TZ = "Europe/Zurich"
ZURICH = ZoneInfo(TZ)

DRY_RUN = False  # Set True to log without writing


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def short_id(s: str, n: int = 24) -> str:
    """Stable short id (helps if public_id has length limits)."""
    return hashlib.sha1(s.encode("utf-8")).hexdigest()[:n]


def make_public_id(kind: str, ext_id: str, extra: str = "") -> str:
    base = f"{SOURCE_ID}:{kind}:{ext_id}{(':' + extra) if extra else ''}"
    return f"{SOURCE_ID}:{kind}:{short_id(base)}"


def safe_upsert(table: str, payload: Dict[str, Any], on_conflict: str) -> Dict[str, Any]:
    if DRY_RUN:
        print(f"[DRY_RUN] UPSERT {table} on {on_conflict}: {payload}")
        return {"id": "dry-run-id"}
    res = supabase.from_(table).upsert(payload, on_conflict=on_conflict).execute()
    if not res.data:
        raise RuntimeError(f"Upsert returned no data for {table}: {payload}")
    return res.data[0]


def zurich_date(dt_iso: str) -> Optional[str]:
    if not dt_iso:
        return None
    dt = datetime.fromisoformat(dt_iso.replace("Z", "+00:00"))
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=ZURICH)
    return dt.astimezone(ZURICH).date().isoformat()


def main() -> None:
    print("=== Bridge Maennedorf -> canonical (happening/offering/occurrence) ===")
    print(f"Source: {SOURCE_ID} (TIER B - text heuristics)")
    print(f"TZ: {TZ} | DRY_RUN: {DRY_RUN}\n")

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
    bridged = 0
    skipped = 0

    for e in events:
        ext_id = e["external_id"]
        title = (e.get("title") or "Untitled").strip()
        start_at = e.get("start_at")
        end_at = e.get("end_at")

        if not start_at:
            print(f"Skip {ext_id[:12]}… (no start_at)")
            skipped += 1
            continue

        # Skip if end_at is before start_at (data quality issue)
        if end_at and start_at:
            from datetime import datetime as dt
            try:
                start_dt = dt.fromisoformat(start_at.replace("Z", "+00:00"))
                end_dt = dt.fromisoformat(end_at.replace("Z", "+00:00"))
                if end_dt < start_dt:
                    print(f"Skip {ext_id[:12]}… (end_at before start_at)")
                    skipped += 1
                    continue
            except ValueError:
                pass

        # 1) HAPPENING
        happening_pid = make_public_id("happening", ext_id)
        happening_payload = {
            "public_id": happening_pid,
            "happening_kind": "event",
            "title": title,
            "visibility_status": "published",
            "updated_at": now_iso,
        }
        happening = safe_upsert("happening", happening_payload, on_conflict="public_id")
        happening_id = happening["id"]

        # 2) OFFERING
        offering_pid = make_public_id("offering", ext_id, "default")
        start_date_local = zurich_date(start_at)
        end_date_local = zurich_date(end_at) if end_at else start_date_local

        offering_payload = {
            "public_id": offering_pid,
            "happening_id": happening_id,
            "offering_type": "one_off",
            "timezone": TZ,
            "start_date": start_date_local,
            "end_date": end_date_local,
            "updated_at": now_iso,
        }
        offering = safe_upsert("offering", offering_payload, on_conflict="public_id")
        offering_id = offering["id"]

        # 3) OCCURRENCE
        occ_pid = make_public_id("occ", ext_id, start_at)
        occurrence_payload = {
            "public_id": occ_pid,
            "offering_id": offering_id,
            "start_at": start_at,
            "end_at": end_at,
            "status": "scheduled",
            "notes": f"bridged_from={SOURCE_ID} (tier_b:text_heuristic)",
            "updated_at": now_iso,
        }
        safe_upsert("occurrence", occurrence_payload, on_conflict="public_id")

        print(f"✅ Bridged: {title[:60]}")
        bridged += 1

    print(f"\nDone. Bridged: {bridged}, Skipped: {skipped}")


if __name__ == "__main__":
    main()
