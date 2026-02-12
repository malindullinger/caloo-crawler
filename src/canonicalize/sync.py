# src/canonicalize/sync.py
from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime
from hashlib import sha256
import re
from typing import Any, Dict, Optional

try:
    from zoneinfo import ZoneInfo  # py>=3.9
except Exception:  # pragma: no cover
    ZoneInfo = None  # type: ignore


ZURICH_TZ = "Europe/Zurich"
ZURICH = ZoneInfo(ZURICH_TZ) if ZoneInfo else None

# Pipeline statuses for source_happenings (Phase 1)
# - queued: has minimum evidence to attempt canonicalization (date derived)
# - needs_review: contract violation or insufficient evidence
# - processed: set later by merge loop (linked/created/rejected)
STATUS_QUEUED = "queued"
STATUS_NEEDS_REVIEW = "needs_review"

# Minimal German month map (canonicalization must be independent of crawler code)
GERMAN_MONTHS = {
    "jan": 1, "januar": 1,
    "feb": 2, "februar": 2,
    "mär": 3, "märz": 3, "maerz": 3,
    "apr": 4, "april": 4,
    "mai": 5,
    "jun": 6, "juni": 6,
    "jul": 7, "juli": 7,
    "aug": 8, "august": 8,
    "sep": 9, "sept": 9, "september": 9,
    "okt": 10, "oktober": 10,
    "nov": 11, "november": 11,
    "dez": 12, "dezember": 12,
}

DATE_RE = re.compile(r"(\d{1,2})\.\s*([A-Za-zÄÖÜäöü]+)\.?\s*(\d{4})")


@dataclass
class SyncResult:
    dry_run: bool
    events_seen: int = 0
    upserted: int = 0
    needs_review: int = 0
    errors: int = 0


def _parse_iso_datetime(value: str) -> Optional[datetime]:
    """
    Parse ISO datetime (events.start_at etc).
    Handles 'Z' suffix.
    """
    if not value:
        return None
    v = value.strip()
    try:
        if v.endswith("Z"):
            v = v[:-1] + "+00:00"
        return datetime.fromisoformat(v)
    except Exception:
        return None


def _to_zurich_date(dt: datetime) -> Optional[date]:
    if not dt:
        return None
    try:
        if ZURICH is None:
            # Fallback: treat as already-local date
            return dt.date()
        if dt.tzinfo is None:
            # Assume UTC if tz missing (rare, but be explicit)
            dt = dt.replace(tzinfo=ZoneInfo("UTC"))
        return dt.astimezone(ZURICH).date()
    except Exception:
        return None


def parse_date_from_datetime_raw(datetime_raw: str) -> Optional[date]:
    """
    Allowed parsing source #2: events.datetime_raw (human-readable).
    We ONLY extract explicit date patterns like:
      '27. Feb. 2026, 19.30 Uhr - 21.00 Uhr'
      '21. März 2026'
    """
    if not datetime_raw:
        return None
    m = DATE_RE.search(datetime_raw)
    if not m:
        return None

    day = int(m.group(1))
    month_str = (m.group(2) or "").strip().lower()
    year = int(m.group(3))

    # Normalize umlauts variants
    month_str = (
        month_str.replace("ä", "ae")
                 .replace("ö", "oe")
                 .replace("ü", "ue")
    )
    # Keep short forms like "feb"
    month_str = month_str[:3] if month_str not in GERMAN_MONTHS else month_str
    month = GERMAN_MONTHS.get(month_str) or GERMAN_MONTHS.get(month_str[:3])

    if not month:
        return None

    try:
        return date(year, month, day)
    except ValueError:
        return None


def normalize_title(title: Optional[str]) -> str:
    if not title:
        return ""
    s = title.lower().strip()
    s = re.sub(r"\s+", " ", s)
    s = re.sub(r"[^\w\s]", "", s)  # conservative: remove punctuation
    return s


def normalize_venue(venue: Optional[str]) -> str:
    if not venue:
        return ""
    s = venue.lower().strip()
    s = re.sub(r"\s+", " ", s)
    # light normalization only
    s = s.replace("str.", "strasse").replace("str ", "strasse ")
    s = s.rstrip(".,;")
    return s


def derive_dedupe_key(title_raw: Optional[str], start_date_local: Optional[date], location_raw: Optional[str]) -> str:
    """
    Deterministic fingerprint used for cross-source matching.
    Uses DATE (not timestamp) to support date-only sources.
    """
    t = normalize_title(title_raw)
    v = normalize_venue(location_raw)
    d = start_date_local.isoformat() if start_date_local else ""
    base = "|".join([x for x in (t, d, v) if x])
    return sha256(base.encode("utf-8")).hexdigest()[:32]


def infer_tier(source_id: str) -> str:
    """
    Keep this minimal and explicit.
    Anything not in the map defaults to 'A' for now (your current preference),
    but you can tighten later.
    """
    TIER_MAP = {
        "maennedorf_portal": "B",  # explicit municipal exception
    }
    return TIER_MAP.get(source_id, "A")


def sync_to_source_happenings(
    supabase: Any,
    source_id: Optional[str] = None,
    dry_run: bool = True,
    limit: Optional[int] = None,
) -> SyncResult:
    """
    Migrate rows from existing 'events' staging table into 'source_happenings'.

    HARD RULES:
    - start_date_local may be derived ONLY from:
        (1) events.start_at (if present) -> Zurich date
        (2) events.datetime_raw (if present) -> parse date
      Never parse from title.

    STATUS POLICY (Phase 1):
    - If a usable date can be derived -> status='queued' (ready for canonicalization loop)
    - If no date can be derived -> status='needs_review'
    - If date_precision contract is violated -> status='needs_review'
    """
    result = SyncResult(dry_run=dry_run)

    q = supabase.table("events").select("*")
    if source_id:
        q = q.eq("source_id", source_id)
    if limit:
        q = q.limit(limit)

    resp = q.execute()
    events = resp.data or []

    for ev in events:
        result.events_seen += 1
        try:
            ev_source_id = ev.get("source_id")
            ev_external_id = ev.get("external_id")

            # --- Allowed date sources only ---
            start_at_dt = _parse_iso_datetime(ev.get("start_at") or "")
            start_date_local = _to_zurich_date(start_at_dt) if start_at_dt else None

            if start_date_local is None:
                start_date_local = parse_date_from_datetime_raw(ev.get("datetime_raw") or "")

            end_at_dt = _parse_iso_datetime(ev.get("end_at") or "")
            end_date_local = _to_zurich_date(end_at_dt) if end_at_dt else None

            # Status gating:
            # - queued if we have a usable date
            # - needs_review if we cannot derive a date
            status = STATUS_QUEUED if start_date_local is not None else STATUS_NEEDS_REVIEW

            # --- Time contract (migration 008) ---
            dp = ev.get("date_precision") or "datetime"
            start_at_out = ev.get("start_at")
            end_at_out = ev.get("end_at")

            if dp == "date":
                # date-only: timestamps must be NULL
                start_at_out = None
                end_at_out = None
            elif dp == "datetime":
                # datetime precision must have explicit start_at evidence
                if not start_at_out:
                    status = STATUS_NEEDS_REVIEW

            if status == STATUS_NEEDS_REVIEW:
                result.needs_review += 1

            # external_id handling:
            # source_happenings allows external_id nullable, but upsert gets messy with NULL conflicts.
            # If missing, we use the events row id as a stable surrogate external_id.
            if not ev_external_id:
                if ev.get("id"):
                    ev_external_id = f"events:{ev['id']}"
                else:
                    # last resort: deterministic hash of (source_id + title + datetime_raw + item_url)
                    seed = f"{ev_source_id}|{ev.get('title')}|{ev.get('datetime_raw')}|{ev.get('canonical_url')}"
                    ev_external_id = "hash:" + sha256(seed.encode("utf-8")).hexdigest()[:24]

            # dedupe_key contract: must always be non-null.
            # ev_external_id is guaranteed set above (surrogate if needed).
            dedupe_key = ev_external_id
            if not dedupe_key:
                print(
                    f"[sync] SKIP item: empty dedupe_key"
                    f" | source_id={ev_source_id} item_url={ev.get('canonical_url')}"
                )
                result.errors += 1
                continue

            record: Dict[str, Any] = {
                "source_id": ev_source_id,
                "source_type": "crawler",
                "source_tier": infer_tier(ev_source_id),
                "external_id": ev_external_id,

                "title_raw": ev.get("title"),
                "datetime_raw": ev.get("datetime_raw"),
                "location_raw": ev.get("location_name"),
                "description_raw": ev.get("description"),

                "date_precision": dp,
                "start_at": start_at_out,
                "end_at": end_at_out,
                "timezone": ev.get("timezone") or ZURICH_TZ,

                "start_date_local": start_date_local.isoformat() if start_date_local else None,
                "end_date_local": end_date_local.isoformat() if end_date_local else None,

                "item_url": ev.get("canonical_url"),
                "content_hash": ev.get("content_hash"),
                "dedupe_key": dedupe_key,

                "status": status,
                "error_message": None,
                "fetched_at": ev.get("last_seen_at"),
            }

            if dry_run:
                result.upserted += 1
                continue

            # Upsert on (source_id, dedupe_key) – dedupe_key is always non-null
            supabase.table("source_happenings").upsert(
                record,
                on_conflict="source_id,dedupe_key",
            ).execute()

            result.upserted += 1

        except Exception:
            result.errors += 1
            # best-effort: mark needs_review if record exists; otherwise just continue
            continue

    return result
