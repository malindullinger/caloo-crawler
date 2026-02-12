from __future__ import annotations

import hashlib
import re
from datetime import datetime, timezone
from typing import Optional, Tuple
from zoneinfo import ZoneInfo

import dateparser

from .config import TIMEZONE
from .models import RawEvent, NormalizedEvent


# ============================================================
# Helpers
# ============================================================

def _norm_text(s: Optional[str]) -> str:
    return " ".join((s or "").strip().lower().split())


def _sha256_hex(s: str) -> str:
    return hashlib.sha256(s.encode("utf-8")).hexdigest()


def make_external_id(
    *,
    source_id: str,
    item_url: Optional[str],
    title: str,
    start_at_utc: datetime,
    location_name: Optional[str],
) -> str:
    """
    Stable external id:
    - If item_url exists: hash(source_id + url)
    - Else: hash(source_id + normalized title + start_at + location)
    """
    if item_url:
        return _sha256_hex(f"{source_id}|url|{item_url.strip()}")

    seed = "|".join(
        [
            source_id,
            _norm_text(title),
            start_at_utc.replace(microsecond=0).isoformat(),
            _norm_text(location_name),
        ]
    )
    return _sha256_hex(seed)


def _has_time_hint(s: str) -> bool:
    """
    Heuristic: does the raw string look like it contains time info?
    Catches:
      - "2026-01-22T15:00:00" (ISO 8601 with time)
      - "2026-01-22 15:00:00" (ISO-ish with space)
      - "15:00"
      - "18.00 Uhr"
      - "Uhr"
    """
    if not s:
        return False

    s = s.strip()
    s_low = s.lower()

    # ISO 8601 with T or space
    if re.search(r"\d{4}-\d{2}-\d{2}[T ]\d{2}:\d{2}", s):
        return True

    return bool(
        re.search(r"\b\d{1,2}:\d{2}\b", s_low)
        or re.search(r"\b\d{1,2}\.\d{2}\s*uhr\b", s_low)
        or ("uhr" in s_low)
    )


# ============================================================
# Generic datetime parsing (dateparser fallback)
# ============================================================

def _parse_with_dateparser(text: str) -> Optional[datetime]:
    """
    Fallback parser for simpler strings:
      - '24.01.2026'
      - 'Sa, 24.01.2026, 15:00'
    Returns timezone-aware datetime in TIMEZONE.
    """
    s = (text or "").strip()
    if not s:
        return None

    return dateparser.parse(
        s,
        languages=["de", "en"],
        settings={
            "TIMEZONE": TIMEZONE,
            "RETURN_AS_TIMEZONE_AWARE": True,
            "PREFER_DATES_FROM": "future",
            "DATE_ORDER": "DMY",
        },
    )


# Matches numeric range: "06.01.2026 - 10.02.2026"
_NUM_RANGE_RE = re.compile(
    r"^\s*(\d{2}\.\d{2}\.\d{4})\s*-\s*(\d{2}\.\d{2}\.\d{4})\s*$"
)

# ISO range with " | " separator: "2026-01-22T15:00 | 2026-01-22T17:00"
_ISO_PIPE_RANGE_RE = re.compile(
    r"(\d{4}-\d{2}-\d{2}(?:T\d{2}:\d{2}(?::\d{2})?(?:[+-]\d{2}:\d{2}|Z)?)?)"
    r"\s*\|\s*"
    r"(\d{4}-\d{2}-\d{2}(?:T\d{2}:\d{2}(?::\d{2})?(?:[+-]\d{2}:\d{2}|Z)?)?)"
)

# Single ISO datetime: "2026-01-22T15:00:00" or "2026-01-22"
_ISO_SINGLE_RE = re.compile(
    r"^(\d{4}-\d{2}-\d{2}(?:T\d{2}:\d{2}(?::\d{2})?(?:[+-]\d{2}:\d{2}|Z)?)?)$"
)


def _parse_iso_strict(s: str, tz: ZoneInfo) -> Optional[datetime]:
    """Parse ISO 8601 string using fromisoformat (not dateparser).

    - Handles trailing 'Z' as UTC
    - Naive datetimes get default_tz (Europe/Zurich)
    """
    if not s:
        return None
    s = s.strip()
    if s.endswith("Z"):
        s = s[:-1] + "+00:00"
    try:
        dt = datetime.fromisoformat(s)
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=tz)
        return dt
    except ValueError:
        return None


# ============================================================
# DETAIL PAGE datetime parsing (Maennedorf-style)
# ============================================================

_DE_MONTHS = {
    "jan": 1, "januar": 1,
    "feb": 2, "februar": 2,
    "mär": 3, "maerz": 3, "märz": 3,
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


def _month_to_int(mon_raw: str) -> Optional[int]:
    m = (mon_raw or "").strip().lower()
    m = m.replace("ä", "ae").replace("ö", "oe").replace("ü", "ue")
    m = m.replace(".", "")
    if m in _DE_MONTHS:
        return _DE_MONTHS[m]
    if len(m) >= 3 and m[:3] in _DE_MONTHS:
        return _DE_MONTHS[m[:3]]
    return None


# 22. Jan. 2026, 18.00 Uhr - 23.00 Uhr
_SINGLE_DE_RE = re.compile(
    r"(?P<day>\d{1,2})\.\s*(?P<mon>[A-Za-zÄÖÜäöü]+)\.?\s*(?P<year>\d{4})"
    r"(?:,\s*)?"
    r"(?P<start_h>\d{1,2})\.(?P<start_m>\d{2})\s*Uhr"
    r"(?:\s*-\s*(?P<end_h>\d{1,2})\.(?P<end_m>\d{2})\s*Uhr)?"
)

# 6. Jan. 2026 - 10. Feb. 2026, 14.00 Uhr - 14.45 Uhr, 45 Minuten
_RANGE_DE_RE = re.compile(
    r"(?P<sd>\d{1,2})\.\s*(?P<smon>[A-Za-zÄÖÜäöü]+)\.?\s*(?P<sy>\d{4})\s*-\s*"
    r"(?P<ed>\d{1,2})\.\s*(?P<emon>[A-Za-zÄÖÜäöü]+)\.?\s*(?P<ey>\d{4})"
    r"(?:,\s*)?"
    r"(?:(?P<start_h>\d{1,2})\.(?P<start_m>\d{2})\s*Uhr"
    r"(?:\s*-\s*(?P<end_h>\d{1,2})\.(?P<end_m>\d{2})\s*Uhr)?)?"
)


def parse_datetime_or_range(
    datetime_raw: str,
    tz_name: str = TIMEZONE,
) -> Tuple[Optional[datetime], Optional[datetime]]:
    """
    Returns (start_local, end_local) as timezone-aware datetimes.
    """
    s = (datetime_raw or "").strip()
    if not s:
        return None, None

    tz = ZoneInfo(tz_name)

    # 0a) ISO pipe-separated range
    m = _ISO_PIPE_RANGE_RE.match(s)
    if m:
        start_dt = _parse_iso_strict(m.group(1), tz)
        end_dt = _parse_iso_strict(m.group(2), tz)
        if start_dt:
            return start_dt, end_dt

    # 0b) Single ISO
    m = _ISO_SINGLE_RE.match(s)
    if m:
        start_dt = _parse_iso_strict(m.group(1), tz)
        if start_dt:
            return start_dt, None

    # 1) Numeric date range
    m = _NUM_RANGE_RE.match(s)
    if m:
        start_date = _parse_with_dateparser(m.group(1))
        end_date = _parse_with_dateparser(m.group(2))
        if start_date and end_date:
            start_local = start_date.astimezone(tz).replace(hour=0, minute=0, second=0, microsecond=0)
            end_local = end_date.astimezone(tz).replace(hour=23, minute=59, second=0, microsecond=0)
            return start_local, end_local

    # 2) Range with German month names (+ optional time)
    m = _RANGE_DE_RE.search(s)
    if m:
        sm = _month_to_int(m.group("smon"))
        em = _month_to_int(m.group("emon"))
        if sm and em:
            sd = int(m.group("sd")); sy = int(m.group("sy"))
            ed = int(m.group("ed")); ey = int(m.group("ey"))

            sh = m.group("start_h"); smin = m.group("start_m")
            eh = m.group("end_h"); emin = m.group("end_m")

            start_h = int(sh) if sh else 0
            start_m = int(smin) if smin else 0

            if eh and emin:
                end_h = int(eh); end_m = int(emin)
            else:
                end_h = 23; end_m = 59

            start_local = datetime(sy, sm, sd, start_h, start_m, tzinfo=tz)
            end_local = datetime(ey, em, ed, end_h, end_m, tzinfo=tz)
            return start_local, end_local

    # 3) Single date with time (German month names)
    m = _SINGLE_DE_RE.search(s)
    if m:
        mon = _month_to_int(m.group("mon"))
        if mon:
            day = int(m.group("day"))
            year = int(m.group("year"))
            start_local = datetime(
                year, mon, day,
                int(m.group("start_h")), int(m.group("start_m")),
                tzinfo=tz
            )
            end_local = None
            if m.group("end_h") and m.group("end_m"):
                end_local = datetime(
                    year, mon, day,
                    int(m.group("end_h")), int(m.group("end_m")),
                    tzinfo=tz
                )
            return start_local, end_local

    # 4) Fallback
    start_local = _parse_with_dateparser(s)
    return start_local, None


# ============================================================
# Raw → Normalized
# ============================================================

def raw_to_normalized(
    raw: RawEvent,
    now_utc: datetime,
) -> Optional[NormalizedEvent]:
    """
    Core normalization.

    Fixes:
    - date_precision is derived from BOTH raw hints and parsed datetime values
      (prevents Eventbrite time rows being misclassified as 'date').
    """
    start_local, end_local = parse_datetime_or_range(raw.datetime_raw, tz_name=TIMEZONE)
    if not start_local:
        return None

    tz = ZoneInfo(TIMEZONE)

    # Ensure tz-aware (defensive)
    if start_local.tzinfo is None:
        start_local = start_local.replace(tzinfo=tz)
    if end_local and end_local.tzinfo is None:
        end_local = end_local.replace(tzinfo=tz)

    # Convert to UTC
    start_at_utc = start_local.astimezone(timezone.utc)
    end_at_utc = end_local.astimezone(timezone.utc) if end_local else None

    raw_s = (raw.datetime_raw or "").strip()
    raw_has_time = _has_time_hint(raw_s)

    # Determine if parsed datetimes actually contain "meaningful time"
    # - For date-only singles, start_local is typically 00:00
    # - For date-only ranges we intentionally set end_local to 23:59
    parsed_start_has_time = (start_local.hour, start_local.minute) != (0, 0)

    parsed_end_has_meaningful_time = False
    if end_local:
        # If it's a real single-day event with end time, this will be true (not just 23:59 default)
        if end_local.date() == start_local.date():
            parsed_end_has_meaningful_time = True
        else:
            # multi-day ranges:
            # only treat as having "time" if start isn't 00:00 or end isn't the 23:59 default
            parsed_end_has_meaningful_time = (
                (start_local.hour, start_local.minute) != (0, 0)
                or (end_local.hour, end_local.minute) != (23, 59)
            )

    has_time = bool(raw_has_time or parsed_start_has_time or parsed_end_has_meaningful_time)

    # Event type classification
    if end_local:
        if end_local.date() == start_local.date():
            event_type = "single"
        else:
            event_type = "date_range"
    else:
        event_type = "single"

    # Precision flags
    if has_time:
        is_all_day = False
        date_precision = "datetime"
    else:
        is_all_day = True
        date_precision = "date"

    title = (raw.title_raw or "").strip()
    location_name = raw.location_raw.strip() if raw.location_raw else None

    external_id = make_external_id(
        source_id=raw.source_id,
        item_url=str(raw.item_url) if raw.item_url else None,
        title=title,
        start_at_utc=start_at_utc,
        location_name=location_name,
    )

    canonical_url = str(raw.item_url) if raw.item_url else str(raw.source_url)

    return NormalizedEvent(
        external_id=external_id,
        source_id=raw.source_id,
        event_type=event_type,
        is_all_day=is_all_day,
        date_precision=date_precision,
        title=title,
        start_at=start_at_utc,
        end_at=end_at_utc,
        timezone=TIMEZONE,
        location_name=location_name,
        description=raw.description_raw.strip() if raw.description_raw else None,
        canonical_url=canonical_url,
        last_seen_at=now_utc,
        extra=raw.extra or {},  # ✅ image_url survives
    )
