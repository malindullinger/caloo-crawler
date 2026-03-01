# src/canonicalize/eligibility.py
from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, date
from typing import Any, Mapping, Sequence

from src.junk_titles import is_junk_title


@dataclass(frozen=True)
class EligibilityResult:
    eligible: bool
    reasons: list[str]


def _get(h: Mapping[str, Any], *keys: str) -> Any:
    """Return first existing key in mapping (supports gradual schema evolution)."""
    for k in keys:
        if k in h:
            return h[k]
    return None


def _has_any_location(h: Mapping[str, Any]) -> bool:
    # Allow multiple possible schemas without coupling.
    is_online = bool(_get(h, "is_online", "online", "online_event"))
    location_name = _get(h, "location_name", "venue_name", "venue", "location")
    address = _get(h, "address", "street_address")
    lat = _get(h, "lat", "latitude")
    lng = _get(h, "lng", "longitude")
    has_geo = (lat is not None and lng is not None)

    return is_online or bool(location_name) or bool(address) or has_geo


def _is_cancelled(h: Mapping[str, Any]) -> bool:
    status = (_get(h, "status") or "").strip().lower()
    return status == "cancelled"


def _time_contract_ok(h: Mapping[str, Any]) -> bool:
    """
    v1 contract mirror (canonical side):
    - if date_precision == 'date' => start_at/end_at must be NULL
    - if date_precision == 'datetime' => start_at is required
    """
    precision = (_get(h, "date_precision") or "").strip().lower()
    start_at = _get(h, "start_at")
    end_at = _get(h, "end_at")

    if precision == "date":
        return start_at is None and end_at is None

    if precision == "datetime":
        return start_at is not None

    # If precision missing/unknown, fail closed (auditable)
    return False


def _newborn_only_excluded_v1(h: Mapping[str, Any]) -> bool:
    """
    v1: exclude happenings that are *solely* for newborns/infants.
    This must be deterministic (no NLP inference).
    Accept signals like:
      - audience_age_group == 'newborn_only' / 'infant_only'
      - min_age_months/max_age_months boundaries
      - explicit flags
    """
    flag = _get(h, "is_newborn_only", "newborn_only")
    if flag is True:
        return True

    audience = (_get(h, "audience_age_group") or "").strip().lower()
    if audience in {"newborn_only", "infant_only", "babies_only"}:
        return True

    # If you store ages in months (recommended for precision):
    min_m = _get(h, "min_age_months")
    max_m = _get(h, "max_age_months")

    # newborn-only if max age <= 12 months (and min is 0/None)
    if max_m is not None:
        try:
            max_m_int = int(max_m)
            min_m_int = int(min_m) if min_m is not None else 0
            if max_m_int <= 12 and min_m_int <= 0:
                return True
        except (ValueError, TypeError):
            pass

    return False


def is_feed_eligible(
    happening: Mapping[str, Any],
    now: datetime | None = None,
    locale: str | None = None,
) -> EligibilityResult:
    """
    Single source of truth eligibility gate.
    - Pure function
    - Fail closed (eligible=False) when invariants are unclear
    - Returns explicit reasons for auditability
    """
    _ = now, locale  # reserved for v2 rules (time windows, locale exceptions)

    reasons: list[str] = []

    title = (_get(happening, "title") or "").strip()
    if not title:
        reasons.append("missing_title")

    if is_junk_title(title):
        reasons.append("junk_title")

    # Date presence: allow either a date-only canonical field OR start_at.
    # Prefer "start_date_local" or "start_date" if those exist on canonical.
    start_at = _get(happening, "start_at")
    start_date_local = _get(happening, "start_date_local", "start_date")

    has_date = isinstance(start_date_local, (date, str)) and bool(start_date_local)
    has_dt = start_at is not None
    if not (has_date or has_dt):
        reasons.append("missing_start_date_or_start_at")

    if not _has_any_location(happening):
        reasons.append("missing_location_or_online")

    if _is_cancelled(happening):
        reasons.append("cancelled")

    if not _time_contract_ok(happening):
        reasons.append("time_contract_violation_or_unknown_precision")

    if _newborn_only_excluded_v1(happening):
        reasons.append("excluded_newborn_only_v1")

    return EligibilityResult(eligible=(len(reasons) == 0), reasons=reasons)
