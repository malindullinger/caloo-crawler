# src/canonicalize/merge_loop.py
from __future__ import annotations

import argparse
import hashlib
import os
import random
import time
from collections import defaultdict
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Mapping, Sequence
from uuid import uuid4

import httpx
from supabase import Client, create_client
from postgrest.exceptions import APIError

from src.canonicalize.canonical_dedupe_key import (
    compute_canonical_dedupe_key_from_source,
)
from src.canonicalize.matching import (
    CONFIDENCE_THRESHOLD,
    compute_fingerprint,
    confidence_score,
)
from src.canonicalize.reviews_supabase import (
    Candidate,
    ignore_open_reviews_for_source_row,
    mark_source_needs_review,
    write_ambiguous_match_review,
    write_constraint_violation_review,
)
from src.canonicalize.confidence import compute_confidence_score as compute_quality_score
from src.canonicalize.scoring import compute_relevance_score
from src.canonicalize.tagging import (
    infer_audience_tags,
    infer_topic_tags,
    pg_array_literal,
)
from src.db.canonical_field_history import (
    FieldChange,
    diff_happening_fields,
    log_field_changes,
)
from src.db.confidence_telemetry import ConfidenceTelemetry
from src.db.merge_run_stats import (
    MergeRunCounters,
    create_merge_run,
    finish_merge_run,
)

# ---------------------------------------------------------------------------
# Constants & helpers
# ---------------------------------------------------------------------------

NEAR_TIE_DELTA = 0.03  # prevent wrong auto-merges

# Editorial fields that the pipeline must NEVER overwrite.
# These are set by admins and take absolute precedence.
EDITORIAL_PROTECTED_FIELDS = frozenset({
    "editorial_priority",
    "visibility_override",
    "override_reason",
    "override_set_by",
    "override_set_at",
    "override_expires_at",
})

# If multiple candidates hit perfect confidence, force review (avoid duplicates / wrong merges)
PERFECT_CONFIDENCE = 1.0
PERFECT_TIE_EPS = 1e-9  # float safety

# Pipeline statuses for source_happenings (Phase 1+)
STATUS_QUEUED = "queued"
STATUS_NEEDS_REVIEW = "needs_review"
STATUS_PROCESSED = "processed"
STATUS_PROCESSING = "processing"
STATUS_IGNORED = "ignored"

# Only v1 dedupe_key rows are processable (Phase 3 contract).
# Legacy rows (URL-based keys) are permanently quarantined.
DEDUPE_KEY_PREFIX = "v1|"

def execute_with_retry(rb, *, tries: int = 6, base_sleep: float = 0.5):
    """
    Supabase/PostgREST calls can occasionally drop HTTP/2 connections under load.
    Wrap .execute() with retry + exponential backoff.
    """
    last = None
    for attempt in range(tries):
        try:
            return rb.execute()
        except (
            httpx.RemoteProtocolError,
            httpx.ReadTimeout,
            httpx.ConnectError,
            httpx.WriteError,
        ) as e:
            last = e
            sleep = base_sleep * (2 ** attempt) + random.random() * 0.25
            print(
                f"[merge_loop] transient http error: {type(e).__name__} "
                f"attempt={attempt+1}/{tries} sleep={sleep:.2f}s"
            )
            time.sleep(sleep)
    raise last  # type: ignore[misc]


def _is_unique_violation(err: Exception) -> bool:
    """Best-effort detection for Postgres 23505 unique_violation."""
    s = repr(err).lower()
    return "23505" in s or "duplicate key value violates unique constraint" in s


def _quality_score_from_source_row(
    source_row: Mapping[str, Any],
    happening_description: str | None = None,
) -> int:
    """Compute data-quality confidence score from source_happenings fields."""
    description = happening_description or source_row.get("description_raw")
    return compute_quality_score(
        source_tier=source_row.get("source_tier"),
        date_precision=source_row.get("date_precision"),
        image_url=source_row.get("image_url"),
        description=description,
        canonical_url=source_row.get("item_url"),
        timezone=source_row.get("timezone"),
        extraction_method=source_row.get("extraction_method"),
    )


def source_priority_from_row(source_row: Mapping[str, Any]) -> int:
    """
    Deterministic source precedence.
    Tier A > B > C. Unknown -> 0
    """
    tier = (source_row.get("source_tier") or "").upper()
    return {"A": 300, "B": 200, "C": 100}.get(tier, 0)


def _pick_best_occurrence_for_offering(
    occurrences: list[dict[str, Any]],
    *,
    source_start_at: Any,
) -> dict[str, Any] | None:
    """
    Deterministic selection of one occurrence per offering to enrich tie-breaking.
    Rules:
      - If source_start_at exists, prefer exact match on start_at
      - Else prefer an occurrence that has a venue_id
      - Else fall back to the first occurrence (stable via sort key)
    """
    if not occurrences:
        return None

    # Stable sort: prefer non-null start_at, then non-null venue_id, then id
    def sort_key(o: dict[str, Any]) -> tuple[int, int, str]:
        return (
            0 if o.get("start_at") is not None else 1,
            0 if o.get("venue_id") is not None else 1,
            str(o.get("id") or ""),
        )

    occs = sorted(occurrences, key=sort_key)

    if source_start_at:
        for o in occs:
            if o.get("start_at") == source_start_at:
                return o

    for o in occs:
        if o.get("venue_id"):
            return o

    return occs[0]


def _cv_fingerprint(*parts: str) -> str:
    """Deterministic fingerprint for constraint violation reviews."""
    seed = "|".join(parts)
    return "cv|" + hashlib.sha256(seed.encode()).hexdigest()[:16]


def _compute_offering_nk_key(
    happening_id: str,
    offering_type: str,
    timezone_str: str | None,
    start_date: str | None,
    end_date: str | None,
) -> str:
    """
    Compute the offering natural key, matching the SQL function
    public.compute_offering_nk_key exactly.
    """
    tz = timezone_str or "Europe/Zurich"
    sd = start_date if start_date is not None else "_"
    ed = end_date if end_date is not None else "_"
    return f"{happening_id}|{offering_type}|{tz}|{sd}|{ed}"


# ---------------------------------------------------------------------------
# Match decision
# ---------------------------------------------------------------------------

@dataclass(frozen=True)
class MatchDecision:
    kind: str  # "merge" | "create" | "review"
    best_happening_id: str | None = None
    candidates: list[Candidate] | None = None
    top_confidence: float | None = None


# ---------------------------------------------------------------------------
# Fetchers
# ---------------------------------------------------------------------------

def fetch_queued_source_happenings(
    supabase: Client,
    limit: int = 200,
    include_needs_review: bool = False,
    *,
    exclude_ids: set[str] | None = None,
) -> list[dict[str, Any]]:
    """
    Select processable source_happenings.

    HARD GUARDS (Phase 3):
      1. dedupe_key LIKE 'v1|%'  — only content-based v1 rows
      2. status IN (queued [, needs_review])

    Legacy rows (URL-based keys) are permanently excluded regardless of status.

    When exclude_ids is provided (dry-mode seen-set), fetches extra rows and
    filters client-side, so we can page through without DB writes.
    """
    statuses = [STATUS_QUEUED]
    if include_needs_review:
        statuses.append(STATUS_NEEDS_REVIEW)

    # In dry mode we may need to over-fetch to fill a batch after excluding
    # already-seen ids. Fetch up to 5x the limit to compensate.
    fetch_limit = limit if not exclude_ids else min(limit * 5, 1000)

    resp = execute_with_retry(
        supabase.table("source_happenings")
        .select("*")
        .like("dedupe_key", "v1|%")
        .in_("status", statuses)
        .order("created_at", desc=False)
        .limit(fetch_limit)
    )
    rows = resp.data or []

    if exclude_ids:
        rows = [r for r in rows if str(r.get("id", "")) not in exclude_ids]

    return rows[:limit]


def claim_source_happenings(
    supabase: Client,
    rows: Sequence[Mapping[str, Any]],
    *,
    dry_run: bool,
) -> None:
    """
    Mark fetched rows as STATUS_PROCESSING so we never refetch the same batch forever.
    In DRY RUN, do nothing.
    """
    if dry_run:
        return

    ids = [str(r["id"]) for r in rows if r.get("id") is not None]
    if not ids:
        return

    execute_with_retry(
        supabase.table("source_happenings")
        .update(
            {
                "status": STATUS_PROCESSING,
                "updated_at": datetime.now(timezone.utc).isoformat(),
            }
        )
        .in_("id", ids)
    )


def lookup_happening_by_dedupe_key(
    supabase: Client,
    source_row: Mapping[str, Any],
) -> str | None:
    """
    Dedupe-key fast path: check if a previously-processed source_happening
    with the same (source_id, dedupe_key) is already linked to a canonical
    happening via happening_sources.

    This avoids redundant fuzzy scoring on re-runs of the same source.

    Returns: happening_id if found, None otherwise.

    Guarantees:
      - Read-only (no writes).
      - Only matches within the same source (source_id scoped).
      - Only matches processed rows (status='processed').
      - Returns None if the linked happening is archived.
      - Does not change any decision logic — just a performance shortcut
        that produces the same merge result as fuzzy scoring would.
      - Any error gracefully falls back to None (fuzzy path takes over).
    """
    try:
        dedupe_key = source_row.get("dedupe_key")
        source_name = source_row.get("source_id")
        row_id = str(source_row.get("id") or "")

        if not dedupe_key or not source_name:
            return None

        # Find a sibling: another source_happening row with the same
        # (source_id, dedupe_key) that is already processed.
        siblings = (
            execute_with_retry(
                supabase.table("source_happenings")
                .select("id")
                .eq("source_id", source_name)
                .eq("dedupe_key", dedupe_key)
                .eq("status", STATUS_PROCESSED)
                .neq("id", row_id)
                .limit(1)
            ).data
            or []
        )

        if not siblings or not isinstance(siblings, list):
            return None

        sibling_id = siblings[0].get("id")
        if not sibling_id:
            return None

        # Look up the happening linked to the sibling via happening_sources.
        links = (
            execute_with_retry(
                supabase.table("happening_sources")
                .select("happening_id")
                .eq("source_happening_id", str(sibling_id))
                .limit(1)
            ).data
            or []
        )

        if not links or not isinstance(links, list):
            return None

        happening_id = links[0].get("happening_id")
        if not happening_id:
            return None

        # Verify the happening is not archived.
        happenings = (
            execute_with_retry(
                supabase.table("happening")
                .select("id,visibility_status")
                .eq("id", str(happening_id))
                .limit(1)
            ).data
            or []
        )

        if not happenings or not isinstance(happenings, list):
            return None

        if happenings[0].get("visibility_status") == "archived":
            return None

        return str(happening_id)

    except Exception:
        # Fast path is best-effort. Any failure falls back to fuzzy scoring.
        return None


def fetch_candidate_bundles(
    supabase: Client,
    source_row: Mapping[str, Any],
) -> list[dict[str, Any]]:
    """
    Returns candidate bundles:
      {
        "happening": {...},
        "offering": {...}
      }

    Narrowing rule (v1):
    - offering.start_date <= source.start_date_local <= offering.end_date
    - archived happenings are excluded
    - no inference, no guessing
    """
    start_date = source_row.get("start_date_local")
    if not start_date:
        return []

    offerings = (
        execute_with_retry(
            supabase.table("offering")
            .select("*, happening(*)")
            .lte("start_date", start_date)
            .gte("end_date", start_date)
            .limit(200)
        ).data
        or []
    )

    # -------------------------------
    # Enrichment: occurrence + venue
    # -------------------------------
    offering_ids = [o["id"] for o in offerings if o.get("id") is not None]
    occ_by_offering: dict[str, list[dict[str, Any]]] = {}

    if offering_ids:
        occ_rows = (
            execute_with_retry(
                supabase.table("occurrence")
                .select("id,offering_id,venue_id,start_at,end_at,status")
                .in_("offering_id", offering_ids)
                .limit(2000)
            ).data
            or []
        )

        for occ in occ_rows:
            oid = occ.get("offering_id")
            if not oid:
                continue
            occ_by_offering.setdefault(str(oid), []).append(occ)

    venue_name_by_id: dict[str, str] = {}
    venue_ids = {
        str(occ.get("venue_id"))
        for occs in occ_by_offering.values()
        for occ in occs
        if occ.get("venue_id") is not None
    }

    if venue_ids:
        venue_rows = (
            execute_with_retry(
                supabase.table("venue")
                .select("id,name")
                .in_("id", list(venue_ids))
                .limit(2000)
            ).data
            or []
        )
        venue_name_by_id = {
            str(v["id"]): (v.get("name") or "")
            for v in venue_rows
            if v.get("id") is not None
        }

    # -------------------------------
    # Build bundles (with enrichment)
    # -------------------------------
    bundles: list[dict[str, Any]] = []
    source_start_at = source_row.get("start_at")

    for offering in offerings:
        happening = offering.get("happening")
        if not happening:
            continue

        if happening.get("visibility_status") == "archived":
            continue

        occs = occ_by_offering.get(str(offering.get("id")), [])
        best_occ = _pick_best_occurrence_for_offering(occs, source_start_at=source_start_at)

        if best_occ:
            offering["__occ_start_at"] = best_occ.get("start_at")
            offering["__occ_end_at"] = best_occ.get("end_at")
            offering["__occ_status"] = best_occ.get("status")
            offering["__venue_id"] = best_occ.get("venue_id")

            venue_id = best_occ.get("venue_id")
            if venue_id is not None:
                venue_name = venue_name_by_id.get(str(venue_id)) or ""
                if venue_name:
                    happening["__venue_name"] = venue_name

        bundles.append({"happening": happening, "offering": offering})

    return bundles


# ---------------------------------------------------------------------------
# Decision logic
# ---------------------------------------------------------------------------

def decide_match(
    source_row: Mapping[str, Any],
    candidate_bundles: Sequence[Mapping[str, Any]],
) -> MatchDecision:
    best_by_happening: dict[str, float] = {}

    for bundle in candidate_bundles:
        happening = bundle["happening"]
        offering = bundle["offering"]

        hid = str(happening["id"])
        score = float(confidence_score(happening, offering, source_row))

        prev = best_by_happening.get(hid)
        if prev is None or score > prev:
            best_by_happening[hid] = score

    scored: list[Candidate] = [
        Candidate(happening_id=hid, confidence=conf)
        for hid, conf in best_by_happening.items()
    ]
    scored.sort(key=lambda c: c.confidence, reverse=True)

    if not scored:
        return MatchDecision(kind="create")

    top = float(scored[0].confidence)
    second = float(scored[1].confidence) if len(scored) > 1 else None

    if top < CONFIDENCE_THRESHOLD:
        # Low confidence = no match. Create a new canonical happening.
        # Only near-ties above threshold are truly "ambiguous".
        return MatchDecision(kind="create", top_confidence=top)

    perfect = [
        c
        for c in scored
        if abs(float(c.confidence) - PERFECT_CONFIDENCE) <= PERFECT_TIE_EPS
    ]
    if len(perfect) >= 2:
        return MatchDecision(kind="review", candidates=scored[:10], top_confidence=top)

    if second is not None and (top - second) < NEAR_TIE_DELTA:
        return MatchDecision(kind="review", candidates=scored[:10], top_confidence=top)

    return MatchDecision(kind="merge", best_happening_id=scored[0].happening_id, top_confidence=top)


# ---------------------------------------------------------------------------
# Offering: upsert by natural key (offering_nk_key)
# ---------------------------------------------------------------------------

def _get_or_create_offering(
    *,
    supabase: Client,
    happening_id: str,
    offering_type: str,
    start_date: str | None,
    end_date: str | None,
    timezone_str: str | None,
    run_id: str,
    source_happening_id: str,
    source_id: str,
    counts: dict[str, int],
) -> str | None:
    """
    Idempotent get-or-create for an offering by its natural key:
      (happening_id, offering_type, timezone, start_date, end_date)

    Uses offering_nk_key column with UNIQUE INDEX for true upsert.

    Strategy:
      1. Compute offering_nk_key deterministically.
      2. Upsert: INSERT ... ON CONFLICT (offering_nk_key) DO UPDATE SET updated_at=now().
      3. On any other error, fall back to SELECT.
      4. If still unresolvable, create a constraint_violation review and return None.

    Returns offering_id on success, None on unresolvable conflict.
    """
    tz = timezone_str or "Europe/Zurich"
    nk_key = _compute_offering_nk_key(happening_id, offering_type, tz, start_date, end_date)

    offering_payload: dict[str, Any] = {
        "happening_id": happening_id,
        "offering_type": offering_type,
        "start_date": start_date,
        "end_date": end_date or start_date,
        "timezone": tz,
        "offering_nk_key": nk_key,
    }
    # Filter None values except for fields that should remain
    offering_payload = {k: v for k, v in offering_payload.items() if v is not None}

    try:
        resp = execute_with_retry(
            supabase.table("offering")
            .upsert(offering_payload, on_conflict="offering_nk_key")
        )
        row = resp.data[0]
        if row.get("id") != row.get("id"):  # impossible, but defensive
            pass
        # Detect if this was a reuse (existing row) vs new insert
        # We can't easily distinguish, but the counter helps observability
        counts["offering_nk_reused"] = counts.get("offering_nk_reused", 0) + 1
        return str(row["id"])
    except (APIError, Exception) as e:
        if not _is_unique_violation(e):
            raise

    # Race condition fallback: SELECT by nk_key
    try:
        resp = execute_with_retry(
            supabase.table("offering")
            .select("id")
            .eq("offering_nk_key", nk_key)
            .limit(1)
        )
        rows = resp.data or []
        if rows:
            counts["offering_nk_reused"] = counts.get("offering_nk_reused", 0) + 1
            return str(rows[0]["id"])
    except Exception:
        pass

    # Unresolvable — create review
    fp = _cv_fingerprint("offering", happening_id, offering_type or "",
                         tz, start_date or "", end_date or "")
    write_constraint_violation_review(
        supabase=supabase,
        run_id=run_id,
        source_happening_id=source_happening_id,
        source_id=source_id,
        fingerprint=fp,
        constraint_name="offering_nk_key_unique",
        details={
            "happening_id": happening_id,
            "offering_type": offering_type,
            "start_date": start_date,
            "end_date": end_date,
            "timezone": tz,
            "offering_nk_key": nk_key,
        },
    )
    counts["reviews_created"] = counts.get("reviews_created", 0) + 1
    return None


# ---------------------------------------------------------------------------
# Occurrence: upsert by (happening_id, start_at, status) unique index
# ---------------------------------------------------------------------------

def _upsert_occurrence(
    *,
    supabase: Client,
    offering_id: str,
    happening_id: str,
    start_at: str,
    end_at: str | None,
    counts: dict[str, int],
) -> None:
    """
    Idempotent upsert for an occurrence using the non-partial unique index:
      occurrence_happening_start_status_uq (happening_id, start_at, status)

    Strategy:
      1. Try UPSERT with happening_id and status='scheduled'.
         The DB trigger trg_sync_occurrence_happening_id will also set
         happening_id from offering, but we supply it explicitly for the
         ON CONFLICT clause.
      2. ON CONFLICT (happening_id, start_at, status) DO UPDATE sets
         end_at if provided (idempotent enrichment).

    Caller must ensure start_at is not None (strict contract).
    """
    occurrence_payload: dict[str, Any] = {
        "offering_id": offering_id,
        "happening_id": happening_id,
        "start_at": start_at,
        "status": "scheduled",
    }
    if end_at is not None:
        occurrence_payload["end_at"] = end_at

    try:
        execute_with_retry(
            supabase.table("occurrence")
            .upsert(
                occurrence_payload,
                on_conflict="happening_id,start_at,status",
            )
        )
        return
    except (APIError, Exception) as e:
        if not _is_unique_violation(e):
            raise

    # Fallback: the upsert hit a conflict we couldn't resolve via on_conflict.
    # This means the row exists — count it as reused.
    counts["occurrence_conflict_reused"] = counts.get("occurrence_conflict_reused", 0) + 1


# ---------------------------------------------------------------------------
# Create canonical chain
# ---------------------------------------------------------------------------

def create_happening_schedule_occurrence(
    *,
    supabase: Client,
    source_row: Mapping[str, Any],
    run_id: str = "",
    counts: dict[str, int] | None = None,
) -> tuple[str, bool]:
    """
    Create or upsert:
      1) Happening (identity) — deduplicated via canonical_dedupe_key
      2) Offering (schedule) — upsert by offering_nk_key
      3) Occurrence (instance) — upsert by (happening_id, start_at, status)

    If canonical_dedupe_key can be computed:
      → upsert on canonical_dedupe_key; update only pipeline-safe fields
        (never editorial_priority, visibility_override, visibility_status, etc.)
    If canonical_dedupe_key is None/empty:
      → plain insert + the row will need manual review

    Returns: (happening_id, fully_resolved)
      fully_resolved=True  → all steps succeeded, caller should mark processed.
      fully_resolved=False → offering/occurrence had unresolvable constraint
                              violation; review was logged; caller should mark
                              needs_review instead of processed.
    """
    if counts is None:
        counts = {}

    audience_tags = infer_audience_tags(
        source_row.get("title_raw"), source_row.get("description_raw"),
    )
    topic_tags = infer_topic_tags(
        source_row.get("title_raw"), source_row.get("description_raw"),
    )

    canonical_key = compute_canonical_dedupe_key_from_source(source_row)

    happening_payload: dict[str, Any] = {
        "title": source_row.get("title_raw"),
        "description": source_row.get("description_raw"),
    }
    if canonical_key:
        happening_payload["canonical_dedupe_key"] = canonical_key
    if audience_tags:
        happening_payload["audience_tags"] = audience_tags
    if topic_tags:
        happening_payload["topic_tags"] = topic_tags

    score = compute_relevance_score(audience_tags, topic_tags)
    if score != 0:
        happening_payload["relevance_score_global"] = score

    happening_payload["confidence_score"] = _quality_score_from_source_row(source_row)

    if canonical_key:
        # Upsert: if a happening with this canonical_dedupe_key already exists,
        # update only pipeline-safe fields. Never touch editorial or visibility.
        upsert_payload = {
            k: v for k, v in happening_payload.items()
            if k not in EDITORIAL_PROTECTED_FIELDS
            and k != "visibility_status"
        }
        happening = execute_with_retry(
            supabase.table("happening")
            .upsert(upsert_payload, on_conflict="canonical_dedupe_key")
        ).data[0]
    else:
        # No canonical key — plain insert (fallback; should not happen for v1 rows).
        print(f"[merge_loop] WARNING: no canonical_dedupe_key for source row {source_row.get('id')}")
        happening = execute_with_retry(
            supabase.table("happening").insert(happening_payload)
        ).data[0]

    happening_id = happening["id"]

    source_happening_id = str(source_row.get("id") or "")
    source_id_str = str(source_row.get("source_id") or "unknown")

    # --- Offering: upsert by offering_nk_key ---
    start_date = source_row.get("start_date_local")
    end_date = source_row.get("end_date_local") or start_date

    offering_id = _get_or_create_offering(
        supabase=supabase,
        happening_id=happening_id,
        offering_type="one_off",
        start_date=start_date,
        end_date=end_date,
        timezone_str=source_row.get("timezone"),
        run_id=run_id,
        source_happening_id=source_happening_id,
        source_id=source_id_str,
        counts=counts,
    )

    if offering_id is None:
        # Offering could not be created or found — review was logged.
        # Signal to caller: source row should be marked needs_review, not processed.
        return str(happening_id), False

    # --- Occurrence: upsert by (happening_id, start_at, status) ---
    # Only create an occurrence when we have a real start_at timestamp.
    # Date-only items (date_precision='date', start_at=NULL) must NOT
    # produce occurrence rows — the DB enforces NOT NULL on start_at
    # and the time contract forbids inventing midnight placeholders.
    start_at = source_row.get("start_at")
    if start_at is None:
        counts["occurrence_null_start_skipped"] = counts.get("occurrence_null_start_skipped", 0) + 1
    else:
        _upsert_occurrence(
            supabase=supabase,
            offering_id=offering_id,
            happening_id=happening_id,
            start_at=start_at,
            end_at=source_row.get("end_at"),
            counts=counts,
        )

    return str(happening_id), True


# ---------------------------------------------------------------------------
# Provenance
# ---------------------------------------------------------------------------

def update_happening_on_merge(
    *,
    supabase: Client,
    happening_id: str,
    source_row: Mapping[str, Any],
) -> tuple[int, int]:
    """
    Compare tracked fields between current happening and source row.
    If any differ (and source is non-null), update the happening and
    log the old→new transition to canonical_field_history.

    SAFETY: editorial fields (editorial_priority, visibility_override, etc.)
    and visibility_status are NEVER written by the pipeline.

    Also backfills canonical_dedupe_key if the existing row is missing one.

    Returns: (field_updates_count, history_rows_inserted)
    """
    current = execute_with_retry(
        supabase.table("happening").select("*").eq("id", happening_id)
    ).data
    if not current:
        return (0, 0)

    changes = diff_happening_fields(current[0], source_row)
    # Filter out editorial-protected fields (safety net)
    changes = [
        c for c in changes
        if c.field_name not in EDITORIAL_PROTECTED_FIELDS
        and c.field_name != "visibility_status"
    ]

    update_payload = {c.field_name: c.new_value for c in changes}

    # Backfill canonical_dedupe_key if missing on the existing row
    if not current[0].get("canonical_dedupe_key"):
        canonical_key = compute_canonical_dedupe_key_from_source(source_row)
        if canonical_key:
            update_payload["canonical_dedupe_key"] = canonical_key

    if not update_payload:
        return (0, 0)

    execute_with_retry(
        supabase.table("happening").update(update_payload).eq("id", happening_id)
    )

    history_inserts = log_field_changes(
        supabase, happening_id, str(source_row["id"]), changes,
    )

    return (len(changes), history_inserts)


def _recompute_confidence_on_merge(
    *,
    supabase: Client,
    happening_id: str,
    source_row: Mapping[str, Any],
) -> bool:
    """
    Recompute data-quality confidence score after a merge.

    Uses the source_row's metadata (best available at merge time) and
    the happening's description (if present) to compute the score.
    Only writes if the score has changed (idempotent).

    Returns: True if the score was updated, False otherwise.
    """
    current = execute_with_retry(
        supabase.table("happening")
        .select("confidence_score,description")
        .eq("id", happening_id)
        .limit(1)
    ).data
    if not current:
        return False

    current_score = current[0].get("confidence_score", 100)
    happening_desc = current[0].get("description")

    new_score = _quality_score_from_source_row(source_row, happening_desc)

    if new_score == current_score:
        return False

    execute_with_retry(
        supabase.table("happening")
        .update({"confidence_score": new_score})
        .eq("id", happening_id)
    )
    return True


def apply_heuristic_tags(
    *,
    supabase: Client,
    happening_id: str,
    source_row: Mapping[str, Any],
) -> tuple[int, int]:
    """
    Apply heuristic audience/topic tags to a canonical happening,
    but ONLY when the existing tags are empty (admin edits win).

    Never modifies editorial_priority.

    Returns: (field_updates_count, history_rows_inserted)
    """
    current = execute_with_retry(
        supabase.table("happening")
        .select("id,audience_tags,topic_tags,relevance_score_global,title,description")
        .eq("id", happening_id)
        .limit(1)
    ).data

    if not current:
        return (0, 0)

    row = current[0]
    existing_audience = row.get("audience_tags") or []
    existing_topic = row.get("topic_tags") or []

    # Both already populated → nothing to do (admin edits win)
    if existing_audience and existing_topic:
        return (0, 0)

    # Prefer source_row text (fresher), fall back to happening text
    title = source_row.get("title_raw") or row.get("title")
    description = source_row.get("description_raw") or row.get("description")

    update_payload: dict[str, Any] = {}
    changes: list[FieldChange] = []

    if not existing_audience:
        new_audience = infer_audience_tags(title, description)
        if new_audience:
            update_payload["audience_tags"] = new_audience
            changes.append(FieldChange(
                field_name="audience_tags",
                old_value=pg_array_literal([]),
                new_value=pg_array_literal(new_audience),
            ))

    if not existing_topic:
        new_topic = infer_topic_tags(title, description)
        if new_topic:
            update_payload["topic_tags"] = new_topic
            changes.append(FieldChange(
                field_name="topic_tags",
                old_value=pg_array_literal([]),
                new_value=pg_array_literal(new_topic),
            ))

    if not update_payload:
        return (0, 0)

    # Recompute relevance score from final tag state
    final_audience = update_payload.get("audience_tags", existing_audience)
    final_topic = update_payload.get("topic_tags", existing_topic)
    new_score = compute_relevance_score(final_audience, final_topic)
    current_score = row.get("relevance_score_global") or 0
    if new_score != current_score:
        update_payload["relevance_score_global"] = new_score

    execute_with_retry(
        supabase.table("happening").update(update_payload).eq("id", happening_id)
    )

    history_inserts = 0
    if changes:
        history_inserts = log_field_changes(
            supabase, happening_id, str(source_row["id"]), changes,
        )

    return (len(update_payload), history_inserts)


def link_happening_source(
    *,
    supabase: Client,
    happening_id: str,
    source_row: Mapping[str, Any],
    is_primary: bool = False,
) -> None:
    payload = {
        "happening_id": happening_id,
        "source_happening_id": source_row["id"],
        "source_priority": source_priority_from_row(source_row),
        "is_primary": bool(is_primary),
        "merged_at": datetime.now(timezone.utc).isoformat(),
    }

    execute_with_retry(
        supabase.table("happening_sources")
        .upsert(payload, on_conflict="source_happening_id")
    )


def mark_source_processed(
    *,
    supabase: Client,
    source_happening_id: str,
) -> None:
    execute_with_retry(
        supabase.table("source_happenings")
        .update(
            {
                "status": STATUS_PROCESSED,
                "updated_at": datetime.now(timezone.utc).isoformat(),
            }
        )
        .eq("id", source_happening_id)
    )


def mark_source_processing_failed(
    *,
    supabase: Client,
    source_happening_id: str,
    error_message: str,
) -> None:
    """
    If a row was claimed as PROCESSING but something crashes, we want it to
    be visible again. Put it back to NEEDS_REVIEW with an error message.

    Phase 3 guard: only v1| rows can be requeued.
    """
    execute_with_retry(
        supabase.table("source_happenings")
        .update(
            {
                "status": STATUS_NEEDS_REVIEW,
                "error_message": (error_message or "")[:500],
                "updated_at": datetime.now(timezone.utc).isoformat(),
            }
        )
        .eq("id", source_happening_id)
        .like("dedupe_key", "v1|%")
    )


# ---------------------------------------------------------------------------
# Ensure occurrence on merge path
# ---------------------------------------------------------------------------

def _ensure_occurrence_on_merge(
    *,
    supabase: Client,
    happening_id: str,
    source_row: Mapping[str, Any],
    run_id: str,
    counts: dict[str, int],
) -> None:
    """
    When merging a source_row into an existing happening, ensure that the
    occurrence for the source's start_at exists. Previously, the merge path
    did not create occurrences — only the create path did. This was a root
    cause of missing occurrences for merged sources.
    """
    start_at = source_row.get("start_at")
    if start_at is None:
        counts["occurrence_null_start_skipped"] = counts.get("occurrence_null_start_skipped", 0) + 1
        return

    source_happening_id = str(source_row.get("id") or "")
    source_id_str = str(source_row.get("source_id") or "unknown")

    start_date = source_row.get("start_date_local")
    end_date = source_row.get("end_date_local") or start_date

    offering_id = _get_or_create_offering(
        supabase=supabase,
        happening_id=happening_id,
        offering_type="one_off",
        start_date=start_date,
        end_date=end_date,
        timezone_str=source_row.get("timezone"),
        run_id=run_id,
        source_happening_id=source_happening_id,
        source_id=source_id_str,
        counts=counts,
    )

    if offering_id is None:
        return

    _upsert_occurrence(
        supabase=supabase,
        offering_id=offering_id,
        happening_id=happening_id,
        start_at=start_at,
        end_at=source_row.get("end_at"),
        counts=counts,
    )


# ---------------------------------------------------------------------------
# Main loop
# ---------------------------------------------------------------------------

def run_merge_loop(
    *,
    supabase: Client,
    batch_size: int = 200,
    dry_run: bool = True,
    code_version: str | None = None,
    environment: str | None = None,
    include_needs_review: bool = False,
    persist_run_stats: bool = True,
    max_batches: int | None = None,
    max_rows: int | None = None,
) -> dict[str, int]:
    run_id = str(uuid4())

    counts: dict[str, int] = {
        "queued": 0,   # total fetched across loops
        "merged": 0,
        "created": 0,
        "review": 0,
        "skipped": 0,
        "claimed": 0,
        "errors": 0,
        "canonical_updates": 0,
        "history_rows": 0,
        "dedupe_fast_path": 0,
        # Phase 10: collision-proofing counters
        "offering_nk_reused": 0,
        "occurrence_conflict_reused": 0,
        "occurrence_null_start_skipped": 0,
        "reviews_created": 0,
    }

    # Per-source breakdown for observability (Phase 7)
    source_breakdown: dict[str, dict[str, int]] = defaultdict(
        lambda: {"created": 0, "merged": 0, "review": 0, "field_updates": 0, "errors": 0}
    )
    stage_timings: dict[str, int] = {}
    t_start = time.monotonic()

    # Confidence telemetry (Phase 9)
    telemetry = ConfidenceTelemetry()

    # Dry-mode seen set: track fetched ids to avoid infinite re-fetching.
    # In live mode the claim mechanism (status → processing) prevents this.
    dry_seen_ids: set[str] = set() if dry_run else set()  # always init, only used in dry

    # Resolve effective max_batches: dry defaults to 10 if caller didn't specify.
    effective_max_batches = max_batches
    if effective_max_batches is None and dry_run:
        effective_max_batches = 10
    batch_count = 0

    # --- Run stats: create row at start ---
    stats_run_id: str | None = None
    if persist_run_stats:
        try:
            stats_run_id = create_merge_run(supabase)
        except Exception as e:
            print(f"[merge_loop] WARNING: failed to create merge_run_stats row: {e!r}")

    try:
        while True:
            # --- Guard: max_batches ---
            if effective_max_batches is not None and batch_count >= effective_max_batches:
                print(f"[merge_loop] reached max_batches={effective_max_batches}, stopping.")
                break

            # --- Guard: max_rows ---
            if max_rows is not None and counts["queued"] >= max_rows:
                print(f"[merge_loop] reached max_rows={max_rows} (processed {counts['queued']}), stopping.")
                break

            # In dry mode, pass seen ids so we don't re-fetch the same rows.
            rows = fetch_queued_source_happenings(
                supabase,
                limit=batch_size,
                include_needs_review=include_needs_review,
                exclude_ids=dry_seen_ids if dry_run else None,
            )

            print(f"[merge_loop] fetched_batch={len(rows)} include_needs_review={include_needs_review} batch={batch_count+1}")

            if not rows:
                break

            # --- Safety guard: zero new ids means we're stuck ---
            new_ids = {str(r.get("id", "")) for r in rows}
            if dry_run and not (new_ids - dry_seen_ids):
                print("[merge_loop] WARNING: fetched batch contains 0 new ids, breaking to avoid infinite loop.")
                break

            # Track seen ids for dry mode pagination
            if dry_run:
                dry_seen_ids.update(new_ids)

            # Optionally trim batch if max_rows would be exceeded
            if max_rows is not None:
                remaining = max_rows - counts["queued"]
                if remaining <= 0:
                    break
                rows = rows[:remaining]

            counts["queued"] += len(rows)
            batch_count += 1

            # Claim rows to avoid infinite refetch loops
            try:
                claim_source_happenings(supabase, rows, dry_run=dry_run)
                counts["claimed"] += len(rows) if not dry_run else 0
            except Exception as e:
                # If claiming fails, bail out to avoid spinning forever.
                print(f"[merge_loop] ERROR claiming batch: {repr(e)}")
                counts["errors"] += 1
                break

            for source_row in rows:
                source_id = str(source_row.get("id") or "")
                src_name = str(source_row.get("source_id") or "unknown")
                try:
                    # -------------------------------------------------------
                    # Dedupe-key fast path: if a sibling source_happening
                    # with the same (source_id, dedupe_key) was already
                    # processed and linked, skip fuzzy scoring entirely.
                    # This is a performance optimization only — the merge
                    # result is identical to what fuzzy scoring would produce.
                    # -------------------------------------------------------
                    fast_happening_id = lookup_happening_by_dedupe_key(
                        supabase, source_row,
                    )
                    if fast_happening_id is not None:
                        counts["merged"] += 1
                        counts["dedupe_fast_path"] += 1
                        source_breakdown[src_name]["merged"] += 1
                        if not dry_run:
                            link_happening_source(
                                supabase=supabase,
                                happening_id=fast_happening_id,
                                source_row=source_row,
                            )
                            field_updates, history_inserts = update_happening_on_merge(
                                supabase=supabase,
                                happening_id=fast_happening_id,
                                source_row=source_row,
                            )
                            tag_updates, tag_history = apply_heuristic_tags(
                                supabase=supabase,
                                happening_id=fast_happening_id,
                                source_row=source_row,
                            )
                            counts["canonical_updates"] += field_updates + tag_updates
                            counts["history_rows"] += history_inserts + tag_history
                            source_breakdown[src_name]["field_updates"] += field_updates + tag_updates
                            if _recompute_confidence_on_merge(
                                supabase=supabase,
                                happening_id=fast_happening_id,
                                source_row=source_row,
                            ):
                                counts["canonical_updates"] += 1
                            # Ensure occurrence exists for this merge
                            _ensure_occurrence_on_merge(
                                supabase=supabase,
                                happening_id=fast_happening_id,
                                source_row=source_row,
                                run_id=run_id,
                                counts=counts,
                            )
                            mark_source_processed(
                                supabase=supabase,
                                source_happening_id=source_id,
                            )
                            ignore_open_reviews_for_source_row(
                                supabase=supabase,
                                source_happening_id=source_id,
                            )
                        continue

                    fingerprint = compute_fingerprint(source_row)
                    candidate_bundles = fetch_candidate_bundles(supabase, source_row)
                    decision = decide_match(source_row, candidate_bundles)

                    # Phase 9: record confidence telemetry (passive, no decision changes)
                    if decision.top_confidence is not None:
                        telemetry.add(src_name, decision.top_confidence)

                    if decision.kind == "review":
                        counts["review"] += 1
                        source_breakdown[src_name]["review"] += 1
                        if not dry_run:
                            mark_source_needs_review(
                                supabase=supabase,
                                source_happening_id=source_id,
                            )
                            write_ambiguous_match_review(
                                supabase=supabase,
                                run_id=run_id,
                                source_row=source_row,
                                fingerprint=fingerprint,
                                candidates=decision.candidates or [],
                                threshold=CONFIDENCE_THRESHOLD,
                                code_version=code_version,
                                environment=environment,
                            )
                        continue

                    if decision.kind == "create":
                        counts["created"] += 1
                        source_breakdown[src_name]["created"] += 1
                        if not dry_run:
                            happening_id, fully_resolved = create_happening_schedule_occurrence(
                                supabase=supabase,
                                source_row=source_row,
                                run_id=run_id,
                                counts=counts,
                            )
                            link_happening_source(
                                supabase=supabase,
                                happening_id=happening_id,
                                source_row=source_row,
                                is_primary=True,
                            )
                            if fully_resolved:
                                mark_source_processed(
                                    supabase=supabase,
                                    source_happening_id=source_id,
                                )
                                ignore_open_reviews_for_source_row(
                                    supabase=supabase,
                                    source_happening_id=source_id,
                                )
                            else:
                                # Offering/occurrence had unresolvable constraint
                                # violation — review was already logged; mark
                                # source row needs_review so it stays visible.
                                mark_source_needs_review(
                                    supabase=supabase,
                                    source_happening_id=source_id,
                                )
                        continue

                    if decision.kind == "merge":
                        counts["merged"] += 1
                        source_breakdown[src_name]["merged"] += 1
                        if not dry_run:
                            if not decision.best_happening_id:
                                mark_source_needs_review(
                                    supabase=supabase,
                                    source_happening_id=source_id,
                                )
                                write_ambiguous_match_review(
                                    supabase=supabase,
                                    run_id=run_id,
                                    source_row=source_row,
                                    fingerprint=fingerprint,
                                    candidates=decision.candidates or [],
                                    threshold=CONFIDENCE_THRESHOLD,
                                    code_version=code_version,
                                    environment=environment,
                                )
                                continue

                            link_happening_source(
                                supabase=supabase,
                                happening_id=decision.best_happening_id,
                                source_row=source_row,
                            )
                            field_updates, history_inserts = update_happening_on_merge(
                                supabase=supabase,
                                happening_id=decision.best_happening_id,
                                source_row=source_row,
                            )
                            tag_updates, tag_history = apply_heuristic_tags(
                                supabase=supabase,
                                happening_id=decision.best_happening_id,
                                source_row=source_row,
                            )
                            counts["canonical_updates"] += field_updates + tag_updates
                            counts["history_rows"] += history_inserts + tag_history
                            source_breakdown[src_name]["field_updates"] += field_updates + tag_updates
                            if _recompute_confidence_on_merge(
                                supabase=supabase,
                                happening_id=decision.best_happening_id,
                                source_row=source_row,
                            ):
                                counts["canonical_updates"] += 1
                            # Ensure occurrence exists for this merge
                            _ensure_occurrence_on_merge(
                                supabase=supabase,
                                happening_id=decision.best_happening_id,
                                source_row=source_row,
                                run_id=run_id,
                                counts=counts,
                            )
                            mark_source_processed(
                                supabase=supabase,
                                source_happening_id=source_id,
                            )
                            ignore_open_reviews_for_source_row(
                                supabase=supabase,
                                source_happening_id=source_id,
                            )
                        continue

                    counts["skipped"] += 1

                except Exception as e:
                    counts["errors"] += 1
                    source_breakdown[src_name]["errors"] += 1
                    print(f"[merge_loop] ERROR row id={source_id}: {repr(e)}")
                    if not dry_run and source_id:
                        # Put row back to needs_review so it remains visible.
                        try:
                            mark_source_processing_failed(
                                supabase=supabase,
                                source_happening_id=source_id,
                                error_message=repr(e),
                            )
                        except Exception as e2:
                            print(f"[merge_loop] ERROR while marking row needs_review: {repr(e2)}")
                    continue

        stage_timings["total_processing_ms"] = int(
            (time.monotonic() - t_start) * 1000
        )

    finally:
        # --- Run stats: update row at end (always, even on error) ---
        if persist_run_stats and stats_run_id:
            try:
                finish_merge_run(
                    supabase,
                    stats_run_id,
                    MergeRunCounters(
                        source_rows_processed=counts["queued"],
                        canonical_created=counts["created"],
                        canonical_merged=counts["merged"],
                        canonical_review=counts["review"],
                        errors=counts["errors"],
                        canonical_updates_count=counts["canonical_updates"],
                        history_rows_created=counts["history_rows"],
                        offering_nk_reused=counts["offering_nk_reused"],
                        occurrence_conflict_reused=counts["occurrence_conflict_reused"],
                        occurrence_null_start_skipped=counts["occurrence_null_start_skipped"],
                        reviews_created=counts["reviews_created"],
                    ),
                    source_breakdown=dict(source_breakdown) if source_breakdown else None,
                    stage_timings_ms=stage_timings if stage_timings else None,
                    confidence_min=telemetry.global_stats.min,
                    confidence_avg=telemetry.global_stats.avg,
                    confidence_max=telemetry.global_stats.max,
                    confidence_histogram=telemetry.global_hist,
                    source_confidence=telemetry.as_source_json(),
                )
            except Exception as e:
                print(f"[merge_loop] WARNING: failed to finish merge_run_stats row: {e!r}")

    return counts


# ---------------------------------------------------------------------------
# CLI helpers (.env loading + Supabase client)
# ---------------------------------------------------------------------------

def _load_dotenv_if_present() -> None:
    """
    Minimal .env loader (no external dependency).
    Supports lines like KEY=value. Ignores comments and blank lines.
    Does NOT support shell expansions.
    Loads .env then .env.local (and never overwrites already-exported env vars).
    """
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

    missing: list[str] = []
    if not url:
        missing.append("SUPABASE_URL (or NEXT_PUBLIC_SUPABASE_URL)")
    if not key:
        missing.append(
            "SUPABASE_SERVICE_ROLE_KEY (preferred) or SUPABASE_ANON_KEY / NEXT_PUBLIC_SUPABASE_ANON_KEY"
        )

    if missing:
        raise RuntimeError(
            "Missing Supabase env vars: "
            + ", ".join(missing)
            + ".\n"
            + "Fix: ensure .env or .env.local contains these keys, or export them in your shell.\n"
            + "Example (.env.local):\n"
            + "  SUPABASE_URL=https://xxxx.supabase.co\n"
            + "  SUPABASE_SERVICE_ROLE_KEY=xxxx\n"
        )

    return create_client(url, key)


def main() -> None:
    parser = argparse.ArgumentParser(description="Caloo canonicalization merge loop")
    parser.add_argument(
        "--mode",
        choices=["dry", "live"],
        default="dry",
        help="dry: no DB writes, live: perform writes",
    )
    parser.add_argument("--batch-size", type=int, default=200)
    parser.add_argument(
        "--include-needs-review",
        action="store_true",
        help="Also process needs_review rows (off by default).",
    )
    parser.add_argument("--code-version", type=str, default=None)
    parser.add_argument("--environment", type=str, default=None)
    parser.add_argument(
        "--max-batches",
        type=int,
        default=None,
        help="Max number of batches to process (default: 10 for dry, unlimited for live).",
    )
    parser.add_argument(
        "--max-rows",
        type=int,
        default=None,
        help="Max total source rows to process across all batches.",
    )

    args = parser.parse_args()

    supabase = _get_supabase_client()
    dry_run = args.mode != "live"

    counts = run_merge_loop(
        supabase=supabase,
        batch_size=args.batch_size,
        dry_run=dry_run,
        code_version=args.code_version,
        environment=args.environment,
        include_needs_review=bool(args.include_needs_review),
        max_batches=args.max_batches,
        max_rows=args.max_rows,
    )

    mode = "DRY RUN" if dry_run else "LIVE"
    print(
        f"[merge_loop] mode={mode} batch_size={args.batch_size} "
        f"include_needs_review={args.include_needs_review} "
        f"max_batches={args.max_batches} max_rows={args.max_rows}"
    )
    print(f"[merge_loop] counts={counts}")


if __name__ == "__main__":
    main()
