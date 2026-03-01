# src/canonicalize/reviews_supabase.py
from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Mapping

from supabase import Client
from postgrest.exceptions import APIError


@dataclass(frozen=True)
class Candidate:
    happening_id: str
    confidence: float


def _extract_postgrest_error(e: APIError) -> dict[str, Any]:
    """
    Normalize PostgREST APIError across versions.
    We try to recover the dict that contains: message, code, details, hint.
    """
    if getattr(e, "args", None) and len(e.args) >= 1 and isinstance(e.args[0], dict):
        return e.args[0]  # common case
    # Some versions store useful text in str(e); keep minimal fallback
    return {"message": str(e)}


def _is_duplicate_open_review(err: dict[str, Any]) -> bool:
    code = err.get("code")
    msg = (err.get("message") or "").lower()
    details = (err.get("details") or "").lower()

    if code in ("23505", 23505):
        return True
    if "duplicate key value" in msg:
        return True
    if "uniq_open_review_per_source_row" in msg or "uniq_open_review_per_source_row" in details:
        return True
    return False


def write_ambiguous_match_review(
    *,
    supabase: Client,
    run_id: str,
    source_row: Mapping[str, Any],
    fingerprint: str,
    candidates: list[Candidate],
    threshold: float,
    code_version: str | None = None,
    environment: str | None = None,
) -> None:
    """
    Idempotent review write that works with a PARTIAL unique constraint like:
      uniq_open_review_per_source_row (source_happening_id, review_type) WHERE status='open'

    Strategy:
      - try INSERT
      - if duplicate (23505 / dup message) -> UPDATE existing open row
    """
    source_happening_id = str(source_row["id"])
    review_type = "ambiguous_match"

    source_id = source_row.get("source_id")
    if not source_id:
        raise RuntimeError(
            f"canonicalization_reviews requires source_id, but source_row[{source_happening_id}] has no source_id"
        )

    cand_payload = [
        {"happening_id": c.happening_id, "confidence": float(c.confidence)}
        for c in (candidates or [])
    ]
    top_conf = float(candidates[0].confidence) if candidates else 0.0

    insert_payload = {
        "run_id": run_id,
        "review_type": review_type,
        "status": "open",
        "source_happening_id": source_happening_id,
        "source_id": str(source_id),
        "fingerprint": fingerprint,
        "candidates": cand_payload,
        "top_confidence": top_conf,
        "threshold": float(threshold),
        "code_version": code_version,
        "environment": environment,
    }
    # Avoid sending nulls for optional columns
    insert_payload = {k: v for k, v in insert_payload.items() if v is not None}

    try:
        supabase.table("canonicalization_reviews").insert(insert_payload).execute()
        return
    except APIError as e:
        err = _extract_postgrest_error(e)
        if not _is_duplicate_open_review(err):
            raise

    update_payload = {
        "run_id": run_id,
        "fingerprint": fingerprint,
        "candidates": cand_payload,
        "top_confidence": top_conf,
        "threshold": float(threshold),
        "code_version": code_version,
        "environment": environment,
        "source_id": str(source_id),
    }
    update_payload = {k: v for k, v in update_payload.items() if v is not None}

    supabase.table("canonicalization_reviews").update(update_payload).eq(
        "source_happening_id", source_happening_id
    ).eq("review_type", review_type).eq("status", "open").execute()


def write_constraint_violation_review(
    *,
    supabase: Client,
    run_id: str,
    source_happening_id: str,
    source_id: str,
    fingerprint: str,
    constraint_name: str,
    details: dict[str, Any],
) -> bool:
    """
    Record a constraint violation as a canonicalization_review.

    Uses the partial unique index on (fingerprint) WHERE status='open'
    for idempotency: if a review with this fingerprint is already open,
    this is a no-op.

    Returns True if a review was created, False if it already existed.
    """
    insert_payload: dict[str, Any] = {
        "run_id": run_id,
        "review_type": "constraint_violation",
        "status": "open",
        "source_happening_id": source_happening_id,
        "source_id": source_id,
        "fingerprint": fingerprint,
        "details": {
            "constraint": constraint_name,
            **details,
        },
    }

    try:
        supabase.table("canonicalization_reviews").insert(insert_payload).execute()
        return True
    except APIError as e:
        err = _extract_postgrest_error(e)
        if _is_duplicate_open_review(err):
            return False
        raise


def mark_source_needs_review(
    *,
    supabase: Client,
    source_happening_id: str,
) -> None:
    # Phase 3 guard: only v1| rows can be set to needs_review
    supabase.table("source_happenings").update(
        {"status": "needs_review"}
    ).eq("id", source_happening_id).like("dedupe_key", "v1|%").execute()


def ignore_open_reviews_for_source_row(
    *,
    supabase: Client,
    source_happening_id: str,
) -> None:
    """
    Keep the review table meaningful:
    if a source_happening is now processed, any lingering open reviews for it
    should no longer be considered actionable.
    Allowed statuses: open, resolved_merge, resolved_create_new, ignored
    """
    supabase.table("canonicalization_reviews").update(
        {"status": "ignored"}
    ).eq("source_happening_id", source_happening_id).eq("status", "open").execute()
