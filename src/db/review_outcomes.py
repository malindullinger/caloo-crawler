# src/db/review_outcomes.py
"""
Log review outcome decisions to canonical_review_outcomes.
Analytics / audit trail only — does not affect merge behavior.

Phase 9: ready to wire into review resolution flow when it is built.
Currently no resolution code path exists; this module stands alone.

Idempotent via UPSERT on review_id (unique constraint).
"""
from __future__ import annotations

from typing import Any

from supabase import Client


def log_review_outcome(
    supabase: Client,
    *,
    review_id: str,
    source_happening_id: str,
    decision: str,
    happening_id: str | None = None,
    selected_candidate_happening_id: str | None = None,
    confidence_score: float | None = None,
    confidence_breakdown: dict[str, Any] | None = None,
    resolved_by: str | None = None,
) -> None:
    """
    Idempotent upsert of a review outcome row.

    Uses review_id as the conflict key — repeated calls with the same
    review_id update the existing row instead of creating duplicates.
    """
    payload: dict[str, Any] = {
        "review_id": review_id,
        "source_happening_id": source_happening_id,
        "decision": decision,
    }

    # Only include optional fields when provided (avoid nullifying existing data)
    if happening_id is not None:
        payload["happening_id"] = happening_id
    if selected_candidate_happening_id is not None:
        payload["selected_candidate_happening_id"] = selected_candidate_happening_id
    if confidence_score is not None:
        payload["confidence_score"] = confidence_score
    if confidence_breakdown is not None:
        payload["confidence_breakdown"] = confidence_breakdown
    if resolved_by is not None:
        payload["resolved_by"] = resolved_by

    supabase.table("canonical_review_outcomes").upsert(
        payload, on_conflict="review_id",
    ).execute()
