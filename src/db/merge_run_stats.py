# src/db/merge_run_stats.py
"""
Persist one row per merge_loop run in public.merge_run_stats.
Pure observability — must never affect merge behavior.

Phase 7: expanded with canonical_updates_count, history_rows_created,
per-source breakdown (JSONB), and stage timing (JSONB).

Phase 9: expanded with confidence telemetry (min/avg/max, histogram,
per-source confidence).

Phase 10: expanded with collision-proofing counters (offering_nk_reused,
occurrence_conflict_reused, occurrence_null_start_skipped, reviews_created).
"""
from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any

from supabase import Client


def _utc_now() -> datetime:
    return datetime.now(timezone.utc)


@dataclass
class MergeRunCounters:
    source_rows_processed: int = 0
    canonical_created: int = 0
    canonical_merged: int = 0
    canonical_review: int = 0
    errors: int = 0
    canonical_updates_count: int = 0
    history_rows_created: int = 0
    # Phase 10: collision-proofing counters
    offering_nk_reused: int = 0
    occurrence_conflict_reused: int = 0
    occurrence_null_start_skipped: int = 0
    reviews_created: int = 0


def create_merge_run(supabase: Client) -> str:
    """Insert a new merge_run_stats row and return its id."""
    res = (
        supabase.table("merge_run_stats")
        .insert({"started_at": _utc_now().isoformat()})
        .execute()
    )
    return str(res.data[0]["id"])


def finish_merge_run(
    supabase: Client,
    run_id: str,
    counters: MergeRunCounters,
    *,
    source_breakdown: dict[str, dict[str, Any]] | None = None,
    stage_timings_ms: dict[str, int] | None = None,
    confidence_min: float | None = None,
    confidence_avg: float | None = None,
    confidence_max: float | None = None,
    confidence_histogram: dict[str, int] | None = None,
    source_confidence: dict[str, Any] | None = None,
) -> None:
    """Update the merge_run_stats row with final counters, breakdown, and timestamp."""
    payload: dict[str, Any] = {
        "finished_at": _utc_now().isoformat(),
        "source_rows_processed": counters.source_rows_processed,
        "canonical_created": counters.canonical_created,
        "canonical_merged": counters.canonical_merged,
        "canonical_review": counters.canonical_review,
        "errors": counters.errors,
        "canonical_updates_count": counters.canonical_updates_count,
        "history_rows_created": counters.history_rows_created,
        "offering_nk_reused": counters.offering_nk_reused,
        "occurrence_conflict_reused": counters.occurrence_conflict_reused,
        "occurrence_null_start_skipped": counters.occurrence_null_start_skipped,
        "reviews_created": counters.reviews_created,
    }
    if source_breakdown is not None:
        payload["source_breakdown"] = source_breakdown
    if stage_timings_ms is not None:
        payload["stage_timings_ms"] = stage_timings_ms
    if confidence_min is not None:
        payload["confidence_min"] = confidence_min
    if confidence_avg is not None:
        payload["confidence_avg"] = confidence_avg
    if confidence_max is not None:
        payload["confidence_max"] = confidence_max
    if confidence_histogram is not None:
        payload["confidence_histogram"] = confidence_histogram
    if source_confidence is not None:
        payload["source_confidence"] = source_confidence

    supabase.table("merge_run_stats").update(payload).eq("id", run_id).execute()
