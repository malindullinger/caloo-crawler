# src/canonicalize/reviews.py
from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Mapping, Sequence, Optional
from uuid import UUID

# This assumes you have some db client already.
# Replace `db.execute(...)` with your actual Supabase/Postgres client call.

@dataclass(frozen=True)
class Candidate:
    happening_id: str  # UUID as string is fine for JSON
    confidence: float
    features: dict[str, Any] | None = None


def _candidate_to_json(c: Candidate) -> dict[str, Any]:
    payload: dict[str, Any] = {
        "happening_id": c.happening_id,
        "confidence": float(c.confidence),
    }
    if c.features:
        payload["features"] = c.features
    return payload


def write_ambiguous_match_review(
    *,
    db: Any,
    run_id: str,
    source_row: Mapping[str, Any],
    fingerprint: str | None,
    candidates: Sequence[Candidate],
    threshold: float,
    code_version: str | None = None,
    environment: str | None = None,
) -> None:
    """
    Writes an 'open' review row capturing candidates and confidence scores.
    Keep this function dumb and deterministic: no matching logic here.
    """
    # Required identifiers (adjust keys to your source_happenings schema)
    source_happening_id = source_row["id"]
    source_id = source_row["source_id"]
    external_id = source_row.get("external_id")

    cand_json = [_candidate_to_json(c) for c in candidates]
    top_conf = max((c.confidence for c in candidates), default=None)

    payload = {
        "run_id": run_id,
        "code_version": code_version,
        "environment": environment,
        "source_happening_id": str(source_happening_id),
        "source_id": str(source_id),
        "external_id": external_id,
        "review_type": "ambiguous_match",
        "fingerprint": fingerprint,
        "candidates": cand_json,
        "top_confidence": float(top_conf) if top_conf is not None else None,
        "threshold": float(threshold),
        "status": "open",
    }

    db.execute(
        """
        insert into canonicalization_reviews
          (run_id, code_version, environment, source_happening_id, source_id, external_id,
           review_type, fingerprint, candidates, top_confidence, threshold, status)
        values
          (%(run_id)s, %(code_version)s, %(environment)s, %(source_happening_id)s, %(source_id)s, %(external_id)s,
           %(review_type)s, %(fingerprint)s, %(candidates)s::jsonb, %(top_confidence)s, %(threshold)s, %(status)s)
        on conflict do nothing
        """,
        payload,
    )


def mark_source_needs_review(*, db: Any, source_happening_id: str) -> None:
    """
    Single-purpose state transition. Keep it explicit.
    """
    db.execute(
        """
        update source_happenings
        set status = 'needs_review'
        where id = %(id)s
        """,
        {"id": str(source_happening_id)},
    )
