"""
Phase 9: Review Outcome Logging — Contract Tests

Verifies:
  1. log_review_outcome calls upsert with correct payload
  2. Idempotency key uses review_id (on_conflict)
  3. Optional fields omitted when None
  4. All required fields always present
"""
from __future__ import annotations

from unittest.mock import MagicMock


def _mock_supabase() -> tuple[MagicMock, MagicMock]:
    """Returns (supabase_mock, builder_mock) with chained methods."""
    sb = MagicMock()
    builder = MagicMock()
    for method in [
        "select", "like", "in_", "order", "limit",
        "lte", "gte", "eq", "update", "insert", "upsert",
    ]:
        getattr(builder, method).return_value = builder
    result = MagicMock()
    result.data = [{"id": "mock-outcome-id"}]
    builder.execute.return_value = result
    sb.table.return_value = builder
    return sb, builder


def test_log_review_outcome_calls_upsert():
    """Upsert is called on canonical_review_outcomes table."""
    from src.db.review_outcomes import log_review_outcome

    sb, builder = _mock_supabase()

    log_review_outcome(
        sb,
        review_id="review-1",
        source_happening_id="src-1",
        decision="merge",
        happening_id="hap-1",
        selected_candidate_happening_id="hap-1",
        confidence_score=0.92,
    )

    sb.table.assert_called_with("canonical_review_outcomes")
    builder.upsert.assert_called_once()


def test_log_review_outcome_on_conflict_review_id():
    """Upsert uses review_id as the conflict key for idempotency."""
    from src.db.review_outcomes import log_review_outcome

    sb, builder = _mock_supabase()

    log_review_outcome(
        sb,
        review_id="review-1",
        source_happening_id="src-1",
        decision="create",
    )

    _, kwargs = builder.upsert.call_args
    assert kwargs.get("on_conflict") == "review_id"


def test_log_review_outcome_required_fields_always_present():
    """review_id, source_happening_id, decision are always in payload."""
    from src.db.review_outcomes import log_review_outcome

    sb, builder = _mock_supabase()

    log_review_outcome(
        sb,
        review_id="review-1",
        source_happening_id="src-1",
        decision="ignore",
    )

    payload = builder.upsert.call_args[0][0]
    assert payload["review_id"] == "review-1"
    assert payload["source_happening_id"] == "src-1"
    assert payload["decision"] == "ignore"


def test_log_review_outcome_omits_none_optional_fields():
    """Optional fields not passed should not appear in the payload."""
    from src.db.review_outcomes import log_review_outcome

    sb, builder = _mock_supabase()

    log_review_outcome(
        sb,
        review_id="review-1",
        source_happening_id="src-1",
        decision="merge",
    )

    payload = builder.upsert.call_args[0][0]
    assert "happening_id" not in payload
    assert "selected_candidate_happening_id" not in payload
    assert "confidence_score" not in payload
    assert "confidence_breakdown" not in payload
    assert "resolved_by" not in payload


def test_log_review_outcome_includes_optional_fields_when_provided():
    """Optional fields are included when non-None values are provided."""
    from src.db.review_outcomes import log_review_outcome

    sb, builder = _mock_supabase()

    log_review_outcome(
        sb,
        review_id="review-1",
        source_happening_id="src-1",
        decision="merge",
        happening_id="hap-1",
        selected_candidate_happening_id="hap-2",
        confidence_score=0.88,
        confidence_breakdown={"title": 0.9, "date": 1.0},
        resolved_by="admin@example.com",
    )

    payload = builder.upsert.call_args[0][0]
    assert payload["happening_id"] == "hap-1"
    assert payload["selected_candidate_happening_id"] == "hap-2"
    assert payload["confidence_score"] == 0.88
    assert payload["confidence_breakdown"] == {"title": 0.9, "date": 1.0}
    assert payload["resolved_by"] == "admin@example.com"
