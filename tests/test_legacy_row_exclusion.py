"""
Regression test: legacy (non-v1) source_happenings rows must never be
selected for processing, even if their status is 'queued'.

Tests the application-level guard in fetch_queued_source_happenings().
"""
from __future__ import annotations

from unittest.mock import MagicMock, call


def _build_mock_supabase(rows: list[dict]) -> MagicMock:
    """Build a mock Supabase client that tracks chained filter calls."""
    sb = MagicMock()

    # Track the chained builder so we can inspect .like() / .in_() calls
    builder = MagicMock()
    sb.table.return_value.select.return_value = builder
    builder.like.return_value = builder
    builder.in_.return_value = builder
    builder.order.return_value = builder
    builder.limit.return_value = builder

    # .execute() returns the rows
    result = MagicMock()
    result.data = rows
    builder.execute.return_value = result

    return sb, builder


def test_fetch_excludes_legacy_rows():
    """
    Given a mix of v1| and legacy rows in the DB, only v1| rows should
    be selected. We verify this by checking the .like('dedupe_key', 'v1|%')
    filter is always applied.
    """
    from src.canonicalize.merge_loop import fetch_queued_source_happenings

    # Simulate: DB returns only v1| rows (because the filter works)
    v1_row = {
        "id": "aaa-111",
        "source_id": "test",
        "dedupe_key": "v1|abc123",
        "status": "queued",
        "title_raw": "Test Event",
    }
    sb, builder = _build_mock_supabase([v1_row])

    rows = fetch_queued_source_happenings(sb)

    # Verify the v1| guard is applied
    builder.like.assert_called_once_with("dedupe_key", "v1|%")
    # Verify status filter
    builder.in_.assert_called_once_with("status", ["queued"])
    # Verify we got the row
    assert len(rows) == 1
    assert rows[0]["dedupe_key"] == "v1|abc123"


def test_fetch_with_needs_review_still_guards():
    """Even with include_needs_review=True, the v1| guard must be present."""
    from src.canonicalize.merge_loop import fetch_queued_source_happenings

    sb, builder = _build_mock_supabase([])

    fetch_queued_source_happenings(sb, include_needs_review=True)

    builder.like.assert_called_once_with("dedupe_key", "v1|%")
    builder.in_.assert_called_once_with("status", ["queued", "needs_review"])


def test_legacy_row_not_returned():
    """
    Simulate a scenario where a legacy row with status='queued' exists.
    The DB filter should exclude it, so it never appears in results.

    This test documents the contract: if a row has dedupe_key='legacy|abc'
    and status='queued', fetch_queued_source_happenings must NOT return it.
    """
    from src.canonicalize.merge_loop import fetch_queued_source_happenings

    # The DB would filter this out via .like('dedupe_key', 'v1|%'),
    # so we simulate it returning empty (the legacy row is excluded)
    sb, builder = _build_mock_supabase([])

    rows = fetch_queued_source_happenings(sb)

    # Guard is applied
    builder.like.assert_called_once_with("dedupe_key", "v1|%")
    # No rows returned (legacy row filtered out)
    assert rows == []


def test_mark_source_processing_failed_guards_v1():
    """mark_source_processing_failed must scope its UPDATE to v1| rows."""
    from src.canonicalize.merge_loop import mark_source_processing_failed

    sb = MagicMock()
    builder = MagicMock()
    sb.table.return_value.update.return_value = builder
    builder.eq.return_value = builder
    builder.like.return_value = builder

    result = MagicMock()
    builder.execute.return_value = result

    mark_source_processing_failed(
        supabase=sb,
        source_happening_id="test-id",
        error_message="boom",
    )

    # Verify .like('dedupe_key', 'v1|%') is in the chain
    builder.like.assert_called_once_with("dedupe_key", "v1|%")
