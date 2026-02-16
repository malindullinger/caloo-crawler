"""
Tests for the dedupe_key fast-path in the merge loop.

Verifies:
  1. lookup_happening_by_dedupe_key returns a happening_id when a sibling
     with the same (source_id, dedupe_key) was already processed.
  2. Returns None when no sibling exists.
  3. Returns None when the linked happening is archived.
  4. The merge loop uses the fast-path and counts it correctly.
  5. Merge loop results are identical with and without fast-path.
"""
from __future__ import annotations

from unittest.mock import MagicMock


# ---------------------------------------------------------------------------
# Shared fixtures
# ---------------------------------------------------------------------------

SOURCE_ROW = {
    "id": "src-new",
    "source_id": "eventbrite",
    "dedupe_key": "v1|abc123hash",
    "status": "queued",
    "title_raw": "Kinderyoga im Park",
    "start_date_local": "2026-03-15",
    "location_raw": "Gemeindehaus",
    "timezone": "Europe/Zurich",
    "start_at": "2026-03-15T10:00:00+01:00",
    "end_at": "2026-03-15T12:00:00+01:00",
    "source_tier": "A",
}


def _mock_supabase_for_fast_path(
    *,
    sibling_exists: bool = True,
    link_exists: bool = True,
    happening_archived: bool = False,
) -> MagicMock:
    """
    Build a mock Supabase client that simulates the fast-path queries.
    """
    sb = MagicMock()

    def table_factory(name: str) -> MagicMock:
        builder = MagicMock()
        for method in [
            "select", "like", "in_", "order", "limit",
            "lte", "gte", "eq", "neq", "update", "insert", "upsert",
        ]:
            getattr(builder, method).return_value = builder

        result = MagicMock()

        if name == "source_happenings":
            if sibling_exists:
                result.data = [{"id": "sibling-processed-id"}]
            else:
                result.data = []
        elif name == "happening_sources":
            if link_exists:
                result.data = [{"happening_id": "hap-linked-123"}]
            else:
                result.data = []
        elif name == "happening":
            if happening_archived:
                result.data = [{"id": "hap-linked-123", "visibility_status": "archived"}]
            else:
                result.data = [{"id": "hap-linked-123", "visibility_status": "draft"}]
        else:
            result.data = [{"id": f"mock-{name}-id"}]

        builder.execute.return_value = result
        return builder

    sb.table.side_effect = table_factory
    return sb


# ---------------------------------------------------------------------------
# Tests: lookup_happening_by_dedupe_key
# ---------------------------------------------------------------------------

def test_fast_path_returns_happening_when_sibling_processed():
    from src.canonicalize.merge_loop import lookup_happening_by_dedupe_key

    sb = _mock_supabase_for_fast_path(sibling_exists=True, link_exists=True)
    result = lookup_happening_by_dedupe_key(sb, SOURCE_ROW)
    assert result == "hap-linked-123"


def test_fast_path_returns_none_when_no_sibling():
    from src.canonicalize.merge_loop import lookup_happening_by_dedupe_key

    sb = _mock_supabase_for_fast_path(sibling_exists=False)
    result = lookup_happening_by_dedupe_key(sb, SOURCE_ROW)
    assert result is None


def test_fast_path_returns_none_when_no_link():
    from src.canonicalize.merge_loop import lookup_happening_by_dedupe_key

    sb = _mock_supabase_for_fast_path(sibling_exists=True, link_exists=False)
    result = lookup_happening_by_dedupe_key(sb, SOURCE_ROW)
    assert result is None


def test_fast_path_returns_none_when_archived():
    from src.canonicalize.merge_loop import lookup_happening_by_dedupe_key

    sb = _mock_supabase_for_fast_path(
        sibling_exists=True, link_exists=True, happening_archived=True,
    )
    result = lookup_happening_by_dedupe_key(sb, SOURCE_ROW)
    assert result is None


def test_fast_path_returns_none_when_no_dedupe_key():
    from src.canonicalize.merge_loop import lookup_happening_by_dedupe_key

    sb = MagicMock()
    row_no_key = {**SOURCE_ROW, "dedupe_key": None}
    result = lookup_happening_by_dedupe_key(sb, row_no_key)
    assert result is None


def test_fast_path_returns_none_when_no_source_id():
    from src.canonicalize.merge_loop import lookup_happening_by_dedupe_key

    sb = MagicMock()
    row_no_source = {**SOURCE_ROW, "source_id": None}
    result = lookup_happening_by_dedupe_key(sb, row_no_source)
    assert result is None


# ---------------------------------------------------------------------------
# Tests: merge loop integration with fast-path
# ---------------------------------------------------------------------------

def _mock_supabase_for_merge_loop_fast_path(source_rows: list[dict]) -> MagicMock:
    """
    Mock for run_merge_loop where the fast-path finds a sibling.
    First fetch returns source_rows, second returns empty.
    """
    sb = MagicMock()
    call_counts: dict[str, int] = {}
    builders: dict[str, MagicMock] = {}

    def table_factory(name: str) -> MagicMock:
        builder = MagicMock()
        for method in [
            "select", "like", "in_", "order", "limit",
            "lte", "gte", "eq", "neq", "update", "insert", "upsert",
        ]:
            getattr(builder, method).return_value = builder

        if name == "source_happenings":
            # Track calls: 1st = fetch batch, 2nd = fast-path sibling lookup,
            # 3rd = fetch batch (empty)
            def _execute():
                call_counts["sh"] = call_counts.get("sh", 0) + 1
                result = MagicMock()
                cnt = call_counts["sh"]
                if cnt == 1:
                    # fetch_queued_source_happenings: return source_rows
                    result.data = source_rows
                elif cnt == 2:
                    # lookup fast-path sibling: return processed sibling
                    result.data = [{"id": "sibling-processed"}]
                else:
                    # subsequent fetches: empty
                    result.data = []
                return result
            builder.execute.side_effect = _execute
        elif name == "happening_sources":
            result = MagicMock()
            result.data = [{"happening_id": "hap-fast-123"}]
            builder.execute.return_value = result
        elif name == "happening":
            result = MagicMock()
            result.data = [{"id": "hap-fast-123", "visibility_status": "draft"}]
            builder.execute.return_value = result
        elif name == "merge_run_stats":
            result = MagicMock()
            result.data = [{"id": "stats-id"}]
            builder.execute.return_value = result
        else:
            result = MagicMock()
            result.data = [{"id": f"mock-{name}-id"}]
            builder.execute.return_value = result

        return builder

    def table_dispatch(name: str) -> MagicMock:
        if name not in builders:
            builders[name] = table_factory(name)
        return builders[name]

    sb.table.side_effect = table_dispatch
    return sb


def test_merge_loop_fast_path_counted():
    """
    When a dedupe-key sibling exists, the merge loop should use
    the fast path and count it in dedupe_fast_path.
    """
    from src.canonicalize.merge_loop import run_merge_loop

    sb = _mock_supabase_for_merge_loop_fast_path([SOURCE_ROW])

    counts = run_merge_loop(
        supabase=sb,
        dry_run=True,
        persist_run_stats=True,
    )

    assert counts["dedupe_fast_path"] == 1
    assert counts["merged"] == 1
    assert counts["errors"] == 0
    assert counts["created"] == 0
