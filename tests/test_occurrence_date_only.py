"""
Tests for date-only (unknown time) occurrence creation behavior.

Verifies:
  1. Date-only source rows (start_at=None) create happening + offering
     but skip occurrence insert entirely — no errors.
  2. Datetime source rows (start_at present) still create all three:
     happening + offering + occurrence.
  3. The merge loop processes date-only rows without errors and marks
     them as created (not errored).
"""
from __future__ import annotations

from unittest.mock import MagicMock, call


# ---------------------------------------------------------------------------
# Shared fixtures
# ---------------------------------------------------------------------------

DATE_ONLY_SOURCE_ROW = {
    "id": "src-date-only",
    "source_id": "test_source",
    "dedupe_key": "v1|dateonly",
    "status": "queued",
    "title_raw": "Kinderflohmarkt",
    "description_raw": "Grosser Kinderflohmarkt im Quartier",
    "start_date_local": "2026-04-12",
    "end_date_local": "2026-04-12",
    "location_raw": "Gemeindesaal",
    "timezone": "Europe/Zurich",
    "start_at": None,
    "end_at": None,
    "item_url": "https://example.com/flohmarkt",
    "external_id": None,
    "source_tier": "A",
}

DATETIME_SOURCE_ROW = {
    "id": "src-datetime",
    "source_id": "test_source",
    "dedupe_key": "v1|datetime",
    "status": "queued",
    "title_raw": "Yoga im Park",
    "description_raw": "Yoga fuer alle",
    "start_date_local": "2026-04-12",
    "end_date_local": "2026-04-12",
    "location_raw": "Stadtpark",
    "timezone": "Europe/Zurich",
    "start_at": "2026-04-12T10:00:00+02:00",
    "end_at": "2026-04-12T11:30:00+02:00",
    "item_url": "https://example.com/yoga",
    "external_id": None,
    "source_tier": "A",
}


def _mock_supabase_tracking_tables() -> tuple[MagicMock, dict[str, list[dict]]]:
    """
    Mock Supabase that tracks insert calls per table name.
    Returns (mock_client, inserts_by_table).
    """
    sb = MagicMock()
    inserts_by_table: dict[str, list[dict]] = {}

    def table_factory(name: str) -> MagicMock:
        builder = MagicMock()
        for method in [
            "select", "like", "in_", "order", "limit",
            "lte", "gte", "eq", "update", "insert", "upsert",
        ]:
            getattr(builder, method).return_value = builder

        # Track insert payloads per table
        def mock_insert(payload, table_name=name):
            inserts_by_table.setdefault(table_name, []).append(payload)
            return builder
        builder.insert.side_effect = mock_insert

        result = MagicMock()
        result.data = [{"id": f"mock-{name}-id"}]
        builder.execute.return_value = result

        return builder

    sb.table.side_effect = table_factory
    return sb, inserts_by_table


# ---------------------------------------------------------------------------
# Test: date-only skips occurrence
# ---------------------------------------------------------------------------

def test_date_only_creates_happening_and_offering_but_no_occurrence():
    """
    A date-only source row (start_at=None) must create a happening
    and offering, but must NOT attempt to insert an occurrence row.
    """
    from src.canonicalize.merge_loop import create_happening_schedule_occurrence

    sb, inserts = _mock_supabase_tracking_tables()

    happening_id = create_happening_schedule_occurrence(
        supabase=sb,
        source_row=DATE_ONLY_SOURCE_ROW,
    )

    assert happening_id is not None

    # Happening and offering created
    assert "happening" in inserts, "happening insert expected"
    assert "offering" in inserts, "offering insert expected"
    assert len(inserts["happening"]) == 1
    assert len(inserts["offering"]) == 1

    # Occurrence NOT created
    assert "occurrence" not in inserts, (
        "date-only row must NOT create an occurrence (start_at is None)"
    )


def test_datetime_creates_happening_offering_and_occurrence():
    """
    A datetime source row (start_at present) must create all three:
    happening, offering, and occurrence.
    """
    from src.canonicalize.merge_loop import create_happening_schedule_occurrence

    sb, inserts = _mock_supabase_tracking_tables()

    happening_id = create_happening_schedule_occurrence(
        supabase=sb,
        source_row=DATETIME_SOURCE_ROW,
    )

    assert happening_id is not None

    assert "happening" in inserts
    assert "offering" in inserts
    assert "occurrence" in inserts, (
        "datetime row must create an occurrence"
    )
    assert len(inserts["occurrence"]) == 1

    # Verify occurrence payload has start_at
    occ_payload = inserts["occurrence"][0]
    assert occ_payload["start_at"] == "2026-04-12T10:00:00+02:00"
    assert occ_payload["end_at"] == "2026-04-12T11:30:00+02:00"
    assert occ_payload["status"] == "scheduled"


# ---------------------------------------------------------------------------
# Test: merge loop integration — date-only row doesn't error
# ---------------------------------------------------------------------------

def _mock_supabase_for_merge_loop(source_rows: list[dict]) -> tuple[MagicMock, dict[str, list[dict]]]:
    """
    Mock for run_merge_loop: returns source_rows on first fetch,
    empty on second. Tracks inserts per table.
    """
    sb = MagicMock()
    inserts_by_table: dict[str, list[dict]] = {}
    call_counts: dict[str, int] = {}

    def table_factory(name: str) -> MagicMock:
        builder = MagicMock()
        for method in [
            "select", "like", "in_", "order", "limit",
            "lte", "gte", "eq", "update", "insert", "upsert",
        ]:
            getattr(builder, method).return_value = builder

        def mock_insert(payload, table_name=name):
            inserts_by_table.setdefault(table_name, []).append(payload)
            return builder
        builder.insert.side_effect = mock_insert

        if name == "merge_run_stats":
            result = MagicMock()
            result.data = [{"id": "mock-stats-id"}]
            builder.execute.return_value = result
        elif name == "source_happenings":
            def _execute(tname=name):
                call_counts[tname] = call_counts.get(tname, 0) + 1
                result = MagicMock()
                if call_counts[tname] == 1:
                    result.data = source_rows
                else:
                    result.data = []
                return result
            builder.execute.side_effect = _execute
        else:
            # For happening, offering, occurrence, etc.
            result = MagicMock()
            result.data = [{"id": f"mock-{name}-id"}]
            builder.execute.return_value = result

        return builder

    builders: dict[str, MagicMock] = {}

    def table_dispatch(name: str) -> MagicMock:
        if name not in builders:
            builders[name] = table_factory(name)
        return builders[name]

    sb.table.side_effect = table_dispatch
    return sb, inserts_by_table


def test_merge_loop_date_only_row_no_errors():
    """
    A date-only source row processed through the full merge loop
    must be counted as 'created' with zero errors.
    """
    from src.canonicalize.merge_loop import run_merge_loop

    sb, inserts = _mock_supabase_for_merge_loop([DATE_ONLY_SOURCE_ROW])

    counts = run_merge_loop(
        supabase=sb,
        dry_run=True,
        persist_run_stats=True,
    )

    assert counts["errors"] == 0, f"date-only row must not cause errors, got {counts}"
    assert counts["created"] == 1
    assert counts["queued"] == 1


def test_merge_loop_mixed_date_only_and_datetime_no_errors():
    """
    A batch with both date-only and datetime rows must process
    without errors. Both should be counted as 'created'.
    """
    from src.canonicalize.merge_loop import run_merge_loop

    sb, inserts = _mock_supabase_for_merge_loop([
        DATE_ONLY_SOURCE_ROW,
        DATETIME_SOURCE_ROW,
    ])

    counts = run_merge_loop(
        supabase=sb,
        dry_run=True,
        persist_run_stats=True,
    )

    assert counts["errors"] == 0
    assert counts["created"] == 2
    assert counts["queued"] == 2
