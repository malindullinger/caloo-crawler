"""
Tests for merge_run_stats persistence.

Verifies:
  1. Running merge_loop on an empty queue still writes a merge_run_stats
     row with finished_at set and all counters at 0.
  2. Two runs create two separate rows (no upsert; one row per run).
  3. Phase 7: canonical_updates_count and history_rows_created in stats payload.
  4. Phase 7: per-source breakdown (source_breakdown JSONB) populated.
  5. Phase 9: confidence telemetry fields passed to finish_merge_run.
"""
from __future__ import annotations

from unittest.mock import MagicMock


def _mock_supabase_for_stats() -> tuple[MagicMock, list[dict], list[dict]]:
    """
    Mock Supabase that:
      - Returns empty for all non-stats tables (empty queue).
      - Captures merge_run_stats inserts and updates for assertions.
    """
    sb = MagicMock()
    stats_inserts: list[dict] = []
    stats_updates: list[dict] = []

    def table_factory(name: str) -> MagicMock:
        builder = MagicMock()
        for method in [
            "select", "like", "in_", "order", "limit",
            "lte", "gte", "eq", "update", "insert", "upsert",
        ]:
            getattr(builder, method).return_value = builder

        if name == "merge_run_stats":
            def mock_insert(payload):
                stats_inserts.append(payload)
                return builder
            builder.insert.side_effect = mock_insert

            def mock_update(payload):
                stats_updates.append(payload)
                return builder
            builder.update.side_effect = mock_update

            result = MagicMock()
            result.data = [{"id": "mock-stats-run-id"}]
            builder.execute.return_value = result
        else:
            result = MagicMock()
            result.data = []
            builder.execute.return_value = result

        return builder

    sb.table.side_effect = table_factory
    return sb, stats_inserts, stats_updates


def test_empty_queue_writes_stats_row_with_zero_counters():
    """
    Running merge_loop on an empty queue still writes a merge_run_stats
    row with finished_at non-null and all counters at 0.
    """
    from src.canonicalize.merge_loop import run_merge_loop

    sb, stats_inserts, stats_updates = _mock_supabase_for_stats()

    counts = run_merge_loop(supabase=sb, dry_run=True, persist_run_stats=True)

    # Verify one insert was made (create_merge_run)
    assert len(stats_inserts) == 1
    assert "started_at" in stats_inserts[0]

    # Verify one update was made (finish_merge_run)
    assert len(stats_updates) == 1
    update = stats_updates[0]
    assert update["finished_at"] is not None
    assert update["source_rows_processed"] == 0
    assert update["canonical_created"] == 0
    assert update["canonical_merged"] == 0
    assert update["canonical_review"] == 0
    assert update["errors"] == 0
    # Phase 7: new counters present and zero
    assert update["canonical_updates_count"] == 0
    assert update["history_rows_created"] == 0

    # Merge behavior unchanged
    assert counts["queued"] == 0
    assert counts["created"] == 0


def test_two_runs_create_two_stats_rows():
    """
    Each merge_loop run creates its own merge_run_stats row.
    No upsert — one row per run.
    """
    from src.canonicalize.merge_loop import run_merge_loop

    sb, stats_inserts, stats_updates = _mock_supabase_for_stats()

    run_merge_loop(supabase=sb, dry_run=True, persist_run_stats=True)
    run_merge_loop(supabase=sb, dry_run=True, persist_run_stats=True)

    assert len(stats_inserts) == 2, "Each run must create a separate stats row"
    assert len(stats_updates) == 2, "Each run must finish its stats row"


def test_merge_run_stats_includes_updates_and_history_counts():
    """
    Phase 7: When a source row creates a canonical happening,
    the stats payload includes canonical_updates_count and
    history_rows_created (both at 0 for creates, non-zero for merges).

    In this test: one source row → create (no existing candidates) →
    canonical_updates_count=0, history_rows_created=0.
    """
    from src.canonicalize.merge_loop import run_merge_loop

    source_row = {
        "id": "src-1",
        "source_id": "zurich_gemeinde",
        "dedupe_key": "v1|abc",
        "status": "queued",
        "title_raw": "Yoga im Park",
        "description_raw": "Beschreibung",
        "start_date_local": "2026-03-15",
        "location_raw": "Gemeindehaus",
        "timezone": "Europe/Zurich",
        "item_url": "https://example.com/1",
        "external_id": None,
        "source_tier": "A",
    }

    call_count = 0

    def _table_factory(name: str) -> MagicMock:
        nonlocal call_count
        builder = MagicMock()
        for method in [
            "select", "like", "in_", "order", "limit",
            "lte", "gte", "eq", "update", "insert", "upsert",
        ]:
            getattr(builder, method).return_value = builder

        if name == "merge_run_stats":
            result = MagicMock()
            result.data = [{"id": "mock-stats-run-id"}]
            builder.execute.return_value = result
            # Capture update payload
            captured_updates = []

            def mock_update(payload):
                captured_updates.append(payload)
                return builder
            builder.update.side_effect = mock_update
            builder._captured_updates = captured_updates
        else:
            def _execute():
                nonlocal call_count
                call_count += 1
                result = MagicMock()
                if call_count == 1:
                    result.data = [source_row]  # fetch_queued
                else:
                    result.data = []
                return result
            builder.execute.side_effect = _execute

        return builder

    sb = MagicMock()
    builders: dict[str, MagicMock] = {}

    def table_dispatch(name: str) -> MagicMock:
        if name not in builders:
            builders[name] = _table_factory(name)
        return builders[name]

    sb.table.side_effect = table_dispatch

    counts = run_merge_loop(supabase=sb, dry_run=True, persist_run_stats=True)

    assert counts["created"] == 1
    assert counts["canonical_updates"] == 0
    assert counts["history_rows"] == 0

    # Check stats update payload
    stats_builder = builders["merge_run_stats"]
    assert len(stats_builder._captured_updates) == 1
    update = stats_builder._captured_updates[0]
    assert update["canonical_updates_count"] == 0
    assert update["history_rows_created"] == 0
    assert update["canonical_created"] == 1


def test_per_source_breakdown_populated():
    """
    Phase 7: When rows from two different source_ids are processed,
    source_breakdown JSONB in the stats update contains both source_ids
    with correct per-source counts.
    """
    from src.canonicalize.merge_loop import run_merge_loop

    source_row_a = {
        "id": "src-a",
        "source_id": "zurich_gemeinde",
        "dedupe_key": "v1|abc",
        "status": "queued",
        "title_raw": "Yoga im Park",
        "description_raw": "Beschreibung",
        "start_date_local": "2026-03-15",
        "location_raw": "Gemeindehaus",
        "timezone": "Europe/Zurich",
        "item_url": "https://example.com/1",
        "external_id": None,
        "source_tier": "A",
    }
    source_row_b = {
        "id": "src-b",
        "source_id": "winterthur_stadt",
        "dedupe_key": "v1|def",
        "status": "queued",
        "title_raw": "Schwimmen für Kinder",
        "description_raw": "Schwimmkurs",
        "start_date_local": "2026-03-16",
        "location_raw": "Hallenbad",
        "timezone": "Europe/Zurich",
        "item_url": "https://example.com/2",
        "external_id": None,
        "source_tier": "B",
    }

    call_count = 0

    def _table_factory(name: str) -> MagicMock:
        nonlocal call_count
        builder = MagicMock()
        for method in [
            "select", "like", "in_", "order", "limit",
            "lte", "gte", "eq", "update", "insert", "upsert",
        ]:
            getattr(builder, method).return_value = builder

        if name == "merge_run_stats":
            result = MagicMock()
            result.data = [{"id": "mock-stats-run-id"}]
            builder.execute.return_value = result
            captured_updates = []

            def mock_update(payload):
                captured_updates.append(payload)
                return builder
            builder.update.side_effect = mock_update
            builder._captured_updates = captured_updates
        else:
            def _execute():
                nonlocal call_count
                call_count += 1
                result = MagicMock()
                if call_count == 1:
                    result.data = [source_row_a, source_row_b]  # fetch_queued batch
                else:
                    result.data = []
                return result
            builder.execute.side_effect = _execute

        return builder

    sb = MagicMock()
    builders: dict[str, MagicMock] = {}

    def table_dispatch(name: str) -> MagicMock:
        if name not in builders:
            builders[name] = _table_factory(name)
        return builders[name]

    sb.table.side_effect = table_dispatch

    counts = run_merge_loop(supabase=sb, dry_run=True, persist_run_stats=True)

    assert counts["created"] == 2
    assert counts["queued"] == 2

    # Check stats update payload has source_breakdown
    stats_builder = builders["merge_run_stats"]
    assert len(stats_builder._captured_updates) == 1
    update = stats_builder._captured_updates[0]

    assert "source_breakdown" in update
    breakdown = update["source_breakdown"]

    assert "zurich_gemeinde" in breakdown
    assert "winterthur_stadt" in breakdown
    assert breakdown["zurich_gemeinde"]["created"] == 1
    assert breakdown["winterthur_stadt"]["created"] == 1


def test_finish_merge_run_includes_confidence_fields_when_provided():
    """
    Phase 9: When confidence telemetry kwargs are provided to
    finish_merge_run, they appear in the update payload. When not
    provided, they are absent (not set to null).
    """
    from src.db.merge_run_stats import MergeRunCounters, finish_merge_run

    sb = MagicMock()
    builder = MagicMock()
    for method in [
        "select", "like", "in_", "order", "limit",
        "lte", "gte", "eq", "update", "insert", "upsert",
    ]:
        getattr(builder, method).return_value = builder
    result = MagicMock()
    result.data = [{"id": "mock-stats-run-id"}]
    builder.execute.return_value = result

    captured_updates: list[dict] = []

    def mock_update(payload):
        captured_updates.append(payload)
        return builder
    builder.update.side_effect = mock_update
    sb.table.return_value = builder

    hist = {"0_50": 1, "50_70": 2, "70_85": 3, "85_95": 0, "95_99": 0, "99_100": 1}
    src_conf = {"zurich": {"min": 0.3, "avg": 0.7, "max": 1.0, "hist": hist}}

    finish_merge_run(
        sb,
        "run-1",
        MergeRunCounters(),
        confidence_min=0.3,
        confidence_avg=0.7,
        confidence_max=1.0,
        confidence_histogram=hist,
        source_confidence=src_conf,
    )

    assert len(captured_updates) == 1
    update = captured_updates[0]

    # Confidence fields present
    assert update["confidence_min"] == 0.3
    assert update["confidence_avg"] == 0.7
    assert update["confidence_max"] == 1.0
    assert update["confidence_histogram"] == hist
    assert update["source_confidence"] == src_conf

    # Now test without confidence args — fields should be absent
    captured_updates.clear()
    finish_merge_run(
        sb,
        "run-2",
        MergeRunCounters(),
    )

    assert len(captured_updates) == 1
    update2 = captured_updates[0]
    assert "confidence_min" not in update2
    assert "confidence_avg" not in update2
    assert "confidence_max" not in update2
    assert "confidence_histogram" not in update2
    assert "source_confidence" not in update2
