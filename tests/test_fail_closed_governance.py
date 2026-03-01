"""
Phase A.3: Fail-closed + governance-by-default non-regression tests.

Verifies:
  1. Dry mode terminates deterministically (no infinite loop).
  2. NULL start_at is never inserted into occurrence.
  3. Offering NK get-or-create does not create duplicates under simulated conflict.
  4. Convergence job LIVE mode aborts when RPC is missing.
  5. Unresolvable offering constraint marks source row needs_review (not processed).
"""
from __future__ import annotations

import time
from unittest.mock import MagicMock

import pytest


# ---------------------------------------------------------------------------
# Shared fixtures
# ---------------------------------------------------------------------------

SOURCE_ROW_QUEUED = {
    "id": "src-1",
    "source_id": "zurich_gemeinde",
    "dedupe_key": "v1|abc",
    "status": "queued",
    "title_raw": "Kinderyoga im Park",
    "description_raw": "Beschreibung",
    "start_date_local": "2026-03-15",
    "location_raw": "Gemeindehaus",
    "timezone": "Europe/Zurich",
    "item_url": "https://example.com/1",
    "external_id": None,
    "source_tier": "A",
    "start_at": None,  # date-only: no start_at
    "end_at": None,
}


def _make_source_rows(n: int) -> list[dict]:
    """Generate n distinct source rows with unique ids."""
    return [
        {
            **SOURCE_ROW_QUEUED,
            "id": f"src-{i}",
            "dedupe_key": f"v1|key{i}",
            "title_raw": f"Event {i}",
        }
        for i in range(n)
    ]


def _mock_supabase_returning_rows(source_rows: list[dict]) -> MagicMock:
    """
    Mock Supabase that returns source_rows on the first fetch call,
    then empty for everything else. Suitable for dry-run merge_loop tests.
    """
    sb = MagicMock()
    call_count = {"n": 0}

    def table_factory(name: str) -> MagicMock:
        builder = MagicMock()
        for method in [
            "select", "like", "in_", "order", "limit",
            "lte", "gte", "eq", "neq", "update", "insert", "upsert",
        ]:
            getattr(builder, method).return_value = builder

        if name == "merge_run_stats":
            result = MagicMock()
            result.data = [{"id": "stats-id"}]
            builder.execute.return_value = result
        else:
            def _execute():
                call_count["n"] += 1
                result = MagicMock()
                if call_count["n"] == 1:
                    result.data = list(source_rows)
                else:
                    result.data = []
                return result
            builder.execute.side_effect = _execute

        return builder

    builders: dict[str, MagicMock] = {}

    def table_dispatch(name: str) -> MagicMock:
        if name not in builders:
            builders[name] = table_factory(name)
        return builders[name]

    sb.table.side_effect = table_dispatch
    return sb


# ===========================================================================
# Test 1: Dry mode terminates (no infinite loop)
# ===========================================================================

def test_dry_mode_terminates_with_queued_rows():
    """
    Dry mode must terminate even when queued rows exist and no DB writes
    happen (claim is a no-op). The seen-set + max_batches guards must
    prevent infinite looping.

    Regression test for the bug where dry mode looped forever on 16 queued rows.
    """
    from src.canonicalize.merge_loop import run_merge_loop

    rows = _make_source_rows(16)

    # Mock that always returns the same 16 rows (simulating the original bug)
    sb = MagicMock()

    def table_factory(name: str) -> MagicMock:
        builder = MagicMock()
        for method in [
            "select", "like", "in_", "order", "limit",
            "lte", "gte", "eq", "neq", "update", "insert", "upsert",
        ]:
            getattr(builder, method).return_value = builder

        if name == "merge_run_stats":
            result = MagicMock()
            result.data = [{"id": "stats-id"}]
            builder.execute.return_value = result
        else:
            # Always return the same rows (no status change in dry mode)
            result = MagicMock()
            result.data = list(rows)
            builder.execute.return_value = result

        return builder

    builders: dict[str, MagicMock] = {}

    def table_dispatch(name: str) -> MagicMock:
        if name not in builders:
            builders[name] = table_factory(name)
        return builders[name]

    sb.table.side_effect = table_dispatch

    t0 = time.monotonic()
    counts = run_merge_loop(
        supabase=sb,
        dry_run=True,
        persist_run_stats=True,
        max_batches=3,  # hard cap
    )
    elapsed = time.monotonic() - t0

    # Must terminate in a reasonable time (well under 10s)
    assert elapsed < 10.0, f"Dry mode took {elapsed:.1f}s — likely an infinite loop"

    # Must have processed exactly one batch of 16 rows (seen-set prevents re-fetch)
    assert counts["queued"] == 16
    assert counts["errors"] == 0


def test_dry_mode_terminates_on_empty_queue():
    """Dry mode on empty queue terminates immediately."""
    from src.canonicalize.merge_loop import run_merge_loop

    sb = _mock_supabase_returning_rows([])

    counts = run_merge_loop(supabase=sb, dry_run=True, persist_run_stats=False)

    assert counts["queued"] == 0
    assert counts["created"] == 0


def test_dry_mode_max_batches_default_is_bounded():
    """
    Dry mode defaults to max_batches=10 when caller doesn't specify.
    This prevents unbounded looping even if seen-set has a bug.
    """
    from src.canonicalize.merge_loop import run_merge_loop

    # This mock returns 1 row per batch, never empty, to test batch cap
    sb = MagicMock()
    batch_counter = {"n": 0}

    def table_factory(name: str) -> MagicMock:
        builder = MagicMock()
        for method in [
            "select", "like", "in_", "order", "limit",
            "lte", "gte", "eq", "neq", "update", "insert", "upsert",
        ]:
            getattr(builder, method).return_value = builder

        if name == "merge_run_stats":
            result = MagicMock()
            result.data = [{"id": "stats-id"}]
            builder.execute.return_value = result
        elif name == "source_happenings":
            def _execute():
                batch_counter["n"] += 1
                result = MagicMock()
                # Each batch returns a unique row so seen-set doesn't block
                result.data = [{
                    **SOURCE_ROW_QUEUED,
                    "id": f"src-batch-{batch_counter['n']}",
                    "dedupe_key": f"v1|batch{batch_counter['n']}",
                }]
                return result
            builder.execute.side_effect = _execute
        else:
            result = MagicMock()
            result.data = []
            builder.execute.return_value = result

        return builder

    builders: dict[str, MagicMock] = {}

    def table_dispatch(name: str) -> MagicMock:
        if name not in builders:
            builders[name] = table_factory(name)
        return builders[name]

    sb.table.side_effect = table_dispatch

    counts = run_merge_loop(
        supabase=sb,
        dry_run=True,
        persist_run_stats=True,
        # Don't pass max_batches — should default to 10
    )

    # Must stop at the default cap (10 batches)
    assert counts["queued"] <= 10


# ===========================================================================
# Test 2: NULL start_at never inserted into occurrence
# ===========================================================================

def test_null_start_at_never_creates_occurrence():
    """
    When source_row.start_at is None (date-only event), the pipeline must
    NEVER attempt to insert an occurrence row. It should increment
    occurrence_null_start_skipped instead.
    """
    from src.canonicalize.merge_loop import create_happening_schedule_occurrence

    sb = MagicMock()
    builder = MagicMock()
    for method in [
        "select", "like", "in_", "order", "limit",
        "lte", "gte", "eq", "neq", "update", "insert", "upsert",
    ]:
        getattr(builder, method).return_value = builder

    # Track what tables get insert() calls
    insert_calls: list[tuple[str, dict]] = []

    def table_factory(name: str) -> MagicMock:
        tbl_builder = MagicMock()
        for method in [
            "select", "like", "in_", "order", "limit",
            "lte", "gte", "eq", "neq", "update", "upsert",
        ]:
            getattr(tbl_builder, method).return_value = tbl_builder

        result = MagicMock()
        result.data = [{"id": f"mock-{name}-id"}]
        tbl_builder.execute.return_value = result

        def mock_insert(payload):
            insert_calls.append((name, payload))
            return tbl_builder
        tbl_builder.insert.side_effect = mock_insert

        return tbl_builder

    sb.table.side_effect = table_factory

    source_row = {
        **SOURCE_ROW_QUEUED,
        "start_at": None,  # date-only — no occurrence allowed
        "start_date_local": "2026-03-15",
    }

    counts: dict[str, int] = {}
    create_happening_schedule_occurrence(
        supabase=sb,
        source_row=source_row,
        run_id="test-run",
        counts=counts,
    )

    # Verify no insert was made to the occurrence table
    occurrence_inserts = [
        (name, payload) for name, payload in insert_calls
        if name == "occurrence"
    ]
    assert occurrence_inserts == [], (
        f"NULL start_at must never insert into occurrence. Got: {occurrence_inserts}"
    )

    # Verify the counter was incremented
    assert counts.get("occurrence_null_start_skipped", 0) >= 1


def test_null_start_at_skipped_in_full_merge_loop():
    """
    End-to-end: a source row with start_at=None processed through the
    merge loop (dry mode) increments occurrence_null_start_skipped
    without errors.
    """
    from src.canonicalize.merge_loop import run_merge_loop

    row_no_start_at = {
        **SOURCE_ROW_QUEUED,
        "start_at": None,
    }

    sb = _mock_supabase_returning_rows([row_no_start_at])

    counts = run_merge_loop(
        supabase=sb,
        dry_run=True,
        persist_run_stats=False,
    )

    assert counts["errors"] == 0
    assert counts["created"] == 1


# ===========================================================================
# Test 3: Offering NK get-or-create handles conflicts
# ===========================================================================

def test_offering_nk_reuses_existing():
    """
    When an offering with the same natural key already exists,
    _get_or_create_offering must reuse it (increment offering_nk_reused)
    and NOT insert a duplicate.
    """
    from src.canonicalize.merge_loop import _get_or_create_offering

    sb = MagicMock()
    insert_calls: list[dict] = []

    def table_factory(name: str) -> MagicMock:
        tbl_builder = MagicMock()
        for method in [
            "select", "like", "in_", "order", "limit",
            "lte", "gte", "eq", "neq", "update", "upsert", "is_",
        ]:
            getattr(tbl_builder, method).return_value = tbl_builder

        # SELECT finds existing offering
        result = MagicMock()
        result.data = [{"id": "existing-offering-id"}]
        tbl_builder.execute.return_value = result

        def mock_insert(payload):
            insert_calls.append(payload)
            return tbl_builder
        tbl_builder.insert.side_effect = mock_insert

        return tbl_builder

    sb.table.side_effect = table_factory

    counts: dict[str, int] = {}
    result_id = _get_or_create_offering(
        supabase=sb,
        happening_id="hap-1",
        offering_type="one_off",
        start_date="2026-03-15",
        end_date="2026-03-15",
        timezone_str="Europe/Zurich",
        run_id="run-1",
        source_happening_id="src-1",
        source_id="zurich_gemeinde",
        counts=counts,
    )

    assert result_id == "existing-offering-id"
    assert counts.get("offering_nk_reused", 0) == 1
    # Must NOT have called insert
    assert insert_calls == [], "Should reuse existing, not insert"


def test_offering_nk_conflict_retries_select():
    """
    When INSERT hits unique_violation (race condition), the function must
    retry SELECT and return the existing offering. No review created.
    """
    from src.canonicalize.merge_loop import _get_or_create_offering
    from postgrest.exceptions import APIError

    sb = MagicMock()
    select_call_count = {"n": 0}

    def table_factory(name: str) -> MagicMock:
        tbl_builder = MagicMock()
        for method in [
            "select", "like", "in_", "order", "limit",
            "lte", "gte", "eq", "neq", "update", "upsert", "is_",
        ]:
            getattr(tbl_builder, method).return_value = tbl_builder

        def _execute():
            select_call_count["n"] += 1
            result = MagicMock()
            if select_call_count["n"] == 1:
                # First SELECT: not found
                result.data = []
            else:
                # Retry SELECT after conflict: found
                result.data = [{"id": "race-winner-id"}]
            return result
        tbl_builder.execute.side_effect = _execute

        def mock_insert(payload):
            # Simulate unique violation
            raise APIError({"message": "duplicate key value violates unique constraint (23505)"})
        tbl_builder.insert.side_effect = mock_insert

        return tbl_builder

    sb.table.side_effect = table_factory

    counts: dict[str, int] = {}
    result_id = _get_or_create_offering(
        supabase=sb,
        happening_id="hap-1",
        offering_type="one_off",
        start_date="2026-03-15",
        end_date="2026-03-15",
        timezone_str="Europe/Zurich",
        run_id="run-1",
        source_happening_id="src-1",
        source_id="zurich_gemeinde",
        counts=counts,
    )

    assert result_id == "race-winner-id"
    assert counts.get("offering_nk_reused", 0) == 1


# ===========================================================================
# Test 4: Convergence job LIVE mode aborts when RPC is missing
# ===========================================================================

def test_convergence_live_mode_aborts_without_rpc():
    """
    In LIVE mode, run_convergence must call preflight_check_rpc before
    processing any groups. If the RPC is missing, it must raise
    RPCNotAvailableError immediately.
    """
    from src.jobs.converge_canonical_duplicates import (
        preflight_check_rpc,
        RPCNotAvailableError,
    )

    sb = MagicMock()

    # Simulate RPC missing (42883 = undefined_function)
    def mock_rpc(name, params):
        mock_builder = MagicMock()
        mock_builder.execute.side_effect = Exception(
            "42883: function converge_one_canonical_key(p_key) does not exist"
        )
        return mock_builder

    sb.rpc.side_effect = mock_rpc

    with pytest.raises(RPCNotAvailableError, match="ABORT"):
        preflight_check_rpc(sb, rpc_name="converge_one_canonical_key")


def test_convergence_live_mode_passes_when_rpc_exists():
    """
    When the RPC exists and responds, preflight_check_rpc must succeed
    without raising.
    """
    from src.jobs.converge_canonical_duplicates import preflight_check_rpc

    sb = MagicMock()

    # Simulate successful RPC call (no-op result)
    def mock_rpc(name, params):
        mock_builder = MagicMock()
        result = MagicMock()
        result.data = {}
        mock_builder.execute.return_value = result
        return mock_builder

    sb.rpc.side_effect = mock_rpc

    # Must not raise
    preflight_check_rpc(sb, rpc_name="converge_one_canonical_key")


def test_convergence_dry_mode_skips_rpc_preflight():
    """
    Dry mode must NOT call preflight_check_rpc (it doesn't use the RPC).
    Verify by checking that sb.rpc is never called.
    """
    from src.jobs.converge_canonical_duplicates import run_convergence

    sb = MagicMock()

    def table_factory(name: str) -> MagicMock:
        builder = MagicMock()
        for method in [
            "select", "like", "in_", "order", "limit",
            "lte", "gte", "eq", "neq", "not_", "is_",
            "update", "insert", "upsert",
        ]:
            getattr(builder, method).return_value = builder

        result = MagicMock()
        result.data = []  # empty = no duplicate groups
        builder.execute.return_value = result
        return builder

    sb.table.side_effect = table_factory

    counters = run_convergence(supabase=sb, dry_run=True)

    assert counters.groups_found == 0
    # RPC should never be called in dry mode
    sb.rpc.assert_not_called()


# ===========================================================================
# Test 5: Unresolvable offering constraint → source marked needs_review
# ===========================================================================

def test_unresolvable_offering_returns_not_fully_resolved():
    """
    When _get_or_create_offering returns None (unresolvable constraint),
    create_happening_schedule_occurrence must return fully_resolved=False,
    signaling the caller to mark the source row needs_review.
    """
    from src.canonicalize.merge_loop import create_happening_schedule_occurrence
    from postgrest.exceptions import APIError

    sb = MagicMock()
    select_count = {"n": 0}

    def table_factory(name: str) -> MagicMock:
        tbl_builder = MagicMock()
        for method in [
            "select", "like", "in_", "order", "limit",
            "lte", "gte", "eq", "neq", "update", "upsert", "is_",
        ]:
            getattr(tbl_builder, method).return_value = tbl_builder

        if name == "happening":
            result = MagicMock()
            result.data = [{"id": "hap-test-1"}]
            tbl_builder.execute.return_value = result
        elif name == "offering":
            def _execute():
                select_count["n"] += 1
                result = MagicMock()
                # Both SELECTs return empty (unresolvable)
                result.data = []
                return result
            tbl_builder.execute.side_effect = _execute

            def mock_insert(payload):
                # Simulate unique violation
                raise APIError({"message": "23505 duplicate key"})
            tbl_builder.insert.side_effect = mock_insert
        elif name == "canonicalization_reviews":
            result = MagicMock()
            result.data = [{"id": "review-1"}]
            tbl_builder.execute.return_value = result
        else:
            result = MagicMock()
            result.data = [{"id": f"mock-{name}-id"}]
            tbl_builder.execute.return_value = result

        return tbl_builder

    sb.table.side_effect = table_factory

    source_row = {
        **SOURCE_ROW_QUEUED,
        "start_at": "2026-03-15T10:00:00+01:00",
    }

    counts: dict[str, int] = {}
    happening_id, fully_resolved = create_happening_schedule_occurrence(
        supabase=sb,
        source_row=source_row,
        run_id="test-run",
        counts=counts,
    )

    assert happening_id == "hap-test-1"
    assert fully_resolved is False, (
        "Unresolvable offering conflict must signal fully_resolved=False"
    )
    assert counts.get("reviews_created", 0) == 1
