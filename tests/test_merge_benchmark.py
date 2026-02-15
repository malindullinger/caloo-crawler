"""
Phase 8: Smoke test for the merge benchmark harness.

Verifies:
  1. run_benchmark returns a dict with expected keys.
  2. wall_clock_ms is non-negative.
  3. All counter keys from run_merge_loop are present.
  4. Does not crash on an empty queue.
"""
from __future__ import annotations

from unittest.mock import MagicMock


EXPECTED_KEYS = {
    "wall_clock_ms",
    "queued",
    "merged",
    "created",
    "review",
    "skipped",
    "claimed",
    "errors",
    "canonical_updates",
    "history_rows",
}


def _mock_supabase_empty_queue() -> MagicMock:
    """Mock Supabase that returns an empty queue on every table query."""
    sb = MagicMock()
    builder = MagicMock()
    for method in [
        "select", "like", "in_", "order", "limit",
        "lte", "gte", "eq", "update", "insert", "upsert",
    ]:
        getattr(builder, method).return_value = builder
    result = MagicMock()
    result.data = []
    builder.execute.return_value = result
    sb.table.return_value = builder
    return sb


def test_benchmark_returns_expected_keys():
    """run_benchmark returns a dict containing all expected counter keys."""
    from scripts.merge_benchmark import run_benchmark

    sb = _mock_supabase_empty_queue()
    report = run_benchmark(sb)

    assert isinstance(report, dict)
    missing = EXPECTED_KEYS - set(report.keys())
    assert not missing, f"Missing keys: {missing}"


def test_benchmark_wall_clock_non_negative():
    """wall_clock_ms is a non-negative integer."""
    from scripts.merge_benchmark import run_benchmark

    sb = _mock_supabase_empty_queue()
    report = run_benchmark(sb)

    assert report["wall_clock_ms"] >= 0


def test_benchmark_empty_queue_zero_counters():
    """With empty queue, all counters should be zero."""
    from scripts.merge_benchmark import run_benchmark

    sb = _mock_supabase_empty_queue()
    report = run_benchmark(sb)

    assert report["queued"] == 0
    assert report["created"] == 0
    assert report["merged"] == 0
    assert report["errors"] == 0
