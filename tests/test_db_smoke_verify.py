"""
Tests for scripts/db_smoke_verify.py — mocked Supabase, no network.
"""
from __future__ import annotations

from unittest.mock import MagicMock

from scripts.db_smoke_verify import (
    check_merge_run_stats_columns,
    check_review_outcomes_table,
    check_rpc_field_history_batch,
    run_smoke_checks,
    print_results,
)


def _make_supabase(
    *,
    table_ok: bool = True,
    rpc_ok: bool = True,
    rpc_return: int = 0,
) -> MagicMock:
    """Build a mock Supabase client with configurable behavior."""
    sb = MagicMock()

    # --- table() chain ---
    builder = MagicMock()
    for m in ("select", "limit", "eq", "order"):
        getattr(builder, m).return_value = builder

    if table_ok:
        result = MagicMock()
        result.data = []
        builder.execute.return_value = result
    else:
        builder.execute.side_effect = Exception("PostgREST 400: column not found")

    sb.table.return_value = builder

    # --- rpc() chain ---
    rpc_builder = MagicMock()
    if rpc_ok:
        rpc_result = MagicMock()
        rpc_result.data = rpc_return
        rpc_builder.execute.return_value = rpc_result
    else:
        rpc_builder.execute.side_effect = Exception("RPC not found")

    sb.rpc.return_value = rpc_builder

    return sb


# --------------------------------------------------------------------------
# Individual check tests
# --------------------------------------------------------------------------


def test_columns_check_pass():
    sb = _make_supabase()
    passed, detail = check_merge_run_stats_columns(sb)
    assert passed is True
    assert "9 columns" in detail


def test_columns_check_fail():
    sb = _make_supabase(table_ok=False)
    passed, detail = check_merge_run_stats_columns(sb)
    assert passed is False
    assert "failed" in detail.lower()


def test_rpc_check_pass():
    sb = _make_supabase(rpc_return=0)
    passed, detail = check_rpc_field_history_batch(sb)
    assert passed is True
    assert "returned 0" in detail


def test_rpc_check_fail():
    sb = _make_supabase(rpc_ok=False)
    passed, detail = check_rpc_field_history_batch(sb)
    assert passed is False
    assert "failed" in detail.lower()


def test_review_outcomes_pass():
    sb = _make_supabase()
    passed, detail = check_review_outcomes_table(sb)
    assert passed is True
    assert "selectable" in detail.lower()


def test_review_outcomes_fail():
    sb = _make_supabase(table_ok=False)
    passed, detail = check_review_outcomes_table(sb)
    assert passed is False
    assert "failed" in detail.lower()


# --------------------------------------------------------------------------
# Aggregate tests
# --------------------------------------------------------------------------


def test_run_smoke_checks_all_pass():
    sb = _make_supabase()
    results = run_smoke_checks(sb)
    assert len(results) == 3
    assert all(passed for _, passed, _ in results)


def test_run_smoke_checks_mixed():
    sb = _make_supabase(rpc_ok=False)
    results = run_smoke_checks(sb)
    # tables pass, rpc fails
    assert results[0][1] is True   # merge_run_stats columns
    assert results[1][1] is False  # rpc
    assert results[2][1] is True   # review_outcomes table


def test_print_results_exit_code_zero_on_all_pass(capsys):
    results = [
        ("check1", True, "ok"),
        ("check2", True, "ok"),
    ]
    code = print_results(results)
    assert code == 0
    out = capsys.readouterr().out
    assert "PASS" in out
    assert "All checks passed" in out


def test_print_results_exit_code_one_on_failure(capsys):
    results = [
        ("check1", True, "ok"),
        ("check2", False, "boom"),
    ]
    code = print_results(results)
    assert code == 1
    out = capsys.readouterr().out
    assert "FAIL" in out
    assert "FAILED" in out
