#!/usr/bin/env python3
# scripts/db_smoke_verify.py
"""
Read-only DB smoke verification for Phases 7–9 migrations.

Connects to Supabase and verifies:
  1. merge_run_stats has all expected columns (Phase 7 + Phase 9).
  2. RPC function insert_field_history_batch is callable (empty no-op).
  3. canonical_review_outcomes table is selectable.

Prints a PASS/FAIL summary per check. Exits non-zero on any failure.

Usage:
    python -m scripts.db_smoke_verify
"""
from __future__ import annotations

import sys
from typing import Any


def check_merge_run_stats_columns(supabase: Any) -> tuple[bool, str]:
    """
    Attempt to SELECT the Phase 7 + Phase 9 columns from merge_run_stats.
    A successful (possibly empty) response means the columns exist.
    """
    try:
        resp = (
            supabase.table("merge_run_stats")
            .select(
                "canonical_updates_count,"
                "history_rows_created,"
                "source_breakdown,"
                "stage_timings_ms,"
                "confidence_min,"
                "confidence_avg,"
                "confidence_max,"
                "confidence_histogram,"
                "source_confidence"
            )
            .limit(1)
            .execute()
        )
        # PostgREST returns 200 with data (possibly empty list) on success.
        # If a column doesn't exist, PostgREST returns a 400-level error
        # which supabase-py raises as an exception.
        _ = resp.data  # access to ensure it resolved
        return True, "All 9 columns selectable"
    except Exception as exc:
        return False, f"Column select failed: {exc}"


def check_rpc_field_history_batch(supabase: Any) -> tuple[bool, str]:
    """
    Call insert_field_history_batch with an empty changes array.
    Expected: returns 0 (no rows inserted).
    """
    try:
        resp = supabase.rpc(
            "insert_field_history_batch", {"changes": []}
        ).execute()
        count = int(resp.data) if resp.data is not None else 0
        return True, f"RPC callable, returned {count}"
    except Exception as exc:
        return False, f"RPC call failed: {exc}"


def check_review_outcomes_table(supabase: Any) -> tuple[bool, str]:
    """
    Attempt to SELECT from canonical_review_outcomes (limit 1).
    Success (even empty) means the table exists.
    """
    try:
        resp = (
            supabase.table("canonical_review_outcomes")
            .select("id")
            .limit(1)
            .execute()
        )
        _ = resp.data
        return True, "Table selectable"
    except Exception as exc:
        return False, f"Table select failed: {exc}"


CHECKS = [
    ("merge_run_stats columns", check_merge_run_stats_columns),
    ("insert_field_history_batch RPC", check_rpc_field_history_batch),
    ("canonical_review_outcomes table", check_review_outcomes_table),
]


def run_smoke_checks(supabase: Any) -> list[tuple[str, bool, str]]:
    """Run all checks and return list of (name, passed, detail)."""
    results: list[tuple[str, bool, str]] = []
    for name, fn in CHECKS:
        passed, detail = fn(supabase)
        results.append((name, passed, detail))
    return results


def print_results(results: list[tuple[str, bool, str]]) -> int:
    """Print results and return exit code (0 = all pass, 1 = failures)."""
    print("\n=== DB Smoke Verification ===\n")
    all_pass = True
    for name, passed, detail in results:
        status = "PASS" if passed else "FAIL"
        if not passed:
            all_pass = False
        print(f"  [{status}] {name}: {detail}")
    print()
    if all_pass:
        print("All checks passed.")
    else:
        print("Some checks FAILED. See above.")
    return 0 if all_pass else 1


def main() -> int:
    from scripts.canonicalize_cli import get_supabase_client

    supabase = get_supabase_client()
    results = run_smoke_checks(supabase)
    return print_results(results)


if __name__ == "__main__":
    raise SystemExit(main())
