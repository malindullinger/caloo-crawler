#!/usr/bin/env python3
# scripts/merge_benchmark.py
"""
Merge loop performance baseline — read-only, no DB writes.

Runs run_merge_loop in dry-run mode (no DB mutations, no stats persistence)
and prints a structured JSON report with wall-clock timing and all counters.

Usage:
    python -m scripts.merge_benchmark
"""
from __future__ import annotations

import json
import sys
import time
from typing import Any


def run_benchmark(supabase: Any) -> dict[str, int]:
    """
    Execute a dry-run merge loop and return timing + counters.

    The supabase client is passed in (not created here) so tests
    can inject a mock without touching env vars.
    """
    from src.canonicalize.merge_loop import run_merge_loop

    t0 = time.monotonic()
    counts = run_merge_loop(
        supabase=supabase,
        dry_run=True,
        persist_run_stats=False,
    )
    wall_ms = int((time.monotonic() - t0) * 1000)
    return {"wall_clock_ms": wall_ms, **counts}


def main() -> int:
    from scripts.canonicalize_cli import get_supabase_client

    supabase = get_supabase_client()
    report = run_benchmark(supabase)
    print(json.dumps(report, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
