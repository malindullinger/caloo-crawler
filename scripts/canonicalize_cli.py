#!/usr/bin/env python3
# scripts/canonicalize_cli.py

from __future__ import annotations

import argparse
import os
from typing import Any, Optional


def _load_dotenv_if_available() -> None:
    """
    Load .env from the project root if python-dotenv is installed.
    Safe no-op if python-dotenv is not present.
    """
    try:
        from dotenv import load_dotenv  # type: ignore
    except Exception:
        return
    load_dotenv()


def get_supabase_client() -> Any:
    """
    Try to reuse the project's existing Supabase client helper if present.
    Fallback to supabase-py client from env vars.

    Expected env vars (prefer service role for backend scripts):
      - SUPABASE_URL
      - SUPABASE_SERVICE_ROLE_KEY  (preferred for crawler/admin scripts)
        OR SUPABASE_ANON_KEY       (only if RLS/policies permit your read/write)
    """
    _load_dotenv_if_available()

    # 1) Try common internal helpers (adjustable, but safe to attempt)
    candidates = [
        ("src.db.supabase", "get_supabase"),
        ("src.db.supabase", "supabase"),
        ("src.supabase", "get_supabase"),
        ("src.supabase", "supabase"),
    ]
    for module_name, attr in candidates:
        try:
            mod = __import__(module_name, fromlist=[attr])
            client = getattr(mod, attr)
            return client() if callable(client) else client
        except Exception:
            pass

    # 2) Fallback: environment variables
    url = os.getenv("SUPABASE_URL") or os.getenv("NEXT_PUBLIC_SUPABASE_URL")
    key = (
        os.getenv("SUPABASE_SERVICE_ROLE_KEY")
        or os.getenv("SUPABASE_ANON_KEY")
        or os.getenv("NEXT_PUBLIC_SUPABASE_ANON_KEY")
    )

    if not url or not key:
        raise RuntimeError(
            "Supabase client not found. Set SUPABASE_URL and SUPABASE_SERVICE_ROLE_KEY "
            "(preferred for backend scripts), or SUPABASE_ANON_KEY if RLS allows it. "
            "Alternatively provide a project helper (e.g., src/db/supabase.py)."
        )

    try:
        from supabase import create_client  # type: ignore
    except Exception as e:
        raise RuntimeError(
            "supabase-py is not installed in this environment. Install it (pip install supabase) "
            "or provide a project supabase helper."
        ) from e

    return create_client(url, key)


def main(argv: Optional[list[str]] = None) -> int:
    parser = argparse.ArgumentParser(description="Caloo canonicalization CLI (Milestone 2 helper).")
    parser.add_argument("--source-id", default=None, help="Filter to a single source_id (optional).")
    parser.add_argument("--limit", type=int, default=None, help="Limit number of events to sync (optional).")

    # Default: dry-run (safe). Use --write to allow DB writes.
    parser.add_argument(
        "--write",
        action="store_true",
        help="Write to DB (dangerous). Default is dry-run.",
    )

    args = parser.parse_args(argv)
    dry_run = not args.write

    # Import here so the file can be imported without supabase deps
    from src.canonicalize.sync import sync_to_source_happenings

    supabase = get_supabase_client()

    result = sync_to_source_happenings(
        supabase=supabase,
        source_id=args.source_id,
        dry_run=dry_run,
        limit=args.limit,
    )

    print("\n=== sync_to_source_happenings ===")
    print(f"dry_run:        {result.dry_run}")
    print(f"events_seen:    {result.events_seen}")
    print(f"upserted:       {result.upserted}")
    print(f"needs_review:   {result.needs_review}")
    print(f"errors:         {result.errors}")

    return 0 if result.errors == 0 else 2


if __name__ == "__main__":
    raise SystemExit(main())
