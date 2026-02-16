#!/usr/bin/env python3
# scripts/recompute_relevance_scores.py
"""
Batch-recompute relevance_score_global for all happenings.

Safe to rerun — deterministic and idempotent. Reads current
audience_tags + topic_tags, computes score, writes it back.
Only updates rows where the score has actually changed.

Usage:
  # Dry run (default) — report what would change
  python -m scripts.recompute_relevance_scores

  # Live — apply updates
  python -m scripts.recompute_relevance_scores --write
"""
from __future__ import annotations

import argparse
import os
import sys
from typing import Any


def _load_dotenv_if_present() -> None:
    for fname in (".env", ".env.local"):
        if not os.path.exists(fname):
            continue
        try:
            with open(fname, "r", encoding="utf-8") as f:
                for line in f:
                    s = line.strip()
                    if not s or s.startswith("#") or "=" not in s:
                        continue
                    k, v = s.split("=", 1)
                    k = k.strip()
                    v = v.strip().strip('"').strip("'")
                    os.environ.setdefault(k, v)
        except Exception:
            continue


def _get_supabase_client() -> Any:
    _load_dotenv_if_present()

    url = os.getenv("SUPABASE_URL") or os.getenv("NEXT_PUBLIC_SUPABASE_URL")
    key = (
        os.getenv("SUPABASE_SERVICE_ROLE_KEY")
        or os.getenv("SUPABASE_SERVICE_KEY")
        or os.getenv("SUPABASE_ANON_KEY")
        or os.getenv("NEXT_PUBLIC_SUPABASE_ANON_KEY")
    )

    if not url or not key:
        raise RuntimeError(
            "Missing Supabase env vars. "
            "Set SUPABASE_URL + SUPABASE_SERVICE_ROLE_KEY in .env or shell."
        )

    from supabase import create_client
    return create_client(url, key)


def recompute_all(supabase: Any, *, dry_run: bool = True) -> dict[str, int]:
    """
    Fetch all happenings, compute relevance_score_global, update changed rows.

    Returns: {"total": N, "changed": N, "unchanged": N, "errors": N}
    """
    from src.canonicalize.scoring import compute_relevance_score

    counts = {"total": 0, "changed": 0, "unchanged": 0, "errors": 0}

    # Paginate through all happenings
    offset = 0
    batch_size = 500

    while True:
        resp = (
            supabase.table("happening")
            .select("id,audience_tags,topic_tags,relevance_score_global")
            .range(offset, offset + batch_size - 1)
            .execute()
        )

        rows = resp.data or []
        if not rows:
            break

        for row in rows:
            counts["total"] += 1
            try:
                audience = row.get("audience_tags") or []
                topic = row.get("topic_tags") or []
                current_score = row.get("relevance_score_global") or 0

                new_score = compute_relevance_score(audience, topic)

                if new_score == current_score:
                    counts["unchanged"] += 1
                    continue

                counts["changed"] += 1

                if not dry_run:
                    supabase.table("happening").update(
                        {"relevance_score_global": new_score}
                    ).eq("id", row["id"]).execute()

                print(
                    f"  {'[DRY]' if dry_run else '[UPD]'} "
                    f"id={row['id'][:8]}… "
                    f"score {current_score} → {new_score} "
                    f"audience={audience} topic={topic}"
                )

            except Exception as e:
                counts["errors"] += 1
                print(f"  [ERR] id={row.get('id', '?')}: {e!r}")

        offset += batch_size

    return counts


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Recompute relevance_score_global for all happenings."
    )
    parser.add_argument(
        "--write",
        action="store_true",
        help="Apply updates (default: dry run).",
    )
    args = parser.parse_args()

    dry_run = not args.write
    mode = "DRY RUN" if dry_run else "LIVE"

    print(f"[recompute] mode={mode}")

    supabase = _get_supabase_client()
    counts = recompute_all(supabase, dry_run=dry_run)

    print(f"\n[recompute] done mode={mode}")
    print(
        f"  total={counts['total']} "
        f"changed={counts['changed']} "
        f"unchanged={counts['unchanged']} "
        f"errors={counts['errors']}"
    )

    return 0 if counts["errors"] == 0 else 1


if __name__ == "__main__":
    raise SystemExit(main())
