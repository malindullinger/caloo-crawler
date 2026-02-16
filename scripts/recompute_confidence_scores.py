#!/usr/bin/env python3
# scripts/recompute_confidence_scores.py
"""
Batch-recompute confidence_score for all happenings.

For each happening, fetches the best source_happenings row (via
happening_sources, ordered by is_primary DESC, source_priority,
merged_at DESC — same as best_source CTE in views) and computes
the data-quality confidence score.

Safe to rerun — deterministic and idempotent. Only updates rows
where the score has actually changed.

Usage:
  # Dry run (default) — report what would change
  python -m scripts.recompute_confidence_scores

  # Live — apply updates
  python -m scripts.recompute_confidence_scores --write
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


def _fetch_best_source_for_happening(
    supabase: Any,
    happening_id: str,
) -> dict[str, Any] | None:
    """
    Fetch the best source_happenings row for a happening.

    Uses the same priority as the best_source CTE:
      is_primary DESC, source_priority, merged_at DESC
    """
    resp = (
        supabase.table("happening_sources")
        .select(
            "source_happening_id,"
            "is_primary,"
            "source_priority,"
            "merged_at,"
            "source_happenings!inner("
            "  source_tier,"
            "  date_precision,"
            "  image_url,"
            "  item_url,"
            "  description_raw,"
            "  timezone,"
            "  extraction_method"
            ")"
        )
        .eq("happening_id", happening_id)
        .order("is_primary", desc=True)
        .order("source_priority", desc=False)
        .order("merged_at", desc=True)
        .limit(1)
        .execute()
    )

    rows = resp.data or []
    if not rows:
        return None

    # Flatten the nested source_happenings join
    row = rows[0]
    sh = row.get("source_happenings") or {}
    return {
        "source_tier": sh.get("source_tier"),
        "date_precision": sh.get("date_precision"),
        "image_url": sh.get("image_url"),
        "item_url": sh.get("item_url"),
        "description_raw": sh.get("description_raw"),
        "timezone": sh.get("timezone"),
        "extraction_method": sh.get("extraction_method"),
    }


def recompute_all(supabase: Any, *, dry_run: bool = True) -> dict[str, int]:
    """
    Fetch all happenings, compute confidence_score, update changed rows.

    Processes in deterministic order (by happening_id).

    Returns: {"total": N, "changed": N, "unchanged": N, "errors": N}
    """
    from src.canonicalize.confidence import compute_confidence_score

    counts = {"total": 0, "changed": 0, "unchanged": 0, "errors": 0}

    offset = 0
    batch_size = 500

    while True:
        resp = (
            supabase.table("happening")
            .select("id,confidence_score,description")
            .order("id", desc=False)
            .range(offset, offset + batch_size - 1)
            .execute()
        )

        rows = resp.data or []
        if not rows:
            break

        for row in rows:
            counts["total"] += 1
            hid = row["id"]
            try:
                current_score = row.get("confidence_score", 100)

                best_source = _fetch_best_source_for_happening(supabase, hid)

                if best_source is None:
                    # No source linked — score stays at default (100)
                    # or could be penalized; for now, skip
                    counts["unchanged"] += 1
                    continue

                happening_desc = row.get("description")
                description = happening_desc or best_source.get("description_raw")

                new_score = compute_confidence_score(
                    source_tier=best_source.get("source_tier"),
                    date_precision=best_source.get("date_precision"),
                    image_url=best_source.get("image_url"),
                    description=description,
                    canonical_url=best_source.get("item_url"),
                    timezone=best_source.get("timezone"),
                    extraction_method=best_source.get("extraction_method"),
                )

                if new_score == current_score:
                    counts["unchanged"] += 1
                    continue

                counts["changed"] += 1

                if not dry_run:
                    supabase.table("happening").update(
                        {"confidence_score": new_score}
                    ).eq("id", hid).execute()

                print(
                    f"  {'[DRY]' if dry_run else '[UPD]'} "
                    f"id={hid[:8]}… "
                    f"score {current_score} → {new_score}"
                )

            except Exception as e:
                counts["errors"] += 1
                print(f"  [ERR] id={hid[:8] if hid else '?'}: {e!r}")

        offset += batch_size

    return counts


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Recompute confidence_score for all happenings."
    )
    parser.add_argument(
        "--write",
        action="store_true",
        help="Apply updates (default: dry run).",
    )
    args = parser.parse_args()

    dry_run = not args.write
    mode = "DRY RUN" if dry_run else "LIVE"

    print(f"[recompute_confidence] mode={mode}")

    supabase = _get_supabase_client()
    counts = recompute_all(supabase, dry_run=dry_run)

    print(f"\n[recompute_confidence] done mode={mode}")
    print(
        f"  total={counts['total']} "
        f"changed={counts['changed']} "
        f"unchanged={counts['unchanged']} "
        f"errors={counts['errors']}"
    )

    return 0 if counts["errors"] == 0 else 1


if __name__ == "__main__":
    raise SystemExit(main())
