# src/jobs/converge_canonical_duplicates.py
"""
Convergence job: merge existing canonical duplicate happenings.

Groups happenings by canonical_dedupe_key, selects a deterministic winner,
repoints offering/occurrence/happening_sources rows to the winner, and
archives losers.

=== Winner selection rule (deterministic) ===

Among duplicates sharing the same canonical_dedupe_key, the winner is:
  1. Highest editorial_priority (admin intent wins)
  2. Most happening_sources links (best-connected canonical)
  3. Earliest created_at (oldest row = original)
  4. Lexicographic id (final tiebreaker for pure determinism)

=== Atomic per-key convergence ===

Each canonical_dedupe_key group is converged inside a single Postgres
transaction via the RPC `converge_one_canonical_key(p_key text)`.
This guarantees no half-merged groups ever exist.

The RPC handles:
  A) Offering natural-key collision (happening_id, offering_type, timezone, start_date, end_date):
     merge occurrences into existing winner offering, delete empty loser offering.
  B) Occurrence deduplication on (offering_id, start_at):
     skip/delete duplicates, delete NULL start_at rows (strict contract).
  C) Happening_sources conflict on (source_happening_id):
     delete loser row if winner already linked to that source.
  D) Constraint violations → canonicalization_reviews row:
     deterministic fingerprint + details payload. Never aborts the group.

=== Safety ===

  - Losers are archived (visibility_status='archived'), NOT deleted.
  - Everything repointed/deduped before archiving.
  - Dry-run mode (default) performs no writes — simulates locally.
  - Counters + per-group logging for auditability.
"""
from __future__ import annotations

import argparse
import json
import os
import sys
from dataclasses import dataclass, asdict
from typing import Any

from supabase import Client, create_client


# ---------------------------------------------------------------------------
# Counters
# ---------------------------------------------------------------------------

@dataclass
class ConvergenceCounters:
    groups_found: int = 0
    groups_converged: int = 0
    losers_archived: int = 0

    offerings_repointed: int = 0
    offering_collisions_merged: int = 0
    offerings_deleted_after_merge: int = 0

    occurrences_moved: int = 0
    occurrences_deleted_duplicate: int = 0
    occurrences_deleted_null_start: int = 0

    happening_sources_repointed: int = 0
    happening_sources_deleted_on_conflict: int = 0

    reviews_created: int = 0

    errors: int = 0
    skipped_groups: int = 0


# ---------------------------------------------------------------------------
# DB helpers
# ---------------------------------------------------------------------------

def _execute(rb: Any) -> Any:
    """Execute a Supabase query builder. Minimal wrapper."""
    return rb.execute()


# ---------------------------------------------------------------------------
# Core logic — discovery (runs in Python, read-only)
# ---------------------------------------------------------------------------

def find_duplicate_groups(supabase: Client) -> list[dict[str, Any]]:
    """
    Find canonical_dedupe_key values that appear on more than one happening.
    Returns list of {"canonical_dedupe_key": str, "rows": [happening rows]}.
    Client-side grouping: fetch non-null keys and group locally.
    """
    resp = _execute(
        supabase.table("happening")
        .select("id,canonical_dedupe_key,editorial_priority,created_at")
        .not_.is_("canonical_dedupe_key", "null")
        .neq("visibility_status", "archived")
        .order("canonical_dedupe_key")
        .limit(50000)
    )
    rows = resp.data or []

    from collections import defaultdict
    groups: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for r in rows:
        key = r.get("canonical_dedupe_key")
        if key:
            groups[key].append(r)

    return [
        {"canonical_dedupe_key": key, "rows": members}
        for key, members in groups.items()
        if len(members) > 1
    ]


def get_source_counts(supabase: Client, happening_ids: list[str]) -> dict[str, int]:
    """Count happening_sources rows per happening_id."""
    if not happening_ids:
        return {}

    resp = _execute(
        supabase.table("happening_sources")
        .select("happening_id")
        .in_("happening_id", happening_ids)
        .limit(10000)
    )
    rows = resp.data or []

    counts: dict[str, int] = {}
    for r in rows:
        hid = r.get("happening_id")
        if hid:
            counts[hid] = counts.get(hid, 0) + 1
    return counts


def select_winner(rows: list[dict[str, Any]], source_counts: dict[str, int]) -> dict[str, Any]:
    """
    Deterministic winner selection among duplicate happenings.

    Priority:
      1. Highest editorial_priority
      2. Most happening_sources links
      3. Earliest created_at
      4. Lexicographic id (final tiebreaker)
    """
    def sort_key(r: dict[str, Any]) -> tuple:
        return (
            -(r.get("editorial_priority") or 0),
            -source_counts.get(r["id"], 0),
            r.get("created_at") or "",
            r.get("id") or "",
        )

    return sorted(rows, key=sort_key)[0]


# ---------------------------------------------------------------------------
# RPC path (atomic per-key convergence — the only write path)
# ---------------------------------------------------------------------------

class RPCNotAvailableError(RuntimeError):
    """Raised when the required RPC function is not available in Postgres."""
    pass


def preflight_check_rpc(
    supabase: Client,
    *,
    rpc_name: str = "converge_one_canonical_key",
) -> None:
    """
    Fail-closed pre-flight: verify the RPC function exists and is callable
    BEFORE processing any groups. Calls the RPC with a key guaranteed to
    match zero rows ('__preflight_noop__'). If the RPC is missing or errors
    for any reason other than "no rows matched", raise RPCNotAvailableError.

    This ensures LIVE mode never silently falls back to client-side writes.
    """
    try:
        _execute(supabase.rpc(rpc_name, {"p_key": "__preflight_noop__"}))
        # RPC exists and returned successfully (empty result is fine)
        return
    except Exception as e:
        err_str = repr(e).lower()
        # 42883 = undefined_function in Postgres
        if "42883" in err_str or "function" in err_str and "does not exist" in err_str:
            raise RPCNotAvailableError(
                f"ABORT: RPC '{rpc_name}' is not available in the database. "
                f"Deploy the migration that creates it before running in LIVE mode. "
                f"Original error: {e!r}"
            ) from e
        # Any other RPC error (permissions, network, etc.) is also fatal
        raise RPCNotAvailableError(
            f"ABORT: RPC '{rpc_name}' pre-flight check failed. "
            f"Cannot proceed in LIVE mode without a working RPC. "
            f"Original error: {e!r}"
        ) from e


def converge_one_key_via_rpc(
    supabase: Client,
    *,
    canonical_dedupe_key: str,
    rpc_name: str = "converge_one_canonical_key",
) -> dict[str, Any]:
    """
    Call the Postgres RPC that atomically converges one canonical_dedupe_key
    group. The RPC runs in a single transaction: either the whole group
    converges or nothing changes.

    Returns the RPC result dict with merge counters.
    Raises on missing RPC or any DB error (caller decides what to do).
    """
    resp = _execute(
        supabase.rpc(
            rpc_name,
            {"p_key": canonical_dedupe_key},
        )
    )
    # RPC returns a JSON object with counters
    result = resp.data
    if isinstance(result, list) and len(result) == 1:
        result = result[0]
    if isinstance(result, str):
        result = json.loads(result)
    return result if isinstance(result, dict) else {"raw": result}


# ---------------------------------------------------------------------------
# Dry-run simulation helpers (read-only, no DB writes)
# ---------------------------------------------------------------------------

_OFFERING_NK_FIELDS = ["offering_type", "timezone", "start_date", "end_date"]


def _simulate_group_convergence(
    supabase: Client,
    *,
    winner_id: str,
    loser_ids: list[str],
    counters: ConvergenceCounters,
) -> None:
    """
    Dry-run simulation: reads DB to estimate what the RPC would do, but
    performs zero writes. Counts are approximate (we can't simulate the
    exact RPC transaction logic, but we can detect NK collisions).
    """
    for loser_id in loser_ids:
        # --- Offerings ---
        loser_offerings = _execute(
            supabase.table("offering")
            .select("id,offering_type,timezone,start_date,end_date")
            .eq("happening_id", loser_id)
            .limit(5000)
        ).data or []

        for off in loser_offerings:
            # Check if winner already has an offering with same NK
            rb = (
                supabase.table("offering")
                .select("id")
                .eq("happening_id", winner_id)
            )
            for f in _OFFERING_NK_FIELDS:
                if off.get(f) is None:
                    rb = rb.is_(f, "null")
                else:
                    rb = rb.eq(f, off.get(f))
            existing = (_execute(rb.limit(1)).data or [])

            if existing:
                counters.offering_collisions_merged += 1
                counters.offerings_deleted_after_merge += 1
                # Count occurrences that would be moved/deleted
                loser_occs = _execute(
                    supabase.table("occurrence")
                    .select("id,start_at")
                    .eq("offering_id", off["id"])
                    .limit(20000)
                ).data or []
                target_off_id = existing[0]["id"]
                target_start_ats = {
                    r["start_at"]
                    for r in (_execute(
                        supabase.table("occurrence")
                        .select("start_at")
                        .eq("offering_id", target_off_id)
                        .limit(20000)
                    ).data or [])
                    if r.get("start_at")
                }
                for occ in loser_occs:
                    sa = occ.get("start_at")
                    if not sa:
                        counters.occurrences_deleted_null_start += 1
                    elif sa in target_start_ats:
                        counters.occurrences_deleted_duplicate += 1
                    else:
                        counters.occurrences_moved += 1
                        target_start_ats.add(sa)
            else:
                counters.offerings_repointed += 1

        # --- Happening sources ---
        loser_hs = _execute(
            supabase.table("happening_sources")
            .select("id,source_happening_id")
            .eq("happening_id", loser_id)
            .limit(20000)
        ).data or []

        winner_source_ids = {
            r["source_happening_id"]
            for r in (_execute(
                supabase.table("happening_sources")
                .select("source_happening_id")
                .eq("happening_id", winner_id)
                .limit(20000)
            ).data or [])
            if r.get("source_happening_id")
        }

        for hs in loser_hs:
            if hs.get("source_happening_id") in winner_source_ids:
                counters.happening_sources_deleted_on_conflict += 1
            else:
                counters.happening_sources_repointed += 1
                winner_source_ids.add(hs.get("source_happening_id"))

        counters.losers_archived += 1


# ---------------------------------------------------------------------------
# Main convergence
# ---------------------------------------------------------------------------

def run_convergence(
    *,
    supabase: Client,
    dry_run: bool = True,
    rpc_name: str = "converge_one_canonical_key",
) -> ConvergenceCounters:
    """
    Main convergence loop.

    1. Find all duplicate groups by canonical_dedupe_key
    2. For each group:
       - DRY RUN:  simulate locally (read-only) to produce counts
       - LIVE:     call RPC per key (atomic transaction in Postgres)
    3. Aggregate counters from RPC results or simulation
    """
    counters = ConvergenceCounters()

    mode_label = "DRY RUN" if dry_run else "LIVE"
    print(f"[convergence] mode={mode_label} rpc={rpc_name!r}")

    # Fail-closed: in LIVE mode, verify the RPC is available BEFORE doing any work.
    # If the RPC is missing or broken, abort immediately — no client-side fallback.
    if not dry_run:
        print(f"[convergence] LIVE mode: verifying RPC '{rpc_name}' is available...")
        preflight_check_rpc(supabase, rpc_name=rpc_name)
        print(f"[convergence] RPC '{rpc_name}' pre-flight OK.")

    print("[convergence] finding duplicate groups...")

    groups = find_duplicate_groups(supabase)
    counters.groups_found = len(groups)
    print(f"[convergence] found {len(groups)} duplicate groups")

    if not groups:
        print("[convergence] no duplicates found — nothing to do")
        return counters

    # Precompute source counts for winner selection (used in dry-run simulation)
    all_ids: list[str] = []
    for g in groups:
        all_ids.extend(r["id"] for r in g["rows"])
    source_counts = get_source_counts(supabase, all_ids)

    for group in groups:
        key = group["canonical_dedupe_key"]
        rows = group["rows"]

        try:
            winner = select_winner(rows, source_counts)
            losers = [r for r in rows if r["id"] != winner["id"]]
            loser_ids = [l["id"] for l in losers]

            print(
                f"[convergence] key={key[:30]}... "
                f"winner={winner['id']} "
                f"losers={loser_ids}"
            )

            if dry_run:
                _simulate_group_convergence(
                    supabase,
                    winner_id=winner["id"],
                    loser_ids=loser_ids,
                    counters=counters,
                )
                counters.groups_converged += 1
                continue

            # LIVE: atomic convergence via RPC
            result = converge_one_key_via_rpc(
                supabase,
                canonical_dedupe_key=key,
                rpc_name=rpc_name,
            )

            # Accumulate counters from RPC result
            counters.losers_archived += int(result.get("losers_archived", 0))
            counters.offerings_repointed += int(result.get("offerings_repointed", 0))
            counters.offering_collisions_merged += int(result.get("offering_collisions_merged", 0))
            counters.offerings_deleted_after_merge += int(result.get("offerings_deleted_after_merge", 0))
            counters.occurrences_moved += int(result.get("occurrences_moved", 0))
            counters.occurrences_deleted_duplicate += int(result.get("occurrences_deleted_duplicate", 0))
            counters.occurrences_deleted_null_start += int(result.get("occurrences_deleted_null_start", 0))
            counters.happening_sources_repointed += int(result.get("happening_sources_repointed", 0))
            counters.happening_sources_deleted_on_conflict += int(result.get("happening_sources_deleted_on_conflict", 0))
            counters.reviews_created += int(result.get("reviews_created", 0))
            counters.groups_converged += 1

            print(
                f"[convergence]   RPC result: {json.dumps(result, default=str)}"
            )

        except Exception as e:
            counters.errors += 1
            print(f"[convergence] ERROR group key={key[:30]}...: {e!r}")
            continue

    print(f"[convergence] done. counters={asdict(counters)}")
    return counters


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

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


def _get_supabase_client() -> Client:
    _load_dotenv_if_present()

    url = os.getenv("SUPABASE_URL") or os.getenv("NEXT_PUBLIC_SUPABASE_URL")
    key = (
        os.getenv("SUPABASE_SERVICE_ROLE_KEY")
        or os.getenv("SUPABASE_SERVICE_KEY")
        or os.getenv("SUPABASE_ANON_KEY")
        or os.getenv("NEXT_PUBLIC_SUPABASE_ANON_KEY")
    )

    if not url or not key:
        print("ERROR: Missing SUPABASE_URL or SUPABASE_SERVICE_ROLE_KEY", file=sys.stderr)
        sys.exit(1)

    return create_client(url, key)


def main() -> None:
    parser = argparse.ArgumentParser(description="Converge canonical duplicate happenings")
    parser.add_argument(
        "--mode",
        choices=["dry", "live"],
        default="dry",
        help="dry: no DB writes (default), live: perform writes via RPC",
    )
    parser.add_argument(
        "--rpc-name",
        default="converge_one_canonical_key",
        help="RPC function name (default: converge_one_canonical_key).",
    )
    args = parser.parse_args()

    supabase = _get_supabase_client()
    dry_run = args.mode != "live"

    counters = run_convergence(
        supabase=supabase,
        dry_run=dry_run,
        rpc_name=args.rpc_name,
    )

    print("\n=== Convergence Summary ===")
    print(f"  Groups found:                    {counters.groups_found}")
    print(f"  Groups converged:                {counters.groups_converged}")
    print(f"  Losers archived:                 {counters.losers_archived}")
    print(f"  Offerings repointed:             {counters.offerings_repointed}")
    print(f"  Offering collisions merged:      {counters.offering_collisions_merged}")
    print(f"  Offerings deleted after merge:   {counters.offerings_deleted_after_merge}")
    print(f"  Occurrences moved:               {counters.occurrences_moved}")
    print(f"  Occurrences deleted (duplicate):  {counters.occurrences_deleted_duplicate}")
    print(f"  Occurrences deleted (NULL start): {counters.occurrences_deleted_null_start}")
    print(f"  Happening_sources repointed:     {counters.happening_sources_repointed}")
    print(f"  Happening_sources deleted(conf): {counters.happening_sources_deleted_on_conflict}")
    print(f"  Reviews created:                 {counters.reviews_created}")
    print(f"  Errors:                          {counters.errors}")


if __name__ == "__main__":
    main()
