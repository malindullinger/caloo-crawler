# src/canonicalize/merge_loop.py
from __future__ import annotations

import argparse
import os
import random
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Mapping, Sequence
from uuid import uuid4

import httpx
from supabase import Client, create_client

from src.canonicalize.matching import (
    CONFIDENCE_THRESHOLD,
    compute_fingerprint,
    confidence_score,
)
from src.canonicalize.reviews_supabase import (
    Candidate,
    ignore_open_reviews_for_source_row,
    mark_source_needs_review,
    write_ambiguous_match_review,
)

# ---------------------------------------------------------------------------
# Constants & helpers
# ---------------------------------------------------------------------------

NEAR_TIE_DELTA = 0.03  # prevent wrong auto-merges

# If multiple candidates hit perfect confidence, force review (avoid duplicates / wrong merges)
PERFECT_CONFIDENCE = 1.0
PERFECT_TIE_EPS = 1e-9  # float safety

# Pipeline statuses for source_happenings (Phase 1+)
STATUS_QUEUED = "queued"
STATUS_NEEDS_REVIEW = "needs_review"
STATUS_PROCESSED = "processed"
STATUS_PROCESSING = "processing"
STATUS_IGNORED = "ignored"


def execute_with_retry(rb, *, tries: int = 6, base_sleep: float = 0.5):
    """
    Supabase/PostgREST calls can occasionally drop HTTP/2 connections under load.
    Wrap .execute() with retry + exponential backoff.
    """
    last = None
    for attempt in range(tries):
        try:
            return rb.execute()
        except (
            httpx.RemoteProtocolError,
            httpx.ReadTimeout,
            httpx.ConnectError,
            httpx.WriteError,
        ) as e:
            last = e
            sleep = base_sleep * (2 ** attempt) + random.random() * 0.25
            print(
                f"[merge_loop] transient http error: {type(e).__name__} "
                f"attempt={attempt+1}/{tries} sleep={sleep:.2f}s"
            )
            time.sleep(sleep)
    raise last  # type: ignore[misc]


def source_priority_from_row(source_row: Mapping[str, Any]) -> int:
    """
    Deterministic source precedence.
    Tier A > B > C. Unknown -> 0
    """
    tier = (source_row.get("source_tier") or "").upper()
    return {"A": 300, "B": 200, "C": 100}.get(tier, 0)


def _pick_best_occurrence_for_offering(
    occurrences: list[dict[str, Any]],
    *,
    source_start_at: Any,
) -> dict[str, Any] | None:
    """
    Deterministic selection of one occurrence per offering to enrich tie-breaking.
    Rules:
      - If source_start_at exists, prefer exact match on start_at
      - Else prefer an occurrence that has a venue_id
      - Else fall back to the first occurrence (stable via sort key)
    """
    if not occurrences:
        return None

    # Stable sort: prefer non-null start_at, then non-null venue_id, then id
    def sort_key(o: dict[str, Any]) -> tuple[int, int, str]:
        return (
            0 if o.get("start_at") is not None else 1,
            0 if o.get("venue_id") is not None else 1,
            str(o.get("id") or ""),
        )

    occs = sorted(occurrences, key=sort_key)

    if source_start_at:
        for o in occs:
            if o.get("start_at") == source_start_at:
                return o

    for o in occs:
        if o.get("venue_id"):
            return o

    return occs[0]


# ---------------------------------------------------------------------------
# Match decision
# ---------------------------------------------------------------------------

@dataclass(frozen=True)
class MatchDecision:
    kind: str  # "merge" | "create" | "review"
    best_happening_id: str | None = None
    candidates: list[Candidate] | None = None


# ---------------------------------------------------------------------------
# Fetchers
# ---------------------------------------------------------------------------

def fetch_queued_source_happenings(
    supabase: Client,
    limit: int = 200,
    include_needs_review: bool = False,
) -> list[dict[str, Any]]:
    """
    Default queue = STATUS_QUEUED.
    Optionally include STATUS_NEEDS_REVIEW.

    NOTE: We also explicitly exclude STATUS_PROCESSING so we can "claim" rows
    and avoid reprocessing loops.
    """
    statuses = [STATUS_QUEUED]
    if include_needs_review:
        statuses.append(STATUS_NEEDS_REVIEW)

    resp = execute_with_retry(
        supabase.table("source_happenings")
        .select("*")
        .in_("status", statuses)
        .order("created_at", desc=False)
        .limit(limit)
    )
    return resp.data or []


def claim_source_happenings(
    supabase: Client,
    rows: Sequence[Mapping[str, Any]],
    *,
    dry_run: bool,
) -> None:
    """
    Mark fetched rows as STATUS_PROCESSING so we never refetch the same batch forever.
    In DRY RUN, do nothing.
    """
    if dry_run:
        return

    ids = [str(r["id"]) for r in rows if r.get("id") is not None]
    if not ids:
        return

    execute_with_retry(
        supabase.table("source_happenings")
        .update(
            {
                "status": STATUS_PROCESSING,
                "updated_at": datetime.now(timezone.utc).isoformat(),
            }
        )
        .in_("id", ids)
    )


def fetch_candidate_bundles(
    supabase: Client,
    source_row: Mapping[str, Any],
) -> list[dict[str, Any]]:
    """
    Returns candidate bundles:
      {
        "happening": {...},
        "offering": {...}
      }

    Narrowing rule (v1):
    - offering.start_date <= source.start_date_local <= offering.end_date
    - archived happenings are excluded
    - no inference, no guessing
    """
    start_date = source_row.get("start_date_local")
    if not start_date:
        return []

    offerings = (
        execute_with_retry(
            supabase.table("offering")
            .select("*, happening(*)")
            .lte("start_date", start_date)
            .gte("end_date", start_date)
            .limit(200)
        ).data
        or []
    )

    # -------------------------------
    # Enrichment: occurrence + venue
    # -------------------------------
    offering_ids = [o["id"] for o in offerings if o.get("id") is not None]
    occ_by_offering: dict[str, list[dict[str, Any]]] = {}

    if offering_ids:
        occ_rows = (
            execute_with_retry(
                supabase.table("occurrence")
                .select("id,offering_id,venue_id,start_at,end_at,status")
                .in_("offering_id", offering_ids)
                .limit(2000)
            ).data
            or []
        )

        for occ in occ_rows:
            oid = occ.get("offering_id")
            if not oid:
                continue
            occ_by_offering.setdefault(str(oid), []).append(occ)

    venue_name_by_id: dict[str, str] = {}
    venue_ids = {
        str(occ.get("venue_id"))
        for occs in occ_by_offering.values()
        for occ in occs
        if occ.get("venue_id") is not None
    }

    if venue_ids:
        venue_rows = (
            execute_with_retry(
                supabase.table("venue")
                .select("id,name")
                .in_("id", list(venue_ids))
                .limit(2000)
            ).data
            or []
        )
        venue_name_by_id = {
            str(v["id"]): (v.get("name") or "")
            for v in venue_rows
            if v.get("id") is not None
        }

    # -------------------------------
    # Build bundles (with enrichment)
    # -------------------------------
    bundles: list[dict[str, Any]] = []
    source_start_at = source_row.get("start_at")

    for offering in offerings:
        happening = offering.get("happening")
        if not happening:
            continue

        if happening.get("visibility_status") == "archived":
            continue

        occs = occ_by_offering.get(str(offering.get("id")), [])
        best_occ = _pick_best_occurrence_for_offering(occs, source_start_at=source_start_at)

        if best_occ:
            offering["__occ_start_at"] = best_occ.get("start_at")
            offering["__occ_end_at"] = best_occ.get("end_at")
            offering["__occ_status"] = best_occ.get("status")
            offering["__venue_id"] = best_occ.get("venue_id")

            venue_id = best_occ.get("venue_id")
            if venue_id is not None:
                venue_name = venue_name_by_id.get(str(venue_id)) or ""
                if venue_name:
                    happening["__venue_name"] = venue_name

        bundles.append({"happening": happening, "offering": offering})

    return bundles


# ---------------------------------------------------------------------------
# Decision logic
# ---------------------------------------------------------------------------

def decide_match(
    source_row: Mapping[str, Any],
    candidate_bundles: Sequence[Mapping[str, Any]],
) -> MatchDecision:
    best_by_happening: dict[str, float] = {}

    for bundle in candidate_bundles:
        happening = bundle["happening"]
        offering = bundle["offering"]

        hid = str(happening["id"])
        score = float(confidence_score(happening, offering, source_row))

        prev = best_by_happening.get(hid)
        if prev is None or score > prev:
            best_by_happening[hid] = score

    scored: list[Candidate] = [
        Candidate(happening_id=hid, confidence=conf)
        for hid, conf in best_by_happening.items()
    ]
    scored.sort(key=lambda c: c.confidence, reverse=True)

    if not scored:
        return MatchDecision(kind="create")

    top = float(scored[0].confidence)
    second = float(scored[1].confidence) if len(scored) > 1 else None

    if top < CONFIDENCE_THRESHOLD:
        return MatchDecision(kind="review", candidates=scored[:10])

    perfect = [
        c
        for c in scored
        if abs(float(c.confidence) - PERFECT_CONFIDENCE) <= PERFECT_TIE_EPS
    ]
    if len(perfect) >= 2:
        return MatchDecision(kind="review", candidates=scored[:10])

    if second is not None and (top - second) < NEAR_TIE_DELTA:
        return MatchDecision(kind="review", candidates=scored[:10])

    return MatchDecision(kind="merge", best_happening_id=scored[0].happening_id)


# ---------------------------------------------------------------------------
# Create canonical chain
# ---------------------------------------------------------------------------

def create_happening_schedule_occurrence(
    *,
    supabase: Client,
    source_row: Mapping[str, Any],
) -> str:
    """
    Create:
      1) Happening (identity)
      2) Offering (schedule)
      3) Occurrence (instance)

    Returns: happening_id
    """
    happening_payload = {
        "title": source_row.get("title_raw"),
        "description": source_row.get("description_raw"),
        "visibility_status": "draft",  # safe default
    }

    happening = execute_with_retry(
        supabase.table("happening").insert(happening_payload)
    ).data[0]
    happening_id = happening["id"]

    offering_payload = {
        "happening_id": happening_id,
        "offering_type": "one_off",
        "start_date": source_row.get("start_date_local"),
        "end_date": source_row.get("end_date_local") or source_row.get("start_date_local"),
        "timezone": source_row.get("timezone"),
    }

    offering = execute_with_retry(
        supabase.table("offering").insert(offering_payload)
    ).data[0]
    offering_id = offering["id"]

    occurrence_payload = {
        "offering_id": offering_id,
        "start_at": source_row.get("start_at"),
        "end_at": source_row.get("end_at"),
        "status": "scheduled",
    }
    occurrence_payload = {k: v for k, v in occurrence_payload.items() if v is not None}

    execute_with_retry(
        supabase.table("occurrence").insert(occurrence_payload)
    )

    return str(happening_id)


# ---------------------------------------------------------------------------
# Provenance
# ---------------------------------------------------------------------------

def link_happening_source(
    *,
    supabase: Client,
    happening_id: str,
    source_row: Mapping[str, Any],
    is_primary: bool = False,
) -> None:
    payload = {
        "happening_id": happening_id,
        "source_happening_id": source_row["id"],
        "source_priority": source_priority_from_row(source_row),
        "is_primary": bool(is_primary),
        "merged_at": datetime.now(timezone.utc).isoformat(),
    }

    execute_with_retry(
        supabase.table("happening_sources")
        .upsert(payload, on_conflict="source_happening_id")
    )


def mark_source_processed(
    *,
    supabase: Client,
    source_happening_id: str,
) -> None:
    execute_with_retry(
        supabase.table("source_happenings")
        .update(
            {
                "status": STATUS_PROCESSED,
                "updated_at": datetime.now(timezone.utc).isoformat(),
            }
        )
        .eq("id", source_happening_id)
    )


def mark_source_processing_failed(
    *,
    supabase: Client,
    source_happening_id: str,
    error_message: str,
) -> None:
    """
    If a row was claimed as PROCESSING but something crashes, we want it to
    be visible again. Put it back to NEEDS_REVIEW with an error message.
    """
    execute_with_retry(
        supabase.table("source_happenings")
        .update(
            {
                "status": STATUS_NEEDS_REVIEW,
                "error_message": (error_message or "")[:500],
                "updated_at": datetime.now(timezone.utc).isoformat(),
            }
        )
        .eq("id", source_happening_id)
    )


# ---------------------------------------------------------------------------
# Main loop
# ---------------------------------------------------------------------------

def run_merge_loop(
    *,
    supabase: Client,
    batch_size: int = 200,
    dry_run: bool = True,
    code_version: str | None = None,
    environment: str | None = None,
    include_needs_review: bool = False,
) -> dict[str, int]:
    run_id = str(uuid4())

    counts = {
        "queued": 0,   # total fetched across loops
        "merged": 0,
        "created": 0,
        "review": 0,
        "skipped": 0,
        "claimed": 0,
        "errors": 0,
    }

    while True:
        rows = fetch_queued_source_happenings(
            supabase,
            limit=batch_size,
            include_needs_review=include_needs_review,
        )

        print(f"[merge_loop] fetched_batch={len(rows)} include_needs_review={include_needs_review}")

        if not rows:
            break

        counts["queued"] += len(rows)

        # ✅ Claim rows to avoid infinite refetch loops
        try:
            claim_source_happenings(supabase, rows, dry_run=dry_run)
            counts["claimed"] += len(rows) if not dry_run else 0
        except Exception as e:
            # If claiming fails, bail out to avoid spinning forever.
            print(f"[merge_loop] ERROR claiming batch: {repr(e)}")
            counts["errors"] += 1
            break

        for source_row in rows:
            source_id = str(source_row.get("id") or "")
            try:
                fingerprint = compute_fingerprint(source_row)
                candidate_bundles = fetch_candidate_bundles(supabase, source_row)
                decision = decide_match(source_row, candidate_bundles)

                if decision.kind == "review":
                    counts["review"] += 1
                    if not dry_run:
                        mark_source_needs_review(
                            supabase=supabase,
                            source_happening_id=source_id,
                        )
                        write_ambiguous_match_review(
                            supabase=supabase,
                            run_id=run_id,
                            source_row=source_row,
                            fingerprint=fingerprint,
                            candidates=decision.candidates or [],
                            threshold=CONFIDENCE_THRESHOLD,
                            code_version=code_version,
                            environment=environment,
                        )
                    continue

                if decision.kind == "create":
                    counts["created"] += 1
                    if not dry_run:
                        happening_id = create_happening_schedule_occurrence(
                            supabase=supabase,
                            source_row=source_row,
                        )
                        link_happening_source(
                            supabase=supabase,
                            happening_id=happening_id,
                            source_row=source_row,
                            is_primary=True,
                        )
                        mark_source_processed(
                            supabase=supabase,
                            source_happening_id=source_id,
                        )
                        ignore_open_reviews_for_source_row(
                            supabase=supabase,
                            source_happening_id=source_id,
                        )
                    continue

                if decision.kind == "merge":
                    counts["merged"] += 1
                    if not dry_run:
                        if not decision.best_happening_id:
                            mark_source_needs_review(
                                supabase=supabase,
                                source_happening_id=source_id,
                            )
                            write_ambiguous_match_review(
                                supabase=supabase,
                                run_id=run_id,
                                source_row=source_row,
                                fingerprint=fingerprint,
                                candidates=decision.candidates or [],
                                threshold=CONFIDENCE_THRESHOLD,
                                code_version=code_version,
                                environment=environment,
                            )
                            continue

                        link_happening_source(
                            supabase=supabase,
                            happening_id=decision.best_happening_id,
                            source_row=source_row,
                        )
                        mark_source_processed(
                            supabase=supabase,
                            source_happening_id=source_id,
                        )
                        ignore_open_reviews_for_source_row(
                            supabase=supabase,
                            source_happening_id=source_id,
                        )
                    continue

                counts["skipped"] += 1

            except Exception as e:
                counts["errors"] += 1
                print(f"[merge_loop] ERROR row id={source_id}: {repr(e)}")
                if not dry_run and source_id:
                    # Put row back to needs_review so it remains visible.
                    try:
                        mark_source_processing_failed(
                            supabase=supabase,
                            source_happening_id=source_id,
                            error_message=repr(e),
                        )
                    except Exception as e2:
                        print(f"[merge_loop] ERROR while marking row needs_review: {repr(e2)}")
                continue

    return counts


# ---------------------------------------------------------------------------
# CLI helpers (.env loading + Supabase client)
# ---------------------------------------------------------------------------

def _load_dotenv_if_present() -> None:
    """
    Minimal .env loader (no external dependency).
    Supports lines like KEY=value. Ignores comments and blank lines.
    Does NOT support shell expansions.
    Loads .env then .env.local (and never overwrites already-exported env vars).
    """
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

    missing: list[str] = []
    if not url:
        missing.append("SUPABASE_URL (or NEXT_PUBLIC_SUPABASE_URL)")
    if not key:
        missing.append(
            "SUPABASE_SERVICE_ROLE_KEY (preferred) or SUPABASE_ANON_KEY / NEXT_PUBLIC_SUPABASE_ANON_KEY"
        )

    if missing:
        raise RuntimeError(
            "Missing Supabase env vars: "
            + ", ".join(missing)
            + ".\n"
            + "Fix: ensure .env or .env.local contains these keys, or export them in your shell.\n"
            + "Example (.env.local):\n"
            + "  SUPABASE_URL=https://xxxx.supabase.co\n"
            + "  SUPABASE_SERVICE_ROLE_KEY=xxxx\n"
        )

    return create_client(url, key)


def main() -> None:
    parser = argparse.ArgumentParser(description="Caloo canonicalization merge loop")
    parser.add_argument(
        "--mode",
        choices=["dry", "live"],
        default="dry",
        help="dry: no DB writes, live: perform writes",
    )
    parser.add_argument("--batch-size", type=int, default=200)
    parser.add_argument(
        "--include-needs-review",
        action="store_true",
        help="Also process needs_review rows (off by default).",
    )
    parser.add_argument("--code-version", type=str, default=None)
    parser.add_argument("--environment", type=str, default=None)

    args = parser.parse_args()

    supabase = _get_supabase_client()
    dry_run = args.mode != "live"

    counts = run_merge_loop(
        supabase=supabase,
        batch_size=args.batch_size,
        dry_run=dry_run,
        code_version=args.code_version,
        environment=args.environment,
        include_needs_review=bool(args.include_needs_review),
    )

    mode = "DRY RUN" if dry_run else "LIVE"
    print(
        f"[merge_loop] mode={mode} batch_size={args.batch_size} include_needs_review={args.include_needs_review}"
    )
    print(f"[merge_loop] counts={counts}")


if __name__ == "__main__":
    main()
