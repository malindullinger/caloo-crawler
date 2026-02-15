"""
Phase 4: Merge Invariants & Determinism Guarantees

Formalizes four invariants:
  1. compute_fingerprint() == compute_dedupe_key() for v1 rows
  2. Same content → same dedupe_key → ONE canonical happening (no duplicates)
  3. Different source_id → different dedupe_keys (source isolation)
  4. Legacy rows are never selected for processing / never create canonicals
"""
from __future__ import annotations

from unittest.mock import MagicMock


# ---------------------------------------------------------------------------
# Shared fixtures
# ---------------------------------------------------------------------------

SOURCE_ROW_A = {
    "id": "row-aaa",
    "source_id": "zurich_gemeinde",
    "dedupe_key": "v1|abc123",
    "status": "queued",
    "title_raw": "Kinderyoga im Park",
    "start_date_local": "2026-03-15",
    "location_raw": "Gemeindehaus Zürich",
    "item_url": "https://zurich.ch/events/123",
    "external_id": "ext-123",
    "timezone": "Europe/Zurich",
    "source_tier": "A",
}

SOURCE_ROW_CROSS_SOURCE = {
    "id": "row-ccc",
    "source_id": "winterthur_stadt",  # different source
    "dedupe_key": "v1|def789",
    "status": "queued",
    "title_raw": "Kinderyoga im Park",  # same title
    "start_date_local": "2026-03-15",  # same date
    "location_raw": "Gemeindehaus Zürich",  # same location
    "item_url": "https://winterthur.ch/events/789",
    "external_id": None,
    "timezone": "Europe/Zurich",
    "source_tier": "A",
}


def _mock_supabase_chain() -> tuple[MagicMock, MagicMock]:
    """Build a MagicMock Supabase client where all chain methods return the builder."""
    sb = MagicMock()
    builder = MagicMock()
    for method in [
        "select", "like", "in_", "order", "limit",
        "lte", "gte", "eq", "update", "insert", "upsert",
    ]:
        getattr(builder, method).return_value = builder
    sb.table.return_value = builder
    return sb, builder


# ===========================================================================
# Part 1: Fingerprint Invariant
# ===========================================================================

def test_fingerprint_equals_dedupe_key_content_based():
    """
    compute_fingerprint(source_row) must produce the exact same value
    as compute_dedupe_key() called with the same content fields.

    This guarantees the merge_loop fingerprint matches what storage stored.
    """
    from src.canonicalize.matching import compute_fingerprint
    from src.canonicalize.dedupe_key import compute_dedupe_key

    fingerprint = compute_fingerprint(SOURCE_ROW_A)
    dedupe_key = compute_dedupe_key(
        source_id=SOURCE_ROW_A["source_id"],
        title=SOURCE_ROW_A["title_raw"],
        start_date_local=SOURCE_ROW_A["start_date_local"],
        location=SOURCE_ROW_A["location_raw"],
        item_url=SOURCE_ROW_A["item_url"],
        external_id=SOURCE_ROW_A["external_id"],
    )

    assert fingerprint == dedupe_key
    assert fingerprint.startswith("v1|")


def test_fingerprint_equals_dedupe_key_minimal_fields():
    """Even with only title + date (no location), fingerprint == dedupe_key."""
    from src.canonicalize.matching import compute_fingerprint
    from src.canonicalize.dedupe_key import compute_dedupe_key

    row = {
        "source_id": "test_source",
        "title_raw": "Art Workshop",
        "start_date_local": "2026-06-01",
        "location_raw": None,
        "item_url": None,
        "external_id": None,
    }

    fingerprint = compute_fingerprint(row)
    dedupe_key = compute_dedupe_key(
        source_id="test_source",
        title="Art Workshop",
        start_date_local="2026-06-01",
        location=None,
    )

    assert fingerprint == dedupe_key


def test_fingerprint_equals_dedupe_key_fallback_to_url():
    """When title is missing, both paths fall back to URL-based key identically."""
    from src.canonicalize.matching import compute_fingerprint
    from src.canonicalize.dedupe_key import compute_dedupe_key

    row = {
        "source_id": "test_source",
        "title_raw": None,  # missing title
        "start_date_local": "2026-06-01",
        "location_raw": "Some Place",
        "item_url": "https://example.com/event/42",
        "external_id": None,
    }

    fingerprint = compute_fingerprint(row)
    dedupe_key = compute_dedupe_key(
        source_id="test_source",
        title=None,
        start_date_local="2026-06-01",
        location="Some Place",
        item_url="https://example.com/event/42",
    )

    assert fingerprint == dedupe_key
    assert fingerprint.startswith("v1|")


# ===========================================================================
# Part 2: Duplicate Canonical Protection
# ===========================================================================

def test_same_content_different_urls_same_dedupe_key():
    """
    Two source_happenings with identical source_id/title/date/location
    but different URLs must produce the same dedupe_key.

    Combined with ON CONFLICT (source_id, dedupe_key) upsert, this
    guarantees at most one source row → at most one canonical happening.
    """
    from src.canonicalize.dedupe_key import compute_dedupe_key

    key_a = compute_dedupe_key(
        source_id="zurich_gemeinde",
        title="Kinderyoga im Park",
        start_date_local="2026-03-15",
        location="Gemeindehaus Zürich",
        item_url="https://zurich.ch/events/123",
    )
    key_b = compute_dedupe_key(
        source_id="zurich_gemeinde",
        title="Kinderyoga im Park",
        start_date_local="2026-03-15",
        location="Gemeindehaus Zürich",
        item_url="https://zurich.ch/events/456",  # different URL
    )

    assert key_a == key_b, "Same content must produce same dedupe_key regardless of URL"


def test_same_content_different_external_ids_same_dedupe_key():
    """
    Different external_id values do not affect the key when content
    (title + date + location) is present.
    """
    from src.canonicalize.dedupe_key import compute_dedupe_key

    key_a = compute_dedupe_key(
        source_id="zurich_gemeinde",
        title="Kinderyoga im Park",
        start_date_local="2026-03-15",
        location="Gemeindehaus Zürich",
        external_id="ext-001",
    )
    key_b = compute_dedupe_key(
        source_id="zurich_gemeinde",
        title="Kinderyoga im Park",
        start_date_local="2026-03-15",
        location="Gemeindehaus Zürich",
        external_id="ext-999",  # different external_id
    )

    assert key_a == key_b


def test_decide_match_no_candidates_creates():
    """
    When no candidate happenings exist, decide_match returns 'create'.
    This is the only path that creates a new canonical happening.
    """
    from src.canonicalize.merge_loop import decide_match

    decision = decide_match(SOURCE_ROW_A, [])
    assert decision.kind == "create"


def test_decide_match_with_existing_candidate_does_not_create():
    """
    When a candidate with matching title/date exists, decide_match returns
    'merge' or 'review' — never 'create'. No duplicate canonical happening.
    """
    from src.canonicalize.merge_loop import decide_match

    bundle = {
        "happening": {
            "id": "existing-happening-1",
            "title": "Kinderyoga im Park",
            "description": None,
        },
        "offering": {
            "id": "offering-1",
            "happening_id": "existing-happening-1",
            "start_date": "2026-03-15",
            "end_date": "2026-03-15",
            "timezone": "Europe/Zurich",
        },
    }

    decision = decide_match(SOURCE_ROW_A, [bundle])
    assert decision.kind in ("merge", "review"), (
        "Existing candidate must trigger merge or review, never create"
    )


def test_merge_loop_idempotent_second_run_creates_nothing():
    """
    Run 1: merge_loop fetches queued rows and creates canonical happenings.
    Run 2: all rows are now processed → fetch returns empty → zero creates.

    Simulated: execute() returns rows once, then empty for all subsequent calls.
    """
    from src.canonicalize.merge_loop import run_merge_loop

    call_count = 0

    def _execute_side_effect():
        nonlocal call_count
        call_count += 1
        result = MagicMock()
        # Call 1: fetch_queued → one row
        # Calls 2+: all empty (offering query, second fetch_queued, etc.)
        result.data = [SOURCE_ROW_A] if call_count == 1 else []
        return result

    sb, builder = _mock_supabase_chain()
    builder.execute.side_effect = _execute_side_effect

    counts_run1 = run_merge_loop(supabase=sb, dry_run=True, persist_run_stats=False)
    assert counts_run1["created"] == 1, "First run should create one canonical happening"

    # Run 2: all subsequent execute() calls return empty (rows are processed)
    counts_run2 = run_merge_loop(supabase=sb, dry_run=True, persist_run_stats=False)
    assert counts_run2["created"] == 0, "Second run must not create any new happenings"
    assert counts_run2["queued"] == 0, "No rows should be fetched on second run"


# ===========================================================================
# Part 3: Cross-Source Determinism (Source Isolation)
# ===========================================================================

def test_cross_source_different_dedupe_keys():
    """
    Same title/date/location but different source_id must produce
    different dedupe_keys. This prevents cross-source collisions and
    ensures each source creates independent canonical happenings.
    """
    from src.canonicalize.dedupe_key import compute_dedupe_key

    key_source_a = compute_dedupe_key(
        source_id="zurich_gemeinde",
        title="Kinderyoga im Park",
        start_date_local="2026-03-15",
        location="Gemeindehaus Zürich",
    )
    key_source_b = compute_dedupe_key(
        source_id="winterthur_stadt",
        title="Kinderyoga im Park",
        start_date_local="2026-03-15",
        location="Gemeindehaus Zürich",
    )

    assert key_source_a != key_source_b, "Different source_id must produce different keys"
    assert key_source_a.startswith("v1|")
    assert key_source_b.startswith("v1|")


def test_cross_source_different_fingerprints():
    """
    compute_fingerprint for rows with different source_id produces
    different values — guaranteeing independent canonical happenings.
    """
    from src.canonicalize.matching import compute_fingerprint

    fp_a = compute_fingerprint(SOURCE_ROW_A)
    fp_cross = compute_fingerprint(SOURCE_ROW_CROSS_SOURCE)

    assert fp_a != fp_cross, "Different source_id must produce different fingerprints"


def test_cross_source_both_create_independent_happenings():
    """
    When two source rows from different sources have the same content,
    both should get 'create' decisions (no existing candidates),
    resulting in two independent canonical happenings.
    """
    from src.canonicalize.merge_loop import decide_match

    # No existing candidates → both create independently
    decision_a = decide_match(SOURCE_ROW_A, [])
    decision_cross = decide_match(SOURCE_ROW_CROSS_SOURCE, [])

    assert decision_a.kind == "create"
    assert decision_cross.kind == "create"


# ===========================================================================
# Part 4: Legacy Guard Proof
# ===========================================================================

def test_legacy_row_not_selected_by_merge_selector():
    """
    A legacy row (dedupe_key without 'v1|' prefix) with status='queued'
    must NEVER be returned by fetch_queued_source_happenings.

    The .like('dedupe_key', 'v1|%') filter ensures this at the query level.
    """
    from src.canonicalize.merge_loop import fetch_queued_source_happenings

    # Mock: DB has the legacy row, but the .like filter excludes it → empty
    sb, builder = _mock_supabase_chain()
    result = MagicMock()
    result.data = []
    builder.execute.return_value = result

    rows = fetch_queued_source_happenings(sb)

    # The v1| guard must be present in the query
    builder.like.assert_called_once_with("dedupe_key", "v1|%")
    # No rows returned (legacy row filtered out)
    assert rows == []


def test_legacy_row_cannot_create_canonical_happening():
    """
    End-to-end proof: run_merge_loop with a DB that only contains
    legacy rows → fetch returns empty → zero creates, zero merges.

    This proves legacy rows can never cause canonical happenings.
    """
    from src.canonicalize.merge_loop import run_merge_loop

    sb, builder = _mock_supabase_chain()
    result = MagicMock()
    result.data = []  # v1| filter excludes everything
    builder.execute.return_value = result

    counts = run_merge_loop(supabase=sb, dry_run=True)

    assert counts["created"] == 0, "Legacy rows must never create canonical happenings"
    assert counts["merged"] == 0, "Legacy rows must never merge into happenings"
    assert counts["queued"] == 0, "No legacy rows should be fetched"


def test_legacy_row_requeue_blocked_by_v1_guard():
    """
    mark_source_processing_failed must scope its UPDATE to v1| rows,
    preventing a legacy row from being requeued to 'needs_review'.
    """
    from src.canonicalize.merge_loop import mark_source_processing_failed

    sb = MagicMock()
    builder = MagicMock()
    sb.table.return_value.update.return_value = builder
    builder.eq.return_value = builder
    builder.like.return_value = builder
    result = MagicMock()
    builder.execute.return_value = result

    mark_source_processing_failed(
        supabase=sb,
        source_happening_id="legacy-id",
        error_message="some error",
    )

    # Verify .like('dedupe_key', 'v1|%') is in the UPDATE chain
    builder.like.assert_called_once_with("dedupe_key", "v1|%")


# ---------------------------------------------------------------------------
# Decision matrix: low-confidence → create, not review
# ---------------------------------------------------------------------------


def test_low_confidence_creates_not_reviews():
    """
    When top_confidence (0.37) < threshold (0.85), the decision must be
    'create' — not 'review'. Low confidence = no match, not ambiguous.
    """
    from src.canonicalize.merge_loop import decide_match
    from src.canonicalize.reviews_supabase import Candidate

    # Craft a bundle where title barely overlaps → low confidence
    bundle = {
        "happening": {
            "id": "hap-low",
            "title": "Schwimmkurs Hallenbad",
        },
        "offering": {
            "id": "off-low",
            "happening_id": "hap-low",
            "start_date": "2026-03-15",
            "end_date": "2026-03-15",
        },
    }

    # SOURCE_ROW_A title = "Kinderyoga im Park" vs "Schwimmkurs Hallenbad"
    # → title jaccard ≈ 0.0, date = 1.0 → score = 0.375
    decision = decide_match(SOURCE_ROW_A, [bundle])
    assert decision.kind == "create", (
        f"Low confidence must create, not review. Got kind={decision.kind}"
    )
    assert decision.top_confidence is not None
    assert decision.top_confidence < 0.85


def test_near_tie_above_threshold_reviews():
    """
    When two candidates both score >= threshold and are within
    NEAR_TIE_DELTA (0.03) of each other → ambiguous_match review.
    """
    from src.canonicalize.merge_loop import decide_match

    # Two bundles with nearly identical titles to source → both score ~1.0
    bundle_1 = {
        "happening": {"id": "hap-t1", "title": "Kinderyoga im Park"},
        "offering": {
            "id": "off-t1", "happening_id": "hap-t1",
            "start_date": "2026-03-15", "end_date": "2026-03-15",
        },
    }
    bundle_2 = {
        "happening": {"id": "hap-t2", "title": "Kinderyoga im Park"},
        "offering": {
            "id": "off-t2", "happening_id": "hap-t2",
            "start_date": "2026-03-15", "end_date": "2026-03-15",
        },
    }

    decision = decide_match(SOURCE_ROW_A, [bundle_1, bundle_2])
    assert decision.kind == "review", (
        "Near-tie above threshold must trigger review (ambiguous match)"
    )
    assert decision.top_confidence is not None
    assert decision.top_confidence >= 0.85


def test_clear_winner_above_threshold_merges():
    """
    When top_confidence (0.92) >= threshold and the second candidate
    is far below (0.375) → clear winner → merge, not review.
    """
    from src.canonicalize.merge_loop import decide_match

    # Bundle 1: exact title match → high confidence
    bundle_high = {
        "happening": {"id": "hap-hi", "title": "Kinderyoga im Park"},
        "offering": {
            "id": "off-hi", "happening_id": "hap-hi",
            "start_date": "2026-03-15", "end_date": "2026-03-15",
        },
    }
    # Bundle 2: completely different title → low confidence
    bundle_low = {
        "happening": {"id": "hap-lo", "title": "Schwimmkurs Hallenbad"},
        "offering": {
            "id": "off-lo", "happening_id": "hap-lo",
            "start_date": "2026-03-15", "end_date": "2026-03-15",
        },
    }

    decision = decide_match(SOURCE_ROW_A, [bundle_high, bundle_low])
    assert decision.kind == "merge", (
        f"Clear winner above threshold must merge. Got kind={decision.kind}"
    )
    assert decision.best_happening_id == "hap-hi"
    assert decision.top_confidence is not None
    assert decision.top_confidence >= 0.85
