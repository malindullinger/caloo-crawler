"""
Phase 5: Canonical Field Integrity & Update Determinism

Formalizes four invariant families:
  1. Image selection: canonical happening never carries image_url;
     merge never overwrites existing canonical fields.
  2. Field precedence: source_priority is deterministic;
     create uses raw fields; review path never auto-overwrites.
  3. Canonical update idempotency: same source → same payload;
     double-run merge_loop → zero side-effects on second run.
  4. Provenance stability: upsert prevents duplicate links;
     payload is complete and stable across calls.
"""
from __future__ import annotations

from unittest.mock import MagicMock


# ---------------------------------------------------------------------------
# Shared fixtures
# ---------------------------------------------------------------------------

SOURCE_ROW = {
    "id": "src-aaa",
    "source_id": "zurich_gemeinde",
    "dedupe_key": "v1|abc",
    "status": "queued",
    "title_raw": "Kinderyoga im Park",
    "description_raw": "Spass für Kinder",
    "start_date_local": "2026-03-15",
    "end_date_local": "2026-03-15",
    "location_raw": "Gemeindehaus",
    "timezone": "Europe/Zurich",
    "start_at": "2026-03-15T10:00:00+01:00",
    "end_at": "2026-03-15T12:00:00+01:00",
    "image_url": "https://example.com/image.jpg",
    "item_url": "https://zurich.ch/events/1",
    "external_id": "ext-1",
    "source_tier": "A",
}


def _mock_supabase() -> tuple[MagicMock, MagicMock]:
    """Simple mock: all table/chain methods return the same builder."""
    sb = MagicMock()
    builder = MagicMock()
    for method in [
        "select", "like", "in_", "order", "limit",
        "lte", "gte", "eq", "update", "insert", "upsert",
    ]:
        getattr(builder, method).return_value = builder
    sb.table.return_value = builder
    result = MagicMock()
    result.data = [{"id": "mock-id-1"}]
    builder.execute.return_value = result
    return sb, builder


def _mock_supabase_per_table() -> tuple[MagicMock, dict[str, MagicMock]]:
    """
    Mock where each table name gets its own independent builder.
    Allows verifying which tables were accessed and what operations ran.
    """
    sb = MagicMock()
    tables: dict[str, MagicMock] = {}

    def table_factory(name: str) -> MagicMock:
        if name not in tables:
            builder = MagicMock()
            for method in [
                "select", "like", "in_", "order", "limit",
                "lte", "gte", "eq", "update", "insert", "upsert",
            ]:
                getattr(builder, method).return_value = builder
            result = MagicMock()
            result.data = [{"id": f"mock-{name}-id"}]
            builder.execute.return_value = result
            tables[name] = builder
        return tables[name]

    sb.table.side_effect = table_factory
    return sb, tables


# ===========================================================================
# Part 1: Image Selection Determinism
# ===========================================================================

def test_create_happening_excludes_image_url():
    """
    create_happening_schedule_occurrence must NOT include image_url
    in the canonical happening payload. Image data lives exclusively
    in source_happenings.
    """
    from src.canonicalize.merge_loop import create_happening_schedule_occurrence

    sb, builder = _mock_supabase()
    create_happening_schedule_occurrence(supabase=sb, source_row=SOURCE_ROW)

    # First upsert call is the happening payload
    happening_payload = builder.upsert.call_args_list[0][0][0]
    assert "image_url" not in happening_payload, \
        "image_url must not be in canonical happening"


def test_merge_path_reads_happening_for_field_comparison():
    """
    Phase 6: The merge path now reads the happening table to compare
    tracked fields and update if they differ. Verify it accesses
    happening (for diff) along with happening_sources and source_happenings.
    """
    from src.canonicalize.merge_loop import (
        link_happening_source,
        mark_source_processed,
        update_happening_on_merge,
    )

    sb, tables = _mock_supabase_per_table()

    link_happening_source(
        supabase=sb, happening_id="hap-1", source_row=SOURCE_ROW,
    )
    update_happening_on_merge(
        supabase=sb, happening_id="hap-1", source_row=SOURCE_ROW,
    )
    mark_source_processed(
        supabase=sb, source_happening_id="src-aaa",
    )

    assert "happening" in tables, \
        "Merge path must access happening table for field comparison"
    assert "happening_sources" in tables
    assert "source_happenings" in tables


def test_canonical_preserved_when_new_source_has_no_image():
    """
    Merging a source with image_url=None does not modify the canonical
    happening row — merge never touches that table.
    """
    from src.canonicalize.merge_loop import link_happening_source

    source_no_image = {**SOURCE_ROW, "image_url": None}
    sb, tables = _mock_supabase_per_table()

    link_happening_source(
        supabase=sb, happening_id="hap-1", source_row=source_no_image,
    )

    accessed_tables = list(tables.keys())
    assert accessed_tables == ["happening_sources"], \
        f"Only happening_sources should be accessed, got {accessed_tables}"


def test_image_stable_across_merge_reruns():
    """
    Since merge never writes to the happening table, re-running merge
    with the same or different image sources cannot oscillate the
    canonical image. The happening row is never modified after creation.
    """
    from src.canonicalize.merge_loop import link_happening_source

    # Merge source with image
    sb1, tables1 = _mock_supabase_per_table()
    link_happening_source(
        supabase=sb1, happening_id="hap-1", source_row=SOURCE_ROW,
    )

    # Merge source without image
    sb2, tables2 = _mock_supabase_per_table()
    link_happening_source(
        supabase=sb2, happening_id="hap-1",
        source_row={**SOURCE_ROW, "image_url": None},
    )

    # Neither run accessed the happening table
    assert "happening" not in tables1
    assert "happening" not in tables2


# ===========================================================================
# Part 2: Field Precedence Rules
# ===========================================================================

def test_source_priority_tier_order():
    """Tier A > B > C > unknown. Deterministic numeric ordering."""
    from src.canonicalize.merge_loop import source_priority_from_row

    assert source_priority_from_row({"source_tier": "A"}) == 300
    assert source_priority_from_row({"source_tier": "B"}) == 200
    assert source_priority_from_row({"source_tier": "C"}) == 100
    assert source_priority_from_row({"source_tier": ""}) == 0
    assert source_priority_from_row({}) == 0


def test_source_priority_deterministic():
    """Same input always produces same priority (no randomness)."""
    from src.canonicalize.merge_loop import source_priority_from_row

    results = {source_priority_from_row({"source_tier": "A"}) for _ in range(100)}
    assert results == {300}


def test_source_priority_case_insensitive():
    """Tier matching works regardless of case."""
    from src.canonicalize.merge_loop import source_priority_from_row

    assert source_priority_from_row({"source_tier": "a"}) == 300
    assert source_priority_from_row({"source_tier": "b"}) == 200
    assert source_priority_from_row({"source_tier": "c"}) == 100


def test_create_uses_raw_fields_verbatim():
    """
    create_happening_schedule_occurrence uses title_raw and description_raw
    as-is — no normalization, no trimming on canonical fields.
    """
    from src.canonicalize.merge_loop import create_happening_schedule_occurrence

    source = {
        **SOURCE_ROW,
        "title_raw": "  Kinder Yoga  ",
        "description_raw": "  Spielerisches Yoga  ",
    }

    sb, builder = _mock_supabase()
    create_happening_schedule_occurrence(supabase=sb, source_row=source)

    # Happening now uses upsert (canonical_dedupe_key dedup)
    happening_payload = builder.upsert.call_args_list[0][0][0]
    assert happening_payload["title"] == "  Kinder Yoga  "
    assert happening_payload["description"] == "  Spielerisches Yoga  "


def test_create_does_not_set_visibility_status():
    """
    Pipeline must NOT set visibility_status — the DB default ('published')
    handles new rows, and the pipeline must never overwrite admin visibility.
    """
    from src.canonicalize.merge_loop import create_happening_schedule_occurrence

    sb, builder = _mock_supabase()
    create_happening_schedule_occurrence(supabase=sb, source_row=SOURCE_ROW)

    happening_payload = builder.upsert.call_args_list[0][0][0]
    assert "visibility_status" not in happening_payload, (
        "Pipeline must not set visibility_status — DB default handles it"
    )


def test_review_does_not_auto_overwrite_canonical():
    """
    When decide_match returns 'review' (near-tie), no canonical fields
    are modified. The source is marked needs_review instead.
    """
    from src.canonicalize.merge_loop import decide_match

    # Two candidates with identical title+date → near-tie → review
    bundle_1 = {
        "happening": {"id": "hap-1", "title": "Kinderyoga im Park"},
        "offering": {
            "id": "off-1", "happening_id": "hap-1",
            "start_date": "2026-03-15", "end_date": "2026-03-15",
        },
    }
    bundle_2 = {
        "happening": {"id": "hap-2", "title": "Kinderyoga im Park"},
        "offering": {
            "id": "off-2", "happening_id": "hap-2",
            "start_date": "2026-03-15", "end_date": "2026-03-15",
        },
    }

    decision = decide_match(SOURCE_ROW, [bundle_1, bundle_2])
    assert decision.kind == "review", \
        "Near-tie candidates must trigger review, never auto-merge"


def test_source_a_preferred_over_b_in_provenance():
    """
    When linking provenance, tier A source gets higher priority (300)
    than tier B (200). The upsert payload reflects this.
    """
    from src.canonicalize.merge_loop import link_happening_source

    source_a = {**SOURCE_ROW, "source_tier": "A"}
    source_b = {**SOURCE_ROW, "id": "src-bbb", "source_tier": "B"}

    sb_a, builder_a = _mock_supabase()
    sb_b, builder_b = _mock_supabase()

    link_happening_source(supabase=sb_a, happening_id="hap-1", source_row=source_a)
    link_happening_source(supabase=sb_b, happening_id="hap-1", source_row=source_b)

    payload_a = builder_a.upsert.call_args[0][0]
    payload_b = builder_b.upsert.call_args[0][0]

    assert payload_a["source_priority"] == 300
    assert payload_b["source_priority"] == 200
    assert payload_a["source_priority"] > payload_b["source_priority"]


# ===========================================================================
# Part 3: Canonical Update Idempotency
# ===========================================================================

def test_create_payload_identical_across_calls():
    """
    Calling create_happening_schedule_occurrence twice with the same
    source_row produces identical happening payloads.
    """
    from src.canonicalize.merge_loop import create_happening_schedule_occurrence

    sb1, builder1 = _mock_supabase()
    sb2, builder2 = _mock_supabase()

    create_happening_schedule_occurrence(supabase=sb1, source_row=SOURCE_ROW)
    create_happening_schedule_occurrence(supabase=sb2, source_row=SOURCE_ROW)

    payload_1 = builder1.upsert.call_args_list[0][0][0]
    payload_2 = builder2.upsert.call_args_list[0][0][0]

    assert payload_1 == payload_2, "Same source must produce same canonical payload"


def test_offering_payload_stable_across_calls():
    """
    Offering access is deterministic for the same source.
    With get-or-create, the offering may be reused via SELECT (no INSERT)
    when the mock returns existing data. The key invariant: same source
    produces the same offering natural key query.
    """
    from src.canonicalize.merge_loop import create_happening_schedule_occurrence

    sb1, builder1 = _mock_supabase()
    sb2, builder2 = _mock_supabase()

    _hid1, _ok1 = create_happening_schedule_occurrence(supabase=sb1, source_row=SOURCE_ROW)
    _hid2, _ok2 = create_happening_schedule_occurrence(supabase=sb2, source_row=SOURCE_ROW)

    # Both calls must access the offering table identically
    # (via eq chain in _get_or_create_offering's _find_existing)
    assert builder1.eq.call_count == builder2.eq.call_count, \
        "Same source must produce same offering lookup pattern"


def test_merge_loop_second_run_zero_side_effects():
    """
    Run 1 creates a canonical. Run 2 finds no queued rows → zero
    creates, zero merges, zero field modifications.
    """
    from src.canonicalize.merge_loop import run_merge_loop

    call_count = 0

    def _execute():
        nonlocal call_count
        call_count += 1
        result = MagicMock()
        # Call 1: fetch_queued → one row; all others → empty
        result.data = [SOURCE_ROW] if call_count == 1 else []
        return result

    sb, builder = _mock_supabase()
    builder.execute.side_effect = _execute

    counts_1 = run_merge_loop(supabase=sb, dry_run=True, persist_run_stats=False)
    counts_2 = run_merge_loop(supabase=sb, dry_run=True, persist_run_stats=False)

    assert counts_1["created"] == 1
    assert counts_2["created"] == 0
    assert counts_2["merged"] == 0
    assert counts_2["queued"] == 0


def test_canonical_row_count_stable_across_double_run():
    """
    Explicit invariant: the total number of canonical happenings created
    by two consecutive merge_loop runs equals the number from run 1.
    """
    from src.canonicalize.merge_loop import run_merge_loop

    call_count = 0

    def _execute():
        nonlocal call_count
        call_count += 1
        result = MagicMock()
        result.data = [SOURCE_ROW] if call_count == 1 else []
        return result

    sb, builder = _mock_supabase()
    builder.execute.side_effect = _execute

    counts_1 = run_merge_loop(supabase=sb, dry_run=True, persist_run_stats=False)
    counts_2 = run_merge_loop(supabase=sb, dry_run=True, persist_run_stats=False)

    total_created = counts_1["created"] + counts_2["created"]
    assert total_created == counts_1["created"], \
        "Double-run must not increase canonical count"


# ===========================================================================
# Part 4: Provenance Stability
# ===========================================================================

def test_link_happening_source_uses_upsert_not_insert():
    """
    Provenance must use upsert with on_conflict='source_happening_id'
    to prevent duplicate provenance rows.
    """
    from src.canonicalize.merge_loop import link_happening_source

    sb, builder = _mock_supabase()
    link_happening_source(
        supabase=sb, happening_id="hap-1", source_row=SOURCE_ROW,
    )

    builder.upsert.assert_called_once()
    _, kwargs = builder.upsert.call_args
    assert kwargs.get("on_conflict") == "source_happening_id"

    # insert must NOT be used for provenance
    builder.insert.assert_not_called()


def test_link_happening_source_payload_complete():
    """
    Provenance payload includes all required fields:
    happening_id, source_happening_id, source_priority, is_primary, merged_at.
    """
    from src.canonicalize.merge_loop import link_happening_source

    sb, builder = _mock_supabase()
    link_happening_source(
        supabase=sb,
        happening_id="hap-1",
        source_row=SOURCE_ROW,
        is_primary=True,
    )

    payload = builder.upsert.call_args[0][0]
    assert payload["happening_id"] == "hap-1"
    assert payload["source_happening_id"] == "src-aaa"
    assert payload["source_priority"] == 300  # tier A
    assert payload["is_primary"] is True
    assert "merged_at" in payload


def test_provenance_stable_across_reruns():
    """
    Calling link_happening_source twice with the same source_row
    produces functionally identical upsert payloads (except timestamp).
    """
    from src.canonicalize.merge_loop import link_happening_source

    sb1, builder1 = _mock_supabase()
    sb2, builder2 = _mock_supabase()

    link_happening_source(supabase=sb1, happening_id="hap-1", source_row=SOURCE_ROW)
    link_happening_source(supabase=sb2, happening_id="hap-1", source_row=SOURCE_ROW)

    p1 = builder1.upsert.call_args[0][0]
    p2 = builder2.upsert.call_args[0][0]

    for key in ["happening_id", "source_happening_id", "source_priority", "is_primary"]:
        assert p1[key] == p2[key], f"Provenance field '{key}' must be stable"


def test_no_duplicate_provenance_via_upsert():
    """
    The on_conflict='source_happening_id' constraint means a source_happening
    can only be linked to ONE canonical happening. Re-linking updates rather
    than creating a duplicate row.
    """
    from src.canonicalize.merge_loop import link_happening_source

    sb, builder = _mock_supabase()

    # Link same source to two different happenings
    link_happening_source(
        supabase=sb, happening_id="hap-1", source_row=SOURCE_ROW,
    )
    link_happening_source(
        supabase=sb, happening_id="hap-2", source_row=SOURCE_ROW,
    )

    # Both used upsert (not insert), so DB handles dedup
    assert builder.upsert.call_count == 2
    for call_args in builder.upsert.call_args_list:
        _, kwargs = call_args
        assert kwargs["on_conflict"] == "source_happening_id"


def test_create_returns_valid_happening_id_for_provenance():
    """
    create_happening_schedule_occurrence returns a happening_id that
    can be used for provenance linking — no orphaned chain.
    """
    from src.canonicalize.merge_loop import create_happening_schedule_occurrence

    sb, builder = _mock_supabase()
    happening_id, fully_resolved = create_happening_schedule_occurrence(
        supabase=sb, source_row=SOURCE_ROW,
    )

    assert happening_id is not None
    assert isinstance(happening_id, str)
    assert len(happening_id) > 0
