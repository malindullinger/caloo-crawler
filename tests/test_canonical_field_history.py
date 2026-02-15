"""
Phase 6+7: Canonical Field History — Contract Tests

Verifies:
  1. change_key determinism: same inputs → same key
  2. change_key excludes source_happening_id
  3. diff_happening_fields detects changes only when values differ
  4. diff_happening_fields skips null source values
  5. log_field_changes uses RPC for idempotent batch insert (Phase 7)
  6. update_happening_on_merge updates happening + logs history, returns counts
  7. update_happening_on_merge is a no-op when fields match, returns (0, 0)
  8. Full merge_loop with field update end-to-end
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
    "item_url": "https://zurich.ch/events/1",
    "external_id": "ext-1",
    "source_tier": "A",
}

CURRENT_HAPPENING = {
    "id": "hap-1",
    "title": "Yoga für Kinder",
    "description": "Altes Beschreibung",
    "visibility_status": "draft",
}


def _mock_supabase_per_table() -> tuple[MagicMock, dict[str, MagicMock]]:
    """Mock where each table name gets its own independent builder."""
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

    # Mock RPC for log_field_changes (Phase 7)
    rpc_builder = MagicMock()
    rpc_result = MagicMock()
    rpc_result.data = 0
    rpc_builder.execute.return_value = rpc_result
    sb.rpc.return_value = rpc_builder

    return sb, tables


# ===========================================================================
# Part 1: change_key determinism
# ===========================================================================

def test_change_key_deterministic():
    """Same inputs always produce the same change_key."""
    from src.db.canonical_field_history import compute_change_key

    key1 = compute_change_key("hap-1", "title", "old", "new")
    key2 = compute_change_key("hap-1", "title", "old", "new")

    assert key1 == key2
    assert len(key1) == 64  # sha256 hex


def test_change_key_different_inputs_different_keys():
    """Different inputs produce different keys."""
    from src.db.canonical_field_history import compute_change_key

    key_a = compute_change_key("hap-1", "title", "old", "new")
    key_b = compute_change_key("hap-1", "title", "old", "different")
    key_c = compute_change_key("hap-2", "title", "old", "new")
    key_d = compute_change_key("hap-1", "description", "old", "new")

    assert len({key_a, key_b, key_c, key_d}) == 4, "All keys must be distinct"


# ===========================================================================
# Part 2: change_key excludes source_happening_id
# ===========================================================================

def test_change_key_source_agnostic():
    """
    change_key is computed from (happening_id, field_name, old_value, new_value).
    source_happening_id is excluded — the same logical change from any source
    produces the same key.
    """
    from src.db.canonical_field_history import compute_change_key

    # Same field change, different source — must produce identical key
    key = compute_change_key("hap-1", "title", "old title", "new title")

    # The function signature doesn't take source_happening_id at all
    # so this is guaranteed by design. Verify key is stable.
    assert key == compute_change_key("hap-1", "title", "old title", "new title")


# ===========================================================================
# Part 3: diff detects changes only when values differ
# ===========================================================================

def test_diff_returns_changes_when_values_differ():
    """diff_happening_fields returns changes for fields that differ."""
    from src.db.canonical_field_history import diff_happening_fields

    current = {"title": "Old Title", "description": "Old Desc"}
    source = {"title_raw": "New Title", "description_raw": "New Desc"}

    changes = diff_happening_fields(current, source)

    assert len(changes) == 2
    field_names = {c.field_name for c in changes}
    assert field_names == {"title", "description"}

    title_change = next(c for c in changes if c.field_name == "title")
    assert title_change.old_value == "Old Title"
    assert title_change.new_value == "New Title"


def test_diff_returns_empty_when_values_match():
    """No changes when current values match source values."""
    from src.db.canonical_field_history import diff_happening_fields

    current = {"title": "Same Title", "description": "Same Desc"}
    source = {"title_raw": "Same Title", "description_raw": "Same Desc"}

    changes = diff_happening_fields(current, source)
    assert changes == []


# ===========================================================================
# Part 4: diff skips null source values
# ===========================================================================

def test_diff_skips_null_source_values():
    """
    When a source field is None, the canonical field must NOT be updated.
    Prevents overwriting good data with null.
    """
    from src.db.canonical_field_history import diff_happening_fields

    current = {"title": "Existing Title", "description": "Existing Desc"}
    source = {"title_raw": None, "description_raw": None}

    changes = diff_happening_fields(current, source)
    assert changes == [], "Null source values must not trigger changes"


def test_diff_partial_null():
    """One field null, one field changed — only the changed one returned."""
    from src.db.canonical_field_history import diff_happening_fields

    current = {"title": "Old Title", "description": "Same Desc"}
    source = {"title_raw": None, "description_raw": "New Desc"}

    changes = diff_happening_fields(current, source)
    assert len(changes) == 1
    assert changes[0].field_name == "description"


# ===========================================================================
# Part 5: log_field_changes uses RPC for idempotent batch insert
# ===========================================================================

def test_log_field_changes_calls_rpc():
    """
    Phase 7: log_field_changes must call the insert_field_history_batch
    RPC with a list of change payloads and return the insert count.
    """
    from src.db.canonical_field_history import FieldChange, log_field_changes

    sb = MagicMock()
    rpc_builder = MagicMock()
    rpc_result = MagicMock()
    rpc_result.data = 2  # RPC returns count of actual inserts
    rpc_builder.execute.return_value = rpc_result
    sb.rpc.return_value = rpc_builder

    changes = [
        FieldChange(field_name="title", old_value="old", new_value="new"),
        FieldChange(field_name="description", old_value="old desc", new_value="new desc"),
    ]

    result = log_field_changes(sb, "hap-1", "src-aaa", changes)

    # Verify RPC was called
    sb.rpc.assert_called_once()
    rpc_name, rpc_kwargs = sb.rpc.call_args[0][0], sb.rpc.call_args[0][1]
    assert rpc_name == "insert_field_history_batch"
    assert "changes" in rpc_kwargs
    assert len(rpc_kwargs["changes"]) == 2

    # Verify each payload has required fields
    for payload in rpc_kwargs["changes"]:
        assert "happening_id" in payload
        assert "source_happening_id" in payload
        assert "field_name" in payload
        assert "change_key" in payload

    # Verify return value is the RPC response
    assert result == 2


def test_log_field_changes_returns_zero_for_empty():
    """log_field_changes with empty changes list returns 0 without calling RPC."""
    from src.db.canonical_field_history import log_field_changes

    sb = MagicMock()
    result = log_field_changes(sb, "hap-1", "src-aaa", [])

    assert result == 0
    sb.rpc.assert_not_called()


# ===========================================================================
# Part 6: update_happening_on_merge updates + logs + returns counts
# ===========================================================================

def test_update_happening_on_merge_updates_and_logs():
    """
    When current happening has different field values than source,
    update_happening_on_merge must:
    1. Read the happening table
    2. Update the happening with new values
    3. Log changes via RPC
    4. Return (field_count, history_inserts)
    """
    from src.canonicalize.merge_loop import update_happening_on_merge

    sb, tables = _mock_supabase_per_table()

    # Set up happening table to return current happening with different values
    happening_builder = MagicMock()
    for method in [
        "select", "like", "in_", "order", "limit",
        "lte", "gte", "eq", "update", "insert", "upsert",
    ]:
        getattr(happening_builder, method).return_value = happening_builder
    happening_result = MagicMock()
    happening_result.data = [CURRENT_HAPPENING]
    happening_builder.execute.return_value = happening_result
    tables["happening"] = happening_builder

    # Mock RPC to return 2 (both changes inserted)
    rpc_builder = MagicMock()
    rpc_result = MagicMock()
    rpc_result.data = 2
    rpc_builder.execute.return_value = rpc_result
    sb.rpc.return_value = rpc_builder

    field_updates, history_inserts = update_happening_on_merge(
        supabase=sb, happening_id="hap-1", source_row=SOURCE_ROW,
    )

    # Verify happening was read (select + eq)
    happening_builder.select.assert_called()
    # Verify happening was updated
    happening_builder.update.assert_called_once()
    update_payload = happening_builder.update.call_args[0][0]
    assert update_payload["title"] == "Kinderyoga im Park"
    assert update_payload["description"] == "Spass für Kinder"

    # Verify RPC was called for history logging
    sb.rpc.assert_called_once()

    # Verify return values
    assert field_updates == 2  # title + description changed
    assert history_inserts == 2  # RPC returned 2


# ===========================================================================
# Part 7: update_happening_on_merge no-op when fields match
# ===========================================================================

def test_update_happening_on_merge_noop_when_same():
    """
    When current happening fields match source values, no update
    and no history logging should occur. Returns (0, 0).
    """
    from src.canonicalize.merge_loop import update_happening_on_merge

    sb, tables = _mock_supabase_per_table()

    # Current happening already has the same values as source
    same_happening = {
        "id": "hap-1",
        "title": "Kinderyoga im Park",
        "description": "Spass für Kinder",
        "visibility_status": "draft",
    }
    happening_builder = MagicMock()
    for method in [
        "select", "like", "in_", "order", "limit",
        "lte", "gte", "eq", "update", "insert", "upsert",
    ]:
        getattr(happening_builder, method).return_value = happening_builder
    happening_result = MagicMock()
    happening_result.data = [same_happening]
    happening_builder.execute.return_value = happening_result
    tables["happening"] = happening_builder

    field_updates, history_inserts = update_happening_on_merge(
        supabase=sb, happening_id="hap-1", source_row=SOURCE_ROW,
    )

    # Happening was read but NOT updated
    happening_builder.select.assert_called()
    happening_builder.update.assert_not_called()

    # No RPC call for history
    sb.rpc.assert_not_called()

    # Returns (0, 0)
    assert field_updates == 0
    assert history_inserts == 0


# ===========================================================================
# Part 8: Full merge_loop end-to-end with field update
# ===========================================================================

def test_merge_loop_merge_path_triggers_field_update():
    """
    End-to-end: run_merge_loop where a source row matches an existing
    happening with different description → merge decision.

    Simulated via execute side_effect:
      Call 1: fetch_queued → one row
      Call 2: fetch_candidate_bundles (offering query) → one matching bundle
      Call 3+: remaining queries
    """
    from src.canonicalize.merge_loop import run_merge_loop

    call_count = 0
    # Title must match source's title_raw for confidence to exceed threshold.
    # Description differs → triggers field update logging.
    existing_happening = {
        "id": "existing-hap-1",
        "title": "Kinderyoga im Park",
        "description": "Old Description",
        "visibility_status": "draft",
    }
    matching_offering = {
        "id": "off-1",
        "happening_id": "existing-hap-1",
        "start_date": "2026-03-15",
        "end_date": "2026-03-15",
        "timezone": "Europe/Zurich",
        "happening": existing_happening,
    }

    def _execute():
        nonlocal call_count
        call_count += 1
        result = MagicMock()
        if call_count == 1:
            # fetch_queued → one row
            result.data = [SOURCE_ROW]
        elif call_count == 2:
            # fetch_candidate_bundles → offerings with happening
            result.data = [matching_offering]
        elif call_count == 3:
            # occurrence query (enrichment)
            result.data = []
        else:
            # Second fetch_queued + everything else → empty
            result.data = []
        return result

    sb = MagicMock()
    builder = MagicMock()
    for method in [
        "select", "like", "in_", "order", "limit",
        "lte", "gte", "eq", "update", "insert", "upsert",
    ]:
        getattr(builder, method).return_value = builder
    builder.execute.side_effect = _execute
    sb.table.return_value = builder

    counts = run_merge_loop(supabase=sb, dry_run=True, persist_run_stats=False)

    # The source row should match the existing happening → merge (not create)
    assert counts["merged"] == 1
    assert counts["created"] == 0
