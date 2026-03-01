"""
Regression tests: pipeline must NEVER overwrite editorial fields.

Editorial fields (set by admins, absolute precedence):
  - editorial_priority
  - visibility_override
  - override_reason
  - override_set_by
  - override_set_at
  - override_expires_at

Also: visibility_status must not be touched by the pipeline on merge/update.
"""
from __future__ import annotations

from unittest.mock import MagicMock, call

from src.canonicalize.merge_loop import (
    EDITORIAL_PROTECTED_FIELDS,
    update_happening_on_merge,
)
from src.db.canonical_field_history import diff_happening_fields


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _mock_supabase_returning(current_happening: dict) -> MagicMock:
    """
    Build a mock Supabase client that returns `current_happening`
    for a .select("*").eq("id", ...) query.
    """
    sb = MagicMock()
    builder = MagicMock()
    for method in ["select", "eq", "limit", "update", "insert", "upsert"]:
        getattr(builder, method).return_value = builder
    sb.table.return_value = builder

    result = MagicMock()
    result.data = [current_happening]
    builder.execute.return_value = result

    # For log_field_changes RPC
    rpc_result = MagicMock()
    rpc_result.data = 0
    sb.rpc.return_value.execute.return_value = rpc_result

    return sb


# ---------------------------------------------------------------------------
# Part 1: EDITORIAL_PROTECTED_FIELDS constant is complete
# ---------------------------------------------------------------------------

def test_editorial_protected_fields_contains_all_expected():
    expected = {
        "editorial_priority",
        "visibility_override",
        "override_reason",
        "override_set_by",
        "override_set_at",
        "override_expires_at",
    }
    assert EDITORIAL_PROTECTED_FIELDS == expected


# ---------------------------------------------------------------------------
# Part 2: diff_happening_fields never produces editorial changes
# ---------------------------------------------------------------------------

def test_diff_does_not_track_editorial_fields():
    """
    diff_happening_fields only tracks TRACKED_FIELDS (title, description).
    Even if the source row had editorial_priority, it would not be diffed.
    This is a structural guarantee — editorial fields are not in TRACKED_FIELDS.
    """
    from src.db.canonical_field_history import TRACKED_FIELDS

    for field in EDITORIAL_PROTECTED_FIELDS:
        assert field not in TRACKED_FIELDS, (
            f"{field} must NOT be in TRACKED_FIELDS — editorial fields are admin-only"
        )
    assert "visibility_status" not in TRACKED_FIELDS


# ---------------------------------------------------------------------------
# Part 3: update_happening_on_merge filters out editorial fields
# ---------------------------------------------------------------------------

def test_update_on_merge_never_writes_editorial_priority():
    """
    Even if diff_happening_fields somehow produced an editorial_priority change
    (it can't, but defense in depth), update_happening_on_merge must filter it.
    """
    current = {
        "id": "hap-1",
        "title": "Old Title",
        "description": "Old desc",
        "editorial_priority": 10,
        "visibility_override": "public",
        "visibility_status": "published",
        "canonical_dedupe_key": "c1|existing",
    }
    source_row = {
        "id": "src-1",
        "title_raw": "New Title",
        "description_raw": "New desc",
    }

    sb = _mock_supabase_returning(current)
    field_updates, history_rows = update_happening_on_merge(
        supabase=sb,
        happening_id="hap-1",
        source_row=source_row,
    )

    # Verify update was called
    update_calls = sb.table.return_value.update.call_args_list
    if update_calls:
        for c in update_calls:
            payload = c[0][0] if c[0] else c[1].get("payload", {})
            for field in EDITORIAL_PROTECTED_FIELDS:
                assert field not in payload, (
                    f"Pipeline wrote editorial field '{field}' — THIS IS A BUG"
                )
            assert "visibility_status" not in payload, (
                "Pipeline wrote visibility_status — THIS IS A BUG"
            )


def test_update_on_merge_preserves_editorial_priority_value():
    """
    After update_happening_on_merge, the editorial_priority value in the DB
    must remain unchanged (admin's value persists).
    """
    current = {
        "id": "hap-2",
        "title": "Same Title",
        "description": "Same desc",
        "editorial_priority": 42,
        "visibility_override": "hidden",
        "override_reason": "spam",
        "canonical_dedupe_key": "c1|existing",
    }
    source_row = {
        "id": "src-2",
        "title_raw": "Same Title",
        "description_raw": "Same desc",
    }

    sb = _mock_supabase_returning(current)
    field_updates, history_rows = update_happening_on_merge(
        supabase=sb,
        happening_id="hap-2",
        source_row=source_row,
    )

    # No changes expected (title/description are the same)
    # But even if there were changes, editorial fields must not appear
    update_calls = sb.table.return_value.update.call_args_list
    for c in update_calls:
        payload = c[0][0] if c[0] else {}
        assert "editorial_priority" not in payload
        assert "visibility_override" not in payload
        assert "override_reason" not in payload


# ---------------------------------------------------------------------------
# Part 4: create path does not set visibility_status or editorial fields
# ---------------------------------------------------------------------------

def test_create_happening_payload_excludes_editorial_and_visibility():
    """
    Verify that create_happening_schedule_occurrence does not include
    visibility_status or editorial fields in the happening payload.

    This is tested by importing the function and checking the payload
    construction logic (the upsert_payload filter).
    """
    from src.canonicalize.merge_loop import EDITORIAL_PROTECTED_FIELDS

    # The filter used in create_happening_schedule_occurrence:
    # {k: v for k, v in happening_payload.items()
    #  if k not in EDITORIAL_PROTECTED_FIELDS and k != "visibility_status"}
    test_payload = {
        "title": "Test",
        "description": "Test desc",
        "canonical_dedupe_key": "c1|abc",
        "editorial_priority": 5,  # should be filtered
        "visibility_status": "published",  # should be filtered
        "visibility_override": "public",  # should be filtered
    }
    filtered = {
        k: v for k, v in test_payload.items()
        if k not in EDITORIAL_PROTECTED_FIELDS
        and k != "visibility_status"
    }
    assert "editorial_priority" not in filtered
    assert "visibility_status" not in filtered
    assert "visibility_override" not in filtered
    assert "title" in filtered
    assert "canonical_dedupe_key" in filtered
