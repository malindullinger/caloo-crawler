# tests/test_heuristic_tagging.py
"""
Heuristic tagging tests:
  - Pure utility tests (infer_audience_tags, infer_topic_tags)
  - Integration tests (create path includes tags, merge path respects non-empty)
  - Idempotency tests (rerun does not create extra history rows)
"""
from __future__ import annotations

from unittest.mock import MagicMock

import pytest


# ---------------------------------------------------------------------------
# Shared fixtures
# ---------------------------------------------------------------------------

SOURCE_ROW = {
    "id": "src-tag-1",
    "source_id": "zurich_gemeinde",
    "dedupe_key": "v1|abc",
    "status": "queued",
    "title_raw": "Kinderyoga im Park",
    "description_raw": "Spass für Kinder und Familien",
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


def _mock_supabase() -> tuple[MagicMock, MagicMock]:
    sb = MagicMock()
    builder = MagicMock()
    for method in [
        "select", "like", "in_", "order", "limit",
        "lte", "gte", "eq", "neq", "update", "insert", "upsert",
    ]:
        getattr(builder, method).return_value = builder
    sb.table.return_value = builder
    result = MagicMock()
    result.data = [{"id": "mock-id-1"}]
    builder.execute.return_value = result
    return sb, builder


def _mock_supabase_per_table() -> tuple[MagicMock, dict[str, MagicMock]]:
    sb = MagicMock()
    tables: dict[str, MagicMock] = {}

    def table_factory(name: str) -> MagicMock:
        if name not in tables:
            builder = MagicMock()
            for method in [
                "select", "like", "in_", "order", "limit",
                "lte", "gte", "eq", "neq", "update", "insert", "upsert",
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
# Part 1: Pure utility tests
# ===========================================================================

class TestInferAudienceTags:

    def test_family_kids_from_title(self):
        from src.canonicalize.tagging import infer_audience_tags
        assert "family_kids" in infer_audience_tags("Kinderyoga im Park")

    def test_family_kids_from_description(self):
        from src.canonicalize.tagging import infer_audience_tags
        assert "family_kids" in infer_audience_tags(description="Spass für Kinder")

    def test_seniors_from_title(self):
        from src.canonicalize.tagging import infer_audience_tags
        assert "seniors" in infer_audience_tags("Seniorentreff im Quartier")

    def test_adults_from_title(self):
        from src.canonicalize.tagging import infer_audience_tags
        assert "adults" in infer_audience_tags("Kurs für Erwachsene")

    def test_no_match_returns_empty(self):
        from src.canonicalize.tagging import infer_audience_tags
        assert infer_audience_tags("Konzert im Stadtpark") == []

    def test_case_insensitive(self):
        from src.canonicalize.tagging import infer_audience_tags
        assert "family_kids" in infer_audience_tags("KINDERFEST")

    def test_eszett_normalized(self):
        """ß is casefold'ed to ss; 'fussball' keyword matches 'Fußball'."""
        from src.canonicalize.tagging import infer_topic_tags
        assert "sport" in infer_topic_tags("Fußball-Turnier")

    def test_umlaut_preserved(self):
        """Umlauts stay as-is in casefold; keywords match."""
        from src.canonicalize.tagging import infer_audience_tags
        # "Familientreff" contains "familientreff"
        assert "family_kids" in infer_audience_tags("Familientreff Männedorf")

    def test_compound_word_match(self):
        """German compound words: 'Kinderzirkus' matches keyword 'kinder'."""
        from src.canonicalize.tagging import infer_audience_tags
        assert "family_kids" in infer_audience_tags("Kinderzirkus")

    def test_multiple_audience_tags(self):
        from src.canonicalize.tagging import infer_audience_tags
        tags = infer_audience_tags("Kinder und Senioren willkommen")
        assert "family_kids" in tags
        assert "seniors" in tags

    def test_deterministic_sorted_output(self):
        from src.canonicalize.tagging import infer_audience_tags
        tags = infer_audience_tags("Kinder und Senioren willkommen")
        assert tags == sorted(tags)
        # Run 100 times — same result
        for _ in range(100):
            assert infer_audience_tags("Kinder und Senioren willkommen") == tags


class TestInferTopicTags:

    def test_sport(self):
        from src.canonicalize.tagging import infer_topic_tags
        assert "sport" in infer_topic_tags("Yoga für Anfänger")

    def test_culture(self):
        from src.canonicalize.tagging import infer_topic_tags
        assert "culture" in infer_topic_tags("Konzert im Park")

    def test_nature(self):
        from src.canonicalize.tagging import infer_topic_tags
        assert "nature" in infer_topic_tags("Waldspaziergang")

    def test_civic(self):
        from src.canonicalize.tagging import infer_topic_tags
        assert "civic" in infer_topic_tags("Gemeindeversammlung")

    def test_multiple_topics(self):
        from src.canonicalize.tagging import infer_topic_tags
        tags = infer_topic_tags("Yoga im Wald")
        assert "sport" in tags
        assert "nature" in tags

    def test_no_match(self):
        from src.canonicalize.tagging import infer_topic_tags
        assert infer_topic_tags("Apéro") == []

    def test_spielplatz_is_nature(self):
        """spielplatz is in both family_kids (audience) and nature (topic)."""
        from src.canonicalize.tagging import infer_audience_tags, infer_topic_tags
        assert "family_kids" in infer_audience_tags("Spielplatz-Eröffnung")
        assert "nature" in infer_topic_tags("Spielplatz-Eröffnung")


class TestPgArrayLiteral:

    def test_empty(self):
        from src.canonicalize.tagging import pg_array_literal
        assert pg_array_literal([]) == "{}"

    def test_single(self):
        from src.canonicalize.tagging import pg_array_literal
        assert pg_array_literal(["sport"]) == "{sport}"

    def test_sorted(self):
        from src.canonicalize.tagging import pg_array_literal
        assert pg_array_literal(["sport", "culture"]) == "{culture,sport}"

    def test_deterministic(self):
        from src.canonicalize.tagging import pg_array_literal
        for _ in range(100):
            assert pg_array_literal(["nature", "civic", "sport"]) == "{civic,nature,sport}"


# ===========================================================================
# Part 2: CREATE path — tags included in initial insert
# ===========================================================================

def test_create_happening_includes_audience_tags():
    """
    create_happening_schedule_occurrence should include audience_tags
    when keywords match the source row title/description.
    """
    from src.canonicalize.merge_loop import create_happening_schedule_occurrence

    sb, builder = _mock_supabase()
    create_happening_schedule_occurrence(supabase=sb, source_row=SOURCE_ROW)

    # First insert call is the happening payload
    happening_payload = builder.upsert.call_args_list[0][0][0]
    assert "audience_tags" in happening_payload
    assert "family_kids" in happening_payload["audience_tags"]


def test_create_happening_includes_topic_tags():
    """Topic tags should be inferred from title/description."""
    from src.canonicalize.merge_loop import create_happening_schedule_occurrence

    source = {**SOURCE_ROW, "title_raw": "Yoga im Wald"}
    sb, builder = _mock_supabase()
    create_happening_schedule_occurrence(supabase=sb, source_row=source)

    happening_payload = builder.upsert.call_args_list[0][0][0]
    assert "topic_tags" in happening_payload
    assert "sport" in happening_payload["topic_tags"]
    assert "nature" in happening_payload["topic_tags"]


def test_create_happening_omits_empty_tags():
    """
    When no keywords match, tags should NOT be in the payload at all
    (let DB default handle it).
    """
    from src.canonicalize.merge_loop import create_happening_schedule_occurrence

    source = {**SOURCE_ROW, "title_raw": "Apéro", "description_raw": "Ein gemütlicher Abend"}
    sb, builder = _mock_supabase()
    create_happening_schedule_occurrence(supabase=sb, source_row=source)

    happening_payload = builder.upsert.call_args_list[0][0][0]
    assert "audience_tags" not in happening_payload
    assert "topic_tags" not in happening_payload


# ===========================================================================
# Part 3: MERGE path — tags only when empty
# ===========================================================================

def test_apply_heuristic_tags_fills_empty():
    """When existing tags are empty, apply_heuristic_tags should update."""
    from src.canonicalize.merge_loop import apply_heuristic_tags

    sb, tables = _mock_supabase_per_table()

    # Mock happening with empty tags
    happening_result = MagicMock()
    happening_result.data = [{
        "id": "hap-1",
        "audience_tags": [],
        "topic_tags": [],
        "title": "Kinderyoga",
        "description": "Spass für Kinder",
    }]
    tables["happening"] = MagicMock()
    for method in ["select", "eq", "limit", "update"]:
        getattr(tables["happening"], method).return_value = tables["happening"]
    tables["happening"].execute.return_value = happening_result

    # Mock RPC for field history
    rpc_result = MagicMock()
    rpc_result.data = 1
    sb.rpc.return_value = rpc_result
    rpc_result.execute.return_value = rpc_result

    field_updates, history_inserts = apply_heuristic_tags(
        supabase=sb, happening_id="hap-1", source_row=SOURCE_ROW,
    )

    assert field_updates > 0
    # Verify update was called on happening table
    tables["happening"].update.assert_called()
    update_payload = tables["happening"].update.call_args[0][0]
    assert "audience_tags" in update_payload
    assert "family_kids" in update_payload["audience_tags"]


def test_apply_heuristic_tags_skips_nonempty():
    """When existing tags are non-empty, apply_heuristic_tags must not overwrite."""
    from src.canonicalize.merge_loop import apply_heuristic_tags

    sb, tables = _mock_supabase_per_table()

    # Mock happening with existing tags (admin-edited)
    happening_result = MagicMock()
    happening_result.data = [{
        "id": "hap-2",
        "audience_tags": ["custom_tag"],
        "topic_tags": ["custom_topic"],
        "title": "Kinderyoga",
        "description": "Spass für Kinder",
    }]
    tables["happening"] = MagicMock()
    for method in ["select", "eq", "limit", "update"]:
        getattr(tables["happening"], method).return_value = tables["happening"]
    tables["happening"].execute.return_value = happening_result

    field_updates, history_inserts = apply_heuristic_tags(
        supabase=sb, happening_id="hap-2", source_row=SOURCE_ROW,
    )

    assert field_updates == 0
    assert history_inserts == 0
    # update() should NOT have been called
    tables["happening"].update.assert_not_called()


def test_apply_heuristic_tags_partial_empty():
    """If only audience_tags is empty, fill only that one."""
    from src.canonicalize.merge_loop import apply_heuristic_tags

    sb, tables = _mock_supabase_per_table()

    happening_result = MagicMock()
    happening_result.data = [{
        "id": "hap-3",
        "audience_tags": [],
        "topic_tags": ["admin_topic"],
        "relevance_score_global": 0,
        "title": "Kinderyoga",
        "description": "Spass für Kinder",
    }]
    tables["happening"] = MagicMock()
    for method in ["select", "eq", "limit", "update"]:
        getattr(tables["happening"], method).return_value = tables["happening"]
    tables["happening"].execute.return_value = happening_result

    rpc_result = MagicMock()
    rpc_result.data = 1
    sb.rpc.return_value = rpc_result
    rpc_result.execute.return_value = rpc_result

    field_updates, _ = apply_heuristic_tags(
        supabase=sb, happening_id="hap-3", source_row=SOURCE_ROW,
    )

    assert field_updates == 2  # audience_tags + relevance_score_global
    update_payload = tables["happening"].update.call_args[0][0]
    assert "audience_tags" in update_payload
    assert "topic_tags" not in update_payload
    assert "relevance_score_global" in update_payload


def test_apply_heuristic_tags_no_keywords_match():
    """If heuristics find no tags, no update should happen."""
    from src.canonicalize.merge_loop import apply_heuristic_tags

    sb, tables = _mock_supabase_per_table()

    happening_result = MagicMock()
    happening_result.data = [{
        "id": "hap-4",
        "audience_tags": [],
        "topic_tags": [],
        "title": "Apéro",
        "description": "Gemütlich",
    }]
    tables["happening"] = MagicMock()
    for method in ["select", "eq", "limit", "update"]:
        getattr(tables["happening"], method).return_value = tables["happening"]
    tables["happening"].execute.return_value = happening_result

    source = {**SOURCE_ROW, "title_raw": "Apéro", "description_raw": "Gemütlich"}
    field_updates, history_inserts = apply_heuristic_tags(
        supabase=sb, happening_id="hap-4", source_row=source,
    )

    assert field_updates == 0
    assert history_inserts == 0
    tables["happening"].update.assert_not_called()


# ===========================================================================
# Part 4: Idempotency — change_key prevents duplicate history
# ===========================================================================

def test_change_key_deterministic_for_tags():
    """
    The change_key for a tag transition should be stable across calls.
    sha256(happening_id|field_name|old_value|new_value) with sorted tags.
    """
    from src.db.canonical_field_history import compute_change_key
    from src.canonicalize.tagging import pg_array_literal

    old = pg_array_literal([])
    new = pg_array_literal(["family_kids"])

    key1 = compute_change_key("hap-1", "audience_tags", old, new)
    key2 = compute_change_key("hap-1", "audience_tags", old, new)
    assert key1 == key2
    assert len(key1) == 64  # sha256 hex


def test_rerun_produces_same_change_key():
    """
    On re-merge, the same source row + happening produces the same
    change_key, so ON CONFLICT (change_key) DO NOTHING prevents
    duplicate history rows.
    """
    from src.db.canonical_field_history import compute_change_key
    from src.canonicalize.tagging import pg_array_literal, infer_audience_tags

    tags = infer_audience_tags("Kinderyoga im Park", "Spass für Kinder")
    old = pg_array_literal([])
    new = pg_array_literal(tags)

    # Simulate two runs
    key_run1 = compute_change_key("hap-1", "audience_tags", old, new)
    key_run2 = compute_change_key("hap-1", "audience_tags", old, new)
    assert key_run1 == key_run2


def test_editorial_priority_never_modified():
    """apply_heuristic_tags must never touch editorial_priority."""
    from src.canonicalize.merge_loop import apply_heuristic_tags

    sb, tables = _mock_supabase_per_table()

    happening_result = MagicMock()
    happening_result.data = [{
        "id": "hap-5",
        "audience_tags": [],
        "topic_tags": [],
        "title": "Kinderyoga",
        "description": "Spass",
    }]
    tables["happening"] = MagicMock()
    for method in ["select", "eq", "limit", "update"]:
        getattr(tables["happening"], method).return_value = tables["happening"]
    tables["happening"].execute.return_value = happening_result

    rpc_result = MagicMock()
    rpc_result.data = 1
    sb.rpc.return_value = rpc_result
    rpc_result.execute.return_value = rpc_result

    apply_heuristic_tags(supabase=sb, happening_id="hap-5", source_row=SOURCE_ROW)

    update_payload = tables["happening"].update.call_args[0][0]
    assert "editorial_priority" not in update_payload
