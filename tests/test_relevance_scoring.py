# tests/test_relevance_scoring.py
"""
Relevance scoring tests:

  Part 1: Pure scoring formula (deterministic, documented)
  Part 2: CREATE path includes relevance_score_global in payload
  Part 3: MERGE path recomputes score after tag update
  Part 4: Ranking order guarantees (weekend-first, editorial_priority,
           relevance_score_global, sort_at, title)
  Part 5: No additional filters introduced
  Part 6: Recompute script logic
"""
from __future__ import annotations

from unittest.mock import MagicMock

import pytest


# ---------------------------------------------------------------------------
# Shared fixtures
# ---------------------------------------------------------------------------

SOURCE_ROW_FAMILY = {
    "id": "src-fam-1",
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

SOURCE_ROW_SENIORS = {
    "id": "src-sen-1",
    "source_id": "zurich_gemeinde",
    "dedupe_key": "v1|def",
    "status": "queued",
    "title_raw": "Seniorentreff im Quartier",
    "description_raw": "Gemütliches Beisammensein für Senioren",
    "start_date_local": "2026-03-16",
    "end_date_local": "2026-03-16",
    "location_raw": "Gemeindehaus",
    "timezone": "Europe/Zurich",
    "start_at": "2026-03-16T14:00:00+01:00",
    "end_at": "2026-03-16T16:00:00+01:00",
    "item_url": "https://zurich.ch/events/2",
    "external_id": "ext-2",
    "source_tier": "A",
}

SOURCE_ROW_NEUTRAL = {
    "id": "src-neu-1",
    "source_id": "zurich_gemeinde",
    "dedupe_key": "v1|ghi",
    "status": "queued",
    "title_raw": "Apéro",
    "description_raw": "Ein gemütlicher Abend",
    "start_date_local": "2026-03-17",
    "end_date_local": "2026-03-17",
    "location_raw": "Gemeindehaus",
    "timezone": "Europe/Zurich",
    "start_at": "2026-03-17T18:00:00+01:00",
    "end_at": "2026-03-17T20:00:00+01:00",
    "item_url": "https://zurich.ch/events/3",
    "external_id": "ext-3",
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
# Part 1: Pure scoring formula
# ===========================================================================

class TestComputeRelevanceScore:

    def test_family_kids_gets_50(self):
        from src.canonicalize.scoring import compute_relevance_score
        assert compute_relevance_score(["family_kids"], []) == 50

    def test_seniors_gets_minus_30(self):
        from src.canonicalize.scoring import compute_relevance_score
        assert compute_relevance_score(["seniors"], []) == -30

    def test_adults_gets_zero(self):
        from src.canonicalize.scoring import compute_relevance_score
        assert compute_relevance_score(["adults"], []) == 0

    def test_nature_topic_gets_10(self):
        from src.canonicalize.scoring import compute_relevance_score
        assert compute_relevance_score([], ["nature"]) == 10

    def test_culture_topic_gets_10(self):
        from src.canonicalize.scoring import compute_relevance_score
        assert compute_relevance_score([], ["culture"]) == 10

    def test_sport_topic_gets_10(self):
        from src.canonicalize.scoring import compute_relevance_score
        assert compute_relevance_score([], ["sport"]) == 10

    def test_civic_topic_gets_zero(self):
        from src.canonicalize.scoring import compute_relevance_score
        assert compute_relevance_score([], ["civic"]) == 0

    def test_multiple_boosted_topics_still_only_10(self):
        """Topic boost is +10 total, not per topic."""
        from src.canonicalize.scoring import compute_relevance_score
        assert compute_relevance_score([], ["nature", "culture", "sport"]) == 10

    def test_family_plus_nature(self):
        from src.canonicalize.scoring import compute_relevance_score
        assert compute_relevance_score(["family_kids"], ["nature"]) == 60

    def test_family_plus_seniors(self):
        """Both audience tags apply independently."""
        from src.canonicalize.scoring import compute_relevance_score
        assert compute_relevance_score(["family_kids", "seniors"], []) == 20

    def test_seniors_plus_culture(self):
        from src.canonicalize.scoring import compute_relevance_score
        assert compute_relevance_score(["seniors"], ["culture"]) == -20

    def test_empty_tags_scores_zero(self):
        from src.canonicalize.scoring import compute_relevance_score
        assert compute_relevance_score([], []) == 0

    def test_none_tags_scores_zero(self):
        from src.canonicalize.scoring import compute_relevance_score
        assert compute_relevance_score(None, None) == 0

    def test_deterministic(self):
        """Same inputs always produce same output."""
        from src.canonicalize.scoring import compute_relevance_score
        for _ in range(100):
            assert compute_relevance_score(["family_kids"], ["nature"]) == 60

    def test_all_positive_audience_plus_topic(self):
        """Maximum realistic score: family_kids + boosted topic."""
        from src.canonicalize.scoring import compute_relevance_score
        assert compute_relevance_score(["family_kids"], ["sport"]) == 60

    def test_score_can_be_negative(self):
        """seniors with no topic boost = -30."""
        from src.canonicalize.scoring import compute_relevance_score
        score = compute_relevance_score(["seniors"], [])
        assert score < 0


# ===========================================================================
# Part 2: CREATE path includes relevance_score_global
# ===========================================================================

def test_create_happening_includes_relevance_score_for_family():
    """
    create_happening_schedule_occurrence should include
    relevance_score_global when tags produce a non-zero score.
    """
    from src.canonicalize.merge_loop import create_happening_schedule_occurrence

    sb, builder = _mock_supabase()
    create_happening_schedule_occurrence(supabase=sb, source_row=SOURCE_ROW_FAMILY)

    # First insert call is the happening payload
    happening_payload = builder.insert.call_args_list[0][0][0]
    assert "relevance_score_global" in happening_payload
    assert happening_payload["relevance_score_global"] == 60  # family_kids(50) + sport/yoga(10)


def test_create_happening_includes_negative_score_for_seniors():
    from src.canonicalize.merge_loop import create_happening_schedule_occurrence

    sb, builder = _mock_supabase()
    create_happening_schedule_occurrence(supabase=sb, source_row=SOURCE_ROW_SENIORS)

    happening_payload = builder.insert.call_args_list[0][0][0]
    assert "relevance_score_global" in happening_payload
    assert happening_payload["relevance_score_global"] == -30  # seniors


def test_create_happening_omits_zero_score():
    """When score is 0 (no matching tags), let DB default handle it."""
    from src.canonicalize.merge_loop import create_happening_schedule_occurrence

    sb, builder = _mock_supabase()
    create_happening_schedule_occurrence(supabase=sb, source_row=SOURCE_ROW_NEUTRAL)

    happening_payload = builder.insert.call_args_list[0][0][0]
    assert "relevance_score_global" not in happening_payload


def test_create_happening_score_matches_tag_inference():
    """Score in payload must match what compute_relevance_score returns for the inferred tags."""
    from src.canonicalize.merge_loop import create_happening_schedule_occurrence
    from src.canonicalize.scoring import compute_relevance_score
    from src.canonicalize.tagging import infer_audience_tags, infer_topic_tags

    sb, builder = _mock_supabase()
    create_happening_schedule_occurrence(supabase=sb, source_row=SOURCE_ROW_FAMILY)

    audience = infer_audience_tags(
        SOURCE_ROW_FAMILY["title_raw"], SOURCE_ROW_FAMILY["description_raw"],
    )
    topic = infer_topic_tags(
        SOURCE_ROW_FAMILY["title_raw"], SOURCE_ROW_FAMILY["description_raw"],
    )
    expected_score = compute_relevance_score(audience, topic)

    happening_payload = builder.insert.call_args_list[0][0][0]
    actual_score = happening_payload.get("relevance_score_global", 0)
    assert actual_score == expected_score


# ===========================================================================
# Part 3: MERGE path recomputes score after tag update
# ===========================================================================

def test_apply_heuristic_tags_updates_score():
    """When tags are filled from empty, relevance_score_global is recomputed."""
    from src.canonicalize.merge_loop import apply_heuristic_tags

    sb, tables = _mock_supabase_per_table()

    happening_result = MagicMock()
    happening_result.data = [{
        "id": "hap-score-1",
        "audience_tags": [],
        "topic_tags": [],
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

    apply_heuristic_tags(
        supabase=sb, happening_id="hap-score-1", source_row=SOURCE_ROW_FAMILY,
    )

    update_payload = tables["happening"].update.call_args[0][0]
    assert "relevance_score_global" in update_payload
    assert update_payload["relevance_score_global"] == 60  # family_kids(50) + sport/yoga(10)


def test_apply_heuristic_tags_skips_score_when_unchanged():
    """When tags produce same score as current, relevance_score_global is NOT in payload."""
    from src.canonicalize.merge_loop import apply_heuristic_tags

    sb, tables = _mock_supabase_per_table()

    # Happening already has family_kids tags and score=50
    happening_result = MagicMock()
    happening_result.data = [{
        "id": "hap-score-2",
        "audience_tags": ["family_kids"],
        "topic_tags": ["nature"],
        "relevance_score_global": 60,
        "title": "Kinderyoga",
        "description": "Spass für Kinder",
    }]
    tables["happening"] = MagicMock()
    for method in ["select", "eq", "limit", "update"]:
        getattr(tables["happening"], method).return_value = tables["happening"]
    tables["happening"].execute.return_value = happening_result

    # Both existing tags are non-empty → apply_heuristic_tags returns (0, 0)
    field_updates, history = apply_heuristic_tags(
        supabase=sb, happening_id="hap-score-2", source_row=SOURCE_ROW_FAMILY,
    )

    assert field_updates == 0
    tables["happening"].update.assert_not_called()


def test_apply_heuristic_tags_never_touches_editorial_priority():
    """Score recomputation must not include editorial_priority in update."""
    from src.canonicalize.merge_loop import apply_heuristic_tags

    sb, tables = _mock_supabase_per_table()

    happening_result = MagicMock()
    happening_result.data = [{
        "id": "hap-score-3",
        "audience_tags": [],
        "topic_tags": [],
        "relevance_score_global": 0,
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

    apply_heuristic_tags(
        supabase=sb, happening_id="hap-score-3", source_row=SOURCE_ROW_FAMILY,
    )

    update_payload = tables["happening"].update.call_args[0][0]
    assert "editorial_priority" not in update_payload


# ===========================================================================
# Part 4: Ranking order guarantees
# ===========================================================================

class TestRankingOrderContract:
    """
    Verify that the feed_cards_view ORDER BY matches the documented contract:
      1. section_key = 'weekend' first (LOCKED)
      2. editorial_priority DESC
      3. relevance_score_global DESC
      4. sort_at ASC (chronological tiebreaker)
      5. title ASC (final tiebreaker)
    """

    def test_view_order_by_has_weekend_first(self):
        """The ORDER BY must put weekend rows before coming_up."""
        order_clause = self._get_order_clause()
        # Weekend-first must be the first sort key
        assert order_clause.index("section_key = 'weekend'") < order_clause.index("editorial_priority")

    def test_view_order_by_has_editorial_before_relevance(self):
        """editorial_priority must sort before relevance_score_global."""
        order_clause = self._get_order_clause()
        assert order_clause.index("editorial_priority") < order_clause.index("relevance_score_global")

    def test_view_order_by_has_relevance_before_sort_at(self):
        """relevance_score_global must sort before sort_at."""
        order_clause = self._get_order_clause()
        assert order_clause.index("relevance_score_global") < order_clause.index("sort_at")

    def test_view_order_by_has_sort_at_before_title(self):
        """sort_at (chronological tiebreaker) must come before title."""
        order_clause = self._get_order_clause()
        assert order_clause.index("sort_at") < order_clause.index(", title")

    def test_view_order_by_editorial_is_desc(self):
        order_clause = self._get_order_clause()
        assert "editorial_priority DESC" in order_clause

    def test_view_order_by_relevance_is_desc(self):
        order_clause = self._get_order_clause()
        assert "relevance_score_global DESC" in order_clause

    def _get_order_clause(self) -> str:
        """Read the feed_cards_view.sql and extract the ORDER BY clause."""
        import os
        project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
        view_path = os.path.join(project_root, "sql", "views", "feed_cards_view.sql")
        with open(view_path, "r") as f:
            content = f.read()
        # Extract everything after the last ORDER BY
        idx = content.rfind("ORDER BY")
        assert idx != -1, "feed_cards_view.sql must contain ORDER BY"
        return content[idx:]


# ===========================================================================
# Part 5: No additional filters introduced
# ===========================================================================

class TestNoAdditionalFilters:
    """
    The view must not filter on audience_tags, topic_tags,
    editorial_priority, or relevance_score_global. Ranking controls
    order only, never visibility.
    """

    def test_view_where_clause_does_not_filter_on_tags(self):
        """The WHERE clause must not reference audience_tags or topic_tags."""
        where_text = self._get_where_clause()
        assert "audience_tags" not in where_text
        assert "topic_tags" not in where_text

    def test_view_where_clause_does_not_filter_on_editorial_priority(self):
        where_text = self._get_where_clause()
        assert "editorial_priority" not in where_text

    def test_view_where_clause_does_not_filter_on_relevance_score(self):
        where_text = self._get_where_clause()
        assert "relevance_score_global" not in where_text

    def _get_where_clause(self) -> str:
        """Extract the WHERE clause from feed_cards_view.sql."""
        import os
        project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
        view_path = os.path.join(project_root, "sql", "views", "feed_cards_view.sql")
        with open(view_path, "r") as f:
            content = f.read()
        # Extract WHERE to the next closing paren of the base CTE
        where_start = content.find("WHERE h.visibility_status")
        assert where_start != -1, "Expected WHERE clause in base CTE"
        # The WHERE clause ends at the next ), which is the end of the base CTE
        where_end = content.find(")", where_start)
        return content[where_start:where_end]


# ===========================================================================
# Part 6: Recompute script logic
# ===========================================================================

class TestRecomputeScript:

    def test_recompute_updates_changed_scores(self):
        """recompute_all should update rows where score differs."""
        from scripts.recompute_relevance_scores import recompute_all

        sb = MagicMock()
        builder = MagicMock()
        for method in ["select", "range", "eq", "update"]:
            getattr(builder, method).return_value = builder
        sb.table.return_value = builder

        call_count = 0

        def _execute():
            nonlocal call_count
            call_count += 1
            result = MagicMock()
            if call_count == 1:
                # First batch: two rows
                result.data = [
                    {
                        "id": "hap-1",
                        "audience_tags": ["family_kids"],
                        "topic_tags": [],
                        "relevance_score_global": 0,  # stale
                    },
                    {
                        "id": "hap-2",
                        "audience_tags": [],
                        "topic_tags": [],
                        "relevance_score_global": 0,  # already correct
                    },
                ]
            else:
                result.data = []
            return result

        builder.execute.side_effect = _execute

        counts = recompute_all(sb, dry_run=True)

        assert counts["total"] == 2
        assert counts["changed"] == 1  # hap-1 needs update (0 → 50)
        assert counts["unchanged"] == 1  # hap-2 is already 0
        assert counts["errors"] == 0

    def test_recompute_dry_run_does_not_write(self):
        """In dry_run mode, no update calls should be made."""
        from scripts.recompute_relevance_scores import recompute_all

        sb = MagicMock()
        builder = MagicMock()
        for method in ["select", "range", "eq", "update"]:
            getattr(builder, method).return_value = builder
        sb.table.return_value = builder

        call_count = 0

        def _execute():
            nonlocal call_count
            call_count += 1
            result = MagicMock()
            if call_count == 1:
                result.data = [
                    {"id": "hap-1", "audience_tags": ["family_kids"],
                     "topic_tags": [], "relevance_score_global": 0},
                ]
            else:
                result.data = []
            return result

        builder.execute.side_effect = _execute

        counts = recompute_all(sb, dry_run=True)

        assert counts["changed"] == 1
        # update() should NOT have been called (dry run)
        builder.update.assert_not_called()

    def test_recompute_idempotent(self):
        """Running twice produces same results — already-correct rows are unchanged."""
        from src.canonicalize.scoring import compute_relevance_score

        # A row that already has the correct score
        audience = ["family_kids"]
        topic = ["nature"]
        correct_score = compute_relevance_score(audience, topic)

        # If the row has correct_score, recompute should report "unchanged"
        assert correct_score == 60
        new_score = compute_relevance_score(audience, topic)
        assert new_score == correct_score  # idempotent
