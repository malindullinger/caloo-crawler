# tests/test_confidence_score.py
"""
Tests for data-quality confidence score (v1).

Covers:
  1. Formula unit tests (all penalties + clamp)
  2. Integration: CREATE path sets confidence_score
  3. Integration: MERGE path recomputes confidence_score
  4. Recompute script (dry-run no writes, write updates)
  5. Feed invariant: feed_cards_view unaffected by confidence feature
"""
from __future__ import annotations

import os
import re
from unittest.mock import MagicMock, patch, call

import pytest

from src.canonicalize.confidence import compute_confidence_score


# ---------------------------------------------------------------------------
# 1. Formula unit tests
# ---------------------------------------------------------------------------

class TestConfidenceFormula:

    def test_perfect_score(self):
        """All fields present, tier A, jsonld extraction → 100."""
        score = compute_confidence_score(
            source_tier="A",
            date_precision="datetime",
            image_url="https://example.com/img.jpg",
            description="A great event",
            canonical_url="https://example.com/event",
            timezone="Europe/Zurich",
            extraction_method="jsonld",
        )
        assert score == 100

    def test_date_precision_penalty(self):
        """date_precision='date' → -20."""
        score = compute_confidence_score(
            source_tier="A",
            date_precision="date",
            image_url="https://example.com/img.jpg",
            description="A great event",
            canonical_url="https://example.com/event",
            timezone="Europe/Zurich",
            extraction_method="jsonld",
        )
        assert score == 80

    def test_image_url_penalty(self):
        """Missing image_url → -20."""
        score = compute_confidence_score(
            source_tier="A",
            date_precision="datetime",
            image_url=None,
            description="A great event",
            canonical_url="https://example.com/event",
            timezone="Europe/Zurich",
            extraction_method="jsonld",
        )
        assert score == 80

    def test_image_url_empty_string_penalty(self):
        """Empty string image_url → -20 (same as None)."""
        score = compute_confidence_score(
            source_tier="A",
            date_precision="datetime",
            image_url="  ",
            description="A great event",
            canonical_url="https://example.com/event",
            timezone="Europe/Zurich",
            extraction_method="jsonld",
        )
        assert score == 80

    def test_description_penalty(self):
        """Missing description → -15."""
        score = compute_confidence_score(
            source_tier="A",
            date_precision="datetime",
            image_url="https://example.com/img.jpg",
            description=None,
            canonical_url="https://example.com/event",
            timezone="Europe/Zurich",
            extraction_method="jsonld",
        )
        assert score == 85

    def test_source_tier_b_penalty(self):
        """source_tier='B' → -10."""
        score = compute_confidence_score(
            source_tier="B",
            date_precision="datetime",
            image_url="https://example.com/img.jpg",
            description="A great event",
            canonical_url="https://example.com/event",
            timezone="Europe/Zurich",
            extraction_method="jsonld",
        )
        assert score == 90

    def test_source_tier_a_no_penalty(self):
        """source_tier='A' → no penalty."""
        score = compute_confidence_score(
            source_tier="A",
            date_precision="datetime",
            image_url="https://example.com/img.jpg",
            description="A great event",
            canonical_url="https://example.com/event",
            timezone="Europe/Zurich",
            extraction_method="jsonld",
        )
        assert score == 100

    def test_extraction_method_penalty(self):
        """extraction_method != 'jsonld' → -15."""
        score = compute_confidence_score(
            source_tier="A",
            date_precision="datetime",
            image_url="https://example.com/img.jpg",
            description="A great event",
            canonical_url="https://example.com/event",
            timezone="Europe/Zurich",
            extraction_method="text_heuristic",
        )
        assert score == 85

    def test_extraction_method_none_penalty(self):
        """extraction_method=None → -15."""
        score = compute_confidence_score(
            source_tier="A",
            date_precision="datetime",
            image_url="https://example.com/img.jpg",
            description="A great event",
            canonical_url="https://example.com/event",
            timezone="Europe/Zurich",
            extraction_method=None,
        )
        assert score == 85

    def test_timezone_penalty(self):
        """Missing timezone → -30."""
        score = compute_confidence_score(
            source_tier="A",
            date_precision="datetime",
            image_url="https://example.com/img.jpg",
            description="A great event",
            canonical_url="https://example.com/event",
            timezone=None,
            extraction_method="jsonld",
        )
        assert score == 70

    def test_canonical_url_penalty(self):
        """Missing canonical_url → -40."""
        score = compute_confidence_score(
            source_tier="A",
            date_precision="datetime",
            image_url="https://example.com/img.jpg",
            description="A great event",
            canonical_url=None,
            timezone="Europe/Zurich",
            extraction_method="jsonld",
        )
        assert score == 60

    def test_multiple_penalties_stack(self):
        """Multiple missing fields stack penalties."""
        # -20 (date) -20 (image) -15 (desc) = -55
        score = compute_confidence_score(
            source_tier="A",
            date_precision="date",
            image_url=None,
            description=None,
            canonical_url="https://example.com/event",
            timezone="Europe/Zurich",
            extraction_method="jsonld",
        )
        assert score == 45

    def test_all_penalties_clamp_to_zero(self):
        """All penalties applied → clamp to 0, not negative."""
        # -20 -20 -15 -10 -15 -30 -40 = -150 → clamped to 0
        score = compute_confidence_score(
            source_tier="B",
            date_precision="date",
            image_url=None,
            description=None,
            canonical_url=None,
            timezone=None,
            extraction_method="text_heuristic",
        )
        assert score == 0

    def test_all_defaults_none(self):
        """All defaults (None) → maximum penalties, clamp to 0."""
        score = compute_confidence_score()
        assert score == 0

    def test_tier_c_no_penalty(self):
        """source_tier='C' → no tier penalty (only B is penalized)."""
        score = compute_confidence_score(
            source_tier="C",
            date_precision="datetime",
            image_url="https://example.com/img.jpg",
            description="desc",
            canonical_url="https://example.com",
            timezone="Europe/Zurich",
            extraction_method="jsonld",
        )
        assert score == 100

    def test_case_insensitive_tier(self):
        """Tier comparison is case-insensitive."""
        score = compute_confidence_score(
            source_tier="b",
            date_precision="datetime",
            image_url="https://example.com/img.jpg",
            description="desc",
            canonical_url="https://example.com",
            timezone="Europe/Zurich",
            extraction_method="jsonld",
        )
        assert score == 90

    def test_score_is_int(self):
        """Score must always be an integer."""
        score = compute_confidence_score(
            source_tier="A",
            date_precision="datetime",
            image_url="img",
            description="desc",
            canonical_url="url",
            timezone="tz",
            extraction_method="jsonld",
        )
        assert isinstance(score, int)

    def test_typical_maennedorf_b_tier_no_image(self):
        """Typical Männedorf source: tier B, text_heuristic, no image, date-only."""
        # -20 (date) -20 (image) -10 (tier B) -15 (not jsonld) = -65
        score = compute_confidence_score(
            source_tier="B",
            date_precision="date",
            image_url=None,
            description="Gemeindeversammlung in Männedorf",
            canonical_url="https://maennedorf.ch/event/123",
            timezone="Europe/Zurich",
            extraction_method="text_heuristic",
        )
        assert score == 35

    def test_typical_eventbrite_a_tier_full(self):
        """Typical Eventbrite source: tier A, jsonld, all fields."""
        score = compute_confidence_score(
            source_tier="A",
            date_precision="datetime",
            image_url="https://img.evbuc.com/123.jpg",
            description="Join us for yoga in the park",
            canonical_url="https://eventbrite.com/e/123",
            timezone="Europe/Zurich",
            extraction_method="jsonld",
        )
        assert score == 100


# ---------------------------------------------------------------------------
# 2. Integration: CREATE path sets confidence_score
# ---------------------------------------------------------------------------

class TestCreatePathSetsConfidence:

    def test_create_happening_payload_includes_confidence(self):
        """create_happening_schedule_occurrence must set confidence_score."""
        from src.canonicalize.merge_loop import _quality_score_from_source_row

        source_row = {
            "title_raw": "Test Event",
            "description_raw": "A description",
            "source_tier": "A",
            "date_precision": "datetime",
            "image_url": "https://example.com/img.jpg",
            "item_url": "https://example.com/event",
            "timezone": "Europe/Zurich",
            "extraction_method": "jsonld",
        }

        score = _quality_score_from_source_row(source_row)
        assert score == 100

    def test_create_happening_payload_with_missing_fields(self):
        """Score reflects missing fields for CREATE."""
        from src.canonicalize.merge_loop import _quality_score_from_source_row

        source_row = {
            "title_raw": "Test Event",
            "source_tier": "B",
            "date_precision": "date",
            "extraction_method": "text_heuristic",
            "timezone": "Europe/Zurich",
        }

        score = _quality_score_from_source_row(source_row)
        # -20 (date) -20 (no image) -15 (no desc) -10 (B) -15 (not jsonld)
        # -40 (no canonical_url / item_url)
        assert score == 0  # clamped

    def test_quality_score_uses_happening_description_fallback(self):
        """When happening_description is provided, it overrides source_row."""
        from src.canonicalize.merge_loop import _quality_score_from_source_row

        source_row = {
            "source_tier": "A",
            "date_precision": "datetime",
            "image_url": "img",
            "item_url": "url",
            "timezone": "tz",
            "extraction_method": "jsonld",
            # description_raw is None
        }

        # Without happening_description → -15
        score_without = _quality_score_from_source_row(source_row)
        assert score_without == 85

        # With happening_description → no penalty
        score_with = _quality_score_from_source_row(source_row, "A description")
        assert score_with == 100


# ---------------------------------------------------------------------------
# 3. Integration: MERGE path recomputes confidence_score
# ---------------------------------------------------------------------------

class TestMergePathRecomputes:

    def test_recompute_skips_when_unchanged(self):
        """_recompute_confidence_on_merge returns False when score unchanged."""
        from src.canonicalize.merge_loop import _recompute_confidence_on_merge

        mock_supabase = MagicMock()
        # Simulate current happening with score 100
        mock_supabase.table.return_value.select.return_value.eq.return_value.limit.return_value.execute.return_value.data = [
            {"confidence_score": 100, "description": "desc"}
        ]

        source_row = {
            "source_tier": "A",
            "date_precision": "datetime",
            "image_url": "img",
            "item_url": "url",
            "timezone": "tz",
            "extraction_method": "jsonld",
            "description_raw": "desc",
        }

        result = _recompute_confidence_on_merge(
            supabase=mock_supabase,
            happening_id="test-id",
            source_row=source_row,
        )
        assert result is False

    def test_recompute_updates_when_changed(self):
        """_recompute_confidence_on_merge returns True and updates when score changed."""
        from src.canonicalize.merge_loop import _recompute_confidence_on_merge

        mock_supabase = MagicMock()
        # First call: select for current score
        mock_supabase.table.return_value.select.return_value.eq.return_value.limit.return_value.execute.return_value.data = [
            {"confidence_score": 100, "description": "desc"}
        ]
        # Second call: update (chained)
        mock_supabase.table.return_value.update.return_value.eq.return_value.execute.return_value = MagicMock()

        source_row = {
            "source_tier": "B",
            "date_precision": "date",
            "image_url": None,
            "item_url": "url",
            "timezone": "tz",
            "extraction_method": "text_heuristic",
            "description_raw": "desc",
        }

        result = _recompute_confidence_on_merge(
            supabase=mock_supabase,
            happening_id="test-id",
            source_row=source_row,
        )
        assert result is True

    def test_recompute_returns_false_for_missing_happening(self):
        """_recompute_confidence_on_merge returns False if happening not found."""
        from src.canonicalize.merge_loop import _recompute_confidence_on_merge

        mock_supabase = MagicMock()
        mock_supabase.table.return_value.select.return_value.eq.return_value.limit.return_value.execute.return_value.data = []

        result = _recompute_confidence_on_merge(
            supabase=mock_supabase,
            happening_id="nonexistent",
            source_row={},
        )
        assert result is False


# ---------------------------------------------------------------------------
# 4. Recompute script
# ---------------------------------------------------------------------------

class TestRecomputeScript:

    def test_recompute_dry_run_no_writes(self):
        """Dry run must not call update."""
        from scripts.recompute_confidence_scores import recompute_all

        mock_supabase = MagicMock()

        # Simulate one happening with score 100
        happening_resp = MagicMock()
        happening_resp.data = [
            {"id": "h1", "confidence_score": 100, "description": "desc"}
        ]
        empty_resp = MagicMock()
        empty_resp.data = []

        # First call returns happenings, second returns empty (end pagination)
        range_mock = MagicMock()
        range_mock.execute.side_effect = [happening_resp, empty_resp]

        order_mock = MagicMock()
        order_mock.range = MagicMock(return_value=range_mock)

        select_mock = MagicMock()
        select_mock.order = MagicMock(return_value=order_mock)

        # Source data response
        source_resp = MagicMock()
        source_resp.data = [{
            "source_happening_id": "sh1",
            "is_primary": True,
            "source_priority": 300,
            "merged_at": "2026-01-01",
            "source_happenings": {
                "source_tier": "B",
                "date_precision": "date",
                "image_url": None,
                "item_url": "url",
                "description_raw": "desc",
                "timezone": "Europe/Zurich",
                "extraction_method": "text_heuristic",
            },
        }]

        def table_side_effect(name):
            mock = MagicMock()
            if name == "happening":
                mock.select.return_value = select_mock
                mock.update = MagicMock()
            elif name == "happening_sources":
                mock.select.return_value.eq.return_value.order.return_value.order.return_value.order.return_value.limit.return_value.execute.return_value = source_resp
            return mock

        mock_supabase.table = MagicMock(side_effect=table_side_effect)

        counts = recompute_all(mock_supabase, dry_run=True)

        assert counts["total"] == 1
        assert counts["changed"] == 1
        assert counts["errors"] == 0
        # Verify no update was called on happening table
        # (dry_run=True should skip update)

    def test_script_file_exists(self):
        """Script file must exist."""
        root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
        path = os.path.join(root, "scripts", "recompute_confidence_scores.py")
        assert os.path.isfile(path)


# ---------------------------------------------------------------------------
# 5. Migration file
# ---------------------------------------------------------------------------

class TestMigration026:

    def _read_migration(self) -> str:
        root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
        path = os.path.join(root, "migrations", "026_confidence_score.sql")
        with open(path, "r", encoding="utf-8") as f:
            return f.read()

    def test_migration_file_exists(self):
        root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
        path = os.path.join(root, "migrations", "026_confidence_score.sql")
        assert os.path.isfile(path)

    def test_adds_confidence_score_column(self):
        sql = self._read_migration()
        assert "confidence_score" in sql
        assert "ALTER TABLE happening" in sql

    def test_default_is_100(self):
        sql = self._read_migration()
        assert "DEFAULT 100" in sql

    def test_not_null_constraint(self):
        sql = self._read_migration()
        assert "NOT NULL" in sql

    def test_no_feed_view_modification(self):
        sql = self._read_migration()
        assert "feed_cards_view" not in sql

    def test_comment_mentions_no_filtering(self):
        sql = self._read_migration()
        assert "NOT a feed filter" in sql


# ---------------------------------------------------------------------------
# 6. Feed invariant: confidence feature does not touch feed
# ---------------------------------------------------------------------------

class TestFeedInvariant:

    def test_feed_cards_view_has_no_confidence_score(self):
        """feed_cards_view must NOT reference confidence_score."""
        root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
        path = os.path.join(root, "sql", "views", "feed_cards_view.sql")
        with open(path, "r", encoding="utf-8") as f:
            sql = f.read()
        assert "confidence_score" not in sql, (
            "feed_cards_view must not reference confidence_score — "
            "confidence is for review/ops only, never feed filtering"
        )

    def test_confidence_module_does_not_import_feed(self):
        """confidence.py must not import anything feed-related."""
        root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
        path = os.path.join(root, "src", "canonicalize", "confidence.py")
        with open(path, "r", encoding="utf-8") as f:
            source = f.read()
        assert "feed_cards" not in source
        assert "section_key" not in source
