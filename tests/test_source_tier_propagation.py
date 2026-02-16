# tests/test_source_tier_propagation.py
"""
Regression tests: source_tier must propagate correctly from config → storage.

Expected tiers (locked by docs/tier-b-sources.md + migration 022):
  eventbrite_zurich     → A  (JSON-LD structured)
  maennedorf_portal     → B  (text_heuristic, municipal exception)
  elternverein_uetikon  → B  (text_heuristic, FairGate SPA)
"""
from __future__ import annotations

from datetime import datetime, timezone
from unittest.mock import MagicMock, patch

import pytest

from src.models import NormalizedEvent


# -- Expected tiers (source of truth: docs/tier-b-sources.md) ----------------

EXPECTED_TIERS = {
    "eventbrite_zurich": "A",
    "maennedorf_portal": "B",
    "elternverein_uetikon": "B",
}


# -- Helpers ------------------------------------------------------------------

def _make_normalized(source_id: str, source_tier: str) -> NormalizedEvent:
    return NormalizedEvent(
        external_id="ext-1",
        source_id=source_id,
        title="Test Event",
        start_at=datetime(2026, 6, 15, 10, 0, tzinfo=timezone.utc),
        timezone="Europe/Zurich",
        canonical_url="https://example.com/1",
        last_seen_at=datetime(2026, 1, 1, tzinfo=timezone.utc),
        extra={"source_tier": source_tier},
    )


# -- Test: hardcoded fallback sources carry correct tiers ---------------------

def test_hardcoded_sources_have_correct_tiers():
    """_hardcoded_sources_fallback must set source_tier matching docs."""
    from src.sources.multi_source import _hardcoded_sources_fallback

    sources = _hardcoded_sources_fallback()
    tier_map = {s.source_id: s.source_tier for s in sources}

    for source_id, expected_tier in EXPECTED_TIERS.items():
        assert tier_map.get(source_id) == expected_tier, (
            f"{source_id}: expected tier {expected_tier!r}, got {tier_map.get(source_id)!r}"
        )


# -- Test: enqueue_source_happening reads tier from extra ---------------------

@pytest.mark.parametrize("source_id,expected_tier", list(EXPECTED_TIERS.items()))
def test_enqueue_writes_correct_source_tier(source_id: str, expected_tier: str):
    """enqueue_source_happening must write the tier from ev.extra, not hardcode 'A'."""
    from src.storage import enqueue_source_happening

    mock_builder = MagicMock()
    mock_table = MagicMock(return_value=mock_builder)
    mock_builder.upsert.return_value = mock_builder
    mock_builder.execute.return_value = MagicMock(data=[{}])

    mock_supabase = MagicMock()
    mock_supabase.table = mock_table

    ev = _make_normalized(source_id, expected_tier)

    with patch("src.storage.get_supabase", return_value=mock_supabase):
        enqueue_source_happening(ev)

    payload = mock_builder.upsert.call_args[0][0]
    assert payload["source_tier"] == expected_tier, (
        f"{source_id}: payload source_tier={payload['source_tier']!r}, expected {expected_tier!r}"
    )


# -- Test: missing/invalid tier defaults to 'A' ------------------------------

def test_enqueue_defaults_tier_a_when_extra_missing():
    """If extra has no source_tier, storage defaults to 'A'."""
    from src.storage import enqueue_source_happening

    mock_builder = MagicMock()
    mock_table = MagicMock(return_value=mock_builder)
    mock_builder.upsert.return_value = mock_builder
    mock_builder.execute.return_value = MagicMock(data=[{}])

    mock_supabase = MagicMock()
    mock_supabase.table = mock_table

    ev = NormalizedEvent(
        external_id="ext-1",
        source_id="some_new_source",
        title="Test",
        start_at=datetime(2026, 6, 15, 10, 0, tzinfo=timezone.utc),
        timezone="Europe/Zurich",
        canonical_url="https://example.com/1",
        last_seen_at=datetime(2026, 1, 1, tzinfo=timezone.utc),
        extra={},
    )

    with patch("src.storage.get_supabase", return_value=mock_supabase):
        enqueue_source_happening(ev)

    payload = mock_builder.upsert.call_args[0][0]
    assert payload["source_tier"] == "A"


def test_enqueue_defaults_tier_a_for_invalid_value():
    """If extra.source_tier is invalid, storage defaults to 'A'."""
    from src.storage import enqueue_source_happening

    mock_builder = MagicMock()
    mock_table = MagicMock(return_value=mock_builder)
    mock_builder.upsert.return_value = mock_builder
    mock_builder.execute.return_value = MagicMock(data=[{}])

    mock_supabase = MagicMock()
    mock_supabase.table = mock_table

    ev = _make_normalized("test_source", "X")  # invalid tier

    with patch("src.storage.get_supabase", return_value=mock_supabase):
        enqueue_source_happening(ev)

    payload = mock_builder.upsert.call_args[0][0]
    assert payload["source_tier"] == "A"


# -- Test: DbSourceRow preserves text tier ------------------------------------

def test_db_source_row_preserves_text_tier():
    """DbSourceRow.source_tier stores the text tier letter, not an int."""
    from src.db.db_sources import DbSourceRow

    row = DbSourceRow(
        source_id="test",
        adapter="test",
        seed_url="https://example.com",
        max_items=50,
        source_tier="B",
        is_enabled=True,
    )
    assert row.source_tier == "B"


# -- Test: SourceConfig carries tier ------------------------------------------

def test_source_config_default_tier_is_a():
    """SourceConfig.source_tier defaults to 'A' (Tier A = structured)."""
    from src.sources.types import SourceConfig

    cfg = SourceConfig(
        source_id="test",
        adapter="test",
        seed_url="https://example.com",
    )
    assert cfg.source_tier == "A"
