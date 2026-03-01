# tests/test_header_rows_are_never_enqueued.py
"""
Regression tests: header / noise titles must NEVER reach source_happenings.

The ingestion gate in src/storage.py rejects junk titles before any DB write.
This is enforced at the two choke points:
  - enqueue_source_happening (NormalizedEvent path)
  - upsert_source_happening_row (raw dict path)

Rejection criteria (is_junk_title):
  1. Empty / whitespace-only
  2. Exact match against known noise words (case-insensitive)
  3. Starts with a known noise prefix (case-insensitive)
  4. Contains only whitespace / digits / punctuation (no real letters)
"""
from __future__ import annotations

from datetime import datetime, timezone
from unittest.mock import MagicMock, patch

import pytest

from src.storage import is_junk_title


# ---------------------------------------------------------------------------
# Part 1: is_junk_title unit tests
# ---------------------------------------------------------------------------

class TestIsJunkTitle:
    """Deterministic title rejection rules."""

    @pytest.mark.parametrize("title", [
        None,
        "",
        "   ",
        "\t\n",
    ])
    def test_empty_or_whitespace_is_junk(self, title):
        assert is_junk_title(title) is True

    @pytest.mark.parametrize("title", [
        "Kopfzeile",
        "kopfzeile",
        "KOPFZEILE",
        "Fusszeile",
        "fusszeile",
        "FUSSZEILE",
    ])
    def test_known_noise_words_are_junk(self, title):
        assert is_junk_title(title) is True

    @pytest.mark.parametrize("title", [
        "Kopfzeile ",       # trailing whitespace (stripped)
        " kopfzeile",       # leading whitespace (stripped)
        "  Fusszeile  ",
    ])
    def test_noise_words_with_whitespace_are_junk(self, title):
        assert is_junk_title(title) is True

    @pytest.mark.parametrize("title", [
        "Kopfzeile: Veranstaltungen",
        "Kopfzeile Navigation",
        "Fusszeile Links",
    ])
    def test_noise_prefix_matches_are_junk(self, title):
        assert is_junk_title(title) is True

    @pytest.mark.parametrize("title", [
        "---",
        "...",
        "  123  ",
        "***",
        "  ",
        "42",
    ])
    def test_structural_only_titles_are_junk(self, title):
        assert is_junk_title(title) is True

    @pytest.mark.parametrize("title", [
        "Kinderflohmarkt",
        "Yoga im Park",
        "Elternabend 2026",
        "Familientanzen",
        "Kino für die Chline",
        "Spielgruppe Bärliland",
        "Open Air Kino",
    ])
    def test_real_event_titles_are_not_junk(self, title):
        assert is_junk_title(title) is False

    def test_single_letter_title_is_not_junk(self):
        """A single real letter is unusual but not structural noise."""
        assert is_junk_title("A") is False


# ---------------------------------------------------------------------------
# Part 2: enqueue_source_happening rejects junk titles (no DB call)
# ---------------------------------------------------------------------------

def _make_normalized(title: str):
    from src.models import NormalizedEvent
    return NormalizedEvent(
        external_id="ext-1",
        source_id="test_source",
        title=title,
        start_at=datetime(2026, 6, 15, 10, 0, tzinfo=timezone.utc),
        timezone="Europe/Zurich",
        canonical_url="https://example.com/1",
        last_seen_at=datetime(2026, 1, 1, tzinfo=timezone.utc),
        extra={},
    )


@pytest.mark.parametrize("junk_title", [
    "",
    "Kopfzeile",
    "fusszeile",
    "---",
    "   ",
])
def test_enqueue_source_happening_rejects_junk_titles(junk_title):
    """enqueue_source_happening must return early without any DB call."""
    from src.storage import enqueue_source_happening

    mock_supabase = MagicMock()

    with patch("src.storage.get_supabase", return_value=mock_supabase):
        enqueue_source_happening(_make_normalized(junk_title))

    # No RPC call should have been made
    mock_supabase.rpc.assert_not_called()


def test_enqueue_source_happening_allows_real_titles():
    """Real event titles must pass the gate and reach the DB."""
    from src.storage import enqueue_source_happening

    mock_supabase = MagicMock()
    rpc_builder = MagicMock()
    rpc_builder.execute.return_value = MagicMock(data=[{}])
    mock_supabase.rpc.return_value = rpc_builder

    with patch("src.storage.get_supabase", return_value=mock_supabase):
        enqueue_source_happening(_make_normalized("Kinderflohmarkt"))

    mock_supabase.rpc.assert_called_once()


# ---------------------------------------------------------------------------
# Part 3: upsert_source_happening_row rejects junk titles (no DB call)
# ---------------------------------------------------------------------------

@pytest.mark.parametrize("junk_title", [
    "",
    "Kopfzeile",
    "FUSSZEILE",
    "...",
    None,
])
def test_upsert_source_happening_row_rejects_junk_titles(junk_title):
    """upsert_source_happening_row must return False without any DB call."""
    from src.storage import upsert_source_happening_row

    mock_supabase = MagicMock()

    payload = {
        "source_id": "test_source",
        "title_raw": junk_title,
        "start_date_local": "2026-04-12",
        "location_raw": "Gemeindesaal",
        "item_url": "https://example.com/1",
    }

    with patch("src.storage.get_supabase", return_value=mock_supabase):
        result = upsert_source_happening_row(payload)

    assert result is False
    mock_supabase.rpc.assert_not_called()


def test_upsert_source_happening_row_allows_real_titles():
    """Real titles must pass through and reach the DB."""
    from src.storage import upsert_source_happening_row

    mock_supabase = MagicMock()
    rpc_builder = MagicMock()
    rpc_builder.execute.return_value = MagicMock(data=[{}])
    mock_supabase.rpc.return_value = rpc_builder

    payload = {
        "source_id": "test_source",
        "title_raw": "Yoga im Park",
        "start_date_local": "2026-04-12",
        "location_raw": "Stadtpark",
        "item_url": "https://example.com/yoga",
    }

    with patch("src.storage.get_supabase", return_value=mock_supabase):
        result = upsert_source_happening_row(payload)

    assert result is True
    mock_supabase.rpc.assert_called_once()


# ---------------------------------------------------------------------------
# Part 4: the "title" key fallback in upsert_source_happening_row
# ---------------------------------------------------------------------------

def test_upsert_source_happening_row_checks_title_key_fallback():
    """If payload has 'title' instead of 'title_raw', the gate still fires."""
    from src.storage import upsert_source_happening_row

    mock_supabase = MagicMock()

    payload = {
        "source_id": "test_source",
        "title": "Kopfzeile",  # uses 'title' key, not 'title_raw'
        "start_date_local": "2026-04-12",
    }

    with patch("src.storage.get_supabase", return_value=mock_supabase):
        result = upsert_source_happening_row(payload)

    assert result is False
    mock_supabase.rpc.assert_not_called()
