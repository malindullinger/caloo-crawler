# tests/test_junk_titles.py
"""
Unit tests for the shared junk-title predicate (src/junk_titles.py).

Covers all four rules:
  1. Empty / whitespace-only → junk
  2. Exact match against known noise words → junk
  3. Known noise prefix → junk
  4. Structural-only (digits/punctuation, no letters) → junk
"""
from __future__ import annotations

import pytest

from src.junk_titles import is_junk_title, JUNK_TITLES_EXACT, JUNK_TITLE_PREFIXES


# ---------------------------------------------------------------------------
# Rule 1: Empty / whitespace-only
# ---------------------------------------------------------------------------

class TestEmptyTitles:
    @pytest.mark.parametrize("title", [
        None,
        "",
        "   ",
        "\t",
        "\n",
        "  \t\n  ",
    ])
    def test_empty_or_whitespace_is_junk(self, title):
        assert is_junk_title(title) is True


# ---------------------------------------------------------------------------
# Rule 2: Exact match against known noise words
# ---------------------------------------------------------------------------

class TestExactMatch:
    @pytest.mark.parametrize("title", [
        "kopfzeile",
        "Kopfzeile",
        "KOPFZEILE",
        "  Kopfzeile  ",
        "fusszeile",
        "Fusszeile",
        "FUSSZEILE",
        "  fusszeile  ",
    ])
    def test_known_noise_words_are_junk(self, title):
        assert is_junk_title(title) is True

    def test_blocklist_contains_expected_entries(self):
        assert "kopfzeile" in JUNK_TITLES_EXACT
        assert "fusszeile" in JUNK_TITLES_EXACT


# ---------------------------------------------------------------------------
# Rule 3: Known noise prefix
# ---------------------------------------------------------------------------

class TestPrefixMatch:
    @pytest.mark.parametrize("title", [
        "Kopfzeile 2026",
        "kopfzeile - März",
        "Fusszeile navigation",
        "fusszeile links",
    ])
    def test_noise_prefix_is_junk(self, title):
        assert is_junk_title(title) is True

    def test_prefix_list_matches_exact_list(self):
        # Prefixes should cover at least all exact-match entries
        for exact in JUNK_TITLES_EXACT:
            assert any(exact.startswith(p) for p in JUNK_TITLE_PREFIXES)


# ---------------------------------------------------------------------------
# Rule 4: Structural-only (digits/punctuation, no letters)
# ---------------------------------------------------------------------------

class TestStructuralOnly:
    @pytest.mark.parametrize("title", [
        "123",
        "---",
        "...",
        "12.03.2026",
        "  42  ",
        "***",
    ])
    def test_digits_punctuation_only_is_junk(self, title):
        assert is_junk_title(title) is True


# ---------------------------------------------------------------------------
# Legitimate titles (should NOT be junk)
# ---------------------------------------------------------------------------

class TestLegitTitles:
    @pytest.mark.parametrize("title", [
        "Kinderflohmarkt",
        "Yoga im Park",
        "Gemeindeversammlung 2026",
        "Laternenumzug",
        "1. Mai Feier",
        "Ferienpass 2026",
        "Konzert: Bach & Händel",
    ])
    def test_legitimate_titles_are_not_junk(self, title):
        assert is_junk_title(title) is False
