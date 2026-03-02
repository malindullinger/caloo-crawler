# tests/test_organizer_normalize.py
"""
Unit tests for organizer name normalization and junk filtering.
"""
from __future__ import annotations

from src.extraction.organizer.normalize import (
    is_junk_organizer_name,
    normalize_organizer_name,
)


# ---------------------------------------------------------------------------
# normalize_organizer_name
# ---------------------------------------------------------------------------

class TestNormalizeOrganizerName:
    def test_strips_whitespace(self):
        assert normalize_organizer_name("  Elternverein  ") == "Elternverein"

    def test_collapses_internal_whitespace(self):
        assert normalize_organizer_name("Elternverein   Männedorf") == "Elternverein Männedorf"

    def test_removes_trailing_punctuation(self):
        assert normalize_organizer_name("Elternverein Männedorf:") == "Elternverein Männedorf"
        assert normalize_organizer_name("Sportclub.") == "Sportclub"
        assert normalize_organizer_name("Verein;") == "Verein"
        assert normalize_organizer_name("Club,") == "Club"
        assert normalize_organizer_name("Name---") == "Name"

    def test_nfkc_normalization(self):
        # \u00e9 (precomposed) vs e + combining acute
        name_composed = "Caf\u00e9"
        name_decomposed = "Cafe\u0301"
        assert normalize_organizer_name(name_composed) == normalize_organizer_name(name_decomposed)

    def test_all_caps_to_title_case(self):
        assert normalize_organizer_name("ELTERNVEREIN MÄNNEDORF") == "Elternverein Männedorf"

    def test_short_all_caps_preserved(self):
        # <= 5 chars total should NOT be title-cased (could be abbreviation)
        assert normalize_organizer_name("OKJA") == "OKJA"
        assert normalize_organizer_name("AG") == "AG"

    def test_preserves_known_abbreviations_in_all_caps(self):
        assert normalize_organizer_name("SPORTVEREIN GMBH") == "Sportverein GmbH"

    def test_mixed_case_preserved(self):
        assert normalize_organizer_name("Elternverein Männedorf") == "Elternverein Männedorf"

    def test_empty_string(self):
        assert normalize_organizer_name("") == ""

    def test_none_like_empty(self):
        assert normalize_organizer_name("") == ""

    def test_whitespace_only(self):
        assert normalize_organizer_name("   ") == ""

    def test_trailing_dash_removed(self):
        assert normalize_organizer_name("Verein –") == "Verein"


# ---------------------------------------------------------------------------
# is_junk_organizer_name
# ---------------------------------------------------------------------------

class TestIsJunkOrganizerName:
    def test_empty_is_junk(self):
        assert is_junk_organizer_name("") is True

    def test_whitespace_only_is_junk(self):
        assert is_junk_organizer_name("   ") is True

    def test_too_short_is_junk(self):
        assert is_junk_organizer_name("AB") is True
        assert is_junk_organizer_name("X") is True

    def test_kontakt_is_junk(self):
        assert is_junk_organizer_name("Kontakt") is True
        assert is_junk_organizer_name("kontakt") is True
        assert is_junk_organizer_name("KONTAKT") is True

    def test_impressum_is_junk(self):
        assert is_junk_organizer_name("Impressum") is True

    def test_weitere_informationen_is_junk(self):
        assert is_junk_organizer_name("Weitere Informationen") is True

    def test_anmeldung_is_junk(self):
        assert is_junk_organizer_name("Anmeldung") is True

    def test_veranstaltungen_is_junk(self):
        assert is_junk_organizer_name("Veranstaltungen") is True

    def test_gemeinde_bare_is_junk(self):
        assert is_junk_organizer_name("Gemeinde") is True

    def test_navigation_is_junk(self):
        assert is_junk_organizer_name("Navigation") is True

    def test_datenschutz_is_junk(self):
        assert is_junk_organizer_name("Datenschutz") is True

    def test_valid_org_name_not_junk(self):
        assert is_junk_organizer_name("Elternverein Männedorf") is False
        assert is_junk_organizer_name("Sportclub Zürich") is False
        assert is_junk_organizer_name("Bibliothek Männedorf") is False

    def test_gemeinde_as_part_of_name_not_junk(self):
        # "Gemeinde" alone is junk, but "Gemeinde Männedorf" is not
        # (it won't match exact junk list)
        assert is_junk_organizer_name("Gemeinde Männedorf") is False

    def test_punctuation_only_is_junk(self):
        assert is_junk_organizer_name("--") is True
        assert is_junk_organizer_name("...") is True
