# tests/test_junk_title_suppression.py
"""
Non-regression tests: junk titles are suppressed at every layer.

Proves that 'Kopfzeile', 'Fusszeile', blank, and structural-only titles
can never become publishable or appear in a feed-eligible result.

Layers tested:
  1. Shared predicate (src.junk_titles.is_junk_title)
  2. Adapter (maennedorf_portal._is_junk_title delegates to shared)
  3. Storage ingestion gate (storage.is_junk_title re-exports shared)
  4. Bridge to canonical (bridge uses shared predicate)
  5. Eligibility gate (eligibility.is_feed_eligible rejects junk)
"""
from __future__ import annotations

import pytest

from src.junk_titles import is_junk_title


# ---------------------------------------------------------------------------
# 1. Shared predicate: all known junk patterns
# ---------------------------------------------------------------------------

JUNK_TITLES = [
    "Kopfzeile",
    "kopfzeile",
    "Fusszeile",
    "fusszeile",
    "Kopfzeile 2026",
    "fusszeile navigation",
    "",
    None,
    "   ",
    "123",
    "---",
]

LEGIT_TITLES = [
    "Kinderflohmarkt",
    "Yoga im Park",
    "Ferienpass 2026",
    "Gemeindeversammlung",
]


class TestSharedPredicateRegression:
    @pytest.mark.parametrize("title", JUNK_TITLES)
    def test_junk_detected(self, title):
        assert is_junk_title(title) is True

    @pytest.mark.parametrize("title", LEGIT_TITLES)
    def test_legit_not_blocked(self, title):
        assert is_junk_title(title) is False


# ---------------------------------------------------------------------------
# 2. Adapter: _is_junk_title delegates to shared predicate
# ---------------------------------------------------------------------------

class TestAdapterDelegation:
    def test_adapter_uses_shared_predicate(self):
        from src.sources.adapters.maennedorf_portal import _is_junk_title

        # Verify both functions agree on all test cases
        for title in JUNK_TITLES:
            assert _is_junk_title(title) is True, f"Adapter missed junk: {title!r}"
        for title in LEGIT_TITLES:
            assert _is_junk_title(title) is False, f"Adapter false positive: {title!r}"


# ---------------------------------------------------------------------------
# 3. Storage: is_junk_title is the same shared predicate
# ---------------------------------------------------------------------------

class TestStorageReExport:
    def test_storage_exports_shared_predicate(self):
        from src.storage import is_junk_title as storage_is_junk

        # Must be the exact same function object (re-export, not copy)
        assert storage_is_junk is is_junk_title


# ---------------------------------------------------------------------------
# 4. Bridge to canonical: uses shared predicate
# ---------------------------------------------------------------------------

class TestBridgeCanonical:
    def test_bridge_imports_shared_predicate(self):
        """Verify bridge_maennedorf_to_canonical imports is_junk_title from src.junk_titles."""
        import ast
        from pathlib import Path

        src = Path("src/jobs/bridge_maennedorf_to_canonical.py").read_text()
        tree = ast.parse(src)
        imports = [
            node for node in ast.walk(tree)
            if isinstance(node, ast.ImportFrom)
            and node.module == "src.junk_titles"
            and any(alias.name == "is_junk_title" for alias in node.names)
        ]
        assert len(imports) == 1, "bridge must import is_junk_title from src.junk_titles"

    def test_bridge_uses_shared_predicate_for_skip(self):
        """Verify bridge calls is_junk_title (not old should_skip_title)."""
        src = open("src/jobs/bridge_maennedorf_to_canonical.py").read()
        assert "is_junk_title(title_raw)" in src
        assert "should_skip_title" not in src
        assert "SKIP_TITLES_EXACT" not in src


# ---------------------------------------------------------------------------
# 5. Eligibility: junk titles make happenings ineligible
# ---------------------------------------------------------------------------

class TestEligibilityRejectsJunk:
    @pytest.mark.parametrize("title", ["Kopfzeile", "fusszeile", "", None])
    def test_junk_title_ineligible(self, title):
        from src.canonicalize.eligibility import is_feed_eligible

        happening = {
            "title": title,
            "start_at": "2026-06-01T10:00:00+02:00",
            "date_precision": "datetime",
            "location_name": "Gemeindesaal",
        }
        result = is_feed_eligible(happening)
        assert result.eligible is False
        assert "junk_title" in result.reasons

    @pytest.mark.parametrize("title", LEGIT_TITLES)
    def test_legit_title_no_junk_reason(self, title):
        from src.canonicalize.eligibility import is_feed_eligible

        happening = {
            "title": title,
            "start_at": "2026-06-01T10:00:00+02:00",
            "date_precision": "datetime",
            "location_name": "Gemeindesaal",
        }
        result = is_feed_eligible(happening)
        assert "junk_title" not in result.reasons
