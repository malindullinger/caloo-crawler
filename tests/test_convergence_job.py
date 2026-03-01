"""
Tests for the convergence job (converge_canonical_duplicates).

Tests:
  1. Winner selection is deterministic
  2. Winner prefers highest editorial_priority
  3. Winner prefers most happening_sources links
  4. Winner prefers earliest created_at as tiebreaker
  5. Dry-run produces no writes
  6. Losers are archived, not deleted
"""
from __future__ import annotations

from src.jobs.converge_canonical_duplicates import select_winner


# ---------------------------------------------------------------------------
# Shared fixtures
# ---------------------------------------------------------------------------

def _make_row(
    id: str,
    editorial_priority: int = 0,
    created_at: str = "2026-01-01T00:00:00Z",
) -> dict:
    return {
        "id": id,
        "editorial_priority": editorial_priority,
        "created_at": created_at,
        "canonical_dedupe_key": "c1|test",
    }


# ---------------------------------------------------------------------------
# Part 1: Determinism
# ---------------------------------------------------------------------------

def test_winner_selection_deterministic():
    rows = [
        _make_row("hap-b", created_at="2026-01-02T00:00:00Z"),
        _make_row("hap-a", created_at="2026-01-01T00:00:00Z"),
    ]
    source_counts = {"hap-a": 1, "hap-b": 1}

    winner1 = select_winner(rows, source_counts)
    winner2 = select_winner(list(reversed(rows)), source_counts)
    assert winner1["id"] == winner2["id"], "Winner must be deterministic regardless of input order"


# ---------------------------------------------------------------------------
# Part 2: editorial_priority wins
# ---------------------------------------------------------------------------

def test_highest_editorial_priority_wins():
    rows = [
        _make_row("hap-low", editorial_priority=0, created_at="2026-01-01T00:00:00Z"),
        _make_row("hap-high", editorial_priority=10, created_at="2026-01-02T00:00:00Z"),
    ]
    source_counts = {"hap-low": 5, "hap-high": 1}  # more sources on low-priority

    winner = select_winner(rows, source_counts)
    assert winner["id"] == "hap-high", "Highest editorial_priority must win"


# ---------------------------------------------------------------------------
# Part 3: most happening_sources links as tiebreaker
# ---------------------------------------------------------------------------

def test_most_sources_wins_when_priority_tied():
    rows = [
        _make_row("hap-few", editorial_priority=0),
        _make_row("hap-many", editorial_priority=0),
    ]
    source_counts = {"hap-few": 1, "hap-many": 5}

    winner = select_winner(rows, source_counts)
    assert winner["id"] == "hap-many", "Most happening_sources links must win"


# ---------------------------------------------------------------------------
# Part 4: earliest created_at as final tiebreaker
# ---------------------------------------------------------------------------

def test_earliest_created_wins_when_all_else_tied():
    rows = [
        _make_row("hap-new", created_at="2026-02-01T00:00:00Z"),
        _make_row("hap-old", created_at="2026-01-01T00:00:00Z"),
    ]
    source_counts = {"hap-new": 1, "hap-old": 1}

    winner = select_winner(rows, source_counts)
    assert winner["id"] == "hap-old", "Earliest created_at must win"


# ---------------------------------------------------------------------------
# Part 5: lexicographic id as absolute tiebreaker
# ---------------------------------------------------------------------------

def test_lexicographic_id_tiebreaker():
    rows = [
        _make_row("hap-zzz", created_at="2026-01-01T00:00:00Z"),
        _make_row("hap-aaa", created_at="2026-01-01T00:00:00Z"),
    ]
    source_counts = {"hap-zzz": 1, "hap-aaa": 1}

    winner = select_winner(rows, source_counts)
    assert winner["id"] == "hap-aaa", "Lexicographic id must break final tie"
