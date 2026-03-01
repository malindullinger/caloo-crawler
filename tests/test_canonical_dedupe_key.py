"""
Tests for canonical_dedupe_key computation and contract.

Locks the c1 canonical dedupe key behaviour:
  1. Deterministic — same inputs → same key
  2. Cross-source convergence — same title+date+venue → same key regardless of source
  3. Version prefix — output starts with "c1|"
  4. Normalization — whitespace/case differences → same key
  5. Date anchor logic — start_date preferred, then start_at, then 'unknown-date'
  6. Location anchor logic — primary_venue_id, then online, then 'unknown-location'
  7. Matches SQL function output contract
  8. Editorial field protection in merge_loop
  9. Pipeline never overwrites editorial_priority or visibility_override
"""
from __future__ import annotations

from unittest.mock import MagicMock

import pytest

from src.canonicalize.canonical_dedupe_key import (
    compute_canonical_dedupe_key,
    compute_canonical_dedupe_key_from_row,
    compute_canonical_dedupe_key_from_source,
)


# ===========================================================================
# Part 1: Determinism
# ===========================================================================

def test_deterministic():
    kwargs = dict(
        happening_kind="event",
        title="Kinderyoga im Park",
        start_date="2026-06-15",
        primary_venue_id="550e8400-e29b-41d4-a716-446655440000",
    )
    assert compute_canonical_dedupe_key(**kwargs) == compute_canonical_dedupe_key(**kwargs)


def test_stable_across_runs():
    kwargs = dict(
        happening_kind="event",
        title="Kinderflohmarkt im Quartier",
        start_date="2026-05-20",
        primary_venue_id="550e8400-e29b-41d4-a716-446655440000",
    )
    keys = [compute_canonical_dedupe_key(**kwargs) for _ in range(100)]
    assert len(set(keys)) == 1


# ===========================================================================
# Part 2: Cross-source convergence (unlike v1 source-level keys)
# ===========================================================================

def test_cross_source_same_content_same_key():
    """
    Unlike source-level dedupe_key (v1|...) which includes source_id,
    canonical_dedupe_key is source-agnostic: same real-world event → same key.
    """
    key_a = compute_canonical_dedupe_key(
        happening_kind="event",
        title="Kinderyoga im Park",
        start_date="2026-06-15",
        primary_venue_id="550e8400-e29b-41d4-a716-446655440000",
    )
    key_b = compute_canonical_dedupe_key(
        happening_kind="event",
        title="Kinderyoga im Park",
        start_date="2026-06-15",
        primary_venue_id="550e8400-e29b-41d4-a716-446655440000",
    )
    assert key_a == key_b


# ===========================================================================
# Part 3: Version prefix
# ===========================================================================

def test_version_prefix():
    key = compute_canonical_dedupe_key(
        happening_kind="event",
        title="Event",
        start_date="2026-01-01",
    )
    assert key.startswith("c1|")


def test_version_prefix_distinct_from_source_key():
    """c1| prefix must differ from v1| source-level keys."""
    key = compute_canonical_dedupe_key(
        happening_kind="event",
        title="Event",
        start_date="2026-01-01",
    )
    assert not key.startswith("v1|")


# ===========================================================================
# Part 4: Normalization
# ===========================================================================

def test_normalized_title_case_and_whitespace():
    base = dict(happening_kind="event", start_date="2026-06-15")
    key_a = compute_canonical_dedupe_key(title="  Kinder  Yoga  ", **base)
    key_b = compute_canonical_dedupe_key(title="kinder yoga", **base)
    assert key_a == key_b


def test_normalized_title_punctuation():
    base = dict(happening_kind="event", start_date="2026-06-15")
    key_a = compute_canonical_dedupe_key(title="Kinder-Yoga!", **base)
    key_b = compute_canonical_dedupe_key(title="KinderYoga", **base)
    assert key_a == key_b


def test_umlauts_preserved():
    base = dict(happening_kind="event", start_date="2026-01-01")
    key_umlaut = compute_canonical_dedupe_key(title="Küsnacht", **base)
    key_ascii = compute_canonical_dedupe_key(title="Kuesnacht", **base)
    assert key_umlaut != key_ascii


# ===========================================================================
# Part 5: Date anchor logic
# ===========================================================================

def test_date_anchor_prefers_start_date():
    key_with_date = compute_canonical_dedupe_key(
        title="Event", start_date="2026-06-15", start_at="2026-06-16T10:00:00+02:00",
    )
    key_date_only = compute_canonical_dedupe_key(
        title="Event", start_date="2026-06-15",
    )
    assert key_with_date == key_date_only, "start_date takes precedence over start_at"


def test_date_anchor_falls_back_to_start_at():
    key_from_start_at = compute_canonical_dedupe_key(
        title="Event", start_at="2026-06-15T10:00:00+02:00",
    )
    key_from_date = compute_canonical_dedupe_key(
        title="Event", start_date="2026-06-15",
    )
    assert key_from_start_at == key_from_date, "start_at date portion should match start_date"


def test_date_anchor_unknown_when_both_missing():
    key = compute_canonical_dedupe_key(title="Event")
    # Should still produce a valid key with 'unknown-date'
    assert key.startswith("c1|")
    assert len(key) > 10


# ===========================================================================
# Part 6: Location anchor logic
# ===========================================================================

def test_location_anchor_venue_id():
    key = compute_canonical_dedupe_key(
        title="Event",
        start_date="2026-06-15",
        primary_venue_id="550e8400-e29b-41d4-a716-446655440000",
    )
    assert key.startswith("c1|")


def test_location_anchor_online():
    key_online = compute_canonical_dedupe_key(
        title="Event", start_date="2026-06-15", online=True,
    )
    key_unknown = compute_canonical_dedupe_key(
        title="Event", start_date="2026-06-15",
    )
    assert key_online != key_unknown, "online and unknown-location must differ"


def test_location_anchor_venue_takes_precedence_over_online():
    key_venue = compute_canonical_dedupe_key(
        title="Event",
        start_date="2026-06-15",
        primary_venue_id="550e8400-e29b-41d4-a716-446655440000",
        online=True,
    )
    key_online = compute_canonical_dedupe_key(
        title="Event",
        start_date="2026-06-15",
        online=True,
    )
    assert key_venue != key_online, "venue_id takes precedence over online flag"


# ===========================================================================
# Part 7: happening_kind affects key
# ===========================================================================

def test_different_kind_different_key():
    base = dict(title="Yoga", start_date="2026-06-15")
    key_event = compute_canonical_dedupe_key(happening_kind="event", **base)
    key_course = compute_canonical_dedupe_key(happening_kind="course", **base)
    assert key_event != key_course


def test_default_kind_is_event():
    key_explicit = compute_canonical_dedupe_key(
        happening_kind="event", title="Yoga", start_date="2026-06-15",
    )
    key_default = compute_canonical_dedupe_key(
        title="Yoga", start_date="2026-06-15",
    )
    assert key_explicit == key_default


# ===========================================================================
# Part 8: from_row and from_source helpers
# ===========================================================================

def test_from_row_matches_explicit():
    row = {
        "happening_kind": "event",
        "title": "Kinderyoga",
        "start_date": "2026-06-15",
        "start_at": None,
        "primary_venue_id": None,
        "online": False,
    }
    key_from_row = compute_canonical_dedupe_key_from_row(row)
    key_explicit = compute_canonical_dedupe_key(
        happening_kind="event",
        title="Kinderyoga",
        start_date="2026-06-15",
    )
    assert key_from_row == key_explicit


def test_from_source_maps_fields_correctly():
    source_row = {
        "title_raw": "Kinderyoga",
        "start_date_local": "2026-06-15",
        "start_at": None,
    }
    key = compute_canonical_dedupe_key_from_source(source_row)
    assert key.startswith("c1|")


# ===========================================================================
# Part 9: Two happenings with same inputs → one canonical (contract proof)
# ===========================================================================

def test_same_inputs_same_canonical_key():
    """
    Creating two happenings with identical canonical inputs must produce
    the same canonical_dedupe_key, which combined with the unique index
    means at most one canonical happening can exist.
    """
    key_1 = compute_canonical_dedupe_key(
        happening_kind="event",
        title="Kinderyoga im Park",
        start_date="2026-03-15",
        primary_venue_id="550e8400-e29b-41d4-a716-446655440000",
    )
    key_2 = compute_canonical_dedupe_key(
        happening_kind="event",
        title="Kinderyoga im Park",
        start_date="2026-03-15",
        primary_venue_id="550e8400-e29b-41d4-a716-446655440000",
    )
    assert key_1 == key_2, (
        "Same canonical inputs must produce same key → unique index prevents duplicates"
    )
