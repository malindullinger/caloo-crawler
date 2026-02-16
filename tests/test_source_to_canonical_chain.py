"""
Regression tests: source_happening → merge loop → canonical chain.

Verifies that source records from eventbrite_zurich and elternverein_uetikon
create published happenings with full canonical chains (happening → offering →
occurrence → happening_sources) when processed by the merge loop's CREATE path.

Root cause of the original bug: create_happening_schedule_occurrence() set
visibility_status='draft', so new happenings never appeared in the feed.
"""
from __future__ import annotations

from unittest.mock import MagicMock


# ---------------------------------------------------------------------------
# Fixtures: realistic source rows for each source
# ---------------------------------------------------------------------------

EVENTBRITE_SOURCE_ROW = {
    "id": "src-eb-001",
    "source_id": "eventbrite_zurich",
    "dedupe_key": "v1|eb_hash_abc",
    "status": "queued",
    "title_raw": "Familien-Brunch am See",
    "description_raw": "Gemütlicher Brunch für die ganze Familie am Zürichsee.",
    "start_date_local": "2026-03-21",
    "end_date_local": "2026-03-21",
    "location_raw": "Seerestaurant Zürichhorn",
    "timezone": "Europe/Zurich",
    "start_at": "2026-03-21T10:00:00+01:00",
    "end_at": "2026-03-21T13:00:00+01:00",
    "image_url": "https://img.evbuc.com/example.jpg",
    "item_url": "https://www.eventbrite.com/e/familien-brunch-123",
    "external_id": "eb-123",
    "source_tier": "A",
}

ELTERNVEREIN_SOURCE_ROW = {
    "id": "src-ev-001",
    "source_id": "elternverein_uetikon",
    "dedupe_key": "v1|ev_hash_xyz",
    "status": "queued",
    "title_raw": "Kinderfest Uetikon",
    "description_raw": None,
    "start_date_local": "2026-04-05",
    "end_date_local": "2026-04-05",
    "location_raw": "Schulhaus Uetikon",
    "timezone": "Europe/Zurich",
    "start_at": "2026-04-05T14:00:00+02:00",
    "end_at": "2026-04-05T17:00:00+02:00",
    "image_url": "https://elternverein-uetikon.ch/img/kinderfest.jpg",
    "item_url": "https://elternverein-uetikon.ch/veranstaltungen/kinderfest",
    "external_id": None,
    "source_tier": "A",
}


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _mock_supabase():
    """Mock where all chain methods return the builder. Returns (sb, builder)."""
    sb = MagicMock()
    builder = MagicMock()
    for method in [
        "select", "like", "in_", "order", "limit",
        "lte", "gte", "eq", "update", "insert", "upsert",
    ]:
        getattr(builder, method).return_value = builder
    sb.table.return_value = builder
    result = MagicMock()
    result.data = [{"id": "mock-id-1"}]
    builder.execute.return_value = result
    return sb, builder


def _mock_supabase_per_table():
    """Mock with per-table builders for verifying table access."""
    sb = MagicMock()
    tables: dict[str, MagicMock] = {}

    def table_factory(name: str) -> MagicMock:
        if name not in tables:
            builder = MagicMock()
            for method in [
                "select", "like", "in_", "order", "limit",
                "lte", "gte", "eq", "update", "insert", "upsert",
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
# Test 1: Eventbrite source → full canonical chain
# ===========================================================================

def test_eventbrite_source_creates_published_canonical_chain():
    """
    An eventbrite_zurich source_happening processed by the merge loop's
    CREATE path must produce:
      - happening (visibility_status='published')
      - offering
      - occurrence (with start_at + end_at)
      - happening_sources link (is_primary=True)
    """
    from src.canonicalize.merge_loop import (
        create_happening_schedule_occurrence,
        link_happening_source,
    )

    sb, tables = _mock_supabase_per_table()

    # Step 1: CREATE path produces happening + offering + occurrence
    happening_id = create_happening_schedule_occurrence(
        supabase=sb, source_row=EVENTBRITE_SOURCE_ROW,
    )

    # Verify happening payload
    happening_payload = tables["happening"].insert.call_args[0][0]
    assert happening_payload["visibility_status"] == "published", \
        "New happenings must be published, not draft"
    assert happening_payload["title"] == "Familien-Brunch am See"

    # Verify offering was created
    assert "offering" in tables, "Offering table must be accessed"
    offering_payload = tables["offering"].insert.call_args[0][0]
    assert offering_payload["start_date"] == "2026-03-21"
    assert offering_payload["timezone"] == "Europe/Zurich"

    # Verify occurrence was created (has start_at)
    assert "occurrence" in tables, "Occurrence table must be accessed"
    occurrence_payload = tables["occurrence"].insert.call_args[0][0]
    assert occurrence_payload["start_at"] == "2026-03-21T10:00:00+01:00"
    assert occurrence_payload["status"] == "scheduled"

    # Step 2: Link provenance
    link_happening_source(
        supabase=sb,
        happening_id=happening_id,
        source_row=EVENTBRITE_SOURCE_ROW,
        is_primary=True,
    )

    # Verify happening_sources upsert
    assert "happening_sources" in tables
    hs_payload = tables["happening_sources"].upsert.call_args[0][0]
    assert hs_payload["source_happening_id"] == "src-eb-001"
    assert hs_payload["is_primary"] is True
    assert hs_payload["source_priority"] == 300  # Tier A


# ===========================================================================
# Test 2: Elternverein source → full canonical chain
# ===========================================================================

def test_elternverein_source_creates_published_canonical_chain():
    """
    An elternverein_uetikon source_happening processed by the merge loop's
    CREATE path must produce:
      - happening (visibility_status='published')
      - offering
      - occurrence (with start_at + end_at)
      - happening_sources link (is_primary=True)
    """
    from src.canonicalize.merge_loop import (
        create_happening_schedule_occurrence,
        link_happening_source,
    )

    sb, tables = _mock_supabase_per_table()

    happening_id = create_happening_schedule_occurrence(
        supabase=sb, source_row=ELTERNVEREIN_SOURCE_ROW,
    )

    # Verify happening is published
    happening_payload = tables["happening"].insert.call_args[0][0]
    assert happening_payload["visibility_status"] == "published"
    assert happening_payload["title"] == "Kinderfest Uetikon"

    # Verify occurrence was created
    assert "occurrence" in tables
    occurrence_payload = tables["occurrence"].insert.call_args[0][0]
    assert occurrence_payload["start_at"] == "2026-04-05T14:00:00+02:00"

    # Link provenance
    link_happening_source(
        supabase=sb,
        happening_id=happening_id,
        source_row=ELTERNVEREIN_SOURCE_ROW,
        is_primary=True,
    )

    assert "happening_sources" in tables
    hs_payload = tables["happening_sources"].upsert.call_args[0][0]
    assert hs_payload["source_happening_id"] == "src-ev-001"
    assert hs_payload["is_primary"] is True


# ===========================================================================
# Test 3: Date-only source → no occurrence (correct behavior)
# ===========================================================================

def test_date_only_source_creates_no_occurrence():
    """
    A source_happening with date_precision='date' and start_at=None
    must NOT create an occurrence row (no placeholder times).
    The happening + offering are still created.
    """
    from src.canonicalize.merge_loop import create_happening_schedule_occurrence

    date_only_row = {
        **ELTERNVEREIN_SOURCE_ROW,
        "id": "src-dateonly",
        "start_at": None,
        "end_at": None,
    }

    sb, tables = _mock_supabase_per_table()
    create_happening_schedule_occurrence(supabase=sb, source_row=date_only_row)

    # Happening and offering created
    assert "happening" in tables
    assert "offering" in tables

    # Occurrence NOT created (no insert call)
    assert "occurrence" not in tables, \
        "Date-only source must NOT create an occurrence row"


# ===========================================================================
# Test 4: image_url stays in source, not in canonical happening
# ===========================================================================

def test_image_url_not_in_happening_payload():
    """
    image_url must not leak into the happening payload — it lives in
    source_happenings and is selected via best_source CTE at query time.
    """
    from src.canonicalize.merge_loop import create_happening_schedule_occurrence

    sb, tables = _mock_supabase_per_table()
    create_happening_schedule_occurrence(
        supabase=sb, source_row=EVENTBRITE_SOURCE_ROW,
    )

    happening_payload = tables["happening"].insert.call_args[0][0]
    assert "image_url" not in happening_payload
