# tests/test_storage.py
"""Unit tests for storage.build_events_row (pure, no DB)."""
from __future__ import annotations

from datetime import datetime, timezone

from src.models import NormalizedEvent
from src.storage import build_events_row


def _make_event(**overrides) -> NormalizedEvent:
    defaults = {
        "external_id": "abc123",
        "source_id": "maennedorf_portal",
        "title": "Kinderflohmarkt",
        "start_at": datetime(2026, 3, 15, 9, 0, tzinfo=timezone.utc),
        "end_at": datetime(2026, 3, 15, 13, 0, tzinfo=timezone.utc),
        "timezone": "Europe/Zurich",
        "location_name": "Gemeindesaal",
        "description": "Flohmarkt für Kinder",
        "canonical_url": "https://www.maennedorf.ch/_rte/anlass/500",
        "last_seen_at": datetime(2026, 2, 24, 10, 0, tzinfo=timezone.utc),
        "extra": {},
    }
    defaults.update(overrides)
    return NormalizedEvent(**defaults)


class TestBuildEventsRow:
    def test_extra_with_organizer_and_image(self):
        ev = _make_event(extra={
            "organizer_name": "Elternverein",
            "image_url": "https://example.com/img.jpg",
            "adapter": "maennedorf_portal",
        })
        row = build_events_row(ev)

        assert row["extra"] is not None
        assert isinstance(row["extra"], dict)
        assert row["extra"]["organizer_name"] == "Elternverein"
        assert row["extra"]["image_url"] == "https://example.com/img.jpg"
        assert row["extra"]["adapter"] == "maennedorf_portal"
        assert row["image_url"] == "https://example.com/img.jpg"

    def test_extra_empty_dict_becomes_none(self):
        ev = _make_event(extra={})
        row = build_events_row(ev)
        assert row["extra"] is None

    def test_image_url_from_extra(self):
        ev = _make_event(extra={"image_url": "  https://img.ch/photo.png  "})
        row = build_events_row(ev)
        assert row["image_url"] == "https://img.ch/photo.png"

    def test_no_image_url(self):
        ev = _make_event(extra={"adapter": "test"})
        row = build_events_row(ev)
        assert row["image_url"] is None
        assert row["extra"]["adapter"] == "test"

    def test_core_fields_mapped(self):
        ev = _make_event()
        row = build_events_row(ev)
        assert row["external_id"] == "abc123"
        assert row["source_id"] == "maennedorf_portal"
        assert row["title"] == "Kinderflohmarkt"
        assert row["canonical_url"] == "https://www.maennedorf.ch/_rte/anlass/500"
        assert row["location_name"] == "Gemeindesaal"
        assert row["timezone"] == "Europe/Zurich"
        assert row["start_at"] is not None
        assert row["last_seen_at"] is not None

    def test_date_precision_and_event_type(self):
        ev = _make_event(date_precision="date", event_type="date_range", is_all_day=True)
        row = build_events_row(ev)
        assert row["date_precision"] == "date"
        assert row["event_type"] == "date_range"
        assert row["is_all_day"] is True
