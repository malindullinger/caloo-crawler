# tests/test_pipeline_summary.py
"""
Verify the [pipeline][summary] line is emitted at the end of a pipeline run.
"""
from __future__ import annotations

import re
from datetime import datetime, timezone
from unittest.mock import patch, MagicMock

import pytest

from src.models import RawEvent, NormalizedEvent


def _make_raw(source_id: str = "test_source", title: str = "Test Event") -> RawEvent:
    return RawEvent(
        source_id=source_id,
        source_url="https://example.com",
        item_url="https://example.com/1",
        title_raw=title,
        datetime_raw="2026-06-15 10:00",
        location_raw="Gemeindesaal",
        description_raw="A test event",
        extra={},
        fetched_at=datetime(2026, 1, 1, tzinfo=timezone.utc),
    )


def _make_normalized(source_id: str = "test_source") -> NormalizedEvent:
    return NormalizedEvent(
        external_id="ext-1",
        source_id=source_id,
        title="Test Event",
        start_at=datetime(2026, 6, 15, 10, 0, tzinfo=timezone.utc),
        timezone="Europe/Zurich",
        canonical_url="https://example.com/1",
        last_seen_at=datetime(2026, 1, 1, tzinfo=timezone.utc),
    )


_PATCHES = {
    "src.pipeline.fetch_and_extract": None,
    "src.pipeline.raw_to_normalized": None,
    "src.pipeline.store_raw": MagicMock(),
    "src.pipeline.upsert_event": MagicMock(),
    "src.pipeline.insert_schedules": MagicMock(),
}


def _reset_dedupe_counters():
    """Reset module-level dedupe counters between tests."""
    import src.storage as st
    st._DEDUPE_CONTENT = 0
    st._DEDUPE_FALLBACK = 0
    st._DEDUPE_ERROR = 0


@pytest.fixture(autouse=True)
def _clean_dedupe():
    _reset_dedupe_counters()
    yield
    _reset_dedupe_counters()


def test_summary_line_present(capsys: pytest.CaptureFixture[str]) -> None:
    """The [pipeline][summary] line must appear exactly once."""
    raws = [_make_raw(), _make_raw(title="Second Event")]

    with (
        patch("src.pipeline.fetch_and_extract", return_value=(raws, 1)),
        patch("src.pipeline.raw_to_normalized", side_effect=[_make_normalized(), _make_normalized()]),
        patch("src.pipeline.store_raw"),
        patch("src.pipeline.upsert_event"),
        patch("src.pipeline.insert_schedules"),
    ):
        from src.pipeline import main
        main()

    out = capsys.readouterr().out
    summary_lines = [l for l in out.splitlines() if "[pipeline][summary]" in l]
    assert len(summary_lines) == 1, f"Expected exactly 1 summary line, got {len(summary_lines)}"


def test_summary_counters_correct(capsys: pytest.CaptureFixture[str]) -> None:
    """Counters in the summary line must match what happened."""
    raws = [_make_raw(), _make_raw(title="Second"), _make_raw(title="Third")]

    # Second event fails normalization (returns None)
    normalized = [_make_normalized(), None, _make_normalized()]

    with (
        patch("src.pipeline.fetch_and_extract", return_value=(raws, 2)),
        patch("src.pipeline.raw_to_normalized", side_effect=normalized),
        patch("src.pipeline.store_raw"),
        patch("src.pipeline.upsert_event"),
        patch("src.pipeline.insert_schedules"),
    ):
        from src.pipeline import main
        main()

    out = capsys.readouterr().out
    summary = [l for l in out.splitlines() if "[pipeline][summary]" in l][0]

    assert "sources_run=2" in summary
    assert "extracted=3" in summary
    assert "normalized_written=2" in summary
    assert "source_upserted=2" in summary
    assert "normalize_failed=1" in summary
    assert "upsert_errors=0" in summary
    assert "errors=1" in summary


def test_summary_counts_upsert_errors(capsys: pytest.CaptureFixture[str]) -> None:
    """Upsert failures are counted and reflected in errors."""
    raws = [_make_raw(), _make_raw(title="Failing")]

    def upsert_side_effect(n: NormalizedEvent) -> None:
        if n.source_id == "fail_source":
            raise RuntimeError("DB down")

    norm_ok = _make_normalized()
    norm_fail = _make_normalized(source_id="fail_source")

    with (
        patch("src.pipeline.fetch_and_extract", return_value=(raws, 1)),
        patch("src.pipeline.raw_to_normalized", side_effect=[norm_ok, norm_fail]),
        patch("src.pipeline.store_raw"),
        patch("src.pipeline.upsert_event", side_effect=upsert_side_effect),
        patch("src.pipeline.insert_schedules"),
    ):
        from src.pipeline import main
        main()

    out = capsys.readouterr().out
    summary = [l for l in out.splitlines() if "[pipeline][summary]" in l][0]

    assert "source_upserted=1" in summary
    assert "upsert_errors=1" in summary
    assert "normalized_written=1" in summary
    assert "errors=1" in summary


def test_summary_with_zero_events(capsys: pytest.CaptureFixture[str]) -> None:
    """Empty pipeline run still emits a summary line with all zeros."""
    with (
        patch("src.pipeline.fetch_and_extract", return_value=([], 0)),
        patch("src.pipeline.store_raw"),
        patch("src.pipeline.upsert_event"),
        patch("src.pipeline.insert_schedules"),
    ):
        from src.pipeline import main
        main()

    out = capsys.readouterr().out
    summary = [l for l in out.splitlines() if "[pipeline][summary]" in l][0]

    assert "sources_run=0" in summary
    assert "extracted=0" in summary
    assert "normalized_written=0" in summary
    assert "source_upserted=0" in summary
    assert "errors=0" in summary


def test_summary_line_is_parseable(capsys: pytest.CaptureFixture[str]) -> None:
    """The summary line must be key=value pairs parseable by simple regex."""
    raws = [_make_raw()]

    with (
        patch("src.pipeline.fetch_and_extract", return_value=(raws, 1)),
        patch("src.pipeline.raw_to_normalized", return_value=_make_normalized()),
        patch("src.pipeline.store_raw"),
        patch("src.pipeline.upsert_event"),
        patch("src.pipeline.insert_schedules"),
    ):
        from src.pipeline import main
        main()

    out = capsys.readouterr().out
    summary = [l for l in out.splitlines() if "[pipeline][summary]" in l][0]

    # Must start with [pipeline][summary]
    assert summary.startswith("[pipeline][summary]")

    # All key=value pairs must be integers
    pairs = re.findall(r"(\w+)=(\d+)", summary)
    keys = {k for k, v in pairs}

    expected_keys = {
        "sources_run", "extracted", "normalized_written",
        "source_upserted", "normalize_failed", "upsert_errors",
        "dedupe_content", "dedupe_fallback", "dedupe_error", "errors",
    }
    assert expected_keys == keys, f"Missing keys: {expected_keys - keys}, extra: {keys - expected_keys}"
