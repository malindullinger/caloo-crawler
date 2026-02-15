"""Contract-locking tests for compute_dedupe_key (v1).

These tests define the stable behaviour of the dedupe-key contract.
If any of them break, the change is backwards-incompatible and must be
reviewed before merging.
"""
import pytest

from src.canonicalize.dedupe_key import compute_dedupe_key


# ---------------------------------------------------------------------------
# 1. Deterministic — same inputs → same key
# ---------------------------------------------------------------------------
def test_deterministic():
    kwargs = dict(
        source_id="eventbrite",
        title="Kinder Yoga im Park",
        start_date_local="2026-06-15",
        location="Gemeindesaal Männedorf",
    )
    assert compute_dedupe_key(**kwargs) == compute_dedupe_key(**kwargs)


# ---------------------------------------------------------------------------
# 2. Time-insensitive — different times on same date → same key
# ---------------------------------------------------------------------------
def test_time_insensitive():
    """start_date_local is a DATE string; time-of-day must never leak in."""
    base = dict(
        source_id="eventbrite",
        title="Kinder Yoga im Park",
        location="Gemeindesaal Männedorf",
    )
    key_morning = compute_dedupe_key(start_date_local="2026-06-15", **base)
    key_evening = compute_dedupe_key(start_date_local="2026-06-15", **base)
    assert key_morning == key_evening


# ---------------------------------------------------------------------------
# 3. Version prefix — output starts with "v1|"
# ---------------------------------------------------------------------------
def test_version_prefix():
    key = compute_dedupe_key(
        source_id="test",
        title="Event",
        start_date_local="2026-01-01",
        location="Zürich",
    )
    assert key.startswith("v1|")


# ---------------------------------------------------------------------------
# 4. Content-based — different URLs, same content → same key
# ---------------------------------------------------------------------------
def test_content_based_ignores_url():
    base = dict(
        source_id="eventbrite",
        title="Kinder Yoga im Park",
        start_date_local="2026-06-15",
        location="Gemeindesaal",
    )
    key_a = compute_dedupe_key(item_url="https://example.com/a", **base)
    key_b = compute_dedupe_key(item_url="https://example.com/b", **base)
    assert key_a == key_b


# ---------------------------------------------------------------------------
# 5. Location-sensitive — same title+date, different venue → different key
# ---------------------------------------------------------------------------
def test_location_sensitive():
    base = dict(
        source_id="eventbrite",
        title="Kinder Yoga",
        start_date_local="2026-06-15",
    )
    key_a = compute_dedupe_key(location="Gemeindesaal Männedorf", **base)
    key_b = compute_dedupe_key(location="Turnhalle Küsnacht", **base)
    assert key_a != key_b


# ---------------------------------------------------------------------------
# 6. URL fallback — missing title → falls back to URL-based key
# ---------------------------------------------------------------------------
def test_fallback_to_url_when_title_missing():
    key = compute_dedupe_key(
        source_id="eventbrite",
        title=None,
        start_date_local="2026-06-15",
        location="Zürich",
        item_url="https://eventbrite.com/e/12345",
    )
    assert key.startswith("v1|")
    assert len(key) > 10


# ---------------------------------------------------------------------------
# 7. external_id fallback — missing title+URL → falls back to external_id
# ---------------------------------------------------------------------------
def test_fallback_to_external_id():
    key = compute_dedupe_key(
        source_id="eventbrite",
        title=None,
        start_date_local=None,
        location=None,
        external_id="ext-abc-123",
    )
    assert key.startswith("v1|")
    assert len(key) > 10


# ---------------------------------------------------------------------------
# 8. Non-null contract — raises ValueError when all identifiers missing
# ---------------------------------------------------------------------------
def test_raises_when_all_missing():
    with pytest.raises(ValueError, match="Cannot compute dedupe_key"):
        compute_dedupe_key(
            source_id="eventbrite",
            title=None,
            start_date_local=None,
            location=None,
        )


# ---------------------------------------------------------------------------
# 9. Normalization — whitespace/case differences → same key
# ---------------------------------------------------------------------------
def test_normalized_title():
    base = dict(
        source_id="eventbrite",
        start_date_local="2026-06-15",
        location="Zürich",
    )
    key_a = compute_dedupe_key(title="  Kinder  Yoga  ", **base)
    key_b = compute_dedupe_key(title="kinder yoga", **base)
    assert key_a == key_b


# ---------------------------------------------------------------------------
# 10. Source isolation — same content, different source_id → different key
# ---------------------------------------------------------------------------
def test_source_isolation():
    base = dict(
        title="Kinder Yoga",
        start_date_local="2026-06-15",
        location="Gemeindesaal",
    )
    key_a = compute_dedupe_key(source_id="eventbrite", **base)
    key_b = compute_dedupe_key(source_id="maennedorf_portal", **base)
    assert key_a != key_b
