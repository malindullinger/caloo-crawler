"""Tests for resolve_eventbrite_image_url helper."""
from src.sources.adapters.eventbrite import resolve_eventbrite_image_url


def test_next_image_proxy_decodes_to_cdn():
    """Real sample from pipeline log: double-encoded CDN URL inside _next/image proxy."""
    raw = (
        "/e/_next/image?url=https%3A%2F%2Fimg.evbuc.com%2Fhttps%253A%252F%252Fcdn.evbuc.com"
        "%252Fimages%252F1172300786%252F105570843895%252F1%252Foriginal.20251204-104831"
        "%3Fcrop%3Dfocalpoint%26fit%3Dcrop%26w%3D480%26auto%3Dformat%252Ccompress%26q%3D75"
        "%26sharp%3D10%26fp-x%3D0.498%26fp-y%3D0.62%26s%3D49d015dfd878b5400276e7218fda73d8"
        "&w=940&q=75"
    )
    result = resolve_eventbrite_image_url(raw, "https://www.eventbrite.com/e/some-event-123")
    # Should resolve to CDN URL, not the proxy
    assert result is not None
    assert result.startswith("https://img.evbuc.com/https://cdn.evbuc.com/images/")
    assert "_next/image" not in result


def test_absolute_url_passthrough():
    """Already-absolute URLs are returned as-is."""
    url = "https://img.evbuc.com/https://cdn.evbuc.com/images/123/456/1/original.jpg"
    assert resolve_eventbrite_image_url(url) == url


def test_relative_url_made_absolute():
    """Relative paths without _next/image are made absolute."""
    raw = "/images/event/123.jpg"
    result = resolve_eventbrite_image_url(raw, "https://www.eventbrite.ch/e/test-123")
    assert result == "https://www.eventbrite.ch/images/event/123.jpg"


def test_relative_url_default_domain():
    """Relative paths without page_url use eventbrite.com."""
    raw = "/images/event/123.jpg"
    result = resolve_eventbrite_image_url(raw)
    assert result == "https://www.eventbrite.com/images/event/123.jpg"


def test_none_and_empty():
    """None and empty strings return None."""
    assert resolve_eventbrite_image_url(None) is None
    assert resolve_eventbrite_image_url("") is None
    assert resolve_eventbrite_image_url("  ") is None
