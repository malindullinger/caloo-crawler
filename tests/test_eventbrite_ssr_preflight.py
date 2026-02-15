import json

from bs4 import BeautifulSoup

from src.sources.adapters.eventbrite import EventbriteAdapter


def _soup_with_jsonld(obj) -> BeautifulSoup:
    html = f'<html><head><script type="application/ld+json">{json.dumps(obj)}</script></head></html>'
    return BeautifulSoup(html, "html.parser")


def test_skip_when_country_is_spain():
    adapter = EventbriteAdapter()
    soup = _soup_with_jsonld({
        "@type": "EducationEvent",
        "name": "Some Event",
        "location": {
            "@type": "Place",
            "address": {
                "addressLocality": "Madrid",
                "addressCountry": "ES",
            }
        }
    })
    assert adapter._is_definitely_not_zurich_from_ssr(soup) is True


def test_skip_when_country_is_argentina():
    adapter = EventbriteAdapter()
    soup = _soup_with_jsonld({
        "@type": "SocialEvent",
        "name": "Some Event",
        "location": {
            "@type": "Place",
            "address": {
                "addressLocality": "Buenos Aires",
                "addressCountry": "AR",
            }
        }
    })
    assert adapter._is_definitely_not_zurich_from_ssr(soup) is True


def test_skip_when_city_not_zurich_no_country():
    adapter = EventbriteAdapter()
    soup = _soup_with_jsonld({
        "@type": "BusinessEvent",
        "name": "Some Event",
        "location": {
            "@type": "Place",
            "address": {
                "addressLocality": "Lima",
            }
        }
    })
    assert adapter._is_definitely_not_zurich_from_ssr(soup) is True


def test_keep_when_city_is_zurich():
    adapter = EventbriteAdapter()
    soup = _soup_with_jsonld({
        "@type": "SocialEvent",
        "name": "Some Event",
        "location": {
            "@type": "Place",
            "address": {
                "addressLocality": "Zürich",
                "addressCountry": "CH",
            }
        }
    })
    assert adapter._is_definitely_not_zurich_from_ssr(soup) is False


def test_keep_when_city_is_zurich_area():
    adapter = EventbriteAdapter()
    soup = _soup_with_jsonld({
        "@type": "SocialEvent",
        "name": "Some Event",
        "location": {
            "@type": "Place",
            "address": {
                "addressLocality": "Küsnacht",
                "addressCountry": "CH",
            }
        }
    })
    assert adapter._is_definitely_not_zurich_from_ssr(soup) is False


def test_keep_when_no_jsonld():
    adapter = EventbriteAdapter()
    soup = BeautifulSoup("<html><head></head></html>", "html.parser")
    assert adapter._is_definitely_not_zurich_from_ssr(soup) is False


def test_keep_when_no_location_in_jsonld():
    adapter = EventbriteAdapter()
    soup = _soup_with_jsonld({
        "@type": "SocialEvent",
        "name": "Some Event",
    })
    assert adapter._is_definitely_not_zurich_from_ssr(soup) is False


def test_keep_when_no_address_in_location():
    adapter = EventbriteAdapter()
    soup = _soup_with_jsonld({
        "@type": "SocialEvent",
        "name": "Some Event",
        "location": {
            "@type": "Place",
            "name": "Some Venue",
        }
    })
    assert adapter._is_definitely_not_zurich_from_ssr(soup) is False
