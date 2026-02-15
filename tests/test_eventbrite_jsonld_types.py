import json

from bs4 import BeautifulSoup

from src.sources.adapters.eventbrite import EventbriteAdapter


def _soup_with_jsonld(obj) -> BeautifulSoup:
    html = f'<html><head><script type="application/ld+json">{json.dumps(obj)}</script></head></html>'
    return BeautifulSoup(html, "html.parser")


def test_eventbrite_jsonld_title_accepts_social_event():
    adapter = EventbriteAdapter()
    soup = _soup_with_jsonld({"@type": "SocialEvent", "name": "My Event"})
    assert adapter._get_title_from_jsonld(soup) == "My Event"


def test_eventbrite_jsonld_location_accepts_business_event():
    adapter = EventbriteAdapter()
    soup = _soup_with_jsonld({
        "@type": "BusinessEvent",
        "name": "X",
        "location": {
            "name": "Prime Tower",
            "address": {"addressLocality": "Zürich", "postalCode": "8005"}
        }
    })
    loc = adapter._get_location_from_jsonld(soup)
    assert "Prime Tower" in (loc or "")
    assert "8005" in (loc or "")
    assert "Zürich" in (loc or "")
