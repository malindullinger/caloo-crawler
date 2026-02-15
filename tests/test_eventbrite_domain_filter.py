from src.sources.adapters.eventbrite import _is_allowed_eventbrite_domain


def test_allowed_domains_pass():
    assert _is_allowed_eventbrite_domain("https://www.eventbrite.com/e/some-event-123") is True
    assert _is_allowed_eventbrite_domain("https://www.eventbrite.ch/e/some-event-123") is True
    assert _is_allowed_eventbrite_domain("https://www.eventbrite.de/e/some-event-123") is True
    assert _is_allowed_eventbrite_domain("https://www.eventbrite.co.uk/e/some-event-123") is True
    assert _is_allowed_eventbrite_domain("https://www.eventbrite.at/e/some-event-123") is True
    assert _is_allowed_eventbrite_domain("https://www.eventbrite.ca/e/some-event-123") is True


def test_disallowed_domains_rejected():
    assert _is_allowed_eventbrite_domain("https://www.eventbrite.com.mx/e/some-event-123") is False
    assert _is_allowed_eventbrite_domain("https://www.eventbrite.es/e/some-event-123") is False
    assert _is_allowed_eventbrite_domain("https://www.eventbrite.com.ar/e/some-event-123") is False
    assert _is_allowed_eventbrite_domain("https://www.eventbrite.com.br/e/some-event-123") is False
    assert _is_allowed_eventbrite_domain("https://www.eventbrite.fr/e/some-event-123") is False


def test_edge_cases():
    assert _is_allowed_eventbrite_domain("") is False
    assert _is_allowed_eventbrite_domain("not-a-url") is False
