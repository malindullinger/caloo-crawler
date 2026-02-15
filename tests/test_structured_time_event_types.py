from src.sources.structured_time import _is_event_type


def test_is_event_type_accepts_event_subtypes():
    assert _is_event_type("Event") is True
    assert _is_event_type("SocialEvent") is True
    assert _is_event_type("EducationEvent") is True
    assert _is_event_type("BusinessEvent") is True


def test_is_event_type_accepts_list_of_types():
    assert _is_event_type(["Thing", "SocialEvent"]) is True
    assert _is_event_type(["Thing", "Event"]) is True


def test_is_event_type_rejects_non_event_types():
    assert _is_event_type("WebPage") is False
    assert _is_event_type(["WebPage", "Organization"]) is False
    assert _is_event_type(None) is False
