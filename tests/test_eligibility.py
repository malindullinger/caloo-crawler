from datetime import datetime, date

from src.canonicalize.eligibility import is_feed_eligible


def base_happening(**overrides):
    h = {
        "title": "Kinder-Fasnacht",
        "date_precision": "date",
        "start_date_local": date(2026, 2, 14),
        "start_at": None,
        "end_at": None,
        "venue_name": "Gemeindesaal",
        "is_online": False,
        "status": "scheduled",
    }
    h.update(overrides)
    return h


def test_eligible_date_only_with_location():
    res = is_feed_eligible(base_happening())
    assert res.eligible is True
    assert res.reasons == []


def test_missing_title_not_eligible():
    res = is_feed_eligible(base_happening(title=""))
    assert res.eligible is False
    assert "missing_title" in res.reasons


def test_missing_date_and_datetime_not_eligible():
    res = is_feed_eligible(base_happening(start_date_local=None, start_at=None))
    assert res.eligible is False
    assert "missing_start_date_or_start_at" in res.reasons


def test_online_ok_without_location():
    res = is_feed_eligible(base_happening(venue_name=None, is_online=True))
    assert res.eligible is True


def test_cancelled_not_eligible():
    res = is_feed_eligible(base_happening(status="cancelled"))
    assert res.eligible is False
    assert "cancelled" in res.reasons


def test_time_contract_date_precision_requires_null_start_at():
    res = is_feed_eligible(
        base_happening(
            date_precision="date",
            start_at=datetime(2026, 2, 14, 10, 0),
        )
    )
    assert res.eligible is False
    assert "time_contract_violation_or_unknown_precision" in res.reasons


def test_time_contract_datetime_requires_start_at():
    res = is_feed_eligible(
        base_happening(
            date_precision="datetime",
            start_at=None,
            start_date_local=date(2026, 2, 14),
        )
    )
    assert res.eligible is False
    assert "time_contract_violation_or_unknown_precision" in res.reasons


def test_datetime_ok():
    res = is_feed_eligible(
        base_happening(
            date_precision="datetime",
            start_at=datetime(2026, 2, 14, 10, 0),
            end_at=datetime(2026, 2, 14, 12, 0),
        )
    )
    assert res.eligible is True


def test_newborn_only_flag_excluded():
    res = is_feed_eligible(base_happening(is_newborn_only=True))
    assert res.eligible is False
    assert "excluded_newborn_only_v1" in res.reasons


def test_newborn_only_age_range_excluded():
    res = is_feed_eligible(base_happening(min_age_months=0, max_age_months=6))
    assert res.eligible is False
    assert "excluded_newborn_only_v1" in res.reasons


def test_unknown_precision_fails_closed():
    res = is_feed_eligible(base_happening(date_precision=None))
    assert res.eligible is False
    assert "time_contract_violation_or_unknown_precision" in res.reasons
