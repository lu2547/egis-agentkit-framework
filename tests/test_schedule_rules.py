from datetime import UTC, datetime

import pytest

from egis_agentkit.service.schedule._rules import (
    ScheduleSettings,
    cron_expression,
    due_occurrence,
    next_occurrence,
)


def task(rule, **settings):
    return ScheduleSettings.model_validate(
        {
            "name": "检查材料",
            "description": "核对材料清单",
            "project_id": "project",
            "rule": rule,
            **settings,
        }
    )


def test_weekly_and_monthly_use_local_calendar():
    monthly = task({"kind": "monthly", "time": "09:00", "day": 31})
    assert next_occurrence(monthly, datetime(2026, 2, 1, tzinfo=UTC)) == datetime(
        2026, 3, 31, 1, tzinfo=UTC
    )
    weekly = task({"kind": "weekly", "time": "17:00", "weekdays": [4, 0]})
    assert cron_expression(weekly.rule) == "0 17 * * 1,5"
    assert next_occurrence(weekly, datetime(2026, 11, 23, 9, tzinfo=UTC)) == datetime(
        2026, 11, 27, 9, tzinfo=UTC
    )


def test_biweekly_anchor_survives_year_boundary():
    schedule = task(
        {"kind": "biweekly", "date": "2026-12-21", "time": "09:00", "weekdays": [0, 4]}
    )
    assert next_occurrence(schedule, datetime(2026, 12, 25, 1, tzinfo=UTC)) == datetime(
        2027, 1, 4, 1, tzinfo=UTC
    )


def test_due_interval_backfills_only_latest_within_window():
    schedule = task(
        {
            "kind": "interval",
            "date": "2026-11-20",
            "time": "09:00",
            "interval_seconds": 3600,
        }
    )
    now = datetime(2026, 11, 23, 4, 42, tzinfo=UTC)
    assert due_occurrence(schedule, datetime(2026, 11, 21, 1, tzinfo=UTC), now) == (
        datetime(2026, 11, 23, 4, tzinfo=UTC),
        datetime(2026, 11, 23, 5, tzinfo=UTC),
    )


def test_once_does_not_fire_twice_or_backfill_outside_validity():
    schedule = task(
        {"kind": "once", "date": "2026-11-23", "time": "09:00"}, ends_on="2026-11-23"
    )
    moment = datetime(2026, 11, 23, 1, tzinfo=UTC)
    assert next_occurrence(schedule, moment) is None
    assert due_occurrence(schedule, moment, datetime(2026, 11, 23, 17, tzinfo=UTC)) == (
        None,
        None,
    )


@pytest.mark.parametrize(
    "rule",
    [
        {"kind": "daily", "time": "09:00", "interval_seconds": 60},
        {"kind": "weekly", "time": "09:00", "weekdays": []},
        {"kind": "once", "time": "24:00", "date": "2026-11-23"},
        {"kind": "yearly", "time": "09:00", "month": 2, "day": 30},
    ],
)
def test_rejects_invalid_or_mixed_schedule_rules(rule):
    with pytest.raises(ValueError):
        task(rule)
