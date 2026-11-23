"""Validated user-facing recurrence and durable occurrence calculations."""

from datetime import UTC, datetime, time, timedelta
from datetime import date as CalendarDate
from typing import Annotated, Literal
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError

from apscheduler.triggers.cron import CronTrigger
from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator

from egis_agentkit.core.agent._runtime import DEFAULT_TIMEZONE

BACKFILL_WINDOW = timedelta(hours=24)
WEEKDAY_NAMES = "mon tue wed thu fri sat sun".split()
Minute = Annotated[str, Field(pattern=r"^(?:[01]\d|2[0-3]):[0-5]\d$")]
Weekday = Annotated[int, Field(ge=0, le=6, strict=True)]


class _Rule(BaseModel):
    model_config = ConfigDict(extra="forbid")


class Once(_Rule):
    kind: Literal["once"]
    date: CalendarDate
    time: Minute


class Interval(_Rule):
    kind: Literal["interval"]
    date: CalendarDate | None = None
    time: Minute | None = None
    interval_seconds: int = Field(ge=60, le=31 * 86400, strict=True)


class Daily(_Rule):
    kind: Literal["daily"]
    time: Minute


class Weekly(_Rule):
    kind: Literal["weekly"]
    time: Minute
    weekdays: list[Weekday] = Field(min_length=1, max_length=7)

    @field_validator("weekdays")
    @classmethod
    def normalize_weekdays(cls, days):
        return sorted(set(days))


class Biweekly(Weekly):
    kind: Literal["biweekly"]
    date: CalendarDate | None = None


class Monthly(_Rule):
    kind: Literal["monthly"]
    time: Minute
    day: int = Field(ge=1, le=31, strict=True)


class Yearly(Monthly):
    kind: Literal["yearly"]
    month: int = Field(ge=1, le=12, strict=True)

    @model_validator(mode="after")
    def check_date(self):
        CalendarDate(2000, self.month, self.day)
        return self


Recurrence = Annotated[
    Once | Interval | Daily | Weekly | Biweekly | Monthly | Yearly,
    Field(discriminator="kind"),
]


class ScheduleTiming(BaseModel):
    """The time rule shared by creation, editing and preview."""

    model_config = ConfigDict(extra="forbid", str_strip_whitespace=True)
    timezone: str = Field(default=DEFAULT_TIMEZONE, min_length=1, max_length=64)
    enabled: bool = True
    starts_on: CalendarDate | None = None
    ends_on: CalendarDate | None = None
    rule: Recurrence

    @field_validator("timezone")
    @classmethod
    def validate_zone(cls, value):
        try:
            ZoneInfo(value)
        except (ZoneInfoNotFoundError, ValueError):
            raise ValueError("请选择有效时区") from None
        return value

    @model_validator(mode="after")
    def validate_window(self):
        if self.starts_on and self.ends_on and self.ends_on < self.starts_on:
            raise ValueError("结束日期不能早于开始日期")
        if isinstance(self.rule, Once):
            if self.starts_on and self.rule.date < self.starts_on:
                raise ValueError("执行日期早于有效期")
            if self.ends_on and self.rule.date > self.ends_on:
                raise ValueError("执行日期晚于有效期")
        return self


class ScheduleInput(ScheduleTiming):
    """Shared form and chat tool schema."""

    name: str = Field(min_length=1, max_length=80)
    description: str = Field(min_length=1, max_length=4000)
    model_key: str | None = Field(default=None, min_length=1, max_length=128)
    permission_mode: Literal["dont_ask", "explore"] = "dont_ask"
    stateful: bool = False
    files: list[Annotated[str, Field(min_length=1, max_length=512)]] = Field(
        default_factory=list, max_length=50
    )


class ScheduleSettings(ScheduleInput):
    project_id: str = Field(min_length=1, max_length=64)

    @classmethod
    def from_record(cls, row):
        return cls.model_validate(
            {name: row[name] for name in cls.model_fields if name in row}
        )


def normalize_rule(schedule: ScheduleTiming, now: datetime) -> ScheduleTiming:
    rule = schedule.rule
    if not isinstance(rule, (Interval, Biweekly)):
        return schedule
    local_now = now.astimezone(ZoneInfo(schedule.timezone))
    anchor = rule.date or schedule.starts_on or local_now.date()
    fields = {"date": anchor}
    if isinstance(rule, Interval):
        fields["time"] = rule.time or (
            "00:00" if rule.date or schedule.starts_on else local_now.strftime("%H:%M")
        )
    return schedule.model_copy(update={"rule": rule.model_copy(update=fields)})


def cron_expression(rule: Recurrence) -> str | None:
    if rule.kind not in {"daily", "weekly", "monthly", "yearly"}:
        return None
    hour, minute = map(int, rule.time.split(":"))
    days = ",".join(str((value + 1) % 7) for value in getattr(rule, "weekdays", ()))
    return f"{minute} {hour} {getattr(rule, 'day', '*')} {getattr(rule, 'month', '*')} {days or '*'}"


def _next_occurrence(schedule: ScheduleTiming, after: datetime, zone: ZoneInfo):
    rule = schedule.rule
    if isinstance(rule, Once):
        return datetime.combine(rule.date, time.fromisoformat(rule.time), zone)
    if isinstance(rule, Interval):
        anchor = datetime.combine(
            rule.date, time.fromisoformat(rule.time), zone
        ).astimezone(UTC)
        interval = timedelta(seconds=rule.interval_seconds)
        elapsed = max(0, (after - anchor) // interval + 1)
        return anchor + elapsed * interval
    if isinstance(rule, Biweekly):
        local_day = after.astimezone(zone).date()
        anchor_week = rule.date - timedelta(days=rule.date.weekday())
        candidate_time = time.fromisoformat(rule.time)
        first_day = max(local_day, rule.date)
        for day_index in range(15):
            day = first_day + timedelta(days=day_index)
            week = (day - anchor_week).days // 7
            if week % 2 == 0 and day.weekday() in rule.weekdays:
                candidate = datetime.combine(day, candidate_time, zone)
                if candidate.astimezone(UTC) > after:
                    return candidate
        return None
    hour, minute = map(int, rule.time.split(":"))
    days = ",".join(WEEKDAY_NAMES[value] for value in getattr(rule, "weekdays", ()))
    trigger = CronTrigger(
        minute=minute,
        hour=hour,
        day=getattr(rule, "day", "*"),
        month=getattr(rule, "month", "*"),
        day_of_week=days or "*",
        timezone=zone,
    )
    return trigger.get_next_fire_time(None, after + timedelta(microseconds=1))


def next_occurrence(schedule: ScheduleTiming, after: datetime) -> datetime | None:
    """The first eligible instant strictly after the input timestamp."""
    if not schedule.enabled:
        return None
    zone = ZoneInfo(schedule.timezone)
    schedule = normalize_rule(schedule, after)
    threshold = after.astimezone(UTC)
    if schedule.starts_on:
        start = datetime.combine(schedule.starts_on, time.min, zone).astimezone(UTC)
        threshold = max(threshold, start - timedelta(microseconds=1))
    candidate = _next_occurrence(schedule, threshold, zone)
    if (
        candidate is None
        or candidate.astimezone(UTC) <= threshold
        or schedule.ends_on
        and candidate.astimezone(zone).date() > schedule.ends_on
    ):
        return None
    return candidate.astimezone(UTC)


def due_occurrence(
    schedule: ScheduleTiming,
    cursor: datetime,
    now: datetime,
) -> tuple[datetime | None, datetime | None]:
    """Only recover the latest occurrence within the approved outage window."""
    upcoming = next_occurrence(schedule, now)
    if not schedule.enabled or (
        schedule.ends_on
        and now.astimezone(ZoneInfo(schedule.timezone)).date() > schedule.ends_on
    ):
        return None, upcoming
    lower = max(cursor, now - BACKFILL_WINDOW)
    candidate = next_occurrence(schedule, lower - timedelta(microseconds=1))
    latest = None
    while candidate is not None and candidate <= now:
        latest = candidate
        candidate = next_occurrence(schedule, candidate)
    return latest, upcoming
