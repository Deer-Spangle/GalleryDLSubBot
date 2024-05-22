import datetime


def _pluralise(num: int, noun: str) -> str:
    if num == 1:
        return f"{num} {noun}"
    return f"{num} {noun}s"


def _seconds(num: int) -> str:
    return _pluralise(num, "second")


def _minutes(num: int) -> str:
    return _pluralise(num, "minute")


def _hours(num: int) -> str:
    return _pluralise(num, "hour")


def _days(num: int) -> str:
    return _pluralise(num, "day")


def _format_time_since(date: datetime.datetime) -> str:
    now = datetime.datetime.now(datetime.timezone.utc)
    seconds_since = int((now - date).total_seconds())
    minutes_since, seconds_since = divmod(seconds_since, 60)
    hours_since, minutes_since = divmod(minutes_since, 60)
    days_since, hours_since = divmod(hours_since, 24)
    match (days_since, hours_since, minutes_since, seconds_since):
        case (0, 0, 0, seconds_since) if seconds_since < 0:
            return "In the future?"
        case (0, 0, 0, 0):
            return "Now"
        case (0, 0, 0, seconds_since):
            return f"{_seconds(seconds_since)} ago"
        case (0, 0, 1 | 2 as minutes_since, seconds_since):
            return f"{_minutes(minutes_since)}, {_seconds(seconds_since)} ago"
        case (0, 0, minutes_since, _):
            return f"{_minutes(minutes_since)} ago"
        case (0, 1 | 2 as hours_since, minutes_since, _):
            return f"{_hours(hours_since)}, {_minutes(minutes_since)} ago"
        case (0, hours_since, _, _):
            return f"{hours_since} ago"
        case (1 | 2 as days_since, hours_since, _, _):
            return f"{_days(days_since)}, {_hours(hours_since)} ago"
        case (days_since, _, _, _):
            return f"{_days(days_since)} ago"


def _format_datetime(date: datetime.datetime) -> str:
    now = datetime.datetime.now(datetime.timezone.utc)
    seconds_since = (now - date).total_seconds()
    if 0 < seconds_since < 24*60*60:
        return date.strftime("%H:%M %Z")
    return date.strftime("%Y-%m-%d %H:%M %Z")


def format_last_check(date: datetime.datetime) -> str:
    return f"{_format_time_since(date)} ({_format_datetime(date)})"
