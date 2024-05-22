import datetime


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
        case (0, 0, 0, 1):
            return "1 second ago"
        case (0, 0, 0, seconds_since):
            return f"{seconds_since} seconds ago"
        case (0, 0, 1, seconds_since):
            return f"1 minute, {seconds_since} seconds ago"
        case (0, 0, 2, seconds_since):
            return f"2 minutes, {seconds_since} seconds ago"
        case (0, 0, minutes_since, _):
            return f"{minutes_since} minutes ago"
        case (0, 1, minutes_since, _):
            return f"1 hour, {minutes_since} minutes ago"
        case (0, 2, minutes_since, _):
            return f"2 hours, {minutes_since} minutes ago"
        case (0, hours_since, _, _):
            return f"{hours_since} hours ago"
        case (1, hours_since, _, _):
            return f"1 day, {hours_since} hours ago"
        case (2, hours_since, _, _):
            return f"2 days, {hours_since} hours ago"
        case (days_since, _, _, _):
            return f"{days_since} days ago"


def _format_datetime(date: datetime.datetime) -> str:
    now = datetime.datetime.now(datetime.timezone.utc)
    seconds_since = (now - date).total_seconds()
    if 0 < seconds_since < 24*60*60:
        return date.strftime("%H:%M %Z")
    return date.strftime("%Y-%m-%d %H:%M %Z")


def format_last_check(date: datetime.datetime) -> str:
    return f"{_format_time_since(date)} ({_format_datetime(date)})"
