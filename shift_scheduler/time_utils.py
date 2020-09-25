import calendar
import datetime
from typing import Iterable


def get_all_days_in_month(year: int, month: int) -> Iterable[datetime.date]:
    return (
        datetime.date(year, month, day)
        for day in range(1, calendar.monthrange(year, month)[1] + 1)
    )


def date_to_nice_str(date: datetime.date):
    return date.strftime("%Y-%m-%d %A")


def is_friday(date: datetime.date):
    return date.weekday() == 4


def is_saturday(date: datetime.date):
    return date.weekday() == 5


def is_sunday(date: datetime.date):
    return date.weekday() == 6
