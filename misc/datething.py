# coding=utf-8
from __future__ import unicode_literals

from datetime import datetime


DATE_FORMAT = "%m/%d"


def convert_date(date_str):
    return datetime.strptime(date_str, DATE_FORMAT)


def get_closest_date(target, others):
    """
    Accepts a date and an iterable of dates and returns the date from the iterable that is closest to the target.

    All date values are strings in mm/dd format. February 29th does not exist.
    """
    target = convert_date(target)
    _, closest = min((abs(target - x), x) for x in map(convert_date, others))
    return closest.strftime(DATE_FORMAT)
