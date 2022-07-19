import datetime
from typing import Tuple

Date = int


def previous_years(date: Date, years_back: int, cutoff: int = 630) -> Tuple[Date, Date]:
    # Find largest cutoff that's less than date.
    year, month_day = divmod(date, 10000)
    if month_day <= cutoff:
        year -= 1

    en = year * 10000 + cutoff
    st = en - 10000 * years_back

    return (st, en)


def previous_year(date: Date, cutoff: int = 630) -> Tuple[Date, Date]:
    return previous_years(date, 1, cutoff=cutoff)


def current_year(date: Date, cutoff: int = 630) -> Tuple[Date, Date]:
    # Find largest cutoff that's less than date.
    year, month_day = divmod(date, 10000)
    if month_day <= cutoff:
        year -= 1

    st = year * 10000 + cutoff
    en = st + 10000

    return (st, en)


def _get_yesterday(date: Date) -> datetime.datetime:
    dt = datetime.datetime.strptime(str(date), "%Y%m%d").date()
    dt = dt - datetime.timedelta(1)
    return dt


def current_year_through_week(date: Date, cutoff: int = 630) -> Tuple[Date, Date]:
    # Find largest cutoff that's less than date.
    year, month_day = divmod(date, 10000)
    if month_day <= cutoff:
        year -= 1

    st = year * 10000 + cutoff

    dt = _get_yesterday(date)
    en = dt - datetime.timedelta(dt.weekday())
    en = int(en.strftime("%Y%m%d"))

    return (st, en)


def current_year_through_yesterday(date: Date, cutoff: int = 630) -> Tuple[Date, Date]:
    # Find largest cutoff that's less than date.
    year, month_day = divmod(date, 10000)
    if month_day <= cutoff:
        year -= 1

    st = year * 10000 + cutoff

    dt = _get_yesterday(date)
    en = int(dt.strftime("%Y%m%d"))

    return (st, en)

