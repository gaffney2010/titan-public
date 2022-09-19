import datetime
from typing import Optional, Tuple
import warnings

from . import shared_types


def season_year_label(
    date: shared_types.Date,
    cutoff: Optional[int] = None,
    sport: str = "ncaam",
) -> shared_types.Date:
    if cutoff:
        # Need to deprecate because logic may change completely for other sports.
        warnings.warn("cutoff argument is deprecated")
    else:
        if sport == "ncaam":
            cutoff = 630
        else:
            raise NotImplementedError(f"Sport {sport} is not supported for year model.")

    year, month_day = divmod(date, 10000)
    if month_day > cutoff:
        year += 1

    return year


def previous_years(
    date: shared_types.Date,
    years_back: int,
    cutoff: Optional[int] = None,
    sport: str = "ncaam",
) -> Tuple[shared_types.Date, shared_types.Date]:
    if cutoff:
        # Need to deprecate because logic may change completely for other sports.
        warnings.warn("cutoff argument is deprecated")
    else:
        if sport == "ncaam":
            cutoff = 630
        else:
            raise NotImplementedError(f"Sport {sport} is not supported for year model.")

    # Find largest cutoff that's less than date.
    year, month_day = divmod(date, 10000)
    if month_day <= cutoff:
        year -= 1

    en = year * 10000 + cutoff
    st = en - 10000 * years_back

    return (st, en)


def previous_year(
    date: shared_types.Date, cutoff: Optional[int] = None, sport: str = "ncaam"
) -> Tuple[shared_types.Date, shared_types.Date]:
    return previous_years(date, 1, cutoff=cutoff, sport=sport)


def current_year(
    date: shared_types.Date, cutoff: Optional[int] = None, sport: str = "ncaam"
) -> Tuple[shared_types.Date, shared_types.Date]:
    if cutoff:
        # Need to deprecate because logic may change completely for other sports.
        warnings.warn("cutoff argument is deprecated")
    else:
        if sport == "ncaam":
            cutoff = 630
        else:
            raise NotImplementedError(f"Sport {sport} is not supported for year model.")

    # Find largest cutoff that's less than date.
    year, month_day = divmod(date, 10000)
    if month_day <= cutoff:
        year -= 1

    st = year * 10000 + cutoff
    en = st + 10000

    return (st, en)


def _get_yesterday(date: shared_types.Date) -> datetime.datetime:
    dt = datetime.datetime.strptime(str(date), "%Y%m%d").date()
    dt = dt - datetime.timedelta(1)
    return dt


def current_year_through_week(
    date: shared_types.Date, cutoff: Optional[int] = None, sport: str = "ncaam"
) -> Tuple[shared_types.Date, shared_types.Date]:
    if cutoff:
        # Need to deprecate because logic may change completely for other sports.
        warnings.warn("cutoff argument is deprecated")
    else:
        if sport == "ncaam":
            cutoff = 630
        else:
            raise NotImplementedError(f"Sport {sport} is not supported for year model.")

    # Find largest cutoff that's less than date.
    year, month_day = divmod(date, 10000)
    if month_day <= cutoff:
        year -= 1

    st = year * 10000 + cutoff

    dt = _get_yesterday(date)
    en = dt - datetime.timedelta(dt.weekday())
    en = int(en.strftime("%Y%m%d"))

    return (st, en)


def current_year_through_yesterday(
    date: shared_types.Date, cutoff: Optional[int] = None, sport: str = "ncaam"
) -> Tuple[shared_types.Date, shared_types.Date]:
    if cutoff:
        # Need to deprecate because logic may change completely for other sports.
        warnings.warn("cutoff argument is deprecated")
    else:
        if sport == "ncaam":
            cutoff = 630
        else:
            raise NotImplementedError(f"Sport {sport} is not supported for year model.")

    # Find largest cutoff that's less than date.
    year, month_day = divmod(date, 10000)
    if month_day <= cutoff:
        year -= 1

    st = year * 10000 + cutoff

    dt = _get_yesterday(date)
    en = int(dt.strftime("%Y%m%d"))

    return (st, en)
