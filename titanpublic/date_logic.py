import datetime
from typing import List, Optional, Tuple
import warnings

from . import shared_types


def _current_year_start(date: shared_types.Date, sport: str) -> shared_types.Date:
    """Return a date between season containing `date` and previous season."""
    if sport in ("ncaam", "ncaaw"):
        cutoff = 630
    elif "ncaaf" == sport:
        # Some pandemic games were played in the spring.  :/
        cutoff = 630
    else:
        raise NotImplementedError(f"Sport {sport} is not supported.")

    # Find largest cutoff that's less than date.
    year, month_day = divmod(date, 10000)
    if month_day <= cutoff:
        year -= 1

    return year * 10000 + cutoff


def season_year_label(
    date: shared_types.Date,
    sport: str = "ncaam",
) -> shared_types.Date:
    if sport in ("ncaam", "ncaaw"):
        cutoff = 630
        year, month_day = divmod(date, 10000)
        if month_day > cutoff:
            year += 1

        return year
    
    if "ncaaf" == sport:
        # NCAAF is marked by the beginning of the season instead of the end
        cutoff = 630
        year, month_day = divmod(date, 10000)
        if month_day < cutoff:
            year -= 1

        return year
    
    raise NotImplementedError(f"Sport {sport} is not supported for year model.")


def previous_years(
    date: shared_types.Date,
    years_back: int,
    sport: str = "ncaam",
) -> Tuple[shared_types.Date, shared_types.Date]:
    en = _current_year_start(date, sport)
    st = en - 10000 * years_back

    return (st, en)


def previous_year(
    date: shared_types.Date, sport: str = "ncaam"
) -> Tuple[shared_types.Date, shared_types.Date]:
    return previous_years(date, 1, sport=sport)


def previous_years_with_gaps(
    date: shared_types.Date,
    years_back: int,
    sport: str,
    gaps: List[shared_types.Date],
) -> shared_types.MultiRange:
    """Gaps is a list of dates that should be excluded."""
    years_counted, years_considered = 0, 0
    en = _current_year_start(date, sport)
    results = list()
    
    # Do limit for safety
    while years_considered < 10 and years_counted < years_back:
        years_considered += 1
        st = en - 10000
        for gap in gaps:
            if st <= gap < en:
                break
        else:
            years_counted += 1
            results.append((st, en))
        en = st

    return shared_types.MultiRange(results)


def current_year(
    date: shared_types.Date, sport: str = "ncaam"
) -> Tuple[shared_types.Date, shared_types.Date]:
    st = _current_year_start(date, sport)
    en = st + 10000

    return (st, en)


def current_year_from_season(season: int, sport: str) -> Tuple[shared_types.Date, shared_types.Date]:
    # Just pick some midseason date
    if sport in ("ncaam", "ncaaw"):
        return current_year(season*10000+101, sport=sport)

    if "ncaaf" == sport:
        return current_year(season*10000+901, sport=sport)
    
    raise NotImplementedError(f"Sport {sport} is not supported for year model.")


def _get_yesterday(date: shared_types.Date) -> datetime.datetime:
    dt = datetime.datetime.strptime(str(date), "%Y%m%d").date()
    dt = dt - datetime.timedelta(1)
    return dt


def current_year_through_week(
    date: shared_types.Date, sport: str = "ncaam"
) -> Tuple[shared_types.Date, shared_types.Date]:
    st = _current_year_start(date, sport)

    dt = _get_yesterday(date)
    en = dt - datetime.timedelta(dt.weekday())
    en = int(en.strftime("%Y%m%d"))

    return (st, en)


def current_year_through_yesterday(
    date: shared_types.Date, sport: str = "ncaam"
) -> Tuple[shared_types.Date, shared_types.Date]:
    st = _current_year_start(date, sport)

    dt = _get_yesterday(date)
    en = int(dt.strftime("%Y%m%d"))

    return (st, en)


def approx_season_start(date: shared_types.Date, sport: str) -> shared_types.Date:
    year = _current_year_start(date, sport) // 10000
    if sport in ("ncaam", "ncaaw"):
        return year * 10000 + 825
    if "ncaaf" == sport:
        return year * 10000 + 1106
    raise NotImplementedError(f"Sport {sport} is not supported for year model.")
