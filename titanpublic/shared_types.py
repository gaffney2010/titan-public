from typing import List, Tuple

Date = int
TeamName = str
Year = int


class MultiRange(object):
    """Some day we'll replace all our single ranges with multi-ranges"""
    def __init__(self, ranges: List[Tuple[Date, Date]]):
        # TODO: Assert here that the ranges don't overlap
        self.ranges = ranges


class TitanException(Exception):
    pass


class TitanTransientException(TitanException):
    """These are failures that should be retried."""

    pass


class TitanRecurrentException(TitanException):
    """These are failures that will persist for the model/record.
    
    This may be due to, for example, a current-season model run on the first game of
    the season.

    These failures should be recorded to the database, so that titan won't repeatedly
    rerun.
    """

    pass


class TitanCriticalException(TitanException):
    """These are unexpected errors that should crash titan."""

    pass
