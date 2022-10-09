from typing import List, Tuple

Date = int
TeamName = str
Year = int


class MultiRange(object):
    """Some day we'll replace all our single ranges with multi-ranges"""
    def __init__(self, ranges: List[Tuple[Date, Date]]):
        ranges = sorted(ranges)

        merged_ranges = list()
        working_st, working_en = None, None
        for i, ri in enumerate(ranges):
            # These don't overlap but may touch.
            if i+1 < len(ranges):
                assert ri[1] <= ranges[i+1][0]

            if working_st is None:
                working_st, working_en = ri
                continue

            if working_en < ri[0]:
                # There's been a gap add a range and start fresh
                merged_ranges.append((working_st, working_en))
                working_st, working_en = ri
            else:
                # Merge these
                working_en = ri[1]
        
        # May need to go one more
        if working_st is not None:
            assert(working_en is not None)
            merged_ranges.append((working_st, working_en))

        self.ranges = merged_ranges


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
