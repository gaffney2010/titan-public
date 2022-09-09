Date = int
TeamName = str
Year = int


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
