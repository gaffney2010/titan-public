Date = int
TeamName = str
Year = int


class TitanException(Exception):
    pass


class TitanTransientException(TitanException):
    """These are failures that should be retried."""

    pass


class TitanRecurrentException(TitanException):
    """These are failures that will persist for the model/record."""

    pass
