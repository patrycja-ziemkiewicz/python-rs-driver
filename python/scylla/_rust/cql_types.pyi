from dateutil.relativedelta import relativedelta

class CqlEmpty:
    """Represents an empty CQL value."""

class CqlDuration:
    """
    Value of a CQL ``duration`` column.

    A duration has three independent parts. The driver does not convert one
    part into another, because months and days do not have a fixed length.

    Parameters
    ----------
    months : int
        Number of months. Must fit in a signed 32-bit integer.
    days : int
        Number of days. Must fit in a signed 32-bit integer.
    nanoseconds : int
        Number of nanoseconds. Must fit in a signed 64-bit integer.

    Raises
    ------
    ValueError
        If the parts do not all have the same sign.
    OverflowError
        If a part does not fit in its integer size.
    """

    def __init__(self, months: int = 0, days: int = 0, nanoseconds: int = 0) -> None: ...
    @staticmethod
    def from_relativedelta(delta: relativedelta) -> CqlDuration:
        """
        Make a ``CqlDuration`` from a ``dateutil.relativedelta.relativedelta``.

        Years become months. Hours, minutes, seconds and microseconds become
        nanoseconds.

        Raises
        ------
        TypeError
            If ``delta`` is not a ``relativedelta``.
        ValueError
            If the parts do not all have the same sign.
        OverflowError
            If a part does not fit in its integer size.
        """
    def to_relativedelta(self) -> relativedelta:
        """
        Convert the value to a ``dateutil.relativedelta.relativedelta``.

        The result has microsecond precision. The last three digits of
        ``nanoseconds`` are lost. ``relativedelta`` moves each full 24 hours
        into ``days``.

        Raises
        ------
        ImportError
            If ``python-dateutil`` is not installed.
        """
    @property
    def months(self) -> int:
        """Number of months."""
    @property
    def days(self) -> int:
        """Number of days."""
    @property
    def nanoseconds(self) -> int:
        """Number of nanoseconds."""
    def __eq__(self, other: object) -> bool: ...
    def __hash__(self) -> int: ...
