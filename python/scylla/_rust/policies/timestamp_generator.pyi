from datetime import timedelta
from typing import Protocol, runtime_checkable

@runtime_checkable
class TimestampGenerator(Protocol):
    """
    Protocol for custom client-side timestamp generation.
    """
    def next_timestamp(self) -> int:
        """
        Generate the next timestamp for a request.

        This method should return an integer representing the timestamp.

        If this method is not implemented or raises an exception, the
        driver will log the error and fallback to the current system timestamp.

        """

class SimpleTimestampGenerator:
    """
    A simple client-side timestamp generator based on the system clock.

    This generator returns the current system time in microseconds since
    the Unix Epoch (1970-01-01)
    """
    def __init__(self) -> None: ...
    def next_timestamp(self) -> int: ...

class MonotonicTimestampGenerator:
    """
    Timestamp generator that guarantees monotonically increasing timestamps.

    Parameters
    ----------
    warn_on_drift : bool, default True
        Whether to log warnings when generated timestamps drift too far from
        the system clock.

    warning_threshold : float | timedelta, default 1
        Drift threshold in seconds after which warnings may be emitted.

    warning_interval : float | timedelta, default 1
        Minimum interval in seconds between drift warnings.
    """

    def __init__(
        self,
        warn_on_drift: bool = True,
        warning_threshold: float | timedelta = 1,
        warning_interval: float | timedelta = 1,
    ) -> None: ...
    def next_timestamp(self) -> int: ...
