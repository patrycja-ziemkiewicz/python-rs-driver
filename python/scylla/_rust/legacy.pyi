from typing import Any

from scylla.results import RowFactoryLike

class QueryExhausted(Exception):
    """
    Raised by `ResponseFuture.start_fetching_next_page()` when there are no more pages.
    """

class ResponseFuture:
    """
    A request in flight, delivering its result synchronously via :meth:`result`.
    Returned by ``LegacySession.execute_async()``.
    """

    def result(self) -> list[Any] | None:
        """Block until the request settles; return the rows of its page or raise."""

    @property
    def query(self) -> Any:
        """The statement being executed."""

    @property
    def timeout(self) -> float | None:
        """Client-side timeout of the request in seconds, ``None`` for no timeout."""

    @property
    def row_factory(self) -> RowFactoryLike | None: ...
