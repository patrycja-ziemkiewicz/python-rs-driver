from typing import Any

from scylla.results import PagingState, RowFactoryLike

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

    def start_fetching_next_page(self) -> None:
        """
        Start fetching the next page.
        Raises ``QueryExhausted`` if there is none.
        """

    @property
    def has_more_pages(self) -> bool:
        """Whether the last page received says more follow."""

    @property
    def paging_state(self) -> PagingState | None: ...
    @property
    def query(self) -> Any:
        """The statement being executed."""

    @property
    def timeout(self) -> float | None:
        """Client-side timeout of the request in seconds, ``None`` for no timeout."""

    @property
    def row_factory(self) -> RowFactoryLike | None: ...
