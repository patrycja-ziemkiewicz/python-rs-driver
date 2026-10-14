from collections.abc import Callable, Iterable
from typing import Any

from scylla.results import PagingState, RowFactoryLike

class QueryExhausted(Exception):
    """
    Raised by `ResponseFuture.start_fetching_next_page()` when there are no more pages.
    """

class ResponseFuture:
    """
    A request in flight, delivering its result synchronously via :meth:`result`
    or asynchronously via callbacks. Returned by ``LegacySession.execute_async()``.

    Callbacks receive the rows of one page (a list, or ``None`` for a result
    without rows) and persist across pages: after
    :meth:`start_fetching_next_page` they fire again for the next page.
    """

    def result(self) -> list[Any] | None:
        """Block until the request settles; return the rows of its page or raise."""

    def add_callback(self, callback: Callable[..., Any], *args: Any, **kwargs: Any) -> ResponseFuture:
        """
        Register ``callback(rows, *args, **kwargs)`` to run when a page arrives.
        Runs immediately if one already has. Returns ``self``.
        """

    def add_errback(self, errback: Callable[..., Any], *args: Any, **kwargs: Any) -> ResponseFuture:
        """
        Register ``errback(exception, *args, **kwargs)`` to run when a request fails.
        Runs immediately if one already has. Returns ``self``.
        """

    def add_callbacks(
        self,
        callback: Callable[..., Any],
        errback: Callable[..., Any],
        callback_args: Iterable[Any] | None = None,
        callback_kwargs: dict[str, Any] | None = None,
        errback_args: Iterable[Any] | None = None,
        errback_kwargs: dict[str, Any] | None = None,
    ) -> None:
        """:meth:`add_callback` and :meth:`add_errback` in one call."""

    def clear_callbacks(self) -> None: ...
    def start_fetching_next_page(self) -> None:
        """
        Start fetching the next page; callbacks fire again when it arrives.
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
