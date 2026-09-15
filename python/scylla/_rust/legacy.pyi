from collections.abc import Iterator
from typing import Any

from .cluster.metadata import CqlColumnType
from .results import PagingState

class ResultSet:
    """
    Rows of a request, iterated page by page with further pages fetched
    transparently. Handed out by :meth:`ResponseFuture.result`; not constructible.

    ``==`` and indexing materialize every remaining row first.
    """

    def __iter__(self) -> Iterator[Any]: ...
    def __next__(self) -> Any: ...
    def __getitem__(self, index: Any) -> Any: ...
    def __eq__(self, other: object) -> bool: ...
    def __bool__(self) -> bool: ...
    def one(self) -> Any | None:
        """The first row of the current page, ``None`` if it is empty."""

    def all(self) -> list[Any]:
        """Every remaining row as a list; ``list(result_set)``."""

    def fetch_next_page(self) -> None:
        """
        Fetch the next page synchronously into ``current_rows``; not needed when
        iterating. If iteration has started, it continues from the new page's
        first row.
        """

    @property
    def has_more_pages(self) -> bool: ...
    @property
    def current_rows(self) -> list[Any]:
        """Rows of the current page. Empty does not mean exhausted; see ``has_more_pages``."""

    @property
    def paging_state(self) -> PagingState | None: ...
    @property
    def column_names(self) -> list[str] | None: ...
    @property
    def column_types(self) -> list[CqlColumnType] | None: ...
    @property
    def was_applied(self) -> bool:
        """For an LWT result, whether the transaction was applied."""

    def get_query_trace(self, max_wait_sec: float | None = None) -> Any:
        """Not supported; raises ``NotImplementedError``."""

    def get_all_query_traces(self, max_wait_sec_per: float | None = None) -> Any:
        """Not supported; raises ``NotImplementedError``."""
