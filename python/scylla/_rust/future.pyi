from collections.abc import Callable, Generator
from datetime import timedelta
from typing import Any, Generic, TypeVar

_T = TypeVar("_T")

class DriverFuture(Generic[_T]):
    """
    An awaitable handle representing a pending asynchronous database operation.

    This future is **lazy** — the underlying operation is not driven to
    completion until it is awaited. The simplest and recommended way to
    consume it::

        result = await session.execute("SELECT * FROM users")

    Preferred for most code — it avoids the overhead of spawning the
    operation onto a background task.
    """

    def __await__(self) -> Generator[Any, None, _T]:
        """Return an iterator that drives this future to completion, yielding ``_T``."""

    def __iter__(self) -> Generator[Any, None, _T]: ...
    def __next__(self) -> Any: ...
    def send(self, value: Any) -> Any: ...
    def throw(self, exc: BaseException) -> Any: ...
    def close(self) -> None: ...
