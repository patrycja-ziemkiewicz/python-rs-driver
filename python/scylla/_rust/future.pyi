from typing import Any, Callable, Generator, Generic, TypeVar

T = TypeVar("T")

class ResponseFuture(Generic[T]):
    """
    An awaitable handle representing a pending asynchronous database operation.

    Can be used directly with ``await`` in async code::

        result = await session.execute(statement)

    Or resolved synchronously, blocking the calling thread until the operation
    completes::

        result = session.execute(statement).result()

    Callbacks can be registered to react to completion without awaiting::

        future.on_success(lambda result: print(result))
        future.on_error(lambda exc: print(exc))
    """

    def __await__(self) -> Generator[Any, None, T]:
        """Return an iterator that drives this future to completion, yielding ``T``."""
        ...

    def __iter__(self) -> Generator[Any, None, T]: ...
    def __next__(self) -> Any: ...
    def send(self, value: Any) -> Any: ...
    def throw(self, exc: BaseException) -> Any: ...
    def close(self) -> None: ...
    def result(self) -> T:
        """
        Return the result of the operation, blocking until it completes if still pending.

        Returns
        -------
        T
            The resolved value.
        """
        ...

    def on_success(
        self,
        callback: Callable[..., Any],
        /,
        *args: Any,
        **kwargs: Any,
    ) -> None:
        """
        Register a callback to be invoked when the operation completes successfully.

        The callback is called as ``callback(result, *args, **kwargs)``.
        If the future is already resolved successfully, the callback is invoked immediately.

        Parameters
        ----------
        callback : Callable
            The callable to invoke with the result value as the first argument.
        *args : Any
            Extra positional arguments forwarded to the callback.
        **kwargs : Any
            Extra keyword arguments forwarded to the callback.
        """
        ...

    def on_error(
        self,
        callback: Callable[..., Any],
        /,
        *args: Any,
        **kwargs: Any,
    ) -> None:
        """
        Register a callback to be invoked when the operation completes with an error.

        The callback is called as ``callback(exception, *args, **kwargs)``.
        If the future is already resolved with an error, the callback is invoked immediately.

        Parameters
        ----------
        callback : Callable
            The callable to invoke with the exception as the first argument.
        *args : Any
            Extra positional arguments forwarded to the callback.
        **kwargs : Any
            Extra keyword arguments forwarded to the callback.
        """
        ...
