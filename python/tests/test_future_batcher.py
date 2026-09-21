from __future__ import annotations

import asyncio
import gc
import threading
from collections.abc import Awaitable, Callable, Iterator
from typing import Any, TypeVar

import pytest
from scylla.results import RequestResult
from scylla.session import Session
from scylla.session_builder import SessionBuilder

T = TypeVar("T")

QUERY = "SELECT release_version FROM system.local"

BURST = 128

LOOPS = 4

THREAD_TIMEOUT = 30.0


async def _connect() -> Session:
    return await SessionBuilder().contact_points([("127.0.0.2", 9042)]).connect()


@pytest.fixture(scope="module")
def session() -> Iterator[Session]:
    """A session outliving the loop it was created on."""
    yield asyncio.run(_connect())


async def _burst(session: Session, count: int = BURST) -> list[RequestResult]:
    """Fire `count` queries at once and wait for all of them."""
    return list(await asyncio.gather(*(session.execute(QUERY) for _ in range(count))))


def _assert_burst(results: list[RequestResult], count: int = BURST) -> None:
    assert len(results) == count
    assert all(r is not None for r in results)


def _run_on_new_loop(
    body: Callable[[], Awaitable[T]],
    loop_factory: Callable[[], asyncio.AbstractEventLoop] = asyncio.new_event_loop,
) -> T:
    """Run `body` on a fresh loop, then close it and let it be collected."""
    loop = loop_factory()
    try:
        return loop.run_until_complete(body())
    finally:
        loop.close()


# --------------------------------------------------------------------------- #
# One loop
# --------------------------------------------------------------------------- #


@pytest.mark.requires_db
def test_burst_completes_on_a_single_loop(session: Session) -> None:
    _assert_burst(_run_on_new_loop(lambda: _burst(session)))


@pytest.mark.requires_db
def test_alternating_single_and_burst_rearms_every_round(session: Session) -> None:
    async def body() -> int:
        rounds = 0
        for _ in range(20):
            assert await session.execute(QUERY) is not None
            _assert_burst(await _burst(session, 32), 32)
            rounds += 1
        return rounds

    assert _run_on_new_loop(body) == 20


# --------------------------------------------------------------------------- #
# Several loops
# --------------------------------------------------------------------------- #


@pytest.mark.requires_db
def test_sequential_loops_each_batch_independently(session: Session) -> None:
    """One loop after another, each with its own batcher registration."""
    for _ in range(5):
        _assert_burst(_run_on_new_loop(lambda: _burst(session, 32)), 32)
        gc.collect()


@pytest.mark.requires_db
def test_loop_address_reuse_does_not_resolve_to_a_dead_batcher(
    session: Session,
) -> None:
    """The reason entries are keyed by address *and* evicted by weakref.

    Loops are created and dropped in a tight cycle, so CPython hands the same
    address out again. If a dead loop's entry survived, a later loop would be
    handed its batcher and park forever on a loop that is gone.
    """
    addresses: list[int] = []

    for _ in range(30):
        loop = asyncio.new_event_loop()
        addresses.append(id(loop))
        try:
            _assert_burst(loop.run_until_complete(_burst(session, 16)), 16)
        finally:
            loop.close()
        del loop
        gc.collect()

    if len(set(addresses)) == len(addresses):
        pytest.skip("no loop address was reused, so eviction was not exercised")


@pytest.mark.requires_db
def test_parallel_loops_in_threads(session: Session) -> None:
    """Several loops alive at once, each batching its own completions."""
    results: dict[int, list[RequestResult]] = {}
    errors: list[BaseException] = []
    lock = threading.Lock()

    def worker(index: int) -> None:
        try:
            burst = _run_on_new_loop(lambda: _burst(session, 32))
            with lock:
                results[index] = burst
        except BaseException as exc:  # noqa: BLE001 - reported below
            with lock:
                errors.append(exc)

    threads = [threading.Thread(target=worker, args=(i,), daemon=True) for i in range(LOOPS)]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join(timeout=THREAD_TIMEOUT)
        assert not thread.is_alive(), "a loop thread never finished"

    assert not errors, errors
    assert len(results) == LOOPS
    for burst in results.values():
        _assert_burst(burst, 32)


@pytest.mark.requires_db
def test_parallel_loops_churning(session: Session) -> None:
    """Loops created, used and destroyed concurrently in several threads."""
    errors: list[BaseException] = []
    lock = threading.Lock()

    def worker() -> None:
        try:
            for _ in range(5):
                _assert_burst(_run_on_new_loop(lambda: _burst(session, 16)), 16)
                gc.collect()
        except BaseException as exc:  # noqa: BLE001 - reported below
            with lock:
                errors.append(exc)

    threads = [threading.Thread(target=worker, daemon=True) for _ in range(LOOPS)]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join(timeout=THREAD_TIMEOUT)
        assert not thread.is_alive(), "a churning loop thread never finished"

    assert not errors, errors


# --------------------------------------------------------------------------- #
# uvloop
# --------------------------------------------------------------------------- #


@pytest.mark.requires_db
def test_uvloop_burst(session: Session) -> None:
    """uvloop supports `add_reader`, so it takes the pipe path like asyncio."""
    uvloop = pytest.importorskip("uvloop")
    _assert_burst(_run_on_new_loop(lambda: _burst(session), uvloop.new_event_loop))


@pytest.mark.requires_db
def test_uvloop_and_asyncio_side_by_side(session: Session) -> None:
    """A uvloop loop and an asyncio loop batching at the same time.

    Two different loop implementations, two registrations, one map.
    """
    uvloop = pytest.importorskip("uvloop")

    results: dict[str, list[RequestResult]] = {}
    errors: list[BaseException] = []
    lock = threading.Lock()

    def worker(name: str, factory: Callable[[], asyncio.AbstractEventLoop]) -> None:
        try:
            burst = _run_on_new_loop(lambda: _burst(session, 64), factory)
            with lock:
                results[name] = burst
        except BaseException as exc:  # noqa: BLE001 - reported below
            with lock:
                errors.append(exc)

    threads = [
        threading.Thread(target=worker, args=("uvloop", uvloop.new_event_loop), daemon=True),
        threading.Thread(target=worker, args=("asyncio", asyncio.new_event_loop), daemon=True),
    ]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join(timeout=THREAD_TIMEOUT)
        assert not thread.is_alive(), "a loop thread never finished"

    assert not errors, errors
    assert set(results) == {"uvloop", "asyncio"}
    for burst in results.values():
        _assert_burst(burst, 64)


@pytest.mark.requires_db
def test_uvloop_then_asyncio_on_the_same_thread(session: Session) -> None:
    """uvloop's loop dies and an asyncio loop replaces it, possibly at the same
    address. Each must get its own batcher."""
    uvloop = pytest.importorskip("uvloop")

    for factory in (uvloop.new_event_loop, asyncio.new_event_loop) * 3:
        _assert_burst(_run_on_new_loop(lambda: _burst(session, 32), factory), 32)
        gc.collect()


# --------------------------------------------------------------------------- #
# Loops the fast path cannot serve
# --------------------------------------------------------------------------- #


class NoAddReaderLoop(asyncio.SelectorEventLoop):
    """A loop that refuses `add_reader`"""

    def add_reader(self, fd: Any, callback: Callable[..., object], *args: object) -> None:
        raise NotImplementedError("add_reader disabled for this test")


@pytest.mark.requires_db
def test_loop_without_add_reader_falls_back_to_call_soon(session: Session) -> None:
    """Without `add_reader` the batcher still batches, via `call_soon_threadsafe`."""
    _assert_burst(_run_on_new_loop(lambda: _burst(session), NoAddReaderLoop))


class UnweakrefableLoop:
    """A duck-typed loop that cannot be weakly referenced.

    `__slots__` without `__weakref__` leaves `tp_weaklistoffset` at 0, which is
    exactly what `weakref.ref` refuses. Only the handful of methods the parking
    path touches are implemented.
    """

    __slots__ = ()

    def get_debug(self) -> bool:
        return False

    def create_future(self) -> asyncio.Future[None]:
        return asyncio.Future(loop=self)  # type: ignore[arg-type]


@pytest.mark.requires_db
def test_loop_that_cannot_be_weakly_referenced_panics(session: Session) -> None:
    fake = UnweakrefableLoop()

    for _ in range(10):
        future = session.execute(QUERY)
        asyncio.events._set_running_loop(fake)  # type: ignore[arg-type]
        try:
            future.send(None)
        except StopIteration:
            # Completed before it could park; try again with a fresh future.
            continue
        except BaseException as exc:  # noqa: BLE001 - this is the assertion
            assert "cannot be weakly referenced" in str(exc)
            return
        else:
            pytest.fail("parking succeeded on a loop that has no weakref support")
        finally:
            asyncio.events._set_running_loop(None)

    pytest.skip("no query stayed pending long enough to park")
