"""Scenarios that can only be observed in an interpreter of their own.

The driver registers its runtime shutdown on `atexit`, so everything it reports
happens after pytest has finished reporting and torn down its capture. A test
that wants to see that report has to run the scenario in a child interpreter and
read its exit status and stderr, which is what `test_future.py` does with this
module.

Each scenario leaves the process in a state the shutdown hook has to cope with,
verifies it actually reached that state, prints `SCENARIO_READY`, and returns.
Falling off the end of `main()` is the point: the hook then runs for real, with
nothing left to assert from in-process.
"""

import os
import socket
import sys
import threading

from scylla.errors import FutureCancelledError
from scylla.future import DriverFuture
from scylla.results import RequestResult
from scylla.session import Session
from scylla.session_builder import SessionBuilder

CONTACT_POINT = ("127.0.0.2", 9042)

# Printed on stdout once the scenario is set up. Without it a child that failed
# to reach the interesting state would exit cleanly and prove nothing.
SCENARIO_READY = "SCENARIO READY"

CALLBACK_TIMEOUT = 10.0

# Sockets kept open for the lifetime of the process: closing one would reset the
# connection the driver is still handshaking on, resolving the future a scenario
# needs to keep pending.
_PARKED_LISTENERS: list[socket.socket] = []

# Far longer than the child lives, so the handshake never gives up on its own.
NEVER_CONNECT_TIMEOUT = 3600.0


def _never_completing_future() -> DriverFuture[Session]:
    """A future that cannot resolve: a connect to a socket that never speaks CQL.

    The TCP connect succeeds (the kernel completes it into the listen backlog)
    and the CQL handshake then waits for a reply that never comes.
    """
    listener = socket.socket()
    listener.bind(("127.0.0.1", 0))
    listener.listen(1)
    _PARKED_LISTENERS.append(listener)

    return SessionBuilder().contact_points([listener.getsockname()]).connection_timeout(NEVER_CONNECT_TIMEOUT).connect()


def blocked_callback() -> None:
    """Exit with a callback blocked in result() on a future that can never resolve.

    Callbacks run on the driver's blocking pool, which the runtime shutdown waits
    for, and this one waits on a future whose task that same shutdown drops.
    Unless dropping the task resolves the future, the callback never returns: the
    shutdown burns its whole timeout, says so on stderr, and finalization proceeds
    with the thread still blocked.
    """
    session = SessionBuilder().contact_points([CONTACT_POINT]).connect().result()
    never = _never_completing_future()

    entered = threading.Event()
    ready = threading.Event()
    ran_on: list[int] = []

    def block_on_pending_future(_result: RequestResult) -> None:
        ran_on.append(threading.get_ident())
        entered.set()
        try:
            # Only the shutdown dropping this future's task releases this thread.
            never.result()
        except FutureCancelledError:
            pass
        finally:
            if ready.is_set():
                print("block_on_pending_future returned during shutdown", flush=True)
            else:
                print("block_on_pending_future returned before the scenario was ready", file=sys.stderr, flush=True)
                os._exit(1)

    session.execute("SELECT release_version FROM system.local").on_success(block_on_pending_future)

    assert entered.wait(CALLBACK_TIMEOUT), "the on_success callback never ran"
    # A callback registered on an already-resolved future fires inline instead of
    # on the blocking pool, which would block the main thread and leave the pool -
    # the thing the shutdown waits for - free to drain.
    assert not never.done(), "the future meant to stay pending is already done"
    ready.set()

    print(SCENARIO_READY, flush=True)


SCENARIOS = {"blocked-callback": blocked_callback}


def main(argv: list[str]) -> int:
    if len(argv) != 2 or argv[1] not in SCENARIOS:
        print(f"usage: {argv[0]} {{{'|'.join(SCENARIOS)}}}", file=sys.stderr)
        return 2

    SCENARIOS[argv[1]]()
    # Return, don't exit: the atexit hook shuts the runtime down from here.
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv))
