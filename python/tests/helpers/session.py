"""Session construction shared by the test suite.

Every test that talks to a cluster goes through here, so the connection
details live in one place.
"""

from scylla.session import Session, SessionBuilder

CONTACT_POINTS = [("127.0.0.2", 9042)]


def session_builder() -> SessionBuilder:
    """A builder pointed at the test cluster.

    Tests needing further configuration chain onto the returned builder.
    """
    return SessionBuilder().contact_points(CONTACT_POINTS)


async def connect() -> Session:
    """Connects to the test cluster with the suite's default configuration."""
    return await session_builder().connect()
