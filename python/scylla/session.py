import ipaddress
from typing import TypeAlias

from ._rust.enums import (  # pyright: ignore[reportMissingModuleSource]
    Compression,
    PoolSize,
    SelfIdentity,
    WriteCoalescingDelay,
)
from ._rust.execution_profile import ExecutionProfile  # pyright: ignore[reportMissingModuleSource]
from ._rust.session import Session  # pyright: ignore[reportMissingModuleSource]
from ._rust.session_builder import SessionBuilder, SessionBuilderConfig  # pyright: ignore[reportMissingModuleSource]

ContactPoint: TypeAlias = str | tuple[str | ipaddress.IPv4Address | ipaddress.IPv6Address, int]
"""Address of a node to connect to, either a hostname or a host and port pair."""

__all__ = [
    "Compression",
    "ContactPoint",
    "ExecutionProfile",
    "PoolSize",
    "SelfIdentity",
    "Session",
    "SessionBuilder",
    "SessionBuilderConfig",
    "WriteCoalescingDelay",
]
