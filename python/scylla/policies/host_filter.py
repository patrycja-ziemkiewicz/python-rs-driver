from typing import Protocol, runtime_checkable

from .._rust.policies.host_filter import (  # pyright: ignore[reportMissingModuleSource]
    AcceptAllHostFilter,
    AllowListHostFilter,
    DcHostFilter,
    Peer,
)


@runtime_checkable
class HostFilter(Protocol):
    def accept(self, peer: Peer) -> bool: ...


__all__ = [
    "AcceptAllHostFilter",
    "AllowListHostFilter",
    "DcHostFilter",
    "HostFilter",
    "Peer",
]
