import uuid
from collections.abc import Sequence
from ipaddress import IPv4Address, IPv6Address
from typing import Protocol, runtime_checkable

from ..routing import Token

class Peer:
    """
    Information about a ScyllaDB node discovered by the driver.
    """

    @property
    def host_id(self) -> uuid.UUID: ...
    @property
    def address(self) -> tuple[IPv4Address | IPv6Address, int]: ...
    @property
    def tokens(self) -> tuple[Token]: ...
    @property
    def datacenter(self) -> str | None: ...
    @property
    def rack(self) -> str | None: ...

@runtime_checkable
class HostFilter(Protocol):
    """
    Protocol for implementing custom host filtering.
    """
    def accept(self, peer: Peer) -> bool:
        """
        Decide whether the given peer should be accepted.

        Parameters
        ----------
        peer : Peer
            Information about the node being evaluated.

        Returns
        -------
        bool
            ``True`` if the node should be accepted, ``False`` otherwise.

        If this method is not overridden, raises an exception, or returns
        an invalid value, the driver logs the error and falls back to
        accepting the host.
        """

class AcceptAllHostFilter:
    """
    A host filter that accepts every node in the cluster.
    """
    def __init__(self) -> None: ...
    def accept(self, peer: Peer) -> bool: ...

class DcHostFilter:
    """
    A host filter that accepts nodes only from the specified datacenter.
    """
    def __init__(self, local_dc: str) -> None: ...
    def accept(self, peer: Peer) -> bool: ...

class AllowListHostFilter:
    """
    A host filter that accepts only nodes whose addresses are present in the provided allow list.
    """
    def __init__(self, list: Sequence[str | tuple[str | IPv4Address | IPv6Address, int]]) -> None: ...
    def accept(self, peer: Peer) -> bool: ...
