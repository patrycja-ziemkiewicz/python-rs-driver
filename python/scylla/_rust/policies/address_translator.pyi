import uuid
from ipaddress import IPv4Address, IPv6Address
from typing import Any, Protocol, runtime_checkable

from ..session_builder import ContactPoint

class UntranslatedPeer:
    """
    Information about a ScyllaDB node discovered by the driver.
    """

    @property
    def host_id(self) -> uuid.UUID: ...
    @property
    def untranslated_address(self) -> tuple[IPv4Address | IPv6Address, int]: ...
    @property
    def datacenter(self) -> str | None: ...
    @property
    def rack(self) -> str | None: ...

@runtime_checkable
class AddressTranslator(Protocol):
    """
    Protocol for custom address translation.
    """
    def translate(self, info: UntranslatedPeer) -> str | tuple[str | IPv4Address | IPv6Address, int]:
        """
        Translates a node's address.
        Must return a tuple of (ip_address | str, port_integer) or string with valid address and port.

        When returning a string, it should therefore be a numeric IP address
        plus port (for example ``"127.0.0.1:9042"`` or ``"[::1]:9042"``).
        """

class DictAddressTranslator:
    """
    Address translator backed by a dictionary.

    The keys are untranslated node addresses and the values are addresses that
    should be used instead.

    The expected dictionary type is:
    `dict[AddressType, AddressType]`
    where `AddressType` is `str | Tuple[str | IPv4Address | IPv6Address, int]`.

    Addresses in the dictionary can be provided as:

    * A string: ``"127.0.0.1:9042"``
    * A tuple: ``("127.0.0.1", 9042)``, ``(IPv4Address("127.0.0.1"), 9042)``, ``(IPv6Address("::1"), 9042)``.

    Notes
    -----
    When lookups or keys are provided as strings, they must be valid IP addresses
    and **must explicitly include the port** (e.g., ``"127.0.0.1:9042"`` or
    ``"[::1]:9042"``). A plain IP string without a port will not match properly.
    """

    def __init__(self, translation_map: dict[Any, Any]) -> None: ...
    def translate(self, info: UntranslatedPeer) -> ContactPoint: ...
