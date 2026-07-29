from ipaddress import IPv4Address, IPv6Address
from typing import Protocol, runtime_checkable

from .._rust.policies.address_translator import (  # pyright: ignore[reportMissingModuleSource]
    DictAddressTranslator,
    UntranslatedPeer,
)


@runtime_checkable
class AddressTranslator(Protocol):
    def translate(self, info: UntranslatedPeer) -> str | tuple[str | IPv4Address | IPv6Address, int]: ...


__all__ = [
    "AddressTranslator",
    "DictAddressTranslator",
    "UntranslatedPeer",
]
