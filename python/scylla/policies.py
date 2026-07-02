from ipaddress import IPv4Address, IPv6Address
from typing import Protocol, runtime_checkable

from ._rust.policies import (  # pyright: ignore[reportMissingModuleSource]
    Authenticator,
    AuthenticatorProvider,
    DictAddressTranslator,
    HostFilter,
    MonotonicTimestampGenerator,
    Peer,
    SimpleTimestampGenerator,
    UntranslatedPeer,
)


@runtime_checkable
class AddressTranslator(Protocol):
    def translate(self, info: UntranslatedPeer) -> str | tuple[str | IPv4Address | IPv6Address, int]: ...


@runtime_checkable
class TimestampGenerator(Protocol):
    def next_timestamp(self) -> int: ...


__all__ = [
    "AddressTranslator",
    "Authenticator",
    "AuthenticatorProvider",
    "DictAddressTranslator",
    "HostFilter",
    "MonotonicTimestampGenerator",
    "Peer",
    "SimpleTimestampGenerator",
    "TimestampGenerator",
    "UntranslatedPeer",
]
