from typing import Protocol, runtime_checkable

from .._rust.policies.timestamp_generator import (  # pyright: ignore[reportMissingModuleSource]
    MonotonicTimestampGenerator,
    SimpleTimestampGenerator,
)


@runtime_checkable
class TimestampGenerator(Protocol):
    def next_timestamp(self) -> int: ...


__all__ = [
    "MonotonicTimestampGenerator",
    "SimpleTimestampGenerator",
    "TimestampGenerator",
]
