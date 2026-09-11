from __future__ import annotations

from typing import NamedTuple, TypeAlias
from uuid import UUID

from ._rust.routing import (  # pyright: ignore[reportMissingModuleSource]
    ReplicaLocator,
    Token,
)
from .cluster import Node

Shard: TypeAlias = int


class Target(NamedTuple):
    """
    A request target: the node to send a request to, and optionally a single
    shard on that node.

    Parameters
    ----------
    node: Node | UUID
        The node to target, either as a ``Node`` or as its host id.

        ``None`` resolves to a **uniformly random** shard, not to the shard that
        owns the partition, so pinning a node without a shard gives up shard
        locality entirely.

        A shard is a **best-effort** hint: if the driver holds no live connection
        to it, the request is sent over a connection to another shard of the same
        node instead. Shards are also ignored for non-sharded nodes.
    """

    node: Node | UUID
    shard: Shard | None = None


__all__ = [
    "ReplicaLocator",
    "Shard",
    "Target",
    "Token",
]
