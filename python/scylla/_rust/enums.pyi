from enum import IntEnum, Enum

class Consistency(IntEnum):
    Any = ...
    One = ...
    Two = ...
    Three = ...
    Quorum = ...
    All = ...
    LocalQuorum = ...
    EachQuorum = ...
    LocalOne = ...
    Serial = ...
    LocalSerial = ...

class SerialConsistency(IntEnum):
    Serial = ...
    LocalSerial = ...

class Compression(Enum):
    Lz4 = ...
    Snappy = ...

class PoolSize:
    @staticmethod
    def per_host(connections: int) -> PoolSize:
        """
        Creates a pool size with a fixed number of connections per node.

        Parameters
        ----------
        connections : int
            Number of connections per node. Must be greater than 0.

        Returns
        -------
        PoolSize
        """
        ...

    @staticmethod
    def per_shard(connections: int) -> PoolSize:
        """
        Creates a pool size with a fixed number of connections per shard.

        For Cassandra, nodes are treated as having a single shard.

        The recommended setting for Scylla is ``per_shard(1)``.

        Parameters
        ----------
        connections : int
            Number of connections per shard. Must be greater than 0.

        Returns
        -------
        PoolSize
        """
        ...

class WriteCoalescingDelay:
    @staticmethod
    def small_nondeterministic() -> WriteCoalescingDelay:
        """
        Creates a small nondeterministic delay configuration.

        This is the default setting and is intended for sub-millisecond delays.

        Returns
        -------
        WriteCoalescingDelay
        """
        ...

    @staticmethod
    def milliseconds(delay: int) -> WriteCoalescingDelay:
        """
        Creates a delay in millisecond.

        Parameters
        ----------
        delay : int
            Delay in milliseconds. Must be greater than 0.

        Returns
        -------
        WriteCoalescingDelay
        """
        ...
