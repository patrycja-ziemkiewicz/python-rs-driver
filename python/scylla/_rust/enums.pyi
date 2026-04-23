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

class SelfIdentity:
    def __init__(
        self,
        *,
        custom_driver_name: str | None = None,
        custom_driver_version: str | None = None,
        application_name: str | None = None,
        application_version: str | None = None,
        client_id: str | None = None,
    ) -> None:
        """
        Self-identifying information sent by the driver in the STARTUP message.

        By default, the driver sends its built-in driver name and version.
        Application name, application version, and client ID are not sent unless
        explicitly set.

        Parameters
        ----------
        custom_driver_name : str | None, default None
            Custom driver name to advertise.

        custom_driver_version : str | None, default None
            Custom driver version to advertise.

        application_name : str | None, default None
            Application name to advertise. This can be used to distinguish
            different applications connected to the same cluster.

        application_version : str | None, default None
            Application version to advertise.

        client_id : str | None, default None
            Client identifier to advertise. This can be used to distinguish
            different instances of the same application connected to the same
            cluster.
        """
        ...

    @property
    def custom_driver_name(self) -> str | None:
        """
        Custom driver name advertised by the driver.
        """
        ...

    @custom_driver_name.setter
    def custom_driver_name(self, value: str) -> None:
        """
        Sets the custom driver name to advertise.

        Parameters
        ----------
        value : str
            Custom driver name.
        """
        ...

    @property
    def custom_driver_version(self) -> str | None:
        """
        Custom driver version advertised by the driver.
        """
        ...

    @custom_driver_version.setter
    def custom_driver_version(self, value: str) -> None:
        """
        Sets the custom driver version to advertise.

        Parameters
        ----------
        value : str
            Custom driver version.
        """
        ...

    @property
    def application_name(self) -> str | None:
        """
        Application name advertised by the driver.

        This can be used to distinguish different applications connected to the
        same cluster.
        """
        ...

    @application_name.setter
    def application_name(self, value: str) -> None:
        """
        Sets the application name to advertise.

        Parameters
        ----------
        value : str
            Application name.
        """
        ...

    @property
    def application_version(self) -> str | None:
        """
        Application version advertised by the driver.
        """
        ...

    @application_version.setter
    def application_version(self, value: str) -> None:
        """
        Sets the application version to advertise.

        Parameters
        ----------
        value : str
            Application version.
        """
        ...

    @property
    def client_id(self) -> str | None:
        """
        Client identifier advertised by the driver.

        This can be used to distinguish different instances of the same
        application connected to the same cluster.
        """
        ...

    @client_id.setter
    def client_id(self, value: str) -> None:
        """
        Sets the client identifier to advertise.

        Parameters
        ----------
        value : str
            Client identifier.
        """
        ...
