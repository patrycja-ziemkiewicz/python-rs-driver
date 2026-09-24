from collections.abc import Mapping
from enum import IntEnum

class StrategyKind(IntEnum):
    Simple = ...
    NetworkTopology = ...
    Local = ...
    Other = ...

class Strategy:
    @property
    def kind(self) -> StrategyKind:
        """
        Access the kind of this strategy.
        """
    @property
    def replication_factor(self) -> Mapping[str, int] | int | None:
        """
        Access the replication factor for this strategy.

        For Simple and Local strategies, this is a positive integer.
        For Network Topology strategy, this is a read-only dictionary mapping datacenter names to replication factors.
        None means Driver cannot determine the replication factor based on Strategy.
        """
    @property
    def other_name(self) -> str | None:
        """
        Access the name of the strategy, if it is of the Other kind.
        """
    @property
    def other_data(self) -> Mapping[str, str] | None:
        """
        Access the data of the strategy, if it is of the Other kind.
        """

class CqlColumnType:
    """Base class for all CQL column types (native, collections, vectors, tuples, UDTs)."""

class CqlNativeType(CqlColumnType):
    """Base class for native Cassandra scalar types."""

class CqlAscii(CqlNativeType):
    """CQL ``ascii``: US-ASCII string."""

class CqlBigInt(CqlNativeType):
    """CQL ``bigint``: 64-bit signed integer."""

class CqlBlob(CqlNativeType):
    """CQL ``blob``: arbitrary bytes."""

class CqlBoolean(CqlNativeType):
    """CQL ``boolean``: true or false."""

class CqlCounter(CqlNativeType):
    """CQL ``counter``: 64-bit signed integer that is only incremented or decremented."""

class CqlDate(CqlNativeType):
    """CQL ``date``: calendar date without a time of day."""

class CqlDecimal(CqlNativeType):
    """CQL ``decimal``: arbitrary-precision decimal number."""

class CqlDouble(CqlNativeType):
    """CQL ``double``: 64-bit floating point number."""

class CqlDuration(CqlNativeType):
    """CQL ``duration``: length of time in months, days and nanoseconds."""

class CqlFloat(CqlNativeType):
    """CQL ``float``: 32-bit floating point number."""

class CqlInt(CqlNativeType):
    """CQL ``int``: 32-bit signed integer."""

class CqlInet(CqlNativeType):
    """CQL ``inet``: IPv4 or IPv6 address."""

class CqlSmallInt(CqlNativeType):
    """CQL ``smallint``: 16-bit signed integer."""

class CqlText(CqlNativeType):
    """CQL ``text``: UTF-8 string."""

class CqlTime(CqlNativeType):
    """CQL ``time``: time of day with nanosecond precision."""

class CqlTimestamp(CqlNativeType):
    """CQL ``timestamp``: date and time with millisecond precision."""

class CqlTimeuuid(CqlNativeType):
    """CQL ``timeuuid``: version 1, time-based UUID."""

class CqlTinyInt(CqlNativeType):
    """CQL ``tinyint``: 8-bit signed integer."""

class CqlUuid(CqlNativeType):
    """CQL ``uuid``: UUID of any version."""

class CqlVarint(CqlNativeType):
    """CQL ``varint``: arbitrary-precision integer."""

class CqlCollectionType(CqlColumnType):
    """Base class for CQL collection types (List, Map, Set)."""

    frozen: bool

class CqlList(CqlCollectionType):
    """CqlList<T> — ordered sequence of elements."""

    column_type: CqlColumnType

class CqlMap(CqlCollectionType):
    """CqlMap<K, V> — key-value pairs."""

    key_type: CqlColumnType
    value_type: CqlColumnType

class CqlSet(CqlCollectionType):
    """CqlSet<T> — unordered set of elements."""

    column_type: CqlColumnType

class CqlTuple(CqlColumnType):
    """CqlTuple<T1, T2, ...> — positional tuple of column types."""

    element_types: list[CqlColumnType]

class CqlVector(CqlColumnType):
    """CqlVector<T, N> — fixed-length vector of elements."""

    typ: CqlColumnType
    dimensions: int

class CqlUserDefinedType(CqlColumnType):
    """CQL user-defined type (UDT) — custom type with named fields."""

    name: str
    frozen: bool
    keyspace: str
    field_types: list[tuple[str, CqlColumnType]]

class ColumnKind(IntEnum):
    Regular = ...
    Static = ...
    Clustering = ...
    PartitionKey = ...

class Column:
    @property
    def typ(self) -> CqlColumnType:
        """
        Access the type of this column.
        """
    @property
    def kind(self) -> ColumnKind:
        """
        Access the kind of this column.
        """

class Table:
    @property
    def columns(self) -> Mapping[str, Column]:
        """
        Access the columns of this table as a read-only dictionary of name to column.
        """
    @property
    def partition_key(self) -> Mapping[str, Column]:
        """
        Access the partition key of this table as a read-only dictionary of name to column.
        """
    @property
    def clustering_key(self) -> Mapping[str, Column]:
        """
        Access the clustering key of this table as a read-only dictionary of name to column.
        """
    @property
    def partitioner(self) -> str | None:
        """
        Access the name of partitioner used by this table or None.
        """

class MaterializedView:
    @property
    def base_table_name(self) -> str:
        """
        Access the name of the base table of this materialized view.
        """
    @property
    def partition_key(self) -> Mapping[str, Column]:
        """
        Access the partition key of this view as a read-only dictionary of name to column.
        """
    @property
    def clustering_key(self) -> Mapping[str, Column]:
        """
        Access the clustering key of this view as a read-only dictionary of name to column.
        """
    @property
    def partitioner(self) -> str | None:
        """
        Access the name of partitioner used by this materialized view or None.
        """

class Keyspace:
    @property
    def strategy(self) -> Strategy:
        """
        Access the strategy used by this keyspace.
        """
    @property
    def tables(self) -> Mapping[str, Table]:
        """
        Access the tables of this keyspace as a read-only dictionary of name to table.
        """
    @property
    def views(self) -> Mapping[str, MaterializedView]:
        """
        Access the materialized views of this keyspace as a read-only dictionary of name to view.
        """

class ColumnSpec:
    """
    Specification of a column.
    """

    @property
    def name(self) -> str: ...
    @property
    def table_name(self) -> str: ...
    @property
    def keyspace_name(self) -> str: ...
    @property
    def cql_type(self) -> CqlColumnType: ...
