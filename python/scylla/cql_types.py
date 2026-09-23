"""
The CQL type system as seen from Python.

``Cql*`` classes describe the CQL type of a column, as reported by the
schema or by a prepared statement. The ``CqlValue`` aliases describe the
Python objects the driver produces when reading those columns and accepts
when writing them.
"""

import ipaddress
from datetime import date, datetime, time
from decimal import Decimal
from typing import TYPE_CHECKING, TypeAlias
from uuid import UUID

from ._rust.cluster.metadata import (  # pyright: ignore[reportMissingModuleSource]
    CqlAscii,
    CqlBigInt,
    CqlBlob,
    CqlBoolean,
    CqlCollectionType,
    CqlColumnType,
    CqlCounter,
    CqlDate,
    CqlDecimal,
    CqlDouble,
    CqlDuration,
    CqlFloat,
    CqlInet,
    CqlInt,
    CqlList,
    CqlMap,
    CqlNativeType,
    CqlSet,
    CqlSmallInt,
    CqlText,
    CqlTime,
    CqlTimestamp,
    CqlTimeuuid,
    CqlTinyInt,
    CqlTuple,
    CqlUserDefinedType,
    CqlUuid,
    CqlVarint,
    CqlVector,
)
from ._rust.value import CqlEmpty  # pyright: ignore[reportMissingModuleSource]

if TYPE_CHECKING:
    from dateutil.relativedelta import relativedelta

    CqlNative: TypeAlias = (
        # CQL:
        # - Counter
        # - TinyInt
        # - SmallInt
        # - Int
        # - BigInt
        # - Varint
        int
        # CQL:
        # - Float
        # - Double
        | float
        # CQL:
        # - Ascii
        # - Text
        | str
        # CQL:
        # - Boolean
        | bool
        # CQL:
        # - Blob
        | bytes
        # CQL:
        # - Decimal
        | Decimal
        # CQL:
        # - Uuid
        # - Timeuuid
        | UUID
        # CQL:
        # - Inet (IPv4)
        | ipaddress.IPv4Address
        # CQL:
        # - Inet (IPv6)
        | ipaddress.IPv6Address
        # CQL:
        # - Date
        | date
        # CQL:
        # - Timestamp
        | datetime
        # CQL:
        # - Time
        | time
        # CQL:
        # - Duration
        | relativedelta
        # CQL:
        # - Empty
        # - null
        | None
    )

    # CQL list and vector -> list, set -> set, tuple -> tuple,
    # map and user defined type -> dict.
    CqlCollection: TypeAlias = (
        list["CqlValue"] | set["CqlValue"] | tuple["CqlValue", ...] | dict["CqlValue", "CqlValue"]
    )

    CqlValue: TypeAlias = CqlNative | CqlCollection
else:
    # Runtime stand-ins without recursion, so get_type_hints() can resolve them.
    try:
        from dateutil.relativedelta import relativedelta
    except ImportError:
        relativedelta = None

    CqlNative = (
        int
        | float
        | str
        | bool
        | bytes
        | Decimal
        | UUID
        | ipaddress.IPv4Address
        | ipaddress.IPv6Address
        | date
        | datetime
        | time
        | relativedelta
        | None
    )
    CqlCollection = list | set | tuple | dict
    CqlValue = CqlNative | CqlCollection

__all__ = [
    "CqlAscii",
    "CqlBigInt",
    "CqlBlob",
    "CqlBoolean",
    "CqlCollection",
    "CqlCollectionType",
    "CqlColumnType",
    "CqlCounter",
    "CqlDate",
    "CqlDecimal",
    "CqlDouble",
    "CqlDuration",
    "CqlEmpty",
    "CqlFloat",
    "CqlInet",
    "CqlInt",
    "CqlList",
    "CqlMap",
    "CqlNative",
    "CqlNativeType",
    "CqlSet",
    "CqlSmallInt",
    "CqlText",
    "CqlTime",
    "CqlTimestamp",
    "CqlTimeuuid",
    "CqlTinyInt",
    "CqlTuple",
    "CqlUserDefinedType",
    "CqlUuid",
    "CqlValue",
    "CqlVarint",
    "CqlVector",
]
