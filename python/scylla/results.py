from collections.abc import Callable
from typing import Any, TypeAlias

from ._rust.cluster.metadata import ColumnSpec  # pyright: ignore[reportMissingModuleSource]
from ._rust.results import (  # pyright: ignore[reportMissingModuleSource]
    AsyncRowsIterator,
    ColumnIterator,
    DictRowFactory,
    PagingState,
    RequestResult,
    RowFactory,
    SinglePageIterator,
)
from .cql_types import CqlValue

RowBuilder: TypeAlias = Callable[[tuple[CqlValue, ...]], Any]
"""Builds a single row from its column values, in column order."""


RowFactoryLike: TypeAlias = RowFactory | RowBuilder
"""Anything accepted as `factory=`: a `RowFactory` instance, or a bare row builder."""


__all__ = [
    "AsyncRowsIterator",
    "ColumnIterator",
    "ColumnSpec",
    "DictRowFactory",
    "PagingState",
    "RequestResult",
    "RowBuilder",
    "RowFactory",
    "RowFactoryLike",
    "SinglePageIterator",
]
