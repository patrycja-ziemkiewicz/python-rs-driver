from typing import Final

from ._rust.batch import Batch, BatchType  # pyright: ignore[reportMissingModuleSource]
from ._rust.enums import Consistency, SerialConsistency  # pyright: ignore[reportMissingModuleSource]
from ._rust.statement import PreparedStatement, Statement  # pyright: ignore[reportMissingModuleSource]
from ._rust.types import UnsetType  # pyright: ignore[reportMissingModuleSource]

# Singleton instance
UNSET: Final[UnsetType] = UnsetType()
"""
No value provided, distinct from `None`, which means explicitly set to no value.

Used for options like `serial_consistency` and `request_timeout`,
where the two cases may result in different behaviour.
"""

# Make UnsetType unimportable in user facing API
del UnsetType

__all__ = [
    "UNSET",
    "Batch",
    "BatchType",
    "Consistency",
    "PreparedStatement",
    "SerialConsistency",
    "Statement",
]
