"""
Core data types and constants for the RDBMS.
"""

from enum import Enum
from typing import Any, Union
from dataclasses import dataclass


class DataType(Enum):
    """Supported SQL data types."""
    INT = "INT"
    TEXT = "TEXT"
    BOOL = "BOOL"


@dataclass
class Column:
    """Represents a table column definition."""
    name: str
    dtype: DataType
    is_primary: bool = False
    is_unique: bool = False

    def validate_value(self, value: Any) -> bool:
        """Validate a value against the column's data type."""
        if value is None:
            return True  # Allow NULL values

        if self.dtype == DataType.INT:
            return isinstance(value, int)
        elif self.dtype == DataType.TEXT:
            return isinstance(value, str)
        elif self.dtype == DataType.BOOL:
            return isinstance(value, bool)
        return False


class Index:
    """Basic hash-based index implementation."""

    def __init__(self, column_name: str, is_unique: bool = False):
        self.column_name = column_name
        self.is_unique = is_unique
        self._index = {}  # value -> set of row_ids

    def insert(self, value: Any, row_id: int) -> bool:
        """Insert a value into the index."""
        if value is None:
            return True  # Don't index NULL values

        if self.is_unique and value in self._index and len(self._index[value]) > 0:
            return False  # Unique constraint violation

        if value not in self._index:
            self._index[value] = set()
        self._index[value].add(row_id)
        return True

    def delete(self, value: Any, row_id: int):
        """Remove a value from the index."""
        if value in self._index:
            self._index[value].discard(row_id)
            if not self._index[value]:
                del self._index[value]

    def update(self, old_value: Any, new_value: Any, row_id: int) -> bool:
        """Update index entry."""
        self.delete(old_value, row_id)
        return self.insert(new_value, row_id)

    def search(self, value: Any) -> set:
        """Find row_ids for a given value."""
        return self._index.get(value, set())

    def has_value(self, value: Any) -> bool:
        """Check if value exists in index."""
        return value in self._index