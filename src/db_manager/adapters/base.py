from abc import ABC, abstractmethod
from typing import Any, Dict, List


class DatabaseAdapter(ABC):
    """Abstract interface for database‑specific operations."""

    @abstractmethod
    async def connect(self) -> None:
        """Establish connection / pool."""
        pass

    @abstractmethod
    async def close(self) -> None:
        """Close connection / pool."""
        pass

    @abstractmethod
    async def execute(self, sql: str, *args) -> Any:
        """Execute a statement that does not return rows."""
        pass

    @abstractmethod
    async def fetch(self, sql: str, *args) -> List[Any]:
        """Execute a SELECT and return all rows."""
        pass

    @abstractmethod
    async def fetchval(self, sql: str, *args) -> Any:
        """Execute a SELECT and return a single value."""
        pass

    @abstractmethod
    async def fetchrow(self, sql: str, *args) -> Any:
        """Execute a SELECT and return a single row."""
        pass

    @abstractmethod
    async def get_column_types(self, table: str, schema: str) -> Dict[str, str]:
        """Return mapping of column name → data type."""
        pass

    @abstractmethod
    async def get_table_info(self, table: str, schema: str) -> Dict[str, Any]:
        """Return comprehensive table metadata (size, row count, etc.)."""
        pass

    @abstractmethod
    async def table_exists(self, table: str, schema: str) -> bool:
        """Check if a table exists in the given schema."""
        pass

    @abstractmethod
    def placeholder(self, index: int = 1) -> str:
        """Parameter placeholder style (e.g. '$1', '?')."""
        pass

    @abstractmethod
    def quote_identifier(self, name: str) -> str:
        """Return a properly quoted SQL identifier."""
        pass
