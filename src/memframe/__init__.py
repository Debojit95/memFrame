"""memFrame - Database-backed DataFrame operations."""

from .main import MemFrame
from .core.ingestion.datatype_detector import Backend
from .db_manager.setup import DatabaseBackend
from .db_manager.context import ContextManager

__all__ = ["MemFrame", "Backend", "DatabaseBackend", "ContextManager"]
