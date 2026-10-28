"""Database-native index operations.

Indexes are owned by the backend: memFrame never stores index metadata. Each
subclass implements ``create_index`` / ``drop_index`` / ``list_indexes`` for
its dialect and catalog; ClickHouse additionally implements the physical
``ORDER BY`` rebuild.
"""

from __future__ import annotations

import hashlib
import re
from typing import Any, Dict, List, Optional

from memframe.core.analytix._response import fail, ok
from memframe.utils.helper import SQLIdentifierSanitizer


class DataIndexOps:
    backend = ""
    index_name_limit = 255

    def __init__(self, db):
        self.db = db

    # ------------------------------------------------------------------
    # Shared helpers
    # ------------------------------------------------------------------
    def _q(self, name: str) -> str:
        return self.db.quote_identifier(SQLIdentifierSanitizer.sanitize(name))

    def _qualified(self, table: str, schema: str) -> str:
        return f"{self._q(schema)}.{self._q(table)}"

    async def _available_columns(self, table: str, schema: str) -> Dict[str, str]:
        return await self.db.get_column_types(table, schema)

    def _as_columns(self, columns: Any) -> List[str]:
        if isinstance(columns, str):
            return [columns.strip()]
        if not isinstance(columns, (list, tuple)):
            return []
        return [str(c).strip() for c in columns if str(c).strip()]

    def _fit_name(self, name: str) -> str:
        """Keep a generated/explicit index name within the backend limit."""
        safe = re.sub(r"[^0-9A-Za-z_]+", "_", str(name)).strip("_") or "idx"
        if len(safe) > self.index_name_limit:
            digest = hashlib.sha1(safe.encode("utf-8")).hexdigest()[:8]
            safe = f"{safe[: self.index_name_limit - 9]}_{digest}"
        return safe

    def _resolve_name(
        self,
        name: Optional[str],
        table: str,
        columns: List[str],
        expression: Optional[str] = None,
    ) -> str:
        if name:
            return self._fit_name(name)
        suffix = "_".join(columns) if columns else "skip"
        return self._fit_name(f"{table}_{suffix}_idx")

    def _validate_columns(
        self, columns: List[str], available: Dict[str, str]
    ) -> Optional[Dict[str, Any]]:
        if not columns:
            return fail("Provide at least one column for the index")
        missing = [c for c in columns if c not in available]
        if missing:
            return fail(f"Columns not found: {missing}", missing)
        return None

    @staticmethod
    def _ok(message: str, result: Any, **extra: Any) -> Dict[str, Any]:
        return ok(message, result=result, **extra)

    async def create_index(self, *args, **kwargs) -> Dict[str, Any]:
        raise NotImplementedError

    async def drop_index(self, *args, **kwargs) -> Dict[str, Any]:
        raise NotImplementedError

    async def list_indexes(self, *args, **kwargs) -> Dict[str, Any]:
        raise NotImplementedError

    async def set_order_by(self, *args, **kwargs) -> Dict[str, Any]:
        return fail("set_order_by is only supported for ClickHouse")
