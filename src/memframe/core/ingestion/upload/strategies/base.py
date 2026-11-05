from __future__ import annotations

import logging
from abc import ABC, abstractmethod
from typing import TYPE_CHECKING, Any, Tuple

from memframe.core.ingestion.datatype_detector import _generate_6char_id
from memframe.exceptions import ConnectionNotReady

if TYPE_CHECKING:
    from memframe.core.ingestion.upload.base import Uploader


logger = logging.getLogger("memFrame")


class UploadStrategy(ABC):
    """Base upload strategy.

    Each strategy owns the full upload pipeline for one input format
    (CSV / Parquet / DataFrame) and returns a ``data_id``. Shared
    infrastructure lives on the ``Uploader`` context (``self._uploader``).
    """

    def __init__(self, uploader: "Uploader"):
        self._uploader = uploader

    async def _alloc_data_id(self) -> Tuple[str, str]:
        while True:
            data_id = _generate_6char_id()
            base_table = f"{self._uploader.get_upload_table_name(data_id)}__base"
            if not await self._uploader.table_exists(base_table):
                return data_id, base_table

    async def _register(
        self,
        data_id: str,
        filename: str,
        table_name: str,
        row_count: int,
    ) -> None:
        backend = self._uploader._backend
        schema = backend.upload_schema
        await backend.execute(
            f"""
            INSERT INTO {backend.csv_registry_table} (data_id, filename, table_name, row_count, is_upload_success, schema, base_table, base_schema)
            VALUES ({self._uploader._placeholder(1)}, {self._uploader._placeholder(2)}, {self._uploader._placeholder(3)}, {self._uploader._placeholder(4)}, {self._uploader._placeholder(5)}, {self._uploader._placeholder(6)}, {self._uploader._placeholder(7)}, {self._uploader._placeholder(8)})
            """,
            data_id,
            filename,
            data_id,
            row_count,
            True,
            schema,
            table_name,
            schema,
        )
        await self._uploader._ensure_mf_idx(data_id, table_name, row_count)
        await self._uploader._create_public_view(data_id, table_name, schema)

    async def _require_connection(self) -> None:
        if not self._uploader._backend:
            raise ConnectionNotReady("Not connected. Call await connect() first.")

    @abstractmethod
    async def upload(self, **kwargs: Any) -> str:
        """Upload the source and return the registered data_id."""
