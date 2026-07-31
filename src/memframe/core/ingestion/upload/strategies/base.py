from __future__ import annotations

import logging
from abc import ABC, abstractmethod
from typing import TYPE_CHECKING, Any, Dict, Optional, Tuple

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
            table_name = self._uploader.get_upload_table_name(data_id)
            if not await self._uploader.table_exists(table_name):
                return data_id, table_name

    async def _register(
        self,
        data_id: str,
        filename: str,
        table_name: str,
        row_count: int,
    ) -> None:
        await self._uploader._backend.execute(
            f"""
            INSERT INTO {self._uploader._backend.csv_registry_table} (data_id, filename, table_name, row_count, is_upload_success)
            VALUES ({self._uploader._placeholder(1)}, {self._uploader._placeholder(2)}, {self._uploader._placeholder(3)}, {self._uploader._placeholder(4)}, {self._uploader._placeholder(5)})
            """,
            data_id,
            filename,
            table_name,
            row_count,
            True,
        )

    async def _require_connection(self) -> None:
        if not self._uploader._backend:
            raise ConnectionNotReady("Not connected. Call await connect() first.")

    @abstractmethod
    async def upload(self, **kwargs: Any) -> str:
        """Upload the source and return the registered data_id."""
