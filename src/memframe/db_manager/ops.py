import logging
from typing import Any, Dict, List, Optional

from memframe.exceptions import ConfigurationError, ConnectionNotReady, DataNotFound
from memframe.utils.async_sync import async_to_sync

logger = logging.getLogger("memFrame")


class OpsMixin:
    """Dataset registry and operation-history management for ``MemFrame``.

    Operates on ``self._backend``, ``self._active_id`` and
    ``self._placeholder`` — provided by the owning ``MemFrame`` instance.
    """

    # ── table listing / active management ──────────────────────────

    async def alist_tables(self) -> List[Dict[str, str]]:
        if not self._backend:
            raise ConnectionNotReady("Not connected.")
        rows = await self._backend.fetch(
            f"SELECT data_id, filename FROM {self._backend.csv_registry_table} "
            f"WHERE is_upload_success = TRUE ORDER BY uploaded_at DESC"
        )
        return [{"data_id": r[0], "filename": r[1]} for r in rows]

    @async_to_sync
    async def list_tables(self) -> List[Dict[str, str]]:
        return await self.alist_tables()

    async def aset_active(self, data_id: str) -> str:
        table_name = self._backend.get_upload_table_name(data_id)
        if not await self._backend.table_exists(table_name):
            raise DataNotFound(f"Table for data_id '{data_id}' does not exist")
        self._active_id = data_id
        logger.info(f"Active CSV set to {data_id}")
        return data_id

    @async_to_sync
    async def set_active(self, data_id: str) -> str:
        return await self.aset_active(data_id)

    async def aget_active_table(self) -> Optional[str]:
        return self._active_id

    @async_to_sync
    async def get_active_table(self) -> Optional[str]:
        return await self.aget_active_table()

    # ── delete / cache ────────────────────────────────────────────

    async def adelete_table(self, data_id: Optional[str] = None, filename: Optional[str] = None) -> None:
        if not self._backend:
            raise ConnectionNotReady("Not connected.")
        if not data_id and not filename:
            raise ConfigurationError("Provide either data_id or filename")
        if not data_id:
            row = await self._backend.fetch_row(
                f"SELECT data_id FROM {self._backend.csv_registry_table} "
                f"WHERE filename = {self._placeholder(1)}",
                filename,
            )
            if not row:
                raise DataNotFound(f"No table found for filename: {filename}")
            data_id = row[0]
        row = await self._backend.fetch_row(
            f"SELECT table_name FROM {self._backend.csv_registry_table} "
            f"WHERE data_id = {self._placeholder(1)}",
            data_id,
        )
        if not row:
            raise DataNotFound(f"No table found for data_id: {data_id}")
        upload_table = row[0]
        transient_rows = await self._backend.fetch(
            f"SELECT generated_table_name FROM {self._backend.transient_registry_table} "
            f"WHERE data_id = {self._placeholder(1)}",
            data_id,
        )
        for t in transient_rows:
            await self._backend.drop_table(t[0])
        await self._backend.drop_table(upload_table)
        await self._backend.execute(
            f"DELETE FROM {self._backend.csv_registry_table} WHERE data_id = {self._placeholder(1)}",
            data_id,
        )
        await self._backend.execute(
            f"DELETE FROM {self._backend.transient_registry_table} WHERE data_id = {self._placeholder(1)}",
            data_id,
        )
        if self._active_id == data_id:
            self._active_id = None
        logger.info(f"Deleted dataset {data_id}")

    @async_to_sync
    async def delete_table(self, data_id: Optional[str] = None, filename: Optional[str] = None) -> None:
        return await self.adelete_table(data_id, filename)

    async def _aclear_cache(self, data_id: str) -> None:
        rows = await self._backend.fetch(
            f"SELECT generated_table_name FROM {self._backend.transient_registry_table} "
            f"WHERE data_id = {self._placeholder(1)} AND generated_table_name IS NOT NULL",
            data_id,
        )
        for row in rows:
            await self._backend.drop_table(row[0])
        await self._backend.execute(
            f"DELETE FROM {self._backend.transient_registry_table} WHERE data_id = {self._placeholder(1)}",
            data_id,
        )

    # ── operation recording (used by ContextManager / wrappers) ──

    async def _arecord_operation(self, data_id: str, operation_type: str, generated_table_name: str) -> int:
        max_op = await self._backend.fetchval(
            f"SELECT COALESCE(MAX(opidx), 0) FROM {self._backend.transient_registry_table} WHERE data_id = {self._placeholder(1)}",
            data_id,
        )
        opidx = max_op + 1
        await self._backend.execute(
            f"INSERT INTO {self._backend.transient_registry_table} "
            f"(data_id, opidx, generated_table_name, operation_type) "
            f"VALUES ({self._placeholder(1)}, {self._placeholder(2)}, {self._placeholder(3)}, {self._placeholder(4)})",
            data_id, opidx, generated_table_name, operation_type,
        )
        return opidx

    async def _arecord_method_call(
        self, data_id: str, class_name: str, method_name: str,
        args_sig: str, kwargs_sig: str,
        generated_table_name: Optional[str] = None,
        is_deep_cache: bool = False, schema: Optional[str] = None,
    ) -> int:
        if not self._backend:
            raise ConnectionNotReady("Not connected.")
        max_op = await self._backend.fetchval(
            f"SELECT COALESCE(MAX(opidx), 0) FROM {self._backend.transient_registry_table} "
            f"WHERE data_id = {self._placeholder(1)}",
            data_id,
        )
        opidx = max_op + 1
        await self._backend.execute(
            f"INSERT INTO {self._backend.transient_registry_table} "
            f"(data_id, opidx, operation_type, class_name, method_name, args, kwargs, "
            f"generated_table_name, is_deep_cache, schema) "
            f"VALUES ({self._placeholder(1)}, {self._placeholder(2)}, {self._placeholder(3)}, "
            f"{self._placeholder(4)}, {self._placeholder(5)}, {self._placeholder(6)}, "
            f"{self._placeholder(7)}, {self._placeholder(8)}, {self._placeholder(9)}, {self._placeholder(10)})",
            data_id, opidx, "method_call", class_name, method_name,
            args_sig, kwargs_sig,
            generated_table_name, is_deep_cache, schema,
        )
        return opidx

    # ── operation listing / retrieval ────────────────────────────

    async def alist_operations(self, data_id: Optional[str] = None) -> List[Dict[str, Any]]:
        if data_id is None:
            data_id = self._active_id
            if data_id is None:
                raise DataNotFound("No data_id provided and no active CSV set.")
        rows = await self._backend.fetch(
            f"SELECT opidx, operation_type, generated_table_name, created_at "
            f"FROM {self._backend.transient_registry_table} "
            f"WHERE data_id = {self._placeholder(1)} ORDER BY opidx",
            data_id,
        )
        return [
            {"opidx": r[0], "operation_type": r[1], "table_name": r[2], "created_at": r[3]}
            for r in rows
        ]

    @async_to_sync
    async def list_operations(self, data_id: Optional[str] = None) -> List[Dict[str, Any]]:
        return await self.alist_operations(data_id)

    async def aretrieve_operation(self, data_id: str, opidx: int) -> str:
        row = await self._backend.fetch_row(
            f"SELECT generated_table_name FROM {self._backend.transient_registry_table} "
            f"WHERE data_id = {self._placeholder(1)} AND opidx = {self._placeholder(2)}",
            data_id, opidx,
        )
        if not row:
            raise DataNotFound(f"Operation {opidx} not found for data_id {data_id}")
        return row[0]

    @async_to_sync
    async def retrieve_operation(self, data_id: str, opidx: int) -> str:
        return await self.aretrieve_operation(data_id, opidx)
