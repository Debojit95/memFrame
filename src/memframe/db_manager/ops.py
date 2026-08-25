import logging
from typing import Any, Dict, List, Optional

from memframe.exceptions import ConfigurationError, ConnectionNotReady, DataNotFound
from memframe.core.ingestion.datatype_detector import _generate_6char_id
from memframe.db_manager.context import ContextManager
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

    # ── SyncDB: register pre-existing tables ──────────────────────

    async def _alloc_sync_id(self) -> str:
        """Generate a data_id not already present in csv_registry."""
        registry = self._backend.csv_registry_table
        ph = self._placeholder
        while True:
            data_id = _generate_6char_id()
            row = await self._backend.fetch_row(
                f"SELECT data_id FROM {registry} WHERE data_id = {ph(1)}",
                data_id,
            )
            if not row:
                return data_id

    async def aregister_tables(self) -> Dict[str, List[Dict[str, Any]]]:
        """Register every non-empty user table in the DB into csv_registry.

        Enumerates schemas → tables via the backend, skips already-registered
        and empty tables, and inserts a fresh ``data_id`` for each so it can be
        activated with ``aset_active`` exactly like an upload. Returns
        ``{schema: [{data_id, table_name, row_count}, ...]}`` for what was
        registered this call.
        """
        if not self._backend:
            raise ConnectionNotReady("Not connected.")
        registry = self._backend.csv_registry_table
        ph = self._placeholder

        table_dict = await self._backend.list_user_tables()
        registered: Dict[str, List[Dict[str, Any]]] = {}

        for schema, tables in table_dict.items():
            for table in tables:
                existing = await self._backend.fetch_row(
                    f"SELECT data_id FROM {registry} "
                    f"WHERE schema = {ph(1)} AND table_name = {ph(2)}",
                    schema, table,
                )
                if existing:
                    continue
                qualified = f"{schema}.{table}" if schema else table
                if not await self._backend.table_exists(qualified):
                    continue
                row_count = await self._backend.fetchval(f"SELECT COUNT(*) FROM {qualified}")
                if not row_count or row_count == 0:
                    continue
                data_id = await self._alloc_sync_id()
                await self._backend.execute(
                    f"INSERT INTO {registry} "
                    f"(data_id, filename, table_name, row_count, is_upload_success, schema, is_external) "
                    f"VALUES ({ph(1)}, {ph(2)}, {ph(3)}, {ph(4)}, {ph(5)}, {ph(6)}, {ph(7)})",
                    data_id, table, table, row_count, True, schema, True,
                )
                registered.setdefault(schema, []).append(
                    {"data_id": data_id, "table_name": table, "row_count": row_count}
                )
        return registered

    @async_to_sync
    async def register_tables(self) -> Dict[str, List[Dict[str, Any]]]:
        return await self.aregister_tables()

    async def aset_active(self, data_id: str) -> ContextManager:
        row = await self._backend.fetch_row(
            f"SELECT table_name, schema FROM {self._backend.csv_registry_table} "
            f"WHERE data_id = {self._placeholder(1)}",
            data_id,
        )
        if not row:
            raise DataNotFound(f"No registry entry for {data_id}")
        table_name = row[0]
        schema = row[1] or self._backend.upload_schema
        qualified = f"{schema}.{table_name}" if schema else table_name
        if not await self._backend.table_exists(qualified):
            raise DataNotFound(f"Table for data_id '{data_id}' does not exist")
        self._active_id = data_id
        logger.info(f"Active CSV set to {data_id}")
        # ponytail: return a context bound to this data_id, matching the
        # ContextManager that upload_df/ops() hand back from main.py.
        return ContextManager(self, data_id=data_id)

    @async_to_sync
    async def set_active(self, data_id: str) -> ContextManager:
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
            f"SELECT table_name, is_external FROM {self._backend.csv_registry_table} "
            f"WHERE data_id = {self._placeholder(1)}",
            data_id,
        )
        if not row:
            raise DataNotFound(f"No table found for data_id: {data_id}")
        upload_table = row[0]
        is_external = bool(row[1]) if len(row) > 1 else False
        transient_rows = await self._backend.fetch(
            f"SELECT generated_table_name FROM {self._backend.transient_registry_table} "
            f"WHERE data_id = {self._placeholder(1)}",
            data_id,
        )
        for t in transient_rows:
            await self._backend.drop_table(t[0])
        # ponytail: synced tables live outside memFrame — never drop the
        # user's real table, only its registry/transient entries.
        if not is_external:
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
