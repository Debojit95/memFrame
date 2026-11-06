import logging
from typing import Any, Dict, List, Optional

from memframe.exceptions import ConfigurationError, ConnectionNotReady, DataNotFound
from memframe.core.indexing import mf_idx_sql_type, select_mf_idx_dtype
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
                # ponytail: qualify (not bare) so names with spaces/dashes
                # address themselves instead of failing as raw SQL.
                qualified = self._backend.qualify_table(schema, table) if schema else table
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

    async def _ensure_mf_idx(self, data_id: str, table: str, schema: str) -> None:
        """Add _mf_idx to a synced/external dataset on first activation."""
        backend = self._backend
        if await backend._column_exists(schema, table, "_mf_idx"):
            return
        row_count = int(await backend.fetchval(
            f"SELECT COUNT(*) FROM {backend.qualify_table(schema, table)}"
        ) or 0)
        logical = select_mf_idx_dtype(row_count)
        sql_type = mf_idx_sql_type(logical, backend.backend)
        q = backend.qualify_table(schema, table)
        tmp = f"{table}__mfidx_tmp"
        q_tmp = backend.qualify_table(schema, tmp)
        if getattr(backend, "backend", None) == "clickhouse":
            await backend.execute(
                f"CREATE TABLE {q_tmp} ENGINE = MergeTree() ORDER BY tuple() AS "
                f"SELECT *, CAST(ROW_NUMBER() OVER () - 1 AS {sql_type}) AS `_mf_idx` FROM {q}"
            )
            await backend.execute(f"EXCHANGE TABLES {q} AND {q_tmp}")
            await backend.execute(f"DROP TABLE {q_tmp}")
        else:
            await backend.execute(
                f'CREATE TABLE {q_tmp} AS '
                f'SELECT *, CAST(ROW_NUMBER() OVER () - 1 AS {sql_type}) AS "_mf_idx" FROM {q}'
            )
            await backend.execute(f"DROP TABLE {q}")
            await backend.execute(f'ALTER TABLE {q_tmp} RENAME TO "{table}"')
        await backend.execute(
            f"UPDATE {backend.csv_registry_table} "
            f"SET mf_idx_dtype = {self._placeholder(1)}, "
            f"mf_idx_max = {self._placeholder(2)}, mf_idx_ready = TRUE "
            f"WHERE data_id = {self._placeholder(3)}",
            logical, max(row_count - 1, 0), data_id,
        )

    async def aset_active(self, data_id: str) -> ContextManager:
        row = await self._backend.fetch_row(
            f"SELECT table_name, schema, base_table, base_schema "
            f"FROM {self._backend.csv_registry_table} "
            f"WHERE data_id = {self._placeholder(1)}",
            data_id,
        )
        if not row:
            raise DataNotFound(f"No registry entry for {data_id}")
        base_table = row[2] if len(row) > 2 and row[2] else row[0]
        schema = (
            (row[3] if len(row) > 3 and row[3] else row[1])
            or self._backend.upload_schema
        )
        qualified = f"{schema}.{base_table}" if schema else base_table
        if not await self._backend.table_exists(qualified):
            raise DataNotFound(f"Table for data_id '{data_id}' does not exist")
        await self._ensure_mf_idx(data_id, base_table, schema)
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
            f"SELECT table_name, is_external, base_table, base_schema "
            f"FROM {self._backend.csv_registry_table} "
            f"WHERE data_id = {self._placeholder(1)}",
            data_id,
        )
        if not row:
            raise DataNotFound(f"No table found for data_id: {data_id}")
        public_table = row[0]
        is_external = bool(row[1]) if len(row) > 1 else False
        base_table = row[2] if len(row) > 2 else None
        base_schema = (
            (row[3] if len(row) > 3 else None)
            or self._backend.upload_schema
        )
        cache_rows = await self._owned_cache_tables(data_id)
        # ponytail: one transaction — drops + registry deletes commit together,
        # so a crash can't leave tables without rows or rows without tables.
        async with self._backend.transaction():
            for sch, bare in cache_rows:
                # ponytail: stored names are bare — qualify with the recorded
                # schema (pre-upgrade rows fall back to cache); an
                # unqualified drop would target the upload schema and miss.
                await self._backend.drop_table(
                    self._backend.qualify_table(sch or self._backend.cache_schema, bare)
                )
            # ponytail: synced tables live outside memFrame — never drop the
            # user's real table, only its registry/cache entries.
            if not is_external:
                q_public = self._backend.qualify_table(
                    self._backend.upload_schema, public_table
                )
                if base_table:
                    await self._backend.execute(
                        f"DROP VIEW IF EXISTS {q_public}"
                    )
                    await self._backend.drop_table(
                        self._backend.qualify_table(base_schema, base_table)
                    )
                else:
                    await self._backend.drop_table(q_public)
            await self._delete_cache_payloads(data_id)
            await self._delete_cache_markers(data_id)
            await self._backend.execute(
                f"DELETE FROM {self._backend.csv_registry_table} WHERE data_id = {self._placeholder(1)}",
                data_id,
            )
            await self._backend.execute(
                f"DELETE FROM {self._backend.cache_registry_table} WHERE data_id = {self._placeholder(1)}",
                data_id,
            )
        if self._active_id == data_id:
            self._active_id = None
        logger.info(f"Deleted dataset {data_id}")

    @async_to_sync
    async def delete_table(self, data_id: Optional[str] = None, filename: Optional[str] = None) -> None:
        return await self.adelete_table(data_id, filename)

    async def _owned_cache_tables(self, data_id: str):
        """Tables the cache may drop: legacy copies and owns_table refs only."""
        protected_row = await self._backend.fetch_row(
            f"SELECT table_name, base_table FROM {self._backend.csv_registry_table} "
            f"WHERE data_id = {self._placeholder(1)}",
            data_id,
        )
        protected = {name for name in (protected_row or ()) if name}
        rows = await self._backend.fetch(
            f"SELECT schema, generated_table_name, ref_schema, ref_table, "
            f"owns_table, cache_kind, order_schema, order_table "
            f"FROM {self._backend.cache_registry_table} "
            f"WHERE data_id = {self._placeholder(1)} "
            f"AND operation_type NOT IN "
            f"('window_map', 'groupby_map', 'groupby_cum_map', 'groupby_win_map')",
            data_id,
        )
        out = []
        for sch, bare, ref_sch, ref_table, owns, kind, ord_sch, ord_tbl in rows:
            if ord_tbl:
                out.append((ord_sch or self._backend.cache_schema, ord_tbl))
            if kind == "actual":
                continue
            if kind == "ref":
                if owns and ref_table and ref_table not in protected:
                    out.append((ref_sch or sch or self._backend.cache_schema, ref_table))
            elif bare and bare not in protected:
                out.append((sch or self._backend.cache_schema, bare))
        return out

    async def _delete_cache_payloads(self, data_id: str) -> None:
        await self._backend.execute(
            f"DELETE FROM {self._backend.cache_payloads_table} "
            f"WHERE data_id = {self._placeholder(1)}",
            data_id,
        )

    async def _delete_cache_markers(self, data_id: str) -> None:
        await self._backend.execute(
            f"DELETE FROM {self._backend.cache_markers_table} "
            f"WHERE data_id = {self._placeholder(1)}",
            data_id,
        )

    async def _aclear_cache(self, data_id: str) -> None:
        rows = await self._owned_cache_tables(data_id)
        async with self._backend.transaction():
            for sch, bare in rows:
                await self._backend.drop_table(
                    self._backend.qualify_table(sch or self._backend.cache_schema, bare)
                )
            await self._delete_cache_payloads(data_id)
            await self._delete_cache_markers(data_id)
            await self._backend.execute(
                f"DELETE FROM {self._backend.cache_registry_table} WHERE data_id = {self._placeholder(1)}",
                data_id,
            )

    # ── operation recording (used by ContextManager / wrappers) ──

    async def _arecord_operation(self, data_id: str, operation_type: str, generated_table_name: str) -> int:
        async with self._backend._locked_op(data_id):
            max_op = await self._backend.fetchval(
                f"SELECT COALESCE(MAX(opidx), 0) FROM {self._backend.cache_registry_table} WHERE data_id = {self._placeholder(1)}",
                data_id,
            )
            opidx = (max_op or 0) + 1
            await self._backend.execute(
                f"INSERT INTO {self._backend.cache_registry_table} "
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
        table_scope: Optional[str] = None, base_fingerprint: Optional[str] = None,
        result_envelope: Optional[str] = None,
        cache_kind: Optional[str] = None,
        ref_schema: Optional[str] = None,
        ref_table: Optional[str] = None,
        involved_cols: Optional[str] = None,
        generated_cols: Optional[str] = None,
        result_cols: Optional[str] = None,
        owns_table: bool = False,
        epoch: int = 0,
        input_refs: Optional[str] = None,
        order_schema: Optional[str] = None,
        order_table: Optional[str] = None,
        order_position_dtype: Optional[str] = None,
        order_kind: Optional[str] = None,
        order_limit: Optional[int] = None,
    ) -> int:
        if not self._backend:
            raise ConnectionNotReady("Not connected.")
        async with self._backend._locked_op(data_id):
            max_op = await self._backend.fetchval(
                f"SELECT COALESCE(MAX(opidx), 0) FROM {self._backend.cache_registry_table} "
                f"WHERE data_id = {self._placeholder(1)}",
                data_id,
            )
            opidx = (max_op or 0) + 1
            await self._backend.execute(
                f"INSERT INTO {self._backend.cache_registry_table} "
                f"(data_id, opidx, operation_type, class_name, method_name, args, kwargs, "
                f"generated_table_name, is_deep_cache, schema, table_scope, base_fingerprint, "
                f"result_envelope, cache_kind, ref_schema, ref_table, involved_cols, "
                f"generated_cols, result_cols, owns_table, epoch, input_refs, "
                f"order_schema, order_table, order_position_dtype, order_kind, order_limit) "
                f"VALUES ({self._placeholder(1)}, {self._placeholder(2)}, {self._placeholder(3)}, "
                f"{self._placeholder(4)}, {self._placeholder(5)}, {self._placeholder(6)}, "
                f"{self._placeholder(7)}, {self._placeholder(8)}, {self._placeholder(9)}, {self._placeholder(10)}, "
                f"{self._placeholder(11)}, {self._placeholder(12)}, {self._placeholder(13)}, "
                f"{self._placeholder(14)}, {self._placeholder(15)}, {self._placeholder(16)}, "
                f"{self._placeholder(17)}, {self._placeholder(18)}, {self._placeholder(19)}, "
                f"{self._placeholder(20)}, {self._placeholder(21)}, {self._placeholder(22)}, "
                f"{self._placeholder(23)}, {self._placeholder(24)}, {self._placeholder(25)}, "
                f"{self._placeholder(26)}, {self._placeholder(27)})",
                data_id, opidx, "method_call", class_name, method_name,
                args_sig, kwargs_sig,
                generated_table_name, is_deep_cache, schema,
                table_scope, base_fingerprint, result_envelope,
                cache_kind, ref_schema, ref_table, involved_cols,
                generated_cols, result_cols, owns_table, epoch, input_refs,
                order_schema, order_table, order_position_dtype, order_kind, order_limit,
            )
            return opidx

    # ── operation listing / retrieval ────────────────────────────

    async def alist_operations(self, data_id: Optional[str] = None) -> List[Dict[str, Any]]:
        if data_id is None:
            data_id = self._active_id
            if data_id is None:
                raise DataNotFound("No data_id provided and no active CSV set.")
        rows = await self._backend.fetch(
            f"SELECT opidx, operation_type, "
            f"COALESCE(ref_table, generated_table_name), created_at "
            f"FROM {self._backend.cache_registry_table} "
            f"WHERE data_id = {self._placeholder(1)} "
            f"AND operation_type NOT IN "
            f"('window_map', 'groupby_map', 'groupby_cum_map', 'groupby_win_map') "
            f"ORDER BY opidx",
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
            f"SELECT COALESCE(ref_table, generated_table_name) "
            f"FROM {self._backend.cache_registry_table} "
            f"WHERE data_id = {self._placeholder(1)} AND opidx = {self._placeholder(2)}",
            data_id, opidx,
        )
        if not row:
            raise DataNotFound(f"Operation {opidx} not found for data_id {data_id}")
        return row[0]

    @async_to_sync
    async def retrieve_operation(self, data_id: str, opidx: int) -> str:
        return await self.aretrieve_operation(data_id, opidx)
