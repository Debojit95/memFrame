import json
import logging
from pathlib import Path
from typing import TYPE_CHECKING, Any, Dict, List, Optional

if TYPE_CHECKING:
    import pandas as pd

from memframe.core.ingestion.datatype_detector import Backend
from memframe.db_manager.setup import DatabaseBackend, create_backend
from memframe.db_manager.context import ContextManager
from memframe.db_manager.adapters.factory import resolve_backend_config
from memframe.db_manager.pool import create_pool
from memframe.utils.async_sync import async_to_sync

logger = logging.getLogger("memFrame")
logger.setLevel(logging.INFO)
if not logger.handlers:
    handler = logging.StreamHandler()
    handler.setFormatter(logging.Formatter("%(asctime)s - %(name)s - %(levelname)s - %(message)s"))
    logger.addHandler(handler)


class MemFrame(ContextManager):
    def __init__(self, connection_type: str = "local", connection_params: Optional[Dict[str, Any]] = None, deep_cache: Optional[bool] = None):
        super().__init__(self)
        self.connection_type = connection_type
        self.conn_params = connection_params or {}
        self._backend: Optional[DatabaseBackend] = None
        self._active_id: Optional[str] = None
        self.deep_cache = deep_cache
        self.__uploader = None

    @property
    def _uploader(self):
        if self.__uploader is None:
            from memframe.core.ingestion.upload.base import Uploader

            u = Uploader()
            u._backend = self._backend
            u._type_detector = self._backend._type_detector if self._backend else None
            u._memframe_from_data_id = lambda data_id: ContextManager(self, data_id=data_id)
            self.__uploader = u
        return self.__uploader

    # ── connect ─────────────────────────────────────────────────────

    async def aconnect(self) -> None:
        backend_type, params = resolve_backend_config(self.connection_type, self.conn_params)
        self._pool = create_pool(backend_type, params)
        await self._pool.connect()
        self._backend = create_backend(backend_type, params)
        self._backend.pool = self._pool
        await self._backend.initialize()
        if self.__uploader:
            self.__uploader._backend = self._backend
            self.__uploader._type_detector = self._backend._type_detector

    @async_to_sync
    async def connect(self) -> None:
        return await self.aconnect()

    # ── table listing / active management ──────────────────────────

    async def alist_tables(self) -> List[Dict[str, str]]:
        if not self._backend:
            raise RuntimeError("Not connected.")
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
            raise ValueError(f"Table for data_id '{data_id}' does not exist")
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
            raise RuntimeError("Not connected.")
        if not data_id and not filename:
            raise ValueError("Provide either data_id or filename")
        if not data_id:
            row = await self._backend.fetch_row(
                f"SELECT data_id FROM {self._backend.csv_registry_table} "
                f"WHERE filename = {self._placeholder(1)}",
                filename,
            )
            if not row:
                raise ValueError(f"No table found for filename: {filename}")
            data_id = row[0]
        row = await self._backend.fetch_row(
            f"SELECT table_name FROM {self._backend.csv_registry_table} "
            f"WHERE data_id = {self._placeholder(1)}",
            data_id,
        )
        if not row:
            raise ValueError(f"No table found for data_id: {data_id}")
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
        max_op = await self._backend.fetch_val(
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
        args: tuple, kwargs: dict,
        generated_table_name: Optional[str] = None,
        is_deep_cache: bool = False, schema: Optional[str] = None,
    ) -> int:
        if not self._backend:
            raise RuntimeError("Not connected.")
        max_op = await self._backend.fetch_val(
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
            json.dumps(args), json.dumps(kwargs),
            generated_table_name, is_deep_cache, schema,
        )
        return opidx

    # ── operation listing / retrieval ────────────────────────────

    async def alist_operations(self, data_id: Optional[str] = None) -> List[Dict[str, Any]]:
        if data_id is None:
            data_id = self._active_id
            if data_id is None:
                raise ValueError("No data_id provided and no active CSV set.")
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
            raise ValueError(f"Operation {opidx} not found for data_id {data_id}")
        return row[0]

    @async_to_sync
    async def retrieve_operation(self, data_id: str, opidx: int) -> str:
        return await self.aretrieve_operation(data_id, opidx)

    # ── upload ──────────────────────────────────────────────────

    def _placeholder(self, index: int) -> str:
        return self._backend.placeholder(index)

    async def aupload_csv(self, file_path: str | Path) -> ContextManager:
        return await self._uploader._aupload_csv(file_path)

    @async_to_sync
    async def upload_csv(self, file_path: str | Path) -> ContextManager:
        return await self.aupload_csv(file_path)

    async def aupload_parquet(self, file_path: str | Path) -> ContextManager:
        return await self._uploader._aupload_parquet(file_path)

    @async_to_sync
    async def upload_parquet(self, file_path: str | Path) -> ContextManager:
        return await self.aupload_parquet(file_path)

    async def aupload_df(self, df: "pd.DataFrame", filename: Optional[str] = None) -> ContextManager:
        return await self._uploader._aupload_df(df, filename)

    @async_to_sync
    async def upload_df(self, df: "pd.DataFrame", filename: Optional[str] = None) -> ContextManager:
        return await self.aupload_df(df, filename=filename)

    # ── ops / context helpers ────────────────────────────────────

    def _ops(
        self,
        data_id: Optional[str] = None,
        data: Any = None,
        columns: Optional[List[str]] = None,
    ):
        if data_id is not None and data is not None:
            raise ValueError("Pass either `data_id` or `data`, not both.")
        if data is None and data_id is not None and not isinstance(data_id, str):
            data = data_id
            data_id = None
        if data is not None:
            try:
                import pandas as pd
            except ImportError as exc:
                raise ImportError(
                    "ops(data=...) requires pandas for DataFrame conversion."
                ) from exc
            if isinstance(data, pd.DataFrame):
                df = data
            else:
                df = pd.DataFrame(data, columns=columns)
            uploaded = self.upload_df(df)
            if isinstance(uploaded, ContextManager):
                return uploaded
            data_id = uploaded
        return ContextManager(self, data_id=data_id)

    def _local_db_path(self) -> Optional[Path]:
        if not self._backend or self._backend.backend != Backend.DUCKDB:
            raise RuntimeError("Local DuckDB connection is not active.")
        db_path = self._backend.conn_params.get("db_path", "memframe_new.duckdb")
        if db_path == ":memory:":
            return None
        return Path(db_path)

    async def close(self) -> None:
        await super().close()
        if hasattr(self, "_pool") and self._pool:
            await self._pool.close()

    def memFrame(self, data_id: Optional[str] = None, data: Any = None, columns: Optional[List[str]] = None):
        return self._ops(data_id, data, columns)

    async def __aenter__(self):
        await self.aconnect()
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        await self.close()


