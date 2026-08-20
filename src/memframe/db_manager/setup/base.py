import logging
from abc import ABC, abstractmethod
from typing import Any, List, Optional, Tuple

from memframe.core.ingestion.datatype_detector import DatatypeDetector
from memframe.exceptions import ConnectionNotReady

logger = logging.getLogger(__name__)


class DatabaseBackend(ABC):
    backend: str
    conn_params: dict
    registry_schema: str
    registry_table: str

    def __init__(
        self,
        conn_params: dict,
        registry_schema: str = "public",
        registry_table: str = "csv_registry",
    ):
        self.conn_params = conn_params
        self.registry_schema = registry_schema
        self.registry_table = registry_table
        self.registry_table_full = f"{registry_schema}.{registry_table}"
        self._type_detector = DatatypeDetector()
        self.pool = None

    @property
    @abstractmethod
    def placeholder(self) -> str:
        pass

    @abstractmethod
    async def _setup_database(self) -> None:
        pass

    @abstractmethod
    async def _create_transient_schema(self) -> None:
        pass

    async def execute(self, query: str, *params) -> None:
        if self.pool is None:
            raise ConnectionNotReady("Backend pool not set. Call aconnect() first.")
        await self.pool.execute(query, *params)

    async def fetch(self, query: str, *params) -> List[Tuple]:
        if self.pool is None:
            raise ConnectionNotReady("Backend pool not set. Call aconnect() first.")
        return await self.pool.fetch(query, *params)

    async def fetch_row(self, query: str, *params) -> Optional[Tuple]:
        if self.pool is None:
            raise ConnectionNotReady("Backend pool not set. Call aconnect() first.")
        return await self.pool.fetchrow(query, *params)

    async def fetch_one(self, query: str, *params) -> Optional[Tuple]:
        return await self.fetch_row(query, *params)

    async def fetchval(self, query: str, *params) -> Any:
        if self.pool is None:
            raise ConnectionNotReady("Backend pool not set. Call aconnect() first.")
        return await self.pool.fetchval(query, *params)

    async def initialize(self) -> None:
        await self._setup_database()
        await self._create_transient_schema()

    async def _column_exists(self, schema: str, table: str, column: str) -> bool:
        if self.backend == "clickhouse":
            query = """
                SELECT count()
                FROM system.columns
                WHERE database = ?
                  AND table = ?
                  AND name = ?
            """
        else:
            p1 = self.placeholder(1)
            p2 = self.placeholder(2)
            p3 = self.placeholder(3)
            query = f"""
                SELECT COUNT(*)
                FROM information_schema.columns
                WHERE table_schema = {p1}
                  AND table_name = {p2}
                  AND column_name = {p3}
            """
        count = await self.fetchval(query, schema, table, column)
        return bool(count and count > 0)

    async def _migrate_transient_registry_schema(self) -> None:
        schema_name = self.registry_schema
        table_name = "transient_registry"
        fq_table_name = f"{schema_name}.{table_name}"

        required_columns = {
            "class_name": "TEXT",
            "method_name": "TEXT",
            "args": "TEXT",
            "kwargs": "TEXT",
            "is_deep_cache": "BOOLEAN",
            "schema": "TEXT",
        }

        for column_name, column_type in required_columns.items():
            if not await self._column_exists(schema_name, table_name, column_name):
                if self.backend == "clickhouse":
                    if column_name == "is_deep_cache":
                        ch_type = "Bool"
                    else:
                        ch_type = "Nullable(String)"
                else:
                    ch_type = column_type
                await self.execute(
                    f"ALTER TABLE {fq_table_name} ADD COLUMN {column_name} {ch_type}"
                )

        if self.backend == "clickhouse":
            try:
                await self.execute(
                    f"ALTER TABLE {fq_table_name} "
                    f"UPDATE is_deep_cache = false WHERE is_deep_cache IS NULL"
                )
            except Exception:
                pass
            try:
                await self.execute(
                    f"ALTER TABLE {fq_table_name} "
                    f"MODIFY COLUMN is_deep_cache Bool"
                )
            except Exception:
                pass

        if self.backend != "clickhouse":
            try:
                await self.execute(
                    f"ALTER TABLE {fq_table_name} "
                    f"ALTER COLUMN generated_table_name DROP NOT NULL"
                )
            except Exception:
                pass

    async def _migrate_csv_registry_schema(self) -> None:
        schema_name = self.registry_schema
        table_name = "csv_registry"
        fq_table_name = f"{schema_name}.{table_name}"

        if not await self._column_exists(schema_name, table_name, "schema"):
            column_type = "Nullable(String)" if self.backend == "clickhouse" else "TEXT"
            await self.execute(
                f"ALTER TABLE {fq_table_name} ADD COLUMN schema {column_type}"
            )

        if self.backend == "clickhouse":
            try:
                await self.execute(
                    f"ALTER TABLE {fq_table_name} UPDATE schema = 'upload' WHERE schema IS NULL"
                )
            except Exception:
                pass
        else:
            await self.execute(
                f"UPDATE {fq_table_name} SET schema = {self.placeholder(1)} WHERE schema IS NULL",
                self.upload_schema,
            )

    async def _resolve_encoding(self, file_path: str) -> str:
        detected = self._type_detector._detect_encoding(file_path)

        def _validate(enc):
            try:
                with open(file_path, "rb") as f:
                    raw = f.read(65536)
                raw.decode(enc)
                return True
            except (UnicodeDecodeError, LookupError):
                return False

        for enc in (detected, "utf-8", "latin-1", "cp1252"):
            if _validate(enc):
                return enc
        return "latin-1"

    async def _detect_file_type(self, file_path: str) -> str:
        return self._type_detector._detect_file_type(file_path)

    async def _detect_delimiter(self, file_path: str, encoding: str) -> str:
        return self._type_detector._detect_delimiter(file_path, encoding)

    async def _detect_has_header(self, file_path: str, encoding: str, delimiter: str) -> bool:
        return self._type_detector._detect_has_header(file_path, encoding, delimiter)

    async def _infer_columns(self, file_path: str, encoding: str, delimiter: str, has_header: bool) -> List[str]:
        return self._type_detector._infer_columns(file_path, encoding, delimiter, has_header)

    async def _infer_types(self, file_path: str, encoding: str, delimiter: str, has_header: bool, columns: List[str]) -> List[str]:
        return self._type_detector._infer_types(file_path, encoding, delimiter, has_header, columns)

    def _sanitize_table_name(self, name: str) -> str:
        return self._type_detector._sanitize_table_name(name)

    @abstractmethod
    def get_upload_table_name(self, data_id: str) -> str:
        pass

    @abstractmethod
    def get_transient_table_name(self, data_id: str, opidx: int) -> str:
        pass

    @property
    @abstractmethod
    def transient_registry_table(self) -> str:
        pass

    @property
    @abstractmethod
    def csv_registry_table(self) -> str:
        pass
