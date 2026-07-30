import logging
from typing import Any, List, Optional, Tuple

from memframe.core.ingestion.datatype_detector import Backend
from memframe.db_manager.setup.base import DatabaseBackend

logger = logging.getLogger(__name__)


def _sanitize_schema_name(value: str) -> str:
    import re
    name = re.sub(r"[^A-Za-z0-9_]", "_", value.strip())
    name = re.sub(r"_+", "_", name).strip("_")
    if not name:
        raise ValueError("schema_prefix must contain at least one alphanumeric character")
    if name[0].isdigit():
        name = f"mf_{name}"
    return name.lower()


class ClickHouseBackend(DatabaseBackend):
    backend = "clickhouse"

    def __init__(self, conn_params: dict):
        super().__init__(conn_params)
        schema_prefix = conn_params.get("schema_prefix")
        if schema_prefix:
            prefix = _sanitize_schema_name(str(schema_prefix))
            self.upload_schema = f"{prefix}_upload"
            self.transient_schema = f"{prefix}_transient"
            self.registry_schema = f"{prefix}_registry"
        else:
            self.upload_schema = "upload"
            self.transient_schema = "transient"
            self.registry_schema = "registry"

    def _sanitize_schema_name(self, name: str) -> str:
        return _sanitize_schema_name(name)

    @property
    def placeholder(self) -> str:
        return lambda i: "?"

    async def insert_rows(self, table_name: str, rows: List[List[Any]], columns: List[str]) -> None:
        if self.pool is None:
            raise RuntimeError("Backend pool not set.")
        from memframe.db_manager.pool import ClickHousePool
        if isinstance(self.pool, ClickHousePool):
            clean = table_name.replace("`", "").replace('"', "")
            if "." in clean:
                database, table = clean.split(".", 1)
            else:
                database = self.conn_params.get("database")
                if not database:
                    raise ValueError("ClickHouse inserts require a database-qualified table")
                table = clean
            await self.pool.insert(table, rows, database=database, column_names=columns)

    async def insert_arrow_table(self, table_name: str, arrow_table: Any) -> None:
        if self.pool is None:
            raise RuntimeError("Backend pool not set.")
        from memframe.db_manager.pool import ClickHousePool
        if isinstance(self.pool, ClickHousePool):
            clean = table_name.replace("`", "").replace('"', "")
            if "." in clean:
                database, table = clean.split(".", 1)
            else:
                database = self.conn_params.get("database")
                if not database:
                    raise ValueError("ClickHouse inserts require a database-qualified table")
                table = clean
            await self.pool.insert_arrow(table, arrow_table, database=database)

    def _clickhouse_qualified_table_name(self, table_name: str, default_database: Optional[str] = None) -> str:
        database, table = self._split_qualified_table_name(table_name)
        database = database or default_database or self.upload_schema
        return f"`{database}`.`{table}`"

    def _split_qualified_table_name(self, table_name: str) -> Tuple[Optional[str], str]:
        parts = table_name.split(".", 1)
        if len(parts) == 2:
            schema, tbl = parts
            return self._strip_identifier_quotes(schema), self._strip_identifier_quotes(tbl)
        return None, self._strip_identifier_quotes(table_name)

    def _strip_identifier_quotes(self, identifier: str) -> str:
        return identifier.strip('"`')

    async def _setup_database(self) -> None:
        for schema in (self.upload_schema, self.transient_schema, self.registry_schema):
            await self.execute(f"CREATE DATABASE IF NOT EXISTS `{schema}`")
        await self._create_registry_tables()

    async def _create_transient_schema(self) -> None:
        pass

    async def _create_registry_tables(self) -> None:
        await self.execute(f"""
            CREATE TABLE IF NOT EXISTS {self.registry_schema}.csv_registry (
                data_id String,
                filename String,
                uploaded_at DateTime DEFAULT now(),
                is_upload_success UInt8 DEFAULT 1,
                table_name String,
                row_count Int64
            ) ENGINE = MergeTree()
            ORDER BY data_id
        """)
        await self.execute(f"""
            CREATE TABLE IF NOT EXISTS {self.registry_schema}.transient_registry (
                data_id String,
                opidx Int32,
                generated_table_name Nullable(String),
                operation_type Nullable(String),
                class_name Nullable(String),
                method_name Nullable(String),
                args Nullable(String),
                kwargs Nullable(String),
                created_at DateTime DEFAULT now(),
                is_deep_cache Bool,
                schema Nullable(String)
            ) ENGINE = MergeTree()
            ORDER BY (data_id, opidx)
        """)
        await self._migrate_transient_registry_schema()

    def get_upload_table_name(self, data_id: str) -> str:
        return self._clickhouse_qualified_table_name(data_id)

    def get_transient_table_name(self, data_id: str, opidx: int) -> str:
        return f"`{self.transient_schema}`.`{data_id}_{opidx}`"

    @property
    def transient_registry_table(self) -> str:
        return f"`{self.registry_schema}`.transient_registry"

    @property
    def csv_registry_table(self) -> str:
        return f"`{self.registry_schema}`.csv_registry"

    async def drop_table(self, table_name: str) -> None:
        qualified = self._clickhouse_qualified_table_name(table_name)
        await self.execute(f"DROP TABLE IF EXISTS {qualified}")

    async def table_exists(self, table_name: str) -> bool:
        schema, tbl = self._split_qualified_table_name(table_name)
        schema = schema or self.upload_schema
        query = "SELECT count() FROM system.tables WHERE database = ? AND name = ?"
        res = await self.fetch_row(query, schema, tbl)
        return res[0] > 0 if res else False
