import logging
from typing import Any, Dict, List, Optional, Tuple

from memframe.db_manager.setup.base import DatabaseBackend
from memframe.exceptions import ConfigurationError, ConnectionNotReady

logger = logging.getLogger(__name__)


class ClickHouseBackend(DatabaseBackend):
    backend = "clickhouse"

    def __init__(self, conn_params: dict):
        super().__init__(conn_params)
        self.upload_schema = "memframe_upload"
        self.transient_schema = "memframe_transient"
        self.registry_schema = "memframe_csv_registry"

    @property
    def placeholder(self) -> str:
        return lambda i: "?"

    async def insert_rows(self, table_name: str, rows: List[List[Any]], columns: List[str]) -> None:
        if self.pool is None:
            raise ConnectionNotReady("Backend pool not set.")
        from memframe.db_manager.connection import ClickHousePool
        if isinstance(self.pool, ClickHousePool):
            clean = table_name.replace("`", "").replace('"', "")
            if "." in clean:
                database, table = clean.split(".", 1)
            else:
                database = self.conn_params.get("database")
                if not database:
                    raise ConfigurationError("ClickHouse inserts require a database-qualified table")
                table = clean
            await self.pool.insert(table, rows, database=database, column_names=columns)

    async def insert_arrow_table(self, table_name: str, arrow_table: Any) -> None:
        if self.pool is None:
            raise ConnectionNotReady("Backend pool not set.")
        from memframe.db_manager.connection import ClickHousePool
        if isinstance(self.pool, ClickHousePool):
            clean = table_name.replace("`", "").replace('"', "")
            if "." in clean:
                database, table = clean.split(".", 1)
            else:
                database = self.conn_params.get("database")
                if not database:
                    raise ConfigurationError("ClickHouse inserts require a database-qualified table")
                table = clean
            await self.pool.insert_arrow(table, arrow_table, database=database)

    def _clickhouse_qualified_table_name(self, table_name: str, default_database: Optional[str] = None) -> str:
        database, table = self._split_qualified_table_name(table_name)
        database = database or default_database or self.upload_schema
        return f"{self._quote_backtick(database)}.{self._quote_backtick(table)}"

    def _quote_backtick(self, name: str) -> str:
        # ponytail: backtick isn't a Python string literal, so build it via chr
        bt = chr(96)
        return bt + name.replace(bt, bt * 2) + bt

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
            CREATE TABLE IF NOT EXISTS {self.registry_schema}.memframe_csv_registry (
                data_id String,
                filename String,
                uploaded_at DateTime DEFAULT now(),
                is_upload_success UInt8 DEFAULT 1,
                table_name String,
                row_count Int64,
                schema Nullable(String)
            ) ENGINE = MergeTree()
            ORDER BY data_id
        """)
        await self.execute(f"""
            CREATE TABLE IF NOT EXISTS {self.registry_schema}.memframe_transient_registry (
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
        await self._migrate_csv_registry_schema()

    def get_upload_table_name(self, data_id: str) -> str:
        return self._clickhouse_qualified_table_name(data_id)

    def get_transient_table_name(self, data_id: str, opidx: int) -> str:
        return f"`{self.transient_schema}`.`{data_id}_{opidx}`"

    @property
    def transient_registry_table(self) -> str:
        return f"`{self.registry_schema}`.memframe_transient_registry"

    @property
    def csv_registry_table(self) -> str:
        return f"`{self.registry_schema}`.memframe_csv_registry"

    async def list_user_tables(self) -> Dict[str, List[str]]:
        rows = await self.fetch(
            "SELECT database, name FROM system.tables "
            "WHERE engine NOT LIKE 'System%' ORDER BY database, name"
        )
        # ponytail: exclude ClickHouse's preset/system databases (case-insensitive)
        # plus memFrame's own bookkeeping databases so only user tables sync.
        excluded = {"system", "information_schema", "default", "memframe_upload", "memframe_transient", "memframe_csv_registry"}
        result: Dict[str, List[str]] = {}
        for database, name in rows:
            if database.lower() in excluded:
                continue
            result.setdefault(database, []).append(name)
        return result

    async def drop_table(self, table_name: str) -> None:
        qualified = self._clickhouse_qualified_table_name(table_name)
        await self.execute(f"DROP TABLE IF EXISTS {qualified}")

    async def table_exists(self, table_name: str) -> bool:
        schema, tbl = self._split_qualified_table_name(table_name)
        schema = schema or self.upload_schema
        query = "SELECT count() FROM system.tables WHERE database = ? AND name = ?"
        res = await self.fetch_row(query, schema, tbl)
        return res[0] > 0 if res else False
