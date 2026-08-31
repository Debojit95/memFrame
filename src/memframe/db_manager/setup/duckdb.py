import logging
from typing import Dict, List, Optional, Tuple

from memframe.db_manager.setup.base import DatabaseBackend

logger = logging.getLogger(__name__)


class DuckDBBackend(DatabaseBackend):
    backend = "duckdb"

    def __init__(self, conn_params: dict):
        super().__init__(conn_params)
        self.upload_schema = "memframe_upload"
        self.transient_schema = "memframe_transient"
        self.registry_schema = "memframe_csv_registry"

    async def _setup_database(self) -> None:
        for schema in (self.upload_schema, self.transient_schema, self.registry_schema):
            await self.execute(f"CREATE SCHEMA IF NOT EXISTS {schema}")
        await self._create_registry_tables()

    async def _create_transient_schema(self) -> None:
        pass

    async def _create_registry_tables(self) -> None:
        await self.execute(f"""
            CREATE TABLE IF NOT EXISTS {self.registry_schema}.memframe_csv_registry (
                data_id CHAR(6) PRIMARY KEY,
                filename TEXT NOT NULL,
                uploaded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                is_upload_success BOOLEAN DEFAULT TRUE,
                table_name TEXT NOT NULL,
                row_count BIGINT,
                schema TEXT
            )
        """)
        await self.execute(f"""
            CREATE TABLE IF NOT EXISTS {self.registry_schema}.memframe_transient_registry (
                data_id CHAR(6) NOT NULL,
                opidx INTEGER NOT NULL,
                generated_table_name TEXT,
                operation_type TEXT,
                class_name TEXT,
                method_name TEXT,
                args TEXT,
                kwargs TEXT,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                PRIMARY KEY (data_id, opidx)
            )
        """)
        await self._migrate_transient_registry_schema()
        await self._migrate_csv_registry_schema()
        await self.execute(
            f"CREATE INDEX IF NOT EXISTS idx_transient_registry_lookup "
            f"ON {self.registry_schema}.memframe_transient_registry (data_id, class_name, method_name)"
        )

    def get_upload_table_name(self, data_id: str) -> str:
        return data_id

    def get_transient_table_name(self, data_id: str, opidx: int) -> str:
        return f'{self.transient_schema}."{data_id}_{opidx}"'

    @property
    def transient_registry_table(self) -> str:
        return f"{self.registry_schema}.memframe_transient_registry"

    @property
    def csv_registry_table(self) -> str:
        return f"{self.registry_schema}.memframe_csv_registry"

    def placeholder(self, i: int) -> str:
        return "?"

    async def list_user_tables(self) -> Dict[str, List[str]]:
        rows = await self.fetch(
            "SELECT table_schema, table_name FROM information_schema.tables "
            "WHERE table_type = 'BASE TABLE' ORDER BY table_schema, table_name"
        )
        excluded = {"information_schema", "pg_catalog", "memframe_upload", "memframe_transient", "memframe_csv_registry"}
        result: Dict[str, List[str]] = {}
        for schema, table in rows:
            if schema in excluded or schema.startswith("duckdb_"):
                continue
            result.setdefault(schema, []).append(table)
        return result

    async def table_exists(self, table_name: str) -> bool:
        schema, tbl = self._split_qualified_table_name(table_name)
        if schema:
            query = "SELECT COUNT(*) FROM information_schema.tables WHERE table_schema = ? AND table_name = ?"
            res = await self.fetch_row(query, schema, tbl)
        else:
            query = "SELECT COUNT(*) FROM information_schema.tables WHERE table_name = ?"
            res = await self.fetch_row(query, tbl)
        return res[0] > 0 if res else False

    async def drop_table(self, table_name: str) -> None:
        schema, tbl = self._split_qualified_table_name(table_name)
        qualified = f"{schema or self.upload_schema}.{self._quote_ident(tbl)}"
        await self.execute(f"DROP TABLE IF EXISTS {qualified} CASCADE")

    def _quote_ident(self, name: str) -> str:
        return '"' + name.replace('"', '""') + '"'

    def _split_qualified_table_name(self, table_name: str) -> Tuple[Optional[str], str]:
        parts = table_name.split(".", 1)
        if len(parts) == 2:
            schema, tbl = parts
            return self._strip_identifier_quotes(schema), self._strip_identifier_quotes(tbl)
        return None, self._strip_identifier_quotes(table_name)

    def _strip_identifier_quotes(self, identifier: str) -> str:
        return identifier.strip('"`')
