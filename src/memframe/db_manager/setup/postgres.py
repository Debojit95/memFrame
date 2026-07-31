import logging
from typing import Any, List, Optional, Tuple

from memframe.db_manager.setup.base import DatabaseBackend
from memframe.exceptions import ConfigurationError

logger = logging.getLogger(__name__)


def _sanitize_schema_name(value: str) -> str:
    import re
    name = re.sub(r"[^A-Za-z0-9_]", "_", value.strip())
    name = re.sub(r"_+", "_", name).strip("_")
    if not name:
        raise ConfigurationError("schema_prefix must contain at least one alphanumeric character")
    if name[0].isdigit():
        name = f"mf_{name}"
    return name.lower()


class PostgresBackend(DatabaseBackend):
    backend = "postgres"

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
        return lambda i: f"${i}"

    async def _setup_database(self) -> None:
        for schema in (self.upload_schema, self.transient_schema, self.registry_schema):
            await self.execute(f"CREATE SCHEMA IF NOT EXISTS {schema}")
        await self._create_registry_tables()

    async def _create_transient_schema(self) -> None:
        pass

    async def _create_registry_tables(self) -> None:
        await self.execute(f"""
            CREATE TABLE IF NOT EXISTS {self.registry_schema}.csv_registry (
                data_id CHAR(6) PRIMARY KEY,
                filename TEXT NOT NULL,
                uploaded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                is_upload_success BOOLEAN DEFAULT TRUE,
                table_name TEXT NOT NULL,
                row_count BIGINT
            )
        """)
        await self.execute(f"""
            CREATE TABLE IF NOT EXISTS {self.registry_schema}.transient_registry (
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

    def get_upload_table_name(self, data_id: str) -> str:
        return data_id

    def get_transient_table_name(self, data_id: str, opidx: int) -> str:
        return f'{self.transient_schema}."{data_id}_{opidx}"'

    @property
    def transient_registry_table(self) -> str:
        return f"{self.registry_schema}.transient_registry"

    @property
    def csv_registry_table(self) -> str:
        return f"{self.registry_schema}.csv_registry"

    async def table_exists(self, table_name: str) -> bool:
        schema, tbl = self._split_qualified_table_name(table_name)
        if schema:
            query = "SELECT EXISTS (SELECT 1 FROM information_schema.tables WHERE table_schema = $1 AND table_name = $2)"
            res = await self.fetch_row(query, schema, tbl)
        else:
            query = "SELECT EXISTS (SELECT 1 FROM information_schema.tables WHERE table_name = $1)"
            res = await self.fetch_row(query, tbl)
        return res[0] if res else False

    async def drop_table(self, table_name: str) -> None:
        await self.execute(f"DROP TABLE IF EXISTS {table_name} CASCADE")

    def _split_qualified_table_name(self, table_name: str) -> Tuple[Optional[str], str]:
        parts = table_name.split(".", 1)
        if len(parts) == 2:
            schema, tbl = parts
            return self._strip_identifier_quotes(schema), self._strip_identifier_quotes(tbl)
        return None, self._strip_identifier_quotes(table_name)

    def _strip_identifier_quotes(self, identifier: str) -> str:
        return identifier.strip('"`')
