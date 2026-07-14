import asyncio
import logging
import re
from typing import Any, Dict, List, Optional, Tuple
import asyncpg
import duckdb
from src.core.ingestion.datatype_detector import DatatypeDetector, Backend
from src.db_manager.adapters.clickhouse import HttpxClickHouseClient

logger = logging.getLogger("memFrame")
logger.setLevel(logging.INFO)
if not logger.handlers:
    handler = logging.StreamHandler()
    handler.setFormatter(logging.Formatter("%(asctime)s - %(name)s - %(levelname)s - %(message)s"))
    logger.addHandler(handler)


def _sanitize_schema_name(value: str) -> str:
    name = re.sub(r"[^A-Za-z0-9_]", "_", value.strip())
    name = re.sub(r"_+", "_", name).strip("_")
    if not name:
        raise ValueError("schema_prefix must contain at least one alphanumeric character")
    if name[0].isdigit():
        name = f"mf_{name}"
    return name.lower()


def _loop_is_running(loop: Optional[asyncio.AbstractEventLoop]) -> bool:
    return bool(
        loop
        and loop.is_running()
        and not loop.is_closed()
    )


def _terminate_postgres_connection(conn: Any) -> None:
    try:
        conn.terminate()
    except RuntimeError as exc:
        if "Event loop is closed" not in str(exc):
            raise
        logger.debug("Ignoring asyncpg terminate on closed event loop")


class DatabaseBackend:
    def __init__(self, backend: Backend, conn_params: Dict[str, Any]):
        self.backend = backend
        self.conn_params = conn_params
        self._conn: Optional[Any] = None
        self._conn_loop: Optional[asyncio.AbstractEventLoop] = None
        self._type_detector = DatatypeDetector()
        schema_prefix = self.conn_params.get("schema_prefix")
        if schema_prefix:
            prefix = _sanitize_schema_name(str(schema_prefix))
            self.upload_schema = f"{prefix}_upload"
            self.transient_schema = f"{prefix}_transient"
            self.registry_schema = f"{prefix}_registry"
        else:
            self.upload_schema = "upload"
            self.transient_schema = "transient"
            self.registry_schema = "registry"

    async def _connect_postgres(self) -> Any:
        connect_params = {
            key: self.conn_params[key]
            for key in ("host", "port", "user", "password", "database")
            if key in self.conn_params
        }
        try:
            conn = await asyncpg.connect(**connect_params)
        except asyncpg.InvalidCatalogNameError:
            target_db = connect_params["database"]
            admin_params = connect_params.copy()
            admin_params["database"] = "postgres"

            temp_conn = await asyncpg.connect(**admin_params)
            try:
                exists = await temp_conn.fetchval(
                    "SELECT 1 FROM pg_database WHERE datname = $1", target_db
                )
                if not exists:
                    await temp_conn.execute(f'CREATE DATABASE "{target_db}"')
                    logger.info(f"Created database: {target_db}")
            finally:
                await temp_conn.close()

            conn = await asyncpg.connect(**connect_params)

        self._conn_loop = asyncio.get_running_loop()
        return conn

    async def _close_postgres_connection(self) -> None:
        if not self._conn:
            return

        conn = self._conn
        conn_loop = self._conn_loop
        current_loop = asyncio.get_running_loop()
        try:
            if conn_loop is current_loop:
                await conn.close()
            else:
                _terminate_postgres_connection(conn)
        finally:
            self._conn = None
            self._conn_loop = None

    async def _ensure_postgres_connection(self) -> None:
        if self.backend != Backend.POSTGRES:
            return

        current_loop = asyncio.get_running_loop()
        if (
            self._conn is None
            or self._conn_loop is not current_loop
            or self._conn.is_closed()
        ):
            if self._conn is not None and not self._conn.is_closed():
                await self._close_postgres_connection()
            self._conn = await self._connect_postgres()

    # ---------- Connection handling (unchanged) ----------
    async def connect(self) -> None:
        try:
            if self.backend == Backend.DUCKDB:
                db_path = self.conn_params.get("db_path", "memframe_new.duckdb")
                self._conn = duckdb.connect(db_path)
                logger.info(f"Connected to DuckDB: {db_path}")

            elif self.backend == Backend.POSTGRES:
                if self._conn is not None and not self._conn.is_closed():
                    await self._close_postgres_connection()
                self._conn = await self._connect_postgres()
                logger.info(
                    f"Connected to PostgreSQL: {self.conn_params['host']}:{self.conn_params.get('port', 5432)}/{self.conn_params['database']}"
                )
            elif self.backend == Backend.CLICKHOUSE:          
                self._conn = HttpxClickHouseClient(
                    host=self.conn_params["host"],
                    port=self.conn_params.get("port", 8123),
                    username=self.conn_params["user"],
                    password=self.conn_params["password"],
                    database=self.conn_params.get("database"),
                    secure=self.conn_params.get("secure", False),
                    timeout=self.conn_params.get("timeout", 10.0),
                )
                logger.info(
                    f"Connected to ClickHouse: {self.conn_params['host']}:"
                    f"{self.conn_params.get('port', 8123)}"
                )
            else:
                raise ValueError(f"Unsupported backend: {self.backend}")
        except Exception as e:
            logger.error(f"Connection failed: {e}")
            raise

    async def close(self) -> None:
        try:
            if self.backend == Backend.DUCKDB:
                if self._conn:
                    self._conn.close()
                    self._conn = None
            elif self.backend == Backend.POSTGRES:
                if self._conn:
                    await self._close_postgres_connection()
            elif self.backend == Backend.CLICKHOUSE:          # ← ADD
                if self._conn:
                    await self._conn.close()
            else:
                raise ValueError(f"Unsupported backend: {self.backend}")
                
            logger.info("Database connection closed")
        except Exception as e:
            logger.error(f"Error during close: {e}")

    async def execute(self, query: str, *args) -> None:
        try:
            if self.backend == Backend.DUCKDB:
                self._conn.execute(query, args)
            elif self.backend == Backend.POSTGRES:
                await self._ensure_postgres_connection()
                await self._conn.execute(query, *args)
            elif self.backend == Backend.CLICKHOUSE:          # ← ADD
                await self._conn.command(query, parameters=args if args else None)
            else:
                raise ValueError(f"Unsupported backend: {self.backend}")
            logger.debug(f"Executed: {query[:100]}...")
        except Exception as e:
            logger.error(f"Query failed: {query[:200]}\nError: {e}")
            raise

    async def fetch(self, query: str, *args) -> List[Tuple]:
        try:
            if self.backend == Backend.DUCKDB:
                return self._conn.execute(query, args).fetchall()
            elif self.backend == Backend.POSTGRES:
                await self._ensure_postgres_connection()
                rows = await self._conn.fetch(query, *args)
                return [tuple(r) for r in rows]
            elif self.backend == Backend.CLICKHOUSE:          # ← ADD
                result = await self._conn.query(
                    query, parameters=args if args else None
                )
                return result.result_rows
            else:
                raise ValueError(f"Unsupported backend: {self.backend}")
        except Exception as e:
            logger.error(f"Fetch failed: {e}")
            raise

    async def fetch_one(self, query: str, *args) -> Optional[Tuple]:
        try:
            if self.backend == Backend.DUCKDB:
                return self._conn.execute(query, args).fetchone()
            elif self.backend == Backend.POSTGRES:
                await self._ensure_postgres_connection()
                row = await self._conn.fetchrow(query, *args)
                return tuple(row) if row else None
            elif self.backend == Backend.CLICKHOUSE:          
                result = await self._conn.query(
                    query, parameters=args if args else None
                )
                return result.first_row
            else:
                raise ValueError(f"Unsupported backend: {self.backend}")
        except Exception as e:
            logger.error(f"Fetch one failed: {e}")
            raise

    async def fetch_val(self, query: str, *args) -> Any:
        row = await self.fetch_one(query, *args)
        return row[0] if row else None

    def placeholder(self, index: int) -> str:
        return f"${index}" if self.backend == Backend.POSTGRES else "?"

    def _strip_identifier_quotes(self, identifier: str) -> str:
        return identifier.strip('"`')

    def _split_qualified_table_name(self, table_name: str) -> Tuple[Optional[str], str]:
        parts = table_name.split(".", 1)
        if len(parts) == 2:
            schema, tbl = parts
            return self._strip_identifier_quotes(schema), self._strip_identifier_quotes(tbl)
        return None, self._strip_identifier_quotes(table_name)

    def _clickhouse_qualified_table_name(
        self,
        table_name: str,
        default_database: Optional[str] = None,
    ) -> str:
        database, table = self._split_qualified_table_name(table_name)
        database = database or default_database or self.upload_schema
        return f"`{database}`.`{table}`"

    def get_upload_table_name(self, data_id: str) -> str:
        return data_id


    # ---------- Schema helpers (unchanged) ----------
    async def create_schema_if_not_exists(self, schema_name: str) -> None:
        if self.backend in (Backend.DUCKDB, Backend.POSTGRES):
            await self.execute(f"CREATE SCHEMA IF NOT EXISTS {schema_name}")
        elif self.backend == Backend.CLICKHOUSE:
            await self.execute(f"CREATE DATABASE IF NOT EXISTS `{schema_name}`")

    async def table_exists(self, table_name: str) -> bool:
        schema, tbl = self._split_qualified_table_name(table_name)
        if self.backend == Backend.DUCKDB:
            if schema:
                query = "SELECT COUNT(*) FROM information_schema.tables WHERE table_schema = ? AND table_name = ?"
                res = await self.fetch_one(query, schema, tbl)
            else:
                query = "SELECT COUNT(*) FROM information_schema.tables WHERE table_name = ?"
                res = await self.fetch_one(query, tbl)
            return res[0] > 0 if res else False
        elif self.backend == Backend.POSTGRES:
            if schema:
                query = "SELECT EXISTS (SELECT 1 FROM information_schema.tables WHERE table_schema = $1 AND table_name = $2)"
                res = await self.fetch_one(query, schema, tbl)
            else:
                query = "SELECT EXISTS (SELECT 1 FROM information_schema.tables WHERE table_name = $1)"
                res = await self.fetch_one(query, tbl)
            return res[0] if res else False
        elif self.backend == Backend.CLICKHOUSE:              
            schema = schema or self.upload_schema
            query = (
                "SELECT count() FROM system.tables "
                "WHERE database = ? AND name = ?"
            )
            res = await self.fetch_one(query, schema, tbl)
            return res[0] > 0 if res else False
        
        raise ValueError(f"Unsupported backend: {self.backend}")

    async def drop_table(self, table_name: str) -> None:
        if self.backend == Backend.CLICKHOUSE:                
            qualified = self._clickhouse_qualified_table_name(table_name)
            await self.execute(f"DROP TABLE IF EXISTS {qualified}")
        else:
            await self.execute(f"DROP TABLE IF EXISTS {table_name} CASCADE")

    def get_transient_table_name(self, data_id: str, opidx: int) -> str:
        if self.backend == Backend.CLICKHOUSE:                
            return f"`{self.transient_schema}`.`{data_id}_{opidx}`"
        return f'{self.transient_schema}."{data_id}_{opidx}"'

    @property
    def transient_registry_table(self) -> str:
        return f"{self.registry_schema}.transient_registry"

    @property
    def csv_registry_table(self) -> str:
        return f"{self.registry_schema}.csv_registry"

    async def init_database(self) -> None:
        if self.backend in (Backend.DUCKDB, Backend.POSTGRES,Backend.CLICKHOUSE):
            await self.create_schema_if_not_exists(self.upload_schema)
            await self.create_schema_if_not_exists(self.transient_schema)
            await self.create_schema_if_not_exists(self.registry_schema)
        await self.create_registry_tables()
        logger.info("Database initialized")

    async def create_registry_tables(self) -> None:
        if self.backend == Backend.CLICKHOUSE:                
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
                    created_at DateTime DEFAULT now()
                ) ENGINE = MergeTree()
                ORDER BY (data_id, opidx)
            """)
        else:
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

    async def _column_exists(self, schema: str, table: str, column: str) -> bool:
        
        if self.backend == Backend.CLICKHOUSE:                
            query = """
                SELECT count()
                FROM system.columns
                WHERE database = ?
                  AND table = ?
                  AND name = ?
            """
        else:
            query = f"""
                SELECT COUNT(*)
                FROM information_schema.columns
                WHERE table_schema = {self.placeholder(1)}
                AND table_name = {self.placeholder(2)}
                AND column_name = {self.placeholder(3)}
            """
        count = await self.fetch_val(query, schema, table, column)
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
        }

        for column_name, column_type in required_columns.items():
            if not await self._column_exists(schema_name, table_name, column_name):
                ch_type = (
                    "Nullable(String)"
                    if self.backend == Backend.CLICKHOUSE
                    else column_type
                )
                await self.execute(
                    f"ALTER TABLE {fq_table_name} ADD COLUMN {column_name} {ch_type}"
                )

        if self.backend != Backend.CLICKHOUSE:            
            try:
                await self.execute(
                    f"ALTER TABLE {fq_table_name} "
                    f"ALTER COLUMN generated_table_name DROP NOT NULL"
                )
            except Exception:
                pass

    async def insert_rows(
        self, table_name: str, rows: List[List[Any]], columns: List[str]
    ) -> None:
        """ClickHouse-only bulk insert via the local HTTP client."""
        if self.backend != Backend.CLICKHOUSE:
            raise NotImplementedError("insert_rows is ClickHouse-only")

        # The HTTP client expects bare table name + database kwarg.
        clean = table_name.replace("`", "").replace('"', "")
        if "." in clean:
            database, table = clean.split(".", 1)
        else:
            database = self.conn_params.get("database")
            if not database:
                raise ValueError("ClickHouse inserts require a database-qualified table")
            table = clean

        await self._conn.insert(table, rows, database=database, column_names=columns)
    
    
    async def insert_arrow_table(self, table_name: str, arrow_table: Any) -> None:
        """ClickHouse-only ArrowStream insert, avoiding Python row conversion."""
        if self.backend != Backend.CLICKHOUSE:
            raise NotImplementedError("insert_arrow_table is ClickHouse-only")

        clean = table_name.replace("`", "").replace('"', "")
        if "." in clean:
            database, table = clean.split(".", 1)
        else:
            database = self.conn_params.get("database")
            if not database:
                raise ValueError("ClickHouse inserts require a database-qualified table")
            table = clean

        await self._conn.insert_arrow(table, arrow_table, database=database)
    
    
    # ------------------------------------------------------------------
    #  Robust encoding detection + fallback
    # ------------------------------------------------------------------
    async def _resolve_encoding(self, file_path: str) -> str:
        """Return an encoding that can definitely read the whole file."""
        # Try detected encoding with 64 KB check
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
