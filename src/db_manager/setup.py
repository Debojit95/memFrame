import asyncio
import logging
from typing import Any, Dict, List, Optional, Tuple
import asyncpg
import duckdb
from src.core.ingestion.datatype_detector import DatatypeDetector, Backend

logger = logging.getLogger("memFrame")
logger.setLevel(logging.INFO)
if not logger.handlers:
    handler = logging.StreamHandler()
    handler.setFormatter(logging.Formatter("%(asctime)s - %(name)s - %(levelname)s - %(message)s"))
    logger.addHandler(handler)


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

    async def _connect_postgres(self) -> Any:
        try:
            conn = await asyncpg.connect(**self.conn_params)
        except asyncpg.InvalidCatalogNameError:
            target_db = self.conn_params["database"]
            admin_params = self.conn_params.copy()
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

            conn = await asyncpg.connect(**self.conn_params)

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
                # Cross-loop asyncpg closes can deadlock when a sync wrapper is
                # blocking the original event loop. Terminate and reconnect on
                # the current loop instead.
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
                db_path = self.conn_params.get("db_path", "totem_new.duckdb")
                loop = asyncio.get_running_loop()
                self._conn = await loop.run_in_executor(None, duckdb.connect, db_path)
                logger.info(f"Connected to DuckDB: {db_path}")

            elif self.backend == Backend.POSTGRES:
                if self._conn is not None and not self._conn.is_closed():
                    await self._close_postgres_connection()
                self._conn = await self._connect_postgres()
                logger.info(
                    f"Connected to PostgreSQL: {self.conn_params['host']}:{self.conn_params.get('port', 5432)}/{self.conn_params['database']}"
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
                    await asyncio.get_running_loop().run_in_executor(None, self._conn.close)
            elif self.backend == Backend.POSTGRES:
                if self._conn:
                    await self._close_postgres_connection()
            logger.info("Database connection closed")
        except Exception as e:
            logger.error(f"Error during close: {e}")

    async def execute(self, query: str, *args) -> None:
        try:
            if self.backend == Backend.DUCKDB:
                loop = asyncio.get_running_loop()
                await loop.run_in_executor(None, lambda: self._conn.execute(query, args))
            elif self.backend == Backend.POSTGRES:
                await self._ensure_postgres_connection()
                await self._conn.execute(query, *args)
            logger.debug(f"Executed: {query[:100]}...")
        except Exception as e:
            logger.error(f"Query failed: {query[:200]}\nError: {e}")
            raise

    async def fetch(self, query: str, *args) -> List[Tuple]:
        try:
            if self.backend == Backend.DUCKDB:
                loop = asyncio.get_running_loop()
                result = await loop.run_in_executor(
                    None, lambda: self._conn.execute(query, args).fetchall()
                )
                return result
            elif self.backend == Backend.POSTGRES:
                await self._ensure_postgres_connection()
                rows = await self._conn.fetch(query, *args)
                return [tuple(r) for r in rows]
        except Exception as e:
            logger.error(f"Fetch failed: {e}")
            raise

    async def fetch_one(self, query: str, *args) -> Optional[Tuple]:
        try:
            if self.backend == Backend.DUCKDB:
                loop = asyncio.get_running_loop()
                result = await loop.run_in_executor(
                    None, lambda: self._conn.execute(query, args).fetchone()
                )
                return result
            elif self.backend == Backend.POSTGRES:
                await self._ensure_postgres_connection()
                row = await self._conn.fetchrow(query, *args)
                return tuple(row) if row else None
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

    def get_upload_table_name(self, data_id: str) -> str:
        return data_id

    def get_transient_table_name(self, data_id: str, opidx: int) -> str:
        return f'transient."{data_id}_{opidx}"'

    # ---------- Schema helpers (unchanged) ----------
    async def create_schema_if_not_exists(self, schema_name: str) -> None:
        if self.backend in (Backend.DUCKDB, Backend.POSTGRES):
            await self.execute(f"CREATE SCHEMA IF NOT EXISTS {schema_name}")

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
        raise ValueError(f"Unsupported backend: {self.backend}")

    async def drop_table(self, table_name: str) -> None:
        await self.execute(f"DROP TABLE IF EXISTS {table_name} CASCADE")

    @property
    def transient_registry_table(self) -> str:
        return "registry.transient_registry"

    @property
    def csv_registry_table(self) -> str:
        return "registry.csv_registry"

    async def init_database(self) -> None:
        if self.backend in (Backend.DUCKDB, Backend.POSTGRES):
            await self.create_schema_if_not_exists("upload")
            await self.create_schema_if_not_exists("transient")
            await self.create_schema_if_not_exists("registry")
        await self.create_registry_tables()
        logger.info("Database initialized")

    async def create_registry_tables(self) -> None:
        await self.execute("""
            CREATE TABLE IF NOT EXISTS registry.csv_registry (
                data_id CHAR(6) PRIMARY KEY,
                filename TEXT NOT NULL,
                uploaded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                is_upload_success BOOLEAN DEFAULT TRUE,
                table_name TEXT NOT NULL,
                row_count BIGINT
            )
        """)
        await self.execute("""
            CREATE TABLE IF NOT EXISTS registry.transient_registry (
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
        schema_name = "registry"
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
                await self.execute(
                    f"ALTER TABLE {fq_table_name} ADD COLUMN {column_name} {column_type}"
                )

        try:
            await self.execute(
                f"ALTER TABLE {fq_table_name} ALTER COLUMN generated_table_name DROP NOT NULL"
            )
        except Exception:
            # Ignore if column is already nullable or backend/version handles this differently.
            pass

    # ------------------------------------------------------------------
    #  Robust encoding detection + fallback
    # ------------------------------------------------------------------
    async def _resolve_encoding(self, file_path: str) -> str:
        """Return an encoding that can definitely read the whole file."""
        loop = asyncio.get_running_loop()

        # Try detected encoding with 64 KB check
        detected = await loop.run_in_executor(None, self._type_detector._detect_encoding, file_path)

        def _validate(enc):
            try:
                with open(file_path, "rb") as f:
                    raw = f.read(65536)
                raw.decode(enc)
                return True
            except (UnicodeDecodeError, LookupError):
                return False

        for enc in (detected, "utf-8", "latin-1", "cp1252"):
            if await loop.run_in_executor(None, _validate, enc):
                return enc

        return "latin-1"  








