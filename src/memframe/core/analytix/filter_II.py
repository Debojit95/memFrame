from datetime import datetime
from typing import Any, Dict, List, Optional, Union, TYPE_CHECKING
import traceback
import pandas as pd

from memframe.core.analytix.filter_I import Predicate, SQLContext
from memframe.db_manager.adapters.base import DatabaseAdapter
from memframe.db_manager.adapters.duckdb import DuckDBAdapter
from memframe.db_manager.adapters.postgresql import PostgresAdapter
from memframe.db_manager.adapters.clickhouse import ClickHouseAdapter
from memframe.utils.helper import SQLIdentifierSanitizer

if TYPE_CHECKING:
    from memframe.core.ingestion.datatype_detector import Backend


class DataFilteringOps:
    """
    Core filtering operations – translates a Predicate tree into a SQL WHERE
    clause and creates a new transient table with the filtered result.
    """

    def __init__(self, db_adapter: DatabaseAdapter, backend: "Backend"):
        self.db = db_adapter
        self.backend = backend

    # ------------------------------------------------------------------
    #  Adapter wrappers
    # ------------------------------------------------------------------
    async def _exec(self, sql: str, *args):
        return await self.db.execute(sql, *args)

    async def _fetch(self, sql: str, *args):
        return await self.db.fetch(sql, *args)

    async def _fetch_sample(self, table: str, schema: str,
                            columns: Union[str, List[str]] = "*") -> pd.DataFrame:
        qualified = self._qualified_table(table, schema)
        if columns == "*":
            col_clause = "*"
        else:
            sanitized = [SQLIdentifierSanitizer.sanitize(c) for c in columns]
            col_clause = ", ".join(self.db.quote_identifier(c) for c in sanitized)
        rows = await self._fetch(f"SELECT {col_clause} FROM {qualified}")
        return pd.DataFrame([dict(r) for r in rows])

    async def _fetch_in_chunks(self, table: str, schema: str, chunk_size: int,
                               columns: Union[str, List[str]] = "*"):
        qualified = self._qualified_table(table, schema)
        offset = 0
        while True:
            if columns == "*":
                col_clause = "*"
            else:
                sanitized = [SQLIdentifierSanitizer.sanitize(c) for c in columns]
                col_clause = ", ".join(self.db.quote_identifier(c) for c in sanitized)
            query = f"""
                SELECT {col_clause}
                FROM {qualified}
                LIMIT {chunk_size} OFFSET {offset}
            """
            rows = await self._fetch(query)
            if not rows:
                break
            yield pd.DataFrame([dict(r) for r in rows])
            offset += chunk_size

    def _qualified_table(self, table: str, schema: str) -> str:
        t = SQLIdentifierSanitizer.sanitize(table)
        s = SQLIdentifierSanitizer.sanitize(schema)
        return f'{self.db.quote_identifier(s)}.{self.db.quote_identifier(t)}'

    async def _column_lookup(self, table: str, schema: str) -> Dict[str, str]:
        try:
            col_types = await self.db.get_column_types(
                SQLIdentifierSanitizer.sanitize(table),
                SQLIdentifierSanitizer.sanitize(schema),
            )
            return {
                SQLIdentifierSanitizer.sanitize(col).lower(): SQLIdentifierSanitizer.sanitize(col)
                for col in col_types.keys()
            }
        except Exception:
            return {}

    def _resolve_column_name(self, column: str, lookup: Dict[str, str]) -> str:
        safe = SQLIdentifierSanitizer.sanitize(column)
        return lookup.get(safe.lower(), safe)

    def _success_response(self, message, sample_df, **extra):
        return {
            "is_error": False,
            "message": message,
            "error_message": None,
            "result": sample_df,
            **extra,
        }

    def _error_response(self, msg: str):
        return {
            "is_error": True,
            "message": "",
            "error_message": msg,
        }

    def _unsupported_backend_error(self) -> NotImplementedError:
        return NotImplementedError(
            f"Unsupported database backend for filtering operation: {self.db.__class__.__name__}"
        )

    async def _generate_transient_table_name(self, base_table: str,
                                             backend, data_id: str) -> str:
        max_op = await backend.fetchval(
            f"""
            SELECT COALESCE(MAX(opidx), 0)
            FROM {backend.transient_registry_table}
            WHERE data_id = {backend.placeholder(1)}
            """,
            data_id,
        )
        next_op = max_op + 1
        safe_base = SQLIdentifierSanitizer.sanitize(base_table)
        return f"{safe_base}__op_{next_op}"

    def _inline_params_for_ddl(self, sql: str, params: list) -> str:
        """
        Replace ? placeholders with safely quoted inline values.
        Use ONLY when the adapter cannot parameterize DDL statements.
        """
        for p in params:
            if p is None:
                replacement = "NULL"
            elif isinstance(p, bool):
                replacement = "1" if p else "0"
            elif isinstance(p, (int, float)):
                replacement = str(p)
            elif isinstance(p, datetime):
                replacement = f"'{p.strftime('%Y-%m-%d %H:%M:%S')}'"
            elif isinstance(p, str):
                escaped = p.replace("\\", "\\\\").replace("'", "\\'")
                replacement = f"'{escaped}'"
            else:
                escaped = str(p).replace("\\", "\\\\").replace("'", "\\'")
                replacement = f"'{escaped}'"
            sql = sql.replace("?", replacement, 1)
        return sql

    # ------------------------------------------------------------------
    #  MAIN FILTER METHOD
    # ------------------------------------------------------------------
    async def filter_table(
        self,
        table: str,
        schema: str,
        predicate: Predicate,
        columns: Union[str, List[str]] = "*",
        new_table: Optional[str] = None,
        backend=None,
        data_id: str = None,
        chunk_size: Optional[int] = None,
    ) -> Dict[str, Any]:
        """
        Create a new transient table containing only rows that satisfy
        the given predicate. Returns a sample or an async iterator.
        """
        try:
            supported = (PostgresAdapter, DuckDBAdapter, ClickHouseAdapter)
            if isinstance(self.db, supported):
                # Build WHERE clause using an adapter-aware SQL context
                column_lookup = await self._column_lookup(table, schema)
                ctx = BackendAwareSQLContext(self.db, self.backend, column_lookup)
                where_clause = predicate.compile(ctx)

                # Decide output columns
                if columns == "*":
                    select_clause = "*"
                    output_cols = "*"
                else:
                    cols = list(dict.fromkeys(columns))
                    sanitized = [
                        self._resolve_column_name(c, column_lookup)
                        for c in cols
                    ]
                    select_clause = ", ".join(
                        self.db.quote_identifier(c) for c in sanitized
                    )
                    output_cols = sanitized

                qualified = self._qualified_table(table, schema)

                # Generate transient table name if not supplied
                if new_table is None:
                    if backend is None or data_id is None:
                        return self._error_response(
                            "backend and data_id are required to generate transient table"
                        )
                    new_table = await self._generate_transient_table_name(
                        table, backend, data_id
                    )
                new_table_safe = SQLIdentifierSanitizer.sanitize(new_table)

                # ──────────────────────────────────────────────────
                # Backend-specific CREATE TABLE syntax
                # ──────────────────────────────────────────────────
                qualified_new = (
                    f"{self.db.quote_identifier(schema)}"
                    f".{self.db.quote_identifier(new_table_safe)}"
                )

                if isinstance(self.db, ClickHouseAdapter):
                    # ClickHouse requires ENGINE + ORDER BY for MergeTree
                    create_sql = f"""
                        CREATE TABLE {qualified_new}
                        ENGINE = MergeTree()
                        ORDER BY tuple()
                        AS SELECT {select_clause}
                        FROM {qualified}
                        WHERE {where_clause}
                    """
                else:
                    # PostgreSQL / DuckDB – no ENGINE clause needed
                    create_sql = f"""
                        CREATE TABLE {qualified_new} AS
                        SELECT {select_clause}
                        FROM {qualified}
                        WHERE {where_clause}
                    """

                await self._exec(create_sql, *ctx.params)

                # ──────────────────────────────────────────────────
                # Return sample or streaming iterator
                # ──────────────────────────────────────────────────
                if chunk_size is None:
                    sample = await self._fetch_sample(
                        new_table_safe, schema, columns=output_cols
                    )
                    return self._success_response(
                        "Filter applied successfully",
                        sample,
                        new_table=new_table_safe,
                        where_clause=where_clause,
                        params=ctx.params,
                    )
                else:
                    async def iterator():
                        async for chunk in self._fetch_in_chunks(
                            new_table_safe, schema, chunk_size,
                            columns=output_cols,
                        ):
                            yield chunk

                    return {
                        "is_error": False,
                        "message": "Filter applied (streaming)",
                        "error_message": None,
                        "iterator": iterator(),
                        "chunk_size": chunk_size,
                        "new_table": new_table_safe,
                    }
            else:
                raise self._unsupported_backend_error()

        except Exception as e:
            return self._error_response(
                f"filter_table error: {str(e)}\n{traceback.format_exc()}"
            )

# ============================================================================
#  Backend‑aware SQL context 
# ============================================================================
class BackendAwareSQLContext(SQLContext):
    """
    Extends the original SQLContext to use the adapter's placeholder style
    and to handle backend-specific syntax (regex, timezone, etc.).
    """

    def __init__(
        self,
        adapter: DatabaseAdapter,
        backend: "Backend",
        column_lookup: Optional[Dict[str, str]] = None,
    ):
        super().__init__()
        self.adapter = adapter
        self.backend = backend
        self.column_lookup = column_lookup or {}

    def param(self, value: Any) -> str:
        """Use the adapter's placeholder – works for $1 and ?."""
        self.params.append(value)
        return self.adapter.placeholder(len(self.params))

    def col(self, name: str) -> str:
        safe = SQLIdentifierSanitizer.sanitize(name)
        resolved = self.column_lookup.get(safe.lower(), safe)
        return self.adapter.quote_identifier(resolved)

    @property
    def is_duckdb(self) -> bool:
        from memframe.core.ingestion.datatype_detector import Backend
        return self.backend == Backend.DUCKDB

    @property
    def is_postgres(self) -> bool:
        from memframe.core.ingestion.datatype_detector import Backend
        return self.backend == Backend.POSTGRES

    @property
    def is_clickhouse(self) -> bool:
        from memframe.core.ingestion.datatype_detector import Backend
        return self.backend == Backend.CLICKHOUSE
    
    
    
    
    