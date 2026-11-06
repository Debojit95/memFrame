from typing import Any, Dict, List, Optional, Union, TYPE_CHECKING
import traceback
import pandas as pd

from memframe.core.analytix.filter.filter_I import Predicate, SQLContext
from memframe.core.indexing import (
    MF_IDX, assert_user_columns, mf_idx_sql_type, select_mf_idx_dtype,
    visible_columns,
)
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
    clause and creates a new cache table with the filtered result.

    Shared infrastructure lives here on DuckDB/PostgreSQL-flavoured
    defaults; clickhouse.py overrides one small dialect hook
    (_engine_clause) for the MergeTree table engine.
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

    async def _generate_cache_table_name(self, base_table: str,
                                             backend, data_id: str) -> str:
        max_op = await backend.fetchval(
            f"""
            SELECT COALESCE(MAX(opidx), 0)
            FROM {backend.cache_registry_table}
            WHERE data_id = {backend.placeholder(1)}
            """,
            data_id,
        )
        next_op = max_op + 1
        safe_base = SQLIdentifierSanitizer.sanitize(base_table)
        return f"{safe_base}__op_{next_op}"

    def _engine_clause(self) -> str:
        """Extra table-engine SQL for CTAS; ClickHouse overrides."""
        return ""

    # Fixed flag column name for create_flag (auto-suffixed on collision).
    FLAG_COLUMN = "filter_flag"

    def _flag_column_type(self) -> str:
        """Boolean column type for the in-place flag; ClickHouse overrides."""
        return "BOOLEAN"

    def _resolve_flag_column(self, column_lookup: Dict[str, str]) -> str:
        existing = {c.lower() for c in column_lookup.values()}
        name, suffix = self.FLAG_COLUMN, 1
        while name.lower() in existing:
            name = f"{self.FLAG_COLUMN}_{suffix}"
            suffix += 1
        return name

    async def _fill_flag_column(
        self, qualified: str, flag_col: str, where_clause: str, params: list
    ) -> None:
        # ponytail: COALESCE maps non-matching AND null-predicate rows to
        # FALSE — only selected rows read True. ClickHouse overrides with an
        # ALTER UPDATE + mutation wait (UPDATE is async there).
        await self._exec(
            f"UPDATE {qualified} "
            f"SET {self.db.quote_identifier(flag_col)} = "
            f"COALESCE(({where_clause}), FALSE)",
            *params,
        )

    async def _after_source_mutation(self, table: str, schema: str) -> None:
        # ponytail: hook — synchronous backends no-op; ClickHouse waits out
        # the async UPDATE mutation before the caller reads the flag.
        return None

    async def _maybe_write_flag(
        self,
        table: str,
        schema: str,
        qualified: str,
        where_clause: str,
        params: list,
        column_lookup: Dict[str, str],
        create_flag: bool,
    ) -> Optional[str]:
        """Write the boolean flag column in place; None when skipped."""
        if not create_flag:
            return None
        total = await self.db.fetchval(f"SELECT COUNT(*) FROM {qualified}")
        matched = await self.db.fetchval(
            f"SELECT COUNT(*) FROM {qualified} WHERE {where_clause}",
            *params,
        )
        if not matched or matched >= (total or 0):
            return None
        flag_col = self._resolve_flag_column(column_lookup)
        safe_flag = SQLIdentifierSanitizer.sanitize(flag_col)
        if flag_col.lower() not in {c.lower() for c in column_lookup.values()}:
            await self._exec(
                f"ALTER TABLE {qualified} ADD COLUMN "
                f"{self.db.quote_identifier(safe_flag)} {self._flag_column_type()}"
            )
        await self._fill_flag_column(qualified, safe_flag, where_clause, params)
        await self._after_source_mutation(table, schema)
        return flag_col

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
        create_flag: bool = False,
    ) -> Dict[str, Any]:
        """
        Create a new cache table containing only rows that satisfy
        the given predicate. Returns a sample or an async iterator.

        With create_flag=True and a proper row subset, a boolean flag
        column is also written in place onto the source table (True for
        matching rows, False otherwise). Empty/full matches skip the flag.
        """
        try:
            supported = (PostgresAdapter, DuckDBAdapter, ClickHouseAdapter)
            if isinstance(self.db, supported):
                # Build WHERE clause using an adapter-aware SQL context
                column_lookup = await self._column_lookup(table, schema)
                ctx = BackendAwareSQLContext(self.db, self.backend, column_lookup)
                where_clause = predicate.compile(ctx)

                qualified = self._qualified_table(table, schema)

                # Optional in-place flag on the source table. Written before
                # the result is selected so columns="*" includes it.
                flag_column = await self._maybe_write_flag(
                    table, schema, qualified, where_clause, ctx.params,
                    column_lookup, create_flag,
                )

                # Resolve selected columns after any flag column was added.
                try:
                    column_types = await self.db.get_column_types(table, schema) or {}
                except Exception:
                    column_types = {}
                lookup = {
                    SQLIdentifierSanitizer.sanitize(str(c)).lower():
                        SQLIdentifierSanitizer.sanitize(str(c))
                    for c in column_types
                }
                if columns == "*":
                    output_cols = visible_columns(column_types.keys())
                else:
                    requested = (
                        [columns] if isinstance(columns, str) else list(columns)
                    )
                    requested = list(dict.fromkeys(str(c) for c in requested))
                    try:
                        assert_user_columns(requested)
                    except ValueError as exc:
                        return self._error_response(str(exc))
                    output_cols = [
                        self._resolve_column_name(c, lookup) for c in requested
                    ]
                if not output_cols:
                    return self._error_response("No columns selected")

                if backend is None:
                    return self._error_response(
                        "backend is required to store a filter selection"
                    )
                cache_schema = backend.cache_schema

                selection = new_table
                if selection is None:
                    if data_id is None:
                        return self._error_response(
                            "backend and data_id are required to generate "
                            "the filter selection"
                        )
                    selection = await self._generate_cache_table_name(
                        table, backend, data_id
                    )
                selection = SQLIdentifierSanitizer.sanitize(selection)
                base_selection = selection
                suffix = 1
                while await self.db.table_exists(selection, cache_schema):
                    selection = f"{base_selection}_{suffix}"
                    suffix += 1
                qualified_selection = self._qualified_table(
                    selection, cache_schema
                )

                total = int(await self.db.fetchval(
                    f"SELECT COUNT(*) FROM {qualified}"
                ) or 0)
                pos_dtype = select_mf_idx_dtype(total)
                pos_sql = mf_idx_sql_type(pos_dtype, backend.backend)
                idx_q = self.db.quote_identifier(MF_IDX)

                await self._exec(
                    f"CREATE TABLE {qualified_selection} "
                    f"{self._engine_clause()} AS "
                    f"SELECT CAST(ROW_NUMBER() OVER (ORDER BY {idx_q}) - 1 "
                    f"AS {pos_sql}) AS \"position\", {idx_q} "
                    f"FROM {qualified} WHERE {where_clause}",
                    *ctx.params,
                )

                select_clause = ", ".join(
                    f"s.{self.db.quote_identifier(str(c))}" for c in output_cols
                )
                join_sql = (
                    f"SELECT {select_clause} FROM {qualified} s "
                    f"JOIN {qualified_selection} o "
                    f"ON s.{idx_q} = o.{idx_q} "
                    f'ORDER BY o."position"'
                )

                extras = {
                    "new_table": table,
                    "where_clause": where_clause,
                    "params": ctx.params,
                    "flag_column": flag_column,
                    "cache_kind": "ref",
                    "ref_schema": schema,
                    "ref_table": table,
                    "order_schema": cache_schema,
                    "order_table": selection,
                    "order_position_dtype": pos_dtype,
                    "order_kind": "filter",
                    "involved_cols": output_cols,
                    "result_cols": output_cols,
                    "generated_cols": [flag_column] if flag_column else [],
                    "owns_table": False,
                    "mutates": bool(flag_column),
                }

                if chunk_size is None:
                    rows = await self._fetch(join_sql)
                    sample = pd.DataFrame([dict(r) for r in rows])
                    return self._success_response(
                        "Filter applied successfully", sample, **extras
                    )

                async def iterator():
                    offset = 0
                    while True:
                        rows = await self._fetch(
                            f"{join_sql} LIMIT {chunk_size} OFFSET {offset}"
                        )
                        if not rows:
                            break
                        yield pd.DataFrame([dict(r) for r in rows])
                        offset += chunk_size

                return {
                    "is_error": False,
                    "message": "Filter applied (streaming)",
                    "error_message": None,
                    "iterator": iterator(),
                    "chunk_size": chunk_size,
                    **extras,
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
