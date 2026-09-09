from typing import Any, Dict, List, Union
import traceback
import pandas as pd

from memframe.db_manager.adapters.base import DatabaseAdapter
from memframe.db_manager.adapters.duckdb import DuckDBAdapter
from memframe.db_manager.adapters.postgresql import PostgresAdapter
from memframe.db_manager.adapters.clickhouse import ClickHouseAdapter
from memframe.utils.helper import SQLIdentifierSanitizer


class DataSortingOps:
    """
    Core sorting operations (SQL ORDER BY based).
    """

    def __init__(self, db_adapter: DatabaseAdapter):
        self.db = db_adapter


    # -----------------------------
    # Helpers
    # -----------------------------
    async def _exec(self, sql: str, *args):
        return await self.db.execute(sql, *args)

    async def _fetch(self, sql: str, *args):
        return await self.db.fetch(sql, *args)

    async def _fetch_sample(self, table: str, schema: str, columns="*") -> pd.DataFrame:
        qualified = self._qualified_table(table, schema)

        if columns == "*":
            col_clause = "*"
        else:
            sanitized = [
                SQLIdentifierSanitizer.sanitize(c) for c in columns
            ]
            col_clause = ", ".join(self.db.quote_identifier(c) for c in sanitized)

        rows = await self._fetch(f"SELECT {col_clause} FROM {qualified}")
        return pd.DataFrame([dict(r) for r in rows])

    def _qualified_table(self, table: str, schema: str) -> str:
        t = SQLIdentifierSanitizer.sanitize(table)
        s = SQLIdentifierSanitizer.sanitize(schema)
        return f'{self.db.quote_identifier(s)}.{self.db.quote_identifier(t)}'

    def _success_response(self, message, sample_df, involved_cols=None, **extra):
        return {
            "is_error": False,
            "message": message,
            "error_message": None,
            "involved_cols": involved_cols or [],
            "generated_cols": [],
            "result": sample_df,
            **extra,
        }

    def _error_response(self, msg, involved_cols=None):
        # ponytail: "result" key keeps is_operation_response() true so the
        # ContextManager proxy raises OperationError instead of leaking dicts.
        return {
            "is_error": True,
            "message": "",
            "error_message": msg,
            "involved_cols": involved_cols or [],
            "generated_cols": [],
            "result": None,
        }

    def _unsupported_backend_error(self) -> NotImplementedError:
        return NotImplementedError(
            f"Unsupported database backend for sorting operation: {self.db.__class__.__name__}"
        )

    async def _fetch_in_chunks(
        self,
        table: str, schema: str,
        chunk_size: int,
        columns="*",
        backend=None,
    ):
        # ponytail: iterator may be consumed after cache has moved the
        # transient table from the upload schema to the transient schema.
        # Try the original location first, fall back to transient on miss.
        def _quals():
            yield self._qualified_table(table, schema)
            if backend is not None:
                t_schema = getattr(backend, "transient_schema", None)
                if t_schema and t_schema != schema:
                    yield self._qualified_table(table, t_schema)

        offset = 0
        while True:
            if columns == "*":
                col_clause = "*"
            else:
                sanitized = [
                    SQLIdentifierSanitizer.sanitize(c) for c in columns
                ]
                col_clause = ", ".join(self.db.quote_identifier(c) for c in sanitized)

            rows = None
            last_exc = None
            for qualified in _quals():
                query = f"""
                    SELECT {col_clause}
                    FROM {qualified}
                    LIMIT {chunk_size} OFFSET {offset}
                """
                try:
                    rows = await self._fetch(query)
                    break
                except Exception as exc:
                    # ponytail: only fall back on "table does not exist"
                    if "does not exist" in str(exc).lower() or "not found" in str(exc).lower():
                        last_exc = exc
                        continue
                    raise
            if rows is None:
                if last_exc is not None:
                    raise last_exc
                break
            if not rows:
                break
            df = pd.DataFrame([dict(r) for r in rows])
            yield df
            offset += chunk_size

    async def _generate_transient_table_name(
        self,
        base_table: str,
        backend,
        data_id: str,
    ) -> str:
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

    # -------------------------------------------------------------
    #  ClickHouse NULL-ordering helper
    # -------------------------------------------------------------
    def _ch_order_term(self, quoted_col: str, direction: str, na_position: str) -> str:
        """
        Build a single ORDER BY term for ClickHouse.

        ClickHouse does NOT support NULLS FIRST / NULLS LAST.
        Its defaults are:  ASC → NULLs last,  DESC → NULLs first
        (identical to PostgreSQL / DuckDB defaults).

        So we only need an IS NULL sentinel when the user explicitly
        requests the **non-default** position:

          na_position="first" + ASC  →  non-default  →  need sentinel
          na_position="last"  + DESC →  non-default  →  need sentinel
          otherwise                    →  default      →  plain column

        Sentinel logic:
          (col IS NULL) DESC  → 1 (NULL) first, 0 (non-NULL) after
          (col IS NULL) ASC   → 0 (non-NULL) first, 1 (NULL) after
        """
        is_non_default = (
            (na_position == "first" and direction == "ASC") or
            (na_position == "last" and direction == "DESC")
        )

        if is_non_default:
            if na_position == "first":
                null_sort = f"({quoted_col} IS NULL) DESC"
            else:
                null_sort = f"({quoted_col} IS NULL) ASC"
            return f"{null_sort}, {quoted_col} {direction}"
        else:
            return f"{quoted_col} {direction}"

    # -----------------------------
    #  MAIN SORT
    # -----------------------------
    async def sort_values(
        self,
        table: str,
        schema: str,
        by: Union[str, List[str]],
        ascending: Union[bool, List[bool]] = True,
        na_position: str = "last",
        columns: Union[str, List[str]] = "*",
        backend=None,
        data_id: str = None,
        chunk_size: int = None,
    ) -> Dict[str, Any]:

        try:
            # ──────────────────────────────────────────────────────────
            #  Backend gate
            # ──────────────────────────────────────────────────────────
            supported = (PostgresAdapter, DuckDBAdapter, ClickHouseAdapter)
            if not isinstance(self.db, supported):
                raise self._unsupported_backend_error()

            is_clickhouse = isinstance(self.db, ClickHouseAdapter)

            # ──────────────────────────────────────────────────────────
            #  Normalize inputs  (same for all backends)
            # ──────────────────────────────────────────────────────────
            if isinstance(by, str):
                by = [by]

            if isinstance(ascending, bool):
                ascending = [ascending] * len(by)

            if len(by) != len(ascending):
                return self._error_response(
                    "Length of 'ascending' must match 'by'", by
                )

            if na_position not in ("first", "last"):
                return self._error_response(
                    "na_position must be 'first' or 'last'", by
                )

            # ──────────────────────────────────────────────────────────
            #  Sanitize columns  (same for all backends)
            # ──────────────────────────────────────────────────────────
            safe_by = [SQLIdentifierSanitizer.sanitize(c) for c in by]

            # ──────────────────────────────────────────────────────────
            #  SELECT clause  (same for all backends)
            # ──────────────────────────────────────────────────────────
            if columns == "*":
                select_clause = "*"
                output_cols = "*"
            else:
                if isinstance(columns, str):
                    columns = [columns]
                cols = list(dict.fromkeys(list(columns) + list(by)))
                sanitized = [SQLIdentifierSanitizer.sanitize(c) for c in cols]
                select_clause = ", ".join(
                    self.db.quote_identifier(c) for c in sanitized
                )
                output_cols = cols

            # ──────────────────────────────────────────────────────────
            #  ORDER BY clause  (backend-specific)
            # ──────────────────────────────────────────────────────────
            order_clauses = []
            for col, asc in zip(safe_by, ascending):
                direction = "ASC" if asc else "DESC"
                quoted_col = self.db.quote_identifier(col)

                if is_clickhouse:
                    order_clauses.append(
                        self._ch_order_term(quoted_col, direction, na_position)
                    )
                else:
                    # PostgreSQL / DuckDB — native NULLS FIRST/LAST
                    nulls = "NULLS FIRST" if na_position == "first" else "NULLS LAST"
                    order_clauses.append(f"{quoted_col} {direction} {nulls}")

            order_sql = ", ".join(order_clauses)

            # ──────────────────────────────────────────────────────────
            #  Generate transient table name  (same for all backends)
            # ──────────────────────────────────────────────────────────
            qualified = self._qualified_table(table, schema)

            if backend is None or data_id is None:
                return self._error_response("backend and data_id required")

            new_table = await self._generate_transient_table_name(
                table, backend, data_id,
            )
            new_table_safe = SQLIdentifierSanitizer.sanitize(new_table)
            qualified_new = (
                f"{self.db.quote_identifier(schema)}"
                f".{self.db.quote_identifier(new_table_safe)}"
            )

            # ──────────────────────────────────────────────────────────
            #  CREATE TABLE  (backend-specific)
            # ──────────────────────────────────────────────────────────
            if is_clickhouse:
                # ClickHouse requires ENGINE + ORDER BY for MergeTree
                create_sql = f"""
                    CREATE TABLE {qualified_new}
                    ENGINE = MergeTree()
                    ORDER BY tuple()
                    AS SELECT {select_clause}
                    FROM {qualified}
                    ORDER BY {order_sql}
                """
            else:
                # PostgreSQL / DuckDB — no ENGINE clause
                create_sql = f"""
                    CREATE TABLE {qualified_new} AS
                    SELECT {select_clause}
                    FROM {qualified}
                    ORDER BY {order_sql}
                """

            await self._exec(create_sql)

            # ──────────────────────────────────────────────────────────
            #  Return  (same for all backends)
            # ──────────────────────────────────────────────────────────
            if chunk_size is None:
                sample = await self._fetch_sample(
                    new_table_safe, schema, columns=output_cols,
                )
                return self._success_response(
                    f"Sorted by {by}",
                    sample,
                    involved_cols=by,
                    sorted_by=by,
                    ascending=ascending,
                    na_position=na_position,
                    output_columns=output_cols,
                    new_table=new_table_safe,
                )
            else:
                async def iterator():
                    async for chunk in self._fetch_in_chunks(
                        new_table_safe, schema, chunk_size, columns=output_cols,
                        backend=backend,
                    ):
                        yield chunk

                return {
                    "is_error": False,
                    "message": f"Sorted by {by} (streaming)",
                    "error_message": None,
                    "involved_cols": by,
                    "generated_cols": [],
                    "result": None,
                    "iterator": iterator(),
                    "chunk_size": chunk_size,
                    "new_table": new_table_safe,
                }

        except Exception as e:
            return self._error_response(
                f"sort_values error: {str(e)}\n{traceback.format_exc()}"
            )