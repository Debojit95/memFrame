from typing import Any, Dict, List, Union
import traceback
import pandas as pd

from memframe.core.analytix._fetch import fetch_df as _fetch_df_rows
from memframe.db_manager.adapters.base import DatabaseAdapter
from memframe.core.indexing import (
    HIDDEN_COLUMNS, MF_IDX, mf_idx_sql_type, select_mf_idx_dtype, strip_hidden,
)
from memframe.utils.helper import SQLIdentifierSanitizer
from memframe.core.analytix._response import fail, ok


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

        sql = f"SELECT {col_clause} FROM {qualified}"
        limit = getattr(self.db, "preview_rows", None)
        if limit:
            sql += f" LIMIT {int(limit)}"
        return await _fetch_df_rows(self.db, sql)

    def _qualified_table(self, table: str, schema: str) -> str:
        t = SQLIdentifierSanitizer.sanitize(table)
        s = SQLIdentifierSanitizer.sanitize(schema)
        return f'{self.db.quote_identifier(s)}.{self.db.quote_identifier(t)}'

    def _success_response(self, message, sample_df, involved_cols=None, **extra):
        return ok(message, result=sample_df, involved_cols=involved_cols or [], **extra)

    def _error_response(self, msg, involved_cols=None):
        # ponytail: "result" key keeps is_operation_response() true so the
        # ContextManager proxy raises OperationError instead of leaking dicts.
        return fail(msg, involved_cols=involved_cols or [])

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
        # cache table from the upload schema to the cache schema.
        # Try the original location first, fall back to cache on miss.
        def _quals():
            yield self._qualified_table(table, schema)
            if backend is not None:
                t_schema = getattr(backend, "cache_schema", None)
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

            last_exc = None
            df = None
            for qualified in _quals():
                query = f"""
                    SELECT {col_clause}
                    FROM {qualified}
                    LIMIT {chunk_size} OFFSET {offset}
                """
                try:
                    df = await _fetch_df_rows(self.db, query)
                    break
                except Exception as exc:
                    # ponytail: only fall back on "table does not exist"
                    if "does not exist" in str(exc).lower() or "not found" in str(exc).lower():
                        last_exc = exc
                        continue
                    raise
            if df is None:
                if last_exc is not None:
                    raise last_exc
                break
            if df.empty:
                break
            yield df
            offset += chunk_size

    async def _generate_cache_table_name(
        self,
        base_table: str,
        backend,
        data_id: str,
    ) -> str:
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

    # -------------------------------------------------------------
    #  Backend dialect hooks (DuckDB defaults; ClickHouse overrides)
    # -------------------------------------------------------------
    def _order_term(self, quoted_col: str, direction: str, na_position: str) -> str:
        # PostgreSQL / DuckDB — native NULLS FIRST/LAST
        nulls = "NULLS FIRST" if na_position == "first" else "NULLS LAST"
        return f"{quoted_col} {direction} {nulls}"

    def _create_sort_table_sql(
        self,
        qualified_new: str,
        select_clause: str,
        qualified: str,
        order_sql: str,
    ) -> str:
        # PostgreSQL / DuckDB — no ENGINE clause
        return f"""
            CREATE TABLE {qualified_new} AS
            SELECT {select_clause}
            FROM {qualified}
            ORDER BY {order_sql}
        """

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
        return_ctx: bool = False,
        cache_snapshot: bool = False,
    ) -> Dict[str, Any]:

        try:
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

            if backend is None or data_id is None:
                return self._error_response("backend and data_id required")

            col_types = await self.db.get_column_types(table, schema)
            visible = [c for c in col_types if str(c) not in HIDDEN_COLUMNS]
            if columns == "*":
                output_cols = visible
            else:
                if isinstance(columns, str):
                    columns = [columns]
                cols = list(dict.fromkeys(list(columns) + list(by)))
                output_cols = [
                    c for c in cols if str(c) not in HIDDEN_COLUMNS
                ]
            if not output_cols:
                return self._error_response("No columns selected", by)
            select_clause = ", ".join(
                f"s.{self.db.quote_identifier(str(c))}" for c in output_cols
            )

            order_clauses = []
            for col, asc in zip(safe_by, ascending):
                direction = "ASC" if asc else "DESC"
                quoted_col = self.db.quote_identifier(col)
                order_clauses.append(
                    self._order_term(quoted_col, direction, na_position)
                )
            if MF_IDX not in safe_by:
                order_clauses.append(f'{self.db.quote_identifier(MF_IDX)} ASC')
            order_sql = ", ".join(order_clauses)

            qualified = self._qualified_table(table, schema)
            row_count = int(await self.db.fetchval(
                f"SELECT COUNT(*) FROM {qualified}"
            ) or 0)
            pos_dtype = select_mf_idx_dtype(row_count)
            pos_sql = mf_idx_sql_type(pos_dtype, backend.backend)

            order_table = None
            qualified_order = None
            if return_ctx or not cache_snapshot:
                order_table = await self._generate_cache_table_name(
                    table, backend, data_id,
                )
                order_table = SQLIdentifierSanitizer.sanitize(order_table)
                suffix = 1
                base_order = order_table
                while await self.db.table_exists(order_table, backend.cache_schema):
                    order_table = f"{base_order}_{suffix}"
                    suffix += 1
                qualified_order = self._qualified_table(
                    order_table, backend.cache_schema
                )
                await self._exec(
                    f"CREATE TABLE {qualified_order} AS "
                    f"SELECT CAST(ROW_NUMBER() OVER (ORDER BY {order_sql}) - 1 AS {pos_sql}) "
                    f'AS "position", {self.db.quote_identifier(MF_IDX)} '
                    f"FROM {qualified}"
                )

            def _response(sample, iterator=None, extra_override=None):
                extra = {
                    "sorted_by": by,
                    "ascending": ascending,
                    "na_position": na_position,
                    "output_columns": output_cols,
                    "new_table": table,
                    "cache_kind": "ref",
                    "ref_schema": schema,
                    "ref_table": table,
                    "order_schema": backend.cache_schema if order_table else None,
                    "order_table": order_table,
                    "order_position_dtype": pos_dtype,
                    "order_kind": "sort" if order_table else None,
                    "order_limit": None,
                    "result_cols": output_cols,
                    "owns_table": False,
                    "mutates": False,
                }
                if extra_override:
                    extra.update(extra_override)
                if iterator is not None:
                    extra["iterator"] = iterator
                return ok(
                    f"Sorted by {by}", result=sample,
                    involved_cols=by, **extra,
                )

            if return_ctx:
                # The caller materializes the dataset DB-side; no pandas sample
                # is ever returned, so don't fetch millions of rows.
                return _response(None)

            plain_select = ", ".join(
                self.db.quote_identifier(str(c)) for c in output_cols
            )

            if cache_snapshot:
                # One write-once sorted snapshot replaces the permutation +
                # join replay. Cache hits read this table directly.
                snapshot = await self._generate_cache_table_name(
                    table, backend, data_id,
                )
                snapshot = SQLIdentifierSanitizer.sanitize(snapshot)
                suffix = 1
                base_snapshot = snapshot
                while await self.db.table_exists(snapshot, backend.cache_schema):
                    snapshot = f"{base_snapshot}_{suffix}"
                    suffix += 1
                qualified_snapshot = self._qualified_table(
                    snapshot, backend.cache_schema
                )
                # ponytail: the snapshot is write-once and never updated, so
                # its physical order is the sort order. Ordering by an extra
                # index column would re-sort on every read.
                await self._exec(
                    f"CREATE TABLE {qualified_snapshot} AS "
                    f"SELECT {plain_select} FROM {qualified} "
                    f"ORDER BY {order_sql}"
                )
                snapshot_sql = (
                    f"SELECT {plain_select} FROM {qualified_snapshot}"
                )
                snapshot_meta = {
                    "ref_schema": backend.cache_schema,
                    "ref_table": snapshot,
                    "order_schema": None,
                    "order_table": None,
                    "order_kind": None,
                    "owns_table": True,
                }

                if chunk_size is None:
                    limit = getattr(self.db, "preview_rows", None)
                    if limit:
                        snapshot_sql += f" LIMIT {int(limit)}"
                    sample = await _fetch_df_rows(self.db, snapshot_sql)
                    return _response(
                        strip_hidden(sample), extra_override=snapshot_meta
                    )

                async def snapshot_iterator():
                    offset = 0
                    while True:
                        df = await _fetch_df_rows(
                            self.db,
                            f"{snapshot_sql} LIMIT {chunk_size} OFFSET {offset}",
                        )
                        if df.empty:
                            break
                        yield strip_hidden(df)
                        offset += chunk_size

                resp = _response(
                    None, snapshot_iterator(), extra_override=snapshot_meta
                )
                resp["chunk_size"] = chunk_size
                return resp

            join_sql = (
                f"SELECT {select_clause} FROM {qualified} s "
                f"JOIN {qualified_order} o "
                f"ON s.{self.db.quote_identifier(MF_IDX)} = o.{self.db.quote_identifier(MF_IDX)} "
                f"ORDER BY o.\"position\""
            )

            if chunk_size is None:
                sql = join_sql
                limit = getattr(self.db, "preview_rows", None)
                if limit:
                    sql += f" LIMIT {int(limit)}"
                sample = await _fetch_df_rows(self.db, sql)
                return _response(strip_hidden(sample))

            async def iterator():
                offset = 0
                while True:
                    df = await _fetch_df_rows(self.db, 
                        f"{join_sql} LIMIT {chunk_size} OFFSET {offset}"
                    )
                    if df.empty:
                        break
                    yield strip_hidden(df)
                    offset += chunk_size

            resp = _response(None, iterator())
            resp["chunk_size"] = chunk_size
            return resp

        except Exception as e:
            return self._error_response(
                f"sort_values error: {str(e)}\n{traceback.format_exc()}"
            )
