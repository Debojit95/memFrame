"""
Selection operations (asof, at, iat, loc, get, where, iloc, select_dtypes, take)
"""

from typing import Any, Dict, List, Optional, Union
from collections.abc import Mapping
import traceback
import pandas as pd

from db_manager.adapters.base import DatabaseAdapter
from db_manager.adapters.duckdb import DuckDBAdapter
from db_manager.adapters.postgresql import PostgresAdapter
from utils.helper import SQLIdentifierSanitizer


class DataSelectionOps:
    """
    Core SQL operations for row/column selection, label-based access,
    and conditional replacement.
    """

    def __init__(self, db_adapter: DatabaseAdapter):
        self.db = db_adapter

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------
    async def _exec(self, sql: str, *args):
        return await self.db.execute(sql, *args)

    async def _fetch(self, sql: str, *args):
        return await self.db.fetch(sql, *args)

    def _quote(self, identifier: str) -> str:
        return self.db.quote_identifier(SQLIdentifierSanitizer.sanitize(identifier))

    def _qualified_table(self, table: str, schema: str) -> str:
        t = SQLIdentifierSanitizer.sanitize(table)
        s = SQLIdentifierSanitizer.sanitize(schema)
        return f"{self.db.quote_identifier(s)}.{self.db.quote_identifier(t)}"

    def _success_response(self, message, sample_df=None, **extra):
        return {
            "is_error": False,
            "message": message,
            "error_message": None,
            "result": sample_df,
            **extra,
        }

    def _error_response(self, msg):
        return {
            "is_error": True,
            "message": "",
            "error_message": msg,
        }

    def _unsupported_backend_error(self) -> NotImplementedError:
        return NotImplementedError(
            f"Unsupported database backend for selection operation: {self.db.__class__.__name__}"
        )

    @staticmethod
    def _row_get(row: Any, key: str, idx: int):
        if isinstance(row, Mapping):
            return row[key]
        if hasattr(row, "keys") and key in row.keys():
            return row[key]
        return row[idx]

    def _first_value_from_row(self, row: Any):
        if isinstance(row, Mapping):
            return next(iter(row.values()))
        if hasattr(row, "keys"):
            keys = list(row.keys())
            if keys:
                return row[keys[0]]
        return row[0]

    def _first_value_from_rows(self, rows: List[Any]):
        if not rows:
            raise IndexError("Query returned no rows")
        return self._first_value_from_row(rows[0])

    def _is_duckdb_backend(self) -> bool:
        if isinstance(self.db, DuckDBAdapter):
            return True
        elif isinstance(self.db, PostgresAdapter):
            return False
        else:
            raise self._unsupported_backend_error()

    async def _fetch_sample(
        self,
        table: str,
        schema: str,
        columns: Union[str, List[str]] = "*",
    ) -> pd.DataFrame:
        qualified = self._qualified_table(table, schema)
        if columns == "*":
            col_clause = "*"
        else:
            if isinstance(columns, str):
                columns = [columns]
            sanitized = [SQLIdentifierSanitizer.sanitize(c) for c in columns]
            col_clause = ", ".join(self._quote(c) for c in sanitized)
        rows = await self._fetch(f"SELECT {col_clause} FROM {qualified}")
        return pd.DataFrame([dict(r) for r in rows])

    async def _fetch_in_chunks(
        self,
        table: str,
        schema: str,
        chunk_size: int,
        columns: Union[str, List[str]] = "*",
    ):
        qualified = self._qualified_table(table, schema)
        if columns == "*":
            col_clause = "*"
        else:
            if isinstance(columns, str):
                columns = [columns]
            sanitized = [SQLIdentifierSanitizer.sanitize(c) for c in columns]
            col_clause = ", ".join(self._quote(c) for c in sanitized)
        offset = 0
        while True:
            query = f"SELECT {col_clause} FROM {qualified} LIMIT {chunk_size} OFFSET {offset}"
            rows = await self._fetch(query)
            if not rows:
                break
            yield pd.DataFrame([dict(r) for r in rows])
            offset += chunk_size

    async def _generate_transient_table_name(
        self,
        base_table: str,
        backend,
        data_id: str,
    ) -> str:
        max_op = await backend.fetch_val(
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

    async def _resolve_transient_table_name(
        self,
        base_table: str,
        backend,
        data_id: str,
    ) -> str:
        candidate = await self._generate_transient_table_name(base_table, backend, data_id)
        output_table = SQLIdentifierSanitizer.sanitize(candidate)
        dedupe_idx = 1
        transient_schema = getattr(backend, "transient_schema", "transient")
        while await self.db.table_exists(output_table, transient_schema):
            output_table = SQLIdentifierSanitizer.sanitize(f"{candidate}_{dedupe_idx}")
            dedupe_idx += 1
        return output_table

    async def _get_all_columns(self, table: str, schema: str) -> List[str]:
        qualified = self._qualified_table(table, schema)
        if isinstance(self.db, DuckDBAdapter):
            pragma = f"PRAGMA table_info({qualified})"
            cols = await self._fetch(pragma)
            return [self._row_get(c, "name", 1) for c in cols]
        elif isinstance(self.db, PostgresAdapter):
            cols = await self._fetch(
                "SELECT column_name FROM information_schema.columns WHERE table_schema = $1 AND table_name = $2",
                schema,
                table,
            )
            return [self._row_get(c, "column_name", 0) for c in cols]
        else:
            raise self._unsupported_backend_error()

    async def _get_column_types(self, table: str, schema: str) -> Dict[str, str]:
        qualified = self._qualified_table(table, schema)
        if isinstance(self.db, DuckDBAdapter):
            cols = await self._fetch(f"PRAGMA table_info({qualified})")
            return {
                self._row_get(row, "name", 1): self._row_get(row, "type", 2)
                for row in cols
            }
        elif isinstance(self.db, PostgresAdapter):
            rows = await self._fetch(
                "SELECT column_name, data_type FROM information_schema.columns "
                "WHERE table_schema = $1 AND table_name = $2",
                schema,
                table,
            )
            return {
                self._row_get(row, "column_name", 0): self._row_get(row, "data_type", 1)
                for row in rows
            }
        else:
            raise self._unsupported_backend_error()

    @staticmethod
    def _classify_column_type(sql_type: str) -> str:
        t = sql_type.lower().split("(")[0].strip()
        numeric_types = {
            "smallint", "integer", "bigint", "int2", "int4", "int8",
            "decimal", "numeric", "real", "float4", "float8", "double precision",
            "double", "float",
        }
        categorical_types = {
            "varchar", "character varying", "char", "character", "text",
            "nchar", "nvarchar", "clob",
        }
        date_types = {"date"}
        timestamp_types = {"timestamp", "timestamptz", "datetime"}

        if t in numeric_types:
            return "numeric"
        elif t in categorical_types:
            return "categorical"
        elif t in date_types:
            return "date"
        elif t in timestamp_types:
            return "timestamp"
        else:
            return "other"

    # ------------------------------------------------------------------
    # Selection methods (original, untouched for scalars)
    # ------------------------------------------------------------------
    async def asof(
        self,
        table: str,
        schema: str,
        where: Union[str, List[str]],
        on: str,
        subset: Optional[Union[str, List[str]]] = None,
        backend=None,
        data_id: str = None,
        chunk_size: int = None,
    ) -> Dict[str, Any]:
        try:
            if isinstance(self.db, PostgresAdapter) or isinstance(self.db, DuckDBAdapter):
                on_quoted = self._quote(on)
                if isinstance(where, (str, pd.Timestamp)):
                    where_vals = [str(pd.Timestamp(where))]
                    is_scalar = True
                else:
                    where_vals = [str(pd.Timestamp(w)) for w in where]
                    is_scalar = False

                if subset is None:
                    subset_cols = await self._get_all_columns(table, schema)
                else:
                    if isinstance(subset, str):
                        subset = [subset]
                    subset_cols = subset
                subset_quoted = [self._quote(c) for c in subset_cols]

                result_rows = []
                for w in where_vals:
                    condition = " AND ".join(f"{c} IS NOT NULL" for c in subset_quoted)
                    sql = f"""
                        SELECT *
                        FROM {self._qualified_table(table, schema)}
                        WHERE {on_quoted} <= {self.db.placeholder(1)}
                          AND {condition}
                        ORDER BY {on_quoted} DESC
                        LIMIT 1
                    """
                    row = await self._fetch(sql, w)
                    if row:
                        result_rows.append(row[0])
                    else:
                        cols = await self._get_all_columns(table, schema)
                        result_rows.append(tuple([None] * len(cols)))

                all_cols = await self._get_all_columns(table, schema)
                df = pd.DataFrame(result_rows, columns=all_cols)
                if is_scalar and not df.empty:
                    sample = df.iloc[0]
                elif is_scalar:
                    sample = pd.Series(index=all_cols, dtype="object")
                else:
                    sample = df

                return self._success_response(
                    f"asof on {where} using column '{on}'",
                    sample,
                    where=where,
                    subset=subset,
                    on=on,
                )
            else:
                raise self._unsupported_backend_error()
        except Exception as e:
            return self._error_response(f"asof error: {str(e)}\n{traceback.format_exc()}")

    async def at(
        self,
        table: str,
        schema: str,
        row_label: Any,
        column_label: str,
        index_column: Optional[str] = None,
    ) -> Dict[str, Any]:
        try:
            if isinstance(self.db, PostgresAdapter) or isinstance(self.db, DuckDBAdapter):
                all_columns = await self._get_all_columns(table, schema)
                if not all_columns:
                    raise KeyError("No columns available in table.")
                if column_label not in all_columns:
                    raise KeyError(f"Column '{column_label}' not found")
                resolved_index_column = index_column
                if resolved_index_column is None:
                    resolved_index_column = "id" if "id" in all_columns else all_columns[0]
                elif resolved_index_column not in all_columns:
                    raise KeyError(f"Index column '{resolved_index_column}' not found")

                quoted_index = self._quote(resolved_index_column)
                quoted_col = self._quote(column_label)
                sql = f"""
                    SELECT {quoted_col}
                    FROM {self._qualified_table(table, schema)}
                    WHERE {quoted_index} = {self.db.placeholder(1)}
                    LIMIT 1
                """
                row = await self._fetch(sql, row_label)
                if row:
                    scalar = self._first_value_from_rows(row)
                else:
                    raise KeyError(f"Label '{row_label}' not found in index column '{resolved_index_column}'")
                return self._success_response(
                    f"at[{row_label}, {column_label}]",
                    sample_df=None,
                    value=scalar,
                    index_column=resolved_index_column,
                )
            else:
                raise self._unsupported_backend_error()
        except KeyError as ke:
            return self._error_response(str(ke))
        except Exception as e:
            return self._error_response(f"at error: {str(e)}\n{traceback.format_exc()}")

    async def iat(
        self,
        table: str,
        schema: str,
        row_position: int,
        column_label: str,
        order_by: Union[str, List[str]],
    ) -> Dict[str, Any]:
        try:
            if isinstance(self.db, PostgresAdapter) or isinstance(self.db, DuckDBAdapter):
                if isinstance(order_by, str):
                    order_by = [order_by]
                order_clause = ", ".join(self._quote(c) for c in order_by)
                quoted_col = self._quote(column_label)
                sql = f"""
                    SELECT {quoted_col}
                    FROM (
                        SELECT {quoted_col}, ROW_NUMBER() OVER (ORDER BY {order_clause}) AS rn
                        FROM {self._qualified_table(table, schema)}
                    ) sub
                    WHERE sub.rn = {row_position + 1}
                """
                row = await self._fetch(sql)
                if row:
                    scalar = self._first_value_from_rows(row)
                else:
                    raise IndexError(f"Position {row_position} out of bounds")
                return self._success_response(
                    f"iat[{row_position}, {column_label}]",
                    sample_df=None,
                    value=scalar,
                )
            else:
                raise self._unsupported_backend_error()
        except IndexError as ie:
            return self._error_response(str(ie))
        except Exception as e:
            return self._error_response(f"iat error: {str(e)}\n{traceback.format_exc()}")

    # ------------------------------------------------------------------
    # loc (creates transient table if backend provided)
    # ------------------------------------------------------------------
    async def loc(
        self,
        table: str,
        schema: str,
        row_selector: Union[str, List[str], slice, "pd.Series", Any],
        column_selector: Optional[Union[str, List[str]]] = None,
        index_column: Optional[str] = None,
        backend=None,
        data_id: str = None,
        chunk_size: int = None,
    ) -> Dict[str, Any]:
        try:
            if isinstance(self.db, PostgresAdapter) or isinstance(self.db, DuckDBAdapter):
                quoted_cols = "*"
                if column_selector is not None:
                    if isinstance(column_selector, str):
                        column_selector = [column_selector]
                    col_names = [self._quote(c) for c in column_selector]
                    quoted_cols = ", ".join(col_names)

                where_clause = ""
                params = []

                if isinstance(row_selector, str):
                    where_clause = f"WHERE {row_selector}"
                elif isinstance(row_selector, (list, tuple)):
                    if not index_column:
                        raise ValueError("list row_selector requires index_column")
                    placeholder_list = ", ".join(self.db.placeholder(i+1) for i in range(len(row_selector)))
                    where_clause = f"WHERE {self._quote(index_column)} IN ({placeholder_list})"
                    params = list(row_selector)
                elif isinstance(row_selector, slice):
                    if not index_column:
                        raise ValueError("slice row_selector requires index_column")
                    start = row_selector.start
                    stop = row_selector.stop
                    if start is None or stop is None:
                        raise ValueError("Slice must have start and stop for label-based range")
                    where_clause = f"WHERE {self._quote(index_column)} BETWEEN {self.db.placeholder(1)} AND {self.db.placeholder(2)}"
                    params = [start, stop]
                else:
                    if not index_column:
                        raise ValueError("scalar row_selector requires index_column")
                    where_clause = f"WHERE {self._quote(index_column)} = {self.db.placeholder(1)}"
                    params = [row_selector]

                new_table = None
                if backend is not None and data_id is not None:
                    new_table = await self._generate_transient_table_name(table, backend, data_id)
                    full_new = f"{self.db.quote_identifier(schema)}.{self._quote(new_table)}"
                    create_sql = f"""
                        CREATE TABLE {full_new} AS
                        SELECT {quoted_cols}
                        FROM {self._qualified_table(table, schema)}
                        {where_clause}
                    """
                    await self._exec(create_sql, *params)
                    if chunk_size is None:
                        sample = await self._fetch_sample(new_table, schema, columns=column_selector or "*")
                    else:
                        async def iterator():
                            async for chunk in self._fetch_in_chunks(
                                new_table, schema, chunk_size, columns=column_selector or "*"
                            ):
                                yield chunk
                        return self._success_response(
                            "loc selection (streaming)",
                            sample_df=None,
                            iterator=iterator(),
                            chunk_size=chunk_size,
                            new_table=new_table,
                        )
                    return self._success_response(
                        "loc selection",
                        sample,
                        new_table=new_table,
                        output_columns=column_selector or "*",
                    )
                else:
                    sql = f"SELECT {quoted_cols} FROM {self._qualified_table(table, schema)} {where_clause}"
                    rows = await self._fetch(sql, *params)
                    df = pd.DataFrame([dict(r) for r in rows])
                    return self._success_response("loc selection (read-only)", sample_df=df)
            else:
                raise self._unsupported_backend_error()
        except Exception as e:
            return self._error_response(f"loc error: {str(e)}\n{traceback.format_exc()}")

    # ------------------------------------------------------------------
    # get (read‑only)
    # ------------------------------------------------------------------
    async def get(
        self,
        table: str,
        schema: str,
        keys: Union[str, List[str]],
        default: Any = None,
    ) -> Dict[str, Any]:
        try:
            if isinstance(self.db, PostgresAdapter) or isinstance(self.db, DuckDBAdapter):
                if isinstance(keys, str):
                    keys = [keys]
                all_columns = await self._get_all_columns(table, schema)
                safe_keys = [SQLIdentifierSanitizer.sanitize(k) for k in keys]
                valid = [k for k in safe_keys if k in all_columns]
                if not valid:
                    return self._success_response(
                        "get: no matching columns",
                        sample_df=pd.DataFrame({k: [default] for k in keys}),
                        default=default,
                    )
                quoted_cols = ", ".join(self._quote(c) for c in valid)
                sql = f"SELECT {quoted_cols} FROM {self._qualified_table(table, schema)}"
                rows = await self._fetch(sql)
                df = pd.DataFrame([dict(r) for r in rows])
                for k in keys:
                    if k not in valid:
                        df[k] = default
                return self._success_response("get columns", sample_df=df)
            else:
                raise self._unsupported_backend_error()
        except Exception as e:
            return self._error_response(f"get error: {str(e)}\n{traceback.format_exc()}")

    # ------------------------------------------------------------------
    # where (creates transient table)
    # ------------------------------------------------------------------
    async def where(
        self,
        table: str,
        schema: str,
        cond: str,
        other: Optional[Any] = None,
        backend=None,
        data_id: str = None,
        chunk_size: int = None,
    ) -> Dict[str, Any]:
        try:
            if isinstance(self.db, PostgresAdapter) or isinstance(self.db, DuckDBAdapter):
                columns = await self._get_all_columns(table, schema)
                cased = []
                for col in columns:
                    q = self._quote(col)
                    if other is None:
                        replacement = "NULL"
                    elif isinstance(other, (int, float, str)):
                        replacement = f"{self.db.placeholder(1)}"
                    else:
                        replacement = other
                    cased.append(f"CASE WHEN ({cond}) THEN {q} ELSE {replacement} END AS {q}")

                new_table = None
                if backend is not None and data_id is not None:
                    new_table = await self._generate_transient_table_name(table, backend, data_id)
                    full_new = f"{self.db.quote_identifier(schema)}.{self._quote(new_table)}"
                    select_expr = ", ".join(cased)
                    create_sql = f"""
                        CREATE TABLE {full_new} AS
                        SELECT {select_expr}
                        FROM {self._qualified_table(table, schema)}
                    """
                    if other is not None and isinstance(other, (int, float, str)):
                        await self._exec(create_sql, other)
                    else:
                        await self._exec(create_sql)

                    if chunk_size is None:
                        sample = await self._fetch_sample(new_table, schema)
                    else:
                        async def iterator():
                            async for chunk in self._fetch_in_chunks(new_table, schema, chunk_size):
                                yield chunk
                        return self._success_response(
                            "where (streaming)",
                            sample_df=None,
                            iterator=iterator(),
                            chunk_size=chunk_size,
                            new_table=new_table,
                        )
                    return self._success_response(
                        "where applied",
                        sample,
                        new_table=new_table,
                        cond=cond,
                        other=other,
                    )
                else:
                    # No backend: compute directly (won't create table)
                    sql = f"SELECT {select_expr} FROM {self._qualified_table(table, schema)}"
                    if other is not None and isinstance(other, (int, float, str)):
                        rows = await self._fetch(sql, other)
                    else:
                        rows = await self._fetch(sql)
                    df = pd.DataFrame([dict(r) for r in rows])
                    return self._success_response("where applied (read-only)", sample_df=df)
            else:
                raise self._unsupported_backend_error()
        except Exception as e:
            return self._error_response(f"where error: {str(e)}\n{traceback.format_exc()}")

    # ------------------------------------------------------------------
    # select_dtypes (creates transient table)
    # ------------------------------------------------------------------
    async def select_dtypes(
        self,
        table: str,
        schema: str,
        include: Optional[Union[str, List[str]]] = None,
        exclude: Optional[Union[str, List[str]]] = None,
        backend=None,
        data_id: str = None,
        chunk_size: int = None,
    ) -> Dict[str, Any]:
        try:
            if isinstance(self.db, PostgresAdapter) or isinstance(self.db, DuckDBAdapter):
                if include is None and exclude is None:
                    return self._error_response("At least one of 'include' or 'exclude' must be specified.")

                if include is not None:
                    if isinstance(include, str):
                        include = [include]
                    include = [i.lower() for i in include]
                if exclude is not None:
                    if isinstance(exclude, str):
                        exclude = [exclude]
                    exclude = [e.lower() for e in exclude]

                col_types = await self._get_column_types(table, schema)
                all_columns = list(col_types.keys())
                selected = set(all_columns)

                if include is not None:
                    include_set = set(include)
                    selected = {col for col in selected if self._classify_column_type(col_types[col]) in include_set}
                if exclude is not None:
                    exclude_set = set(exclude)
                    selected = {col for col in selected if self._classify_column_type(col_types[col]) not in exclude_set}

                if not selected:
                    return self._error_response("No columns match the given dtypes.")

                selected_sorted = sorted(selected)
                quoted_cols = ", ".join(self._quote(c) for c in selected_sorted)

                new_table = None
                if backend is not None and data_id is not None:
                    new_table = await self._generate_transient_table_name(table, backend, data_id)
                    full_new = f"{self.db.quote_identifier(schema)}.{self._quote(new_table)}"
                    create_sql = f"""
                        CREATE TABLE {full_new} AS
                        SELECT {quoted_cols}
                        FROM {self._qualified_table(table, schema)}
                    """
                    await self._exec(create_sql)

                    if chunk_size is None:
                        sample = await self._fetch_sample(new_table, schema, columns=selected_sorted)
                    else:
                        async def iterator():
                            async for chunk in self._fetch_in_chunks(
                                new_table, schema, chunk_size, columns=selected_sorted
                            ):
                                yield chunk
                        return self._success_response(
                            f"select_dtypes (streaming) include={include} exclude={exclude}",
                            sample_df=None,
                            iterator=iterator(),
                            chunk_size=chunk_size,
                            new_table=new_table,
                            selected_columns=selected_sorted,
                        )
                    return self._success_response(
                        f"select_dtypes include={include} exclude={exclude}",
                        sample,
                        new_table=new_table,
                        selected_columns=selected_sorted,
                    )
                else:
                    sql = f"SELECT {quoted_cols} FROM {self._qualified_table(table, schema)}"
                    rows = await self._fetch(sql)
                    df = pd.DataFrame([dict(r) for r in rows])
                    return self._success_response(
                        f"select_dtypes (read-only) include={include} exclude={exclude}",
                        sample_df=df,
                        selected_columns=selected_sorted,
                    )
            else:
                raise self._unsupported_backend_error()
        except Exception as e:
            return self._error_response(f"select_dtypes error: {str(e)}\n{traceback.format_exc()}")

    # ------------------------------------------------------------------
    # iloc (creates transient table)
    # ------------------------------------------------------------------
    async def iloc(
        self,
        table: str,
        schema: str,
        row_indexer: Optional[Union[int, List[int], slice, list]] = None,
        col_indexer: Optional[Union[int, List[int], slice, list]] = None,
        backend=None,
        data_id: str = None,
    ) -> Dict[str, Any]:
        try:
            if isinstance(self.db, PostgresAdapter) or isinstance(self.db, DuckDBAdapter):
                total_rows = int(
                    self._first_value_from_rows(
                        await self._fetch(
                            f"SELECT COUNT(*) FROM {self._qualified_table(table, schema)}"
                        )
                    )
                )
                all_cols = await self._get_all_columns(table, schema)
                total_cols = len(all_cols)

                if row_indexer is not None:
                    row_pos = self._convert_iloc_indexer(row_indexer, total_rows, "row")
                else:
                    row_pos = list(range(total_rows))

                if col_indexer is not None:
                    col_pos = self._convert_iloc_indexer(col_indexer, total_cols, "column")
                else:
                    col_pos = list(range(total_cols))

                if len(row_pos) == 1 and len(col_pos) == 1:
                    row_idx = row_pos[0]
                    col_name = all_cols[col_pos[0]]
                    sql = f"""
                        SELECT {self._quote(col_name)}
                        FROM {self._qualified_table(table, schema)}
                        LIMIT 1 OFFSET {row_idx}
                    """
                    row = await self._fetch(sql)
                    if row:
                        scalar = self._first_value_from_rows(row)
                    else:
                        raise IndexError(f"Row index {row_idx} out of bounds")
                    return self._success_response(
                        f"iloc[{row_idx}, {col_pos[0]}]",
                        sample_df=None,
                        value=scalar,
                    )

                selected_col_names = [all_cols[i] for i in col_pos]
                quoted_cols = ", ".join(self._quote(c) for c in selected_col_names)
                row_pos_list = [p + 1 for p in row_pos]   # 1‑based
                ord_list = list(range(1, len(row_pos_list) + 1))

                if isinstance(self.db, DuckDBAdapter):
                    idx_arr = "ARRAY[" + ", ".join(map(str, row_pos_list)) + "]"
                    ord_arr = "ARRAY[" + ", ".join(map(str, ord_list)) + "]"
                    join_clause = f"""
                    JOIN (
                        SELECT UNNEST({idx_arr}) AS idx, UNNEST({ord_arr}) AS ord
                    ) v ON t._rn = v.idx
                    ORDER BY v.ord
                    """
                elif isinstance(self.db, PostgresAdapter):
                    idx_arr = "ARRAY[" + ", ".join(map(str, row_pos_list)) + "]::int[]"
                    ord_arr = "ARRAY[" + ", ".join(map(str, ord_list)) + "]::int[]"
                    join_clause = f"""
                    JOIN (
                        SELECT * FROM UNNEST({idx_arr}, {ord_arr}) AS v(idx, ord)
                    ) v ON t._rn = v.idx
                    ORDER BY v.ord
                    """
                else:
                    raise self._unsupported_backend_error()

                sql = f"""
                SELECT {quoted_cols}
                FROM (
                    SELECT {quoted_cols}, ROW_NUMBER() OVER () AS _rn
                    FROM {self._qualified_table(table, schema)}
                ) t
                {join_clause}
                """

                return await self._build_iloc_result(
                    sql, selected_col_names, row_pos, col_pos,
                    backend, data_id,
                )
            else:
                raise self._unsupported_backend_error()
        except Exception as e:
            return self._error_response(f"iloc error: {str(e)}\n{traceback.format_exc()}")

    def _convert_iloc_indexer(self, indexer, total_length: int, axis_name: str) -> List[int]:
        if isinstance(indexer, int):
            if indexer < 0:
                indexer += total_length
            if indexer < 0 or indexer >= total_length:
                raise IndexError(f"{axis_name} index {indexer} out of bounds")
            return [indexer]
        if isinstance(indexer, slice):
            start, stop, step = indexer.indices(total_length)
            return list(range(start, stop, step))
        if isinstance(indexer, (list, tuple)):
            if all(isinstance(i, bool) for i in indexer):
                if len(indexer) != total_length:
                    raise IndexError(
                        f"Boolean indexer length ({len(indexer)}) must match {axis_name} length ({total_length})"
                    )
                return [i for i, val in enumerate(indexer) if val]
            result = []
            for i in indexer:
                if i < 0:
                    i += total_length
                if i < 0 or i >= total_length:
                    raise IndexError(f"{axis_name} index {i} out of bounds")
                result.append(i)
            return result
        raise TypeError(f"Unsupported indexer type: {type(indexer)}")

    async def _build_iloc_result(
        self,
        sql: str,
        selected_cols: List[str],
        row_pos: List[int],
        col_pos: List[int],
        backend,
        data_id: str,
    ) -> Dict[str, Any]:
        if isinstance(self.db, PostgresAdapter) or isinstance(self.db, DuckDBAdapter):
            if backend and data_id:
                base_table_name = f"iloc_{len(row_pos)}x{len(col_pos)}"
                new_table = await self._resolve_transient_table_name(base_table_name, backend, data_id)
                transient_schema = getattr(backend, "transient_schema", "transient")
                full_new = f"{self.db.quote_identifier(transient_schema)}.{self._quote(new_table)}"
                create_sql = f"CREATE TABLE {full_new} AS {sql}"
                await self._exec(create_sql)
                sample = await self._fetch_sample(new_table, transient_schema, columns=selected_cols)
                return self._success_response(
                    f"iloc rows {row_pos} cols {col_pos}",
                    sample,
                    new_table=new_table,
                    row_indices=row_pos,
                    col_indices=col_pos,
                )
            else:
                rows = await self._fetch(sql)
                df = pd.DataFrame([dict(r) for r in rows])
                return self._success_response(
                    f"iloc rows {row_pos} cols {col_pos} (read‑only)",
                    sample_df=df,
                    row_indices=row_pos,
                    col_indices=col_pos,
                )

        else:
            raise self._unsupported_backend_error()

    # ------------------------------------------------------------------
    # take (new)
    # ------------------------------------------------------------------
    async def take(
        self,
        table: str,
        schema: str,
        indices: List[int],
        axis: int = 0,
        backend=None,
        data_id: str = None,
    ) -> Dict[str, Any]:
        """Select rows (axis=0) or columns (axis=1) by integer indices, creating a transient table."""
        try:
            if isinstance(self.db, PostgresAdapter) or isinstance(self.db, DuckDBAdapter):
                if axis == 0:
                    # row selection by indices
                    return await self.iloc(
                        table=table, schema=schema,
                        row_indexer=indices, col_indexer=None,
                        backend=backend, data_id=data_id,
                    )
                elif axis == 1:
                    # column selection by indices
                    return await self.iloc(
                        table=table, schema=schema,
                        row_indexer=None, col_indexer=indices,
                        backend=backend, data_id=data_id,
                    )
                else:
                    return self._error_response(f"axis must be 0 or 1, got {axis}")
            else:
                raise self._unsupported_backend_error()
        except Exception as e:
            return self._error_response(f"take error: {str(e)}\n{traceback.format_exc()}")
