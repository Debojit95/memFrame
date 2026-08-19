"""
Selection operations (asof, at, iat, loc, get, where, iloc, select_dtypes, take)
"""

from datetime import date, datetime
from typing import Any, Dict, List, Optional, Union
from collections.abc import Mapping
import traceback
import pandas as pd

from memframe.db_manager.adapters.base import DatabaseAdapter
from memframe.db_manager.adapters.duckdb import DuckDBAdapter
from memframe.db_manager.adapters.postgresql import PostgresAdapter
from memframe.db_manager.adapters.clickhouse import ClickHouseAdapter
from memframe.utils.helper import SQLIdentifierSanitizer
from memframe.exceptions import DataNotFound, OperationError
from memframe.core.analytix._response import fail, ok


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
        result = extra.pop("result", sample_df)
        return ok(message, result=result, **extra)

    def _error_response(self, msg):
        return fail(msg)

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
            raise DataNotFound("Query returned no rows")
        return self._first_value_from_row(rows[0])

    def _is_duckdb_backend(self) -> bool:
        if isinstance(self.db, DuckDBAdapter):
            return True
        elif isinstance(self.db, PostgresAdapter):
            return False
        elif isinstance(self.db, ClickHouseAdapter):
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

    async def _resolve_transient_table_name(
        self,
        base_table: str,
        backend,
        data_id: str,
    ) -> str:
        candidate = await self._generate_transient_table_name(base_table, backend, data_id)
        output_table = SQLIdentifierSanitizer.sanitize(candidate)
        dedupe_idx = 1
        while await self.db.table_exists(output_table, "transient"):
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
        elif isinstance(self.db, ClickHouseAdapter):
            cols = await self._fetch(
                f"SELECT name FROM system.columns WHERE database = {self.db.placeholder(1)} AND table = {self.db.placeholder(2)}",
                schema,
                table,
            )
            return [self._row_get(c, "name", 0) for c in cols]
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
        elif isinstance(self.db, ClickHouseAdapter):
            rows = await self._fetch(
                f"SELECT name, type FROM system.columns WHERE database = {self.db.placeholder(1)} AND table = {self.db.placeholder(2)}",
                schema,
                table,
            )
            return {
                self._row_get(row, "name", 0): self._row_get(row, "type", 1)
                for row in rows
            }
        else:
            raise self._unsupported_backend_error()

    @staticmethod
    def _classify_column_type(sql_type: str) -> str:
        low = sql_type.lower()
        normalized = low
        if normalized.startswith("nullable(") and normalized.endswith(")"):
            normalized = normalized[len("nullable("):-1]
        t = normalized.split("(")[0].strip()
        numeric_types = {
            "smallint", "integer", "bigint", "int2", "int4", "int8",
            "decimal", "numeric", "real", "float4", "float8", "double precision",
            "double", "float",
            # ClickHouse numeric types
            "int8", "int16", "int32", "int64", "uint8", "uint16", "uint32", "uint64",
            "float32", "float64"
        }
        categorical_types = {
            "varchar", "character varying", "char", "character", "text",
            "nchar", "nvarchar", "clob",
            # ClickHouse string types
            "string", "fixedstring"
        }
        date_types = {"date"}
        timestamp_types = {
            "timestamp", "timestamptz", "datetime", "datetime64",
            "timestamp with time zone", "timestamp without time zone",
        }

        if t in numeric_types:
            return "numeric"
        elif t in categorical_types:
            return "categorical"
        elif t in date_types:
            return "date"
        elif t in timestamp_types or t.startswith("datetime") or low.startswith("timestamp"):
            return "timestamp"
        else:
            return "other"

    def _normalize_asof_value(self, value: Any, column_kind: str) -> Any:
        if column_kind in {"date", "timestamp"}:
            ts = pd.Timestamp(value)
            if column_kind == "date":
                return ts.date()
            return ts.to_pydatetime()
        if column_kind == "numeric":
            if isinstance(value, str):
                parsed = pd.to_numeric(value)
                if hasattr(parsed, "item"):
                    return parsed.item()
                return parsed
            return value
        return value

    def _normalize_asof_where_value(self, value: Any, column_kind: str) -> Any:
        normalized = self._normalize_asof_value(value, column_kind)
        if isinstance(self.db, PostgresAdapter):
            return normalized
        # ponytail: check datetime before date — datetime is a subclass of date.
        # A bare date must serialize as "YYYY-MM-DD" or ClickHouse rejects it
        # against a Date column (TYPE_MISMATCH); a datetime keeps its time.
        if isinstance(normalized, datetime):
            return str(pd.Timestamp(normalized))
        if isinstance(normalized, date):
            return str(normalized)
        return normalized

    # ------------------------------------------------------------------
    # Selection methods
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
            if isinstance(self.db, PostgresAdapter) or isinstance(self.db, DuckDBAdapter) or isinstance(self.db, ClickHouseAdapter):
                on_quoted = self._quote(on)
                column_types = await self._get_column_types(table, schema)
                safe_on = SQLIdentifierSanitizer.sanitize(on)
                if safe_on not in column_types:
                    return self._error_response(f"Column '{on}' does not exist")
                on_kind = self._classify_column_type(column_types[safe_on])

                if isinstance(where, (str, pd.Timestamp, datetime, date)):
                    where_vals = [self._normalize_asof_where_value(where, on_kind)]
                    is_scalar = True
                else:
                    where_vals = [
                        self._normalize_asof_where_value(w, on_kind)
                        for w in where
                    ]
                    is_scalar = False

                all_cols = await self._get_all_columns(table, schema)
                if subset is None:
                    subset_cols = all_cols
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
                        first_row = row[0]
                        result_rows.append(
                            tuple(
                                self._row_get(first_row, col, idx)
                                for idx, col in enumerate(all_cols)
                            )
                        )
                    else:
                        result_rows.append(tuple([None] * len(all_cols)))

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
            if isinstance(self.db, PostgresAdapter) or isinstance(self.db, DuckDBAdapter) or isinstance(self.db, ClickHouseAdapter):
                all_columns = await self._get_all_columns(table, schema)
                if not all_columns:
                    raise DataNotFound("No columns available in table.")
                if column_label not in all_columns:
                    raise DataNotFound(f"Column '{column_label}' not found")
                resolved_index_column = index_column
                if resolved_index_column is None:
                    resolved_index_column = "id" if "id" in all_columns else all_columns[0]
                elif resolved_index_column not in all_columns:
                    raise DataNotFound(f"Index column '{resolved_index_column}' not found")

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
                    raise DataNotFound(f"Label '{row_label}' not found in index column '{resolved_index_column}'")
                return self._success_response(
                    f"at[{row_label}, {column_label}]",
                    result=scalar,
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
            if isinstance(self.db, PostgresAdapter) or isinstance(self.db, DuckDBAdapter) or isinstance(self.db, ClickHouseAdapter):
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
                    raise OperationError(f"Position {row_position} out of bounds")
                return self._success_response(
                    f"iat[{row_position}, {column_label}]",
                    result=scalar,
                )
            else:
                raise self._unsupported_backend_error()
        except IndexError as ie:
            return self._error_response(str(ie))
        except Exception as e:
            return self._error_response(f"iat error: {str(e)}\n{traceback.format_exc()}")

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
            if isinstance(self.db, PostgresAdapter) or isinstance(self.db, DuckDBAdapter) or isinstance(self.db, ClickHouseAdapter):
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
            if isinstance(self.db, PostgresAdapter) or isinstance(self.db, DuckDBAdapter) or isinstance(self.db, ClickHouseAdapter):
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
        row_indexer: Optional[Union[int, List[int], slice, list, str, tuple]] = None,
        col_indexer: Optional[Union[int, List[int], slice, list, str]] = None,
        index_column: Optional[str] = None,
        backend=None,
        data_id: str = None,
    ) -> Dict[str, Any]:
        try:
            if isinstance(self.db, PostgresAdapter) or isinstance(self.db, DuckDBAdapter) or isinstance(self.db, ClickHouseAdapter):
                qualified = self._qualified_table(table, schema)
                total_rows = int(
                    self._first_value_from_rows(await self._fetch(f"SELECT COUNT(*) FROM {qualified}"))
                )
                all_cols = await self._get_all_columns(table, schema)
                total_cols = len(all_cols)

                if col_indexer is not None:
                    col_pos = self._convert_iloc_indexer(col_indexer, total_cols, "column")
                else:
                    col_pos = list(range(total_cols))

                selected_col_names = [all_cols[i] for i in col_pos]
                quoted_cols = ", ".join(self._quote(c) for c in selected_col_names)

                # --- row selection: positional, raw-SQL WHERE, or label list ---
                row_where = None
                row_params: List[Any] = []
                if row_indexer is None:
                    row_pos = list(range(total_rows))
                elif isinstance(row_indexer, str):
                    row_where = row_indexer  # raw SQL WHERE condition
                elif (
                    isinstance(row_indexer, (list, tuple))
                    and index_column is not None
                    and len(row_indexer) > 0
                    and all(isinstance(x, str) for x in row_indexer)
                ):
                    placeholders = ", ".join(
                        self.db.placeholder(i + 1) for i in range(len(row_indexer))
                    )
                    row_where = f"{self._quote(index_column)} IN ({placeholders})"
                    row_params = list(row_indexer)
                else:
                    row_pos = self._convert_iloc_indexer(row_indexer, total_rows, "row")

                # --- WHERE-based path (raw SQL condition or label list) ---
                if row_where is not None:
                    sql = f"SELECT {quoted_cols} FROM {qualified} WHERE {row_where}"
                    if backend is not None and data_id is not None:
                        new_table = await self._resolve_transient_table_name(
                            "iloc_sel", backend, data_id
                        )
                        full_new = f"{self.db.quote_identifier('transient')}.{self._quote(new_table)}"
                        await self._exec(f"CREATE TABLE {full_new} AS {sql}", *row_params)
                        sample = await self._fetch_sample(
                            new_table, "transient", columns=selected_col_names
                        )
                        return self._success_response(
                            "iloc selection (filtered)",
                            sample,
                            new_table=new_table,
                            row_filter=row_where,
                        )
                    rows = await self._fetch(sql, *row_params)
                    df = pd.DataFrame([dict(r) for r in rows])
                    return self._success_response(
                        "iloc selection (read-only, filtered)", sample_df=df
                    )

                # --- positional path ---
                if len(row_pos) == 1 and len(col_pos) == 1:
                    row_idx = row_pos[0]
                    col_name = selected_col_names[0]
                    sql = f"""
                        SELECT {self._quote(col_name)}
                        FROM {qualified}
                        LIMIT 1 OFFSET {row_idx}
                    """
                    row = await self._fetch(sql)
                    if row:
                        scalar = self._first_value_from_rows(row)
                    else:
                        raise OperationError(f"Row index {row_idx} out of bounds")
                    return self._success_response(
                        f"iloc[{row_idx}, {col_pos[0]}]",
                        result=scalar,
                    )

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
                elif isinstance(self.db, ClickHouseAdapter):
                    idx_arr = "[" + ", ".join(map(str, row_pos_list)) + "]"
                    ord_arr = "[" + ", ".join(map(str, ord_list)) + "]"
                    join_clause = f"""
                    JOIN (
                        SELECT idx, ord
                        FROM (SELECT {idx_arr} AS idx_arr, {ord_arr} AS ord_arr)
                        ARRAY JOIN idx_arr AS idx, ord_arr AS ord
                    ) v ON t._rn = v.idx
                    ORDER BY v.ord
                    """
                else:
                    raise self._unsupported_backend_error()

                sql = f"""
                SELECT {quoted_cols}
                FROM (
                    SELECT {quoted_cols}, ROW_NUMBER() OVER () AS _rn
                    FROM {qualified}
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
                raise OperationError(f"{axis_name} index {indexer} out of bounds")
            return [indexer]
        if isinstance(indexer, slice):
            start, stop, step = indexer.indices(total_length)
            return list(range(start, stop, step))
        if isinstance(indexer, (list, tuple)):
            if all(isinstance(i, bool) for i in indexer):
                if len(indexer) != total_length:
                    raise OperationError(
                        f"Boolean indexer length ({len(indexer)}) must match {axis_name} length ({total_length})"
                    )
                return [i for i, val in enumerate(indexer) if val]
            result = []
            for i in indexer:
                if i < 0:
                    i += total_length
                if i < 0 or i >= total_length:
                    raise OperationError(f"{axis_name} index {i} out of bounds")
                result.append(i)
            return result
        raise OperationError(f"Unsupported indexer type: {type(indexer)}")

    async def _build_iloc_result(
        self,
        sql: str,
        selected_cols: List[str],
        row_pos: List[int],
        col_pos: List[int],
        backend,
        data_id: str,
    ) -> Dict[str, Any]:
        if isinstance(self.db, PostgresAdapter) or isinstance(self.db, DuckDBAdapter) or isinstance(self.db, ClickHouseAdapter):
            if backend and data_id:
                base_table_name = f"iloc_{len(row_pos)}x{len(col_pos)}"
                new_table = await self._resolve_transient_table_name(base_table_name, backend, data_id)
                full_new = f"{self.db.quote_identifier('transient')}.{self._quote(new_table)}"
                create_sql = f"CREATE TABLE {full_new} AS {sql}"
                await self._exec(create_sql)
                sample = await self._fetch_sample(new_table, "transient", columns=selected_cols)
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

