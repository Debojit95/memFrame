"""
ClickHouse selection operations.

Self-contained copy of all selection methods plus the ClickHouse hooks
(system.columns introspection, ISO-string asof serialization, and the
ARRAY JOIN positional clause).
"""

from typing import Any, Dict, List, Optional, Tuple, Union
from datetime import date, datetime
import traceback
import pandas as pd

from memframe.utils.helper import SQLIdentifierSanitizer
from memframe.exceptions import DataNotFound, OperationError
from memframe.core.analytix.selection.base import DataSelectionOps


class ClickHouseSelectionOps(DataSelectionOps):
    # ------------------------------------------------------------------
    # Backend hooks (ClickHouse)
    # ------------------------------------------------------------------
    def _list_columns_query(self, qualified: str, schema: str, table: str) -> Tuple[str, List[Any]]:
        ph = self.db.placeholder
        return (
            f"SELECT name FROM system.columns "
            f"WHERE database = {ph(1)} AND table = {ph(2)}",
            [schema, table],
        )

    def _list_column_types_query(self, qualified: str, schema: str, table: str) -> Tuple[str, List[Any]]:
        ph = self.db.placeholder
        return (
            f"SELECT name, type FROM system.columns "
            f"WHERE database = {ph(1)} AND table = {ph(2)}",
            [schema, table],
        )

    def _column_name(self, row: Any) -> str:
        return self._row_get(row, "name", 0)

    def _column_type(self, row: Any) -> str:
        return self._row_get(row, "type", 1)

    def _serialize_asof_where(self, normalized: Any) -> Any:
        if isinstance(normalized, datetime):
            return str(pd.Timestamp(normalized))
        if isinstance(normalized, date):
            return str(normalized)
        return normalized

    def _iloc_join_clause(self, row_pos_list: List[int], ord_list: List[int]) -> str:
        idx_arr = "[" + ", ".join(map(str, row_pos_list)) + "]"
        ord_arr = "[" + ", ".join(map(str, ord_list)) + "]"
        return f"""
        JOIN (
            SELECT idx, ord
            FROM (SELECT {idx_arr} AS idx_arr, {ord_arr} AS ord_arr)
            ARRAY JOIN idx_arr AS idx, ord_arr AS ord
        ) v ON t._rn = v.idx
        ORDER BY v.ord
        """

    # ------------------------------------------------------------------
    # Shared column introspection
    # ------------------------------------------------------------------
    async def _get_all_columns(self, table: str, schema: str) -> List[str]:
        qualified = self._qualified_table(table, schema)
        sql, params = self._list_columns_query(qualified, schema, table)
        cols = await self._fetch(sql, *params)
        return [self._column_name(c) for c in cols]

    async def _get_column_types(self, table: str, schema: str) -> Dict[str, str]:
        qualified = self._qualified_table(table, schema)
        sql, params = self._list_column_types_query(qualified, schema, table)
        rows = await self._fetch(sql, *params)
        return {self._column_name(row): self._column_type(row) for row in rows}

    def _normalize_asof_where_value(self, value: Any, column_kind: str) -> Any:
        normalized = self._normalize_asof_value(value, column_kind)
        return self._serialize_asof_where(normalized)

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
        except IndexError as ie:
            return self._error_response(str(ie))
        except Exception as e:
            return self._error_response(f"iat error: {str(e)}\n{traceback.format_exc()}")

    async def get(
        self,
        table: str,
        schema: str,
        keys: Union[str, List[str]],
        default: Any = None,
    ) -> Dict[str, Any]:
        try:
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
        except Exception as e:
            return self._error_response(f"get error: {str(e)}\n{traceback.format_exc()}")

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
        except Exception as e:
            return self._error_response(f"select_dtypes error: {str(e)}\n{traceback.format_exc()}")

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
                # ponytail: documented raw-SQL WHERE escape hatch; block
                # statement chaining (comments stay allowed, they can't escalate)
                cond = row_indexer.strip().rstrip(";")
                if ";" in cond or not cond:
                    return self._error_response(
                        "row_indexer string cannot contain ';' "
                        "(multi-statement SQL is not allowed)"
                    )
                row_where = cond
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
                    full_new = f"{self.db.quote_identifier(backend.transient_schema)}.{self._quote(new_table)}"
                    await self._exec(f"CREATE TABLE {full_new} AS {sql}", *row_params)
                    sample = await self._fetch_sample(
                        new_table, backend.transient_schema, columns=selected_col_names
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

            row_pos_list = [p + 1 for p in row_pos]   # 1-based
            ord_list = list(range(1, len(row_pos_list) + 1))

            join_clause = self._iloc_join_clause(row_pos_list, ord_list)

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
        if backend and data_id:
            base_table_name = f"iloc_{len(row_pos)}x{len(col_pos)}"
            new_table = await self._resolve_transient_table_name(base_table_name, backend, data_id)
            full_new = f"{self.db.quote_identifier(backend.transient_schema)}.{self._quote(new_table)}"
            create_sql = f"CREATE TABLE {full_new} AS {sql}"
            await self._exec(create_sql)
            sample = await self._fetch_sample(new_table, backend.transient_schema, columns=selected_cols)
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
                f"iloc rows {row_pos} cols {col_pos} (read-only)",
                sample_df=df,
                row_indices=row_pos,
                col_indices=col_pos,
            )
