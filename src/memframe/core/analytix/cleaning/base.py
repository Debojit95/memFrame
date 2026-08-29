from typing import Any, Dict, List, Optional, Union
import traceback
from datetime import datetime, timezone
import pandas as pd

from memframe.db_manager.adapters.base import DatabaseAdapter
from memframe.utils.helper import SQLIdentifierSanitizer
from memframe.exceptions import OperationError

from memframe.core.ingestion.datatype_detector import DatatypeDetector
from memframe.core.analytix._response import fail, ok


class DataCleaningOps:
    """
    Shared data-cleaning infrastructure executed directly on the database.

    Backend-specific operation logic lives in the per-backend subclasses
    (duckdb.py / postgres.py / clickhouse.py); this base keeps only the
    helpers and the genuinely backend-agnostic dataframe_* operations.
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

    async def _fetchval(self, sql: str, *args):
        return await self.db.fetchval(sql, *args)

    async def _fetch_data(self, table: str, schema: str, columns: Any = "*", limit:int = -1) -> pd.DataFrame:
        """Return a DataFrame sample of the table for the response."""
        qualified = self._qualified_table(table, schema)

        if columns is None or (isinstance(columns, str) and columns.strip() == "*"):
            column_clause = "*"
        elif isinstance(columns, (list, tuple)):
            if not columns or (len(columns) == 1 and str(columns[0]).strip() == "*"):
                column_clause = "*"
            else:
                sanitized_cols = [
                    SQLIdentifierSanitizer.sanitize(str(col), allow_qualified=False)
                    for col in columns
                ]
                column_clause = ", ".join(self.db.quote_identifier(col) for col in sanitized_cols)
        else:
            safe_col = SQLIdentifierSanitizer.sanitize(str(columns), allow_qualified=False)
            column_clause = self.db.quote_identifier(safe_col)

        if limit > 0:
            rows = await self._fetch(f"SELECT {column_clause} FROM {qualified} LIMIT {limit}")
        else:
            rows = await self._fetch(f"SELECT {column_clause} FROM {qualified}")

        records = [dict(row) for row in rows]
        return pd.DataFrame.from_records(records)

    async def _get_column_type(self, table: str, schema: str, column: str) -> str:
        types = await self.db.get_column_types(table, schema)
        return types.get(column, "TEXT")

    async def _generate_transient_table_name(self, base_table: str, backend, data_id: str) -> str:
        max_op = await backend.fetchval(
            f"""
            SELECT COALESCE(MAX(opidx), 0)
            FROM {backend.transient_registry_table}
            WHERE data_id = {backend.placeholder(1)}
            """,
            data_id,
        )
        next_op = (max_op or 0) + 1
        safe_base = SQLIdentifierSanitizer.sanitize(base_table)
        return f"{safe_base}__op_{next_op}"

    async def _resolve_output_table_name(
        self,
        table: str,
        schema: str,
        backend=None,
        data_id: Optional[str] = None,
        new_table: Optional[str] = None,
    ) -> str:
        safe_table = SQLIdentifierSanitizer.sanitize(table)
        safe_schema = SQLIdentifierSanitizer.sanitize(schema)

        if new_table:
            candidate = SQLIdentifierSanitizer.sanitize(new_table)
        elif backend is not None and data_id:
            candidate = await self._generate_transient_table_name(safe_table, backend, data_id)
        else:
            candidate = f"{safe_table}__op_{datetime.now(timezone.utc).strftime('%Y%m%d%H%M%S%f')}"

        output_table = SQLIdentifierSanitizer.sanitize(candidate)
        dedupe_idx = 1
        while await self.db.table_exists(output_table, safe_schema):
            output_table = SQLIdentifierSanitizer.sanitize(f"{candidate}_{dedupe_idx}")
            dedupe_idx += 1

        return output_table

    async def _prepare_operation_table(
        self,
        table: str,
        schema: str,
        backend=None,
        data_id: Optional[str] = None,
        new_table: Optional[str] = None,
    ) -> str:
        safe_schema = SQLIdentifierSanitizer.sanitize(schema)
        source_table = SQLIdentifierSanitizer.sanitize(table)
        output_table = await self._resolve_output_table_name(
            source_table,
            safe_schema,
            backend=backend,
            data_id=data_id,
            new_table=new_table,
        )

        qualified_source = self._qualified_table(source_table, safe_schema)
        qualified_target = f'{self.db.quote_identifier(safe_schema)}.{self.db.quote_identifier(output_table)}'
        await self._exec(f"CREATE TABLE {qualified_target} AS SELECT * FROM {qualified_source}")
        return output_table

    async def _prepare_column_operation_table(
        self,
        table: str,
        schema: str,
        columns: List[str],
        backend=None,
        data_id: Optional[str] = None,
        new_table: Optional[str] = None,
    ) -> str:
        safe_schema = SQLIdentifierSanitizer.sanitize(schema)
        source_table = SQLIdentifierSanitizer.sanitize(table)
        output_table = await self._resolve_output_table_name(
            source_table,
            safe_schema,
            backend=backend,
            data_id=data_id,
            new_table=new_table,
        )

        sanitized_cols = []
        seen_cols = set()
        for col in columns:
            safe_col = SQLIdentifierSanitizer.sanitize(str(col), allow_qualified=False)
            if safe_col not in seen_cols:
                sanitized_cols.append(safe_col)
                seen_cols.add(safe_col)

        if not sanitized_cols:
            raise OperationError("At least one column is required for a column operation table")

        column_clause = ", ".join(self.db.quote_identifier(col) for col in sanitized_cols)
        qualified_source = self._qualified_table(source_table, safe_schema)
        qualified_target = f'{self.db.quote_identifier(safe_schema)}.{self.db.quote_identifier(output_table)}'
        await self._exec(f"CREATE TABLE {qualified_target} AS SELECT {column_clause} FROM {qualified_source}")
        return output_table

    async def _materialize_query_as_table(
        self,
        query: str,
        table: str,
        schema: str,
        backend=None,
        data_id: Optional[str] = None,
        new_table: Optional[str] = None,
    ) -> str:
        safe_schema = SQLIdentifierSanitizer.sanitize(schema)
        output_table = await self._resolve_output_table_name(
            table,
            safe_schema,
            backend=backend,
            data_id=data_id,
            new_table=new_table,
        )
        qualified_target = f'{self.db.quote_identifier(safe_schema)}.{self.db.quote_identifier(output_table)}'
        await self._exec(f"CREATE TABLE {qualified_target} AS {query}")
        return output_table

    def _qualified_table(self, table: str, schema: str) -> str:
        safe_table = SQLIdentifierSanitizer.sanitize(table)
        safe_schema = SQLIdentifierSanitizer.sanitize(schema)
        return f'{self.db.quote_identifier(safe_schema)}.{self.db.quote_identifier(safe_table)}'

    def _generate_cleaned_column_name(self, original: str, suffix: str = "") -> str:
        name = f"cleaned_{original}"
        candidate = f"{name}_{suffix}" if suffix else name
        return SQLIdentifierSanitizer.sanitize(candidate, allow_qualified=False)

    def _unsupported_backend_error(self) -> NotImplementedError:
        return NotImplementedError(
            f"Unsupported database backend for cleaning operation: {self.db.__class__.__name__}"
        )

    async def _add_new_column(self, table: str, schema: str, col_name: str, col_type: str):
        qualified = self._qualified_table(table, schema)
        safe_col = SQLIdentifierSanitizer.sanitize(col_name, allow_qualified=False)
        await self._exec(
            f"ALTER TABLE {qualified} ADD COLUMN {self.db.quote_identifier(safe_col)} {col_type}"
        )

    async def _add_new_column_if_not_exists(self, table: str, schema: str, col_name: str, col_type: str):
        safe_col = SQLIdentifierSanitizer.sanitize(col_name, allow_qualified=False)
        types = await self.db.get_column_types(table, schema)
        if safe_col not in types:
            await self._add_new_column(table, schema, col_name, col_type)

    async def _detect_numeric_target(self, qualified: str, cleaned_expr: str) -> str:
        """Pick a per-backend numeric SQL type from the cleaned column sample.

        Uses only the integer/float detectors; if the cleaned tokens are not
        uniformly numeric, fall back to the decimal type (current behavior).
        """
        rows = await self._fetch(
            f"SELECT {cleaned_expr} AS v FROM {qualified} "
            f"WHERE {cleaned_expr} IS NOT NULL LIMIT 5000"
        )
        values = [str(dict(row)["v"]) for row in rows]

        detector = DatatypeDetector()
        int_conf, int_sql = detector._detect_integer(values)
        float_conf = detector._detect_float(values)

        if int_conf == 1.0:
            return self._numeric_target_for(int_sql or "INTEGER")
        if float_conf == 1.0:
            return self._numeric_target_for("FLOAT")
        return self._numeric_target_for("TEXT")

    # ------------------------------------------------------------------
    # Generic dataframe operations (backend-agnostic)
    # ------------------------------------------------------------------
    async def dataframe_dropna(
        self,
        table: str,
        schema: str,
        axis: int = 0,
        how: str = "any",
        thresh: Optional[Union[int, float]] = None,
        backend=None,
        data_id: Optional[str] = None,
        new_table: Optional[str] = None,
    ) -> Dict[str, Any]:
        try:
            source_table = SQLIdentifierSanitizer.sanitize(table)
            qualified = self._qualified_table(source_table, schema)

            how = (how or "any").lower()

            if isinstance(thresh, float):
                total_rows = await self._fetchval(f"SELECT COUNT(*) FROM {qualified}") or 0
                max_nulls = int(total_rows * thresh)
                thresh = total_rows - max_nulls
                if thresh < 1:
                    thresh = 1

            if thresh is not None:
                if not isinstance(thresh, int) or thresh < 1:
                    return fail("thresh must be a positive integer")
                how = None
            elif how not in {"any", "all"}:
                return fail("Invalid 'how'. Use 'any' or 'all'")

            column_types = await self.db.get_column_types(source_table, schema)
            cols = list(column_types.keys())

            if not cols:
                return fail("No columns found")

            def valid_expr(col):
                return f'"{col}" IS NOT NULL AND "{col}" = "{col}"'

            if axis in (0, "index"):
                if thresh is not None:
                    non_null_expr = " + ".join([
                        f'CASE WHEN {valid_expr(c)} THEN 1 ELSE 0 END'
                        for c in cols
                    ])
                    where_clause = f"({non_null_expr}) >= {thresh}"
                elif how == "any":
                    where_clause = " AND ".join([valid_expr(c) for c in cols])
                else:
                    where_clause = " OR ".join([valid_expr(c) for c in cols])

                query = f"SELECT * FROM {qualified} WHERE {where_clause}"

            elif axis in (1, "columns"):
                selected_cols = []

                for col in cols:
                    safe_col = SQLIdentifierSanitizer.sanitize(col)
                    if thresh is not None:
                        non_null_count = await self._fetchval(
                            f'SELECT COUNT(*) FROM {qualified} WHERE {valid_expr(safe_col)}'
                        )
                        if non_null_count >= thresh:
                            selected_cols.append(f'"{safe_col}"')
                    elif how == "any":
                        null_count = await self._fetchval(
                            f'SELECT COUNT(*) FROM {qualified} WHERE NOT ({valid_expr(safe_col)})'
                        )
                        if null_count == 0:
                            selected_cols.append(f'"{safe_col}"')
                    else:
                        non_null_count = await self._fetchval(
                            f'SELECT COUNT(*) FROM {qualified} WHERE {valid_expr(safe_col)}'
                        )
                        if non_null_count > 0:
                            selected_cols.append(f'"{safe_col}"')

                if not selected_cols:
                    return ok(
                        message="All columns dropped (all contained missing values)",
                        involved_cols=cols,
                        generated_cols=[],
                        result=pd.DataFrame(),
                    )

                query = f'SELECT {", ".join(selected_cols)} FROM {qualified}'
            else:
                return fail("Invalid axis. Use 0/'index' or 1/'columns'")

            output_table = await self._materialize_query_as_table(
                query=query, table=source_table, schema=schema,
                backend=backend, data_id=data_id, new_table=new_table,
            )
            df = await self._fetch_data(output_table, schema)

            return ok(
                message=f"dropna applied (axis={axis}, how={how}, thresh={thresh})",
                involved_cols=cols, generated_cols=[], result=df, new_table=output_table,
            )

        except Exception as e:
            return fail(f"dataframe_dropna error: {str(e)}\n{traceback.format_exc()}")

    async def dataframe_drop(
        self,
        table: str,
        schema: str,
        axis: int = 0,
        index: Optional[List[int]] = None,
        columns: Optional[List[str]] = None,
        backend=None,
        data_id: Optional[str] = None,
        new_table: Optional[str] = None,
    ) -> Dict[str, Any]:
        try:
            source_table = SQLIdentifierSanitizer.sanitize(table)
            qualified = self._qualified_table(source_table, schema)

            if axis in (0, "index"):
                if not index:
                    return fail("index must be provided when axis=0")
            elif axis in (1, "columns"):
                if not columns:
                    return fail("columns must be provided when axis=1")
            else:
                return fail("Invalid axis. Use 0 or 1")

            if axis in (1, "columns"):
                column_types = await self.db.get_column_types(source_table, schema)
                existing_cols = list(column_types.keys())

                drop_cols = set([SQLIdentifierSanitizer.sanitize(c) for c in columns])
                missing = drop_cols - set(existing_cols)
                if missing:
                    return fail(f"Columns not found: {missing}")

                remaining_cols = [c for c in existing_cols if c not in drop_cols]
                if not remaining_cols:
                    return fail("Cannot drop all columns")

                select_clause = ", ".join([f'"{c}"' for c in remaining_cols])
                query = f"SELECT {select_clause} FROM {qualified}"
            else:
                index_list = list(set(index))
                index_list_str = ", ".join([str(i) for i in index_list])

                query = f"""
                    SELECT * FROM (
                        SELECT *, ROW_NUMBER() OVER () - 1 AS __rn__
                        FROM {qualified}
                    ) t
                    WHERE __rn__ NOT IN ({index_list_str})
                """

            output_table = await self._materialize_query_as_table(
                query=query, table=source_table, schema=schema,
                backend=backend, data_id=data_id, new_table=new_table,
            )
            df = await self._fetch_data(output_table, schema)

            return ok(
                message=f"drop applied (axis={axis})",
                involved_cols=columns or [], generated_cols=[], result=df, new_table=output_table,
            )

        except Exception as e:
            return fail(f"dataframe_drop error: {str(e)}\n{traceback.format_exc()}")

    async def dataframe_isna(
        self,
        table: str,
        schema: str,
        backend=None,
        data_id: Optional[str] = None,
        new_table: Optional[str] = None,
    ) -> Dict[str, Any]:
        try:
            source_table = SQLIdentifierSanitizer.sanitize(table)
            qualified = self._qualified_table(source_table, schema)

            column_types = await self.db.get_column_types(source_table, schema)
            cols = list(column_types.keys())

            if not cols:
                return fail("No columns found")

            select_parts = [f'"{col}" IS NULL AS "{col}"' for col in cols]
            query = f"""
                SELECT {', '.join(select_parts)}
                FROM {qualified}
            """

            output_table = await self._materialize_query_as_table(
                query=query, table=source_table, schema=schema,
                backend=backend, data_id=data_id, new_table=new_table,
            )
            df = await self._fetch_data(output_table, schema)

            return ok(
                message="Generated NA mask (isna)",
                involved_cols=cols, generated_cols=cols, result=df, new_table=output_table,
            )

        except Exception as e:
            return fail(f"dataframe_isna error: {str(e)}\n{traceback.format_exc()}")

    async def dataframe_notna(
        self,
        table: str,
        schema: str,
        backend=None,
        data_id: Optional[str] = None,
        new_table: Optional[str] = None,
    ) -> Dict[str, Any]:
        try:
            source_table = SQLIdentifierSanitizer.sanitize(table)
            qualified = self._qualified_table(source_table, schema)

            column_types = await self.db.get_column_types(source_table, schema)
            cols = list(column_types.keys())

            if not cols:
                return fail("No columns found")

            select_parts = [f'"{col}" IS NOT NULL AS "{col}"' for col in cols]
            query = f"""
                SELECT {', '.join(select_parts)}
                FROM {qualified}
            """

            output_table = await self._materialize_query_as_table(
                query=query, table=source_table, schema=schema,
                backend=backend, data_id=data_id, new_table=new_table,
            )
            df = await self._fetch_data(output_table, schema)

            return ok(
                message="Generated NA mask (notna)",
                involved_cols=cols, generated_cols=cols, result=df, new_table=output_table,
            )

        except Exception as e:
            return fail(f"dataframe_notna error: {str(e)}\n{traceback.format_exc()}")

    async def dataframe_drop_duplicates(
        self,
        table: str,
        schema: str,
        subset: Optional[List[str]] = None,
        keep: Union[str, bool] = "first",
        backend=None,
        data_id: Optional[str] = None,
        new_table: Optional[str] = None,
    ) -> Dict[str, Any]:
        try:
            source_table = SQLIdentifierSanitizer.sanitize(table)
            qualified = self._qualified_table(source_table, schema)

            column_types = await self.db.get_column_types(source_table, schema)
            all_cols = list(column_types.keys())

            if not all_cols:
                return fail("No columns found")

            if subset:
                subset = [SQLIdentifierSanitizer.sanitize(c) for c in subset]
                missing = set(subset) - set(all_cols)
                if missing:
                    return fail(f"Columns not found: {missing}")
                partition_cols = subset
            else:
                partition_cols = all_cols

            partition_expr = ", ".join([f'"{c}"' for c in partition_cols])
            select_cols = ", ".join([f'"{c}"' for c in all_cols])

            base_query = f"""
                SELECT *,
                    ROW_NUMBER() OVER () AS __row_id__
                FROM {qualified}
            """

            if keep == "first":
                query = f"""
                    SELECT {select_cols} FROM (
                        SELECT *,
                            ROW_NUMBER() OVER (
                                PARTITION BY {partition_expr}
                                ORDER BY __row_id__ ASC
                            ) AS rn
                        FROM ({base_query}) t
                    ) x
                    WHERE rn = 1
                """
            elif keep == "last":
                query = f"""
                    SELECT {select_cols} FROM (
                        SELECT *,
                            ROW_NUMBER() OVER (
                                PARTITION BY {partition_expr}
                                ORDER BY __row_id__ DESC
                            ) AS rn
                        FROM ({base_query}) t
                    ) x
                    WHERE rn = 1
                """
            elif keep is False:
                query = f"""
                    SELECT {select_cols} FROM (
                        SELECT *,
                            COUNT(*) OVER (
                                PARTITION BY {partition_expr}
                            ) AS cnt
                        FROM ({base_query}) t
                    ) x
                    WHERE cnt = 1
                """
            else:
                return fail("Invalid 'keep'. Use 'first', 'last', or False")

            output_table = await self._materialize_query_as_table(
                query=query, table=source_table, schema=schema,
                backend=backend, data_id=data_id, new_table=new_table,
            )
            df = await self._fetch_data(output_table, schema)

            return ok(
                message=f"drop_duplicates applied (subset={subset}, keep={keep})",
                involved_cols=partition_cols, generated_cols=[], result=df, new_table=output_table,
            )

        except Exception as e:
            return fail(f"dataframe_drop_duplicates error: {str(e)}\n{traceback.format_exc()}")
