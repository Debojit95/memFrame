from typing import Dict, List, Any, Optional
import traceback
import pandas as pd
from collections import namedtuple
from datetime import datetime


from memframe.db_manager.adapters.base import DatabaseAdapter
from memframe.db_manager.adapters.postgresql import PostgresAdapter
from memframe.db_manager.adapters.duckdb import DuckDBAdapter
from memframe.db_manager.adapters.clickhouse import ClickHouseAdapter
from memframe.utils.helper import SQLIdentifierSanitizer

class GeneralTableOps:
    """
    DataFrame operations engine following pandas functionality.
    All database access is delegated to the provided DatabaseAdapter.
    """

    def __init__(self, db_adapter: DatabaseAdapter):
        self.db = db_adapter


    # ------------------------------------------------------------------
    # Internal helpers that delegate to the adapter
    # ------------------------------------------------------------------
    async def _exec(self, sql: str, *args):
        return await self.db.execute(sql, *args)

    async def _fetch(self, sql: str, *args):
        return await self.db.fetch(sql, *args)

    async def _fetchval(self, sql: str, *args):
        return await self.db.fetchval(sql, *args)

    async def _fetchrow(self, sql: str, *args):
        return await self.db.fetchrow(sql, *args)

    async def _get_column_types(self, table: str, schema: str) -> Dict[str, str]:
        return await self.db.get_column_types(table, schema)

    async def _get_table_info(self, table: str, schema: str) -> Dict[str, Any]:
        return await self.db.get_table_info(table, schema)

    def _qualified_table(self, table: str, schema: str) -> str:
        safe_table = SQLIdentifierSanitizer.sanitize(table)
        safe_schema = SQLIdentifierSanitizer.sanitize(schema)
        return f'{self.db.quote_identifier(safe_schema)}.{self.db.quote_identifier(safe_table)}'

    # ------------------------------------------------------------------
    # Response builders 
    # ------------------------------------------------------------------
    def _success_response(
        self,
        message: str = "",
        involved_cols: Optional[List[str]] = None,
        generated_cols: Optional[List[str]] = None,
        result: Any = None,
        current_state: Optional[pd.DataFrame] = None,
        **extra: Any,
    ) -> Dict[str, Any]:
        response = {
            "is_error": False,
            "message": message,
            "error_message": None,
            "involved_cols": involved_cols or [],
            "generated_cols": generated_cols or [],
        }
        if result is not None:
            response["result"] = result
        if current_state is not None:
            response["current_state"] = current_state
        response.update(extra)
        return response

    def _error_response(
        self,
        error_message: str,
        involved_cols: Optional[List[str]] = None,
        generated_cols: Optional[List[str]] = None,
        result: Any = None,
        **extra: Any,
    ) -> Dict[str, Any]:
        response = {
            "is_error": True,
            "message": "",
            "error_message": error_message,
            "involved_cols": involved_cols or [],
            "generated_cols": generated_cols or [],
        }
        if result is not None:
            response["result"] = result
        response.update(extra)
        return response

    def _unsupported_backend_error(self) -> NotImplementedError:
        return NotImplementedError(
            f"Unsupported database backend for table operation: {self.db.__class__.__name__}"
        )

    def _rows_to_records(self, rows: List[Any]) -> List[Dict[str, Any]]:
        return [dict(row) for row in rows]

    def _records_to_dataframe(self, records: List[Dict[str, Any]], table: Optional[str] = None, schema: Optional[str] = None) -> pd.DataFrame:
        if table and schema:
            records = self._normalize_records(records, table, schema)
        return pd.DataFrame.from_records(records)

    def _normalize_records(self, records: List[Dict[str, Any]], table: str, schema: str) -> List[Dict[str, Any]]:
        """Normalize datetime values based on column types.
        For DATE columns, convert datetime to date (strip time component).
        For TIMESTAMP columns, keep as datetime.
        """
        if not records:
            return records
        
        # This is a sync method - we need column types from a sync context
        # We'll do a best-effort normalization based on value types
        # For true type-based normalization, use the async version below
        normalized = []
        for record in records:
            new_record = {}
            for col, val in record.items():
                if val is not None and hasattr(val, 'date') and not hasattr(val, 'time'):
                    # It's a date object already
                    new_record[col] = val
                elif val is not None and hasattr(val, 'date'):
                    # It's a datetime - check if time component is 00:00:00
                    # We'll convert to date if it looks like a DATE column (midnight timestamp)
                    if val.hour == 0 and val.minute == 0 and val.second == 0 and val.microsecond == 0:
                        new_record[col] = val.date()
                    else:
                        new_record[col] = val
                else:
                    new_record[col] = val
            normalized.append(new_record)
        return normalized

    async def _normalize_records_by_type(self, records: List[Dict[str, Any]], table: str, schema: str) -> List[Dict[str, Any]]:
        """Normalize datetime values based on actual database column types.
        For DATE columns, convert datetime to date (strip time component).
        """
        if not records:
            return records
        
        column_types = await self._get_column_types(table, schema)
        # Identify DATE columns (not TIMESTAMP)
        date_columns = {
            col for col, dtype in column_types.items() 
            if dtype.lower() in ('date', 'date32')
        }
        
        if not date_columns:
            return records
        
        normalized = []
        for record in records:
            new_record = {}
            for col, val in record.items():
                if col in date_columns and val is not None and hasattr(val, 'date'):
                    new_record[col] = val.date()
                else:
                    new_record[col] = val
            normalized.append(new_record)
        return normalized

    async def _get_table_dataframe(
        self, table: str, schema: str, columns: Optional[List[str]] = None
    ) -> pd.DataFrame:
        table = SQLIdentifierSanitizer.sanitize(table)
        qualified = self._qualified_table(table, schema)
        column_types = await self._get_column_types(table, schema)

        if columns:
            selected_columns = [col for col in columns if col in column_types]
        else:
            selected_columns = list(column_types.keys())

        if selected_columns:
            sanitized = [SQLIdentifierSanitizer.sanitize(col) for col in selected_columns]
            column_clause = ", ".join([f'"{col}"' for col in sanitized])
        else:
            column_clause = "*"

        rows = await self._fetch(f"SELECT {column_clause} FROM {qualified}")
        records = self._rows_to_records(rows)
        records = await self._normalize_records_by_type(records, table, schema)
        return pd.DataFrame.from_records(records)

    async def _generate_transient_table_name(self, base_table: str, backend, data_id: str) -> str:
        fetch_scalar = (
            backend.fetch_val
            if hasattr(backend, "fetch_val")
            else backend.fetchval
        )
        transient_registry_table = getattr(
            backend, "transient_registry_table", "registry.transient_registry"
        )

        max_op = await fetch_scalar(
            f"""
            SELECT COALESCE(MAX(opidx), 0)
            FROM {transient_registry_table}
            WHERE data_id = {backend.placeholder(1)}
            """,
            data_id,
        )
        next_op = (max_op or 0) + 1
        safe_base = SQLIdentifierSanitizer.sanitize(base_table)
        return f"{safe_base}__op_{next_op}"

    def _sql_type_for_series(self, series: pd.Series) -> str:
        dtype = series.dtype
        if isinstance(self.db, ClickHouseAdapter):
            if pd.api.types.is_bool_dtype(dtype):
                return "UInt8"
            if pd.api.types.is_integer_dtype(dtype):
                return "Int64"
            if pd.api.types.is_float_dtype(dtype):
                return "Float64"
            if pd.api.types.is_datetime64_any_dtype(dtype):
                return "DateTime"
            return "String"

        if pd.api.types.is_bool_dtype(dtype):
            return "BOOLEAN"
        if pd.api.types.is_integer_dtype(dtype):
            return "BIGINT"
        if pd.api.types.is_float_dtype(dtype):
            return "DOUBLE PRECISION"
        if pd.api.types.is_datetime64_any_dtype(dtype):
            return "TIMESTAMP"
        return "TEXT"

    def _normalize_cell_value(self, value: Any) -> Any:
        if pd.isna(value):
            return None
        if isinstance(value, pd.Timestamp):
            return value.to_pydatetime()
        if isinstance(value, pd.Timedelta):
            return str(value)
        if hasattr(value, "item"):
            try:
                return value.item()
            except Exception:
                return value
        return value

    async def _save_dataframe_as_table(
        self,
        df: pd.DataFrame,
        schema: str,
        base_table: str,
        backend=None,
        data_id: Optional[str] = None,
        new_table: Optional[str] = None,
    ) -> str:
        df_to_store = df.copy()

        if not isinstance(df_to_store.index, pd.RangeIndex):
            idx_name = df_to_store.index.name or "index"
            df_to_store = df_to_store.reset_index().rename(columns={"index": idx_name})

        if df_to_store.columns.empty:
            df_to_store = pd.DataFrame({"value": []})

        rename_map: Dict[str, str] = {}
        used_cols = set()
        for col in df_to_store.columns:
            base_col = SQLIdentifierSanitizer.sanitize(str(col))
            if not base_col:
                base_col = "col"
            safe_col = base_col
            suffix = 1
            while safe_col in used_cols:
                safe_col = f"{base_col}_{suffix}"
                suffix += 1
            rename_map[col] = safe_col
            used_cols.add(safe_col)
        df_to_store = df_to_store.rename(columns=rename_map)

        safe_schema = SQLIdentifierSanitizer.sanitize(schema)
        if new_table:
            candidate = SQLIdentifierSanitizer.sanitize(new_table)
        elif backend is not None and data_id:
            candidate = await self._generate_transient_table_name(base_table, backend, data_id)
        else:
            safe_base = SQLIdentifierSanitizer.sanitize(base_table)
            ts = datetime.utcnow().strftime("%Y%m%d%H%M%S%f")
            candidate = f"{safe_base}__op_{ts}"

        table_name = SQLIdentifierSanitizer.sanitize(candidate)
        dedupe_idx = 1
        while await self.db.table_exists(table_name, safe_schema):
            table_name = SQLIdentifierSanitizer.sanitize(f"{candidate}_{dedupe_idx}")
            dedupe_idx += 1

        quoted_schema = self.db.quote_identifier(safe_schema)
        quoted_table = self.db.quote_identifier(table_name)
        qualified_new = f"{quoted_schema}.{quoted_table}"

        col_defs = []
        for col in df_to_store.columns:
            sql_type = self._sql_type_for_series(df_to_store[col])
            col_defs.append(f"{self.db.quote_identifier(col)} {sql_type}")
        create_sql = f"CREATE TABLE {qualified_new} ({', '.join(col_defs)})"

        # ClickHouse requires an ENGINE for CREATE TABLE
        if isinstance(self.db, ClickHouseAdapter):
            create_sql += " ENGINE = MergeTree() ORDER BY tuple()"

        await self._exec(create_sql)

        if not df_to_store.empty:
            quoted_cols = ", ".join(self.db.quote_identifier(c) for c in df_to_store.columns)
            rows = [
                [self._normalize_cell_value(v) for v in row]
                for row in df_to_store.itertuples(index=False, name=None)
            ]

            if isinstance(self.db, ClickHouseAdapter):
                await self.db.insert_rows(
                    qualified_new,
                    rows,
                    list(df_to_store.columns),
                )
            else:
                placeholders = ", ".join(self.db.placeholder(i + 1) for i in range(len(df_to_store.columns)))
                insert_sql = f"INSERT INTO {qualified_new} ({quoted_cols}) VALUES ({placeholders})"

                for values in rows:
                    await self._exec(insert_sql, *values)

        return table_name

    # ------------------------------------------------------------------
    # Dispatcher 
    # ------------------------------------------------------------------
    async def execute_operation(self, method_name: str, params: Dict[str, Any]) -> Dict[str, Any]:
        try:
            if not hasattr(self, method_name):
                return self._error_response(f"Unknown DataFrame method '{method_name}'.")
            method = getattr(self, method_name)
            return await method(**params)
        except Exception as e:
            return self._error_response(f"Dispatcher error: {str(e)}\n{traceback.format_exc()}")

    # ------------------------------------------------------------------
    # DataFrame operations 
    # ------------------------------------------------------------------
    async def dataframe_head(self, table: str, schema: str, n: int = 10, columns: Optional[List[str]] = None,**kwargs,) -> Dict[str, Any]:
        try:
            if isinstance(self.db, PostgresAdapter) or isinstance(self.db, DuckDBAdapter) or isinstance(self.db, ClickHouseAdapter):
                table = SQLIdentifierSanitizer.sanitize(table)
                qualified = self._qualified_table(table, schema)
                column_types = await self._get_column_types(table, schema)

                if columns:
                    selected = [c for c in columns if c in column_types]
                    if not selected:
                        selected = list(column_types.keys())
                else:
                    selected = list(column_types.keys())

                if selected:
                    sanitized = [SQLIdentifierSanitizer.sanitize(c) for c in selected]
                    column_clause = ", ".join([f'"{c}"' for c in sanitized])
                else:
                    column_clause = "*"
                    selected = list(column_types.keys())

                rows = await self._fetch(f"SELECT {column_clause} FROM {qualified} LIMIT {n}")
                records = self._rows_to_records(rows)
                records = await self._normalize_records_by_type(records, table, schema)
                df = pd.DataFrame.from_records(records)

                msg = f"Returned first {n} rows from '{table}'"
                if selected and len(selected) < 20:
                    msg += f" (columns: {', '.join(selected)})"

                return self._success_response(
                    involved_cols=selected,
                    message=msg,
                    result=df,
                    result_metadata={"row_count": len(records), "selected_columns": selected},
                )
            else:
                raise self._unsupported_backend_error()
        except Exception as e:
            return self._error_response(f"dataframe_head error: {str(e)}\n{traceback.format_exc()}")

    async def dataframe_tail(self, table: str, schema: str, n: int = 10, columns: Optional[List[str]] = None, **kwargs) -> Dict[str, Any]:
        try:
            if isinstance(self.db, PostgresAdapter) or isinstance(self.db, DuckDBAdapter) or isinstance(self.db, ClickHouseAdapter):
                table = SQLIdentifierSanitizer.sanitize(table)
                qualified = self._qualified_table(table, schema)

                total_rows = await self._fetchval(f"SELECT COUNT(*) FROM {qualified}") or 0
                offset = max(0, total_rows - n)

                column_types = await self._get_column_types(table, schema)

                if columns:
                    selected = [c for c in columns if c in column_types]
                    if not selected:
                        selected = list(column_types.keys())
                else:
                    selected = list(column_types.keys())

                if selected:
                    sanitized = [SQLIdentifierSanitizer.sanitize(c) for c in selected]
                    column_clause = ", ".join([f'"{c}"' for c in sanitized])
                else:
                    column_clause = "*"
                    selected = list(column_types.keys())

                # ───────────────────────────────────────────────
                # ClickHouse: LIMIT must come BEFORE OFFSET
                # Postgres/DuckDB: OFFSET before LIMIT (also works)
                # ───────────────────────────────────────────────
                if isinstance(self.db, ClickHouseAdapter):
                    query = f"SELECT {column_clause} FROM {qualified} LIMIT {n} OFFSET {offset}"
                else:
                    query = f"SELECT {column_clause} FROM {qualified} OFFSET {offset} LIMIT {n}"

                rows = await self._fetch(query)
                records = self._rows_to_records(rows)
                records = await self._normalize_records_by_type(records, table, schema)

                return self._success_response(
                    involved_cols=selected,
                    message=f"Returned last {n} rows from '{table}'",
                    result=pd.DataFrame.from_records(records),
                    result_metadata={"row_count": len(records), "total_rows": total_rows},
                )
            else:
                raise self._unsupported_backend_error()
        except Exception as e:
            return self._error_response(f"dataframe_tail error: {str(e)}\n{traceback.format_exc()}")
    
    async def dataframe_sample(self, table: str, schema: str, n: int = 10, columns: Optional[List[str]] = None, random_state: Optional[int] = None,**kwargs,) -> Dict[str, Any]:

        try:
            if isinstance(self.db, PostgresAdapter) or isinstance(self.db, DuckDBAdapter) or isinstance(self.db, ClickHouseAdapter):
                table = SQLIdentifierSanitizer.sanitize(table)
                qualified = self._qualified_table(table, schema)

                if random_state is not None:
                    if isinstance(self.db, PostgresAdapter):
                        await self._exec(f"SELECT setseed({random_state})")
                    elif isinstance(self.db, DuckDBAdapter):
                        random_state = random_state
                    elif isinstance(self.db, ClickHouseAdapter):
                        # ClickHouse does not support setseed; rand() is non-deterministic
                        pass

                column_types = await self._get_column_types(table, schema)

                if columns:
                    selected = [c for c in columns if c in column_types]
                    if not selected:
                        selected = list(column_types.keys())
                else:
                    selected = list(column_types.keys())

                if selected:
                    sanitized = [SQLIdentifierSanitizer.sanitize(c) for c in selected]
                    column_clause = ", ".join([f'"{c}"' for c in sanitized])
                else:
                    column_clause = "*"
                    selected = list(column_types.keys())

                if isinstance(self.db, ClickHouseAdapter):
                    query = f"""
                        SELECT {column_clause}
                        FROM {qualified}
                        ORDER BY rand()
                        LIMIT {n}
                    """
                else:
                    query = f"""
                        SELECT {column_clause}
                        FROM {qualified}
                        ORDER BY RANDOM()
                        LIMIT {n}
                    """
                rows = await self._fetch(query)
                records = self._rows_to_records(rows)
                records = await self._normalize_records_by_type(records, table, schema)

                msg = f"Returned {n} random samples from '{table}'"
                if random_state is not None:
                    msg += f" (random_state={random_state})"

                return self._success_response(
                    involved_cols=selected,
                    message=msg,
                    result=pd.DataFrame.from_records(records),
                    result_metadata={"row_count": len(records), "sample_size": n},
                )
            else:
                raise self._unsupported_backend_error()
        except Exception as e:
            return self._error_response(f"dataframe_sample error: {str(e)}\n{traceback.format_exc()}")

    async def dataframe_info(self, table: str, schema: str, columns: Optional[List[str]] = None, **kwargs) -> Dict[str, Any]:
        try:
            if isinstance(self.db, PostgresAdapter) or isinstance(self.db, DuckDBAdapter) or isinstance(self.db, ClickHouseAdapter):
                backend = kwargs.get("backend")
                data_id = kwargs.get("data_id")
                requested_new_table = kwargs.get("new_table")
                table = SQLIdentifierSanitizer.sanitize(table)
                qualified = self._qualified_table(table, schema)

                table_info = await self._get_table_info(table, schema)
                if "error" in table_info:
                    return self._error_response(table_info["error"])

                column_details = []

                for col_name, data_type in table_info["columns"].items():
                    null_count = await self._fetchval(
                        f'SELECT COUNT(*) FROM {qualified} WHERE "{col_name}" IS NULL'
                    ) or 0

                    distinct_count = await self._fetchval(
                        f'SELECT COUNT(DISTINCT "{col_name}") FROM {qualified}'
                    ) or 0

                    column_details.append({
                        "column_name": col_name,
                        "data_type": data_type,
                        "null_count": null_count,
                        "non_null_count": table_info["row_count"] - null_count,
                        "null_percentage": (
                            (null_count / table_info["row_count"]) * 100
                            if table_info["row_count"] > 0 else 0
                        ),
                        "distinct_count": distinct_count,
                    })

                df = self._records_to_dataframe(column_details)
                output_table = await self._save_dataframe_as_table(
                    df=df,
                    schema=schema,
                    base_table=f"{table}_info",
                    backend=backend,
                    data_id=data_id,
                    new_table=requested_new_table,
                )

                msg = f"Table '{table}' info: {table_info['row_count']} rows × {table_info['column_count']} columns"

                return self._success_response(
                    involved_cols=list(table_info["columns"].keys()),
                    generated_cols=list(df.columns),
                    message=msg,
                    result=df,
                    new_table=output_table,
                    result_metadata={
                        "row_count": len(column_details),
                        "saved_table": output_table,
                        "table_info": table_info,
                        "memory_usage": {
                            "total_size": table_info["total_size"],
                            "table_size": table_info["table_size"],
                        },
                    },
                )

            else:
                raise self._unsupported_backend_error()
        except Exception as e:
            return self._error_response(
                f"dataframe_info error: {str(e)}\n{traceback.format_exc()}"
            )

    async def dataframe_describe(self, table: str, schema: str, columns: Optional[List[str]] = None, **kwargs) -> Dict[str, Any]:

        try:
            if isinstance(self.db, PostgresAdapter) or isinstance(self.db, DuckDBAdapter) or isinstance(self.db, ClickHouseAdapter):
                backend = kwargs.get("backend")
                data_id = kwargs.get("data_id")
                requested_new_table = kwargs.get("new_table")
                table = SQLIdentifierSanitizer.sanitize(table)
                qualified = self._qualified_table(table, schema)

                column_types = await self._get_column_types(table, schema)

                numeric_types = [
                    "integer", "bigint", "smallint", "decimal", "numeric",
                    "real", "double", "double precision", "float", "float8", "float4",
                    "int", "uint"
                ]

                if not columns:
                    columns = [
                        col for col, dtype in column_types.items()
                        if any(nt in dtype.lower() for nt in numeric_types)
                    ]

                if not columns:
                    return self._error_response("No numeric columns found for descriptive statistics")

                sanitized = [SQLIdentifierSanitizer.sanitize(c) for c in columns]
                column_stats = {}

                for col in sanitized:
                    if isinstance(self.db, PostgresAdapter):
                        q25_expr = f'PERCENTILE_CONT(0.25) WITHIN GROUP (ORDER BY "{col}")'
                        median_expr = f'PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY "{col}")'
                        q75_expr = f'PERCENTILE_CONT(0.75) WITHIN GROUP (ORDER BY "{col}")'
                        std_expr = f'STDDEV_SAMP("{col}")'
                    elif isinstance(self.db, DuckDBAdapter):
                        q25_expr = f'QUANTILE_CONT("{col}", 0.25)'
                        median_expr = f'QUANTILE_CONT("{col}", 0.5)'
                        q75_expr = f'QUANTILE_CONT("{col}", 0.75)'
                        std_expr = f'STDDEV_SAMP("{col}")'
                    elif isinstance(self.db, ClickHouseAdapter):
                        q25_expr = f'quantile(0.25)("{col}")'
                        median_expr = f'quantile(0.5)("{col}")'
                        q75_expr = f'quantile(0.75)("{col}")'
                        std_expr = f'stddevSamp("{col}")'
                    else:
                        raise self._unsupported_backend_error()

                    stats_sql = f"""
                        SELECT
                            COUNT("{col}") as count,
                            AVG("{col}") as mean,
                            {std_expr} as std,
                            MIN("{col}") as min,
                            {q25_expr} as q25,
                            {median_expr} as median,
                            {q75_expr} as q75,
                            MAX("{col}") as max
                        FROM {qualified}
                        WHERE "{col}" IS NOT NULL
                    """
                    row = await self._fetchrow(stats_sql)

                    if row:
                        column_stats[col] = {
                            "count": float(row["count"]) if row["count"] else 0,
                            "mean": float(row["mean"]) if row["mean"] is not None else None,
                            "std": float(row["std"]) if row["std"] is not None else None,
                            "min": float(row["min"]) if row["min"] is not None else None,
                            "25%": float(row["q25"]) if row["q25"] is not None else None,
                            "50%": float(row["median"]) if row["median"] is not None else None,
                            "75%": float(row["q75"]) if row["q75"] is not None else None,
                            "max": float(row["max"]) if row["max"] is not None else None,
                        }

                summary_stats = {
                    "statistic": ["count", "mean", "std", "min", "25%", "50%", "75%", "max"]
                }

                for col in sanitized:
                    if col in column_stats:
                        summary_stats[col] = [
                            column_stats[col]["count"],
                            column_stats[col]["mean"],
                            column_stats[col]["std"],
                            column_stats[col]["min"],
                            column_stats[col]["25%"],
                            column_stats[col]["50%"],
                            column_stats[col]["75%"],
                            column_stats[col]["max"],
                        ]

                records = []
                num_rows = len(summary_stats["statistic"])

                for i in range(num_rows):
                    row = {"statistic": summary_stats["statistic"][i]}
                    for col in sanitized:
                        if col in summary_stats:
                            row[col] = summary_stats[col][i]
                    records.append(row)

                df = self._records_to_dataframe(records)
                output_table = await self._save_dataframe_as_table(
                    df=df,
                    schema=schema,
                    base_table=f"{table}_describe",
                    backend=backend,
                    data_id=data_id,
                    new_table=requested_new_table,
                )

                return self._success_response(
                    involved_cols=columns,
                    generated_cols=list(df.columns),
                    message=f"Descriptive statistics for {len(columns)} numeric columns in '{table}'",
                    result=df,
                    new_table=output_table,
                    result_metadata={
                        "row_count": len(records),
                        "saved_table": output_table,
                        "columns": ["statistic"] + sanitized,
                        "numeric_columns_analyzed": columns,
                    },
                )

            else:
                raise self._unsupported_backend_error()
        except Exception as e:
            return self._error_response(
                f"dataframe_describe error: {str(e)}\n{traceback.format_exc()}"
            )

    async def dataframe_null_analysis(self, table: str, schema: str, columns: Optional[List[str]] = None, **kwargs) -> Dict[str, Any]:
        try:
            if isinstance(self.db, PostgresAdapter) or isinstance(self.db, DuckDBAdapter) or isinstance(self.db, ClickHouseAdapter):
                backend = kwargs.get("backend")
                data_id = kwargs.get("data_id")
                requested_new_table = kwargs.get("new_table")
                table = SQLIdentifierSanitizer.sanitize(table)
                qualified = self._qualified_table(table, schema)

                total_rows = await self._fetchval(f"SELECT COUNT(*) FROM {qualified}") or 0

                if isinstance(self.db, ClickHouseAdapter):
                    actual_cols_rows = await self._fetch("""
                        SELECT name
                        FROM system.columns
                        WHERE database = ? AND table = ?
                    """, schema, table)
                    actual_columns = [row["name"] for row in actual_cols_rows]
                else:
                    actual_cols_rows = await self._fetch(f"""
                        SELECT column_name
                        FROM information_schema.columns
                        WHERE table_name = '{table}'
                        AND table_schema = '{schema}'
                    """)
                    actual_columns = [row["column_name"] for row in actual_cols_rows]

                if not actual_columns:
                    return self._error_response("No columns found in table")

                if columns is None or columns == "*" or columns == ["*"]:
                    target_columns = actual_columns
                else:
                    target_columns = [c for c in columns if c in actual_columns]

                    if not target_columns:
                        return self._error_response(
                            f"No valid columns found. Available columns: {actual_columns}"
                        )

                sanitized = [SQLIdentifierSanitizer.sanitize(c) for c in target_columns]

                null_rows = []

                for col in sanitized:
                    null_count = await self._fetchval(
                        f'SELECT COUNT(*) FROM {qualified} WHERE "{col}" IS NULL'
                    ) or 0

                    pct = (null_count / total_rows * 100) if total_rows > 0 else 0.0

                    null_rows.append({
                        "column_name": col,
                        "contains_null": null_count > 0,
                        "percent_missing": round(pct, 2),
                    })

                df = self._records_to_dataframe(null_rows)

                if not df.empty:
                    df = df.set_index("column_name")

                output_table = await self._save_dataframe_as_table(
                    df=df,
                    schema=schema,
                    base_table=f"{table}_null_analysis",
                    backend=backend,
                    data_id=data_id,
                    new_table=requested_new_table,
                )

                cols_with_nulls = df[df["contains_null"]] if not df.empty else []
                max_pct = df["percent_missing"].max() if not df.empty else 0
                avg_pct = df["percent_missing"].mean() if not df.empty else 0

                return self._success_response(
                    involved_cols=target_columns,
                    generated_cols=list(df.columns),
                    message=f"Null analysis for '{table}' ({len(target_columns)} columns analyzed)",
                    result=df,
                    new_table=output_table,
                    result_metadata={
                        "row_count": len(df),
                        "saved_table": output_table,
                        "summary": {
                            "total_rows_analyzed": total_rows,
                            "columns_analyzed": len(target_columns),
                            "columns_with_nulls": len(cols_with_nulls),
                            "maximum_null_percentage": round(float(max_pct), 2),
                            "average_null_percentage": round(float(avg_pct), 2),
                        },
                    },
                )

            else:
                raise self._unsupported_backend_error()
        except Exception as e:
            return self._error_response(
                f"dataframe_null_analysis error: {str(e)}\n{traceback.format_exc()}"
            )

   
    async def dataframe_full_table(self, table: str, schema: str,columns: Optional[List[str]] = None, chunk_size: Optional[int] = None,**kwargs,) -> Dict[str, Any]:
        try:
            if isinstance(self.db, PostgresAdapter) or isinstance(self.db, DuckDBAdapter) or isinstance(self.db, ClickHouseAdapter):
                table = SQLIdentifierSanitizer.sanitize(table)
                qualified = self._qualified_table(table, schema)
                column_types = await self._get_column_types(table, schema)

                if columns:
                    selected = [c for c in columns if c in column_types]
                    if not selected:
                        selected = list(column_types.keys())
                else:
                    selected = list(column_types.keys())

                if selected:
                    sanitized = [SQLIdentifierSanitizer.sanitize(c) for c in selected]
                    column_clause = ", ".join([f'"{c}"' for c in sanitized])
                else:
                    column_clause = "*"
                    selected = list(column_types.keys())

                if chunk_size is not None:
                    if not isinstance(chunk_size, int) or chunk_size <= 0:
                        return self._error_response("chunk_size must be a positive integer")

                    async def iterator():
                        offset = 0
                        while True:
                            rows = await self._fetch(
                                f"SELECT {column_clause} FROM {qualified} LIMIT {chunk_size} OFFSET {offset}"
                            )
                            if not rows:
                                break

                            records = self._rows_to_records(rows)
                            records = await self._normalize_records_by_type(records, table, schema)
                            yield pd.DataFrame.from_records(records)
                            offset += chunk_size

                    return {
                        "is_error": False,
                        "message": f"Streaming full table '{table}'",
                        "error_message": None,
                        "iterator": iterator(),
                        "chunk_size": chunk_size,
                        "involved_cols": selected,
                    }

                rows = await self._fetch(f"SELECT {column_clause} FROM {qualified}")
                records = self._rows_to_records(rows)
                records = await self._normalize_records_by_type(records, table, schema)
                df = pd.DataFrame.from_records(records)

                msg = f"Returned full table '{table}' with {len(records)} rows"
                if selected and len(selected) < 20:
                    msg += f" (columns: {', '.join(selected)})"

                return self._success_response(
                    involved_cols=selected,
                    message=msg,
                    result=df,
                    result_metadata={"row_count": len(records), "full_table": True},
                )
            else:
                raise self._unsupported_backend_error()
        except Exception as e:
            return self._error_response(f"dataframe_full_table error: {str(e)}\n{traceback.format_exc()}")


    # ------------------------------------------------------------------
    # Additional DataFrame Operations
    # ------------------------------------------------------------------

    async def dataframe_astype(self, table: str, schema: str, dtype_map: Dict[str, str], **kwargs) -> Dict[str, Any]:
        try:
            if isinstance(self.db, PostgresAdapter) or isinstance(self.db, DuckDBAdapter) or isinstance(self.db, ClickHouseAdapter):
                table = SQLIdentifierSanitizer.sanitize(table)
                qualified = self._qualified_table(table, schema)

                if not dtype_map:
                    return self._error_response("dtype_map cannot be empty")

                column_types = await self._get_column_types(table, schema)
                actual_columns = set(column_types.keys())

                # Base translations for Postgres/DuckDB
                dtype_translation = {
                    "int": "INTEGER", "int8": "INTEGER", "int16": "INTEGER", "int32": "INTEGER",
                    "int64": "BIGINT",
                    "float": "FLOAT", "float32": "FLOAT",
                    "float64": "DOUBLE", "double": "DOUBLE",
                    "str": "TEXT", "string": "TEXT", "text": "TEXT"
                }

                # ClickHouse-safe functions (nullable-friendly)
                ch_type_translation = {
                    "int": "Int64",
                    "int8": "Int8",
                    "int16": "Int16",
                    "int32": "Int32",
                    "int64": "Int64",
                    "float": "Float64",
                    "float32": "Float32",
                    "float64": "Float64",
                    "double": "Float64",
                    "str": "String",
                    "string": "String",
                    "text": "String",
                }
                ch_string_cast_functions = {
                    "int": "toInt64OrNull",
                    "int8": "toInt8OrNull",
                    "int16": "toInt16OrNull",
                    "int32": "toInt32OrNull",
                    "int64": "toInt64OrNull",
                    "float": "toFloat64OrNull",
                    "float32": "toFloat32OrNull",
                    "float64": "toFloat64OrNull",
                    "double": "toFloat64OrNull",
                }

                normalized_map = {}
                requested_dtype_map = {}

                for col, dtype in dtype_map.items():
                    if col not in actual_columns:
                        return self._error_response(f"Column '{col}' does not exist")

                    dtype_lower = dtype.lower()

                    if dtype_lower not in dtype_translation:
                        return self._error_response(f"Unsupported dtype '{dtype}'")

                    if isinstance(self.db, ClickHouseAdapter):
                        normalized_map[col] = ch_type_translation[dtype_lower]
                    else:
                        normalized_map[col] = dtype_translation[dtype_lower]
                    requested_dtype_map[col] = dtype_lower

                select_parts = []
                for col, dtype in normalized_map.items():
                    col_safe = SQLIdentifierSanitizer.sanitize(col)
                    
                    if isinstance(self.db, ClickHouseAdapter):
                        col_q = self.db.quote_identifier(col_safe)
                        dtype_lower = requested_dtype_map[col]
                        source_type = column_types[col].lower()
                        source_is_string = "string" in source_type or "text" in source_type

                        if dtype == "String":
                            expr = f"toString({col_q})"
                        elif source_is_string:
                            ch_func = ch_string_cast_functions[dtype_lower]
                            expr = f"{ch_func}({col_q})"
                        else:
                            expr = f"CAST({col_q} AS Nullable({dtype}))"

                        select_parts.append(
                            f"{expr} AS {self.db.quote_identifier(col_safe)}"
                        )
                    else:
                        select_parts.append(f'CAST("{col_safe}" AS {dtype}) AS "{col_safe}"')

                query = f"SELECT {', '.join(select_parts)} FROM {qualified}"

                rows = await self._fetch(query)
                records = self._rows_to_records(rows)
                records = await self._normalize_records_by_type(records, table, schema)
                df = pd.DataFrame.from_records(records)

                return self._success_response(
                    message=f"Returned casted columns from '{table}'",
                    involved_cols=list(normalized_map.keys()),
                    generated_cols=list(normalized_map.keys()),
                    result=df,
                    result_metadata={
                        "row_count": len(df),
                        "casted_columns": normalized_map
                    },
                )

            else:
                raise self._unsupported_backend_error()
        except Exception as e:
            return self._error_response(str(e))
    
    async def dataframe_insert(self, table: str, schema: str, column: str, value: Any, **kwargs) -> Dict[str, Any]:
        try:
            if isinstance(self.db, PostgresAdapter) or isinstance(self.db, DuckDBAdapter):
                table = SQLIdentifierSanitizer.sanitize(table)
                column = SQLIdentifierSanitizer.sanitize(column)
                qualified = self._qualified_table(table, schema)

                if not isinstance(value, list):
                    return self._error_response("Value must be a list")

                total_rows = await self._fetchval(f"SELECT COUNT(*) FROM {qualified}") or 0

                if len(value) != total_rows:
                    return self._error_response(
                        f"Length mismatch: Expected {total_rows}, got {len(value)}"
                    )

                # Determine column type from values
                non_none = [v for v in value if v is not None]
                if all(isinstance(v, bool) for v in non_none):
                    col_type = "BOOLEAN"
                elif all(isinstance(v, int) for v in non_none):
                    col_type = "BIGINT"
                elif all(isinstance(v, float) for v in non_none):
                    col_type = "DOUBLE PRECISION"
                else:
                    col_type = "TEXT"

                await self._exec(f'ALTER TABLE {qualified} ADD COLUMN "{column}" {col_type}')

                if isinstance(self.db, PostgresAdapter):
                    id_col = "ctid"
                elif isinstance(self.db, DuckDBAdapter):
                    id_col = "rowid"
                else:
                    raise self._unsupported_backend_error()

                temp_table = f"temp_insert_{column}"

                await self._exec(f"CREATE TEMP TABLE {temp_table} (idx INT, val {col_type})")

                for i, v in enumerate(value):
                    placeholder1 = self.db.placeholder(1)
                    placeholder2 = self.db.placeholder(2)

                    await self._exec(
                        f"INSERT INTO {temp_table} VALUES ({placeholder1}, {placeholder2})",
                        i,
                        v
                    )

                update_sql = f"""
                    UPDATE {qualified}
                    SET "{column}" = t.val
                    FROM (
                        SELECT {id_col}, ROW_NUMBER() OVER () - 1 as idx
                        FROM {qualified}
                    ) base
                    JOIN {temp_table} t ON base.idx = t.idx
                    WHERE {qualified}.{id_col} = base.{id_col}
                """

                await self._exec(update_sql)

                await self._exec(f"DROP TABLE {temp_table}")

                rows = await self._fetch(f"SELECT * FROM {qualified}")
                records = self._rows_to_records(rows)
                records = await self._normalize_records_by_type(records, table, schema)
                current_df = pd.DataFrame.from_records(records)

                return self._success_response(
                    message=f"Column '{column}' created successfully with {total_rows} values",
                    involved_cols=[],
                    generated_cols=[column],
                    current_state=current_df,
                )

            elif isinstance(self.db, ClickHouseAdapter):
                table = SQLIdentifierSanitizer.sanitize(table)
                column = SQLIdentifierSanitizer.sanitize(column)
                qualified = self._qualified_table(table, schema)

                if not isinstance(value, list):
                    return self._error_response("Value must be a list")

                total_rows = await self._fetchval(f"SELECT count() FROM {qualified}") or 0

                if len(value) != total_rows:
                    return self._error_response(
                        f"Length mismatch: Expected {total_rows}, got {len(value)}"
                    )

                # ClickHouse: get current data, add column, recreate table
                df = await self._get_table_dataframe(table, schema)
                df[column] = value

                temp_name = await self._save_dataframe_as_table(
                    df, schema, table, backend=kwargs.get("backend"), data_id=kwargs.get("data_id")
                )
                temp_qualified = self._qualified_table(temp_name, schema)

                await self._exec(f"DROP TABLE IF EXISTS {qualified}")
                await self._exec(f"RENAME TABLE {temp_qualified} TO {qualified}")

                return self._success_response(
                    message=f"Column '{column}' created successfully with {total_rows} values",
                    involved_cols=[],
                    generated_cols=[column],
                    current_state=df,
                )

            else:
                raise self._unsupported_backend_error()
        except Exception as e:
            return self._error_response(str(e))

    async def dataframe_map(self,table: str,schema: str,func: str,na_action: Optional[str] = None,columns: Optional[List[str]] = None,datetime_action: str = "skip",**kwargs) -> Dict[str, Any]:

        try:
            if isinstance(self.db, PostgresAdapter) or isinstance(self.db, DuckDBAdapter) or isinstance(self.db, ClickHouseAdapter):
                table = SQLIdentifierSanitizer.sanitize(table)
                qualified = self._qualified_table(table, schema)

                if not isinstance(func, str):
                    return self._error_response("func must be a SQL expression string using 'x' as placeholder")

                column_types = await self._get_column_types(table, schema)

                numeric_types = [
                    "integer", "bigint", "smallint", "decimal", "numeric",
                    "real", "double", "double precision", "float", "float8", "float4",
                    "int", "int4", "int8"
                ]

                string_types = [
                    "text", "varchar", "char", "character varying", "string"
                ]

                datetime_types = [
                    "date",
                    "timestamp",
                    "timestamp without time zone",
                    "timestamp with time zone",
                    "datetime"
                ]

                boolean_types = ["boolean", "bool"]

                if columns is None or columns == "*" or columns == ["*"]:
                    target_columns = list(column_types.keys())
                else:
                    target_columns = [c for c in columns if c in column_types]

                if not target_columns:
                    return self._error_response("No valid columns selected")

                select_parts = []
                applied_cols = []
                skipped_cols = []

                for col in target_columns:
                    col_safe = SQLIdentifierSanitizer.sanitize(col)
                    dtype = column_types[col].lower()

                    expr = func.replace("x", f'"{col_safe}"')

                    if any(nt in dtype for nt in numeric_types):
                        final_expr = expr

                    elif any(st in dtype for st in string_types):
                        if any(fn in func.upper() for fn in ["UPPER", "LOWER", "LENGTH", "TRIM"]):
                            final_expr = expr
                        else:
                            skipped_cols.append(col)
                            continue

                    elif any(bt in dtype for bt in boolean_types):
                        if any(op in func for op in ["*", "+", "-", "/", "%"]):
                            expr = func.replace("x", f'CAST("{col_safe}" AS INTEGER)')
                            final_expr = expr
                        else:
                            skipped_cols.append(col)
                            continue

                    elif any(dt in dtype for dt in datetime_types):

                        if datetime_action == "skip":
                            skipped_cols.append(col)
                            continue

                        elif datetime_action == "keep":
                            select_parts.append(f'"{col_safe}"')
                            applied_cols.append(col)
                            continue

                        elif datetime_action == "cast_string":
                            expr = func.replace("x", f'CAST("{col_safe}" AS TEXT)')
                            final_expr = expr

                        elif datetime_action == "extract_epoch":
                            expr = func.replace("x", f'EXTRACT(EPOCH FROM "{col_safe}")')
                            final_expr = expr

                        elif datetime_action == "error":
                            return self._error_response(
                                f"Datetime column '{col}' not allowed with current expression"
                            )

                        else:
                            return self._error_response(
                                f"Invalid datetime_action '{datetime_action}'"
                            )

                    else:
                        skipped_cols.append(col)
                        continue

                    if na_action == "ignore":
                        final_expr = f'CASE WHEN "{col_safe}" IS NULL THEN NULL ELSE {final_expr} END'

                    select_parts.append(f'{final_expr} AS "{col_safe}"')
                    applied_cols.append(col)

                if not select_parts:
                    return self._error_response("No columns compatible with the given expression")

                query = f"SELECT {', '.join(select_parts)} FROM {qualified}"

                rows = await self._fetch(query)
                records = self._rows_to_records(rows)
                records = await self._normalize_records_by_type(records, table, schema)
                df = pd.DataFrame.from_records(records)

                return self._success_response(
                    message=f"Applied map on {len(applied_cols)} columns, skipped {len(skipped_cols)}",
                    involved_cols=target_columns,
                    generated_cols=applied_cols,
                    result=df,
                    result_metadata={
                        "row_count": len(df),
                        "column_count": len(df.columns),
                        "applied_columns": applied_cols,
                        "skipped_columns": skipped_cols,
                        "expression": func,
                        "datetime_action": datetime_action,
                        "na_action": na_action,
                    },
                )

            else:
                raise self._unsupported_backend_error()
        except Exception as e:
            return self._error_response(
                f"dataframe_map error: {str(e)}\n{traceback.format_exc()}"
            )


    async def dataframe_rename(self, table: str, schema: str, columns: Dict[str, str], **kwargs    ) -> Dict[str, Any]:
        try:
            if isinstance(self.db, PostgresAdapter) or isinstance(self.db, DuckDBAdapter) or isinstance(self.db, ClickHouseAdapter):
                table = SQLIdentifierSanitizer.sanitize(table)
                qualified = self._qualified_table(table, schema)

                for old, new in columns.items():
                    old_safe = SQLIdentifierSanitizer.sanitize(old)
                    new_safe = SQLIdentifierSanitizer.sanitize(new)
                    await self._exec(
                        f'ALTER TABLE {qualified} RENAME COLUMN "{old_safe}" TO "{new_safe}"'
                    )

                current_df = await self.dataframe_head(table, schema)

                return self._success_response(
                    message="Columns renamed",
                    involved_cols=list(columns.keys()),
                    generated_cols=list(columns.values()),
                    result=current_df,
                    column_mapping=columns,
                )
            else:
                raise self._unsupported_backend_error()
        except Exception as e:
            return self._error_response(str(e))

    async def dataframe_set_index(self, table: str, schema: str, columns: List[str], **kwargs
    ) -> Dict[str, Any]:
        try:
            if isinstance(self.db, PostgresAdapter) or isinstance(self.db, DuckDBAdapter):
                table = SQLIdentifierSanitizer.sanitize(table)
                qualified = self._qualified_table(table, schema)

                cols = [f'"{SQLIdentifierSanitizer.sanitize(c)}"' for c in columns]
                await self._exec(
                    f"ALTER TABLE {qualified} ADD PRIMARY KEY ({', '.join(cols)})"
                )

                return self._success_response(
                    result="Index set", involved_cols=columns
                )
            elif isinstance(self.db, ClickHouseAdapter):
                table = SQLIdentifierSanitizer.sanitize(table)
                qualified = self._qualified_table(table, schema)

                # ClickHouse uses ORDER BY for sorting; recreate table with proper ORDER BY
                df = await self._get_table_dataframe(table, schema)

                temp_name = await self._save_dataframe_as_table(df, schema, table)
                temp_qualified = self._qualified_table(temp_name, schema)

                await self._exec(f"DROP TABLE IF EXISTS {qualified}")
                await self._exec(f"RENAME TABLE {temp_qualified} TO {qualified}")

                return self._success_response(
                    result="Index set (ORDER BY in ClickHouse)", involved_cols=columns
                )
            else:
                raise self._unsupported_backend_error()
        except Exception as e:
            return self._error_response(str(e))


    async def dataframe_reset_index(self, table: str, schema: str, **kwargs) -> Dict[str, Any]:
        try:
            if isinstance(self.db, PostgresAdapter):
                table = SQLIdentifierSanitizer.sanitize(table)
                qualified = self._qualified_table(table, schema)

                await self._exec(f"""
                    DO $$
                    BEGIN
                        IF EXISTS (
                            SELECT 1 FROM information_schema.columns
                            WHERE table_name = '{table}' AND column_name = 'id'
                        ) THEN
                            ALTER TABLE {qualified} DROP COLUMN id;
                        END IF;
                    END$$;
                """)

                await self._exec(f'ALTER TABLE {qualified} ADD COLUMN id SERIAL')

                current_df = await self._get_table_dataframe(table, schema)

                return self._success_response(
                    message="Index reset with new id column",
                    generated_cols=["id"],
                    current_state=current_df,
                )

            elif isinstance(self.db, DuckDBAdapter):
                table = SQLIdentifierSanitizer.sanitize(table)
                qualified = self._qualified_table(table, schema)

                temp_table = f"{table}_temp_reset"
                qualified_temp = f'{self.db.quote_identifier(schema)}.{self.db.quote_identifier(temp_table)}'

                await self._exec(f"""
                    CREATE TABLE {qualified_temp} AS
                    SELECT 
                        ROW_NUMBER() OVER () AS id,
                        *
                    FROM {qualified}
                """)

                await self._exec(f"DROP TABLE {qualified}")

                await self._exec(f"""
                    ALTER TABLE {qualified_temp}
                    RENAME TO {self.db.quote_identifier(table)}
                """)

                current_df = await self._get_table_dataframe(table, schema)

                return self._success_response(
                    message="Index reset with new id column",
                    generated_cols=["id"],
                    current_state=current_df,
                )

            elif isinstance(self.db, ClickHouseAdapter):
                table = SQLIdentifierSanitizer.sanitize(table)
                qualified = self._qualified_table(table, schema)

                temp_table = f"{table}_temp_reset"
                qualified_temp = self._qualified_table(temp_table, schema)

                await self._exec(f"""
                    CREATE TABLE {qualified_temp} AS
                    SELECT 
                        ROW_NUMBER() OVER () AS id,
                        *
                    FROM {qualified}
                """)

                await self._exec(f"DROP TABLE IF EXISTS {qualified}")
                await self._exec(f"RENAME TABLE {qualified_temp} TO {qualified}")

                current_df = await self._get_table_dataframe(table, schema)

                return self._success_response(
                    message="Index reset with new id column",
                    generated_cols=["id"],
                    current_state=current_df,
                )

            else:
                raise self._unsupported_backend_error()
        except Exception as e:
            return self._error_response(str(e))


    async def dataframe_update(self,table: str,schema: str,other_table: str,other_schema: str,on: str,    overwrite: bool = True,    errors: str = "ignore",    **kwargs) -> Dict[str, Any]:
        try:
            if isinstance(self.db, PostgresAdapter) or isinstance(self.db, DuckDBAdapter):
                table = SQLIdentifierSanitizer.sanitize(table)
                other_table = SQLIdentifierSanitizer.sanitize(other_table)
                on = SQLIdentifierSanitizer.sanitize(on)

                t1 = self._qualified_table(table, schema)
                t2 = self._qualified_table(other_table, other_schema)

                if errors not in ["ignore", "raise"]:
                    return self._error_response("errors must be 'ignore' or 'raise'")

                target_cols = await self._get_column_types(table, schema)
                source_cols = await self._get_column_types(other_table, other_schema)

                common_cols = [c for c in target_cols if c in source_cols and c != on]

                if not common_cols:
                    return self._success_response(
                        message="No overlapping columns to update",
                        involved_cols=[],
                        generated_cols=[]
                    )

                if errors == "raise":
                    for col in common_cols:
                        conflict = await self._fetchval(f"""
                            SELECT COUNT(*)
                            FROM {t1}
                            JOIN {t2} ON {t1}."{on}" = {t2}."{on}"
                            WHERE {t1}."{col}" IS NOT NULL AND {t2}."{col}" IS NOT NULL
                        """)
                        if conflict > 0:
                            return self._error_response(
                                f"Conflict detected in column '{col}'"
                            )

                assignments = []

                for col in common_cols:
                    col_safe = SQLIdentifierSanitizer.sanitize(col)

                    if overwrite:
                        expr = f"""
                        "{col_safe}" = CASE
                            WHEN {t2}."{col_safe}" IS NOT NULL THEN {t2}."{col_safe}"
                            ELSE {t1}."{col_safe}"
                        END
                        """
                    else:
                        expr = f"""
                        "{col_safe}" = CASE
                            WHEN {t1}."{col_safe}" IS NULL AND {t2}."{col_safe}" IS NOT NULL
                            THEN {t2}."{col_safe}"
                            ELSE {t1}."{col_safe}"
                        END
                        """

                    assignments.append(expr)

                assignment_sql = ", ".join(assignments)

                await self._exec(f"""
                    UPDATE {t1}
                    SET {assignment_sql}
                    FROM {t2}
                    WHERE {t1}."{on}" = {t2}."{on}"
                """)

                sample_rows = await self._fetch(f"""
                    SELECT *
                    FROM {t1}
                    LIMIT 10
                """)

                records = self._rows_to_records(sample_rows)
                records = await self._normalize_records_by_type(records, table, schema)
                sample_df = pd.DataFrame.from_records(records)
                return self._success_response(
                    result = sample_df,
                    message=f"Table '{table}' updated using '{other_table}'",
                    involved_cols=[on] + common_cols,
                    generated_cols=common_cols,
                    result_metadata={
                        "updated_columns": common_cols,
                        "overwrite": overwrite,
                        "errors": errors
                    }
                )

            elif isinstance(self.db, ClickHouseAdapter):
                table = SQLIdentifierSanitizer.sanitize(table)
                other_table = SQLIdentifierSanitizer.sanitize(other_table)
                on = SQLIdentifierSanitizer.sanitize(on)

                t1 = self._qualified_table(table, schema)
                t2 = self._qualified_table(other_table, other_schema)

                if errors not in ["ignore", "raise"]:
                    return self._error_response("errors must be 'ignore' or 'raise'")

                target_cols = await self._get_column_types(table, schema)
                source_cols = await self._get_column_types(other_table, other_schema)

                common_cols = [c for c in target_cols if c in source_cols and c != on]

                if not common_cols:
                    return self._success_response(
                        message="No overlapping columns to update",
                        involved_cols=[],
                        generated_cols=[]
                    )

                if errors == "raise":
                    for col in common_cols:
                        conflict = await self._fetchval(f"""
                            SELECT count()
                            FROM {t1}
                            JOIN {t2} ON {t1}.`{on}` = {t2}.`{on}`
                            WHERE {t1}.`{col}` IS NOT NULL AND {t2}.`{col}` IS NOT NULL
                        """)
                        if conflict > 0:
                            return self._error_response(
                                f"Conflict detected in column '{col}'"
                            )

                assignments = []
                for col in common_cols:
                    col_safe = SQLIdentifierSanitizer.sanitize(col)
                    if overwrite:
                        expr = f"`{col_safe}` = (SELECT `{col_safe}` FROM {t2} WHERE `{t2}`.`{on}` = `{t1}`.`{on}` LIMIT 1)"
                    else:
                        expr = f"`{col_safe}` = CASE WHEN `{t1}`.`{col_safe}` IS NULL THEN (SELECT `{col_safe}` FROM {t2} WHERE `{t2}`.`{on}` = `{t1}`.`{on}` LIMIT 1) ELSE `{t1}`.`{col_safe}` END"
                    assignments.append(expr)

                assignment_sql = ", ".join(assignments)
                update_sql = f"ALTER TABLE {t1} UPDATE {assignment_sql} WHERE 1"
                await self._exec(update_sql)

                sample_rows = await self._fetch(f"SELECT * FROM {t1} LIMIT 10")
                records = self._rows_to_records(sample_rows)
                records = await self._normalize_records_by_type(records, table, schema)
                sample_df = pd.DataFrame.from_records(records)

                return self._success_response(
                    result=sample_df,
                    message=f"Table '{table}' updated using '{other_table}'",
                    involved_cols=[on] + common_cols,
                    generated_cols=common_cols,
                    result_metadata={
                        "updated_columns": common_cols,
                        "overwrite": overwrite,
                        "errors": errors
                    }
                )

            else:
                raise self._unsupported_backend_error()
        except Exception as e:
            return self._error_response(str(e))

    async def dataframe_resample(self,table: str,schema: str,time_column: str,    rule: str,
        agg: str = "COUNT",    value_column: Optional[str] = None,    label: str = "left",
        closed: str = "left",    **kwargs,) -> Dict[str, Any]:
        try:
            if isinstance(self.db, PostgresAdapter) or isinstance(self.db, DuckDBAdapter) or isinstance(self.db, ClickHouseAdapter):
                table = SQLIdentifierSanitizer.sanitize(table)
                time_column = SQLIdentifierSanitizer.sanitize(time_column)
                qualified = self._qualified_table(table, schema)

                column_types = await self._get_column_types(table, schema)

                if time_column not in column_types:
                    return self._error_response(f"Column '{time_column}' not found")

                dtype = column_types[time_column].lower()
                if not any(t in dtype for t in ("date", "time", "timestamp")):
                    return self._error_response(f"Column '{time_column}' must be datetime-like")

                if label not in ["left", "right"]:
                    return self._error_response("label must be 'left' or 'right'")

                if closed not in ["left", "right"]:
                    return self._error_response("closed must be 'left' or 'right'")

                if agg.upper() == "COUNT":
                    agg_expr = "COUNT(*)"
                else:
                    if not value_column:
                        return self._error_response("value_column required for aggregation other than COUNT")

                    value_column = SQLIdentifierSanitizer.sanitize(value_column)
                    agg_expr = f'{agg.upper()}("{value_column}")'

                bucket_expr = f'DATE_TRUNC(\'{rule}\', "{time_column}")'

                if label == "right":
                    bucket_expr = f"{bucket_expr} + INTERVAL '1 {rule}'"

                query = f"""
                    SELECT
                        {bucket_expr} AS bucket,
                        {agg_expr} AS value
                    FROM {qualified}
                    GROUP BY bucket
                    ORDER BY bucket
                """

                rows = await self._fetch(query)
                records = self._rows_to_records(rows)
                records = await self._normalize_records_by_type(records, table, schema)
                df = pd.DataFrame.from_records(records)
                df = df.rename(columns={"bucket": time_column})

                return self._success_response(
                    message=f"Resampled '{table}' using rule='{rule}'",
                    involved_cols=[time_column],
                    generated_cols=["bucket", "value"],
                    result=df,
                    result_metadata={
                        "row_count": len(df),
                        "rule": rule,
                        "aggregation": agg,
                        "label": label,
                        "closed": closed,
                    },
                )

            else:
                raise self._unsupported_backend_error()
        except Exception as e:
            return self._error_response(
                f"dataframe_resample error: {str(e)}\n{traceback.format_exc()}"
            )

    # ------------------------------------------------------------------
    # Metadata / Info Methods
    # ------------------------------------------------------------------
    async def dataframe_axes(self, table: str, schema: str, **kwargs) -> Dict[str, Any]:
        try:
            if isinstance(self.db, PostgresAdapter) or isinstance(self.db, DuckDBAdapter) or isinstance(self.db, ClickHouseAdapter):
                columns = list((await self._get_column_types(table, schema)).keys())
                count = await self._fetchval(
                    f"SELECT COUNT(*) FROM {self._qualified_table(table, schema)}"
                )
                index = list(range(count))
                return self._success_response(result=[index, columns])
            else:
                raise self._unsupported_backend_error()
        except Exception as e:
            return self._error_response(str(e))

    async def dataframe_columns(self, table: str, schema: str, **kwargs) -> Dict[str, Any]:
        try:
            if isinstance(self.db, PostgresAdapter) or isinstance(self.db, DuckDBAdapter) or isinstance(self.db, ClickHouseAdapter):
                cols = list((await self._get_column_types(table, schema)).keys())
                return self._success_response(result=cols)
            else:
                raise self._unsupported_backend_error()
        except Exception as e:
            return self._error_response(str(e))

    async def dataframe_dtypes(self, table: str, schema: str, **kwargs) -> Dict[str, Any]:
        try:
            if isinstance(self.db, PostgresAdapter) or isinstance(self.db, DuckDBAdapter) or isinstance(self.db, ClickHouseAdapter):
                col_types = await self._get_column_types(table, schema)
                return self._success_response(result=col_types)
            else:
                raise self._unsupported_backend_error()
        except Exception as e:
            return self._error_response(str(e))

    async def dataframe_first_valid_index(self, table: str, schema: str, **kwargs) -> Dict[str, Any]:
        try:
            if isinstance(self.db, PostgresAdapter) or isinstance(self.db, DuckDBAdapter) or isinstance(self.db, ClickHouseAdapter):
                qualified = self._qualified_table(table, schema)
                cols = await self._get_column_types(table, schema)
                if not cols:
                    return self._success_response(result={"first_valid_index": None})

                condition = " AND ".join([f'"{c}" IS NULL' for c in cols])
                row = await self._fetchrow(
                    f"SELECT * FROM {qualified} WHERE NOT ({condition}) LIMIT 1"
                )
                return self._success_response(result={"first_valid_index": 0 if row else None})
            else:
                raise self._unsupported_backend_error()
        except Exception as e:
            return self._error_response(str(e))

    async def dataframe_memory_usage(self, table: str, schema: str, **kwargs) -> Dict[str, Any]:
        try:
            if isinstance(self.db, PostgresAdapter):
                relation = f'"{schema}"."{table}"'
                size = await self._fetchval(
                    f"SELECT pg_total_relation_size('{relation}') as total_bytes"
                )
                return self._success_response(result={"memory_bytes": size})
            elif isinstance(self.db, DuckDBAdapter):
                size = None
                return self._success_response(result={"memory_bytes": size})
            elif isinstance(self.db, ClickHouseAdapter):
                size = await self._fetchval(
                    f"SELECT sum(bytes) FROM system.parts WHERE database = '{schema}' AND table = '{table}' AND active = 1"
                )
                return self._success_response(result={"memory_bytes": size})
            else:
                raise self._unsupported_backend_error()
        except Exception as e:
            return self._error_response(str(e))

    async def dataframe_ndim(self, table: str, schema: str, **kwargs) -> Dict[str, Any]:
        if isinstance(self.db, PostgresAdapter) or isinstance(self.db, DuckDBAdapter) or isinstance(self.db, ClickHouseAdapter):
            return self._success_response(result={"ndim": 2})

        else:
            raise self._unsupported_backend_error()
    async def dataframe_shape(self, table: str, schema: str, **kwargs) -> Dict[str, Any]:
        try:
            if isinstance(self.db, PostgresAdapter) or isinstance(self.db, DuckDBAdapter) or isinstance(self.db, ClickHouseAdapter):
                rows = await self._fetchval(
                    f"SELECT COUNT(*) FROM {self._qualified_table(table, schema)}"
                )
                cols = len(await self._get_column_types(table, schema))
                return self._success_response(result={"shape": (rows, cols)})
            else:
                raise self._unsupported_backend_error()
        except Exception as e:
            return self._error_response(str(e))

    async def dataframe_size(self, table: str, schema: str, **kwargs) -> Dict[str, Any]:
        try:
            if isinstance(self.db, PostgresAdapter) or isinstance(self.db, DuckDBAdapter) or isinstance(self.db, ClickHouseAdapter):
                shape_res = await self.dataframe_shape(table, schema)
                shape = shape_res["result"]["shape"]
                return self._success_response(result={"size": shape[0] * shape[1]})
            else:
                raise self._unsupported_backend_error()
        except Exception as e:
            return self._error_response(str(e))

    async def dataframe_values(self, table: str, schema: str, **kwargs) -> Dict[str, Any]:
        try:
            if isinstance(self.db, PostgresAdapter) or isinstance(self.db, DuckDBAdapter) or isinstance(self.db, ClickHouseAdapter):
                rows = await self._fetch(f"SELECT * FROM {self._qualified_table(table, schema)}")
                values = [list(row.values()) for row in rows]
                return self._success_response(result={"values": values})
            else:
                raise self._unsupported_backend_error()
        except Exception as e:
            return self._error_response(str(e))

    # ------------------------------------------------------------------
    # Iterator Methods (now async generators)
    # ------------------------------------------------------------------
    async def dataframe_items(self, table: str, schema: str, chunk_size: int = 1000, **kwargs):
        """
        True streaming version: yields (column, Series-like iterator)
        """
        try:
            if isinstance(self.db, PostgresAdapter) or isinstance(self.db, DuckDBAdapter) or isinstance(self.db, ClickHouseAdapter):
                table = SQLIdentifierSanitizer.sanitize(table)
                qualified = self._qualified_table(table, schema)

                cols = await self._get_column_types(table, schema)

                async def item_generator():
                    for col in cols:
                        col_safe = SQLIdentifierSanitizer.sanitize(col)

                        async def column_generator():
                            async for row in self.db.fetch_iter(
                                f'SELECT "{col_safe}" FROM {qualified}',
                                chunk_size=chunk_size
                            ):
                                if isinstance(self.db, PostgresAdapter):
                                    yield row[col]
                                elif isinstance(self.db, DuckDBAdapter):
                                    yield row[0]
                                elif isinstance(self.db, ClickHouseAdapter):
                                    yield row[col]
                                else:
                                    raise self._unsupported_backend_error()

                        yield col, column_generator()


                return self._success_response(
                    message="Streaming column-wise generator (no full materialization)",
                    result=item_generator(),
                    result_metadata={
                        "column_count": len(cols),
                        "mode": "streaming"
                    }
                )

            else:
                raise self._unsupported_backend_error()
        except Exception as e:
            return self._error_response(str(e))  

    async def dataframe_iterrows(self, table: str, schema: str, chunk_size: int = 1000, **kwargs):
        """
        Streaming async version of pandas iterrows()
        Yields (index, row_dict)
        """
        try:
            if isinstance(self.db, PostgresAdapter) or isinstance(self.db, DuckDBAdapter) or isinstance(self.db, ClickHouseAdapter):
                table = SQLIdentifierSanitizer.sanitize(table)
                qualified = self._qualified_table(table, schema)

                columns = list((await self._get_column_types(table, schema)).keys())

                async def row_generator():
                    idx = 0

                    async for row in self.db.fetch_iter(
                        f"SELECT * FROM {qualified}",
                        chunk_size=chunk_size
                    ):
                        if isinstance(self.db, PostgresAdapter):
                            row_dict = dict(row)
                        elif isinstance(self.db, DuckDBAdapter):
                            row_dict = dict(zip(columns, row))
                        elif isinstance(self.db, ClickHouseAdapter):
                            row_dict = dict(row)
                        else:
                            raise self._unsupported_backend_error()

                        yield idx, row_dict
                        idx += 1

                return self._success_response(
                    message="Streaming iterrows generator",
                    result=row_generator(),
                    result_metadata={
                        "mode": "streaming",
                        "column_count": len(columns)
                    }
                )

            else:
                raise self._unsupported_backend_error()
        except Exception as e:
            return self._error_response(str(e))

    async def dataframe_itertuples(self,table: str,schema: str,index: bool = True,name: Optional[str] = "ITER_TUPLES", chunk_size: int = 1000, **kwargs):
        """
        Streaming version of pandas itertuples()
        Yields namedtuples or tuples per row
        """
        try:
            if isinstance(self.db, PostgresAdapter) or isinstance(self.db, DuckDBAdapter) or isinstance(self.db, ClickHouseAdapter):
                table = SQLIdentifierSanitizer.sanitize(table)
                qualified = self._qualified_table(table, schema)

                columns = list((await self._get_column_types(table, schema)).keys())

                if name is not None:
                    fields = ["Index"] + columns if index else columns
                    RowTuple = namedtuple(name, fields)
                else:
                    RowTuple = None

                async def tuple_generator():
                    idx = 0

                    async for row in self.db.fetch_iter(
                        f"SELECT * FROM {qualified}",
                        chunk_size=chunk_size
                    ):
                        if isinstance(self.db, PostgresAdapter):
                            values = [row[col] for col in columns]
                        elif isinstance(self.db, DuckDBAdapter):
                            values = list(row)
                        elif isinstance(self.db, ClickHouseAdapter):
                            values = [row[col] for col in columns]
                        else:
                            raise self._unsupported_backend_error()

                        if index:
                            values = [idx] + values

                        if RowTuple:
                            yield RowTuple(*values)
                        else:
                            yield tuple(values)

                        idx += 1

                return self._success_response(
                    message="Streaming itertuples generator",
                    result=tuple_generator(),
                    result_metadata={
                        "mode": "streaming",
                        "column_count": len(columns),
                        "index": index,
                        "namedtuple": name is not None
                    }
                )

            else:
                raise self._unsupported_backend_error()
        except Exception as e:
            return self._error_response(str(e))
    # ------------------------------------------------------------------
    # GroupBy Methods
    # ------------------------------------------------------------------
    def _build_groupby_context(self, table: str, schema: str, group_cols: List[str],
                               series_col: Optional[str] = None) -> Dict[str, Any]:
        return {
            "table": SQLIdentifierSanitizer.sanitize(table),
            "schema": schema,
            "group_cols": [SQLIdentifierSanitizer.sanitize(c) for c in group_cols],
            "series_col": SQLIdentifierSanitizer.sanitize(series_col) if series_col else None,
        }
