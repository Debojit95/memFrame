from typing import Dict, List, Any, Optional
import traceback
import pandas as pd
from collections import namedtuple
from datetime import datetime, timezone


from db_manager.adapters.base import DatabaseAdapter
from db_manager.adapters.postgresql import PostgresAdapter
from db_manager.adapters.duckdb import DuckDBAdapter
from utils.helper import SQLIdentifierSanitizer

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

    def _records_to_dataframe(self, records: List[Dict[str, Any]]) -> pd.DataFrame:
        return pd.DataFrame.from_records(records)

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
        return self._records_to_dataframe(self._rows_to_records(rows))

    async def _generate_transient_table_name(self, base_table: str, backend, data_id: str) -> str:
        max_op = await backend.fetch_val(
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

    def _sql_type_for_series(self, series: pd.Series) -> str:
        dtype = series.dtype
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
            ts = datetime.now(timezone.utc).strftime("%Y%m%d%H%M%S%f")
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
        await self._exec(create_sql)

        if not df_to_store.empty:
            quoted_cols = ", ".join(self.db.quote_identifier(c) for c in df_to_store.columns)
            placeholders = ", ".join(self.db.placeholder(i + 1) for i in range(len(df_to_store.columns)))
            insert_sql = f"INSERT INTO {qualified_new} ({quoted_cols}) VALUES ({placeholders})"

            for row in df_to_store.itertuples(index=False, name=None):
                values = tuple(self._normalize_cell_value(v) for v in row)
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
            if isinstance(self.db, PostgresAdapter) or isinstance(self.db, DuckDBAdapter):
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
                df = self._records_to_dataframe(records)

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

    async def dataframe_tail(self, table: str, schema: str, n: int = 10, columns: Optional[List[str]] = None,**kwargs,) -> Dict[str, Any]:
        
        try:
            if isinstance(self.db, PostgresAdapter) or isinstance(self.db, DuckDBAdapter):
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

                rows = await self._fetch(f"SELECT {column_clause} FROM {qualified} OFFSET {offset} LIMIT {n}")
                records = self._rows_to_records(rows)

                return self._success_response(
                    involved_cols=selected,
                    message=f"Returned last {n} rows from '{table}'",
                    result=self._records_to_dataframe(records),
                    result_metadata={"row_count": len(records), "total_rows": total_rows},
                )
            else:
                raise self._unsupported_backend_error()
        except Exception as e:
            return self._error_response(f"dataframe_tail error: {str(e)}\n{traceback.format_exc()}")

    async def dataframe_sample(self, table: str, schema: str, n: int = 10, columns: Optional[List[str]] = None, random_state: Optional[int] = None,**kwargs,) -> Dict[str, Any]:
        
        try:
            if isinstance(self.db, PostgresAdapter) or isinstance(self.db, DuckDBAdapter):
                table = SQLIdentifierSanitizer.sanitize(table)
                qualified = self._qualified_table(table, schema)

                if random_state is not None:
                    # PostgreSQL only; DuckDB will ignore or you can implement differently
                    if isinstance(self.db, PostgresAdapter):
                        await self._exec(f"SELECT setseed({random_state})")
                    elif isinstance(self.db, DuckDBAdapter):
                        random_state = random_state
                    else:
                        raise self._unsupported_backend_error()

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

                # Use ORDER BY RANDOM() – works in both PostgreSQL and DuckDB
                query = f"""
                    SELECT {column_clause}
                    FROM {qualified}
                    ORDER BY RANDOM()
                    LIMIT {n}
                """
                rows = await self._fetch(query)
                records = self._rows_to_records(rows)

                msg = f"Returned {n} random samples from '{table}'"
                if random_state is not None:
                    msg += f" (random_state={random_state})"

                return self._success_response(
                    involved_cols=selected,
                    message=msg,
                    result=self._records_to_dataframe(records),
                    result_metadata={"row_count": len(records), "sample_size": n},
                )
            else:
                raise self._unsupported_backend_error()
        except Exception as e:
            return self._error_response(f"dataframe_sample error: {str(e)}\n{traceback.format_exc()}")

    
    async def dataframe_info(self, table: str, schema: str, columns: Optional[List[str]] = None, **kwargs) -> Dict[str, Any]:
        try:
            if isinstance(self.db, PostgresAdapter) or isinstance(self.db, DuckDBAdapter):
                backend = kwargs.get("backend")
                data_id = kwargs.get("data_id")
                requested_new_table = kwargs.get("new_table")
                table = SQLIdentifierSanitizer.sanitize(table)
                qualified = self._qualified_table(table, schema)

                table_info = await self._get_table_info(table, schema)
                if "error" in table_info:
                    return self._error_response(table_info["error"])

                column_details = []

                # =========================
                # EXISTING LOGIC (UNCHANGED)
                # =========================
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

                # =========================
                # 🔥 NEW: CONVERT TO DATAFRAME
                # =========================
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
                    result=df,   # 👈 DataFrame like head/tail
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
            if isinstance(self.db, PostgresAdapter) or isinstance(self.db, DuckDBAdapter):
                backend = kwargs.get("backend")
                data_id = kwargs.get("data_id")
                requested_new_table = kwargs.get("new_table")
                table = SQLIdentifierSanitizer.sanitize(table)
                qualified = self._qualified_table(table, schema)

                column_types = await self._get_column_types(table, schema)
                numeric_types = [
                    "integer", "bigint", "smallint", "decimal", "numeric",
                    "real", "double precision", "float", "float8", "float4"
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

                # =========================
                # EXISTING LOGIC (UNCHANGED)
                # =========================
                for col in sanitized:
                    if isinstance(self.db, PostgresAdapter):
                        q25_expr = f'PERCENTILE_CONT(0.25) WITHIN GROUP (ORDER BY "{col}")'
                        median_expr = f'PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY "{col}")'
                        q75_expr = f'PERCENTILE_CONT(0.75) WITHIN GROUP (ORDER BY "{col}")'
                    elif isinstance(self.db, DuckDBAdapter):
                        q25_expr = f'QUANTILE_CONT("{col}", 0.25)'
                        median_expr = f'QUANTILE_CONT("{col}", 0.5)'
                        q75_expr = f'QUANTILE_CONT("{col}", 0.75)'
                    else:
                        raise self._unsupported_backend_error()
                    stats_sql = f"""
                        SELECT
                            COUNT("{col}") as count,
                            AVG("{col}") as mean,
                            STDDEV_SAMP("{col}") as std,
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

                # =========================
                # 🔥 NEW: CONVERT TO DATAFRAME
                # =========================

                # Convert column-oriented -> row records
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

                # =========================
                # RETURN LIKE HEAD/TAIL
                # =========================
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
            if isinstance(self.db, PostgresAdapter) or isinstance(self.db, DuckDBAdapter):
                backend = kwargs.get("backend")
                data_id = kwargs.get("data_id")
                requested_new_table = kwargs.get("new_table")
                table = SQLIdentifierSanitizer.sanitize(table)
                qualified = self._qualified_table(table, schema)

                total_rows = await self._fetchval(f"SELECT COUNT(*) FROM {qualified}") or 0

                #  STEP 1: Get ACTUAL DB columns (single source of truth)
                actual_cols_rows = await self._fetch(f"""
                    SELECT column_name
                    FROM information_schema.columns
                    WHERE table_name = '{table}'
                    AND table_schema = '{schema}'
                """)
                actual_columns = [row["column_name"] for row in actual_cols_rows]

                if not actual_columns:
                    return self._error_response("No columns found in table")

                # 🔥 STEP 2: Resolve columns input
                if columns is None or columns == "*" or columns == ["*"]:
                    target_columns = actual_columns
                else:
                    # keep only valid ones
                    target_columns = [c for c in columns if c in actual_columns]

                    if not target_columns:
                        return self._error_response(
                            f"No valid columns found. Available columns: {actual_columns}"
                        )

                sanitized = [SQLIdentifierSanitizer.sanitize(c) for c in target_columns]

                # 🔥 STEP 3: Compute null stats
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

                # 🔥 STEP 4: Pandas-style DataFrame (indexed by column)
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

                # 🔥 STEP 5: Extra stats
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
    
    async def dataframe_correlation_analysis(self, table: str, schema: str, columns: Optional[List[str]] = None, method: str = "pearson", **kwargs) -> Dict[str, Any]:
        try:
            if isinstance(self.db, PostgresAdapter) or isinstance(self.db, DuckDBAdapter):
                backend = kwargs.get("backend")
                data_id = kwargs.get("data_id")
                requested_new_table = kwargs.get("new_table")
                table = SQLIdentifierSanitizer.sanitize(table)
                qualified = self._qualified_table(table, schema)

                column_types = await self._get_column_types(table, schema)

                numeric_types = [
                    "integer", "bigint", "smallint", "decimal", "numeric",
                    "real", "double precision", "float", "float8", "float4",
                    "int", "int4", "int8"
                ]

                if columns is None or columns == "*" or columns == ["*"]:
                    target_columns = [
                        c for c, d in column_types.items()
                        if any(nt in d.lower() for nt in numeric_types)
                    ]
                else:
                    target_columns = [
                        c for c in columns
                        if c in column_types and any(nt in column_types[c].lower() for nt in numeric_types)
                    ]

                if len(target_columns) < 2:
                    return self._error_response("Need at least 2 numeric columns for correlation analysis")

                sanitized = [SQLIdentifierSanitizer.sanitize(c) for c in target_columns]

                corr_matrix = {col: {} for col in sanitized}

                for i, c1 in enumerate(sanitized):
                    for j, c2 in enumerate(sanitized):
                        if i == j:
                            corr_matrix[c1][c2] = 1.0
                        elif i < j:
                            if isinstance(self.db, PostgresAdapter):
                                c1_expr = f'"{c1}"::DOUBLE PRECISION'
                                c2_expr = f'"{c2}"::DOUBLE PRECISION'
                            elif isinstance(self.db, DuckDBAdapter):
                                c1_expr = f'CAST("{c1}" AS DOUBLE)'
                                c2_expr = f'CAST("{c2}" AS DOUBLE)'
                            else:
                                raise self._unsupported_backend_error()
                            corr_sql = f"""
                                SELECT CORR({c1_expr}, {c2_expr})
                                FROM {qualified}
                                WHERE "{c1}" IS NOT NULL AND "{c2}" IS NOT NULL
                            """
                            try:
                                corr_val = await self._fetchval(corr_sql)
                                val = float(corr_val) if corr_val is not None else None
                                corr_matrix[c1][c2] = val
                                corr_matrix[c2][c1] = val
                            except Exception:
                                corr_matrix[c1][c2] = None
                                corr_matrix[c2][c1] = None

                df = pd.DataFrame(corr_matrix)

                # Ensure correct ordering
                df = df.loc[sanitized, sanitized]

                output_table = await self._save_dataframe_as_table(
                    df=df,
                    schema=schema,
                    base_table=f"{table}_corr",
                    backend=backend,
                    data_id=data_id,
                    new_table=requested_new_table,
                )

                # 🔥 STEP 5: Optional strong correlations (keep your feature)
                strong = []
                for i, c1 in enumerate(sanitized):
                    for j, c2 in enumerate(sanitized):
                        if i < j:
                            val = df.loc[c1, c2]
                            if val is not None and abs(val) > 0.5:
                                strong.append({
                                    "column1": c1,
                                    "column2": c2,
                                    "correlation": round(val, 3),
                                    "strength": (
                                        "strong_positive" if val > 0.7 else
                                        "moderate_positive" if val > 0.3 else
                                        "strong_negative" if val < -0.7 else
                                        "moderate_negative" if val < -0.3 else
                                        "weak"
                                    ),
                                })

                strong.sort(key=lambda x: abs(x["correlation"]), reverse=True)

                return self._success_response(
                    involved_cols=target_columns,
                    generated_cols=list(df.columns),
                    message=f"Correlation matrix for {len(target_columns)} numeric columns in '{table}'",
                    result=df,
                    new_table=output_table,
                    result_metadata={
                        "row_count": len(df),
                        "saved_table": output_table,
                        "columns": target_columns,
                        "strong_correlations": strong[:10],
                    },
                )

            else:
                raise self._unsupported_backend_error()
        except Exception as e:
            return self._error_response(f"dataframe_correlation_analysis error: {str(e)}\n{traceback.format_exc()}")
    
    async def dataframe_full_table(self, table: str, schema: str,columns: Optional[List[str]] = None, chunk_size: Optional[int] = None,**kwargs,) -> Dict[str, Any]:
        try:
            if isinstance(self.db, PostgresAdapter) or isinstance(self.db, DuckDBAdapter):
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
                            yield self._records_to_dataframe(records)
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
                df = self._records_to_dataframe(records)

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
            if isinstance(self.db, PostgresAdapter) or isinstance(self.db, DuckDBAdapter):
                table = SQLIdentifierSanitizer.sanitize(table)
                qualified = self._qualified_table(table, schema)

                if not dtype_map:
                    return self._error_response("dtype_map cannot be empty")

                column_types = await self._get_column_types(table, schema)
                actual_columns = set(column_types.keys())

                dtype_translation = {
                    "int": "INTEGER", "int8": "INTEGER", "int16": "INTEGER", "int32": "INTEGER",
                    "int64": "BIGINT",
                    "float": "FLOAT", "float32": "FLOAT",
                    "float64": "DOUBLE", "double": "DOUBLE",
                    "str": "TEXT", "string": "TEXT", "text": "TEXT"
                }

                normalized_map = {}

                for col, dtype in dtype_map.items():
                    if col not in actual_columns:
                        return self._error_response(f"Column '{col}' does not exist")

                    dtype_lower = dtype.lower()

                    if dtype_lower not in dtype_translation:
                        return self._error_response(f"Unsupported dtype '{dtype}'")

                    normalized_map[col] = dtype_translation[dtype_lower]

                # 🔥 ONLY CASTED COLUMNS (FIX HERE)
                select_parts = []
                for col, dtype in normalized_map.items():
                    col_safe = SQLIdentifierSanitizer.sanitize(col)
                    select_parts.append(f'CAST("{col_safe}" AS {dtype}) AS "{col_safe}"')

                query = f"SELECT {', '.join(select_parts)} FROM {qualified}"

                rows = await self._fetch(query)
                records = self._rows_to_records(rows)

                df = self._records_to_dataframe(records)

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

                # 🔥 STEP 1: Validate input type
                if not isinstance(value, list):
                    return self._error_response("Value must be a list")

                # 🔥 STEP 2: Get row count
                total_rows = await self._fetchval(f"SELECT COUNT(*) FROM {qualified}") or 0

                if len(value) != total_rows:
                    return self._error_response(
                        f"Length mismatch: Expected {total_rows}, got {len(value)}"
                    )

                # 🔥 STEP 3: Add column
                await self._exec(f'ALTER TABLE {qualified} ADD COLUMN "{column}" TEXT')

                # 🔥 STEP 4: Row-wise update
                if isinstance(self.db, PostgresAdapter):
                    id_col = "ctid"
                elif isinstance(self.db, DuckDBAdapter):
                    id_col = "rowid"
                else:
                    raise self._unsupported_backend_error()

                # Create temp mapping table
                temp_table = f"temp_insert_{column}"

                await self._exec(f"CREATE TEMP TABLE {temp_table} (idx INT, val TEXT)")

                # Insert values with index
                for i, v in enumerate(value):
                    placeholder1 = self.db.placeholder(1)
                    placeholder2 = self.db.placeholder(2)

                    await self._exec(
                        f"INSERT INTO {temp_table} VALUES ({placeholder1}, {placeholder2})",
                        i,
                        str(v)
                    )

                # Update main table using join
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

                # Cleanup
                await self._exec(f"DROP TABLE {temp_table}")

                return self._success_response(
                    message=f"Column '{column}' created successfully with {total_rows} values",
                    involved_cols=[],
                    generated_cols=[column],
                )

            else:
                raise self._unsupported_backend_error()
        except Exception as e:
            return self._error_response(str(e))
    
    async def dataframe_map(self,table: str,schema: str,func: str,na_action: Optional[str] = None,columns: Optional[List[str]] = None,datetime_action: str = "skip",**kwargs) -> Dict[str, Any]:
        
        try:
            if isinstance(self.db, PostgresAdapter) or isinstance(self.db, DuckDBAdapter):
                table = SQLIdentifierSanitizer.sanitize(table)
                qualified = self._qualified_table(table, schema)

                if not isinstance(func, str):
                    return self._error_response("func must be a SQL expression string using 'x' as placeholder")

                # =========================
                # STEP 1: GET COLUMN TYPES
                # =========================
                column_types = await self._get_column_types(table, schema)

                numeric_types = [
                    "integer", "bigint", "smallint", "decimal", "numeric",
                    "real", "double precision", "float", "float8", "float4",
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

                # =========================
                # STEP 2: RESOLVE COLUMNS
                # =========================
                if columns is None or columns == "*" or columns == ["*"]:
                    target_columns = list(column_types.keys())
                else:
                    target_columns = [c for c in columns if c in column_types]

                if not target_columns:
                    return self._error_response("No valid columns selected")

                # =========================
                # STEP 3: BUILD QUERY
                # =========================
                select_parts = []
                applied_cols = []
                skipped_cols = []

                for col in target_columns:
                    col_safe = SQLIdentifierSanitizer.sanitize(col)
                    dtype = column_types[col].lower()

                    expr = func.replace("x", f'"{col_safe}"')

                    # -------------------------
                    # NUMERIC
                    # -------------------------
                    if any(nt in dtype for nt in numeric_types):
                        final_expr = expr

                    # -------------------------
                    # STRING
                    # -------------------------
                    elif any(st in dtype for st in string_types):
                        if any(fn in func.upper() for fn in ["UPPER", "LOWER", "LENGTH", "TRIM"]):
                            final_expr = expr
                        else:
                            skipped_cols.append(col)
                            continue

                    # -------------------------
                    # BOOLEAN
                    # -------------------------
                    elif any(bt in dtype for bt in boolean_types):
                        # Auto-cast boolean → integer for numeric ops
                        if any(op in func for op in ["*", "+", "-", "/", "%"]):
                            expr = func.replace("x", f'CAST("{col_safe}" AS INTEGER)')
                            final_expr = expr
                        else:
                            skipped_cols.append(col)
                            continue

                    # -------------------------
                    # DATETIME 🔥
                    # -------------------------
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

                    # -------------------------
                    # OTHER TYPES
                    # -------------------------
                    else:
                        skipped_cols.append(col)
                        continue

                    # -------------------------
                    # NA HANDLING
                    # -------------------------
                    if na_action == "ignore":
                        final_expr = f'CASE WHEN "{col_safe}" IS NULL THEN NULL ELSE {final_expr} END'

                    select_parts.append(f"{final_expr} AS \"{col_safe}\"")
                    applied_cols.append(col)

                # =========================
                # STEP 4: VALIDATE
                # =========================
                if not select_parts:
                    return self._error_response("No columns compatible with the given expression")

                # =========================
                # STEP 5: EXECUTE
                # =========================
                query = f"SELECT {', '.join(select_parts)} FROM {qualified}"

                rows = await self._fetch(query)
                records = self._rows_to_records(rows)
                df = self._records_to_dataframe(records)

                # =========================
                # STEP 6: RESPONSE
                # =========================
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
            if isinstance(self.db, PostgresAdapter) or isinstance(self.db, DuckDBAdapter):
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
            else:
                raise self._unsupported_backend_error()
        except Exception as e:
            return self._error_response(str(e))

    
    async def dataframe_reset_index(self, table: str, schema: str, **kwargs) -> Dict[str, Any]:
        try:
            if isinstance(self.db, PostgresAdapter) or isinstance(self.db, DuckDBAdapter):
                table = SQLIdentifierSanitizer.sanitize(table)
                qualified = self._qualified_table(table, schema)

                temp_table = f"{table}_temp_reset"

                # =========================
                # POSTGRES
                # =========================
                if isinstance(self.db, PostgresAdapter):

                    # Drop existing id if exists
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

                    # Add new serial column
                    await self._exec(f'ALTER TABLE {qualified} ADD COLUMN id SERIAL')

                # =========================
                # DUCKDB 🔥
                # =========================
                elif isinstance(self.db, DuckDBAdapter):
                    qualified_temp = f'{self.db.quote_identifier(schema)}.{self.db.quote_identifier(temp_table)}'

                    # Create new table with row_number
                    await self._exec(f"""
                        CREATE TABLE {qualified_temp} AS
                        SELECT 
                            ROW_NUMBER() OVER () AS id,
                            *
                        FROM {qualified}
                    """)

                    # Drop old table
                    await self._exec(f"DROP TABLE {qualified}")

                    # Rename new table
                    await self._exec(f"""
                    ALTER TABLE {qualified_temp}
                    RENAME TO {self.db.quote_identifier(table)}
                """)
                else:
                    raise self._unsupported_backend_error()

                # =========================
                # RETURN
                # =========================
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

                # 🔥 STEP 1: Get columns
                target_cols = await self._get_column_types(table, schema)
                source_cols = await self._get_column_types(other_table, other_schema)

                common_cols = [c for c in target_cols if c in source_cols and c != on]

                if not common_cols:
                    return self._success_response(
                        message="No overlapping columns to update",
                        involved_cols=[],
                        generated_cols=[]
                    )

                # 🔥 STEP 2: Error handling (non-null conflict)
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

                # 🔥 STEP 3: Build assignments
                assignments = []

                for col in common_cols:
                    col_safe = SQLIdentifierSanitizer.sanitize(col)

                    if overwrite:
                        # pandas default
                        expr = f"""
                        "{col_safe}" = CASE
                            WHEN {t2}."{col_safe}" IS NOT NULL THEN {t2}."{col_safe}"
                            ELSE {t1}."{col_safe}"
                        END
                        """
                    else:
                        # only update NULLs in original
                        expr = f"""
                        "{col_safe}" = CASE
                            WHEN {t1}."{col_safe}" IS NULL AND {t2}."{col_safe}" IS NOT NULL
                            THEN {t2}."{col_safe}"
                            ELSE {t1}."{col_safe}"
                        END
                        """

                    assignments.append(expr)

                assignment_sql = ", ".join(assignments)

                # 🔥 STEP 4: Execute update
                await self._exec(f"""
                    UPDATE {t1}
                    SET {assignment_sql}
                    FROM {t2}
                    WHERE {t1}."{on}" = {t2}."{on}"
                """)
            
                # 🔥 STEP 5: FETCH SAMPLE (like head)
                sample_rows = await self._fetch(f"""
                    SELECT *
                    FROM {t1}
                    LIMIT 10
                """)

                sample_df = self._records_to_dataframe(self._rows_to_records(sample_rows))
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

            else:
                raise self._unsupported_backend_error()
        except Exception as e:
            return self._error_response(str(e))
    
    async def dataframe_resample(self,table: str,schema: str,time_column: str,    rule: str,
        agg: str = "COUNT",    value_column: Optional[str] = None,    label: str = "left",
        closed: str = "left",    **kwargs,) -> Dict[str, Any]:
        try:
            if isinstance(self.db, PostgresAdapter) or isinstance(self.db, DuckDBAdapter):
                table = SQLIdentifierSanitizer.sanitize(table)
                time_column = SQLIdentifierSanitizer.sanitize(time_column)
                qualified = self._qualified_table(table, schema)

                # =========================
                # STEP 1: VALIDATION
                # =========================
                column_types = await self._get_column_types(table, schema)

                if time_column not in column_types:
                    return self._error_response(f"Column '{time_column}' not found")

                dtype = column_types[time_column].lower()
                if "date" not in dtype and "time" not in dtype:
                    return self._error_response(f"Column '{time_column}' must be datetime-like")

                if label not in ["left", "right"]:
                    return self._error_response("label must be 'left' or 'right'")

                if closed not in ["left", "right"]:
                    return self._error_response("closed must be 'left' or 'right'")

                # =========================
                # STEP 2: AGG COLUMN
                # =========================
                if agg.upper() == "COUNT":
                    agg_expr = "COUNT(*)"
                else:
                    if not value_column:
                        return self._error_response("value_column required for aggregation other than COUNT")

                    value_column = SQLIdentifierSanitizer.sanitize(value_column)
                    agg_expr = f"{agg.upper()}(\"{value_column}\")"

                # =========================
                # STEP 3: BASE BUCKET
                # =========================
                bucket_expr = f"DATE_TRUNC('{rule}', \"{time_column}\")"

                # =========================
                # STEP 4: LABEL HANDLING
                # =========================
                if label == "right":
                    # shift bucket forward by interval
                    bucket_expr = f"{bucket_expr} + INTERVAL '1 {rule}'"

                # =========================
                # STEP 5: BUILD QUERY
                # =========================
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
                df = self._records_to_dataframe(records)

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
            if isinstance(self.db, PostgresAdapter) or isinstance(self.db, DuckDBAdapter):
                columns = list((await self._get_column_types(table, schema)).keys())
                count = await self._fetchval(
                    f"SELECT COUNT(*) FROM {self._qualified_table(table, schema)}"
                )
                index = list(range(count))
                return self._success_response(result={"axes": [index, columns]})
            else:
                raise self._unsupported_backend_error()
        except Exception as e:
            return self._error_response(str(e))

    async def dataframe_columns(self, table: str, schema: str, **kwargs) -> Dict[str, Any]:
        try:
            if isinstance(self.db, PostgresAdapter) or isinstance(self.db, DuckDBAdapter):
                cols = list((await self._get_column_types(table, schema)).keys())
                return self._success_response(result={"columns": cols})
            else:
                raise self._unsupported_backend_error()
        except Exception as e:
            return self._error_response(str(e))

    async def dataframe_dtypes(self, table: str, schema: str, **kwargs) -> Dict[str, Any]:
        try:
            if isinstance(self.db, PostgresAdapter) or isinstance(self.db, DuckDBAdapter):
                col_types = await self._get_column_types(table, schema)
                return self._success_response(result={"dtypes": col_types})
            else:
                raise self._unsupported_backend_error()
        except Exception as e:
            return self._error_response(str(e))

    async def dataframe_first_valid_index(self, table: str, schema: str, **kwargs) -> Dict[str, Any]:
        try:
            if isinstance(self.db, PostgresAdapter) or isinstance(self.db, DuckDBAdapter):
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
            if isinstance(self.db, PostgresAdapter) or isinstance(self.db, DuckDBAdapter):
                if isinstance(self.db, PostgresAdapter):
                    relation = f'"{schema}"."{table}"'
                    size = await self._fetchval(
                        f"SELECT pg_total_relation_size('{relation}') as total_bytes"
                    )
                elif isinstance(self.db, DuckDBAdapter):
                    # DuckDB does not expose relation size easily
                    size = None
                else:
                    raise self._unsupported_backend_error()
                return self._success_response(result={"memory_bytes": size})
            else:
                raise self._unsupported_backend_error()
        except Exception as e:
            return self._error_response(str(e))

    async def dataframe_ndim(self, table: str, schema: str, **kwargs) -> Dict[str, Any]:
        if isinstance(self.db, PostgresAdapter) or isinstance(self.db, DuckDBAdapter):
            return self._success_response(result={"ndim": 2})

        else:
            raise self._unsupported_backend_error()
    async def dataframe_shape(self, table: str, schema: str, **kwargs) -> Dict[str, Any]:
        try:
            if isinstance(self.db, PostgresAdapter) or isinstance(self.db, DuckDBAdapter):
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
            if isinstance(self.db, PostgresAdapter) or isinstance(self.db, DuckDBAdapter):
                shape_res = await self.dataframe_shape(table, schema)
                shape = shape_res["result"]["shape"]
                return self._success_response(result={"size": shape[0] * shape[1]})
            else:
                raise self._unsupported_backend_error()
        except Exception as e:
            return self._error_response(str(e))

    async def dataframe_values(self, table: str, schema: str, **kwargs) -> Dict[str, Any]:
        try:
            if isinstance(self.db, PostgresAdapter) or isinstance(self.db, DuckDBAdapter):
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
            if isinstance(self.db, PostgresAdapter) or isinstance(self.db, DuckDBAdapter):
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
            if isinstance(self.db, PostgresAdapter) or isinstance(self.db, DuckDBAdapter):
                table = SQLIdentifierSanitizer.sanitize(table)
                qualified = self._qualified_table(table, schema)

                columns = list((await self._get_column_types(table, schema)).keys())

                async def row_generator():
                    idx = 0

                    async for row in self.db.fetch_iter(
                        f"SELECT * FROM {qualified}",
                        chunk_size=chunk_size
                    ):
                        # 🔥 Handle DB differences
                        if isinstance(self.db, PostgresAdapter):
                            row_dict = dict(row)   # asyncpg.Record → dict
                        elif isinstance(self.db, DuckDBAdapter):
                            # DuckDB tuple → map with columns
                            row_dict = dict(zip(columns, row))
                        else:
                            raise self._unsupported_backend_error()

                        yield idx, row_dict
                        idx += 1

                return self._success_response(
                    message="Streaming iterrows generator",
                    result=row_generator(),   # 🔥 return generator directly
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
            if isinstance(self.db, PostgresAdapter) or isinstance(self.db, DuckDBAdapter):
                table = SQLIdentifierSanitizer.sanitize(table)
                qualified = self._qualified_table(table, schema)

                # 🔥 Get columns
                columns = list((await self._get_column_types(table, schema)).keys())

                # 🔥 Handle namedtuple creation
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
                        # 🔥 Normalize row
                        if isinstance(self.db, PostgresAdapter):
                            values = [row[col] for col in columns]
                        elif isinstance(self.db, DuckDBAdapter):
                            values = list(row)
                        else:
                            raise self._unsupported_backend_error()

                        # 🔥 Add index if needed
                        if index:
                            values = [idx] + values

                        # 🔥 Namedtuple or plain tuple
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

    
