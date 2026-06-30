from typing import Any, Dict, List, Optional, Union
import traceback
from datetime import datetime, UTC
import pandas as pd

from src.db_manager.adapters.base import DatabaseAdapter
from src.db_manager.adapters.duckdb import DuckDBAdapter
from src.db_manager.adapters.postgresql import PostgresAdapter
from src.utils.helper import SQLIdentifierSanitizer


class DataCleaningOps:
    """
    Core data cleaning operations executed directly on the database.
    Each method returns a standardised response dict with a sample DataFrame.
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

    async def _fetch_data(self, table: str, schema: str, columns: Any = "*") -> pd.DataFrame:
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

        rows = await self._fetch(f"SELECT {column_clause} FROM {qualified}")
        records = [dict(row) for row in rows]
        return pd.DataFrame.from_records(records)

    async def _get_column_type(self, table: str, schema: str, column: str) -> str:
        types = await self.db.get_column_types(table, schema)
        return types.get(column, "TEXT")

    async def _backend_fetch_val(self, backend, sql: str, *args):
        if hasattr(backend, "fetch_val"):
            return await backend.fetch_val(sql, *args)
        return await backend.fetchval(sql, *args)

    async def _generate_transient_table_name(self, base_table: str, backend, data_id: str) -> str:
        max_op = await self._backend_fetch_val(
            backend,
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
            candidate = f"{safe_table}__op_{datetime.now(UTC).strftime('%Y%m%d%H%M%S%f')}"

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
        await self._exec(f'ALTER TABLE {qualified} ADD COLUMN "{safe_col}" {col_type}')

    def _success_response(
        self,
        message: str,
        involved_cols: Optional[List[str]] = None,
        generated_cols: Optional[List[str]] = None,
        sample_df: Optional[pd.DataFrame] = None,
        result: Any = None,
        **extra,
    ) -> Dict[str, Any]:
        payload = {
            "is_error": False,
            "message": message,
            "error_message": None,
            "involved_cols": involved_cols or [],
            "generated_cols": generated_cols or [],
        }
        if result is not None:
            payload["result"] = result
        else:
            payload["result"] = sample_df if sample_df is not None else pd.DataFrame()
        payload.update(extra)
        return payload

    def _error_response( self, error_message: str,   involved_cols: List[str] = None, generated_cols: List[str] = None,) -> Dict[str, Any]:
        return {
            "is_error": True,
            "message": "",
            "error_message": error_message,
            "involved_cols": involved_cols or [],
            "generated_cols": generated_cols or [],
        }

    # ------------------------------------------------------------------
    # Numeric cleaning
    # ------------------------------------------------------------------
    async def numeric_fillna(
        self,
        table: str,
        schema: str,
        column: str,
        value: Any = None,
        mode: str = "mean",
        backend=None,
        data_id: Optional[str] = None,
        new_table: Optional[str] = None,
    ) -> Dict[str, Any]:
        try:
            table = await self._prepare_operation_table(
                table,
                schema,
                backend=backend,
                data_id=data_id,
                new_table=new_table,
            )
            mode = mode.upper()

            # ----------------------------------------
            # Generate column name
            # ----------------------------------------
            suffix_map = {
                "CONSTANT": f"constant_filled_{value}",
                "MEAN": "mean_filled",
                "AVG": "mean_filled",
                "AVERAGE": "mean_filled",
                "MEDIAN": "median_filled",
                "MODE": "mode_filled",
                "STD": "std_filled",
                "VAR": "var_filled",
                "VARIANCE": "var_filled",
                "MIN": "min_filled",
                "MAX": "max_filled",
                "BFILL": "bfill_filled",
                "FFILL": "ffill_filled"
                
            }

            if mode not in suffix_map:
                return self._error_response(f"Unsupported mode: {mode}")

            new_col = self._generate_cleaned_column_name(column, suffix_map[mode])

            # ----------------------------------------
            # Add new column
            # ----------------------------------------
            col_type = await self._get_column_type(table, schema, column)
            await self._add_new_column(table, schema, new_col, col_type)

            qualified = self._qualified_table(table, schema)
            safe_col = SQLIdentifierSanitizer.sanitize(column)
            safe_new = SQLIdentifierSanitizer.sanitize(new_col)

            # ----------------------------------------
            # CONSTANT mode
            # ----------------------------------------
            if mode == "CONSTANT":
                if value is None:
                    return self._error_response("Value must be provided for CONSTANT mode")

                converted = f"'{value}'" if isinstance(value, str) else str(value)

                if isinstance(self.db, PostgresAdapter) or isinstance(self.db, DuckDBAdapter):
                    await self._exec(
                        f'UPDATE {qualified} SET "{safe_new}" = COALESCE("{safe_col}", {converted})'
                    )
                else:
                    raise self._unsupported_backend_error()

                fill_value = value
            # ----------------------------------------
            # FFILL / BFILL (INDEX GENERATED RUNTIME)
            # ----------------------------------------
            elif mode in ["FFILL", "BFILL"]:

                # 🔥 Row identifier (engine specific)
                if isinstance(self.db, PostgresAdapter):
                    row_id = "ctid"
                elif isinstance(self.db, DuckDBAdapter):
                    row_id = "rowid"
                else:
                    raise self._unsupported_backend_error()

                # 🔥 Window expression
                if isinstance(self.db, PostgresAdapter):
                    # Postgres fallback (no IGNORE NULLS)
                    if mode == "FFILL":
                        window_expr = f'''
                            MAX("{safe_col}") OVER (
                                ORDER BY __idx
                                ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
                            )
                        '''
                    else:  # BFILL
                        window_expr = f'''
                            MIN("{safe_col}") OVER (
                                ORDER BY __idx
                                ROWS BETWEEN CURRENT ROW AND UNBOUNDED FOLLOWING
                            )
                        '''
                elif isinstance(self.db, DuckDBAdapter):
                    # DuckDB (supports IGNORE NULLS)
                    if mode == "FFILL":
                        window_expr = f'''
                            LAST_VALUE("{safe_col}" IGNORE NULLS)
                            OVER (
                                ORDER BY __idx
                                ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
                            )
                        '''
                    else:  # BFILL
                        window_expr = f'''
                            FIRST_VALUE("{safe_col}" IGNORE NULLS)
                            OVER (
                                ORDER BY __idx
                                ROWS BETWEEN CURRENT ROW AND UNBOUNDED FOLLOWING
                            )
                        '''
                else:
                    raise self._unsupported_backend_error()

                # 🔥 Main execution
                await self._exec(f"""
                    WITH base AS (
                        SELECT *,
                            ROW_NUMBER() OVER () AS __idx,
                            {row_id} AS __rid
                        FROM {qualified}
                    ),
                    filled AS (
                        SELECT *,
                            {window_expr} AS filled_val
                        FROM base
                    )
                    UPDATE {qualified} t
                    SET "{safe_new}" = COALESCE(t."{safe_col}", f.filled_val)
                    FROM filled f
                    WHERE t.{row_id} = f.__rid
                """)

                fill_value = mode
            # ----------------------------------------
            # STATISTICAL MODES
            # ----------------------------------------
            else:
                if isinstance(self.db, PostgresAdapter) or isinstance(self.db, DuckDBAdapter):
                    stat_map = {
                        "MEAN": f'AVG("{safe_col}")',
                        "AVG": f'AVG("{safe_col}")',
                        "AVERAGE": f'AVG("{safe_col}")',
                        "MEDIAN": f'PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY "{safe_col}")',
                        "MODE": f"""
                            (SELECT "{safe_col}"
                            FROM {qualified}
                            WHERE "{safe_col}" IS NOT NULL
                            GROUP BY "{safe_col}"
                            ORDER BY COUNT(*) DESC
                            LIMIT 1)
                        """,
                        "STD": f'STDDEV_POP("{safe_col}")',
                        "VAR": f'VAR_POP("{safe_col}")',
                        "VARIANCE": f'VAR_POP("{safe_col}")',
                        "MIN": f'MIN("{safe_col}")',
                        "MAX": f'MAX("{safe_col}")',
                    }
                else:
                    raise self._unsupported_backend_error()

                stat_expr = stat_map[mode]

                await self._exec(f"""
                    WITH stat_val AS (
                        SELECT COALESCE({stat_expr}, 0) AS val
                        FROM {qualified}
                        WHERE "{safe_col}" IS NOT NULL
                    )
                    UPDATE {qualified}
                    SET "{safe_new}" = COALESCE("{safe_col}", (SELECT val FROM stat_val))
                """)

                # Fetch stat value for message
                fill_value = await self._fetchval(f"""
                    SELECT {stat_expr}
                    FROM {qualified}
                    WHERE "{safe_col}" IS NOT NULL
                """)

            # ----------------------------------------
            # Metrics + response
            # ----------------------------------------
            null_count = await self._fetchval(
                f'SELECT COUNT(*) FROM {qualified} WHERE "{safe_col}" IS NULL'
            ) or 0

            sample = await self._fetch_data(table, schema,columns=[safe_col,safe_new])

            msg = f"Filled {null_count} null values in '{column}' using {mode} ({fill_value})"

            return self._success_response(
                msg,
                [column],
                [new_col],
                sample,
                fill_mode=mode,
                fill_value=fill_value,
                new_table=table,
            )

        except Exception as e:
            return self._error_response(
                f"numeric_fillna error: {str(e)}\n{traceback.format_exc()}",
                [column],
                [],
            )
            
    async def numeric_enforce_range(
        self,
        table: str,
        schema: str,
        column: str,
        min_value: int | float = None,
        max_value: int | float = None,
        backend=None,
        data_id: Optional[str] = None,
        new_table: Optional[str] = None,
    ) -> Dict[str, Any]:
        try:
            table = await self._prepare_operation_table(
                table,
                schema,
                backend=backend,
                data_id=data_id,
                new_table=new_table,
            )
            new_col = self._generate_cleaned_column_name(column, f"range_{min_value}_{max_value}")
            col_type = await self._get_column_type(table, schema, column)
            await self._add_new_column(table, schema, new_col, col_type)

            qualified = self._qualified_table(table, schema)
            safe_col = SQLIdentifierSanitizer.sanitize(column)
            safe_new = SQLIdentifierSanitizer.sanitize(new_col)

            case_parts = []
            if min_value is not None:
                case_parts.append(f'WHEN "{safe_col}" < {min_value} THEN NULL')
            if max_value is not None:
                case_parts.append(f'WHEN "{safe_col}" > {max_value} THEN NULL')
            case_expr = f"CASE {' '.join(case_parts)} ELSE \"{safe_col}\" END" if case_parts else f'"{safe_col}"'

            if isinstance(self.db, PostgresAdapter) or isinstance(self.db, DuckDBAdapter):
                await self._exec(f'UPDATE {qualified} SET "{safe_new}" = {case_expr}')
            else:
                raise self._unsupported_backend_error()

            affected = 0
            if min_value is not None or max_value is not None:
                cond = []
                if min_value is not None:
                    cond.append(f'"{safe_col}" < {min_value}')
                if max_value is not None:
                    cond.append(f'"{safe_col}" > {max_value}')
                where_clause = " OR ".join(cond)
                if isinstance(self.db, PostgresAdapter) or isinstance(self.db, DuckDBAdapter):
                    affected = await self._fetchval(
                        f'SELECT COUNT(*) FROM {qualified} WHERE {where_clause}'
                    ) or 0
                else:
                    raise self._unsupported_backend_error()

            sample = await self._fetch_data(table, schema, columns=[safe_col,safe_new])
            msg = f"Enforced range on '{column}': {affected} values set to NULL"
            return self._success_response(msg, [column], [new_col], sample, new_table=table)

        except Exception as e:
            return self._error_response(f"numeric_enforce_range error: {str(e)}\n{traceback.format_exc()}")

    async def numeric_drop_outliers_zscore(
        self,
        table: str,
        schema: str,
        column: str,
        z_thresh: float = 3.0,
        backend=None,
        data_id: Optional[str] = None,
        new_table: Optional[str] = None,
    ) -> Dict[str, Any]:
        try:
            table = await self._prepare_operation_table(
                table,
                schema,
                backend=backend,
                data_id=data_id,
                new_table=new_table,
            )
            new_col = self._generate_cleaned_column_name(column, f"zscore_filtered_{z_thresh}")
            col_type = await self._get_column_type(table, schema, column)
            await self._add_new_column(table, schema, new_col, col_type)

            qualified = self._qualified_table(table, schema)
            safe_col = SQLIdentifierSanitizer.sanitize(column)
            safe_new = SQLIdentifierSanitizer.sanitize(new_col)

            if isinstance(self.db, PostgresAdapter) or isinstance(self.db, DuckDBAdapter):
                await self._exec(f"""
                    WITH stats AS (
                        SELECT AVG("{safe_col}") AS mean, STDDEV_POP("{safe_col}") AS sd
                        FROM {qualified}
                        WHERE "{safe_col}" IS NOT NULL
                    )
                    UPDATE {qualified}
                    SET "{safe_new}" = CASE
                        WHEN ABS(("{safe_col}" - stats.mean) / NULLIF(stats.sd, 0)) > {z_thresh} THEN NULL
                        ELSE "{safe_col}"
                    END
                    FROM stats
                """)

                outlier_count = await self._fetchval(f"""
                    WITH stats AS (
                        SELECT AVG("{safe_col}") AS mean, STDDEV_POP("{safe_col}") AS sd
                        FROM {qualified}
                        WHERE "{safe_col}" IS NOT NULL
                    )
                    SELECT COUNT(*)
                    FROM {qualified}, stats
                    WHERE "{safe_col}" IS NOT NULL
                      AND ABS(("{safe_col}" - stats.mean) / NULLIF(stats.sd, 0)) > {z_thresh}
                """) or 0
            else:
                raise self._unsupported_backend_error()

            sample = await self._fetch_data(table, schema, columns=[safe_col,safe_new])
            msg = f"Removed {outlier_count} outliers from '{column}' using Z-score (threshold={z_thresh})"
            return self._success_response(msg, [column], [new_col], sample, new_table=table)

        except Exception as e:
            return self._error_response(f"numeric_drop_outliers_zscore error: {str(e)}\n{traceback.format_exc()}")

    async def numeric_convert_text(
        self,
        table: str,
        schema: str,
        column: str,
        backend=None,
        data_id: Optional[str] = None,
        new_table: Optional[str] = None,
    ) -> Dict[str, Any]:
        try:
            table = await self._prepare_operation_table(
                table,
                schema,
                backend=backend,
                data_id=data_id,
                new_table=new_table,
            )
            new_col = self._generate_cleaned_column_name(column, "numeric_converted")
            await self._add_new_column(table, schema, new_col, "NUMERIC")

            qualified = self._qualified_table(table, schema)
            safe_col = SQLIdentifierSanitizer.sanitize(column)
            safe_new = SQLIdentifierSanitizer.sanitize(new_col)

            # Keep only numeric tokens, then cast only if final shape is numeric.
            if isinstance(self.db, PostgresAdapter):
                cleaned_expr = f"""NULLIF(
                    REGEXP_REPLACE("{safe_col}"::TEXT, '[^0-9.+-]', '', 'g'),
                    ''
                )"""
                numeric_check = f"{cleaned_expr} ~ '^[+-]?([0-9]+([.][0-9]*)?|[.][0-9]+)$'"
            elif isinstance(self.db, DuckDBAdapter):
                cleaned_expr = f"""NULLIF(
                    REGEXP_REPLACE(CAST("{safe_col}" AS VARCHAR), '[^0-9.+-]', '', 'g'),
                    ''
                )"""
                numeric_check = f"REGEXP_MATCHES({cleaned_expr}, '^[+-]?([0-9]+([.][0-9]*)?|[.][0-9]+)$')"
            else:
                raise self._unsupported_backend_error()

            await self._exec(f"""
                UPDATE {qualified}
                SET "{safe_new}" = CASE
                    WHEN {numeric_check}
                        THEN CAST({cleaned_expr} AS NUMERIC)
                    ELSE NULL
                END
            """)

            converted = await self._fetchval(
                f'SELECT COUNT(*) FROM {qualified} WHERE "{safe_new}" IS NOT NULL'
            ) or 0

            sample = await self._fetch_data(table, schema, columns=[safe_col,safe_new])
            msg = f"Converted {converted} text values in '{column}' to numeric"
            return self._success_response(msg, [column], [new_col], sample, new_table=table)

        except Exception as e:
            return self._error_response(f"numeric_convert_text error: {str(e)}\n{traceback.format_exc()}")

    # ------------------------------------------------------------------
    # Categorical cleaning
    # ------------------------------------------------------------------
    async def categorical_fillna(
        self,
        table: str,
        schema: str,
        column: str,
        mode: str = "mode",
        value: Optional[Any] = None,
        mapping: Optional[Dict[Any, Any]] = None,
        backend=None,
        data_id: Optional[str] = None,
        new_table: Optional[str] = None,
    ) -> Dict[str, Any]:
        try:
            table = await self._prepare_operation_table(
                table,
                schema,
                backend=backend,
                data_id=data_id,
                new_table=new_table,
            )
            mode = mode.upper()
            # ----------------------------------------
            # Validate mode
            # ----------------------------------------
            valid_modes = {"CONSTANT", "MODE", "MAP","BFILL","FFILL"}
            if mode not in valid_modes:
                return self._error_response(f"Unsupported mode: {mode}")

            # ----------------------------------------
            # Generate column name
            # ----------------------------------------
            suffix_map = {
                "CONSTANT": "constant_filled",
                "MODE": "mode_filled",
                "MAP": "mapped",
                "BFILL": "bfill_filled",
                "FFILL": "ffill_filled"
            }

            new_col = self._generate_cleaned_column_name(column, suffix_map[mode])

            # ----------------------------------------
            # Add new column
            # ----------------------------------------
            col_type = await self._get_column_type(table, schema, column)
            await self._add_new_column(table, schema, new_col, col_type)

            qualified = self._qualified_table(table, schema)
            safe_col = SQLIdentifierSanitizer.sanitize(column)
            safe_new = SQLIdentifierSanitizer.sanitize(new_col)

            fill_value = None

            # ----------------------------------------
            # CONSTANT mode
            # ----------------------------------------
            if mode == "CONSTANT":
                if value is None:
                    return self._error_response("Value must be provided for CONSTANT mode")

                val_str = str(value).replace("'", "''")

                if isinstance(self.db, PostgresAdapter) or isinstance(self.db, DuckDBAdapter):
                    await self._exec(
                        f'UPDATE {qualified} SET "{safe_new}" = COALESCE("{safe_col}", \'{val_str}\')'
                    )
                else:
                    raise self._unsupported_backend_error()

                fill_value = value
            # ----------------------------------------
            # FFILL / BFILL (INDEX GENERATED RUNTIME)
            # ----------------------------------------
            elif mode in ["FFILL", "BFILL"]:

                # Row identifier (engine specific)
                if isinstance(self.db, PostgresAdapter):
                    row_id = "ctid"
                elif isinstance(self.db, DuckDBAdapter):
                    row_id = "rowid"
                else:
                    raise self._unsupported_backend_error()

                #  Window expression
                if isinstance(self.db, PostgresAdapter):
                    # Postgres fallback (no IGNORE NULLS)
                    if mode == "FFILL":
                        window_expr = f'''
                            MAX("{safe_col}") OVER (
                                ORDER BY __idx
                                ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
                            )
                        '''
                    else:  # BFILL
                        window_expr = f'''
                            MIN("{safe_col}") OVER (
                                ORDER BY __idx
                                ROWS BETWEEN CURRENT ROW AND UNBOUNDED FOLLOWING
                            )
                        '''
                elif isinstance(self.db, DuckDBAdapter):
                    # DuckDB (supports IGNORE NULLS)
                    if mode == "FFILL":
                        window_expr = f'''
                            LAST_VALUE("{safe_col}" IGNORE NULLS)
                            OVER (
                                ORDER BY __idx
                                ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
                            )
                        '''
                    else:  # BFILL
                        window_expr = f'''
                            FIRST_VALUE("{safe_col}" IGNORE NULLS)
                            OVER (
                                ORDER BY __idx
                                ROWS BETWEEN CURRENT ROW AND UNBOUNDED FOLLOWING
                            )
                        '''
                else:
                    raise self._unsupported_backend_error()

                # Main execution
                await self._exec(f"""
                    WITH base AS (
                        SELECT *,
                            ROW_NUMBER() OVER () AS __idx,
                            {row_id} AS __rid
                        FROM {qualified}
                    ),
                    filled AS (
                        SELECT *,
                            {window_expr} AS filled_val
                        FROM base
                    )
                    UPDATE {qualified} t
                    SET "{safe_new}" = COALESCE(t."{safe_col}", f.filled_val)
                    FROM filled f
                    WHERE t.{row_id} = f.__rid
                """)

                fill_value = mode
            # ----------------------------------------
            # MODE mode
            # ----------------------------------------
            elif mode == "MODE":
                if isinstance(self.db, PostgresAdapter) or isinstance(self.db, DuckDBAdapter):
                    await self._exec(f"""
                        WITH mode_val AS (
                            SELECT "{safe_col}" AS mode_value
                            FROM {qualified}
                            WHERE "{safe_col}" IS NOT NULL
                            GROUP BY "{safe_col}"
                            ORDER BY COUNT(*) DESC
                            LIMIT 1
                        )
                        UPDATE {qualified}
                        SET "{safe_new}" = COALESCE("{safe_col}", (SELECT mode_value FROM mode_val))
                    """)

                    fill_value = await self._fetchval(f"""
                        SELECT "{safe_col}" AS mode_value
                        FROM {qualified}
                        WHERE "{safe_col}" IS NOT NULL
                        GROUP BY "{safe_col}"
                        ORDER BY COUNT(*) DESC
                        LIMIT 1
                    """)
                else:
                    raise self._unsupported_backend_error()

            # ----------------------------------------
            # MAP mode (extra powerful)
            # ----------------------------------------
            elif mode == "MAP":
                if not mapping:
                    return self._error_response("Mapping must be provided for MAP mode")

                case_parts = []
                for old, new in mapping.items():
                    old_esc = str(old).replace("'", "''")
                    new_esc = str(new).replace("'", "''")
                    case_parts.append(f'WHEN "{safe_col}" = \'{old_esc}\' THEN \'{new_esc}\'')

                case_expr = f"CASE {' '.join(case_parts)} ELSE \"{safe_col}\" END"

                if isinstance(self.db, PostgresAdapter) or isinstance(self.db, DuckDBAdapter):
                    await self._exec(f"""
                        UPDATE {qualified}
                        SET "{safe_new}" = COALESCE({case_expr}, "{safe_col}")
                    """)
                else:
                    raise self._unsupported_backend_error()

                fill_value = f"{len(mapping)} mappings applied"

            # ----------------------------------------
            # Metrics + response
            # ----------------------------------------
            null_count = await self._fetchval(
                f'SELECT COUNT(*) FROM {qualified} WHERE "{safe_col}" IS NULL'
            ) or 0

            sample = await self._fetch_data(table, schema, columns=[safe_col,safe_new])

            msg = f"Processed '{column}' using {mode} (fill={fill_value}), affected {null_count} nulls"

            return self._success_response(
                msg,
                [column],
                [new_col],
                sample,
                fill_mode=mode,
                fill_value=fill_value,
                new_table=table,
            )

        except Exception as e:
            return self._error_response(
                f"categorical_fillna error: {str(e)}\n{traceback.format_exc()}",
                [column],
                [],
            )
    
    async def categorical_map_values(
        self,
        table: str,
        schema: str,
        column: str,
        mapping: Dict[Any, Any],
        backend=None,
        data_id: Optional[str] = None,
        new_table: Optional[str] = None,
    ) -> Dict[str, Any]:
        try:
            table = await self._prepare_operation_table(
                table,
                schema,
                backend=backend,
                data_id=data_id,
                new_table=new_table,
            )
            if not mapping:
                return self._error_response("No mapping provided")

            new_col = self._generate_cleaned_column_name(column, f"mapped_{'_'.join(list(mapping.keys()))}")
            col_type = await self._get_column_type(table, schema, column)
            await self._add_new_column(table, schema, new_col, col_type)

            qualified = self._qualified_table(table, schema)
            safe_col = SQLIdentifierSanitizer.sanitize(column)
            safe_new = SQLIdentifierSanitizer.sanitize(new_col)

            case_parts = []
            for old, new in mapping.items():
                old_esc = str(old).replace("'", "''")
                new_esc = str(new).replace("'", "''")
                case_parts.append(f'WHEN "{safe_col}" = \'{old_esc}\' THEN \'{new_esc}\'')
            case_expr = f"CASE {' '.join(case_parts)} ELSE \"{safe_col}\" END"

            if isinstance(self.db, PostgresAdapter) or isinstance(self.db, DuckDBAdapter):
                await self._exec(f'UPDATE {qualified} SET "{safe_new}" = {case_expr}')
            else:
                raise self._unsupported_backend_error()

            sample = await self._fetch_data(table, schema, columns=[safe_col,safe_new])
            msg = f"Mapped {len(mapping)} value categories in '{column}'"
            return self._success_response(msg, [column], [new_col], sample, new_table=table)

        except Exception as e:
            return self._error_response(f"categorical_map_values error: {str(e)}\n{traceback.format_exc()}")

    async def categorical_filter_invalid(
        self,
        table: str,
        schema: str,
        column: str,
        valid_values: List[str],
        backend=None,
        data_id: Optional[str] = None,
        new_table: Optional[str] = None,
    ) -> Dict[str, Any]:
        try:
            table = await self._prepare_operation_table(
                table,
                schema,
                backend=backend,
                data_id=data_id,
                new_table=new_table,
            )
            if not valid_values:
                return self._error_response("No valid_values provided")

            new_col = self._generate_cleaned_column_name(column, f"valid_values_{'_'.join(valid_values)}")
            col_type = await self._get_column_type(table, schema, column)
            await self._add_new_column(table, schema, new_col, col_type)

            qualified = self._qualified_table(table, schema)
            safe_col = SQLIdentifierSanitizer.sanitize(column)
            safe_new = SQLIdentifierSanitizer.sanitize(new_col)

            escaped = [f"'{str(v).replace(chr(39), chr(39)+chr(39))}'" for v in valid_values]
            in_list = ",".join(escaped)

            if isinstance(self.db, PostgresAdapter) or isinstance(self.db, DuckDBAdapter):
                await self._exec(f"""
                    UPDATE {qualified}
                    SET "{safe_new}" = CASE WHEN "{safe_col}" IN ({in_list}) THEN "{safe_col}" ELSE NULL END
                """)

                invalid = await self._fetchval(f"""
                    SELECT COUNT(*) FROM {qualified}
                    WHERE "{safe_col}" IS NOT NULL AND "{safe_col}" NOT IN ({in_list})
                """) or 0
            else:
                raise self._unsupported_backend_error()

            sample = await self._fetch_data(table, schema, columns=[safe_col,safe_new])
            msg = f"Set {invalid} invalid values in '{column}' to NULL. Valid values: {valid_values[:10]}..."
            return self._success_response(msg, [column], [new_col], sample, new_table=table)

        except Exception as e:
            return self._error_response(f"categorical_filter_invalid error: {str(e)}\n{traceback.format_exc()}")

    async def categorical_compress_rare(
        self,
        table: str,
        schema: str,
        column: str,
        min_count: int = 10,
        other_label: str = "other",
        backend=None,
        data_id: Optional[str] = None,
        new_table: Optional[str] = None,
    ) -> Dict[str, Any]:
        try:
            table = await self._prepare_operation_table(
                table,
                schema,
                backend=backend,
                data_id=data_id,
                new_table=new_table,
            )
            new_col = self._generate_cleaned_column_name(column, f"compressed_{min_count}_{other_label}")
            col_type = await self._get_column_type(table, schema, column)
            await self._add_new_column(table, schema, new_col, col_type)

            qualified = self._qualified_table(table, schema)
            safe_col = SQLIdentifierSanitizer.sanitize(column)
            safe_new = SQLIdentifierSanitizer.sanitize(new_col)

            other_esc = other_label.replace("'", "''")

            if isinstance(self.db, PostgresAdapter) or isinstance(self.db, DuckDBAdapter):
                await self._exec(f"""
                    WITH freq AS (
                        SELECT "{safe_col}", COUNT(*) AS cnt
                        FROM {qualified}
                        WHERE "{safe_col}" IS NOT NULL
                        GROUP BY "{safe_col}"
                    )
                    UPDATE {qualified} AS tgt
                    SET "{safe_new}" = CASE
                        WHEN freq.cnt < {min_count} THEN '{other_esc}'
                        ELSE tgt."{safe_col}"
                    END
                    FROM freq
                    WHERE tgt."{safe_col}" = freq."{safe_col}"
                """)

                rare_rows = await self._fetch(f"""
                    SELECT DISTINCT "{safe_col}"
                    FROM {qualified}
                    WHERE "{safe_col}" IS NOT NULL
                    GROUP BY "{safe_col}"
                    HAVING COUNT(*) < {min_count}
                """)
            else:
                raise self._unsupported_backend_error()
            rare_count = len(rare_rows)

            sample = await self._fetch_data(table, schema, columns=[safe_col,safe_new])
            msg = f"Compressed {rare_count} rare categories in '{column}' to '{other_label}' (min_count={min_count})"
            return self._success_response(msg, [column], [new_col], sample, new_table=table)

        except Exception as e:
            return self._error_response(f"categorical_compress_rare error: {str(e)}\n{traceback.format_exc()}")

    # ------------------------------------------------------------------
    # Datetime cleaning
    # ------------------------------------------------------------------
    async def datetime_fillna(
        self,
        table: str,
        schema: str,
        column: str,
        mode: str = "mean",
        value: Optional[Any] = None,
        backend=None,
        data_id: Optional[str] = None,
        new_table: Optional[str] = None,
    ) -> Dict[str, Any]:
        try:
            table = await self._prepare_operation_table(
                table,
                schema,
                backend=backend,
                data_id=data_id,
                new_table=new_table,
            )
            mode = mode.upper()

            # ----------------------------------------
            # Validate mode
            # ----------------------------------------
            valid_modes = {"CONSTANT", "MIN", "MAX", "MEAN", "MEDIAN", "MODE", "NOW", "FFILL", "BFILL"}
            if mode not in valid_modes:
                return self._error_response(f"Unsupported mode: {mode}")

            # ----------------------------------------
            # Generate column name
            # ----------------------------------------
            suffix_map = {
                "CONSTANT": "constant_filled",
                "MIN": "min_filled",
                "MAX": "max_filled",
                "MEAN": "mean_filled",
                "MEDIAN": "median_filled",
                "MODE": "mode_filled",
                "NOW": "now_filled",
                "BFILL": "bfill_filled",
                "FFILL": "ffill_filled"
            }

            new_col = self._generate_cleaned_column_name(column, suffix_map[mode])

            # ----------------------------------------
            # Add new column
            # ----------------------------------------
            await self._add_new_column(table, schema, new_col, "TIMESTAMP")

            qualified = self._qualified_table(table, schema)
            safe_col = SQLIdentifierSanitizer.sanitize(column)
            safe_new = SQLIdentifierSanitizer.sanitize(new_col)

            fill_value = None

            # ----------------------------------------
            # CONSTANT
            # ----------------------------------------
            if mode == "CONSTANT":
                if value is None:
                    return self._error_response("Value must be provided for CONSTANT mode")

                if isinstance(self.db, PostgresAdapter):
                    await self._exec(
                        f'UPDATE {qualified} SET "{safe_new}" = COALESCE("{safe_col}", $1::TIMESTAMP)',
                        str(value),
                    )
                elif isinstance(self.db, DuckDBAdapter):
                    await self._exec(
                        f'UPDATE {qualified} SET "{safe_new}" = COALESCE("{safe_col}", ?::TIMESTAMP)',
                        str(value),
                    )
                else:
                    raise self._unsupported_backend_error()
                fill_value = value

            # ----------------------------------------
            # NOW
            # ----------------------------------------
            elif mode == "NOW":
                if isinstance(self.db, PostgresAdapter) or isinstance(self.db, DuckDBAdapter):
                    await self._exec(
                        f'UPDATE {qualified} SET "{safe_new}" = COALESCE("{safe_col}", CURRENT_TIMESTAMP)'
                    )
                else:
                    raise self._unsupported_backend_error()
                fill_value = "CURRENT_TIMESTAMP"

            # ----------------------------------------
            # FFILL / BFILL (INDEX GENERATED AT RUNTIME)
            # ----------------------------------------
            elif mode in ["FFILL", "BFILL"]:

                if isinstance(self.db, PostgresAdapter):
                    row_id = "ctid"
                elif isinstance(self.db, DuckDBAdapter):
                    row_id = "rowid"
                else:
                    raise self._unsupported_backend_error()

                if isinstance(self.db, PostgresAdapter):
                    # Postgres fallback (no IGNORE NULLS)
                    if mode == "FFILL":
                        window_expr = f'''
                            MAX("{safe_col}") OVER (
                                ORDER BY __idx
                                ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
                            )
                        '''
                    else:
                        window_expr = f'''
                            MIN("{safe_col}") OVER (
                                ORDER BY __idx
                                ROWS BETWEEN CURRENT ROW AND UNBOUNDED FOLLOWING
                            )
                        '''
                elif isinstance(self.db, DuckDBAdapter):
                    # DuckDB
                    if mode == "FFILL":
                        window_expr = f'''
                            LAST_VALUE("{safe_col}" IGNORE NULLS)
                            OVER (
                                ORDER BY __idx
                                ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
                            )
                        '''
                    else:
                        window_expr = f'''
                            FIRST_VALUE("{safe_col}" IGNORE NULLS)
                            OVER (
                                ORDER BY __idx
                                ROWS BETWEEN CURRENT ROW AND UNBOUNDED FOLLOWING
                            )
                        '''
                else:
                    raise self._unsupported_backend_error()

                await self._exec(f"""
                    WITH base AS (
                        SELECT *,
                            ROW_NUMBER() OVER () AS __idx,
                            {row_id} AS __rid
                        FROM {qualified}
                    ),
                    filled AS (
                        SELECT *,
                            {window_expr} AS filled_val
                        FROM base
                    )
                    UPDATE {qualified} t
                    SET "{safe_new}" = COALESCE(t."{safe_col}", f.filled_val)
                    FROM filled f
                    WHERE t.{row_id} = f.__rid
                """)

                fill_value = mode

            # ----------------------------------------
            # STATISTICAL MODES
            # ----------------------------------------
            else:

                if isinstance(self.db, PostgresAdapter):
                    stat_map = {
                        "MIN": f'MIN("{safe_col}")',
                        "MAX": f'MAX("{safe_col}")',
                        "MEAN": f'TO_TIMESTAMP(AVG(EXTRACT(EPOCH FROM "{safe_col}")))',
                        "MEDIAN": f'''
                            TO_TIMESTAMP(
                                PERCENTILE_CONT(0.5)
                                WITHIN GROUP (ORDER BY EXTRACT(EPOCH FROM "{safe_col}"))
                            )
                        ''',
                        "MODE": f"""
                            (
                                SELECT "{safe_col}"
                                FROM {qualified}
                                WHERE "{safe_col}" IS NOT NULL
                                GROUP BY "{safe_col}"
                                ORDER BY COUNT(*) DESC
                                LIMIT 1
                            )
                        """,
                    }
                elif isinstance(self.db, DuckDBAdapter):
                    stat_map = {
                        "MIN": f'MIN("{safe_col}")',
                        "MAX": f'MAX("{safe_col}")',
                        "MEAN": f'TO_TIMESTAMP(AVG(EPOCH("{safe_col}")))',
                        "MEDIAN": f'TO_TIMESTAMP(MEDIAN(EPOCH("{safe_col}")))',
                        "MODE": f"""
                            (
                                SELECT "{safe_col}"
                                FROM {qualified}
                                WHERE "{safe_col}" IS NOT NULL
                                GROUP BY "{safe_col}"
                                ORDER BY COUNT(*) DESC
                                LIMIT 1
                            )
                        """,
                    }
                else:
                    raise self._unsupported_backend_error()

                stat_expr = stat_map[mode]

                await self._exec(f"""
                    WITH stat_val AS (
                        SELECT {stat_expr} AS val
                        FROM {qualified}
                        WHERE "{safe_col}" IS NOT NULL
                    )
                    UPDATE {qualified}
                    SET "{safe_new}" = COALESCE("{safe_col}", (SELECT val FROM stat_val))
                """)

                fill_value = await self._fetchval(f"""
                    SELECT {stat_expr}
                    FROM {qualified}
                    WHERE "{safe_col}" IS NOT NULL
                """)

            # ----------------------------------------
            # Metrics + response
            # ----------------------------------------
            null_count = await self._fetchval(
                f'SELECT COUNT(*) FROM {qualified} WHERE "{safe_col}" IS NULL'
            ) or 0

            sample = await self._fetch_data(table, schema, columns=[safe_col, safe_new])

            msg = f"Filled {null_count} null values in '{column}' using {mode} ({fill_value})"

            return self._success_response(
                msg,
                [column],
                [new_col],
                sample,
                fill_mode=mode,
                fill_value=fill_value,
                new_table=table,
            )

        except Exception as e:
            return self._error_response(
                f"datetime_fillna error: {str(e)}\n{traceback.format_exc()}",
                [column],
                [],
            )
    
    async def datetime_fix_invalid(
        self,
        table: str,
        schema: str,
        column: str,
        backend=None,
        data_id: Optional[str] = None,
        new_table: Optional[str] = None,
    ) -> Dict[str, Any]:
        try:
            table = await self._prepare_operation_table(
                table,
                schema,
                backend=backend,
                data_id=data_id,
                new_table=new_table,
            )
            new_col = self._generate_cleaned_column_name(column, "fixed")
            await self._add_new_column(table, schema, new_col, "DATE")

            qualified = self._qualified_table(table, schema)
            safe_col = SQLIdentifierSanitizer.sanitize(column)
            safe_new = SQLIdentifierSanitizer.sanitize(new_col)

            if isinstance(self.db, PostgresAdapter):
                text_expr = f'"{safe_col}"::TEXT'
            elif isinstance(self.db, DuckDBAdapter):
                text_expr = f'CAST("{safe_col}" AS VARCHAR)'
            else:
                raise self._unsupported_backend_error()

            await self._exec(f"""
                UPDATE {qualified}
                SET "{safe_new}" = CASE
                    WHEN {text_expr} = '0000-00-00' THEN NULL
                    ELSE "{safe_col}"
                END
            """)

            invalid = await self._fetchval(
                f"SELECT COUNT(*) FROM {qualified} WHERE {text_expr} = '0000-00-00'"
            ) or 0

            sample = await self._fetch_data(table, schema, columns=[safe_col,safe_new])
            msg = f"Fixed {invalid} invalid dates in '{column}'"
            return self._success_response(msg, [column], [new_col], sample, new_table=table)

        except Exception as e:
            return self._error_response(f"datetime_fix_invalid error: {str(e)}\n{traceback.format_exc()}")

    async def datetime_remove_out_of_range(
        self,
        table: str,
        schema: str,
        column: str,
        min_dt: Optional[str] = None,
        max_dt: Optional[str] = None,
        backend=None,
        data_id: Optional[str] = None,
        new_table: Optional[str] = None,
    ) -> Dict[str, Any]:
        try:
            table = await self._prepare_operation_table(
                table,
                schema,
                backend=backend,
                data_id=data_id,
                new_table=new_table,
            )
            new_col = self._generate_cleaned_column_name(column, "range_filtered")
            await self._add_new_column(table, schema, new_col, "DATE")

            if min_dt is None and max_dt is None:
                min_dt, max_dt = "1900-01-01", "2100-01-01"

            qualified = self._qualified_table(table, schema)
            safe_col = SQLIdentifierSanitizer.sanitize(column)
            safe_new = SQLIdentifierSanitizer.sanitize(new_col)

            case_parts = []
            if min_dt:
                case_parts.append(f'WHEN "{safe_col}" < \'{min_dt}\'::DATE THEN NULL')
            if max_dt:
                case_parts.append(f'WHEN "{safe_col}" > \'{max_dt}\'::DATE THEN NULL')
            case_expr = f"CASE {' '.join(case_parts)} ELSE \"{safe_col}\" END" if case_parts else f'"{safe_col}"'

            if isinstance(self.db, PostgresAdapter) or isinstance(self.db, DuckDBAdapter):
                await self._exec(f'UPDATE {qualified} SET "{safe_new}" = {case_expr}')
            else:
                raise self._unsupported_backend_error()

            affected = 0
            if case_parts:
                cond = []
                if min_dt:
                    cond.append(f'"{safe_col}" < \'{min_dt}\'::DATE')
                if max_dt:
                    cond.append(f'"{safe_col}" > \'{max_dt}\'::DATE')
                where_clause = " OR ".join(cond)
                if isinstance(self.db, PostgresAdapter) or isinstance(self.db, DuckDBAdapter):
                    affected = await self._fetchval(
                        f'SELECT COUNT(*) FROM {qualified} WHERE {where_clause}'
                    ) or 0
                else:
                    raise self._unsupported_backend_error()

            sample = await self._fetch_data(table, schema, columns=[safe_col,safe_new])
            msg = f"Set {affected} out-of-range dates in '{column}' to NULL"
            return self._success_response(msg, [column], [new_col], sample, new_table=table)

        except Exception as e:
            return self._error_response(f"datetime_remove_out_of_range error: {str(e)}\n{traceback.format_exc()}")

    # ------------------------------------------------------------------
    # Groupby cleaning
    # ------------------------------------------------------------------
    
    async def numeric_fillna_groupby(
        self,
        table: str,
        schema: str,
        column: str,
        group_cols: Optional[List[str]] = None,
        value: Any = None,
        mode: str = "mean",
        backend=None,
        data_id: Optional[str] = None,
        new_table: Optional[str] = None,
    ) -> Dict[str, Any]:
        try:
            table = await self._prepare_operation_table(
                table,
                schema,
                backend=backend,
                data_id=data_id,
                new_table=new_table,
            )
            mode = mode.upper()

            # ----------------------------------------
            # Generate column name
            # ----------------------------------------
            suffix_map = {
                "CONSTANT": f"constant_filled_{value}",
                "MEAN": "mean_filled",
                "AVG": "mean_filled",
                "AVERAGE": "mean_filled",
                "MEDIAN": "median_filled",
                "MODE": "mode_filled",
                "STD": "std_filled",
                "VAR": "var_filled",
                "VARIANCE": "var_filled",
                "MIN": "min_filled",
                "MAX": "max_filled",
                "BFILL": "bfill_filled",
                "FFILL": "ffill_filled"
            }

            if mode not in suffix_map:
                return self._error_response(f"Unsupported mode: {mode}")

            new_col = self._generate_cleaned_column_name(column, suffix_map[mode])

            # ----------------------------------------
            # Add new column
            # ----------------------------------------
            col_type = await self._get_column_type(table, schema, column)
            await self._add_new_column(table, schema, new_col, col_type)

            qualified = self._qualified_table(table, schema)
            safe_col = SQLIdentifierSanitizer.sanitize(column)
            safe_new = SQLIdentifierSanitizer.sanitize(new_col)

            safe_group_cols = [SQLIdentifierSanitizer.sanitize(item) for item in group_cols] if group_cols else []

            # ----------------------------------------
            # GROUP COLS
            # ----------------------------------------
            if group_cols:
                group_cols = [SQLIdentifierSanitizer.sanitize(c) for c in group_cols]
                group_expr = ", ".join([f'"{c}"' for c in group_cols])

                # ✅ NULL-safe join
                join_cond = " AND ".join([
                    f'(t."{c}" = g."{c}" OR (t."{c}" IS NULL AND g."{c}" IS NULL))'
                    for c in group_cols
                ])

            # ----------------------------------------
            # CONSTANT
            # ----------------------------------------
            if mode == "CONSTANT":
                if value is None:
                    return self._error_response("Value must be provided")

                converted = f"'{value}'" if isinstance(value, str) else str(value)

                if isinstance(self.db, PostgresAdapter) or isinstance(self.db, DuckDBAdapter):
                    await self._exec(
                        f'UPDATE {qualified} SET "{safe_new}" = COALESCE("{safe_col}", {converted})'
                    )
                else:
                    raise self._unsupported_backend_error()
                fill_value = value

            # ----------------------------------------
            # FFILL / BFILL
            # ----------------------------------------
            elif mode in ["FFILL", "BFILL"]:

                if isinstance(self.db, PostgresAdapter):
                    row_id = "ctid"
                elif isinstance(self.db, DuckDBAdapter):
                    row_id = "rowid"
                else:
                    raise self._unsupported_backend_error()

                partition = f'PARTITION BY {group_expr}' if group_cols else ""

                if isinstance(self.db, PostgresAdapter):
                    if mode == "FFILL":
                        window_expr = f'''
                            MAX("{safe_col}") OVER (
                                {partition}
                                ORDER BY __idx
                                ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
                            )
                        '''
                    else:
                        window_expr = f'''
                            MIN("{safe_col}") OVER (
                                {partition}
                                ORDER BY __idx
                                ROWS BETWEEN CURRENT ROW AND UNBOUNDED FOLLOWING
                            )
                        '''
                elif isinstance(self.db, DuckDBAdapter):
                    if mode == "FFILL":
                        window_expr = f'''
                            LAST_VALUE("{safe_col}" IGNORE NULLS)
                            OVER (
                                {partition}
                                ORDER BY __idx
                                ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
                            )
                        '''
                    else:
                        window_expr = f'''
                            FIRST_VALUE("{safe_col}" IGNORE NULLS)
                            OVER (
                                {partition}
                                ORDER BY __idx
                                ROWS BETWEEN CURRENT ROW AND UNBOUNDED FOLLOWING
                            )
                        '''
                else:
                    raise self._unsupported_backend_error()

                await self._exec(f"""
                    WITH base AS (
                        SELECT *,
                            ROW_NUMBER() OVER () AS __idx,
                            {row_id} AS __rid
                        FROM {qualified}
                    ),
                    filled AS (
                        SELECT *,
                            {window_expr} AS filled_val
                        FROM base
                    )
                    UPDATE {qualified} t
                    SET "{safe_new}" = COALESCE(t."{safe_col}", f.filled_val)
                    FROM filled f
                    WHERE t.{row_id} = f.__rid
                """)

                fill_value = mode

            # ----------------------------------------
            # 🔥 FIXED STAT MODES
            # ----------------------------------------
            else:

                # ✅ NULL-safe aggregation
                if isinstance(self.db, PostgresAdapter) or isinstance(self.db, DuckDBAdapter):
                    stat_map = {
                        "MEAN": f'AVG(CASE WHEN "{safe_col}" IS NOT NULL THEN "{safe_col}" END)',
                        "AVG": f'AVG(CASE WHEN "{safe_col}" IS NOT NULL THEN "{safe_col}" END)',
                        "AVERAGE": f'AVG(CASE WHEN "{safe_col}" IS NOT NULL THEN "{safe_col}" END)',
                        "MEDIAN": f'PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY "{safe_col}")',
                        "MODE": f"""
                            (SELECT "{safe_col}"
                            FROM {qualified}
                            WHERE "{safe_col}" IS NOT NULL
                            GROUP BY "{safe_col}"
                            ORDER BY COUNT(*) DESC
                            LIMIT 1)
                        """,
                        "STD": f'STDDEV_POP("{safe_col}")',
                        "VAR": f'VAR_POP("{safe_col}")',
                        "VARIANCE": f'VAR_POP("{safe_col}")',
                        "MIN": f'MIN("{safe_col}")',
                        "MAX": f'MAX("{safe_col}")',
                    }
                else:
                    raise self._unsupported_backend_error()

                stat_expr = stat_map[mode]

                if group_cols:
                    await self._exec(f"""
                        WITH grouped AS (
                            SELECT {group_expr},
                                {stat_expr} AS val
                            FROM {qualified}
                            GROUP BY {group_expr}
                        ),
                        global_stat AS (
                            SELECT {stat_expr} AS val FROM {qualified}
                        )
                        UPDATE {qualified} t
                        SET "{safe_new}" = COALESCE(
                            t."{safe_col}",
                            g.val,
                            (SELECT val FROM global_stat)
                        )
                        FROM grouped g
                        WHERE {join_cond}
                    """)
                else:
                    await self._exec(f"""
                        WITH stat_val AS (
                            SELECT COALESCE({stat_expr}, 0) AS val
                            FROM {qualified}
                        )
                        UPDATE {qualified}
                        SET "{safe_new}" = COALESCE("{safe_col}", (SELECT val FROM stat_val))
                    """)

                fill_value = mode

            # ----------------------------------------
            # METRICS
            # ----------------------------------------
            null_count = await self._fetchval(
                f'SELECT COUNT(*) FROM {qualified} WHERE "{safe_col}" IS NULL'
            ) or 0

            sample = await self._fetch_data(
                table, schema,
                columns= safe_group_cols + [safe_col, safe_new] 
            )

            msg = f"Filled {null_count} nulls in '{column}' using {mode} (grouped={bool(group_cols)})"

            return self._success_response(
                msg,
                [column],
                [new_col],
                sample,
                fill_mode=mode,
                fill_value=fill_value,
                new_table=table,
            )

        except Exception as e:
            return self._error_response(
                f"numeric_fillna_groupby error: {str(e)}\n{traceback.format_exc()}",
                [column],
                [],
            )
            
    async def categorical_fillna_groupby(
        self,
        table: str,
        schema: str,
        column: str,
        group_cols: Optional[List[str]] = None,
        mode: str = "mode",
        backend=None,
        data_id: Optional[str] = None,
        new_table: Optional[str] = None,
    ) -> Dict[str, Any]:
        try:
            table = await self._prepare_operation_table(
                table,
                schema,
                backend=backend,
                data_id=data_id,
                new_table=new_table,
            )
            mode = mode.upper()

            # ----------------------------------------
            # Validate mode
            # ----------------------------------------
            valid_modes = {"MODE", "BFILL", "FFILL"}
            if mode not in valid_modes:
                return self._error_response(f"Unsupported mode: {mode}")

            # ----------------------------------------
            # Generate column name
            # ----------------------------------------
            suffix_map = {
                "MODE": "mode_filled",
                "BFILL": "bfill_filled",
                "FFILL": "ffill_filled"
            }

            new_col = self._generate_cleaned_column_name(column, suffix_map[mode])

            # ----------------------------------------
            # Add new column
            # ----------------------------------------
            col_type = await self._get_column_type(table, schema, column)
            await self._add_new_column(table, schema, new_col, col_type)

            qualified = self._qualified_table(table, schema)
            safe_col = SQLIdentifierSanitizer.sanitize(column)
            safe_new = SQLIdentifierSanitizer.sanitize(new_col)
            safe_group_cols = [SQLIdentifierSanitizer.sanitize(item) for item in group_cols] if group_cols else []

            # ----------------------------------------
            # GROUP COLS
            # ----------------------------------------
            if group_cols:
                group_cols = [SQLIdentifierSanitizer.sanitize(c) for c in group_cols]
                group_expr = ", ".join([f'"{c}"' for c in group_cols])

                # ✅ FIXED: NULL-safe join + correct alias (m)
                join_cond = " AND ".join([
                    f'(t."{c}" = m."{c}" OR (t."{c}" IS NULL AND m."{c}" IS NULL))'
                    for c in group_cols
                ])

                partition = f'PARTITION BY {group_expr}'
            else:
                partition = ""

            fill_value = None

            # ----------------------------------------
            # FFILL / BFILL
            # ----------------------------------------
            if mode in ["FFILL", "BFILL"]:

                if isinstance(self.db, PostgresAdapter):
                    row_id = "ctid"
                elif isinstance(self.db, DuckDBAdapter):
                    row_id = "rowid"
                else:
                    raise self._unsupported_backend_error()

                if isinstance(self.db, PostgresAdapter):
                    if mode == "FFILL":
                        window_expr = f'''
                            MAX("{safe_col}") OVER (
                                {partition}
                                ORDER BY __idx
                                ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
                            )
                        '''
                    else:
                        window_expr = f'''
                            MIN("{safe_col}") OVER (
                                {partition}
                                ORDER BY __idx
                                ROWS BETWEEN CURRENT ROW AND UNBOUNDED FOLLOWING
                            )
                        '''
                elif isinstance(self.db, DuckDBAdapter):
                    if mode == "FFILL":
                        window_expr = f'''
                            LAST_VALUE("{safe_col}" IGNORE NULLS)
                            OVER (
                                {partition}
                                ORDER BY __idx
                                ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
                            )
                        '''
                    else:
                        window_expr = f'''
                            FIRST_VALUE("{safe_col}" IGNORE NULLS)
                            OVER (
                                {partition}
                                ORDER BY __idx
                                ROWS BETWEEN CURRENT ROW AND UNBOUNDED FOLLOWING
                            )
                        '''
                else:
                    raise self._unsupported_backend_error()

                await self._exec(f"""
                    WITH base AS (
                        SELECT *,
                            ROW_NUMBER() OVER () AS __idx,
                            {row_id} AS __rid
                        FROM {qualified}
                    ),
                    filled AS (
                        SELECT *,
                            {window_expr} AS filled_val
                        FROM base
                    )
                    UPDATE {qualified} t
                    SET "{safe_new}" = COALESCE(t."{safe_col}", f.filled_val)
                    FROM filled f
                    WHERE t.{row_id} = f.__rid
                """)

                fill_value = mode

            # ----------------------------------------
            # 🔥 FIXED MODE GROUPBY
            # ----------------------------------------
            elif mode == "MODE":

                if isinstance(self.db, PostgresAdapter) or isinstance(self.db, DuckDBAdapter):
                    if group_cols:
                        await self._exec(f"""
                            WITH mode_vals AS (
                                SELECT {group_expr},
                                    "{safe_col}" AS mode_val
                                FROM (
                                    SELECT {group_expr},
                                        "{safe_col}",
                                        COUNT(*) AS cnt,
                                        ROW_NUMBER() OVER (
                                            PARTITION BY {group_expr}
                                            ORDER BY COUNT(*) DESC
                                        ) AS rn
                                    FROM {qualified}
                                    WHERE "{safe_col}" IS NOT NULL
                                    GROUP BY {group_expr}, "{safe_col}"
                                ) sub
                                WHERE rn = 1
                            )
                            UPDATE {qualified} t
                            SET "{safe_new}" = COALESCE(t."{safe_col}", m.mode_val)
                            FROM mode_vals m
                            WHERE {join_cond}
                        """)
                    else:
                        await self._exec(f"""
                            WITH mode_val AS (
                                SELECT "{safe_col}" AS mode_value
                                FROM {qualified}
                                WHERE "{safe_col}" IS NOT NULL
                                GROUP BY "{safe_col}"
                                ORDER BY COUNT(*) DESC
                                LIMIT 1
                            )
                            UPDATE {qualified}
                            SET "{safe_new}" = COALESCE("{safe_col}", (SELECT mode_value FROM mode_val))
                        """)
                else:
                    raise self._unsupported_backend_error()

                fill_value = "group_mode" if group_cols else "mode"

            # ----------------------------------------
            # METRICS
            # ----------------------------------------
            null_count = await self._fetchval(
                f'SELECT COUNT(*) FROM {qualified} WHERE "{safe_col}" IS NULL'
            ) or 0

            sample = await self._fetch_data(
                table, schema,
                columns= safe_group_cols + [safe_col, safe_new] 
            )

            msg = f"Processed '{column}' using {mode} (grouped={bool(group_cols)}), affected {null_count} nulls"

            return self._success_response(
                msg,
                [column],
                [new_col],
                sample,
                fill_mode=mode,
                fill_value=fill_value,
                new_table=table,
            )

        except Exception as e:
            return self._error_response(
                f"categorical_fillna_groupby error: {str(e)}\n{traceback.format_exc()}",
                [column],
                [],
            )        
            
    async def datetime_fillna_groupby(
        self,
        table: str,
        schema: str,
        column: str,
        group_cols: Optional[List[str]] = None,
        mode: str = "mean",
        backend=None,
        data_id: Optional[str] = None,
        new_table: Optional[str] = None,
    ) -> Dict[str, Any]:
        try:
            table = await self._prepare_operation_table(
                table,
                schema,
                backend=backend,
                data_id=data_id,
                new_table=new_table,
            )
            mode = mode.upper()

            valid_modes = {"MIN", "MAX", "MEAN", "MEDIAN", "MODE", "FFILL", "BFILL"}
            if mode not in valid_modes:
                return self._error_response(f"Unsupported mode: {mode}")

            suffix_map = {
                "MIN": "min_filled",
                "MAX": "max_filled",
                "MEAN": "mean_filled",
                "MEDIAN": "median_filled",
                "MODE": "mode_filled",
                "BFILL": "bfill_filled",
                "FFILL": "ffill_filled"
            }

            new_col = self._generate_cleaned_column_name(column, suffix_map[mode])

            await self._add_new_column(table, schema, new_col, "TIMESTAMP")

            qualified = self._qualified_table(table, schema)
            safe_col = SQLIdentifierSanitizer.sanitize(column)
            safe_new = SQLIdentifierSanitizer.sanitize(new_col)
            safe_group_cols = [SQLIdentifierSanitizer.sanitize(c) for c in group_cols] if group_cols else []

            # ----------------------------------------
            # GROUP SETUP
            # ----------------------------------------
            if group_cols:
                group_cols = [SQLIdentifierSanitizer.sanitize(c) for c in group_cols]
                group_expr = ", ".join([f'"{c}"' for c in group_cols])

                # ✅ NULL SAFE JOIN
                join_cond = " AND ".join([
                    f'(t."{c}" = g."{c}" OR (t."{c}" IS NULL AND g."{c}" IS NULL))'
                    for c in group_cols
                ])

                partition = f'PARTITION BY {group_expr}'
            else:
                partition = ""

            fill_value = None

            # ----------------------------------------
            # FFILL / BFILL
            # ----------------------------------------
            if mode in ["FFILL", "BFILL"]:

                if isinstance(self.db, PostgresAdapter):
                    row_id = "ctid"
                elif isinstance(self.db, DuckDBAdapter):
                    row_id = "rowid"
                else:
                    raise self._unsupported_backend_error()

                if isinstance(self.db, PostgresAdapter):
                    if mode == "FFILL":
                        window_expr = f'''
                            MAX("{safe_col}") OVER (
                                {partition}
                                ORDER BY __idx
                                ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
                            )
                        '''
                    else:
                        window_expr = f'''
                            MIN("{safe_col}") OVER (
                                {partition}
                                ORDER BY __idx
                                ROWS BETWEEN CURRENT ROW AND UNBOUNDED FOLLOWING
                            )
                        '''
                elif isinstance(self.db, DuckDBAdapter):
                    if mode == "FFILL":
                        window_expr = f'''
                            LAST_VALUE("{safe_col}" IGNORE NULLS)
                            OVER (
                                {partition}
                                ORDER BY __idx
                                ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
                            )
                        '''
                    else:
                        window_expr = f'''
                            FIRST_VALUE("{safe_col}" IGNORE NULLS)
                            OVER (
                                {partition}
                                ORDER BY __idx
                                ROWS BETWEEN CURRENT ROW AND UNBOUNDED FOLLOWING
                            )
                        '''
                else:
                    raise self._unsupported_backend_error()

                await self._exec(f"""
                    WITH base AS (
                        SELECT *,
                            ROW_NUMBER() OVER () AS __idx,
                            {row_id} AS __rid
                        FROM {qualified}
                    ),
                    filled AS (
                        SELECT *,
                            {window_expr} AS filled_val
                        FROM base
                    )
                    UPDATE {qualified} t
                    SET "{safe_new}" = COALESCE(t."{safe_col}", f.filled_val)
                    FROM filled f
                    WHERE t.{row_id} = f.__rid
                """)

                fill_value = mode

            # ----------------------------------------
            # 🔥 FIXED STAT MODES
            # ----------------------------------------
            else:

                if isinstance(self.db, PostgresAdapter):
                    stat_map = {
                        "MIN": f'MIN("{safe_col}")',
                        "MAX": f'MAX("{safe_col}")',
                        "MEAN": f'TO_TIMESTAMP(AVG(EXTRACT(EPOCH FROM "{safe_col}")))',
                        "MEDIAN": f'''
                            TO_TIMESTAMP(
                                PERCENTILE_CONT(0.5)
                                WITHIN GROUP (ORDER BY EXTRACT(EPOCH FROM "{safe_col}"))
                            )
                        ''',
                        "MODE": f'''
                            (
                                SELECT "{safe_col}"
                                FROM {qualified}
                                WHERE "{safe_col}" IS NOT NULL
                                GROUP BY "{safe_col}"
                                ORDER BY COUNT(*) DESC
                                LIMIT 1
                            )
                        '''
                    }
                elif isinstance(self.db, DuckDBAdapter):
                    stat_map = {
                        "MIN": f'MIN("{safe_col}")',
                        "MAX": f'MAX("{safe_col}")',
                        "MEAN": f'TO_TIMESTAMP(AVG(EPOCH("{safe_col}")))',
                        "MEDIAN": f'TO_TIMESTAMP(MEDIAN(EPOCH("{safe_col}")))',
                        "MODE": f'''
                            (
                                SELECT "{safe_col}"
                                FROM {qualified}
                                WHERE "{safe_col}" IS NOT NULL
                                GROUP BY "{safe_col}"
                                ORDER BY COUNT(*) DESC
                                LIMIT 1
                            )
                        '''
                    }
                else:
                    raise self._unsupported_backend_error()

                stat_expr = stat_map[mode]

                if group_cols:
                    await self._exec(f"""
                        WITH grouped AS (
                            SELECT {group_expr},
                                {stat_expr} AS val
                            FROM {qualified}
                            GROUP BY {group_expr}
                        ),
                        global_stat AS (
                            SELECT COALESCE({stat_expr}, CURRENT_TIMESTAMP) AS val
                            FROM {qualified}
                        )
                        UPDATE {qualified} t
                        SET "{safe_new}" = COALESCE(
                            t."{safe_col}",
                            g.val,
                            (SELECT val FROM global_stat)
                        )
                        FROM grouped g
                        WHERE {join_cond}
                    """)

                else:
                    await self._exec(f"""
                        WITH stat_val AS (
                            SELECT {stat_expr} AS val
                            FROM {qualified}
                            WHERE "{safe_col}" IS NOT NULL
                        )
                        UPDATE {qualified}
                        SET "{safe_new}" = COALESCE("{safe_col}", (SELECT val FROM stat_val))
                    """)
                    fill_value = mode

            # ----------------------------------------
            # METRICS
            # ----------------------------------------
            null_count = await self._fetchval(
                f'SELECT COUNT(*) FROM {qualified} WHERE "{safe_col}" IS NULL'
            ) or 0

            sample = await self._fetch_data(
                table, schema,
                columns=safe_group_cols + [safe_col, safe_new]
            )

            msg = f"Filled {null_count} nulls in '{column}' using {mode} (grouped={bool(group_cols)})"

            return self._success_response(
                msg,
                [column],
                [new_col],
                sample,
                fill_mode=mode,
                fill_value=fill_value,
                new_table=table,
            )

        except Exception as e:
            return self._error_response(
                f"datetime_fillna_groupby error: {str(e)}\n{traceback.format_exc()}",
                [column],
                [],
            )        
            
    # ------------------------------------------------------------------
    # Generic cleaning(Drop implementation)
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
        
        """
        Remove missing (NULL / NaN / NaT) values from a table using SQL,
        mimicking pandas.DataFrame.dropna behavior.

        This operation does NOT modify the original table. Instead, it returns
        a filtered DataFrame result based on the specified conditions.

        Parameters
        ----------
        table : str
            Target table name.

        schema : str
            Schema name where the table exists.

        axis : int or str, default 0
            Axis along which to drop missing values.

            - 0 or 'index'   → Drop rows
            - 1 or 'columns' → Drop columns

        how : {'any', 'all'}, default 'any'
            Determines when to drop rows/columns (ignored if `thresh` is provided).

            - 'any' → Drop if ANY value is NULL
            - 'all' → Drop only if ALL values are NULL

        thresh : int or float, optional
            Minimum number of NON-NULL values required to keep a row/column.

            Behavior:
            - If int:
                Minimum number of non-null values required.
            - If float (0 < thresh <= 1):
                Interpreted as maximum allowed NULL fraction.
                Internally converted as:

                    required_non_null = total_rows * (1 - thresh)

            Notes:
            - Cannot be combined with `how` (pandas-compatible behavior)
            - Float mode is an enhanced feature (not in pandas)

        Returns
        -------
        Dict[str, Any]
            Standard response dictionary containing:

            - is_error : bool
            - message : str
            - result : pandas.DataFrame (filtered output)
            - involved_cols : List[str]

        Behavior
        --------
        ROW DROP (axis=0):
            - Applies SQL WHERE filters to remove rows

        COLUMN DROP (axis=1):
            - Dynamically evaluates each column using SQL aggregation
            - Constructs SELECT query with only valid columns

        Notes
        -----
        - Works entirely in SQL (DuckDB / PostgreSQL compatible)
        - Does NOT persist changes (non-destructive)
        - Handles NULL, NaN, NaT uniformly via SQL NULL semantics

        Examples
        --------
        >>> await ops.clean.dropna(axis=0, how="any")
        Drop rows with any missing values.

        >>> await ops.clean.dropna(axis=1, how="all")
        Drop columns where all values are missing.

        >>> await ops.clean.dropna(axis=1, thresh=10)
        Keep columns with at least 10 non-null values.

        >>> await ops.clean.dropna(axis=1, thresh=0.1)
        Keep columns with at least 90% non-null values (enhanced behavior).

        Limitations
        -----------
        - No support for `subset`
        - No in-place modification (always returns result)
        - No index reset (handled separately)

        """
        try:
            source_table = SQLIdentifierSanitizer.sanitize(table)
            qualified = self._qualified_table(source_table, schema)

            if isinstance(self.db, PostgresAdapter) or isinstance(self.db, DuckDBAdapter):
                pass
            else:
                raise self._unsupported_backend_error()

            how = (how or "any").lower()

            # ----------------------------------------
            # ✅ Convert float thresh → absolute count
            # ----------------------------------------
            if isinstance(thresh, float):
                total_rows = await self._fetchval(f"SELECT COUNT(*) FROM {qualified}") or 0

                # 🔥 INTERPRET AS "MAX NULL FRACTION"
                max_nulls = int(total_rows * thresh)
                thresh = total_rows - max_nulls  # convert to min non-null

                if thresh < 1:
                    thresh = 1
            # ----------------------------------------
            # ✅ Validate thresh / how
            # ----------------------------------------
            if thresh is not None:
                if not isinstance(thresh, int) or thresh < 1:
                    return self._error_response("thresh must be a positive integer")
                how = None  # pandas behavior

            elif how not in {"any", "all"}:
                return self._error_response("Invalid 'how'. Use 'any' or 'all'")

            # ----------------------------------------
            # ✅ Get columns
            # ----------------------------------------
            column_types = await self.db.get_column_types(source_table, schema)
            cols = list(column_types.keys())

            if not cols:
                return self._error_response("No columns found")

            # ----------------------------------------
            # 🔥 MISSING VALUE CONDITION (CRITICAL FIX)
            # Treat NULL + NaN + NaT as missing
            # ----------------------------------------
            def valid_expr(col):
                return f'"{col}" IS NOT NULL AND "{col}" = "{col}"'

            # ----------------------------------------
            # ROW DROP (axis=0)
            # ----------------------------------------
            if axis in (0, "index"):

                if thresh is not None:
                    non_null_expr = " + ".join([
                        f'CASE WHEN {valid_expr(c)} THEN 1 ELSE 0 END'
                        for c in cols
                    ])
                    where_clause = f"({non_null_expr}) >= {thresh}"

                elif how == "any":
                    # keep rows where ALL values are valid
                    where_clause = " AND ".join([valid_expr(c) for c in cols])

                else:  # how == "all"
                    # keep rows where ANY value is valid
                    where_clause = " OR ".join([valid_expr(c) for c in cols])

                query = f"SELECT * FROM {qualified} WHERE {where_clause}"

            # ----------------------------------------
            # COLUMN DROP (axis=1)
            # ----------------------------------------
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
                        # drop column if ANY missing exists
                        null_count = await self._fetchval(
                            f'SELECT COUNT(*) FROM {qualified} WHERE NOT ({valid_expr(safe_col)})'
                        )
                        if null_count == 0:
                            selected_cols.append(f'"{safe_col}"')

                    else:  # how == "all"
                        # drop column only if ALL values missing
                        non_null_count = await self._fetchval(
                            f'SELECT COUNT(*) FROM {qualified} WHERE {valid_expr(safe_col)}'
                        )
                        if non_null_count > 0:
                            selected_cols.append(f'"{safe_col}"')

                # ----------------------------------------
                # Handle empty result
                # ----------------------------------------
                if not selected_cols:
                    return self._success_response(
                        message="All columns dropped (all contained missing values)",
                        involved_cols=cols,
                        generated_cols=[],
                        sample_df=pd.DataFrame(),
                    )

                query = f'SELECT {", ".join(selected_cols)} FROM {qualified}'

            else:
                return self._error_response("Invalid axis. Use 0/'index' or 1/'columns'")

            # ----------------------------------------
            # EXECUTE
            # ----------------------------------------
            output_table = await self._materialize_query_as_table(
                query=query,
                table=source_table,
                schema=schema,
                backend=backend,
                data_id=data_id,
                new_table=new_table,
            )
            df = await self._fetch_data(output_table, schema)

            return self._success_response(
                message=f"dropna applied (axis={axis}, how={how}, thresh={thresh})",
                involved_cols=cols,
                generated_cols=[],
                sample_df=df,
                new_table=output_table,
            )

        except Exception as e:
            return self._error_response(
                f"dataframe_dropna error: {str(e)}\n{traceback.format_exc()}"
            )
   
            
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
        """
        Drop specified rows or columns from a table (SQL-based implementation).

        Parameters
        ----------
        axis : {0, 1}
            0 → drop rows
            1 → drop columns

        index : list[int], optional
            Row indices to drop (0-based)

        columns : list[str], optional
            Column names to drop

        Returns
        -------
        Dict with resulting DataFrame
        """
        try:
            source_table = SQLIdentifierSanitizer.sanitize(table)
            qualified = self._qualified_table(source_table, schema)

            if isinstance(self.db, PostgresAdapter) or isinstance(self.db, DuckDBAdapter):
                pass
            else:
                raise self._unsupported_backend_error()

            # ----------------------------------------
            # Validate inputs
            # ----------------------------------------
            if axis in (0, "index"):
                if not index:
                    return self._error_response("index must be provided when axis=0")

            elif axis in (1, "columns"):
                if not columns:
                    return self._error_response("columns must be provided when axis=1")

            else:
                return self._error_response("Invalid axis. Use 0 or 1")

            # ----------------------------------------
            # COLUMN DROP
            # ----------------------------------------
            if axis in (1, "columns"):
                column_types = await self.db.get_column_types(source_table, schema)
                existing_cols = list(column_types.keys())

                drop_cols = set([SQLIdentifierSanitizer.sanitize(c) for c in columns])

                # Validate columns
                missing = drop_cols - set(existing_cols)
                if missing:
                    return self._error_response(f"Columns not found: {missing}")

                remaining_cols = [c for c in existing_cols if c not in drop_cols]

                if not remaining_cols:
                    return self._error_response("Cannot drop all columns")

                select_clause = ", ".join([f'"{c}"' for c in remaining_cols])

                query = f"SELECT {select_clause} FROM {qualified}"

            # ----------------------------------------
            # ROW DROP
            # ----------------------------------------
            else:
                # Create row number (0-based like pandas)
                index_list = list(set(index))  # remove duplicates
                index_list_str = ", ".join([str(i) for i in index_list])

                query = f"""
                    SELECT * FROM (
                        SELECT *, ROW_NUMBER() OVER () - 1 AS __rn__
                        FROM {qualified}
                    ) t
                    WHERE __rn__ NOT IN ({index_list_str})
                """

            # ----------------------------------------
            # Execute
            # ----------------------------------------
            output_table = await self._materialize_query_as_table(
                query=query,
                table=source_table,
                schema=schema,
                backend=backend,
                data_id=data_id,
                new_table=new_table,
            )
            df = await self._fetch_data(output_table, schema)

            return self._success_response(
                message=f"drop applied (axis={axis})",
                involved_cols=columns or [],
                generated_cols=[],
                sample_df=df,
                new_table=output_table,
            )

        except Exception as e:
            return self._error_response(f"dataframe_drop error: {str(e)}\n{traceback.format_exc()}")
        
        

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

            if isinstance(self.db, PostgresAdapter) or isinstance(self.db, DuckDBAdapter):
                pass
            else:
                raise self._unsupported_backend_error()

            column_types = await self.db.get_column_types(source_table, schema)
            cols = list(column_types.keys())

            if not cols:
                return self._error_response("No columns found")

            select_parts = [
                f'"{col}" IS NULL AS "{col}"'
                for col in cols
            ]

            query = f"""
                SELECT {', '.join(select_parts)}
                FROM {qualified}
            """

            output_table = await self._materialize_query_as_table(
                query=query,
                table=source_table,
                schema=schema,
                backend=backend,
                data_id=data_id,
                new_table=new_table,
            )
            df = await self._fetch_data(output_table, schema)

            return self._success_response(
                message="Generated NA mask (isna)",
                involved_cols=cols,
                generated_cols=cols,
                sample_df=df,
                new_table=output_table,
            )

        except Exception as e:
            return self._error_response(
                f"dataframe_isna error: {str(e)}\n{traceback.format_exc()}"
            )
            
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

            if isinstance(self.db, PostgresAdapter) or isinstance(self.db, DuckDBAdapter):
                pass
            else:
                raise self._unsupported_backend_error()

            column_types = await self.db.get_column_types(source_table, schema)
            cols = list(column_types.keys())

            if not cols:
                return self._error_response("No columns found")

            select_parts = [
                f'"{col}" IS NOT NULL AS "{col}"'
                for col in cols
            ]

            query = f"""
                SELECT {', '.join(select_parts)}
                FROM {qualified}
            """

            output_table = await self._materialize_query_as_table(
                query=query,
                table=source_table,
                schema=schema,
                backend=backend,
                data_id=data_id,
                new_table=new_table,
            )
            df = await self._fetch_data(output_table, schema)

            return self._success_response(
                message="Generated NA mask (notna)",
                involved_cols=cols,
                generated_cols=cols,
                sample_df=df,
                new_table=output_table,
            )

        except Exception as e:
            return self._error_response(
                f"dataframe_notna error: {str(e)}\n{traceback.format_exc()}"
            )
            
            
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

            if isinstance(self.db, PostgresAdapter) or isinstance(self.db, DuckDBAdapter):
                pass
            else:
                raise self._unsupported_backend_error()

            # ----------------------------------------
            # Columns
            # ----------------------------------------
            column_types = await self.db.get_column_types(source_table, schema)
            all_cols = list(column_types.keys())

            if not all_cols:
                return self._error_response("No columns found")

            # ----------------------------------------
            # Subset handling
            # ----------------------------------------
            if subset:
                subset = [SQLIdentifierSanitizer.sanitize(c) for c in subset]

                missing = set(subset) - set(all_cols)
                if missing:
                    return self._error_response(f"Columns not found: {missing}")

                partition_cols = subset
            else:
                partition_cols = all_cols

            partition_expr = ", ".join([f'"{c}"' for c in partition_cols])
            select_cols = ", ".join([f'"{c}"' for c in all_cols])

            # ----------------------------------------
            # STEP 1: create stable row index
            # ----------------------------------------
            base_query = f"""
                SELECT *,
                    ROW_NUMBER() OVER () AS __row_id__
                FROM {qualified}
            """

            # ----------------------------------------
            # KEEP LOGIC
            # ----------------------------------------
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
                return self._error_response("Invalid 'keep'. Use 'first', 'last', or False")

            # ----------------------------------------
            # Execute
            # ----------------------------------------
            output_table = await self._materialize_query_as_table(
                query=query,
                table=source_table,
                schema=schema,
                backend=backend,
                data_id=data_id,
                new_table=new_table,
            )
            df = await self._fetch_data(output_table, schema)

            return self._success_response(
                message=f"drop_duplicates applied (subset={subset}, keep={keep})",
                involved_cols=partition_cols,
                generated_cols=[],
                sample_df=df,
                new_table=output_table,
            )

        except Exception as e:
            return self._error_response(
                f"dataframe_drop_duplicates error: {str(e)}\n{traceback.format_exc()}"
            )
            
            
    # =========================================================================
    # DATA QUALITY
    # =========================================================================
    async def data_quality_missing_values(
        self, table: str, schema: str, columns: List[str]
    ) -> Dict[str, Any]:
        try:
            q = self._qualified_table(table, schema)
            if isinstance(self.db, PostgresAdapter) or isinstance(self.db, DuckDBAdapter):
                pass
            else:
                raise self._unsupported_backend_error()

            total = await self._fetchval(f'SELECT COUNT(*) FROM {q}')
            result = {}
            for col in columns:
                safe_col = SQLIdentifierSanitizer.sanitize(col)
                non_null = await self._fetchval(f'SELECT COUNT("{safe_col}") FROM {q}')
                missing = total - non_null
                result[col] = {"total": total, "non_null": non_null, "missing": missing,
                               "missing_pct": (missing/total*100) if total else 0}
            high_missing = [c for c, v in result.items() if v["missing_pct"] > 50]
            msg = (f"Missing value analysis: {len(columns)} columns, "
                   f"{len(high_missing)} columns over 50% missing")
            return self._success_response(msg, columns, result=result)
        except Exception as e:
            return self._error_response(str(e), columns)

    async def data_quality_completeness_score(
        self, table: str, schema: str, columns: List[str]
    ) -> Dict[str, Any]:
        try:
            q = self._qualified_table(table, schema)
            if isinstance(self.db, PostgresAdapter) or isinstance(self.db, DuckDBAdapter):
                pass
            else:
                raise self._unsupported_backend_error()

            total = await self._fetchval(f'SELECT COUNT(*) FROM {q}')
            scores = {}
            for col in columns:
                safe_col = SQLIdentifierSanitizer.sanitize(col)
                non_null = await self._fetchval(f'SELECT COUNT("{safe_col}") FROM {q}')
                scores[col] = {"completeness": (non_null/total*100) if total else 0}
            avg_comp = sum(v["completeness"] for v in scores.values()) / len(scores) if scores else 0
            msg = f"Completeness scores: {len(columns)} columns, average {avg_comp:.1f}%"
            return self._success_response(msg, columns, result=scores)
        except Exception as e:
            return self._error_response(str(e), columns)

    # =========================================================================
    # COMPREHENSIVE SUMMARIES
    # =========================================================================
    async def comprehensive_numeric_summary(
        self, table: str, schema: str, columns: List[str]
    ) -> Dict[str, Any]:
        try:
            if isinstance(self.db, PostgresAdapter) or isinstance(self.db, DuckDBAdapter):
                pass
            else:
                raise self._unsupported_backend_error()

            summaries = {}
            for col in columns[:20]:  # performance limit
                res = await self.numeric_basic_summary(table, schema, col)
                if not res["is_error"]:
                    summaries[col] = res["message"]
            msg = f"Comprehensive numeric summary for {len(summaries)} columns"
            return self._success_response(msg, columns[:20], result=summaries)
        except Exception as e:
            return self._error_response(str(e), columns)

    async def statistical_profile_report(
        self, table: str, schema: str, columns: List[str]
    ) -> Dict[str, Any]:
        try:
            if isinstance(self.db, PostgresAdapter) or isinstance(self.db, DuckDBAdapter):
                pass
            else:
                raise self._unsupported_backend_error()

            # Dummy profile combining completeness + basic stats
            comp = await self.data_quality_completeness_score(table, schema, columns)
            num = await self.comprehensive_numeric_summary(table, schema, columns)
            msg = f"Statistical profile for '{table}': {len(columns)} columns"
            return self._success_response(msg, columns, result={"completeness": comp, "numeric": num})
        except Exception as e:
            return self._error_response(str(e), columns)        
            
            
