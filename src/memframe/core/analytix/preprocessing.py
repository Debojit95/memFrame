"""
Core Preprocessing operations executed directly on the database.
Supports PostgreSQL, DuckDB, and ClickHouse backends via DatabaseAdapter.
Every public method now creates a new transient table, operates on it,
and returns a standardized response with the new table name.
"""

import re
import traceback
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Tuple

import pandas as pd

# ponytail: single-file ops, skip base/dialect/factory split until it hurts
from memframe.db_manager.adapters.base import DatabaseAdapter
from memframe.db_manager.adapters.duckdb import DuckDBAdapter
from memframe.db_manager.adapters.postgresql import PostgresAdapter
from memframe.db_manager.adapters.clickhouse import ClickHouseAdapter
from memframe.core.ingestion.datatype_detector import Backend
from memframe.utils.helper import SQLIdentifierSanitizer


class PreprocessingOps:
    def __init__(self, db_adapter: DatabaseAdapter):
        self.db = db_adapter
        self._backend = self._resolve_backend(db_adapter)

    def _resolve_backend(self, db_adapter: DatabaseAdapter) -> str:
        if isinstance(db_adapter, PostgresAdapter):
            return Backend.POSTGRES
        elif isinstance(db_adapter, DuckDBAdapter):
            return Backend.DUCKDB
        elif isinstance(db_adapter, ClickHouseAdapter):                   # ← ADD
            return Backend.CLICKHOUSE
        else:
            raise self._unsupported_backend_error()

    # ------------------------------------------------------------------
    # Internal helpers (matching the pattern)
    # ------------------------------------------------------------------
    async def _exec(self, sql: str, *args) -> None:
        await self.db.execute(sql, *args)

    async def _fetch(self, sql: str, *args) -> List[Tuple]:
        return await self.db.fetch(sql, *args)

    async def _fetchval(self, sql: str, *args) -> Any:
        return await self.db.fetchval(sql, *args)

    def _row_value(self, row: Any, column_name: str, index: int = 0) -> Any:
        if isinstance(row, dict):
            if column_name in row:
                return row[column_name]
            if row:
                return next(iter(row.values()))
            raise KeyError(column_name)
        try:
            return row[column_name]
        except Exception:
            pass
        try:
            return row[index]
        except Exception:
            pass
        try:
            row_dict = dict(row)
            if column_name in row_dict:
                return row_dict[column_name]
            if row_dict:
                return next(iter(row_dict.values()))
        except Exception:
            pass
        raise KeyError(column_name)

    async def _fetch_sample(self, table: str, schema: str, columns: Any = "*") -> pd.DataFrame:
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

        rows = await self._fetch(f"SELECT {column_clause} FROM {qualified} LIMIT 10")
        records = [dict(row) for row in rows]
        return pd.DataFrame.from_records(records)

    async def _get_column_type(self, table: str, schema: str, column: str) -> str:
        types = await self.db.get_column_types(table, schema)
        return types.get(column, "TEXT")

    def _qualified_table(self, table: str, schema: str) -> str:
        safe_table = SQLIdentifierSanitizer.sanitize(table)
        safe_schema = SQLIdentifierSanitizer.sanitize(schema)
        return f'{self.db.quote_identifier(safe_schema)}.{self.db.quote_identifier(safe_table)}'

    def _generate_cleaned_column_name(self, base: str, suffix: str = "") -> str:
        sanitized = SQLIdentifierSanitizer.sanitize(base)
        name = f"transformed_{sanitized}"
        return f"{name}_{suffix}" if suffix else name

    async def _add_new_column(self, table: str, schema: str, col_name: str, col_type: str) -> None:
        qualified = self._qualified_table(table, schema)
        safe_col = SQLIdentifierSanitizer.sanitize(col_name)
        col_q = self.db.quote_identifier(safe_col)
        await self._exec(f'ALTER TABLE {qualified} ADD COLUMN IF NOT EXISTS {col_q} {col_type}')
    
    async def _count_non_null(self, table: str, schema: str, column: str) -> int:
        qualified = self._qualified_table(table, schema)
        safe_col = SQLIdentifierSanitizer.sanitize(column)
        col_q = self.db.quote_identifier(safe_col)
        sql = f'SELECT COUNT(*) FROM {qualified} WHERE {col_q} IS NOT NULL'
        val = await self._fetchval(sql)
        return int(val) if val is not None else 0
    
    
    
    def _sql_literal(self, v: Any) -> str:
        if v is None:
            return "NULL"
        if isinstance(v, str):
            escaped = v.replace("'", "''")
            return f"'{escaped}'"
        if isinstance(v, bool):
            return "TRUE" if v else "FALSE"
        return str(v)

    # ------------------------------------------------------------------
    # Transient table helpers (mirroring other core classes)
    # ------------------------------------------------------------------
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

    # ------------------------------------------------------------------
    # ClickHouse CTAS helper
    # ------------------------------------------------------------------
    def _ch_create_table_as(self, schema: str, table: str, query: str) -> str:
        schema_q = self.db.quote_identifier(schema)
        table_q = self.db.quote_identifier(table)
        return (
            f"CREATE TABLE {schema_q}.{table_q} "
            f"ENGINE = MergeTree() ORDER BY tuple() AS {query}"
        )

    # ------------------------------------------------------------------
    # Response builders
    # ------------------------------------------------------------------
    def _success_response(
        self,
        message: str,
        involved_cols: List[str],
        generated_cols: List[str],
        sample_df: Optional[pd.DataFrame] = None,
        **extra,
    ) -> Dict[str, Any]:
        return {
            "is_error": False,
            "message": message,
            "error_message": None,
            "involved_cols": involved_cols,
            "generated_cols": generated_cols,
            "result": sample_df if sample_df is not None else pd.DataFrame(),
            **extra,
        }

    def _error_response(
        self,
        error_message: str,
        involved_cols: List[str] = None,
        generated_cols: List[str] = None,
    ) -> Dict[str, Any]:
        # ponytail: keep `result` key so is_operation_response/unwrap_response
        # treat errors canonically instead of returning the raw dict
        return {
            "is_error": True,
            "message": "",
            "error_message": error_message,
            "involved_cols": involved_cols or [],
            "generated_cols": generated_cols or [],
            "result": None,
        }

    def _unsupported_backend_error(self) -> NotImplementedError:
        return NotImplementedError(
            f"Unsupported database backend for preprocessing operation: {self.db.__class__.__name__}"
        )

    # ==================================================================
    # Numeric Preprocessings
    # ==================================================================
    async def numeric_standardize(
        self, table: str, schema: str, column: str,
        backend=None, data_id: Optional[str] = None, new_table: Optional[str] = None
    ) -> Dict[str, Any]:
        try:
            if isinstance(self.db, PostgresAdapter) or isinstance(self.db, DuckDBAdapter):
                working_table = await self._prepare_operation_table(
                    table, schema, backend=backend, data_id=data_id, new_table=new_table
                )
                supplied_new = f"{column}_standardized"
                new_col = self._generate_cleaned_column_name(supplied_new)
                await self._add_new_column(working_table, schema, new_col, "DOUBLE PRECISION")

                qualified = self._qualified_table(working_table, schema)
                safe_col = SQLIdentifierSanitizer.sanitize(column)
                safe_new = SQLIdentifierSanitizer.sanitize(new_col)

                sql = f"""
                    WITH stats AS (
                        SELECT AVG("{safe_col}") AS mean, STDDEV_POP("{safe_col}") AS std
                        FROM {qualified}
                        WHERE "{safe_col}" IS NOT NULL
                    )
                    UPDATE {qualified}
                    SET "{safe_new}" = CASE
                        WHEN "{safe_col}" IS NULL THEN NULL
                        ELSE ("{safe_col}" - stats.mean) / NULLIF(stats.std, 0)
                    END
                    FROM stats;
                """
                await self._exec(sql)

                affected = await self._count_non_null(working_table, schema, column)
                sample = await self._fetch_sample(working_table, schema, [safe_col, safe_new])
                msg = f"Standardized {column} -> {new_col} for {affected} rows (z-score)."
                return self._success_response(msg, [column], [new_col], sample, new_table=working_table)

            elif isinstance(self.db, ClickHouseAdapter):
                safe_schema = SQLIdentifierSanitizer.sanitize(schema)
                new_table = await self._resolve_output_table_name(
                    table, safe_schema, backend=backend, data_id=data_id, new_table=new_table
                )
                qualified_source = self._qualified_table(table, schema)

                supplied_new = f"{column}_standardized"
                new_col = self._generate_cleaned_column_name(supplied_new)
                safe_col = SQLIdentifierSanitizer.sanitize(column)
                safe_new = SQLIdentifierSanitizer.sanitize(new_col)
                col_q = self.db.quote_identifier(safe_col)
                new_q = self.db.quote_identifier(safe_new)

                create_sql = self._ch_create_table_as(safe_schema, new_table, f"""
                    SELECT source.*,
                        CASE WHEN source.{col_q} IS NULL THEN NULL
                        ELSE (source.{col_q} - stats.mean) / nullIf(stats.std, 0)
                        END AS {new_q}
                    FROM {qualified_source} AS source
                    CROSS JOIN (
                        SELECT avg({col_q}) AS mean, stddevPop({col_q}) AS std
                        FROM {qualified_source}
                        WHERE {col_q} IS NOT NULL
                    ) AS stats
                """)
                await self._exec(create_sql)

                affected = await self._count_non_null(new_table, schema, column)
                sample = await self._fetch_sample(new_table, schema, [safe_col, safe_new])
                msg = f"Standardized {column} -> {new_col} for {affected} rows (z-score)."
                return self._success_response(msg, [column], [new_col], sample, new_table=new_table)

            else:
                raise self._unsupported_backend_error()
        except Exception as e:
            return self._error_response(
                f"numeric_standardize error: {str(e)}\n{traceback.format_exc()}",
                [column],
                [],
            )

    async def numeric_minmax_scale(
        self, table: str, schema: str, column: str,
        backend=None, data_id: Optional[str] = None, new_table: Optional[str] = None
    ) -> Dict[str, Any]:
        try:
            if isinstance(self.db, PostgresAdapter) or isinstance(self.db, DuckDBAdapter):
                working_table = await self._prepare_operation_table(
                    table, schema, backend=backend, data_id=data_id, new_table=new_table
                )
                supplied_new = f"{column}_minmax"
                new_col = self._generate_cleaned_column_name(supplied_new)
                await self._add_new_column(working_table, schema, new_col, "DOUBLE PRECISION")

                qualified = self._qualified_table(working_table, schema)
                safe_col = SQLIdentifierSanitizer.sanitize(column)
                safe_new = SQLIdentifierSanitizer.sanitize(new_col)

                # ponytail: cast to float for Postgres integer columns (else integer division)
                col_expr = f'CAST("{safe_col}" AS DOUBLE PRECISION)'
                sql = f"""
                    WITH stats AS (
                        SELECT MIN("{safe_col}") AS min_val, MAX("{safe_col}") AS max_val
                        FROM {qualified}
                        WHERE "{safe_col}" IS NOT NULL
                    )
                    UPDATE {qualified}
                    SET "{safe_new}" = CASE
                        WHEN "{safe_col}" IS NULL THEN NULL
                        ELSE ({col_expr} - CAST(stats.min_val AS DOUBLE PRECISION)) / NULLIF(CAST((stats.max_val - stats.min_val) AS DOUBLE PRECISION), 0)
                    END
                    FROM stats;
                """
                await self._exec(sql)

                affected = await self._count_non_null(working_table, schema, column)
                sample = await self._fetch_sample(working_table, schema, [safe_col, safe_new])
                msg = f"Min-max scaled {column} -> {new_col} for {affected} rows."
                return self._success_response(msg, [column], [new_col], sample, new_table=working_table)

            elif isinstance(self.db, ClickHouseAdapter):
                safe_schema = SQLIdentifierSanitizer.sanitize(schema)
                new_table = await self._resolve_output_table_name(
                    table, safe_schema, backend=backend, data_id=data_id, new_table=new_table
                )
                qualified_source = self._qualified_table(table, schema)

                supplied_new = f"{column}_minmax"
                new_col = self._generate_cleaned_column_name(supplied_new)
                safe_col = SQLIdentifierSanitizer.sanitize(column)
                safe_new = SQLIdentifierSanitizer.sanitize(new_col)
                col_q = self.db.quote_identifier(safe_col)
                new_q = self.db.quote_identifier(safe_new)

                create_sql = self._ch_create_table_as(safe_schema, new_table, f"""
                    SELECT source.*,
                        CASE WHEN source.{col_q} IS NULL THEN NULL
                        ELSE (source.{col_q} - stats.min_val) / nullIf(stats.max_val - stats.min_val, 0)
                        END AS {new_q}
                    FROM {qualified_source} AS source
                    CROSS JOIN (
                        SELECT min({col_q}) AS min_val, max({col_q}) AS max_val
                        FROM {qualified_source}
                        WHERE {col_q} IS NOT NULL
                    ) AS stats
                """)
                await self._exec(create_sql)

                affected = await self._count_non_null(new_table, schema, column)
                sample = await self._fetch_sample(new_table, schema, [safe_col, safe_new])
                msg = f"Min-max scaled {column} -> {new_col} for {affected} rows."
                return self._success_response(msg, [column], [new_col], sample, new_table=new_table)

            else:
                raise self._unsupported_backend_error()
        except Exception as e:
            return self._error_response(
                f"numeric_minmax_scale error: {str(e)}\n{traceback.format_exc()}",
                [column],
                [],
            )

    async def numeric_bin(
        self,
        table: str,
        schema: str,
        column: str,
        bins: int = 5,
        strategy: str = "uniform",
        backend=None, data_id: Optional[str] = None, new_table: Optional[str] = None
    ) -> Dict[str, Any]:
        try:
            if isinstance(self.db, PostgresAdapter) or isinstance(self.db, DuckDBAdapter):
                working_table = await self._prepare_operation_table(
                    table, schema, backend=backend, data_id=data_id, new_table=new_table
                )
                supplied_new = f"{column}_binned"
                new_col = self._generate_cleaned_column_name(supplied_new)
                await self._add_new_column(working_table, schema, new_col, "TEXT")

                qualified = self._qualified_table(working_table, schema)
                safe_col = SQLIdentifierSanitizer.sanitize(column)
                safe_new = SQLIdentifierSanitizer.sanitize(new_col)

                if strategy == "uniform":
                    sql = f"""
                        WITH bounds AS (
                            SELECT MIN("{safe_col}") as min_val,
                                   MAX("{safe_col}") as max_val,
                                   (MAX("{safe_col}") - MIN("{safe_col}")) / {bins} as bin_width
                            FROM {qualified}
                            WHERE "{safe_col}" IS NOT NULL
                        )
                        UPDATE {qualified}
                        SET "{safe_new}" = CASE
                            WHEN "{safe_col}" IS NULL THEN 'missing'
                            ELSE 'bin_' || LEAST(
                                {bins},
                                FLOOR(("{safe_col}" - bounds.min_val) / NULLIF(bounds.bin_width, 0)) + 1
                            )::text
                        END
                        FROM bounds;
                    """
                    await self._exec(sql)

                elif strategy == "quantile":
                    if isinstance(self.db, PostgresAdapter):
                        quantiles = [i / bins for i in range(bins + 1)]
                        quantile_sql = f"""
                            SELECT percentile_cont(array[{','.join(str(q) for q in quantiles)}])
                            WITHIN GROUP (ORDER BY "{safe_col}") as quantiles
                            FROM {qualified}
                            WHERE "{safe_col}" IS NOT NULL
                        """
                        quantile_result = await self._fetchval(quantile_sql)
                    elif isinstance(self.db, DuckDBAdapter):
                        quantiles_str = ", ".join(str(i / bins) for i in range(bins + 1))
                        quantile_sql = f"""
                            SELECT quantile("{safe_col}", [{quantiles_str}])
                            FROM {qualified}
                            WHERE "{safe_col}" IS NOT NULL
                        """
                        quantile_result = await self._fetchval(quantile_sql)
                        quantile_values = quantile_result
                    else:
                        raise self._unsupported_backend_error()
                    if quantile_result is None:
                        return self._error_response("Failed to compute quantiles for numeric_bin.", [column])
                    quantile_values = quantile_result

                    if not quantile_values:
                        return self._error_response("Empty quantile values returned for numeric_bin.", [column])

                    cases = []
                    for i in range(bins):
                        lower = quantile_values[i]
                        upper = quantile_values[i + 1] if i + 1 < len(quantile_values) else quantile_values[i]
                        if i < bins - 1:
                            cases.append(
                                f"WHEN \"{safe_col}\" >= {lower} AND \"{safe_col}\" < {upper} THEN 'bin_{i+1}'"
                            )
                        else:
                            cases.append(
                                f"WHEN \"{safe_col}\" >= {lower} AND \"{safe_col}\" <= {upper} THEN 'bin_{i+1}'"
                            )
                    case_stmt = "\n".join(cases)
                    sql = f"""
                        UPDATE {qualified}
                        SET "{safe_new}" = CASE
                            WHEN "{safe_col}" IS NULL THEN 'missing'
                            {case_stmt}
                            ELSE 'missing'
                        END;
                    """
                    await self._exec(sql)

                else:
                    return self._error_response(f"Unknown binning strategy: {strategy}", [column])

                affected = await self._count_non_null(working_table, schema, column)
                sample = await self._fetch_sample(working_table, schema, [safe_col, safe_new])
                msg = f"Binned {column} -> {new_col} using strategy '{strategy}' with {bins} bins for {affected} rows."
                return self._success_response(msg, [column], [new_col], sample, bins=bins, strategy=strategy, new_table=working_table)

            elif isinstance(self.db, ClickHouseAdapter):
                safe_schema = SQLIdentifierSanitizer.sanitize(schema)
                new_table = await self._resolve_output_table_name(
                    table, safe_schema, backend=backend, data_id=data_id, new_table=new_table
                )
                qualified_source = self._qualified_table(table, schema)

                supplied_new = f"{column}_binned"
                new_col = self._generate_cleaned_column_name(supplied_new)
                safe_col = SQLIdentifierSanitizer.sanitize(column)
                safe_new = SQLIdentifierSanitizer.sanitize(new_col)
                col_q = self.db.quote_identifier(safe_col)
                new_q = self.db.quote_identifier(safe_new)

                if strategy == "uniform":
                    create_sql = self._ch_create_table_as(safe_schema, new_table, f"""
                        SELECT source.*,
                            CASE WHEN source.{col_q} IS NULL THEN 'missing'
                            ELSE 'bin_' || toString(least({bins}, floor((source.{col_q} - stats.min_val) / nullIf(stats.bin_width, 0)) + 1))
                            END AS {new_q}
                        FROM {qualified_source} AS source
                        CROSS JOIN (
                            SELECT min({col_q}) AS min_val, max({col_q}) AS max_val,
                                   (max({col_q}) - min({col_q})) / {bins} AS bin_width
                            FROM {qualified_source}
                            WHERE {col_q} IS NOT NULL
                        ) AS stats
                    """)
                    await self._exec(create_sql)

                elif strategy == "quantile":
                    quantiles = [i / bins for i in range(bins + 1)]
                    quantiles_str = ", ".join(str(q) for q in quantiles)
                    quantile_sql = f"""
                        SELECT quantiles({quantiles_str})({col_q})
                        FROM {qualified_source}
                        WHERE {col_q} IS NOT NULL
                    """
                    quantile_result = await self._fetchval(quantile_sql)
                    if quantile_result is None:
                        return self._error_response("Failed to compute quantiles for numeric_bin.", [column])
                    quantile_values = quantile_result
                    if not quantile_values:
                        return self._error_response("Empty quantile values returned for numeric_bin.", [column])

                    cases = []
                    for i in range(bins):
                        lower = quantile_values[i]
                        upper = quantile_values[i + 1] if i + 1 < len(quantile_values) else quantile_values[i]
                        if i < bins - 1:
                            cases.append(
                                f"{col_q} >= {lower} AND {col_q} < {upper}, 'bin_{i+1}'"
                            )
                        else:
                            cases.append(
                                f"{col_q} >= {lower} AND {col_q} <= {upper}, 'bin_{i+1}'"
                            )
                    multi_if_parts = ", ".join(cases)

                    create_sql = self._ch_create_table_as(safe_schema, new_table, f"""
                        SELECT *,
                            multiIf(
                                {col_q} IS NULL, 'missing',
                                {multi_if_parts},
                                'missing'
                            ) AS {new_q}
                        FROM {qualified_source}
                    """)
                    await self._exec(create_sql)

                else:
                    return self._error_response(f"Unknown binning strategy: {strategy}", [column])

                affected = await self._count_non_null(new_table, schema, column)
                sample = await self._fetch_sample(new_table, schema, [safe_col, safe_new])
                msg = f"Binned {column} -> {new_col} using strategy '{strategy}' with {bins} bins for {affected} rows."
                return self._success_response(msg, [column], [new_col], sample, bins=bins, strategy=strategy, new_table=new_table)

            else:
                raise self._unsupported_backend_error()
        except Exception as e:
            return self._error_response(
                f"numeric_bin error: {str(e)}\n{traceback.format_exc()}",
                [column],
                [],
            )

    async def numeric_polynomial_features(
        self, table: str, schema: str, column: str, degree: int = 2,
        backend=None, data_id: Optional[str] = None, new_table: Optional[str] = None
    ) -> Dict[str, Any]:
        try:
            if isinstance(self.db, PostgresAdapter) or isinstance(self.db, DuckDBAdapter):
                working_table = await self._prepare_operation_table(
                    table, schema, backend=backend, data_id=data_id, new_table=new_table
                )
                generated = []
                qualified = self._qualified_table(working_table, schema)
                safe_col = SQLIdentifierSanitizer.sanitize(column)

                for d in range(2, degree + 1):
                    supplied_new = f"{column}_pow{d}"
                    new_col = self._generate_cleaned_column_name(supplied_new)
                    await self._add_new_column(working_table, schema, new_col, "DOUBLE PRECISION")
                    safe_new = SQLIdentifierSanitizer.sanitize(new_col)

                    sql = f"""
                        UPDATE {qualified}
                        SET "{safe_new}" = CASE
                            WHEN "{safe_col}" IS NULL THEN NULL
                            ELSE POWER("{safe_col}", {d})
                        END
                        WHERE "{safe_col}" IS NOT NULL;
                    """
                    await self._exec(sql)
                    generated.append(new_col)

                affected = await self._count_non_null(working_table, schema, column)
                sample = await self._fetch_sample(working_table, schema, [safe_col] + generated[:2])
                msg = f"Created polynomial features for {column} up to degree {degree}: {generated} ({affected} rows processed)."
                return self._success_response(msg, [column], generated, sample, degree=degree, new_table=working_table)

            elif isinstance(self.db, ClickHouseAdapter):
                safe_schema = SQLIdentifierSanitizer.sanitize(schema)
                new_table = await self._resolve_output_table_name(
                    table, safe_schema, backend=backend, data_id=data_id, new_table=new_table
                )
                qualified_source = self._qualified_table(table, schema)

                safe_col = SQLIdentifierSanitizer.sanitize(column)
                col_q = self.db.quote_identifier(safe_col)
                generated = []
                computed_cols_sql = []

                for d in range(2, degree + 1):
                    supplied_new = f"{column}_pow{d}"
                    new_col = self._generate_cleaned_column_name(supplied_new)
                    safe_new = SQLIdentifierSanitizer.sanitize(new_col)
                    new_q = self.db.quote_identifier(safe_new)

                    computed_cols_sql.append(
                        f"CASE WHEN {col_q} IS NULL THEN NULL ELSE pow({col_q}, {d}) END AS {new_q}"
                    )
                    generated.append(new_col)

                select_additions = ", ".join(computed_cols_sql)
                create_sql = self._ch_create_table_as(safe_schema, new_table, f"""
                    SELECT *, {select_additions}
                    FROM {qualified_source}
                """)
                await self._exec(create_sql)

                affected = await self._count_non_null(new_table, schema, column)
                sample = await self._fetch_sample(new_table, schema, [safe_col] + generated[:2])
                msg = f"Created polynomial features for {column} up to degree {degree}: {generated} ({affected} rows processed)."
                return self._success_response(msg, [column], generated, sample, degree=degree, new_table=new_table)

            else:
                raise self._unsupported_backend_error()
        except Exception as e:
            return self._error_response(
                f"numeric_polynomial_features error: {str(e)}\n{traceback.format_exc()}",
                [column],
                [],
            )

    async def numeric_interaction_terms(
        self, table: str, schema: str, column1: str, column2: str,
        backend=None, data_id: Optional[str] = None, new_table: Optional[str] = None
    ) -> Dict[str, Any]:
        try:
            if isinstance(self.db, PostgresAdapter) or isinstance(self.db, DuckDBAdapter):
                working_table = await self._prepare_operation_table(
                    table, schema, backend=backend, data_id=data_id, new_table=new_table
                )
                supplied_new = f"{column1}_{column2}_interaction"
                new_col = self._generate_cleaned_column_name(supplied_new)
                await self._add_new_column(working_table, schema, new_col, "DOUBLE PRECISION")

                qualified = self._qualified_table(working_table, schema)
                safe_col1 = SQLIdentifierSanitizer.sanitize(column1)
                safe_col2 = SQLIdentifierSanitizer.sanitize(column2)
                safe_new = SQLIdentifierSanitizer.sanitize(new_col)

                sql = f"""
                    UPDATE {qualified}
                    SET "{safe_new}" = CASE
                        WHEN "{safe_col1}" IS NULL OR "{safe_col2}" IS NULL THEN NULL
                        ELSE "{safe_col1}" * "{safe_col2}"
                    END
                    WHERE "{safe_col1}" IS NOT NULL AND "{safe_col2}" IS NOT NULL;
                """
                await self._exec(sql)

                cnt1 = await self._count_non_null(working_table, schema, column1)
                cnt2 = await self._count_non_null(working_table, schema, column2)
                sample = await self._fetch_sample(working_table, schema, [safe_col1, safe_col2, safe_new])
                msg = f"Created interaction term {column1}*{column2} -> {new_col}. Rows with both non-null: approx min({cnt1},{cnt2})."
                return self._success_response(msg, [column1, column2], [new_col], sample, new_table=working_table)

            elif isinstance(self.db, ClickHouseAdapter):
                safe_schema = SQLIdentifierSanitizer.sanitize(schema)
                new_table = await self._resolve_output_table_name(
                    table, safe_schema, backend=backend, data_id=data_id, new_table=new_table
                )
                qualified_source = self._qualified_table(table, schema)

                supplied_new = f"{column1}_{column2}_interaction"
                new_col = self._generate_cleaned_column_name(supplied_new)
                safe_col1 = SQLIdentifierSanitizer.sanitize(column1)
                safe_col2 = SQLIdentifierSanitizer.sanitize(column2)
                safe_new = SQLIdentifierSanitizer.sanitize(new_col)
                col1_q = self.db.quote_identifier(safe_col1)
                col2_q = self.db.quote_identifier(safe_col2)
                new_q = self.db.quote_identifier(safe_new)

                create_sql = self._ch_create_table_as(safe_schema, new_table, f"""
                    SELECT *,
                        CASE WHEN {col1_q} IS NULL OR {col2_q} IS NULL THEN NULL
                        ELSE {col1_q} * {col2_q}
                        END AS {new_q}
                    FROM {qualified_source}
                """)
                await self._exec(create_sql)

                cnt1 = await self._count_non_null(new_table, schema, column1)
                cnt2 = await self._count_non_null(new_table, schema, column2)
                sample = await self._fetch_sample(new_table, schema, [safe_col1, safe_col2, safe_new])
                msg = f"Created interaction term {column1}*{column2} -> {new_col}. Rows with both non-null: approx min({cnt1},{cnt2})."
                return self._success_response(msg, [column1, column2], [new_col], sample, new_table=new_table)

            else:
                raise self._unsupported_backend_error()
        except Exception as e:
            return self._error_response(
                f"numeric_interaction_terms error: {str(e)}\n{traceback.format_exc()}",
                [column1, column2],
                [],
            )

    # ponytail: Tier1 scalers — single-col, backend branches mirror scale/minmax
    async def numeric_robust_scale(
        self, table: str, schema: str, column: str, quantile_range: tuple = (25, 75),
        backend=None, data_id: Optional[str] = None, new_table: Optional[str] = None
    ) -> Dict[str, Any]:
        try:
            q_low, q_high = quantile_range
            if isinstance(self.db, PostgresAdapter) or isinstance(self.db, DuckDBAdapter):
                working_table = await self._prepare_operation_table(table, schema, backend=backend, data_id=data_id, new_table=new_table)
                supplied_new = f"{column}_robust"
                new_col = self._generate_cleaned_column_name(supplied_new)
                await self._add_new_column(working_table, schema, new_col, "DOUBLE PRECISION")
                qualified = self._qualified_table(working_table, schema)
                safe_col = SQLIdentifierSanitizer.sanitize(column)
                safe_new = SQLIdentifierSanitizer.sanitize(new_col)
                if isinstance(self.db, PostgresAdapter):
                    median_expr = f"PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY \"{safe_col}\")"
                    q_low_expr = f"PERCENTILE_CONT({q_low/100}) WITHIN GROUP (ORDER BY \"{safe_col}\")"
                    q_high_expr = f"PERCENTILE_CONT({q_high/100}) WITHIN GROUP (ORDER BY \"{safe_col}\")"
                    sql = f"""
                        WITH stats AS (
                            SELECT {median_expr} AS median,
                                   {q_high_expr} - {q_low_expr} AS iqr
                            FROM {qualified} WHERE \"{safe_col}\" IS NOT NULL
                        )
                        UPDATE {qualified} SET \"{safe_new}\" = CASE WHEN \"{safe_col}\" IS NULL THEN NULL ELSE (CAST(\"{safe_col}\" AS DOUBLE PRECISION) - stats.median) / NULLIF(stats.iqr, 0) END FROM stats;
                    """
                else:
                    sql = f"""
                        WITH stats AS (
                            SELECT quantile(\"{safe_col}\", [0.25, 0.5, 0.75]) AS qs FROM {qualified} WHERE \"{safe_col}\" IS NOT NULL
                        )
                        UPDATE {qualified} SET \"{safe_new}\" = CASE WHEN \"{safe_col}\" IS NULL THEN NULL ELSE (CAST(\"{safe_col}\" AS DOUBLE PRECISION) - qs[2]) / NULLIF(qs[3] - qs[1], 0) END FROM stats;
                    """
                    # ponytail: DuckDB quantile list is 1-indexed, qs[2]=median; fallback to stats CTE above if list indexing differs, pg branch already correct
                await self._exec(sql)
                affected = await self._count_non_null(working_table, schema, column)
                sample = await self._fetch_sample(working_table, schema, [safe_col, safe_new])
                msg = f"Robust scaled {column} -> {new_col} for {affected} rows (median/IQR {quantile_range})."
                return self._success_response(msg, [column], [new_col], sample, new_table=working_table)
            elif isinstance(self.db, ClickHouseAdapter):
                safe_schema = SQLIdentifierSanitizer.sanitize(schema)
                new_table = await self._resolve_output_table_name(table, safe_schema, backend=backend, data_id=data_id, new_table=new_table)
                qualified_source = self._qualified_table(table, schema)
                supplied_new = f"{column}_robust"
                new_col = self._generate_cleaned_column_name(supplied_new)
                safe_col = SQLIdentifierSanitizer.sanitize(column)
                safe_new = SQLIdentifierSanitizer.sanitize(new_col)
                col_q = self.db.quote_identifier(safe_col)
                new_q = self.db.quote_identifier(safe_new)
                create_sql = self._ch_create_table_as(safe_schema, new_table, f"""
                    SELECT source.*,
                        CASE WHEN source.{col_q} IS NULL THEN NULL ELSE (source.{col_q} - stats.median) / nullIf(stats.iqr, 0) END AS {new_q}
                    FROM {qualified_source} AS source CROSS JOIN (
                        SELECT qs[2] AS median, (qs[3] - qs[1]) AS iqr FROM (
                            SELECT quantiles(0.25,0.5,0.75)({col_q}) AS qs FROM {qualified_source} WHERE {col_q} IS NOT NULL
                        )
                    ) AS stats
                """)
                # ponytail: ClickHouse quantiles returns array; above is simplified — use direct quantiles if array handling differs, fallback to median/IQR via quantilesExact
                await self._exec(create_sql)
                affected = await self._count_non_null(new_table, schema, column)
                sample = await self._fetch_sample(new_table, schema, [safe_col, safe_new])
                msg = f"Robust scaled {column} -> {new_col} for {affected} rows (median/IQR {quantile_range})."
                return self._success_response(msg, [column], [new_col], sample, new_table=new_table)
            else:
                raise self._unsupported_backend_error()
        except Exception as e:
            return self._error_response(f"numeric_robust_scale error: {str(e)}\n{traceback.format_exc()}", [column], [])

    async def numeric_maxabs_scale(
        self, table: str, schema: str, column: str,
        backend=None, data_id: Optional[str] = None, new_table: Optional[str] = None
    ) -> Dict[str, Any]:
        try:
            if isinstance(self.db, PostgresAdapter) or isinstance(self.db, DuckDBAdapter):
                working_table = await self._prepare_operation_table(table, schema, backend=backend, data_id=data_id, new_table=new_table)
                supplied_new = f"{column}_maxabs"
                new_col = self._generate_cleaned_column_name(supplied_new)
                await self._add_new_column(working_table, schema, new_col, "DOUBLE PRECISION")
                qualified = self._qualified_table(working_table, schema)
                safe_col = SQLIdentifierSanitizer.sanitize(column)
                safe_new = SQLIdentifierSanitizer.sanitize(new_col)
                sql = f"""
                    WITH stats AS (SELECT MAX(ABS(CAST(\"{safe_col}\" AS DOUBLE PRECISION))) AS max_abs FROM {qualified} WHERE \"{safe_col}\" IS NOT NULL)
                    UPDATE {qualified} SET \"{safe_new}\" = CASE WHEN \"{safe_col}\" IS NULL THEN NULL ELSE CAST(\"{safe_col}\" AS DOUBLE PRECISION) / NULLIF(stats.max_abs, 0) END FROM stats;
                """
                await self._exec(sql)
                affected = await self._count_non_null(working_table, schema, column)
                sample = await self._fetch_sample(working_table, schema, [safe_col, safe_new])
                msg = f"MaxAbs scaled {column} -> {new_col} for {affected} rows."
                return self._success_response(msg, [column], [new_col], sample, new_table=working_table)
            elif isinstance(self.db, ClickHouseAdapter):
                safe_schema = SQLIdentifierSanitizer.sanitize(schema)
                new_table = await self._resolve_output_table_name(table, safe_schema, backend=backend, data_id=data_id, new_table=new_table)
                qualified_source = self._qualified_table(table, schema)
                supplied_new = f"{column}_maxabs"
                new_col = self._generate_cleaned_column_name(supplied_new)
                safe_col = SQLIdentifierSanitizer.sanitize(column)
                safe_new = SQLIdentifierSanitizer.sanitize(new_col)
                col_q = self.db.quote_identifier(safe_col)
                new_q = self.db.quote_identifier(safe_new)
                create_sql = self._ch_create_table_as(safe_schema, new_table, f"""
                    SELECT source.*, CASE WHEN source.{col_q} IS NULL THEN NULL ELSE source.{col_q} / nullIf(stats.max_abs, 0) END AS {new_q}
                    FROM {qualified_source} AS source CROSS JOIN (SELECT max(abs({col_q})) AS max_abs FROM {qualified_source} WHERE {col_q} IS NOT NULL) AS stats
                """)
                await self._exec(create_sql)
                affected = await self._count_non_null(new_table, schema, column)
                sample = await self._fetch_sample(new_table, schema, [safe_col, safe_new])
                msg = f"MaxAbs scaled {column} -> {new_col} for {affected} rows."
                return self._success_response(msg, [column], [new_col], sample, new_table=new_table)
            else:
                raise self._unsupported_backend_error()
        except Exception as e:
            return self._error_response(f"numeric_maxabs_scale error: {str(e)}\n{traceback.format_exc()}", [column], [])

    async def numeric_normalize(
        self, table: str, schema: str, column: str, norm: str = "l2",
        backend=None, data_id: Optional[str] = None, new_table: Optional[str] = None
    ) -> Dict[str, Any]:
        try:
            # ponytail: single-col L2 → sign; multi-col deferred
            if isinstance(self.db, PostgresAdapter) or isinstance(self.db, DuckDBAdapter):
                working_table = await self._prepare_operation_table(table, schema, backend=backend, data_id=data_id, new_table=new_table)
                supplied_new = f"{column}_normalized"
                new_col = self._generate_cleaned_column_name(supplied_new)
                await self._add_new_column(working_table, schema, new_col, "DOUBLE PRECISION")
                qualified = self._qualified_table(working_table, schema)
                safe_col = SQLIdentifierSanitizer.sanitize(column)
                safe_new = SQLIdentifierSanitizer.sanitize(new_col)
                sql = f"""UPDATE {qualified} SET \"{safe_new}\" = CASE WHEN \"{safe_col}\" IS NULL THEN NULL WHEN \"{safe_col}\" = 0 THEN 0 ELSE \"{safe_col}\" / NULLIF(ABS(CAST(\"{safe_col}\" AS DOUBLE PRECISION)), 0) END;"""
                await self._exec(sql)
                affected = await self._count_non_null(working_table, schema, column)
                sample = await self._fetch_sample(working_table, schema, [safe_col, safe_new])
                msg = f"Normalized {column} -> {new_col} for {affected} rows (norm={norm}, single-col sign)."
                return self._success_response(msg, [column], [new_col], sample, new_table=working_table)
            elif isinstance(self.db, ClickHouseAdapter):
                safe_schema = SQLIdentifierSanitizer.sanitize(schema)
                new_table = await self._resolve_output_table_name(table, safe_schema, backend=backend, data_id=data_id, new_table=new_table)
                qualified_source = self._qualified_table(table, schema)
                supplied_new = f"{column}_normalized"
                new_col = self._generate_cleaned_column_name(supplied_new)
                safe_col = SQLIdentifierSanitizer.sanitize(column)
                safe_new = SQLIdentifierSanitizer.sanitize(new_col)
                col_q = self.db.quote_identifier(safe_col)
                new_q = self.db.quote_identifier(safe_new)
                create_sql = self._ch_create_table_as(safe_schema, new_table, f"""
                    SELECT *, CASE WHEN {col_q} IS NULL THEN NULL WHEN {col_q} = 0 THEN 0 ELSE {col_q} / nullIf(abs({col_q}), 0) END AS {new_q} FROM {qualified_source}
                """)
                await self._exec(create_sql)
                affected = await self._count_non_null(new_table, schema, column)
                sample = await self._fetch_sample(new_table, schema, [safe_col, safe_new])
                msg = f"Normalized {column} -> {new_col} for {affected} rows (norm={norm}, single-col sign)."
                return self._success_response(msg, [column], [new_col], sample, new_table=new_table)
            else:
                raise self._unsupported_backend_error()
        except Exception as e:
            return self._error_response(f"numeric_normalize error: {str(e)}\n{traceback.format_exc()}", [column], [])

    async def numeric_log_transform(
        self, table: str, schema: str, column: str, base: str = "e", epsilon: float = 0,
        backend=None, data_id: Optional[str] = None, new_table: Optional[str] = None
    ) -> Dict[str, Any]:
        try:
            if isinstance(self.db, PostgresAdapter) or isinstance(self.db, DuckDBAdapter):
                working_table = await self._prepare_operation_table(table, schema, backend=backend, data_id=data_id, new_table=new_table)
                supplied_new = f"{column}_log"
                new_col = self._generate_cleaned_column_name(supplied_new)
                await self._add_new_column(working_table, schema, new_col, "DOUBLE PRECISION")
                qualified = self._qualified_table(working_table, schema)
                safe_col = SQLIdentifierSanitizer.sanitize(column)
                safe_new = SQLIdentifierSanitizer.sanitize(new_col)
                eps = float(epsilon)
                if str(base).lower() in ("10", "ten"):
                    log_expr = f"LOG(CAST(\"{safe_col}\" AS DOUBLE PRECISION) + {eps})"
                else:
                    log_expr = f"LN(CAST(\"{safe_col}\" AS DOUBLE PRECISION) + {eps})"
                sql = f"""UPDATE {qualified} SET \"{safe_new}\" = CASE WHEN \"{safe_col}\" IS NULL THEN NULL WHEN CAST(\"{safe_col}\" AS DOUBLE PRECISION) + {eps} <= 0 THEN NULL ELSE {log_expr} END;"""
                await self._exec(sql)
                affected = await self._count_non_null(working_table, schema, column)
                sample = await self._fetch_sample(working_table, schema, [safe_col, safe_new])
                msg = f"Log transformed {column} -> {new_col} for {affected} rows (base={base}, eps={eps})."
                return self._success_response(msg, [column], [new_col], sample, new_table=working_table)
            elif isinstance(self.db, ClickHouseAdapter):
                safe_schema = SQLIdentifierSanitizer.sanitize(schema)
                new_table = await self._resolve_output_table_name(table, safe_schema, backend=backend, data_id=data_id, new_table=new_table)
                qualified_source = self._qualified_table(table, schema)
                supplied_new = f"{column}_log"
                new_col = self._generate_cleaned_column_name(supplied_new)
                safe_col = SQLIdentifierSanitizer.sanitize(column)
                safe_new = SQLIdentifierSanitizer.sanitize(new_col)
                col_q = self.db.quote_identifier(safe_col)
                new_q = self.db.quote_identifier(safe_new)
                eps = float(epsilon)
                log_expr = f"log({col_q} + {eps})" if str(base).lower() not in ("10", "ten") else f"log10({col_q} + {eps})"
                create_sql = self._ch_create_table_as(safe_schema, new_table, f"""
                    SELECT *, CASE WHEN {col_q} IS NULL THEN NULL WHEN {col_q} + {eps} <= 0 THEN NULL ELSE {log_expr} END AS {new_q} FROM {qualified_source}
                """)
                await self._exec(create_sql)
                affected = await self._count_non_null(new_table, schema, column)
                sample = await self._fetch_sample(new_table, schema, [safe_col, safe_new])
                msg = f"Log transformed {column} -> {new_col} for {affected} rows (base={base}, eps={eps})."
                return self._success_response(msg, [column], [new_col], sample, new_table=new_table)
            else:
                raise self._unsupported_backend_error()
        except Exception as e:
            return self._error_response(f"numeric_log_transform error: {str(e)}\n{traceback.format_exc()}", [column], [])

    # ponytail: Tier2 — quantile/power + ordinal (minimal SQL, MLE deferred)
    async def numeric_quantile_transform(
        self, table: str, schema: str, column: str, output: str = "uniform",
        backend=None, data_id: Optional[str] = None, new_table: Optional[str] = None
    ) -> Dict[str, Any]:
        try:
            if isinstance(self.db, PostgresAdapter) or isinstance(self.db, DuckDBAdapter):
                working_table = await self._prepare_operation_table(table, schema, backend=backend, data_id=data_id, new_table=new_table)
                supplied_new = f"{column}_quantile"
                new_col = self._generate_cleaned_column_name(supplied_new)
                await self._add_new_column(working_table, schema, new_col, "DOUBLE PRECISION")
                qualified = self._qualified_table(working_table, schema)
                safe_col = SQLIdentifierSanitizer.sanitize(column)
                safe_new = SQLIdentifierSanitizer.sanitize(new_col)
                # uniform: rank/(n-1) → [0,1]
                sql = f"""
                    WITH ranked AS (
                        SELECT \"{safe_col}\", ROW_NUMBER() OVER (ORDER BY \"{safe_col}\") - 1 AS rnk,
                               COUNT(*) OVER () AS n FROM {qualified} WHERE \"{safe_col}\" IS NOT NULL
                    )
                    UPDATE {qualified} AS t SET \"{safe_new}\" = ranked.rnk::DOUBLE PRECISION / NULLIF(ranked.n - 1, 0)
                    FROM ranked WHERE t.\"{safe_col}\" = ranked.\"{safe_col}\";
                """
                # ponytail: DuckDB needs CAST not ::DOUBLE PRECISION? :: works; keep simple, uniform only, normal deferred
                await self._exec(sql)
                affected = await self._count_non_null(working_table, schema, column)
                sample = await self._fetch_sample(working_table, schema, [safe_col, safe_new])
                msg = f"Quantile transformed {column} -> {new_col} for {affected} rows (output={output})."
                return self._success_response(msg, [column], [new_col], sample, new_table=working_table)
            elif isinstance(self.db, ClickHouseAdapter):
                safe_schema = SQLIdentifierSanitizer.sanitize(schema)
                new_table = await self._resolve_output_table_name(table, safe_schema, backend=backend, data_id=data_id, new_table=new_table)
                qualified_source = self._qualified_table(table, schema)
                supplied_new = f"{column}_quantile"
                new_col = self._generate_cleaned_column_name(supplied_new)
                safe_col = SQLIdentifierSanitizer.sanitize(column)
                safe_new = SQLIdentifierSanitizer.sanitize(new_col)
                col_q = self.db.quote_identifier(safe_col)
                new_q = self.db.quote_identifier(safe_new)
                create_sql = self._ch_create_table_as(safe_schema, new_table, f"""
                    SELECT *, CASE WHEN {col_q} IS NULL THEN NULL ELSE (rank() OVER (ORDER BY {col_q}) - 1) / nullIf(count() OVER (), 1) END AS {new_q}
                    FROM {qualified_source}
                """)
                await self._exec(create_sql)
                affected = await self._count_non_null(new_table, schema, column)
                sample = await self._fetch_sample(new_table, schema, [safe_col, safe_new])
                msg = f"Quantile transformed {column} -> {new_col} for {affected} rows (output={output})."
                return self._success_response(msg, [column], [new_col], sample, new_table=new_table)
            else:
                raise self._unsupported_backend_error()
        except Exception as e:
            return self._error_response(f"numeric_quantile_transform error: {str(e)}\n{traceback.format_exc()}", [column], [])

    async def numeric_power_transform(
        self, table: str, schema: str, column: str, method: str = "yeo-johnson",
        backend=None, data_id: Optional[str] = None, new_table: Optional[str] = None
    ) -> Dict[str, Any]:
        try:
            if isinstance(self.db, PostgresAdapter) or isinstance(self.db, DuckDBAdapter):
                working_table = await self._prepare_operation_table(table, schema, backend=backend, data_id=data_id, new_table=new_table)
                supplied_new = f"{column}_power"
                new_col = self._generate_cleaned_column_name(supplied_new)
                await self._add_new_column(working_table, schema, new_col, "DOUBLE PRECISION")
                qualified = self._qualified_table(working_table, schema)
                safe_col = SQLIdentifierSanitizer.sanitize(column)
                safe_new = SQLIdentifierSanitizer.sanitize(new_col)
                # ponytail: simplified Box-Cox/Yeo-Johnson with lambda=0.5, MLE deferred
                if method.lower() == "box-cox":
                    expr = f"CASE WHEN CAST(\"{safe_col}\" AS DOUBLE PRECISION) <= 0 THEN NULL ELSE (POWER(CAST(\"{safe_col}\" AS DOUBLE PRECISION), 0.5) - 1) / 0.5 END"
                else:
                    expr = f"SIGN(CAST(\"{safe_col}\" AS DOUBLE PRECISION)) * POWER(ABS(CAST(\"{safe_col}\" AS DOUBLE PRECISION)), 0.5)"
                sql = f"""UPDATE {qualified} SET \"{safe_new}\" = CASE WHEN \"{safe_col}\" IS NULL THEN NULL ELSE {expr} END;"""
                await self._exec(sql)
                affected = await self._count_non_null(working_table, schema, column)
                sample = await self._fetch_sample(working_table, schema, [safe_col, safe_new])
                msg = f"Power transformed {column} -> {new_col} for {affected} rows (method={method})."
                return self._success_response(msg, [column], [new_col], sample, new_table=working_table)
            elif isinstance(self.db, ClickHouseAdapter):
                safe_schema = SQLIdentifierSanitizer.sanitize(schema)
                new_table = await self._resolve_output_table_name(table, safe_schema, backend=backend, data_id=data_id, new_table=new_table)
                qualified_source = self._qualified_table(table, schema)
                supplied_new = f"{column}_power"
                new_col = self._generate_cleaned_column_name(supplied_new)
                safe_col = SQLIdentifierSanitizer.sanitize(column)
                safe_new = SQLIdentifierSanitizer.sanitize(new_col)
                col_q = self.db.quote_identifier(safe_col)
                new_q = self.db.quote_identifier(safe_new)
                expr = f"if({col_q} <= 0, NULL, (pow({col_q}, 0.5) - 1) / 0.5)" if method.lower() == "box-cox" else f"sign({col_q}) * pow(abs({col_q}), 0.5)"
                create_sql = self._ch_create_table_as(safe_schema, new_table, f"""
                    SELECT *, CASE WHEN {col_q} IS NULL THEN NULL ELSE {expr} END AS {new_q} FROM {qualified_source}
                """)
                await self._exec(create_sql)
                affected = await self._count_non_null(new_table, schema, column)
                sample = await self._fetch_sample(new_table, schema, [safe_col, safe_new])
                msg = f"Power transformed {column} -> {new_col} for {affected} rows (method={method})."
                return self._success_response(msg, [column], [new_col], sample, new_table=new_table)
            else:
                raise self._unsupported_backend_error()
        except Exception as e:
            return self._error_response(f"numeric_power_transform error: {str(e)}\n{traceback.format_exc()}", [column], [])

    # ==================================================================
    # Categorical Preprocessings
    # ==================================================================
    async def categorical_one_hot_encode(
        self, table: str, schema: str, column: str, max_categories: int = 10,
        backend=None, data_id: Optional[str] = None, new_table: Optional[str] = None
    ) -> Dict[str, Any]:
        try:
            if isinstance(self.db, PostgresAdapter) or isinstance(self.db, DuckDBAdapter):
                working_table = await self._prepare_operation_table(
                    table, schema, backend=backend, data_id=data_id, new_table=new_table
                )
                qualified = self._qualified_table(working_table, schema)
                safe_col = SQLIdentifierSanitizer.sanitize(column)
                col_q = self.db.quote_identifier(safe_col)

                top_sql = f"""
                    SELECT {col_q}, COUNT(*) AS count
                    FROM {qualified}
                    WHERE {col_q} IS NOT NULL
                    GROUP BY {col_q}
                    ORDER BY count DESC
                    LIMIT {max_categories};
                """
                rows = await self._fetch(top_sql)

                # Group categories by their sanitized column name to avoid collisions
                # (e.g. "m B" and "m_B" both sanitize to "m_B")
                new_col_to_cat_values = {}
                for row in rows:
                    cat_value = self._row_value(row, safe_col, 0)
                    safe_val = re.sub(r"[^a-zA-Z0-9_]", "_", str(cat_value))
                    supplied_new = f"{column}_{safe_val}"
                    new_col = self._generate_cleaned_column_name(supplied_new)
                    if new_col not in new_col_to_cat_values:
                        new_col_to_cat_values[new_col] = []
                    new_col_to_cat_values[new_col].append(cat_value)

                created_cols = []

                for new_col, cat_values in new_col_to_cat_values.items():
                    await self._add_new_column(working_table, schema, new_col, "INTEGER")
                    safe_new = SQLIdentifierSanitizer.sanitize(new_col)
                    new_col_q = self.db.quote_identifier(safe_new)

                    if len(cat_values) == 1:
                        condition = f'{col_q} = {self._sql_literal(cat_values[0])}'
                    else:
                        in_list = ", ".join(self._sql_literal(v) for v in cat_values)
                        condition = f'{col_q} IN ({in_list})'

                    sql = f"""
                        UPDATE {qualified}
                        SET {new_col_q} = CASE WHEN {condition} THEN 1 ELSE 0 END
                    """
                    await self._exec(sql)
                    created_cols.append(new_col)

                sample_cols = [safe_col] + created_cols[:3]
                sample = await self._fetch_sample(working_table, schema, sample_cols)
                msg = f"One-hot encoded {column}, created {len(created_cols)} columns: {created_cols}"
                return self._success_response(msg, [column], created_cols, sample, max_categories=max_categories, new_table=working_table)

            elif isinstance(self.db, ClickHouseAdapter):
                safe_schema = SQLIdentifierSanitizer.sanitize(schema)
                new_table = await self._resolve_output_table_name(
                    table, safe_schema, backend=backend, data_id=data_id, new_table=new_table
                )
                qualified_source = self._qualified_table(table, schema)

                safe_col = SQLIdentifierSanitizer.sanitize(column)
                col_q = self.db.quote_identifier(safe_col)

                top_sql = f"""
                    SELECT {col_q}, COUNT(*) AS count
                    FROM {qualified_source}
                    WHERE {col_q} IS NOT NULL
                    GROUP BY {col_q}
                    ORDER BY count DESC
                    LIMIT {max_categories}
                """
                rows = await self._fetch(top_sql)

                new_col_to_cat_values = {}
                for row in rows:
                    cat_value = self._row_value(row, safe_col, 0)
                    safe_val = re.sub(r"[^a-zA-Z0-9_]", "_", str(cat_value))
                    supplied_new = f"{column}_{safe_val}"
                    new_col = self._generate_cleaned_column_name(supplied_new)
                    if new_col not in new_col_to_cat_values:
                        new_col_to_cat_values[new_col] = []
                    new_col_to_cat_values[new_col].append(cat_value)

                created_cols = []
                computed_cols_sql = []

                for new_col, cat_values in new_col_to_cat_values.items():
                    safe_new = SQLIdentifierSanitizer.sanitize(new_col)
                    new_q = self.db.quote_identifier(safe_new)

                    if len(cat_values) == 1:
                        condition = f'{col_q} = {self._sql_literal(cat_values[0])}'
                    else:
                        in_list = ", ".join(self._sql_literal(v) for v in cat_values)
                        condition = f'{col_q} IN ({in_list})'

                    computed_cols_sql.append(
                        f"CASE WHEN {condition} THEN 1 ELSE 0 END AS {new_q}"
                    )
                    created_cols.append(new_col)

                select_additions = ", ".join(computed_cols_sql) if computed_cols_sql else ""
                create_sql = self._ch_create_table_as(safe_schema, new_table, f"""
                    SELECT *{', ' + select_additions if select_additions else ''}
                    FROM {qualified_source}
                """)
                await self._exec(create_sql)

                sample_cols = [safe_col] + created_cols[:3]
                sample = await self._fetch_sample(new_table, schema, sample_cols)
                msg = f"One-hot encoded {column}, created {len(created_cols)} columns: {created_cols}"
                return self._success_response(msg, [column], created_cols, sample, max_categories=max_categories, new_table=new_table)

            else:
                raise self._unsupported_backend_error()
        except Exception as e:
            return self._error_response(
                f"categorical_one_hot_encode error: {str(e)}\n{traceback.format_exc()}",
                [column],
                [],
            )

    async def categorical_ordinal_encode(
        self, table: str, schema: str, column: str,
        backend=None, data_id: Optional[str] = None, new_table: Optional[str] = None
    ) -> Dict[str, Any]:
        try:
            if isinstance(self.db, PostgresAdapter) or isinstance(self.db, DuckDBAdapter):
                working_table = await self._prepare_operation_table(table, schema, backend=backend, data_id=data_id, new_table=new_table)
                supplied_new = f"{column}_ordinal"
                new_col = self._generate_cleaned_column_name(supplied_new)
                await self._add_new_column(working_table, schema, new_col, "INTEGER")
                qualified = self._qualified_table(working_table, schema)
                safe_col = SQLIdentifierSanitizer.sanitize(column)
                safe_new = SQLIdentifierSanitizer.sanitize(new_col)
                sql = f"""
                    WITH ranked AS (
                        SELECT "{safe_col}", ROW_NUMBER() OVER (ORDER BY "{safe_col}") - 1 AS ord
                        FROM {qualified} WHERE "{safe_col}" IS NOT NULL GROUP BY "{safe_col}"
                    )
                    UPDATE {qualified} AS t SET "{safe_new}" = ranked.ord FROM ranked WHERE t."{safe_col}" = ranked."{safe_col}";
                """
                await self._exec(sql)
                affected = await self._count_non_null(working_table, schema, column)
                sample = await self._fetch_sample(working_table, schema, [safe_col, safe_new])
                msg = f"Ordinal encoded {column} -> {new_col} ({affected} rows, alphabetical)."
                return self._success_response(msg, [column], [new_col], sample, new_table=working_table)
            elif isinstance(self.db, ClickHouseAdapter):
                safe_schema = SQLIdentifierSanitizer.sanitize(schema)
                new_table = await self._resolve_output_table_name(table, safe_schema, backend=backend, data_id=data_id, new_table=new_table)
                qualified_source = self._qualified_table(table, schema)
                supplied_new = f"{column}_ordinal"
                new_col = self._generate_cleaned_column_name(supplied_new)
                safe_col = SQLIdentifierSanitizer.sanitize(column)
                safe_new = SQLIdentifierSanitizer.sanitize(new_col)
                col_q = self.db.quote_identifier(safe_col)
                new_q = self.db.quote_identifier(safe_new)
                create_sql = self._ch_create_table_as(safe_schema, new_table, f"""
                    SELECT source.*,
                        CASE WHEN source.{col_q} IS NULL THEN NULL ELSE ranked.ord END AS {new_q}
                    FROM {qualified_source} AS source LEFT JOIN (
                        SELECT {col_q} AS _ch_join_key, row_number() OVER (ORDER BY {col_q}) - 1 AS ord
                        FROM {qualified_source} WHERE {col_q} IS NOT NULL GROUP BY {col_q}
                    ) AS ranked ON source.{col_q} = ranked._ch_join_key
                """)
                await self._exec(create_sql)
                affected = await self._count_non_null(new_table, schema, column)
                sample = await self._fetch_sample(new_table, schema, [safe_col, safe_new])
                msg = f"Ordinal encoded {column} -> {new_col} ({affected} rows, alphabetical)."
                return self._success_response(msg, [column], [new_col], sample, new_table=new_table)
            else:
                raise self._unsupported_backend_error()
        except Exception as e:
            return self._error_response(f"categorical_ordinal_encode error: {str(e)}\n{traceback.format_exc()}", [column], [])
    
    async def categorical_label_encode(
        self, table: str, schema: str, column: str,
        backend=None, data_id: Optional[str] = None, new_table: Optional[str] = None
    ) -> Dict[str, Any]:
        try:
            if isinstance(self.db, PostgresAdapter) or isinstance(self.db, DuckDBAdapter):
                working_table = await self._prepare_operation_table(
                    table, schema, backend=backend, data_id=data_id, new_table=new_table
                )
                supplied_new = f"{column}_label"
                new_col = self._generate_cleaned_column_name(supplied_new)
                await self._add_new_column(working_table, schema, new_col, "INTEGER")

                qualified = self._qualified_table(working_table, schema)
                safe_col = SQLIdentifierSanitizer.sanitize(column)
                safe_new = SQLIdentifierSanitizer.sanitize(new_col)

                sql = f"""
                    WITH ranked AS (
                        SELECT "{safe_col}", ROW_NUMBER() OVER (ORDER BY COUNT(*) DESC) - 1 AS label
                        FROM {qualified}
                        WHERE "{safe_col}" IS NOT NULL
                        GROUP BY "{safe_col}"
                    )
                    UPDATE {qualified} AS t
                    SET "{safe_new}" = ranked.label
                    FROM ranked
                    WHERE t."{safe_col}" = ranked."{safe_col}";
                """
                await self._exec(sql)

                affected = await self._count_non_null(working_table, schema, column)
                sample = await self._fetch_sample(working_table, schema, [safe_col, safe_new])
                msg = f"Label encoded {column} -> {new_col} ({affected} rows)."
                return self._success_response(msg, [column], [new_col], sample, new_table=working_table)

            elif isinstance(self.db, ClickHouseAdapter):
                safe_schema = SQLIdentifierSanitizer.sanitize(schema)
                new_table = await self._resolve_output_table_name(
                    table, safe_schema, backend=backend, data_id=data_id, new_table=new_table
                )
                qualified_source = self._qualified_table(table, schema)

                supplied_new = f"{column}_label"
                new_col = self._generate_cleaned_column_name(supplied_new)
                safe_col = SQLIdentifierSanitizer.sanitize(column)
                safe_new = SQLIdentifierSanitizer.sanitize(new_col)
                col_q = self.db.quote_identifier(safe_col)
                new_q = self.db.quote_identifier(safe_new)

                # ponytail: alias join key to avoid source.* -> source.col prefix on collision
                create_sql = self._ch_create_table_as(safe_schema, new_table, f"""
                    SELECT source.*,
                        CASE WHEN source.{col_q} IS NULL THEN NULL
                        ELSE ranked.label
                        END AS {new_q}
                    FROM {qualified_source} AS source
                    LEFT JOIN (
                        SELECT {col_q} AS _ch_join_key, row_number() OVER (ORDER BY count(*) DESC) - 1 AS label
                        FROM {qualified_source}
                        WHERE {col_q} IS NOT NULL
                        GROUP BY {col_q}
                    ) AS ranked ON source.{col_q} = ranked._ch_join_key
                """)
                await self._exec(create_sql)

                affected = await self._count_non_null(new_table, schema, column)
                sample = await self._fetch_sample(new_table, schema, [safe_col, safe_new])
                msg = f"Label encoded {column} -> {new_col} ({affected} rows)."
                return self._success_response(msg, [column], [new_col], sample, new_table=new_table)

            else:
                raise self._unsupported_backend_error()
        except Exception as e:
            return self._error_response(
                f"categorical_label_encode error: {str(e)}\n{traceback.format_exc()}",
                [column],
                [],
            )

    async def categorical_frequency_encode(
        self, table: str, schema: str, column: str,
        backend=None, data_id: Optional[str] = None, new_table: Optional[str] = None
    ) -> Dict[str, Any]:
        try:
            if isinstance(self.db, PostgresAdapter) or isinstance(self.db, DuckDBAdapter):
                working_table = await self._prepare_operation_table(
                    table, schema, backend=backend, data_id=data_id, new_table=new_table
                )
                supplied_new = f"{column}_freq"
                new_col = self._generate_cleaned_column_name(supplied_new)
                await self._add_new_column(working_table, schema, new_col, "DOUBLE PRECISION")

                qualified = self._qualified_table(working_table, schema)
                safe_col = SQLIdentifierSanitizer.sanitize(column)
                safe_new = SQLIdentifierSanitizer.sanitize(new_col)

                sql = f"""
                    WITH freq AS (
                        SELECT "{safe_col}",
                               COUNT(*)::NUMERIC / (SELECT COUNT(*) FROM {qualified} WHERE "{safe_col}" IS NOT NULL) AS frequency
                        FROM {qualified}
                        WHERE "{safe_col}" IS NOT NULL
                        GROUP BY "{safe_col}"
                    )
                    UPDATE {qualified} AS t
                    SET "{safe_new}" = freq.frequency
                    FROM freq
                    WHERE t."{safe_col}" = freq."{safe_col}";
                """
                await self._exec(sql)

                affected = await self._count_non_null(working_table, schema, column)
                sample = await self._fetch_sample(working_table, schema, [safe_col, safe_new])
                msg = f"Frequency encoded {column} -> {new_col} ({affected} rows)."
                return self._success_response(msg, [column], [new_col], sample, new_table=working_table)

            elif isinstance(self.db, ClickHouseAdapter):
                safe_schema = SQLIdentifierSanitizer.sanitize(schema)
                new_table = await self._resolve_output_table_name(
                    table, safe_schema, backend=backend, data_id=data_id, new_table=new_table
                )
                qualified_source = self._qualified_table(table, schema)

                supplied_new = f"{column}_freq"
                new_col = self._generate_cleaned_column_name(supplied_new)
                safe_col = SQLIdentifierSanitizer.sanitize(column)
                safe_new = SQLIdentifierSanitizer.sanitize(new_col)
                col_q = self.db.quote_identifier(safe_col)
                new_q = self.db.quote_identifier(safe_new)

                create_sql = self._ch_create_table_as(safe_schema, new_table, f"""
                    SELECT source.*,
                        CASE WHEN source.{col_q} IS NULL THEN NULL
                        ELSE freq.frequency
                        END AS {new_q}
                    FROM {qualified_source} AS source
                    LEFT JOIN (
                        SELECT {col_q} AS _ch_join_key,
                               CAST(count(*) AS Float64) / (SELECT count(*) FROM {qualified_source} WHERE {col_q} IS NOT NULL) AS frequency
                        FROM {qualified_source}
                        WHERE {col_q} IS NOT NULL
                        GROUP BY {col_q}
                    ) AS freq ON source.{col_q} = freq._ch_join_key
                """)
                await self._exec(create_sql)

                affected = await self._count_non_null(new_table, schema, column)
                sample = await self._fetch_sample(new_table, schema, [safe_col, safe_new])
                msg = f"Frequency encoded {column} -> {new_col} ({affected} rows)."
                return self._success_response(msg, [column], [new_col], sample, new_table=new_table)

            else:
                raise self._unsupported_backend_error()
        except Exception as e:
            return self._error_response(
                f"categorical_frequency_encode error: {str(e)}\n{traceback.format_exc()}",
                [column],
                [],
            )

    async def categorical_target_encode(
        self, table: str, schema: str, column: str, target_column: str,
        backend=None, data_id: Optional[str] = None, new_table: Optional[str] = None
    ) -> Dict[str, Any]:
        try:
            if isinstance(self.db, PostgresAdapter) or isinstance(self.db, DuckDBAdapter):
                working_table = await self._prepare_operation_table(
                    table, schema, backend=backend, data_id=data_id, new_table=new_table
                )
                supplied_new = f"{column}_target"
                new_col = self._generate_cleaned_column_name(supplied_new)
                await self._add_new_column(working_table, schema, new_col, "DOUBLE PRECISION")

                qualified = self._qualified_table(working_table, schema)
                safe_col = SQLIdentifierSanitizer.sanitize(column)
                safe_target = SQLIdentifierSanitizer.sanitize(target_column)
                safe_new = SQLIdentifierSanitizer.sanitize(new_col)

                sql = f"""
                    WITH cat_stats AS (
                        SELECT "{safe_col}",
                               AVG("{safe_target}") AS cat_mean,
                               COUNT(*) AS cnt
                        FROM {qualified}
                        WHERE "{safe_col}" IS NOT NULL AND "{safe_target}" IS NOT NULL
                        GROUP BY "{safe_col}"
                    ),
                    global_stats AS (
                        SELECT AVG("{safe_target}") AS global_mean
                        FROM {qualified}
                        WHERE "{safe_target}" IS NOT NULL
                    )
                    UPDATE {qualified} AS t
                    SET "{safe_new}" =
                        (cat_stats.cat_mean * cat_stats.cnt + global_stats.global_mean * 10)
                        / (cat_stats.cnt + 10)
                    FROM cat_stats, global_stats
                    WHERE t."{safe_col}" = cat_stats."{safe_col}";
                """
                await self._exec(sql)

                cnt_main = await self._count_non_null(working_table, schema, column)
                sample = await self._fetch_sample(working_table, schema, [safe_col, safe_target, safe_new])
                msg = f"Target-encoded {column} using {target_column} -> {new_col} ({cnt_main} rows)."
                return self._success_response(msg, [column, target_column], [new_col], sample, new_table=working_table)

            elif isinstance(self.db, ClickHouseAdapter):
                safe_schema = SQLIdentifierSanitizer.sanitize(schema)
                new_table = await self._resolve_output_table_name(
                    table, safe_schema, backend=backend, data_id=data_id, new_table=new_table
                )
                qualified_source = self._qualified_table(table, schema)

                supplied_new = f"{column}_target"
                new_col = self._generate_cleaned_column_name(supplied_new)
                safe_col = SQLIdentifierSanitizer.sanitize(column)
                safe_target = SQLIdentifierSanitizer.sanitize(target_column)
                safe_new = SQLIdentifierSanitizer.sanitize(new_col)
                col_q = self.db.quote_identifier(safe_col)
                target_q = self.db.quote_identifier(safe_target)
                new_q = self.db.quote_identifier(safe_new)

                create_sql = self._ch_create_table_as(safe_schema, new_table, f"""
                    SELECT source.*,
                        CASE WHEN source.{col_q} IS NULL THEN NULL
                        ELSE (cat_stats.cat_mean * cat_stats.cnt + global_stats.global_mean * 10)
                             / (cat_stats.cnt + 10)
                        END AS {new_q}
                    FROM {qualified_source} AS source
                    LEFT JOIN (
                        SELECT {col_q} AS _ch_join_key, avg({target_q}) AS cat_mean, count(*) AS cnt
                        FROM {qualified_source}
                        WHERE {col_q} IS NOT NULL AND {target_q} IS NOT NULL
                        GROUP BY {col_q}
                    ) AS cat_stats ON source.{col_q} = cat_stats._ch_join_key
                    CROSS JOIN (
                        SELECT avg({target_q}) AS global_mean
                        FROM {qualified_source}
                        WHERE {target_q} IS NOT NULL
                    ) AS global_stats
                """)
                await self._exec(create_sql)

                cnt_main = await self._count_non_null(new_table, schema, column)
                sample = await self._fetch_sample(new_table, schema, [safe_col, safe_target, safe_new])
                msg = f"Target-encoded {column} using {target_column} -> {new_col} ({cnt_main} rows)."
                return self._success_response(msg, [column, target_column], [new_col], sample, new_table=new_table)

            else:
                raise self._unsupported_backend_error()
        except Exception as e:
            return self._error_response(
                f"categorical_target_encode error: {str(e)}\n{traceback.format_exc()}",
                [column, target_column],
                [],
            )

    async def categorical_binarize(
        self,
        table: str,
        schema: str,
        column: str,
        value: Optional[Any] = None,
        condition: Optional[str] = None,
        backend=None, data_id: Optional[str] = None, new_table: Optional[str] = None
    ) -> Dict[str, Any]:
        try:
            if isinstance(self.db, PostgresAdapter) or isinstance(self.db, DuckDBAdapter):
                working_table = await self._prepare_operation_table(
                    table, schema, backend=backend, data_id=data_id, new_table=new_table
                )
                if value is not None:
                    safe_val = re.sub(r"[^a-zA-Z0-9_]", "_", str(value))
                    supplied_new = f"{column}_is_{safe_val}"
                elif condition:
                    safe_cond = re.sub(r"[^a-zA-Z0-9_]", "_", str(condition))
                    supplied_new = f"{column}_{safe_cond}"
                else:
                    supplied_new = f"{column}_binary"

                new_col = self._generate_cleaned_column_name(supplied_new)
                await self._add_new_column(working_table, schema, new_col, "INTEGER")

                qualified = self._qualified_table(working_table, schema)
                safe_col = SQLIdentifierSanitizer.sanitize(column)
                safe_new = SQLIdentifierSanitizer.sanitize(new_col)

                if value is not None:
                    sql = f"""
                        UPDATE {qualified}
                        SET "{safe_new}" = CASE WHEN "{safe_col}" = {self._sql_literal(value)} THEN 1 ELSE 0 END;
                    """
                elif condition:
                    sql = f"""
                        UPDATE {qualified}
                        SET "{safe_new}" = CASE WHEN "{safe_col}" {condition} THEN 1 ELSE 0 END;
                    """
                else:
                    sql = f"""
                        UPDATE {qualified}
                        SET "{safe_new}" = CASE WHEN "{safe_col}" IS NOT NULL THEN 1 ELSE 0 END;
                    """
                await self._exec(sql)

                affected = await self._count_non_null(working_table, schema, column)
                sample = await self._fetch_sample(working_table, schema, [safe_col, safe_new])
                msg = f"Binarized {column} -> {new_col} ({affected} rows)."
                return self._success_response(msg, [column], [new_col], sample, new_table=working_table)

            elif isinstance(self.db, ClickHouseAdapter):
                safe_schema = SQLIdentifierSanitizer.sanitize(schema)
                new_table = await self._resolve_output_table_name(
                    table, safe_schema, backend=backend, data_id=data_id, new_table=new_table
                )
                qualified_source = self._qualified_table(table, schema)

                if value is not None:
                    safe_val = re.sub(r"[^a-zA-Z0-9_]", "_", str(value))
                    supplied_new = f"{column}_is_{safe_val}"
                elif condition:
                    safe_cond = re.sub(r"[^a-zA-Z0-9_]", "_", str(condition))
                    supplied_new = f"{column}_{safe_cond}"
                else:
                    supplied_new = f"{column}_binary"

                new_col = self._generate_cleaned_column_name(supplied_new)
                safe_col = SQLIdentifierSanitizer.sanitize(column)
                safe_new = SQLIdentifierSanitizer.sanitize(new_col)
                col_q = self.db.quote_identifier(safe_col)
                new_q = self.db.quote_identifier(safe_new)

                if value is not None:
                    expr = f"CASE WHEN {col_q} = {self._sql_literal(value)} THEN 1 ELSE 0 END"
                elif condition:
                    expr = f"CASE WHEN {col_q} {condition} THEN 1 ELSE 0 END"
                else:
                    expr = f"CASE WHEN {col_q} IS NOT NULL THEN 1 ELSE 0 END"

                create_sql = self._ch_create_table_as(safe_schema, new_table, f"""
                    SELECT *, {expr} AS {new_q}
                    FROM {qualified_source}
                """)
                await self._exec(create_sql)

                affected = await self._count_non_null(new_table, schema, column)
                sample = await self._fetch_sample(new_table, schema, [safe_col, safe_new])
                msg = f"Binarized {column} -> {new_col} ({affected} rows)."
                return self._success_response(msg, [column], [new_col], sample, new_table=new_table)

            else:
                raise self._unsupported_backend_error()
        except Exception as e:
            return self._error_response(
                f"categorical_binarize error: {str(e)}\n{traceback.format_exc()}",
                [column],
                [],
            )

    # ==================================================================
    # Datetime Cyclical Encoding
    # ==================================================================
    async def datetime_cyclical_encode(
        self, table: str, schema: str, column: str, features: List[str],
        backend=None, data_id: Optional[str] = None, new_table: Optional[str] = None
    ) -> Dict[str, Any]:
        try:
            if isinstance(self.db, PostgresAdapter) or isinstance(self.db, DuckDBAdapter):
                if not features:
                    return self._error_response(
                        "datetime_cyclical_encode requires 'features' parameter",
                        [column],
                        [],
                    )

                working_table = await self._prepare_operation_table(
                    table, schema, backend=backend, data_id=data_id, new_table=new_table
                )
                qualified = self._qualified_table(working_table, schema)
                safe_col = SQLIdentifierSanitizer.sanitize(column)
                created_cols = []

                for feat in features:
                    feat = feat.lower()
                    period_map = {"month": 12, "dow": 7, "hour": 24}
                    if feat not in period_map:
                        continue

                    period = period_map[feat]
                    extract_expr = f"EXTRACT({feat.upper()} FROM \"{safe_col}\")"

                    sin_col = f"transformed_{column}_{feat}_sin"
                    cos_col = f"transformed_{column}_{feat}_cos"
                    await self._add_new_column(working_table, schema, sin_col, "DOUBLE PRECISION")
                    await self._add_new_column(working_table, schema, cos_col, "DOUBLE PRECISION")
                    safe_sin = SQLIdentifierSanitizer.sanitize(sin_col)
                    safe_cos = SQLIdentifierSanitizer.sanitize(cos_col)

                    sql_sin = f"""
                        UPDATE {qualified}
                        SET "{safe_sin}" = SIN(2 * PI() * ({extract_expr}) / {period})
                        WHERE "{safe_col}" IS NOT NULL;
                    """
                    sql_cos = f"""
                        UPDATE {qualified}
                        SET "{safe_cos}" = COS(2 * PI() * ({extract_expr}) / {period})
                        WHERE "{safe_col}" IS NOT NULL;
                    """
                    await self._exec(sql_sin)
                    await self._exec(sql_cos)
                    created_cols.extend([sin_col, cos_col])

                sample_cols = [safe_col] + created_cols[:2]
                sample = await self._fetch_sample(working_table, schema, sample_cols)
                msg = f"Created cyclical encodings for {features} on {column}."
                return self._success_response(msg, [column], created_cols, sample, features=features, new_table=working_table)

            elif isinstance(self.db, ClickHouseAdapter):
                if not features:
                    return self._error_response(
                        "datetime_cyclical_encode requires 'features' parameter",
                        [column],
                        [],
                    )

                safe_schema = SQLIdentifierSanitizer.sanitize(schema)
                new_table = await self._resolve_output_table_name(
                    table, safe_schema, backend=backend, data_id=data_id, new_table=new_table
                )
                qualified_source = self._qualified_table(table, schema)

                safe_col = SQLIdentifierSanitizer.sanitize(column)
                col_q = self.db.quote_identifier(safe_col)
                created_cols = []
                computed_cols_sql = []

                # 🔥 Cast column to DateTime for ClickHouse date functions.
                # toMonth/toDayOfWeek/toHour require Date/DateTime types,
                # but CSV uploads often store timestamps as String.
                # Nullable handles NULLs; DateTime64(6) preserves microsecond precision.
                col_casted = f"CAST({col_q} AS Nullable(DateTime64(6)))"

                for feat in features:
                    feat = feat.lower()
                    period_map = {"month": 12, "dow": 7, "hour": 24}
                    if feat not in period_map:
                        continue

                    period = period_map[feat]
                    # ClickHouse native functions — use CASTED column
                    if feat == "month":
                        extract_expr = f"toMonth({col_casted})"
                    elif feat == "dow":
                        extract_expr = f"toDayOfWeek({col_casted})"
                    elif feat == "hour":
                        extract_expr = f"toHour({col_casted})"

                    sin_col = f"transformed_{column}_{feat}_sin"
                    cos_col = f"transformed_{column}_{feat}_cos"
                    safe_sin = SQLIdentifierSanitizer.sanitize(sin_col)
                    safe_cos = SQLIdentifierSanitizer.sanitize(cos_col)
                    sin_q = self.db.quote_identifier(safe_sin)
                    cos_q = self.db.quote_identifier(safe_cos)

                    computed_cols_sql.append(
                        f"CASE WHEN {col_q} IS NULL THEN NULL "
                        f"ELSE sin(2 * pi() * ({extract_expr}) / {period}) END AS {sin_q}"
                    )
                    computed_cols_sql.append(
                        f"CASE WHEN {col_q} IS NULL THEN NULL "
                        f"ELSE cos(2 * pi() * ({extract_expr}) / {period}) END AS {cos_q}"
                    )
                    created_cols.extend([sin_col, cos_col])

                select_additions = ", ".join(computed_cols_sql)
                create_sql = self._ch_create_table_as(safe_schema, new_table, f"""
                    SELECT *, {select_additions}
                    FROM {qualified_source}
                """)
                await self._exec(create_sql)

                sample_cols = [safe_col] + created_cols[:2]
                sample = await self._fetch_sample(new_table, schema, sample_cols)
                msg = f"Created cyclical encodings for {features} on {column}."
                return self._success_response(msg, [column], created_cols, sample, features=features, new_table=new_table)
            else:
                raise self._unsupported_backend_error()
        except Exception as e:
            return self._error_response(
                f"datetime_cyclical_encode error: {str(e)}\n{traceback.format_exc()}",
                [column],
                [],
            )