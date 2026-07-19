"""
Core statistical operations executed directly on the database.
Mirrors pandas .describe(), .sum(), .mean(), .corr(), etc.
"""

from typing import Any, Dict, List, Optional
from datetime import datetime, timezone
import numpy as np
import traceback
import pandas as pd

from db_manager.adapters.base import DatabaseAdapter
from db_manager.adapters.duckdb import DuckDBAdapter
from db_manager.adapters.postgresql import PostgresAdapter
from utils.helper import SQLIdentifierSanitizer


class DataStatsOps:
    """
    Pure SQL‑based statistics. Receives a DatabaseAdapter and explicit
    (table, schema, column, …) parameters. Returns a unified response dict.
    """

    def __init__(self, db_adapter: DatabaseAdapter):
        self.db = db_adapter

    # ------------------------------------------------------------------
    # Internal helpers (identical to DataDatetimeOps)
    # ------------------------------------------------------------------
    async def _exec(self, sql: str, *args):
        return await self.db.execute(sql, *args)

    async def _fetch(self, sql: str, *args):
        return await self.db.fetch(sql, *args)

    async def _fetchval(self, sql: str, *args):
        return await self.db.fetchval(sql, *args)

    def _qualified_table(self, table: str, schema: str) -> str:
        safe_table = SQLIdentifierSanitizer.sanitize(table)
        safe_schema = SQLIdentifierSanitizer.sanitize(schema)
        return f'{self.db.quote_identifier(safe_schema)}.{self.db.quote_identifier(safe_table)}'

    
    async def _fetch_data(self, table: str, schema: str, columns: Any = "*",limit:int = -1) -> pd.DataFrame:
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

        if limit>0:
            rows = await self._fetch(f"SELECT {column_clause} FROM {qualified}  LIMIT {limit}" )
        else:
            rows = await self._fetch(f"SELECT {column_clause} FROM {qualified}" )
            
        records = [dict(row) for row in rows]
        return pd.DataFrame.from_records(records)

    
    
    def _success_response(
        self,
        message: str,
        involved_cols: Optional[List[str]] = None,
        generated_cols: Optional[List[str]] = None,
        result: Any = None,
        **extra,
    ) -> Dict[str, Any]:
        return {
            "is_error": False,
            "message": message,
            "error_message": None,
            "involved_cols": involved_cols or [],
            "generated_cols": generated_cols or [],
            "result": result,
            **extra,
        }

    def _error_response(
        self,
        error_message: str,
        involved_cols: List[str] = None,
        generated_cols: List[str] = None,
    ) -> Dict[str, Any]:
        return {
            "is_error": True,
            "message": "",
            "error_message": error_message,
            "involved_cols": involved_cols or [],
            "generated_cols": generated_cols or [],
        }

    def _unsupported_backend_error(self) -> NotImplementedError:
        return NotImplementedError(
            f"Unsupported database backend for stats operation: {self.db.__class__.__name__}"
        )

    # ------------------------------------------------------------------
    # Transient table helpers (mirroring PreprocessingOps pattern)
    # ------------------------------------------------------------------
    async def _generate_result_table_name(self, base_table: str, backend, data_id: str) -> str:
        max_op = await self.db.fetchval(
            f"SELECT COALESCE(MAX(opidx), 0) FROM {backend.transient_registry_table} "
            f"WHERE data_id = {backend.placeholder(1)}",
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
            candidate = await self._generate_result_table_name(safe_table, backend, data_id)
        else:
            candidate = f"{safe_table}__op_{datetime.now(timezone.utc).strftime('%Y%m%d%H%M%S%f')}"

        output_table = SQLIdentifierSanitizer.sanitize(candidate)
        dedupe_idx = 1
        while await self.db.table_exists(output_table, safe_schema):
            output_table = SQLIdentifierSanitizer.sanitize(f"{candidate}_{dedupe_idx}")
            dedupe_idx += 1

        return output_table

    # =========================================================================
    # NUMERIC STATISTICS (scalars – unchanged, keep as-is)
    # =========================================================================
    async def numeric_count(self, table: str, schema: str, column: str) -> Dict[str, Any]:
        try:
            if isinstance(self.db, PostgresAdapter) or isinstance(self.db, DuckDBAdapter):
                q = self._qualified_table(table, schema)
                c = SQLIdentifierSanitizer.sanitize(column)
                count = await self._fetchval(f'SELECT COUNT("{c}") FROM {q} WHERE "{c}" IS NOT NULL')
                return self._success_response(message=f"Count of '{column}': {count}", result=count)
            else:
                raise self._unsupported_backend_error()
        except Exception as e:
            return self._error_response(str(e), [column])

    async def numeric_sum(self, table: str, schema: str, column: str) -> Dict[str, Any]:
        try:
            if isinstance(self.db, PostgresAdapter) or isinstance(self.db, DuckDBAdapter):
                q = self._qualified_table(table, schema)
                c = SQLIdentifierSanitizer.sanitize(column)
                sum_res = await self._fetchval(f'SELECT SUM("{c}") FROM {q} WHERE "{c}" IS NOT NULL')
                return self._success_response(message=f"Sum of '{column}': {sum_res}", result=sum_res)
            else:
                raise self._unsupported_backend_error()
        except Exception as e:
            return self._error_response(str(e), [column])

    async def numeric_min(self, table: str, schema: str, column: str) -> Dict[str, Any]:
        try:
            if isinstance(self.db, PostgresAdapter) or isinstance(self.db, DuckDBAdapter):
                q = self._qualified_table(table, schema)
                c = SQLIdentifierSanitizer.sanitize(column)
                min_val = await self._fetchval(f'SELECT MIN("{c}") FROM {q} WHERE "{c}" IS NOT NULL')
                return self._success_response(f"Minimum value in '{column}': {min_val}", result=min_val)
            else:
                raise self._unsupported_backend_error()
        except Exception as e:
            return self._error_response(str(e), [column])

    async def numeric_max(self, table: str, schema: str, column: str) -> Dict[str, Any]:
        try:
            if isinstance(self.db, PostgresAdapter) or isinstance(self.db, DuckDBAdapter):
                q = self._qualified_table(table, schema)
                c = SQLIdentifierSanitizer.sanitize(column)
                max_val = await self._fetchval(f'SELECT MAX("{c}") FROM {q} WHERE "{c}" IS NOT NULL')
                return self._success_response(f"Maximum value in '{column}': {max_val}", result=max_val)
            else:
                raise self._unsupported_backend_error()
        except Exception as e:
            return self._error_response(str(e), [column])

    async def numeric_mean(self, table: str, schema: str, column: str) -> Dict[str, Any]:
        try:
            if isinstance(self.db, PostgresAdapter) or isinstance(self.db, DuckDBAdapter):
                q = self._qualified_table(table, schema)
                c = SQLIdentifierSanitizer.sanitize(column)
                mean_val = await self._fetchval(
                    f'SELECT AVG("{c}") FROM {q} WHERE "{c}" IS NOT NULL'
                )
                msg = f"Mean of '{column}': {mean_val:.4f}" if mean_val is not None else f"Mean of '{column}': N/A"
                return self._success_response(message=msg, result=mean_val)
            else:
                raise self._unsupported_backend_error()
        except Exception as e:
            return self._error_response(str(e), [column])

    async def numeric_median(self, table: str, schema: str, column: str) -> Dict[str, Any]:
        try:
            if isinstance(self.db, PostgresAdapter) or isinstance(self.db, DuckDBAdapter):
                q = self._qualified_table(table, schema)
                c = SQLIdentifierSanitizer.sanitize(column)
                if isinstance(self.db, PostgresAdapter):
                    median_sql = f'PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY "{c}")'
                elif isinstance(self.db, DuckDBAdapter):
                    median_sql = f'MEDIAN("{c}")'
                else:
                    raise self._unsupported_backend_error()
                median_val = await self._fetchval(
                    f'SELECT {median_sql} FROM {q} WHERE "{c}" IS NOT NULL'
                )
                msg = f"Median of '{column}': {median_val}"
                return self._success_response(msg, result=median_val)
            else:
                raise self._unsupported_backend_error()
        except Exception as e:
            return self._error_response(str(e), [column])

    async def numeric_mode(self, table: str, schema: str, column: str, top_n: int = 1) -> Dict[str, Any]:
        try:
            if isinstance(self.db, PostgresAdapter) or isinstance(self.db, DuckDBAdapter):
                q = self._qualified_table(table, schema)
                c = SQLIdentifierSanitizer.sanitize(column)
                rows = await self._fetch(
                    f'SELECT "{c}" AS mode, COUNT(*) AS freq FROM {q} WHERE "{c}" IS NOT NULL '
                    f'GROUP BY "{c}" ORDER BY freq DESC LIMIT {top_n}'
                )
                if not rows:
                    return self._success_response(f"No mode found for '{column}'", [column])
                if top_n == 1:
                    val = rows[0]["mode"]
                    freq = rows[0]["freq"]
                    return self._success_response(
                        f"Mode of '{column}': {val} (frequency: {freq})",
                        [column],
                        result=val,
                    )
                desc = ", ".join(f"{r['mode']}({r['freq']})" for r in rows)
                return self._success_response(
                    f"Top {top_n} modes: {desc}",
                    [column],
                    result=[r["mode"] for r in rows],
                )
            else:
                raise self._unsupported_backend_error()
        except Exception as e:
            return self._error_response(str(e), [column])

    async def numeric_prod(self, table: str, schema: str, column: str) -> Dict[str, Any]:
        try:
            if isinstance(self.db, PostgresAdapter) or isinstance(self.db, DuckDBAdapter):
                q = self._qualified_table(table, schema)
                c = SQLIdentifierSanitizer.sanitize(column)
                prod_val = await self._fetchval(f'SELECT EXP(SUM(LN("{c}"))) FROM {q} WHERE "{c}" > 0')
                return self._success_response(f"Product of positive values in '{column}': {prod_val}", result=prod_val)
            else:
                raise self._unsupported_backend_error()
        except Exception as e:
            return self._error_response(str(e), [column])

    async def numeric_unique(self, table: str, schema: str, column: str) -> Dict[str, Any]:
        try:
            if isinstance(self.db, PostgresAdapter) or isinstance(self.db, DuckDBAdapter):
                q = self._qualified_table(table, schema)
                c = SQLIdentifierSanitizer.sanitize(column)
                rows = await self._fetch(f'SELECT DISTINCT "{c}" FROM {q} WHERE "{c}" IS NOT NULL ORDER BY "{c}"')
                vals = [row[c] for row in rows]
                msg = f"Unique values in '{column}': {len(vals)} distinct"
                if len(vals) <= 20:
                    msg += f" – {vals}"
                return self._success_response(message=msg, result=vals)
            else:
                raise self._unsupported_backend_error()
        except Exception as e:
            return self._error_response(str(e), [column])

    async def numeric_nunique(self, table: str, schema: str, column: str) -> Dict[str, Any]:
        try:
            if isinstance(self.db, PostgresAdapter) or isinstance(self.db, DuckDBAdapter):
                q = self._qualified_table(table, schema)
                c = SQLIdentifierSanitizer.sanitize(column)
                val = await self._fetchval(f'SELECT COUNT(DISTINCT "{c}") FROM {q} WHERE "{c}" IS NOT NULL')
                return self._success_response(f"Number of unique values in '{column}': {val}", result=val)
            else:
                raise self._unsupported_backend_error()
        except Exception as e:
            return self._error_response(str(e), [column])

    async def numeric_value_counts(self, table: str, schema: str, column: str, top_n: int = 10) -> Dict[str, Any]:
        try:
            if isinstance(self.db, PostgresAdapter) or isinstance(self.db, DuckDBAdapter):
                q = self._qualified_table(table, schema)
                c = SQLIdentifierSanitizer.sanitize(column)
                rows = await self._fetch(
                    f'SELECT "{c}" AS val, COUNT(*) AS cnt FROM {q} WHERE "{c}" IS NOT NULL '
                    f'GROUP BY "{c}" ORDER BY cnt DESC LIMIT {top_n}'
                )
                val_counts = {r["val"]: r["cnt"] for r in rows}
                return self._success_response(f"Top {top_n} value counts", result=val_counts)
            else:
                raise self._unsupported_backend_error()
        except Exception as e:
            return self._error_response(str(e), [column])

    async def numeric_std(self, table: str, schema: str, column: str) -> Dict[str, Any]:
        try:
            if isinstance(self.db, PostgresAdapter) or isinstance(self.db, DuckDBAdapter):
                q = self._qualified_table(table, schema)
                c = SQLIdentifierSanitizer.sanitize(column)
                std_val = await self._fetchval(f'SELECT STDDEV_POP("{c}") FROM {q} WHERE "{c}" IS NOT NULL')
                msg = f"Standard deviation of '{column}': {std_val:.4f}" if std_val is not None else f"Std of '{column}': N/A"
                return self._success_response(message=msg, result=std_val)
            else:
                raise self._unsupported_backend_error()
        except Exception as e:
            return self._error_response(str(e), [column])

    async def numeric_var(self, table: str, schema: str, column: str) -> Dict[str, Any]:
        try:
            if isinstance(self.db, PostgresAdapter) or isinstance(self.db, DuckDBAdapter):
                q = self._qualified_table(table, schema)
                c = SQLIdentifierSanitizer.sanitize(column)
                var_val = await self._fetchval(f'SELECT VAR_POP("{c}") FROM {q} WHERE "{c}" IS NOT NULL')
                msg = f"Variance of '{column}': {var_val:.4f}" if var_val is not None else f"Variance of '{column}': N/A"
                return self._success_response(msg, result=var_val)
            else:
                raise self._unsupported_backend_error()
        except Exception as e:
            return self._error_response(str(e), [column])

    async def numeric_sem(self, table: str, schema: str, column: str) -> Dict[str, Any]:
        try:
            if isinstance(self.db, PostgresAdapter) or isinstance(self.db, DuckDBAdapter):
                q = self._qualified_table(table, schema)
                c = SQLIdentifierSanitizer.sanitize(column)
                val = await self._fetchval(f'SELECT STDDEV_SAMP("{c}") / SQRT(COUNT("{c}")) FROM {q} WHERE "{c}" IS NOT NULL')
                msg = f"Standard error of mean for '{column}': {val:.6f}" if val is not None else f"SEM of '{column}': N/A"
                return self._success_response(msg, result=val)
            else:
                raise self._unsupported_backend_error()
        except Exception as e:
            return self._error_response(str(e), [column])

    async def numeric_mad(self, table: str, schema: str, column: str) -> Dict[str, Any]:
        try:
            if isinstance(self.db, PostgresAdapter) or isinstance(self.db, DuckDBAdapter):
                q = self._qualified_table(table, schema)
                c = SQLIdentifierSanitizer.sanitize(column)
                val = await self._fetchval(
                    f'SELECT AVG(ABS("{c}" - sub.avg_val)) FROM {q}, '
                    f'(SELECT AVG("{c}") AS avg_val FROM {q} WHERE "{c}" IS NOT NULL) AS sub '
                    f'WHERE "{c}" IS NOT NULL'
                )
                msg = f"Mean Absolute Deviation of '{column}': {val:.4f}" if val is not None else f"MAD of '{column}': N/A"
                return self._success_response(message=msg, result=val)
            else:
                raise self._unsupported_backend_error()
        except Exception as e:
            return self._error_response(str(e), [column])

    async def numeric_iqr(self, table: str, schema: str, column: str) -> Dict[str, Any]:
        try:
            if isinstance(self.db, PostgresAdapter) or isinstance(self.db, DuckDBAdapter):
                q = self._qualified_table(table, schema)
                c = SQLIdentifierSanitizer.sanitize(column)
                if isinstance(self.db, PostgresAdapter):
                    q3_expr = f'PERCENTILE_CONT(0.75) WITHIN GROUP (ORDER BY "{c}")'
                    q1_expr = f'PERCENTILE_CONT(0.25) WITHIN GROUP (ORDER BY "{c}")'
                elif isinstance(self.db, DuckDBAdapter):
                    q3_expr = f'QUANTILE_CONT("{c}", 0.75)'
                    q1_expr = f'QUANTILE_CONT("{c}", 0.25)'
                else:
                    raise self._unsupported_backend_error()
                val = await self._fetchval(
                    f'SELECT {q3_expr} - {q1_expr} FROM {q} WHERE "{c}" IS NOT NULL'
                )
                msg = f"IQR of '{column}': {val:.4f}" if val is not None else f"IQR of '{column}': N/A"
                return self._success_response(msg, result=val)
            else:
                raise self._unsupported_backend_error()
        except Exception as e:
            return self._error_response(str(e), [column])

    async def numeric_range(self, table: str, schema: str, column: str) -> Dict[str, Any]:
        try:
            if isinstance(self.db, PostgresAdapter) or isinstance(self.db, DuckDBAdapter):
                q = self._qualified_table(table, schema)
                c = SQLIdentifierSanitizer.sanitize(column)
                val = await self._fetchval(f'SELECT MAX("{c}") - MIN("{c}") FROM {q} WHERE "{c}" IS NOT NULL')
                msg = f"Range of '{column}': {val:.4f}" if val is not None else f"Range of '{column}': N/A"
                return self._success_response(msg, result=val)
            else:
                raise self._unsupported_backend_error()
        except Exception as e:
            return self._error_response(str(e), [column])

    async def numeric_skew(self, table: str, schema: str, column: str) -> Dict[str, Any]:
        try:
            if isinstance(self.db, PostgresAdapter) or isinstance(self.db, DuckDBAdapter):
                q = self._qualified_table(table, schema)
                c = SQLIdentifierSanitizer.sanitize(column)
                sql = f'SELECT SKEWNESS("{c}") FROM {q} WHERE "{c}" IS NOT NULL'
                val = await self._fetchval(sql)
                msg = f"Skewness of '{column}': {val:.4f}" if val is not None else "N/A"
                return self._success_response(msg, result=val)
            else:
                raise self._unsupported_backend_error()
        except Exception as e:
            return self._error_response(str(e), [column])

    async def numeric_kurtosis(self, table: str, schema: str, column: str) -> Dict[str, Any]:
        try:
            if isinstance(self.db, PostgresAdapter) or isinstance(self.db, DuckDBAdapter):
                q = self._qualified_table(table, schema)
                c = SQLIdentifierSanitizer.sanitize(column)
                sql = f'SELECT KURTOSIS("{c}") FROM {q} WHERE "{c}" IS NOT NULL'
                val = await self._fetchval(sql)
                msg = f"Kurtosis of '{column}': {val:.4f}" if val is not None else "N/A"
                return self._success_response(msg, result=val)
            else:
                raise self._unsupported_backend_error()
        except Exception as e:
            return self._error_response(str(e), [column])

    async def numeric_entropy(self, table: str, schema: str, column: str) -> Dict[str, Any]:
        try:
            if isinstance(self.db, PostgresAdapter) or isinstance(self.db, DuckDBAdapter):
                q = self._qualified_table(table, schema)
                c = SQLIdentifierSanitizer.sanitize(column)
                sql = f'''
                    WITH counts AS (SELECT "{c}", COUNT(*) AS cnt FROM {q} WHERE "{c}" IS NOT NULL GROUP BY "{c}"),
                         total AS (SELECT SUM(cnt) AS tot FROM counts)
                    SELECT -SUM((cnt::float / tot) * LN(cnt::float / tot)) FROM counts, total
                '''
                val = await self._fetchval(sql)
                msg = f"Entropy of '{column}': {val:.4f}" if val is not None else "N/A"
                return self._success_response(msg, result=val)
            else:
                raise self._unsupported_backend_error()
        except Exception as e:
            return self._error_response(str(e), [column])

    async def numeric_quantile(self, table: str, schema: str, column: str, quantiles: List[float] = None) -> Dict[str, Any]:
        try:
            if isinstance(self.db, PostgresAdapter) or isinstance(self.db, DuckDBAdapter):
                if quantiles is None:
                    quantiles = [0.25, 0.5, 0.75]
                q = self._qualified_table(table, schema)
                c = SQLIdentifierSanitizer.sanitize(column)
                if isinstance(self.db, PostgresAdapter):
                    parts = ', '.join(
                        f'PERCENTILE_CONT({qt}) WITHIN GROUP (ORDER BY "{c}") AS p_{int(qt*100)}'
                        for qt in quantiles
                    )
                elif isinstance(self.db, DuckDBAdapter):
                    parts = ', '.join(
                        f'QUANTILE_CONT("{c}", {qt}) AS p_{int(qt*100)}'
                        for qt in quantiles
                    )
                else:
                    raise self._unsupported_backend_error()
                row = await self._fetch(
                    f'SELECT {parts} FROM {q} WHERE "{c}" IS NOT NULL'
                )
                if row and row[0]:
                    vals = dict(row[0])
                    desc = ", ".join(f"{k}: {v:.4f}" for k, v in vals.items())
                    return self._success_response(f"Quantiles of '{column}': {desc}", result=vals)
                return self._success_response(f"No quantile results for '{column}'", [column])
            else:
                raise self._unsupported_backend_error()
        except Exception as e:
            return self._error_response(str(e), [column])

    async def numeric_autocorr(self, table: str, schema: str, column: str, lag: int = 1) -> Dict[str, Any]:
        try:
            if isinstance(self.db, PostgresAdapter) or isinstance(self.db, DuckDBAdapter):
                q = self._qualified_table(table, schema)
                c = SQLIdentifierSanitizer.sanitize(column)
                sql = f'''
                    WITH lagged AS (
                        SELECT "{c}" AS cur, LAG("{c}", {lag}) OVER (ORDER BY (SELECT NULL)) AS prev
                        FROM {q} WHERE "{c}" IS NOT NULL
                    )
                    SELECT CORR(cur, prev) FROM lagged WHERE cur IS NOT NULL AND prev IS NOT NULL
                '''
                val = await self._fetchval(sql)
                msg = f"Autocorrelation (lag={lag}) for '{column}': {val:.4f}" if val is not None else "N/A"
                return self._success_response(msg, result=val)
            else:
                raise self._unsupported_backend_error()
        except Exception as e:
            return self._error_response(str(e), [column])

    async def numeric_coefficient_of_variation(self, table: str, schema: str, column: str) -> Dict[str, Any]:
        try:
            if isinstance(self.db, PostgresAdapter) or isinstance(self.db, DuckDBAdapter):
                q = self._qualified_table(table, schema)
                c = SQLIdentifierSanitizer.sanitize(column)
                val = await self._fetchval(f'SELECT STDDEV_POP("{c}") / AVG("{c}") FROM {q} WHERE "{c}" IS NOT NULL')
                msg = f"Coefficient of variation for '{column}': {val:.4f}" if val is not None else "N/A"
                return self._success_response(msg, result=val)
            else:
                raise self._unsupported_backend_error()
        except Exception as e:
            return self._error_response(str(e), [column])

    async def numeric_outliers_iqr(self, table: str, schema: str, column: str) -> Dict[str, Any]:
        try:
            if isinstance(self.db, PostgresAdapter) or isinstance(self.db, DuckDBAdapter):
                q = self._qualified_table(table, schema)
                c = SQLIdentifierSanitizer.sanitize(column)
                if isinstance(self.db, PostgresAdapter):
                    q1_expr = f'PERCENTILE_CONT(0.25) WITHIN GROUP (ORDER BY "{c}")'
                    q3_expr = f'PERCENTILE_CONT(0.75) WITHIN GROUP (ORDER BY "{c}")'
                elif isinstance(self.db, DuckDBAdapter):
                    q1_expr = f'QUANTILE_CONT("{c}", 0.25)'
                    q3_expr = f'QUANTILE_CONT("{c}", 0.75)'
                else:
                    raise self._unsupported_backend_error()
                sql = f'''
                    WITH stats AS (
                        SELECT {q1_expr} AS q1,
                               {q3_expr} AS q3
                        FROM {q} WHERE "{c}" IS NOT NULL
                    )
                    SELECT "{c}" AS outlier_value
                    FROM {q}, stats
                    WHERE "{c}" IS NOT NULL
                      AND ("{c}" < (stats.q1 - 1.5 * (stats.q3 - stats.q1))
                        OR "{c}" > (stats.q3 + 1.5 * (stats.q3 - stats.q1)))
                '''
                rows = await self._fetch(sql)
                vals = [r["outlier_value"] for r in rows]
                msg = f"Outliers (IQR) for '{column}': {len(vals)}"
                return self._success_response(msg, result=vals)
            else:
                raise self._unsupported_backend_error()
        except Exception as e:
            return self._error_response(str(e), [column])

    async def numeric_outliers_zscore(self, table: str, schema: str, column: str, threshold: float = 3.0) -> Dict[str, Any]:
        try:
            if isinstance(self.db, PostgresAdapter) or isinstance(self.db, DuckDBAdapter):
                q = self._qualified_table(table, schema)
                c = SQLIdentifierSanitizer.sanitize(column)
                sql = f'''
                    WITH stats AS (
                        SELECT AVG("{c}") AS mean, STDDEV_POP("{c}") AS std
                        FROM {q} WHERE "{c}" IS NOT NULL
                    )
                    SELECT "{c}" AS outlier_value
                    FROM {q}, stats
                    WHERE "{c}" IS NOT NULL AND ABS("{c}" - stats.mean) > {threshold} * stats.std
                '''
                rows = await self._fetch(sql)
                vals = [r["outlier_value"] for r in rows]
                msg = f"Outliers (z-score, threshold={threshold}) for '{column}': {len(vals)}"
                return self._success_response(msg, result=vals)
            else:
                raise self._unsupported_backend_error()
        except Exception as e:
            return self._error_response(str(e), [column])

    # =========================================================================
    # Multi‑column operations that **create a result table** (correlation / covariance)
    # =========================================================================
    async def numeric_multi_column_correlation(
        self,
        table: str,
        schema: str,
        columns: List[str],
        backend=None,
        data_id: Optional[str] = None,
        new_table: Optional[str] = None,
    ) -> Dict[str, Any]:
        """
        Compute pairwise correlation for every pair of columns and store the
        matrix (as a long‑format table) in a new transient table.
        """
        try:
            if isinstance(self.db, PostgresAdapter) or isinstance(self.db, DuckDBAdapter):
                q = self._qualified_table(table, schema)
                qcols = [SQLIdentifierSanitizer.sanitize(c) for c in columns]
                base_table = table

                # resolve output table name
                output_table = await self._resolve_output_table_name(
                    base_table, schema, backend=backend, data_id=data_id, new_table=new_table
                )

                # Build a UNION ALL of all pairs (i <= j) to create the result table
                union_parts = []
                for i, ci in enumerate(qcols):
                    for j in range(i, len(qcols)):
                        cj = qcols[j]
                        # literal column names for the reference columns
                        col1_str = columns[i]
                        col2_str = columns[j]
                        union_parts.append(
                            f"SELECT '{col1_str}' AS column1, '{col2_str}' AS column2, "
                            f"CORR(\"{ci}\", \"{cj}\") AS value "
                            f"FROM {q} WHERE \"{ci}\" IS NOT NULL AND \"{cj}\" IS NOT NULL"
                        )

                if not union_parts:
                    return self._error_response("No columns provided", columns)

                create_sql = (
                    f"CREATE TABLE {self._qualified_table(output_table, schema)} AS "
                    + " UNION ALL ".join(union_parts)
                )
                await self._exec(create_sql)

                # Fetch the long‑format result to build the in‑memory DataFrame
                rows = await self._fetch(f"SELECT * FROM {self._qualified_table(output_table, schema)}")

                # Build the matrix
                mat = {col: {} for col in columns}
                for row in rows:
                    c1 = row["column1"]
                    c2 = row["column2"]
                    val = row["value"]
                    mat[c1][c2] = val
                    if c1 != c2:
                        mat[c2][c1] = val

                df = pd.DataFrame(mat)
                msg = f"Correlation matrix computed for {len(columns)} columns, stored in '{output_table}'"
                return self._success_response(msg, columns, result=df, new_table=output_table)

            else:
                raise self._unsupported_backend_error()
        except Exception as e:
            return self._error_response(
                f"numeric_multi_column_correlation error: {str(e)}\n{traceback.format_exc()}",
                columns,
            )

    async def numeric_multi_column_covariance(
        self,
        table: str,
        schema: str,
        columns: List[str],
        backend=None,
        data_id: Optional[str] = None,
        new_table: Optional[str] = None,
    ) -> Dict[str, Any]:
        """
        Compute pairwise covariance for every pair of columns and store the
        matrix (as a long‑format table) in a new transient table.
        """
        try:
            if isinstance(self.db, PostgresAdapter) or isinstance(self.db, DuckDBAdapter):
                q = self._qualified_table(table, schema)
                qcols = [SQLIdentifierSanitizer.sanitize(c) for c in columns]
                base_table = table

                output_table = await self._resolve_output_table_name(
                    base_table, schema, backend=backend, data_id=data_id, new_table=new_table
                )

                union_parts = []
                for i, ci in enumerate(qcols):
                    for j in range(i, len(qcols)):
                        cj = qcols[j]
                        col1_str = columns[i]
                        col2_str = columns[j]
                        union_parts.append(
                            f"SELECT '{col1_str}' AS column1, '{col2_str}' AS column2, "
                            f"COVAR_SAMP(\"{ci}\", \"{cj}\") AS value "
                            f"FROM {q} WHERE \"{ci}\" IS NOT NULL AND \"{cj}\" IS NOT NULL"
                        )

                if not union_parts:
                    return self._error_response("No columns provided", columns)

                create_sql = (
                    f"CREATE TABLE {self._qualified_table(output_table, schema)} AS "
                    + " UNION ALL ".join(union_parts)
                )
                await self._exec(create_sql)

                rows = await self._fetch(f"SELECT * FROM {self._qualified_table(output_table, schema)}")

                mat = {col: {} for col in columns}
                for row in rows:
                    c1 = row["column1"]
                    c2 = row["column2"]
                    val = row["value"]
                    mat[c1][c2] = val
                    if c1 != c2:
                        mat[c2][c1] = val

                df = pd.DataFrame(mat)
                msg = f"Covariance matrix computed for {len(columns)} columns, stored in '{output_table}'"
                return self._success_response(msg, columns, result=df, new_table=output_table)

            else:
                raise self._unsupported_backend_error()
        except Exception as e:
            return self._error_response(
                f"numeric_multi_column_covariance error: {str(e)}\n{traceback.format_exc()}",
                columns,
            )

    # =========================================================================
    # CATEGORICAL STATISTICS (scalars – unchanged)
    # =========================================================================
    async def categorical_count(self, table: str, schema: str, column: str) -> Dict[str, Any]:
        if isinstance(self.db, PostgresAdapter) or isinstance(self.db, DuckDBAdapter):
            return await self.numeric_count(table, schema, column)

        else:
            raise self._unsupported_backend_error()
    async def categorical_unique(self, table: str, schema: str, column: str) -> Dict[str, Any]:
        if isinstance(self.db, PostgresAdapter) or isinstance(self.db, DuckDBAdapter):
            return await self.numeric_unique(table, schema, column)

        else:
            raise self._unsupported_backend_error()
    async def categorical_nunique(self, table: str, schema: str, column: str) -> Dict[str, Any]:
        if isinstance(self.db, PostgresAdapter) or isinstance(self.db, DuckDBAdapter):
            return await self.numeric_nunique(table, schema, column)

        else:
            raise self._unsupported_backend_error()
    async def categorical_value_counts(self, table: str, schema: str, column: str, top_n: int = 10) -> Dict[str, Any]:
        try:
            if isinstance(self.db, PostgresAdapter) or isinstance(self.db, DuckDBAdapter):
                q = self._qualified_table(table, schema)
                c = SQLIdentifierSanitizer.sanitize(column)
                rows = await self._fetch(
                    f'SELECT "{c}" AS value, COUNT(*) AS count FROM {q} '
                    f'WHERE "{c}" IS NOT NULL GROUP BY "{c}" ORDER BY count DESC LIMIT {top_n}'
                )
                value_counts = {r["value"]: r["count"] for r in rows}
                msg = f"Top {top_n} value counts for '{column}'"
                return self._success_response(msg, result=value_counts)
            else:
                raise self._unsupported_backend_error()
        except Exception as e:
            return self._error_response(str(e), [column])

    async def categorical_proportions(self, table: str, schema: str, column: str) -> Dict[str, Any]:
        try:
            if isinstance(self.db, PostgresAdapter) or isinstance(self.db, DuckDBAdapter):
                q = self._qualified_table(table, schema)
                c = SQLIdentifierSanitizer.sanitize(column)
                total = await self._fetchval(f'SELECT COUNT(*) FROM {q} WHERE "{c}" IS NOT NULL')
                if total == 0:
                    return self._success_response(f"No data in '{column}'", [column], result={})
                rows = await self._fetch(
                    f'SELECT "{c}" AS category, COUNT(*) AS cnt, '
                    f'(COUNT(*) * 1.0 / {total}) AS proportion '
                    f'FROM {q} WHERE "{c}" IS NOT NULL '
                    f'GROUP BY "{c}" ORDER BY cnt DESC'
                )
                result = {r["category"]: float(r["proportion"]) for r in rows}
                msg = f"Proportions for '{column}'"
                return self._success_response(msg, [column], result=result)
            else:
                raise self._unsupported_backend_error()
        except Exception as e:
            return self._error_response(str(e), [column])

    async def categorical_mode(self, table: str, schema: str, column: str, top_n: int = 1) -> Dict[str, Any]:
        if isinstance(self.db, PostgresAdapter) or isinstance(self.db, DuckDBAdapter):
            return await self.numeric_mode(table, schema, column, top_n)

        else:
            raise self._unsupported_backend_error()
    async def categorical_chi_square(self, table: str, schema: str, column1: str, column2: str) -> Dict[str, Any]:
        try:
            if isinstance(self.db, PostgresAdapter) or isinstance(self.db, DuckDBAdapter):
                q = self._qualified_table(table, schema)
                c1 = SQLIdentifierSanitizer.sanitize(column1)
                c2 = SQLIdentifierSanitizer.sanitize(column2)
                sql = f'''
                    WITH observed AS (
                        SELECT "{c1}", "{c2}", COUNT(*) as cnt
                        FROM {q} WHERE "{c1}" IS NOT NULL AND "{c2}" IS NOT NULL
                        GROUP BY "{c1}", "{c2}"
                    ),
                    row_totals AS (SELECT "{c1}", SUM(cnt) as row_total FROM observed GROUP BY "{c1}"),
                    col_totals AS (SELECT "{c2}", SUM(cnt) as col_total FROM observed GROUP BY "{c2}"),
                    grand_total AS (SELECT SUM(cnt) as total FROM observed),
                    expected AS (
                        SELECT o."{c1}", o."{c2}", o.cnt,
                               (rt.row_total * ct.col_total / gt.total) as expected
                        FROM observed o
                        JOIN row_totals rt ON o."{c1}" = rt."{c1}"
                        JOIN col_totals ct ON o."{c2}" = ct."{c2}"
                        CROSS JOIN grand_total gt
                    )
                    SELECT SUM(POWER(cnt - expected, 2) / expected) as chi2,
                           COUNT(DISTINCT "{c1}") as rows,
                           COUNT(DISTINCT "{c2}") as cols
                    FROM expected
                '''
                row = await self._fetch(sql)
                if row and row[0]:
                    vals = dict(row[0])
                    dof = (vals['rows'] - 1) * (vals['cols'] - 1)
                    result = {"chi2": float(vals["chi2"]), "df": int(dof)}
                    msg = f"Chi-square '{column1}' × '{column2}'"
                    return self._success_response(msg, [column1, column2], result=result)
                return self._success_response(f"No chi-square result for '{column1}' × '{column2}'", [column1, column2], result={"chi2": None, "df": 0})
            else:
                raise self._unsupported_backend_error()
        except Exception as e:
            return self._error_response(str(e), [column1, column2])

    async def categorical_cramers_v(self, table: str, schema: str, column1: str, column2: str) -> Dict[str, Any]:
        try:
            if isinstance(self.db, PostgresAdapter) or isinstance(self.db, DuckDBAdapter):
                q = self._qualified_table(table, schema)
                c1 = SQLIdentifierSanitizer.sanitize(column1)
                c2 = SQLIdentifierSanitizer.sanitize(column2)
                sql = f'''
                    WITH observed AS (
                        SELECT "{c1}", "{c2}", COUNT(*) as cnt
                        FROM {q} WHERE "{c1}" IS NOT NULL AND "{c2}" IS NOT NULL
                        GROUP BY "{c1}", "{c2}"
                    ),
                    row_totals AS (SELECT "{c1}", SUM(cnt) as row_total FROM observed GROUP BY "{c1}"),
                    col_totals AS (SELECT "{c2}", SUM(cnt) as col_total FROM observed GROUP BY "{c2}"),
                    grand_total AS (SELECT SUM(cnt) as total FROM observed),
                    expected AS (
                        SELECT o."{c1}", o."{c2}", o.cnt,
                               (rt.row_total * ct.col_total / gt.total) as expected
                        FROM observed o
                        JOIN row_totals rt ON o."{c1}" = rt."{c1}"
                        JOIN col_totals ct ON o."{c2}" = ct."{c2}"
                        CROSS JOIN grand_total gt
                    ),
                    chi AS (
                        SELECT SUM(POWER(cnt - expected, 2) / expected) as chi2,
                               COUNT(DISTINCT "{c1}") as rows,
                               COUNT(DISTINCT "{c2}") as cols,
                               MAX(gt.total) as n
                        FROM expected, grand_total gt
                    )
                    SELECT CASE WHEN rows = 1 OR cols = 1 THEN 0
                            ELSE SQRT(chi2 / (n * LEAST(rows-1, cols-1))) END as V
                    FROM chi
                '''
                val = await self._fetchval(sql)
                msg = f"Cramér's V for '{column1}' × '{column2}': {val:.4f}" if val is not None else "N/A"
                return self._success_response(msg, [column1, column2], result=val)
            else:
                raise self._unsupported_backend_error()
        except Exception as e:
            return self._error_response(str(e), [column1, column2])

    async def categorical_theil_u(self, table: str, schema: str, column1: str, column2: str) -> Dict[str, Any]:
        try:
            if isinstance(self.db, PostgresAdapter) or isinstance(self.db, DuckDBAdapter):
                q = self._qualified_table(table, schema)
                c1 = SQLIdentifierSanitizer.sanitize(column1)
                c2 = SQLIdentifierSanitizer.sanitize(column2)
                # Compute Theil's U in Python from a contingency table for backend portability.
                rows = await self._fetch(
                    f'SELECT "{c1}" AS x, "{c2}" AS y, COUNT(*) AS cnt '
                    f'FROM {q} WHERE "{c1}" IS NOT NULL AND "{c2}" IS NOT NULL '
                    f'GROUP BY "{c1}", "{c2}"'
                )
                if not rows:
                    return self._success_response(f"Theil's U '{column1}' → '{column2}': N/A", [column1, column2], result=None)
                df = pd.DataFrame([dict(r) for r in rows])
                total = float(df["cnt"].sum())
                pxy = df["cnt"] / total
                py = df.groupby("y")["cnt"].sum() / total
                h_y = float(-(py * py.apply(lambda p: 0.0 if p <= 0 else np.log(p))).sum())
                if h_y == 0:
                    val = None
                else:
                    px = df.groupby("x")["cnt"].sum()
                    h_y_given_x = 0.0
                    for x_val, x_cnt in px.items():
                        sub = df[df["x"] == x_val]
                        p_x = x_cnt / total
                        p_y_given_x = sub["cnt"] / x_cnt
                        h_cond = float(-(p_y_given_x * p_y_given_x.apply(lambda p: 0.0 if p <= 0 else np.log(p))).sum())
                        h_y_given_x += p_x * h_cond
                    val = (h_y - h_y_given_x) / h_y
                msg = f"Theil's U '{column1}' → '{column2}': {val:.4f}" if val is not None else "N/A"
                return self._success_response(msg, [column1, column2], result=val)
            else:
                raise self._unsupported_backend_error()
        except Exception as e:
            return self._error_response(str(e), [column1, column2])

    async def categorical_mutual_information(self, table: str, schema: str, column1: str, column2: str) -> Dict[str, Any]:
        try:
            if isinstance(self.db, PostgresAdapter) or isinstance(self.db, DuckDBAdapter):
                q = self._qualified_table(table, schema)
                c1 = SQLIdentifierSanitizer.sanitize(column1)
                c2 = SQLIdentifierSanitizer.sanitize(column2)
                sql = f'''
                    WITH joint AS (
                        SELECT "{c1}", "{c2}", COUNT(*) as cnt
                        FROM {q} WHERE "{c1}" IS NOT NULL AND "{c2}" IS NOT NULL
                        GROUP BY "{c1}", "{c2}"
                    ), total AS (SELECT SUM(cnt) as n FROM joint),
                       marg_x AS (SELECT "{c1}", SUM(cnt) as x_cnt FROM joint GROUP BY "{c1}"),
                       marg_y AS (SELECT "{c2}", SUM(cnt) as y_cnt FROM joint GROUP BY "{c2}")
                    SELECT SUM(
                        (cnt::float / n) *
                        LN((cnt::float / n) / ((x_cnt::float / n) * (y_cnt::float / n)))
                    ) as mi
                    FROM joint
                    JOIN marg_x ON joint."{c1}" = marg_x."{c1}"
                    JOIN marg_y ON joint."{c2}" = marg_y."{c2}"
                    CROSS JOIN total
                '''
                val = await self._fetchval(sql)
                msg = f"Mutual Information '{column1}' & '{column2}': {val:.4f}" if val is not None else "N/A"
                return self._success_response(msg, [column1, column2], result=val)
            else:
                raise self._unsupported_backend_error()
        except Exception as e:
            return self._error_response(str(e), [column1, column2])


    
    # =========================================================================
    # DATETIME STATISTICS
    # =========================================================================
    async def datetime_min(self, table: str, schema: str, column: str) -> Dict[str, Any]:
        try:
            if isinstance(self.db, PostgresAdapter) or isinstance(self.db, DuckDBAdapter):
                q = self._qualified_table(table, schema)
                c = SQLIdentifierSanitizer.sanitize(column)
                min_val = await self._fetchval(f'SELECT MIN("{c}") FROM {q} WHERE "{c}" IS NOT NULL')
                return self._success_response(f"Earliest datetime in '{column}': {min_val}", [column],result=min_val)
            else:
                raise self._unsupported_backend_error()
        except Exception as e:
            return self._error_response(str(e), [column])

    async def datetime_max(self, table: str, schema: str, column: str) -> Dict[str, Any]:
        try:
            if isinstance(self.db, PostgresAdapter) or isinstance(self.db, DuckDBAdapter):
                q = self._qualified_table(table, schema)
                c = SQLIdentifierSanitizer.sanitize(column)
                max_val = await self._fetchval(f'SELECT MAX("{c}") FROM {q} WHERE "{c}" IS NOT NULL')
                return self._success_response(f"Latest datetime in '{column}': {max_val}", [column],result=max_val)
            else:
                raise self._unsupported_backend_error()
        except Exception as e:
            return self._error_response(str(e), [column])

    async def datetime_mean(self, table: str, schema: str, column: str) -> Dict[str, Any]:
        try:
            if isinstance(self.db, PostgresAdapter) or isinstance(self.db, DuckDBAdapter):
                q = self._qualified_table(table, schema)
                c = SQLIdentifierSanitizer.sanitize(column)

                col_types = await self.db.get_column_types(table, schema)
                type_lookup = {str(k).lower(): str(v).lower() for k, v in col_types.items()}
                detected_type = type_lookup.get(column.lower(), "")
                is_date_only = "date" in detected_type and "time" not in detected_type

                if isinstance(self.db, PostgresAdapter):
                    epoch_expr = f'EXTRACT(EPOCH FROM "{c}")'
                elif isinstance(self.db, DuckDBAdapter):
                    epoch_expr = f'epoch(CAST("{c}" AS TIMESTAMP))'
                else:
                    raise self._unsupported_backend_error()
                from_epoch_expr = "TO_TIMESTAMP(value_epoch)"
                if is_date_only:
                    from_epoch_expr = f"CAST({from_epoch_expr} AS DATE)"

                val = await self._fetchval(
                    f"""
                    WITH agg AS (
                        SELECT AVG({epoch_expr}) AS value_epoch
                        FROM {q}
                        WHERE "{c}" IS NOT NULL
                    )
                    SELECT {from_epoch_expr} AS mean_datetime
                    FROM agg
                    """
                )
                msg = f"Mean datetime of '{column}': {val}" if val is not None else f"Mean datetime of '{column}': N/A"
                return self._success_response(msg, [column],result=val)
            else:
                raise self._unsupported_backend_error()
        except Exception as e:
            return self._error_response(str(e), [column])

    async def datetime_median(self, table: str, schema: str, column: str) -> Dict[str, Any]:
        try:
            if isinstance(self.db, PostgresAdapter) or isinstance(self.db, DuckDBAdapter):
                q = self._qualified_table(table, schema)
                c = SQLIdentifierSanitizer.sanitize(column)

                col_types = await self.db.get_column_types(table, schema)
                type_lookup = {str(k).lower(): str(v).lower() for k, v in col_types.items()}
                detected_type = type_lookup.get(column.lower(), "")
                is_date_only = "date" in detected_type and "time" not in detected_type

                if isinstance(self.db, PostgresAdapter):
                    epoch_expr = f'EXTRACT(EPOCH FROM "{c}")'
                    median_epoch_expr = f'PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY {epoch_expr})'
                elif isinstance(self.db, DuckDBAdapter):
                    epoch_expr = f'epoch(CAST("{c}" AS TIMESTAMP))'
                    median_epoch_expr = f"MEDIAN({epoch_expr})"
                else:
                    raise self._unsupported_backend_error()
                from_epoch_expr = "TO_TIMESTAMP(value_epoch)"
                if is_date_only:
                    from_epoch_expr = f"CAST({from_epoch_expr} AS DATE)"

                val = await self._fetchval(
                    f"""
                    WITH agg AS (
                        SELECT {median_epoch_expr} AS value_epoch
                        FROM {q}
                        WHERE "{c}" IS NOT NULL
                    )
                    SELECT {from_epoch_expr} AS median_datetime
                    FROM agg
                    """
                )
                msg = f"Median datetime of '{column}': {val}" if val is not None else f"Median datetime of '{column}': N/A"
                return self._success_response(msg, [column],result=val)
            else:
                raise self._unsupported_backend_error()
        except Exception as e:
            return self._error_response(str(e), [column])

    async def datetime_count(self, table: str, schema: str, column: str) -> Dict[str, Any]:
        if isinstance(self.db, PostgresAdapter) or isinstance(self.db, DuckDBAdapter):
            return await self.numeric_count(table, schema, column)

        else:
            raise self._unsupported_backend_error()
    async def datetime_nunique(self, table: str, schema: str, column: str) -> Dict[str, Any]:
        if isinstance(self.db, PostgresAdapter) or isinstance(self.db, DuckDBAdapter):
            return await self.numeric_nunique(table, schema, column)

        else:
            raise self._unsupported_backend_error()
    async def datetime_diff(self, table: str, schema: str, column: str) -> Dict[str, Any]:
        try:
            if isinstance(self.db, PostgresAdapter) or isinstance(self.db, DuckDBAdapter):
                q = self._qualified_table(table, schema)
                c = SQLIdentifierSanitizer.sanitize(column)
                if isinstance(self.db, PostgresAdapter):
                    diff_expr = (
                        f'EXTRACT(EPOCH FROM CAST(b."{c}" AS TIMESTAMP)) - '
                        f'EXTRACT(EPOCH FROM CAST(a."{c}" AS TIMESTAMP))'
                    )
                elif isinstance(self.db, DuckDBAdapter):
                    diff_expr = f'epoch(CAST(b."{c}" AS TIMESTAMP)) - epoch(CAST(a."{c}" AS TIMESTAMP))'
                else:
                    raise self._unsupported_backend_error()
                sql = f'''
                    WITH ordered AS (
                        SELECT "{c}", ROW_NUMBER() OVER (ORDER BY "{c}") AS rn
                        FROM {q} WHERE "{c}" IS NOT NULL
                    )
                    SELECT ({diff_expr}) AS diff_seconds
                    FROM ordered a JOIN ordered b ON a.rn = b.rn - 1
                    ORDER BY a."{c}"
                '''
                rows = await self._fetch(sql)
                diffs = [r["diff_seconds"] for r in rows if r["diff_seconds"] is not None]
                if not diffs:
                    return self._success_response(f"No time differences for '{column}'", [column], result=[])
                msg = (f"Time differences for '{column}': count={len(diffs)}, min={min(diffs):.1f}s, "
                       f"max={max(diffs):.1f}s")
                return self._success_response(msg, [column], result=diffs)
            else:
                raise self._unsupported_backend_error()
        except Exception as e:
            return self._error_response(str(e), [column])

    async def datetime_delta_stats(self, table: str, schema: str, column: str) -> Dict[str, Any]:
        try:
            if isinstance(self.db, PostgresAdapter) or isinstance(self.db, DuckDBAdapter):
                q = self._qualified_table(table, schema)
                c = SQLIdentifierSanitizer.sanitize(column)
                if isinstance(self.db, PostgresAdapter):
                    diff_expr = (
                        f'EXTRACT(EPOCH FROM CAST(b."{c}" AS TIMESTAMP)) - '
                        f'EXTRACT(EPOCH FROM CAST(a."{c}" AS TIMESTAMP))'
                    )
                    median_expr = 'PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY d)'
                elif isinstance(self.db, DuckDBAdapter):
                    diff_expr = f'epoch(CAST(b."{c}" AS TIMESTAMP)) - epoch(CAST(a."{c}" AS TIMESTAMP))'
                    median_expr = 'MEDIAN(d)'
                else:
                    raise self._unsupported_backend_error()
                sql = f'''
                    WITH ordered AS (
                        SELECT "{c}", ROW_NUMBER() OVER (ORDER BY "{c}") AS rn
                        FROM {q} WHERE "{c}" IS NOT NULL
                    ), diffs AS (
                        SELECT ({diff_expr}) AS d
                        FROM ordered a JOIN ordered b ON a.rn = b.rn - 1
                    )
                    SELECT COUNT(d) AS cnt, MIN(d) AS min_d, MAX(d) AS max_d,
                           AVG(d) AS avg_d, {median_expr} AS median_d,
                           STDDEV_POP(d) AS std_d
                    FROM diffs WHERE d IS NOT NULL
                '''
                row = await self._fetch(sql)
                vals = {}
                if row and row[0] and row[0]["cnt"] > 0:
                    vals = dict(row[0])
                    msg = (f"Delta stats for '{column}': count={vals['cnt']}, "
                           f"min={vals['min_d']:.1f}s, max={vals['max_d']:.1f}s, "
                           f"avg={vals['avg_d']:.1f}s, median={vals['median_d']:.1f}s, "
                           f"std={vals['std_d']:.1f}s")
                else:
                    msg = f"No delta stats for '{column}'"
                return self._success_response(msg, [column],result=vals)
            else:
                raise self._unsupported_backend_error()
        except Exception as e:
            return self._error_response(str(e), [column])

    async def datetime_event_rate( self, table: str, schema: str, column: str, unit: str = "day") -> Dict[str, Any]:
        try:
            if isinstance(self.db, PostgresAdapter) or isinstance(self.db, DuckDBAdapter):
                q = self._qualified_table(table, schema)
                c = SQLIdentifierSanitizer.sanitize(column)
                # Get range and count
                row = await self._fetch(f'SELECT MIN("{c}") as min_dt, MAX("{c}") as max_dt FROM {q} WHERE "{c}" IS NOT NULL')
                if not row or not row[0]["min_dt"]:
                    return self._success_response(f"No valid data for event rate in '{column}'", [column])
                min_dt, max_dt = row[0]["min_dt"], row[0]["max_dt"]
                total = await self._fetchval(f'SELECT COUNT(*) FROM {q} WHERE "{c}" IS NOT NULL')
                diff = max_dt - min_dt
                units = {"second": 1, "minute": 60, "hour": 3600, "day": 86400, "week": 604800}
                seconds = diff.total_seconds()
                if unit in units:
                    rate = total / (seconds / units[unit]) if seconds > 0 else 0
                else:
                    rate = total / (seconds / 86400)  # default day
                    unit = "day"
                msg = f"Event rate for '{column}': {rate:.4f} per {unit}"
                return self._success_response(msg, [column],result=rate)
            else:
                raise self._unsupported_backend_error()
        except Exception as e:
            return self._error_response(str(e), [column])

    async def datetime_time_unit_counts(self, table: str, schema: str, column: str, unit: str = "day") -> Dict[str, Any]:
        try:
            if isinstance(self.db, PostgresAdapter) or isinstance(self.db, DuckDBAdapter):
                q = self._qualified_table(table, schema)
                c = SQLIdentifierSanitizer.sanitize(column)
                mapping = {
                    "hour": "HOUR", "day": "DAY", "month": "MONTH",
                    "year": "YEAR", "dow": "DOW", "quarter": "QUARTER"
                }
                sql_unit = mapping.get(unit.lower(), "DAY")
                rows = await self._fetch(
                    f'SELECT EXTRACT({sql_unit} FROM "{c}") as time_unit, COUNT(*) as cnt '
                    f'FROM {q} WHERE "{c}" IS NOT NULL GROUP BY time_unit ORDER BY time_unit'
                )
                result = {r["time_unit"]: r["cnt"] for r in rows}
                msg = f"Counts by {unit} for '{column}': {len(result)} unique values"
                return self._success_response(msg, [column], result=result)
            else:
                raise self._unsupported_backend_error()
        except Exception as e:
            return self._error_response(str(e), [column])

    async def datetime_weekday_weekend_counts(self, table: str, schema: str, column: str) -> Dict[str, Any]:
        try:
            if isinstance(self.db, PostgresAdapter) or isinstance(self.db, DuckDBAdapter):
                q = self._qualified_table(table, schema)
                c = SQLIdentifierSanitizer.sanitize(column)
                rows = await self._fetch(
                    f'SELECT CASE WHEN EXTRACT(DOW FROM "{c}") IN (0,6) THEN \'weekend\' ELSE \'weekday\' END AS type, '
                    f'COUNT(*) as cnt FROM {q} WHERE "{c}" IS NOT NULL GROUP BY type'
                )
                counts = {r["type"]: r["cnt"] for r in rows}
                wday = counts.get("weekday", 0)
                wend = counts.get("weekend", 0)
                total = wday + wend
                if total > 0:
                    msg = (f"Weekday/weekend for '{column}': weekdays={wday} ({wday/total*100:.1f}%), "
                           f"weekends={wend} ({wend/total*100:.1f}%)")
                else:
                    msg = f"No data for '{column}'"
                return self._success_response(msg, [column],result={"weekday":wday,"weekend":wend})
            else:
                raise self._unsupported_backend_error()
        except Exception as e:
            return self._error_response(str(e), [column])

    async def datetime_holiday_counts(self, table: str, schema: str, column: str) -> Dict[str, Any]:
        try:
            if isinstance(self.db, PostgresAdapter) or isinstance(self.db, DuckDBAdapter):
                q = self._qualified_table(table, schema)
                c = SQLIdentifierSanitizer.sanitize(column)
                rows = await self._fetch(
                    f'SELECT CASE '
                    f'WHEN EXTRACT(MONTH FROM "{c}") = 1 AND EXTRACT(DAY FROM "{c}") = 1 THEN \'New Year\' '
                    f'WHEN EXTRACT(MONTH FROM "{c}") = 12 AND EXTRACT(DAY FROM "{c}") = 25 THEN \'Christmas\' '
                    f'ELSE \'non-holiday\' END AS holiday, COUNT(*) as cnt '
                    f'FROM {q} WHERE "{c}" IS NOT NULL GROUP BY holiday'
                )
                counts = {r["holiday"]: r["cnt"] for r in rows}
                total_holidays = sum(v for k,v in counts.items() if k != "non-holiday")
                msg = f"Holiday counts for '{column}': total holidays={total_holidays}, New Year={counts.get('New Year',0)}, Christmas={counts.get('Christmas',0)}"
                return self._success_response(msg, [column],result=counts)
            else:
                raise self._unsupported_backend_error()
        except Exception as e:
            return self._error_response(str(e), [column])

    
