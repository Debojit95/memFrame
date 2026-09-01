
from typing import Any, Dict, List, Optional

from memframe.core.analytix.stats.base import DataStatsOps
from memframe.utils.helper import SQLIdentifierSanitizer


class PostgresDataStatsOps(DataStatsOps):
    """PostgreSQL backend.

    Dialect hooks (PERCENTILE_CONT/WITHIN GROUP, EXTRACT(EPOCH), ctid row id,
    DOUBLE PRECISION casts, batch-capped streamed matrices) plus skew/kurtosis
    which PG computes in a WITH-stats CTE instead of SKEWNESS()/KURTOSIS().
    """

    def _median_expr(self, c: str) -> str:
        return f'PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY {c})'

    def _quantile_expr(self, c: str, qt) -> str:
        return f'PERCENTILE_CONT({qt}) WITHIN GROUP (ORDER BY {c})'

    def _double_cast(self, c: str) -> str:
        return f'CAST("{c}" AS DOUBLE PRECISION)'

    def _text_to_double(self, c: str) -> str:
        return f'CAST(NULLIF("{c}"::text, \'\') AS DOUBLE PRECISION)'

    def _epoch_expr(self, c: str) -> str:
        return f'EXTRACT(EPOCH FROM {c})'

    def _assoc_batch_cap(self, n_pairs: int) -> int:
        return min(n_pairs, 1000)

    def _rowid_expr(self) -> str:
        return "ctid"

    # ------------------------------------------------------------------
    # PG-specific stats strategies
    # ------------------------------------------------------------------
    async def numeric_skew(self, table: str, schema: str, column: str) -> Dict[str, Any]:
        try:
            q = self._qualified_table(table, schema)
            c = SQLIdentifierSanitizer.sanitize(column)
            col = f'"{c}"::DOUBLE PRECISION'
            sql = f'\n                    WITH stats AS (\n                        SELECT\n                            AVG({col}) AS mean_val,\n                            STDDEV_SAMP({col}) AS std_val,\n                            COUNT({col})::DOUBLE PRECISION AS n\n                        FROM {q}\n                        WHERE "{c}" IS NOT NULL\n                    )\n                    SELECT\n                        CASE\n                            WHEN stats.n < 3 OR stats.std_val = 0 OR stats.std_val IS NULL THEN NULL\n                            ELSE (stats.n / ((stats.n - 1) * (stats.n - 2)))\n                                 * SUM(POWER({col} - stats.mean_val, 3))\n                                 / POWER(stats.std_val, 3)\n                        END\n                    FROM {q}\n                    CROSS JOIN stats\n                    WHERE "{c}" IS NOT NULL\n                    GROUP BY stats.n, stats.std_val, stats.mean_val\n                '
            val = await self._fetchval(sql)
            msg = f"Skewness of '{column}': {val:.4f}" if val is not None else 'N/A'
            return self._success_response(msg, result=val)
        except Exception as e:
            return self._error_response(str(e), [column])

    async def numeric_kurtosis(self, table: str, schema: str, column: str) -> Dict[str, Any]:
        try:
            q = self._qualified_table(table, schema)
            c = SQLIdentifierSanitizer.sanitize(column)
            col = f'"{c}"::DOUBLE PRECISION'
            sql = f'\n                    WITH stats AS (\n                        SELECT\n                            AVG({col}) AS mean_val,\n                            STDDEV_SAMP({col}) AS std_val,\n                            COUNT({col})::DOUBLE PRECISION AS n\n                        FROM {q}\n                        WHERE "{c}" IS NOT NULL\n                    )\n                    SELECT\n                        CASE\n                            WHEN stats.n < 4 OR stats.std_val = 0 OR stats.std_val IS NULL THEN NULL\n                            ELSE ((stats.n * (stats.n + 1))\n                                  / ((stats.n - 1) * (stats.n - 2) * (stats.n - 3)))\n                                 * SUM(POWER({col} - stats.mean_val, 4))\n                                 / POWER(stats.std_val, 4)\n                                 - (3.0 * POWER(stats.n - 1, 2))\n                                   / ((stats.n - 2) * (stats.n - 3))\n                        END\n                    FROM {q}\n                    CROSS JOIN stats\n                    WHERE "{c}" IS NOT NULL\n                    GROUP BY stats.n, stats.std_val, stats.mean_val\n                '
            val = await self._fetchval(sql)
            msg = f"Kurtosis of '{column}': {val:.4f}" if val is not None else 'N/A'
            return self._success_response(msg, result=val)
        except Exception as e:
            return self._error_response(str(e), [column])

    async def datetime_diff(self, table: str, schema: str, column: str, backend=None, data_id: Optional[str]=None, new_table: Optional[str]=None, target_col: Optional[str]=None) -> Dict[str, Any]:
        try:
            q = self._qualified_table(table, schema)
            c = SQLIdentifierSanitizer.sanitize(column)
            e = f'EXTRACT(EPOCH FROM CAST("{c}" AS TIMESTAMP))::DOUBLE PRECISION'
            diff_expr = f'{e} - LAG({e}) OVER (ORDER BY CAST("{c}" AS TIMESTAMP))'
            tgt = SQLIdentifierSanitizer.sanitize(target_col) if target_col else f'{c}__diff_seconds'
            out = await self._materialize_query_as_table(f'SELECT *, ({diff_expr}) AS "{tgt}" FROM {q}', table, schema, backend=backend, data_id=data_id, new_table=new_table)
            await self._add_column_if_not_exists(table, schema, tgt, 'DOUBLE PRECISION')
            orig_q = self._qualified_table(table, schema)
            key = 'ctid'
            await self._exec(f'UPDATE {orig_q} SET "{tgt}" = sub.d FROM (SELECT {key}, ({diff_expr}) AS d FROM {orig_q}) sub WHERE {orig_q}.{key} = sub.{key}')
            sample = await self._fetch_data(out, schema, [column, tgt])
            msg = f"Time differences for '{column}' → '{tgt}'"
            return self._success_response(msg, [column], [tgt], sample, new_table=out)
        except Exception as e:
            return self._error_response(str(e), [column])

    async def datetime_delta_stats(self, table: str, schema: str, column: str) -> Dict[str, Any]:
        try:
            q = self._qualified_table(table, schema)
            c = SQLIdentifierSanitizer.sanitize(column)
            diff_expr = f'EXTRACT(EPOCH FROM CAST(b."{c}" AS TIMESTAMP)) - EXTRACT(EPOCH FROM CAST(a."{c}" AS TIMESTAMP))'
            median_expr = 'PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY d)'
            sql = f'\n                    WITH ordered AS (\n                        SELECT "{c}", ROW_NUMBER() OVER (ORDER BY "{c}") AS rn\n                        FROM {q} WHERE "{c}" IS NOT NULL\n                    ), diffs AS (\n                        SELECT ({diff_expr}) AS d\n                        FROM ordered a JOIN ordered b ON a.rn + 1 = b.rn\n                    )\n                    SELECT COUNT(d) AS cnt, MIN(d) AS min_d, MAX(d) AS max_d,\n                           AVG(d) AS avg_d, {median_expr} AS median_d,\n                           STDDEV_POP(d) AS std_d\n                    FROM diffs WHERE d IS NOT NULL\n                '
            row = await self._fetch(sql)
            vals = {}
            if row and row[0] and (row[0]['cnt'] > 0):
                vals = dict(row[0])
                msg = f"Delta stats for '{column}': count={vals['cnt']}, min={vals['min_d']:.1f}s, max={vals['max_d']:.1f}s, avg={vals['avg_d']:.1f}s, median={vals['median_d']:.1f}s, std={vals['std_d']:.1f}s"
            else:
                msg = f"No delta stats for '{column}'"
            return self._success_response(msg, [column], result=vals)
        except Exception as e:
            return self._error_response(str(e), [column])

    async def numeric_multi_column_correlation(self, table: str, schema: str, columns: List[str], backend=None, data_id: Optional[str]=None, new_table: Optional[str]=None) -> Dict[str, Any]:
        return await self._multi_column_assoc_matrix_streamed(table, schema, columns, 'corr', data_id=data_id)

    async def numeric_multi_column_covariance(self, table: str, schema: str, columns: List[str], backend=None, data_id: Optional[str]=None, new_table: Optional[str]=None) -> Dict[str, Any]:
        return await self._multi_column_assoc_matrix_streamed(table, schema, columns, 'cov', data_id=data_id)
