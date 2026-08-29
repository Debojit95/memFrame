from __future__ import annotations
from typing import Any, Dict, List, Optional
import traceback
import pandas as pd
import numpy as np
from memframe.utils.helper import SQLIdentifierSanitizer
from memframe.core.analytix.stats.base import DataStatsOps

class PostgresDataStatsOps(DataStatsOps):
    """
    Pure SQL‑based statistics. Receives a DatabaseAdapter and explicit
    (table, schema, column, …) parameters. Returns a unified response dict.
    """

    async def _add_column_if_not_exists(self, table: str, schema: str, column: str, data_type: str='DOUBLE PRECISION') -> None:
        qualified = self._qualified_table(table, schema)
        safe_col = SQLIdentifierSanitizer.sanitize(column)
        if False and data_type == 'DOUBLE PRECISION':
            data_type = 'Float64'
        atype = 'UInt8' if False and data_type == 'INTEGER' else data_type
        try:
            await self._exec(f'SELECT {self.db.quote_identifier(safe_col)} FROM {qualified} LIMIT 1')
        except Exception:
            await self._exec(f'ALTER TABLE {qualified} ADD COLUMN {self.db.quote_identifier(safe_col)} {atype}')

    async def numeric_count(self, table: str, schema: str, column: str) -> Dict[str, Any]:
        try:
            q = self._qualified_table(table, schema)
            c = SQLIdentifierSanitizer.sanitize(column)
            count = await self._fetchval(f'SELECT COUNT("{c}") FROM {q} WHERE "{c}" IS NOT NULL')
            return self._success_response(message=f"Count of '{column}': {count}", result=count)
        except Exception as e:
            return self._error_response(str(e), [column])

    async def numeric_sum(self, table: str, schema: str, column: str) -> Dict[str, Any]:
        try:
            q = self._qualified_table(table, schema)
            c = SQLIdentifierSanitizer.sanitize(column)
            sum_res = await self._fetchval(f'SELECT SUM("{c}") FROM {q} WHERE "{c}" IS NOT NULL')
            return self._success_response(message=f"Sum of '{column}': {sum_res}", result=sum_res)
        except Exception as e:
            return self._error_response(str(e), [column])

    async def numeric_min(self, table: str, schema: str, column: str) -> Dict[str, Any]:
        try:
            q = self._qualified_table(table, schema)
            c = SQLIdentifierSanitizer.sanitize(column)
            min_val = await self._fetchval(f'SELECT MIN("{c}") FROM {q} WHERE "{c}" IS NOT NULL')
            return self._success_response(f"Minimum value in '{column}': {min_val}", result=min_val)
        except Exception as e:
            return self._error_response(str(e), [column])

    async def numeric_max(self, table: str, schema: str, column: str) -> Dict[str, Any]:
        try:
            q = self._qualified_table(table, schema)
            c = SQLIdentifierSanitizer.sanitize(column)
            max_val = await self._fetchval(f'SELECT MAX("{c}") FROM {q} WHERE "{c}" IS NOT NULL')
            return self._success_response(f"Maximum value in '{column}': {max_val}", result=max_val)
        except Exception as e:
            return self._error_response(str(e), [column])

    async def numeric_mean(self, table: str, schema: str, column: str) -> Dict[str, Any]:
        try:
            q = self._qualified_table(table, schema)
            c = SQLIdentifierSanitizer.sanitize(column)
            mean_val = await self._fetchval(f'SELECT AVG("{c}") FROM {q} WHERE "{c}" IS NOT NULL')
            msg = f"Mean of '{column}': {mean_val:.4f}" if mean_val is not None else f"Mean of '{column}': N/A"
            return self._success_response(message=msg, result=mean_val)
        except Exception as e:
            return self._error_response(str(e), [column])

    async def numeric_median(self, table: str, schema: str, column: str) -> Dict[str, Any]:
        try:
            q = self._qualified_table(table, schema)
            c = SQLIdentifierSanitizer.sanitize(column)
            median_sql = f'PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY "{c}")'
            median_val = await self._fetchval(f'SELECT {median_sql} FROM {q} WHERE "{c}" IS NOT NULL')
            msg = f"Median of '{column}': {median_val}"
            return self._success_response(msg, result=median_val)
        except Exception as e:
            return self._error_response(str(e), [column])

    async def numeric_mode(self, table: str, schema: str, column: str, top_n: int=1) -> Dict[str, Any]:
        try:
            q = self._qualified_table(table, schema)
            c = SQLIdentifierSanitizer.sanitize(column)
            rows = await self._fetch(f'SELECT "{c}" AS mode, COUNT(*) AS freq FROM {q} WHERE "{c}" IS NOT NULL GROUP BY "{c}" ORDER BY freq DESC LIMIT {top_n}')
            if not rows:
                return self._success_response(f"No mode found for '{column}'", [column])
            if top_n == 1:
                val = rows[0]['mode']
                freq = rows[0]['freq']
                return self._success_response(f"Mode of '{column}': {val} (frequency: {freq})", [column], result=val)
            desc = ', '.join((f"{r['mode']}({r['freq']})" for r in rows))
            return self._success_response(f'Top {top_n} modes: {desc}', [column], result=[r['mode'] for r in rows])
        except Exception as e:
            return self._error_response(str(e), [column])

    async def numeric_prod(self, table: str, schema: str, column: str) -> Dict[str, Any]:
        try:
            q = self._qualified_table(table, schema)
            c = SQLIdentifierSanitizer.sanitize(column)
            prod_val = await self._fetchval(f'SELECT EXP(SUM(LN("{c}"))) FROM {q} WHERE "{c}" > 0')
            return self._success_response(f"Product of positive values in '{column}': {prod_val}", result=prod_val)
        except Exception as e:
            return self._error_response(str(e), [column])

    async def numeric_unique(self, table: str, schema: str, column: str) -> Dict[str, Any]:
        try:
            q = self._qualified_table(table, schema)
            c = SQLIdentifierSanitizer.sanitize(column)
            rows = await self._fetch(f'SELECT DISTINCT "{c}" FROM {q} WHERE "{c}" IS NOT NULL ORDER BY "{c}"')
            vals = [row[c] for row in rows]
            msg = f"Unique values in '{column}': {len(vals)} distinct"
            if len(vals) <= 20:
                msg += f' – {vals}'
            return self._success_response(message=msg, result=vals)
        except Exception as e:
            return self._error_response(str(e), [column])

    async def numeric_nunique(self, table: str, schema: str, column: str) -> Dict[str, Any]:
        try:
            q = self._qualified_table(table, schema)
            c = SQLIdentifierSanitizer.sanitize(column)
            val = await self._fetchval(f'SELECT COUNT(DISTINCT "{c}") FROM {q} WHERE "{c}" IS NOT NULL')
            return self._success_response(f"Number of unique values in '{column}': {val}", result=val)
        except Exception as e:
            return self._error_response(str(e), [column])

    async def numeric_value_counts(self, table: str, schema: str, column: str, top_n: int=10) -> Dict[str, Any]:
        try:
            q = self._qualified_table(table, schema)
            c = SQLIdentifierSanitizer.sanitize(column)
            rows = await self._fetch(f'SELECT "{c}" AS val, COUNT(*) AS cnt FROM {q} WHERE "{c}" IS NOT NULL GROUP BY "{c}" ORDER BY cnt DESC LIMIT {top_n}')
            val_counts = {r['val']: r['cnt'] for r in rows}
            return self._success_response(f'Top {top_n} value counts', result=val_counts)
        except Exception as e:
            return self._error_response(str(e), [column])

    async def numeric_std(self, table: str, schema: str, column: str) -> Dict[str, Any]:
        try:
            q = self._qualified_table(table, schema)
            c = SQLIdentifierSanitizer.sanitize(column)
            std_val = await self._fetchval(f'SELECT STDDEV_POP("{c}") FROM {q} WHERE "{c}" IS NOT NULL')
            msg = f"Standard deviation of '{column}': {std_val:.4f}" if std_val is not None else f"Std of '{column}': N/A"
            return self._success_response(message=msg, result=std_val)
        except Exception as e:
            return self._error_response(str(e), [column])

    async def numeric_var(self, table: str, schema: str, column: str) -> Dict[str, Any]:
        try:
            q = self._qualified_table(table, schema)
            c = SQLIdentifierSanitizer.sanitize(column)
            var_val = await self._fetchval(f'SELECT VAR_POP("{c}") FROM {q} WHERE "{c}" IS NOT NULL')
            msg = f"Variance of '{column}': {var_val:.4f}" if var_val is not None else f"Variance of '{column}': N/A"
            return self._success_response(msg, result=var_val)
        except Exception as e:
            return self._error_response(str(e), [column])

    async def numeric_sem(self, table: str, schema: str, column: str) -> Dict[str, Any]:
        try:
            q = self._qualified_table(table, schema)
            c = SQLIdentifierSanitizer.sanitize(column)
            val = await self._fetchval(f'SELECT STDDEV_SAMP("{c}") / SQRT(COUNT("{c}")) FROM {q} WHERE "{c}" IS NOT NULL')
            msg = f"Standard error of mean for '{column}': {val:.6f}" if val is not None else f"SEM of '{column}': N/A"
            return self._success_response(msg, result=val)
        except Exception as e:
            return self._error_response(str(e), [column])

    async def numeric_mad(self, table: str, schema: str, column: str) -> Dict[str, Any]:
        try:
            q = self._qualified_table(table, schema)
            c = SQLIdentifierSanitizer.sanitize(column)
            val = await self._fetchval(f'SELECT AVG(ABS("{c}" - sub.avg_val)) FROM {q}, (SELECT AVG("{c}") AS avg_val FROM {q} WHERE "{c}" IS NOT NULL) AS sub WHERE "{c}" IS NOT NULL')
            msg = f"Mean Absolute Deviation of '{column}': {val:.4f}" if val is not None else f"MAD of '{column}': N/A"
            return self._success_response(message=msg, result=val)
        except Exception as e:
            return self._error_response(str(e), [column])

    async def numeric_iqr(self, table: str, schema: str, column: str) -> Dict[str, Any]:
        try:
            q = self._qualified_table(table, schema)
            c = SQLIdentifierSanitizer.sanitize(column)
            q3_expr = f'PERCENTILE_CONT(0.75) WITHIN GROUP (ORDER BY "{c}")'
            q1_expr = f'PERCENTILE_CONT(0.25) WITHIN GROUP (ORDER BY "{c}")'
            val = await self._fetchval(f'SELECT {q3_expr} - {q1_expr} FROM {q} WHERE "{c}" IS NOT NULL')
            msg = f"IQR of '{column}': {val:.4f}" if val is not None else f"IQR of '{column}': N/A"
            return self._success_response(msg, result=val)
        except Exception as e:
            return self._error_response(str(e), [column])

    async def numeric_range(self, table: str, schema: str, column: str) -> Dict[str, Any]:
        try:
            q = self._qualified_table(table, schema)
            c = SQLIdentifierSanitizer.sanitize(column)
            val = await self._fetchval(f'SELECT MAX("{c}") - MIN("{c}") FROM {q} WHERE "{c}" IS NOT NULL')
            msg = f"Range of '{column}': {val:.4f}" if val is not None else f"Range of '{column}': N/A"
            return self._success_response(msg, result=val)
        except Exception as e:
            return self._error_response(str(e), [column])

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

    async def numeric_entropy(self, table: str, schema: str, column: str) -> Dict[str, Any]:
        try:
            q = self._qualified_table(table, schema)
            c = SQLIdentifierSanitizer.sanitize(column)
            sql = f'\n                    WITH counts AS (SELECT "{c}", COUNT(*) AS cnt FROM {q} WHERE "{c}" IS NOT NULL GROUP BY "{c}"),\n                         total AS (SELECT SUM(cnt) AS tot FROM counts)\n                    SELECT -SUM((cnt::float / tot) * LN(cnt::float / tot)) FROM counts, total\n                '
            val = await self._fetchval(sql)
            msg = f"Entropy of '{column}': {val:.4f}" if val is not None else 'N/A'
            return self._success_response(msg, result=val)
        except Exception as e:
            return self._error_response(str(e), [column])

    async def numeric_quantile(self, table: str, schema: str, column: str, quantiles: List[float]=None) -> Dict[str, Any]:
        try:
            if quantiles is None:
                quantiles = [0.25, 0.5, 0.75]
            q = self._qualified_table(table, schema)
            c = SQLIdentifierSanitizer.sanitize(column)
            parts = ', '.join((f'PERCENTILE_CONT({qt}) WITHIN GROUP (ORDER BY "{c}") AS p_{int(qt * 100)}' for qt in quantiles))
            row = await self._fetch(f'SELECT {parts} FROM {q} WHERE "{c}" IS NOT NULL')
            if row and row[0]:
                vals = dict(row[0])
                desc = ', '.join((f'{k}: {v:.4f}' for k, v in vals.items()))
                return self._success_response(f"Quantiles of '{column}': {desc}", result=vals)
            return self._success_response(f"No quantile results for '{column}'", [column])
        except Exception as e:
            return self._error_response(str(e), [column])

    async def numeric_autocorr(self, table: str, schema: str, column: str, lag: int=1) -> Dict[str, Any]:
        try:
            q = self._qualified_table(table, schema)
            c = SQLIdentifierSanitizer.sanitize(column)
            sql = f'\n                    WITH lagged AS (\n                        SELECT "{c}" AS cur, LAG("{c}", {lag}) OVER (ORDER BY (SELECT NULL)) AS prev\n                        FROM {q}\n                    )\n                    SELECT CORR(cur, prev) FROM lagged WHERE cur IS NOT NULL AND prev IS NOT NULL\n                '
            val = await self._fetchval(sql)
            msg = f"Autocorrelation (lag={lag}) for '{column}': {val:.4f}" if val is not None else 'N/A'
            return self._success_response(msg, result=val)
        except Exception as e:
            return self._error_response(str(e), [column])

    async def numeric_coefficient_of_variation(self, table: str, schema: str, column: str) -> Dict[str, Any]:
        try:
            q = self._qualified_table(table, schema)
            c = SQLIdentifierSanitizer.sanitize(column)
            val = await self._fetchval(f'SELECT STDDEV_POP("{c}") / AVG("{c}") FROM {q} WHERE "{c}" IS NOT NULL')
            msg = f"Coefficient of variation for '{column}': {val:.4f}" if val is not None else 'N/A'
            return self._success_response(msg, result=val)
        except Exception as e:
            return self._error_response(str(e), [column])

    async def numeric_outliers_iqr(self, table: str, schema: str, column: str) -> Dict[str, Any]:
        try:
            q = self._qualified_table(table, schema)
            c = SQLIdentifierSanitizer.sanitize(column)
            col_expr = f'CAST("{c}" AS DOUBLE PRECISION)'
            q1_expr = f'PERCENTILE_CONT(0.25) WITHIN GROUP (ORDER BY {col_expr})'
            q3_expr = f'PERCENTILE_CONT(0.75) WITHIN GROUP (ORDER BY {col_expr})'
            row = await self._fetch(f'SELECT {q1_expr} AS q1, {q3_expr} AS q3 FROM {q} WHERE "{c}" IS NOT NULL')
            if not row or row[0]['q1'] is None:
                return self._error_response(f"No numeric data for '{column}'", [column])
            q1 = float(row[0]['q1'])
            q3 = float(row[0]['q3'])
            iqr = q3 - q1
            lower = q1 - 1.5 * iqr
            upper = q3 + 1.5 * iqr
            stats = {'column': column, 'q1': q1, 'q3': q3, 'iqr': iqr, 'lower_bound': lower, 'upper_bound': upper}
            msg = f"Outlier bounds (IQR) for '{column}'"
            return self._success_response(msg, [column], result=stats)
        except Exception as e:
            return self._error_response(str(e), [column])

    async def numeric_outliers_zscore(self, table: str, schema: str, column: str, threshold: float=3.0, backend=None, data_id: Optional[str]=None) -> Dict[str, Any]:
        try:
            q = self._qualified_table(table, schema)
            c = SQLIdentifierSanitizer.sanitize(column)
            col_expr = f'CAST("{c}" AS DOUBLE PRECISION)'
            row = await self._fetch(f'SELECT AVG({col_expr}) AS m, STDDEV_POP({col_expr}) AS s FROM {q} WHERE "{c}" IS NOT NULL')
            if not row or row[0]['m'] is None:
                return self._error_response(f"No numeric data for '{column}'", [column])
            mean = float(row[0]['m'])
            std = float(row[0]['s'] or 0.0)
            lower = mean - threshold * std
            upper = mean + threshold * std
            stats = {'column': column, 'mean': mean, 'std': std, 'threshold': threshold, 'lower_bound': lower, 'upper_bound': upper}
            msg = f"Outlier bounds (z-score, threshold={threshold}) for '{column}'"
            return self._success_response(msg, [column], result=stats)
        except Exception as e:
            return self._error_response(str(e), [column])

    async def _multi_column_assoc_matrix(self, table: str, schema: str, columns: List[str], agg_fn: str, backend=None, data_id: Optional[str]=None, new_table: Optional[str]=None) -> Dict[str, Any]:
        """Compute an n x n correlation/covariance matrix purely in the database.

        All CORR/COVAR_SAMP calls are batched into single SELECT statements so the
        engine scans the table once per batch, not once per pair. ClickHouse uses a
        tighter batch so a single scan covers the table whenever the pair count fits
        its expression budget (Tier-2); wider tables fall back to batching.
        """
        try:
            q = self._qualified_table(table, schema)
            cols = [SQLIdentifierSanitizer.sanitize(c) for c in columns]
            n = len(cols)
            if n < 1:
                return self._error_response('No columns provided', columns)

            def _cast(c: str) -> str:
                return f'''CAST(NULLIF("{c}"::text, '') AS DOUBLE PRECISION)'''
            alias = {c: f'a{idx}' for idx, c in enumerate(cols)}
            pairs = [(i, j) for i in range(n) for j in range(i, n)]
            B = min(len(pairs), 1000)
            results: Dict[tuple, Any] = {}
            for start in range(0, len(pairs), B):
                batch = pairs[start:start + B]
                used = sorted({cols[i] for i, j in batch} | {cols[j] for i, j in batch}, key=lambda c: cols.index(c))
                cte = ', '.join((f'{_cast(c)} AS {alias[c]}' for c in used))
                sel = []
                for k, (i, j) in enumerate(batch):
                    ai, aj = (alias[cols[i]], alias[cols[j]])
                    cond = f'{ai} IS NOT NULL AND {aj} IS NOT NULL'
                    sel.append(f'{agg_fn}(CASE WHEN {cond} THEN {ai} END, CASE WHEN {cond} THEN {aj} END) AS v_{k}')
                sql = f'WITH q AS (SELECT {cte} FROM {q}) SELECT ' + ', '.join(sel) + ' FROM q'
                rows = await self._fetch(sql)
                row = rows[0] if rows else {}
                for k, (i, j) in enumerate(batch):
                    val = row.get(f'v_{k}') if row else None
                    results[i, j] = val if val is not None else float('nan')
            mat = [[results.get((min(i, j), max(i, j))) for j in range(n)] for i in range(n)]
            df = pd.DataFrame(mat, index=columns, columns=columns)
            msg = f'Computed {agg_fn} matrix for {n} columns'
            return self._success_response(msg, columns, result=df)
        except Exception as e:
            return self._error_response(f'multi_column_assoc_matrix error: {str(e)}\n{traceback.format_exc()}', columns)

    async def _multi_column_assoc_matrix_streamed(self, table: str, schema: str, columns: List[str], kind: str, data_id: Optional[str]=None, new_table: Optional[str]=None) -> Dict[str, Any]:
        """Postgres-only: stream numeric columns via fetch_iter into a float matrix,
        then compute corr/cov in numpy. Avoids bulk-pulling all rows/columns at once.

        ponytail: holds the full rows x numeric_cols float matrix client-side (the
        irreducible data size); a non-numeric value past the 5k detection sample still
        errors the CAST, identical to the in-DB path.
        """
        try:
            q = self._qualified_table(table, schema)
            cols = [SQLIdentifierSanitizer.sanitize(c) for c in columns]
            n = len(cols)
            if n < 1:
                return self._error_response('No columns provided', columns)

            def _cast(c: str) -> str:
                return f'''CAST(NULLIF("{c}"::text, '') AS DOUBLE PRECISION) AS "{c}"'''
            sql = f"SELECT {', '.join((_cast(c) for c in cols))} FROM {q}"
            row_count = await self._fetchval(f'SELECT COUNT(*) FROM {q}') or 0
            arr = np.empty((row_count, n), dtype=np.float64)
            r = 0
            async for record in self.db.fetch_iter(sql, chunk_size=2000):
                arr[r] = [np.nan if record[c] is None else record[c] for c in cols]
                r += 1
            df = pd.DataFrame(arr, columns=cols)
            mat = df.corr() if kind == 'corr' else df.cov()
            msg = f'Computed {kind} matrix for {n} columns (streamed in-memory)'
            return self._success_response(msg, columns, result=mat)
        except Exception as e:
            return self._error_response(f'multi_column_assoc_matrix_streamed error: {str(e)}\n{traceback.format_exc()}', columns)

    async def numeric_multi_column_correlation(self, table: str, schema: str, columns: List[str], backend=None, data_id: Optional[str]=None, new_table: Optional[str]=None) -> Dict[str, Any]:
        return await self._multi_column_assoc_matrix_streamed(table, schema, columns, 'corr', data_id=data_id)

    async def numeric_multi_column_covariance(self, table: str, schema: str, columns: List[str], backend=None, data_id: Optional[str]=None, new_table: Optional[str]=None) -> Dict[str, Any]:
        return await self._multi_column_assoc_matrix_streamed(table, schema, columns, 'cov', data_id=data_id)

    async def categorical_count(self, table: str, schema: str, column: str) -> Dict[str, Any]:
        return await self.numeric_count(table, schema, column)

    async def categorical_unique(self, table: str, schema: str, column: str) -> Dict[str, Any]:
        return await self.numeric_unique(table, schema, column)

    async def categorical_nunique(self, table: str, schema: str, column: str) -> Dict[str, Any]:
        return await self.numeric_nunique(table, schema, column)

    async def categorical_value_counts(self, table: str, schema: str, column: str, top_n: int=10) -> Dict[str, Any]:
        try:
            q = self._qualified_table(table, schema)
            c = SQLIdentifierSanitizer.sanitize(column)
            rows = await self._fetch(f'SELECT "{c}" AS value, COUNT(*) AS count FROM {q} WHERE "{c}" IS NOT NULL GROUP BY "{c}" ORDER BY count DESC LIMIT {top_n}')
            value_counts = {r['value']: r['count'] for r in rows}
            msg = f"Top {top_n} value counts for '{column}'"
            return self._success_response(msg, result=value_counts)
        except Exception as e:
            return self._error_response(str(e), [column])

    async def categorical_proportions(self, table: str, schema: str, column: str) -> Dict[str, Any]:
        try:
            q = self._qualified_table(table, schema)
            c = SQLIdentifierSanitizer.sanitize(column)
            total = await self._fetchval(f'SELECT COUNT(*) FROM {q} WHERE "{c}" IS NOT NULL')
            if total == 0:
                return self._success_response(f"No data in '{column}'", [column], result={})
            rows = await self._fetch(f'SELECT "{c}" AS category, COUNT(*) AS cnt, (COUNT(*) * 1.0 / {total}) AS proportion FROM {q} WHERE "{c}" IS NOT NULL GROUP BY "{c}" ORDER BY cnt DESC')
            result = {r['category']: float(r['proportion']) for r in rows}
            msg = f"Proportions for '{column}'"
            return self._success_response(msg, [column], result=result)
        except Exception as e:
            return self._error_response(str(e), [column])

    async def categorical_mode(self, table: str, schema: str, column: str, top_n: int=1) -> Dict[str, Any]:
        return await self.numeric_mode(table, schema, column, top_n)

    async def categorical_multi_column_crosstab(self, table: str, schema: str, columns: List[str], backend=None, data_id: Optional[str]=None, new_table: Optional[str]=None) -> Dict[str, Any]:
        try:
            q = self._qualified_table(table, schema)
            qcols = [SQLIdentifierSanitizer.sanitize(c) for c in columns]
            base_table = table
            output_table = await self._resolve_output_table_name(base_table, schema, backend=backend, data_id=data_id, new_table=new_table)
            combined = " || '|' || ".join((f'"{c}"' for c in qcols))
            where = ' AND '.join((f'"{c}" IS NOT NULL' for c in qcols))
            create_sql = f'\n                    CREATE TABLE {self._qualified_table(output_table, schema)} AS\n                    SELECT {combined} AS combined_key, COUNT(*) as cnt\n                    FROM {q}\n                    WHERE {where}\n                    GROUP BY combined_key\n                    ORDER BY cnt DESC\n                '
            await self._exec(create_sql)
            rows = await self._fetch(f'SELECT * FROM {self._qualified_table(output_table, schema)}')
            result = {r['combined_key']: {'values': r['combined_key'].split('|'), 'count': r['cnt']} for r in rows}
            msg = f"Multi-column crosstab for {len(columns)} columns: {len(result)} combinations, stored in '{output_table}'"
            return self._success_response(msg, columns, result=result, new_table=output_table)
        except Exception as e:
            return self._error_response(str(e), columns)

    async def categorical_multi_column_association(self, table: str, schema: str, columns: List[str]) -> Dict[str, Any]:
        try:
            result = {'columns': columns, 'status': 'placeholder'}
            msg = f'Association analysis for {len(columns)} columns'
            return self._success_response(msg, columns, result=result)
        except Exception as e:
            return self._error_response(str(e), columns)

    async def datetime_min(self, table: str, schema: str, column: str) -> Dict[str, Any]:
        try:
            q = self._qualified_table(table, schema)
            c = SQLIdentifierSanitizer.sanitize(column)
            min_val = await self._fetchval(f'SELECT MIN("{c}") FROM {q} WHERE "{c}" IS NOT NULL')
            return self._success_response(f"Earliest datetime in '{column}': {min_val}", [column], result=min_val)
        except Exception as e:
            return self._error_response(str(e), [column])

    async def datetime_max(self, table: str, schema: str, column: str) -> Dict[str, Any]:
        try:
            q = self._qualified_table(table, schema)
            c = SQLIdentifierSanitizer.sanitize(column)
            max_val = await self._fetchval(f'SELECT MAX("{c}") FROM {q} WHERE "{c}" IS NOT NULL')
            return self._success_response(f"Latest datetime in '{column}': {max_val}", [column], result=max_val)
        except Exception as e:
            return self._error_response(str(e), [column])

    async def datetime_mean(self, table: str, schema: str, column: str) -> Dict[str, Any]:
        try:
            q = self._qualified_table(table, schema)
            c = SQLIdentifierSanitizer.sanitize(column)
            col_types = await self.db.get_column_types(table, schema)
            type_lookup = {str(k).lower(): str(v).lower() for k, v in col_types.items()}
            detected_type = type_lookup.get(column.lower(), '')
            is_date_only = 'date' in detected_type and 'time' not in detected_type
            epoch_expr = f'EXTRACT(EPOCH FROM "{c}")'
            from_epoch_expr = 'TO_TIMESTAMP(value_epoch)'
            if is_date_only:
                from_epoch_expr = f'CAST({from_epoch_expr} AS DATE)'
            val = await self._fetchval(f'\n                    WITH agg AS (\n                        SELECT AVG({epoch_expr}) AS value_epoch\n                        FROM {q}\n                        WHERE "{c}" IS NOT NULL\n                    )\n                    SELECT {from_epoch_expr} AS mean_datetime\n                    FROM agg\n                    ')
            msg = f"Mean datetime of '{column}': {val}" if val is not None else f"Mean datetime of '{column}': N/A"
            return self._success_response(msg, [column], result=val)
        except Exception as e:
            return self._error_response(str(e), [column])

    async def datetime_median(self, table: str, schema: str, column: str) -> Dict[str, Any]:
        try:
            q = self._qualified_table(table, schema)
            c = SQLIdentifierSanitizer.sanitize(column)
            col_types = await self.db.get_column_types(table, schema)
            type_lookup = {str(k).lower(): str(v).lower() for k, v in col_types.items()}
            detected_type = type_lookup.get(column.lower(), '')
            is_date_only = 'date' in detected_type and 'time' not in detected_type
            epoch_expr = f'EXTRACT(EPOCH FROM "{c}")'
            median_epoch_expr = f'PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY {epoch_expr})'
            from_epoch_expr = 'TO_TIMESTAMP(value_epoch)'
            if is_date_only:
                from_epoch_expr = f'CAST({from_epoch_expr} AS DATE)'
            val = await self._fetchval(f'\n                    WITH agg AS (\n                        SELECT {median_epoch_expr} AS value_epoch\n                        FROM {q}\n                        WHERE "{c}" IS NOT NULL\n                    )\n                    SELECT {from_epoch_expr} AS median_datetime\n                    FROM agg\n                    ')
            msg = f"Median datetime of '{column}': {val}" if val is not None else f"Median datetime of '{column}': N/A"
            return self._success_response(msg, [column], result=val)
        except Exception as e:
            return self._error_response(str(e), [column])

    async def datetime_count(self, table: str, schema: str, column: str) -> Dict[str, Any]:
        return await self.numeric_count(table, schema, column)

    async def datetime_nunique(self, table: str, schema: str, column: str) -> Dict[str, Any]:
        return await self.numeric_nunique(table, schema, column)

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

    async def datetime_event_rate(self, table: str, schema: str, column: str, unit: str='day') -> Dict[str, Any]:
        try:
            q = self._qualified_table(table, schema)
            c = SQLIdentifierSanitizer.sanitize(column)
            row = await self._fetch(f'SELECT MIN("{c}") as min_dt, MAX("{c}") as max_dt FROM {q} WHERE "{c}" IS NOT NULL')
            if not row or not row[0]['min_dt']:
                return self._success_response(f"No valid data for event rate in '{column}'", [column])
            min_dt, max_dt = (row[0]['min_dt'], row[0]['max_dt'])
            total = await self._fetchval(f'SELECT COUNT(*) FROM {q} WHERE "{c}" IS NOT NULL')
            diff = max_dt - min_dt
            units = {'second': 1, 'minute': 60, 'hour': 3600, 'day': 86400, 'week': 604800}
            seconds = diff.total_seconds()
            if unit in units:
                rate = total / (seconds / units[unit]) if seconds > 0 else 0
            else:
                rate = total / (seconds / 86400)
                unit = 'day'
            msg = f"Event rate for '{column}': {rate:.4f} per {unit}"
            return self._success_response(msg, [column], result=rate)
        except Exception as e:
            return self._error_response(str(e), [column])

    async def datetime_time_unit_counts(self, table: str, schema: str, column: str, unit: str='day') -> Dict[str, Any]:
        try:
            q = self._qualified_table(table, schema)
            c = SQLIdentifierSanitizer.sanitize(column)
            mapping = {'hour': 'HOUR', 'day': 'DAY', 'month': 'MONTH', 'year': 'YEAR', 'dow': 'DOW', 'quarter': 'QUARTER'}
            sql_unit = mapping.get(unit.lower(), 'DAY')
            rows = await self._fetch(f'SELECT EXTRACT({sql_unit} FROM "{c}") as time_unit, COUNT(*) as cnt FROM {q} WHERE "{c}" IS NOT NULL GROUP BY time_unit ORDER BY time_unit')
            result = {r['time_unit']: r['cnt'] for r in rows}
            msg = f"Counts by {unit} for '{column}': {len(result)} unique values"
            return self._success_response(msg, [column], result=result)
        except Exception as e:
            return self._error_response(str(e), [column])

    async def datetime_weekday_weekend_counts(self, table: str, schema: str, column: str) -> Dict[str, Any]:
        try:
            q = self._qualified_table(table, schema)
            c = SQLIdentifierSanitizer.sanitize(column)
            bucket_expr = f'''CASE WHEN EXTRACT(DOW FROM "{c}") IN (0,6) THEN 'weekend' ELSE 'weekday' END'''
            rows = await self._fetch(f'SELECT {bucket_expr} AS type, COUNT(*) as cnt FROM {q} WHERE "{c}" IS NOT NULL GROUP BY {bucket_expr}')
            counts = {r['type']: r['cnt'] for r in rows}
            wday = counts.get('weekday', 0)
            wend = counts.get('weekend', 0)
            total = wday + wend
            if total > 0:
                msg = f"Weekday/weekend for '{column}': weekdays={wday} ({wday / total * 100:.1f}%), weekends={wend} ({wend / total * 100:.1f}%)"
            else:
                msg = f"No data for '{column}'"
            return self._success_response(msg, [column], result={'weekday': wday, 'weekend': wend})
        except Exception as e:
            return self._error_response(str(e), [column])

    async def datetime_holiday_counts(self, table: str, schema: str, column: str) -> Dict[str, Any]:
        try:
            q = self._qualified_table(table, schema)
            c = SQLIdentifierSanitizer.sanitize(column)
            rows = await self._fetch(f'''SELECT CASE WHEN EXTRACT(MONTH FROM "{c}") = 1 AND EXTRACT(DAY FROM "{c}") = 1 THEN 'New Year' WHEN EXTRACT(MONTH FROM "{c}") = 12 AND EXTRACT(DAY FROM "{c}") = 25 THEN 'Christmas' ELSE 'non-holiday' END AS holiday, COUNT(*) as cnt FROM {q} WHERE "{c}" IS NOT NULL GROUP BY holiday''')
            counts = {r['holiday']: r['cnt'] for r in rows}
            total_holidays = sum((v for k, v in counts.items() if k != 'non-holiday'))
            msg = f"Holiday counts for '{column}': total holidays={total_holidays}, New Year={counts.get('New Year', 0)}, Christmas={counts.get('Christmas', 0)}"
            return self._success_response(msg, [column], result=counts)
        except Exception as e:
            return self._error_response(str(e), [column])
