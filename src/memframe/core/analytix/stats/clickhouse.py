
from typing import Any, Dict, List, Optional

from memframe.core.analytix.stats.base import DataStatsOps
from memframe.utils.helper import SQLIdentifierSanitizer


class ClickHouseDataStatsOps(DataStatsOps):
    """ClickHouse backend.

    Dialect hooks (quantile()/toFloat64/toUnixTimestamp, stddevPop-style
    aggregate names, lagInFrame, batch-capped streamed matrices, concat-based
    crosstombs) plus explicit overrides for the operations whose SQL strategy
    genuinely differs (stats-in-CTE skew/kurtosis/entropy, CH date functions,
    parseDateTimeBestEffort casts, lagInFrame window frames).
    """

    def _median_expr(self, c: str) -> str:
        return f'quantile(0.5)({c})'

    def _quantile_expr(self, c: str, qt) -> str:
        return f'quantile({qt})({c})'

    def _double_cast(self, c: str) -> str:
        return f'toFloat64("{c}")'

    def _text_to_double(self, c: str) -> str:
        return f'toFloat64OrNull(toString("{c}"))'

    def _fn_name(self, name: str) -> str:
        return {
            "STDDEV_POP": "stddevPop", "VAR_POP": "varPop",
            "STDDEV_SAMP": "stddevSamp", "SQRT": "sqrt", "AVG": "avg",
            "ABS": "abs", "SUM": "sum", "LN": "log", "EXP": "exp", "CORR": "corr", "COUNT": "count",
        }.get(name, name)

    def _epoch_expr(self, c: str) -> str:
        return f'toUnixTimestamp({c})'

    def _from_epoch(self, expr: str) -> str:
        return f'toDateTime({expr})'

    def _date_from_epoch(self, expr: str) -> str:
        return expr.replace("toDateTime(", "toDate(", 1)

    def _lag_fn(self, c: str, lag) -> str:
        return f'lagInFrame({c}, {lag})'

    def _assoc_batch_cap(self, n_pairs: int) -> int:
        return min(n_pairs, 250)

    def _concat_pipe(self, quoted_cols) -> str:
        return "concat(" + ", '|', ".join(quoted_cols) + ")"

    # ------------------------------------------------------------------
    # CH-specific stats strategies
    # ------------------------------------------------------------------
    async def numeric_skew(self, table: str, schema: str, column: str) -> Dict[str, Any]:
        try:
            q = self._qualified_table(table, schema)
            c = SQLIdentifierSanitizer.sanitize(column)
            sql = f'\n                    WITH stats AS (\n                        SELECT\n                            avg("{c}") AS mean_val,\n                            stddevSamp("{c}") AS std_val,\n                            count() AS n\n                        FROM {q}\n                        WHERE "{c}" IS NOT NULL\n                    ),\n                    deviations AS (\n                        SELECT sum(power("{c}" - (SELECT mean_val FROM stats), 3)) AS sum3\n                        FROM {q}\n                        WHERE "{c}" IS NOT NULL\n                    )\n                    SELECT\n                        CASE\n                            WHEN (SELECT n FROM stats) < 3\n                              OR (SELECT std_val FROM stats) = 0\n                              OR (SELECT std_val FROM stats) IS NULL THEN NULL\n                            ELSE ((SELECT n FROM stats) / ((SELECT n FROM stats) - 1)\n                                   / ((SELECT n FROM stats) - 2))\n                                 * (SELECT sum3 FROM deviations)\n                                 / power((SELECT std_val FROM stats), 3)\n                        END\n                '
            val = await self._fetchval(sql)
            msg = f"Skewness of '{column}': {val:.4f}" if val is not None else 'N/A'
            return self._success_response(msg, result=val)
        except Exception as e:
            return self._error_response(str(e), [column])

    async def numeric_kurtosis(self, table: str, schema: str, column: str) -> Dict[str, Any]:
        try:
            q = self._qualified_table(table, schema)
            c = SQLIdentifierSanitizer.sanitize(column)
            sql = f'\n                    WITH stats AS (\n                        SELECT\n                            avg("{c}") AS mean_val,\n                            stddevSamp("{c}") AS std_val,\n                            count() AS n\n                        FROM {q}\n                        WHERE "{c}" IS NOT NULL\n                    ),\n                    deviations AS (\n                        SELECT sum(power("{c}" - (SELECT mean_val FROM stats), 4)) AS sum4\n                        FROM {q}\n                        WHERE "{c}" IS NOT NULL\n                    )\n                    SELECT\n                        CASE\n                            WHEN (SELECT n FROM stats) < 4\n                              OR (SELECT std_val FROM stats) = 0\n                              OR (SELECT std_val FROM stats) IS NULL THEN NULL\n                            ELSE ((SELECT n FROM stats) * ((SELECT n FROM stats) + 1))\n                                 / ((SELECT n FROM stats) - 1)\n                                 / ((SELECT n FROM stats) - 2)\n                                 / ((SELECT n FROM stats) - 3)\n                                 * (SELECT sum4 FROM deviations)\n                                 / power((SELECT std_val FROM stats), 4)\n                                 - (3.0 * power((SELECT n FROM stats) - 1, 2))\n                                   / (((SELECT n FROM stats) - 2)\n                                      * ((SELECT n FROM stats) - 3))\n                        END\n                '
            val = await self._fetchval(sql)
            msg = f"Kurtosis of '{column}': {val:.4f}" if val is not None else 'N/A'
            return self._success_response(msg, result=val)
        except Exception as e:
            return self._error_response(str(e), [column])

    async def numeric_entropy(self, table: str, schema: str, column: str) -> Dict[str, Any]:
        try:
            q = self._qualified_table(table, schema)
            c = SQLIdentifierSanitizer.sanitize(column)
            sql = f'\n                    WITH counts AS (SELECT "{c}", COUNT(*) AS cnt FROM {q} WHERE "{c}" IS NOT NULL GROUP BY "{c}"),\n                         total AS (SELECT SUM(cnt) AS tot FROM counts)\n                    SELECT -sum((1.0 * cnt / tot) * log((1.0 * cnt / tot))) FROM counts, total\n                '
            val = await self._fetchval(sql)
            msg = f"Entropy of '{column}': {val:.4f}" if val is not None else 'N/A'
            return self._success_response(msg, result=val)
        except Exception as e:
            return self._error_response(str(e), [column])

    async def datetime_diff(self, table: str, schema: str, column: str, backend=None, data_id: Optional[str]=None, new_table: Optional[str]=None, target_col: Optional[str]=None) -> Dict[str, Any]:
        try:
            q = self._qualified_table(table, schema)
            c = SQLIdentifierSanitizer.sanitize(column)
            e = f'toUnixTimestamp(CAST("{c}" AS DateTime))'
            diff_expr = f'{e} - lagInFrame({e}) OVER (ORDER BY CAST("{c}" AS DateTime))'
            tgt = SQLIdentifierSanitizer.sanitize(target_col) if target_col else f'{c}__diff_seconds'
            out = await self._materialize_query_as_table(f'SELECT *, ({diff_expr}) AS "{tgt}" FROM {q}', table, schema, backend=backend, data_id=data_id, new_table=new_table)
            await self._add_column_if_not_exists(table, schema, tgt, 'DOUBLE PRECISION')
            orig_q = self._qualified_table(table, schema)
            try:
                orig_new = await self._materialize_query_as_table(f'SELECT *, ({diff_expr}) AS "{tgt}" FROM {q}', table, schema, backend=backend, data_id=data_id, new_table=f'{table}__tmp_mirror')
                await self._exec(f'DROP TABLE {orig_q}')
                new_qualified = self._qualified_table(orig_new, schema)
                await self._exec(f'RENAME TABLE {new_qualified} TO {orig_q}')
            except Exception:
                pass
            sample = await self._fetch_data(out, schema, [column, tgt])
            msg = f"Time differences for '{column}' → '{tgt}'"
            return self._success_response(msg, [column], [tgt], sample, new_table=out)
        except Exception as e:
            return self._error_response(str(e), [column])

    async def datetime_delta_stats(self, table: str, schema: str, column: str) -> Dict[str, Any]:
        try:
            q = self._qualified_table(table, schema)
            c = SQLIdentifierSanitizer.sanitize(column)
            diff_expr = f'toUnixTimestamp(b."{c}") - toUnixTimestamp(a."{c}")'
            median_expr = 'quantile(0.5)(d)'
            sql = f'\n                    WITH ordered AS (\n                        SELECT "{c}", ROW_NUMBER() OVER (ORDER BY "{c}") AS rn\n                        FROM {q} WHERE "{c}" IS NOT NULL\n                    ), diffs AS (\n                        SELECT ({diff_expr}) AS d\n                        FROM ordered a JOIN ordered b ON a.rn + 1 = b.rn\n                    )\n                    SELECT COUNT(d) AS cnt, MIN(d) AS min_d, MAX(d) AS max_d,\n                           AVG(d) AS avg_d, {median_expr} AS median_d,\n                           stddevPop(d) AS std_d\n                    FROM diffs WHERE d IS NOT NULL\n                '
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
            row = await self._fetch(f'''SELECT COUNT(*) AS total, dateDiff('second', MIN(dt), MAX(dt)) AS seconds FROM (    SELECT parseDateTimeBestEffortOrNull(toString("{c}")) AS dt     FROM {q} WHERE "{c}" IS NOT NULL) WHERE dt IS NOT NULL''')
            if not row or not row[0]['total']:
                return self._success_response(f"No valid data for event rate in '{column}'", [column])
            total = row[0]['total']
            seconds = row[0]['seconds'] or 0
            units = {'second': 1, 'minute': 60, 'hour': 3600, 'day': 86400, 'week': 604800}
            if unit in units:
                rate = total / (seconds / units[unit]) if seconds > 0 else 0
            else:
                rate = total / (seconds / 86400) if seconds > 0 else 0
                unit = 'day'
            msg = f"Event rate for '{column}': {rate:.4f} per {unit}"
            return self._success_response(msg, [column], result=rate)
        except Exception as e:
            return self._error_response(str(e), [column])

    async def datetime_time_unit_counts(self, table: str, schema: str, column: str, unit: str='day') -> Dict[str, Any]:
        try:
            q = self._qualified_table(table, schema)
            c = SQLIdentifierSanitizer.sanitize(column)
            mapping = {'hour': 'toHour', 'day': 'toDayOfMonth', 'month': 'toMonth', 'year': 'toYear', 'dow': 'toDayOfWeek', 'quarter': 'toQuarter'}
            sql_unit = mapping.get(unit.lower(), 'toDayOfMonth')
            unit_expr = f'{sql_unit}(dt)'
            rows = await self._fetch(f'SELECT {unit_expr} as time_unit, COUNT(*) as cnt FROM (    SELECT parseDateTimeBestEffortOrNull(toString("{c}")) AS dt     FROM {q} WHERE "{c}" IS NOT NULL) WHERE dt IS NOT NULL GROUP BY {unit_expr} ORDER BY time_unit')
            result = {r['time_unit']: r['cnt'] for r in rows}
            msg = f"Counts by {unit} for '{column}': {len(result)} unique values"
            return self._success_response(msg, [column], result=result)
        except Exception as e:
            return self._error_response(str(e), [column])

    async def datetime_weekday_weekend_counts(self, table: str, schema: str, column: str) -> Dict[str, Any]:
        try:
            q = self._qualified_table(table, schema)
            c = SQLIdentifierSanitizer.sanitize(column)
            bucket_expr = "CASE WHEN toDayOfWeek(dt) IN (6,7) THEN 'weekend' ELSE 'weekday' END"
            rows = await self._fetch(f'SELECT {bucket_expr} AS type, COUNT(*) as cnt FROM (    SELECT parseDateTimeBestEffortOrNull(toString("{c}")) AS dt     FROM {q} WHERE "{c}" IS NOT NULL) WHERE dt IS NOT NULL GROUP BY {bucket_expr}')
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
            holiday_expr = "CASE WHEN toMonth(dt) = 1 AND toDayOfMonth(dt) = 1 THEN 'New Year' WHEN toMonth(dt) = 12 AND toDayOfMonth(dt) = 25 THEN 'Christmas' ELSE 'non-holiday' END"
            rows = await self._fetch(f'SELECT {holiday_expr} AS holiday, COUNT(*) as cnt FROM (    SELECT parseDateTimeBestEffortOrNull(toString("{c}")) AS dt     FROM {q} WHERE "{c}" IS NOT NULL) WHERE dt IS NOT NULL GROUP BY {holiday_expr}')
            counts = {r['holiday']: r['cnt'] for r in rows}
            total_holidays = sum((v for k, v in counts.items() if k != 'non-holiday'))
            msg = f"Holiday counts for '{column}': total holidays={total_holidays}, New Year={counts.get('New Year', 0)}, Christmas={counts.get('Christmas', 0)}"
            return self._success_response(msg, [column], result=counts)
        except Exception as e:
            return self._error_response(str(e), [column])

    async def _add_column_if_not_exists(self, table: str, schema: str, column: str, data_type: str='DOUBLE PRECISION') -> None:
        qualified = self._qualified_table(table, schema)
        safe_col = SQLIdentifierSanitizer.sanitize(column)
        if True and data_type == 'DOUBLE PRECISION':
            data_type = 'Float64'
        atype = 'UInt8' if True and data_type == 'INTEGER' else data_type
        try:
            await self._exec(f'SELECT {self.db.quote_identifier(safe_col)} FROM {qualified} LIMIT 1')
        except Exception:
            await self._exec(f'ALTER TABLE {qualified} ADD COLUMN {self.db.quote_identifier(safe_col)} {atype}')

    async def numeric_multi_column_correlation(self, table: str, schema: str, columns: List[str], backend=None, data_id: Optional[str]=None, new_table: Optional[str]=None) -> Dict[str, Any]:
        agg = 'corr'
        return await self._multi_column_assoc_matrix(table, schema, columns, agg, backend=backend, data_id=data_id, new_table=new_table)

    async def numeric_multi_column_covariance(self, table: str, schema: str, columns: List[str], backend=None, data_id: Optional[str]=None, new_table: Optional[str]=None) -> Dict[str, Any]:
        agg = 'covarSamp'
        return await self._multi_column_assoc_matrix(table, schema, columns, agg, backend=backend, data_id=data_id, new_table=new_table)
