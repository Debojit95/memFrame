from .base import DatetimeOps


class PostgresDatetimeOps(DatetimeOps):
    """Postgres backend — small dialect hooks over the DuckDB-flavoured base."""

    def _day_name_expr(self, base_expr: str) -> str:
        return f"TO_CHAR({base_expr}, 'FMDay')"

    def _month_name_expr(self, base_expr: str) -> str:
        return f"TO_CHAR({base_expr}, 'FMMonth')"

    def _coerce_datetime_expr(self, safe_col: str) -> str:
        # ponytail: Postgres has no TRY_CAST; ISO-like guard, best-effort.
        return f"""CASE WHEN TRIM("{safe_col}"::TEXT) ~ '^[0-9]{{4}}-[0-9]{{2}}-[0-9]{{2}}' THEN CAST("{safe_col}" AS TIMESTAMP) ELSE NULL END"""

    def _epoch_expr(self, safe_col: str) -> str:
        return f'EXTRACT(EPOCH FROM "{safe_col}")'

    def _strftime_expr(self, base_expr: str, fmt: str) -> str:
        sql_fmt = self._convert_strftime_format(fmt)
        return f"TO_CHAR({base_expr}, '{sql_fmt}')"

    def _strptime_expr(self, safe_col: str, fmt: str) -> str:
        sql_fmt = self._convert_strftime_format(fmt)
        return f"TO_TIMESTAMP(\"{safe_col}\", '{sql_fmt}')"

    def _median_agg_expr(self, qcol: str) -> str:
        # ponytail: Postgres has no MEDIAN() aggregate.
        return f"percentile_cont(0.5) WITHIN GROUP (ORDER BY \"{qcol}\")"

    def _asfreq_grid(self, lo_s: str, hi_s: str, unit: str) -> str:
        # ponytail: Postgres generate_series is set-returning natively;
        # UNNEST() takes arrays only, so the bare form lives here.
        # Grid starts at the truncated min so buckets align to midnights.
        return (
            f"(SELECT generate_series(DATE_TRUNC('{unit}', CAST({self._dt_literal(lo_s)} AS TIMESTAMP)), "
            f"CAST({self._dt_literal(hi_s)} AS TIMESTAMP), INTERVAL '1 {unit}') AS bucket)")

    def _convert_strftime_format(self, fmt: str) -> str:
        """Convert Python strftime format → SQL format (Postgres compatible)"""
        mapping = {
            "%Y": "YYYY",
            "%m": "MM",
            "%d": "DD",
            "%H": "HH24",
            "%I": "HH12",
            "%M": "MI",
            "%S": "SS",
            "%f": "US",
        }
        for py, sql in mapping.items():
            fmt = fmt.replace(py, sql)
        return fmt
