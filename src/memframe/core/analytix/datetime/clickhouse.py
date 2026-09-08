from typing import Any, Dict, Optional
import traceback

from memframe.utils.helper import SQLIdentifierSanitizer
from .base import DatetimeOps, DT_FIELD_MAP

_CH_FIELD_MAP = {
    "year": "toYear", "month": "toMonth", "day": "toDayOfMonth",
    "hour": "toHour", "minute": "toMinute", "second": "toSecond",
    "dayofweek": "toDayOfWeek", "dow": "toDayOfWeek",
    "dayofyear": "toDayOfYear", "doy": "toDayOfYear",
    "week": "toISOWeek", "weekofyear": "toISOWeek",
    "quarter": "toQuarter",
}

_CH_FLOOR_MAP = {
    "year": "toStartOfYear", "quarter": "toStartOfQuarter", "month": "toStartOfMonth",
    "week": "toMonday", "day": "toStartOfDay", "hour": "toStartOfHour",
    "minute": "toStartOfMinute", "second": "toStartOfSecond",
}

_CH_REPLACE_FUNC_MAP = {
    "year": "toYear", "month": "toMonth", "day": "toDayOfMonth",
    "hour": "toHour", "minute": "toMinute", "second": "toSecond",
}


class ClickHouseDatetimeOps(DatetimeOps):
    """
    ClickHouse backend — wholesale overrides wherever the SQL strategy
    genuinely differs (ALTER TABLE mutations, to* function family).
    """

    async def _datetime_base_expr(self, working_table: str, schema: str, column: str) -> str:
        """Raw col if date-like else CAST to DateTime."""
        safe_col = SQLIdentifierSanitizer.sanitize(column)
        col_type = await self._get_column_type(working_table, schema, column)
        if "timestamp" in col_type.lower() or "date" in col_type.lower() or "datetime" in col_type.lower():
            return f'"{safe_col}"'
        return f'CAST("{safe_col}" AS DateTime)'

    def _resample_interval(self, unit: str) -> str:
        return f"INTERVAL 1 {unit}"

    # ==================================================================
    #  DATETIME EXTRACTORS
    # ==================================================================
    async def extract(self, table: str, schema: str, column: str, field: str,
                      backend=None, data_id: Optional[str] = None,
                      new_table: Optional[str] = None) -> Dict[str, Any]:
        try:
            field = field.lower()
            if field not in DT_FIELD_MAP:
                return self._error_response(f"Unsupported datetime field: {field}", [column], [])

            working_table = await self._prepare_operation_table(
                table, schema, backend=backend, data_id=data_id, new_table=new_table
            )

            sql_func = _CH_FIELD_MAP[field]
            new_col = self._generate_cleaned_column_name(column, field)
            await self._add_new_column(working_table, schema, new_col, "INTEGER")

            qualified = self._qualified_table(working_table, schema)
            safe_col = SQLIdentifierSanitizer.sanitize(column)
            safe_new = SQLIdentifierSanitizer.sanitize(new_col)

            # Explicitly cast to DateTime to prevent Illegal type String errors
            base_expr = await self._datetime_base_expr(working_table, schema, column)

            # ponytail: EXTRACT(DOW) on DuckDB/Postgres is 0=Sunday..6=Saturday;
            # toDayOfWeek is 1=Monday..7=Sunday, so wrap with % 7 for parity.
            if field in ("dayofweek", "dow"):
                value_expr = f"toDayOfWeek({base_expr}) % 7"
            else:
                value_expr = f"{sql_func}({base_expr})"

            await self._exec(f"""
                ALTER TABLE {qualified}
                UPDATE "{safe_new}" = {value_expr}
                WHERE 1 SETTINGS mutations_sync = 1
            """)

            sample = await self._fetch_sample(working_table, schema, columns=[safe_col, safe_new])
            msg = f"Extracted {field} from '{column}' → '{new_col}'"
            return self._success_response(msg, [column], [new_col], sample,
                                        extract_field=field, new_table=working_table)

        except Exception as e:
            return self._error_response(f"datetime extract error: {str(e)}\n{traceback.format_exc()}", [column], [])

    # ==================================================================
    #  CEIL, ROUND, FLOOR
    # ==================================================================
    async def ceil(self, table: str, schema: str, column: str, unit: str,
                   backend=None, data_id=None, new_table=None) -> Dict[str, Any]:
        try:
            unit = unit.lower()
            interval_map = {
                "year": "1 year", "quarter": "3 month", "month": "1 month",
                "week": "1 week", "day": "1 day", "hour": "1 hour",
                "minute": "1 minute", "second": "1 second",
            }
            if unit not in interval_map:
                return self._error_response(f"Unsupported ceil unit: {unit}", [column], [])

            ch_interval_map = {
                "year": "1 YEAR", "quarter": "1 QUARTER", "month": "1 MONTH",
                "week": "1 WEEK", "day": "1 DAY", "hour": "1 HOUR",
                "minute": "1 MINUTE", "second": "1 SECOND"
            }
            working_table = await self._prepare_operation_table(
                table, schema, backend=backend, data_id=data_id, new_table=new_table
            )

            new_col = self._generate_cleaned_column_name(column, f"ceil_{unit}")
            await self._add_new_column(working_table, schema, new_col, "TIMESTAMP")

            qualified = self._qualified_table(working_table, schema)
            safe_col = SQLIdentifierSanitizer.sanitize(column)
            safe_new = SQLIdentifierSanitizer.sanitize(new_col)
            floor_fn = _CH_FLOOR_MAP[unit]
            interval = ch_interval_map[unit]

            base_expr = await self._datetime_base_expr(working_table, schema, column)

            await self._exec(f"""
                ALTER TABLE {qualified}
                UPDATE "{safe_new}" =
                    IF({base_expr} = {floor_fn}({base_expr}),
                       {base_expr},
                       {floor_fn}({base_expr} + INTERVAL {interval}))
                WHERE 1 SETTINGS mutations_sync = 1
            """)

            sample = await self._fetch_sample(working_table, schema, columns=[safe_col, safe_new])
            msg = f"Ceiled '{column}' to {unit}"
            return self._success_response(msg, [column], [new_col], sample, unit=unit,
                                        new_table=working_table)

        except Exception as e:
            return self._error_response(f"ceil error: {str(e)}", [column], [])

    async def round(self, table: str, schema: str, column: str, unit: str,
                    backend=None, data_id=None, new_table=None) -> Dict[str, Any]:
        try:
            unit = unit.lower()
            half_interval_map = {
                "year": "6 month", "quarter": "1.5 month", "month": "15 day",
                "week": "3.5 day", "day": "12 hour", "hour": "30 minute",
                "minute": "30 second", "second": "0.5 second",
            }
            if unit not in half_interval_map:
                return self._error_response(f"Unsupported round unit: {unit}", [column], [])

            ch_half_interval_map = {
                "year": "6 MONTH",
                "quarter": "1 MONTH + INTERVAL 15 DAY",
                "month": "15 DAY",
                "week": "3 DAY + INTERVAL 12 HOUR",
                "day": "12 HOUR",
                "hour": "30 MINUTE",
                "minute": "30 SECOND",
                "second": "0 SECOND"
            }
            working_table = await self._prepare_operation_table(
                table, schema, backend=backend, data_id=data_id, new_table=new_table
            )

            new_col = self._generate_cleaned_column_name(column, f"round_{unit}")
            await self._add_new_column(working_table, schema, new_col, "TIMESTAMP")

            qualified = self._qualified_table(working_table, schema)
            safe_col = SQLIdentifierSanitizer.sanitize(column)
            safe_new = SQLIdentifierSanitizer.sanitize(new_col)
            floor_fn = _CH_FLOOR_MAP[unit]
            half_interval = ch_half_interval_map[unit]

            base_expr = await self._datetime_base_expr(working_table, schema, column)

            await self._exec(f"""
                ALTER TABLE {qualified}
                UPDATE "{safe_new}" = {floor_fn}({base_expr} + INTERVAL {half_interval})
                WHERE 1 SETTINGS mutations_sync = 1
            """)

            sample = await self._fetch_sample(working_table, schema, columns=[safe_col, safe_new])
            msg = f"Rounded '{column}' to {unit}"
            return self._success_response(msg, [column], [new_col], sample, unit=unit,
                                        new_table=working_table)

        except Exception as e:
            return self._error_response(f"round error: {str(e)}", [column], [])

    async def floor(self, table: str, schema: str, column: str, unit: str,
                    backend=None, data_id=None, new_table=None) -> Dict[str, Any]:
        try:
            unit = unit.lower()
            valid_units = {"year", "quarter", "month", "week", "day", "hour", "minute", "second"}
            if unit not in valid_units:
                return self._error_response(f"Unsupported floor unit: {unit}", [column], [])

            working_table = await self._prepare_operation_table(
                table, schema, backend=backend, data_id=data_id, new_table=new_table
            )

            new_col = self._generate_cleaned_column_name(column, f"floor_{unit}")
            await self._add_new_column(working_table, schema, new_col, "TIMESTAMP")

            qualified = self._qualified_table(working_table, schema)
            safe_col = SQLIdentifierSanitizer.sanitize(column)
            safe_new = SQLIdentifierSanitizer.sanitize(new_col)
            floor_fn = _CH_FLOOR_MAP[unit]

            base_expr = await self._datetime_base_expr(working_table, schema, column)

            await self._exec(f"""
                ALTER TABLE {qualified}
                UPDATE "{safe_new}" = {floor_fn}({base_expr})
                WHERE 1 SETTINGS mutations_sync = 1
            """)

            sample = await self._fetch_sample(working_table, schema, columns=[safe_col, safe_new])
            msg = f"Floored '{column}' to {unit}"
            return self._success_response(msg, [column], [new_col], sample, unit=unit,
                                        new_table=working_table)

        except Exception as e:
            return self._error_response(f"floor error: {str(e)}", [column], [])

    # ==================================================================
    #  TIMEZONE OPERATIONS
    # ==================================================================
    async def tz_localize(self, table: str, schema: str, column: str, tz: str | None,
                          ambiguous: str = "raise", nonexistent: str = "raise",
                          backend=None, data_id=None, new_table=None) -> Dict[str, Any]:
        try:
            working_table = await self._prepare_operation_table(
                table, schema, backend=backend, data_id=data_id, new_table=new_table
            )

            new_col = self._generate_cleaned_column_name(column, f"tz_{tz or 'naive'}")
            await self._add_new_column(working_table, schema, new_col, "TIMESTAMP")

            qualified = self._qualified_table(working_table, schema)
            safe_col = SQLIdentifierSanitizer.sanitize(column)
            safe_new = SQLIdentifierSanitizer.sanitize(new_col)

            warnings = []
            if ambiguous != "raise":
                warnings.append(f"ambiguous='{ambiguous}' not fully supported in SQL engines")
            if nonexistent != "raise":
                warnings.append(f"nonexistent='{nonexistent}' not fully supported in SQL engines")

            base_expr = await self._datetime_base_expr(working_table, schema, column)

            if tz is None:
                await self._exec(f"""
                    ALTER TABLE {qualified}
                    UPDATE "{safe_new}" = toTimezone({base_expr}, 'UTC')
                    WHERE 1 SETTINGS mutations_sync = 1
                """)
                msg = f"Removed timezone from '{column}' (Defaulted to UTC)"
            else:
                await self._exec(f"""
                    ALTER TABLE {qualified}
                    UPDATE "{safe_new}" = toTimezone({base_expr}, '{tz}')
                    WHERE 1 SETTINGS mutations_sync = 1
                """)
                msg = f"Localized '{column}' to timezone '{tz}'"

            sample = await self._fetch_sample(working_table, schema, columns=[safe_col, safe_new])
            return self._success_response(msg, [column], [new_col], sample, timezone=tz,
                                        warnings=warnings, new_table=working_table)

        except Exception as e:
            return self._error_response(f"tz_localize error: {str(e)}\n{traceback.format_exc()}", [column], [])

    async def tz_convert(self, table: str, schema: str, column: str, tz: str | None,
                         backend=None, data_id=None, new_table=None) -> Dict[str, Any]:
        try:
            working_table = await self._prepare_operation_table(
                table, schema, backend=backend, data_id=data_id, new_table=new_table
            )

            new_col = self._generate_cleaned_column_name(column, f"tzconvert_{tz or 'naive'}")
            await self._add_new_column(working_table, schema, new_col, "TIMESTAMP")

            qualified = self._qualified_table(working_table, schema)
            safe_col = SQLIdentifierSanitizer.sanitize(column)
            safe_new = SQLIdentifierSanitizer.sanitize(new_col)

            base_expr = await self._datetime_base_expr(working_table, schema, column)

            if tz is None:
                await self._exec(f"""
                    ALTER TABLE {qualified}
                    UPDATE "{safe_new}" = toTimezone({base_expr}, 'UTC')
                    WHERE 1 SETTINGS mutations_sync = 1
                """)
                msg = f"Converted '{column}' to UTC and removed timezone"
            else:
                # ponytail: materialize the target wall time as naive DateTime;
                # storing the tz-aware instant in a plain DateTime column would
                # re-render it in the server timezone instead.
                await self._exec(f"""
                    ALTER TABLE {qualified}
                    UPDATE "{safe_new}" = CAST(toString(toTimezone(toTimezone({base_expr}, 'UTC'), '{tz}')) AS DateTime)
                    WHERE 1 SETTINGS mutations_sync = 1
                """)
                msg = f"Converted '{column}' timezone to '{tz}'"

            sample = await self._fetch_sample(working_table, schema, columns=[safe_col, safe_new])
            return self._success_response(msg, [column], [new_col], sample, timezone=tz,
                                        new_table=working_table)

        except Exception as e:
            return self._error_response(f"tz_convert error: {str(e)}\n{traceback.format_exc()}", [column], [])

    # ==================================================================
    #  BOOLEAN CHECKS
    # ==================================================================
    async def is_month_start(self, table: str, schema: str, column: str,
                             backend=None, data_id=None, new_table=None) -> Dict[str, Any]:
        try:
            working_table = await self._prepare_operation_table(
                table, schema, backend=backend, data_id=data_id, new_table=new_table
            )
            new_col = self._generate_cleaned_column_name(column, "is_month_start")
            await self._add_new_column(working_table, schema, new_col, "BOOLEAN")

            q = self._qualified_table(working_table, schema)
            c = SQLIdentifierSanitizer.sanitize(column)
            n = SQLIdentifierSanitizer.sanitize(new_col)

            base_expr = await self._datetime_base_expr(working_table, schema, column)

            await self._exec(f"""ALTER TABLE {q} UPDATE "{n}" = toDayOfMonth({base_expr}) = 1 WHERE 1 SETTINGS mutations_sync = 1""")
            sample = await self._fetch_sample(working_table, schema, [c, n])
            return self._success_response("Computed is_month_start", [column], [new_col], sample,
                                        new_table=working_table)

        except Exception as e:
            return self._error_response(str(e), [column])

    async def is_month_end(self, table: str, schema: str, column: str,
                           backend=None, data_id=None, new_table=None) -> Dict[str, Any]:
        try:
            working_table = await self._prepare_operation_table(
                table, schema, backend=backend, data_id=data_id, new_table=new_table
            )
            new_col = self._generate_cleaned_column_name(column, "is_month_end")
            await self._add_new_column(working_table, schema, new_col, "BOOLEAN")

            q = self._qualified_table(working_table, schema)
            c = SQLIdentifierSanitizer.sanitize(column)
            n = SQLIdentifierSanitizer.sanitize(new_col)

            base_expr = await self._datetime_base_expr(working_table, schema, column)

            await self._exec(f"""
                ALTER TABLE {q}
                UPDATE "{n}" = toDayOfMonth(toLastDayOfMonth({base_expr})) = toDayOfMonth({base_expr})
                WHERE 1 SETTINGS mutations_sync = 1
            """)
            sample = await self._fetch_sample(working_table, schema, [c, n])
            return self._success_response("Computed is_month_end", [column], [new_col], sample,
                                        new_table=working_table)

        except Exception as e:
            return self._error_response(str(e), [column])

    async def is_year_start(self, table: str, schema: str, column: str,
                            backend=None, data_id=None, new_table=None) -> Dict[str, Any]:
        try:
            working_table = await self._prepare_operation_table(
                table, schema, backend=backend, data_id=data_id, new_table=new_table
            )
            new_col = self._generate_cleaned_column_name(column, "is_year_start")
            await self._add_new_column(working_table, schema, new_col, "BOOLEAN")

            q = self._qualified_table(working_table, schema)
            c = SQLIdentifierSanitizer.sanitize(column)
            n = SQLIdentifierSanitizer.sanitize(new_col)

            base_expr = await self._datetime_base_expr(working_table, schema, column)

            await self._exec(f"""
                ALTER TABLE {q}
                UPDATE "{n}" = toMonth({base_expr}) = 1 AND toDayOfMonth({base_expr}) = 1
                WHERE 1 SETTINGS mutations_sync = 1
            """)
            sample = await self._fetch_sample(working_table, schema, [c, n])
            return self._success_response("Computed is_year_start", [column], [new_col], sample,
                                        new_table=working_table)

        except Exception as e:
            return self._error_response(str(e), [column])

    async def is_year_end(self, table: str, schema: str, column: str,
                          backend=None, data_id=None, new_table=None) -> Dict[str, Any]:
        try:
            working_table = await self._prepare_operation_table(
                table, schema, backend=backend, data_id=data_id, new_table=new_table
            )
            new_col = self._generate_cleaned_column_name(column, "is_year_end")
            await self._add_new_column(working_table, schema, new_col, "BOOLEAN")

            q = self._qualified_table(working_table, schema)
            c = SQLIdentifierSanitizer.sanitize(column)
            n = SQLIdentifierSanitizer.sanitize(new_col)

            base_expr = await self._datetime_base_expr(working_table, schema, column)

            await self._exec(f"""
                ALTER TABLE {q}
                UPDATE "{n}" = toMonth({base_expr}) = 12 AND toDayOfMonth({base_expr}) = 31
                WHERE 1 SETTINGS mutations_sync = 1
            """)
            sample = await self._fetch_sample(working_table, schema, [c, n])
            return self._success_response("Computed is_year_end", [column], [new_col], sample,
                                        new_table=working_table)

        except Exception as e:
            return self._error_response(str(e), [column])

    async def is_quarter_start(self, table: str, schema: str, column: str,
                               backend=None, data_id=None, new_table=None) -> Dict[str, Any]:
        try:
            working_table = await self._prepare_operation_table(
                table, schema, backend=backend, data_id=data_id, new_table=new_table
            )
            new_col = self._generate_cleaned_column_name(column, "is_quarter_start")
            await self._add_new_column(working_table, schema, new_col, "BOOLEAN")

            q = self._qualified_table(working_table, schema)
            c = SQLIdentifierSanitizer.sanitize(column)
            n = SQLIdentifierSanitizer.sanitize(new_col)

            base_expr = await self._datetime_base_expr(working_table, schema, column)

            await self._exec(f"""
                ALTER TABLE {q}
                UPDATE "{n}" = toStartOfQuarter({base_expr}) = toStartOfDay({base_expr})
                WHERE 1 SETTINGS mutations_sync = 1
            """)
            sample = await self._fetch_sample(working_table, schema, [c, n])
            return self._success_response("Computed is_quarter_start", [column], [new_col], sample,
                                        new_table=working_table)

        except Exception as e:
            return self._error_response(str(e), [column])

    async def is_quarter_end(self, table: str, schema: str, column: str,
                             backend=None, data_id=None, new_table=None) -> Dict[str, Any]:
        try:
            working_table = await self._prepare_operation_table(
                table, schema, backend=backend, data_id=data_id, new_table=new_table
            )
            new_col = self._generate_cleaned_column_name(column, "is_quarter_end")
            await self._add_new_column(working_table, schema, new_col, "BOOLEAN")

            q = self._qualified_table(working_table, schema)
            c = SQLIdentifierSanitizer.sanitize(column)
            n = SQLIdentifierSanitizer.sanitize(new_col)

            base_expr = await self._datetime_base_expr(working_table, schema, column)

            await self._exec(f"""
                ALTER TABLE {q}
                UPDATE "{n}" = toStartOfQuarter({base_expr} + INTERVAL 1 DAY) != toStartOfQuarter({base_expr})
                WHERE 1 SETTINGS mutations_sync = 1
            """)
            sample = await self._fetch_sample(working_table, schema, [c, n])
            return self._success_response("Computed is_quarter_end", [column], [new_col], sample,
                                        new_table=working_table)

        except Exception as e:
            return self._error_response(str(e), [column])

    async def is_weekend(self, table: str, schema: str, column: str,
                         backend=None, data_id=None, new_table=None) -> Dict[str, Any]:
        try:
            working_table = await self._prepare_operation_table(
                table, schema, backend=backend, data_id=data_id, new_table=new_table
            )
            new_col = self._generate_cleaned_column_name(column, "is_weekend")
            await self._add_new_column(working_table, schema, new_col, "BOOLEAN")

            q = self._qualified_table(working_table, schema)
            c = SQLIdentifierSanitizer.sanitize(column)
            n = SQLIdentifierSanitizer.sanitize(new_col)

            base_expr = await self._datetime_base_expr(working_table, schema, column)

            await self._exec(f"""
                ALTER TABLE {q}
                UPDATE "{n}" = toDayOfWeek({base_expr}) IN (6, 7)
                WHERE 1 SETTINGS mutations_sync = 1
            """)
            sample = await self._fetch_sample(working_table, schema, [c, n])
            return self._success_response("Computed is_weekend", [column], [new_col], sample,
                                        new_table=working_table)

        except Exception as e:
            return self._error_response(str(e), [column])

    async def is_weekday(self, table: str, schema: str, column: str,
                         backend=None, data_id=None, new_table=None) -> Dict[str, Any]:
        try:
            working_table = await self._prepare_operation_table(
                table, schema, backend=backend, data_id=data_id, new_table=new_table
            )
            new_col = self._generate_cleaned_column_name(column, "is_weekday")
            await self._add_new_column(working_table, schema, new_col, "BOOLEAN")

            q = self._qualified_table(working_table, schema)
            c = SQLIdentifierSanitizer.sanitize(column)
            n = SQLIdentifierSanitizer.sanitize(new_col)

            base_expr = await self._datetime_base_expr(working_table, schema, column)

            await self._exec(f"""
                ALTER TABLE {q}
                UPDATE "{n}" = toDayOfWeek({base_expr}) BETWEEN 1 AND 5
                WHERE 1 SETTINGS mutations_sync = 1
            """)
            sample = await self._fetch_sample(working_table, schema, [c, n])
            return self._success_response("Computed is_weekday", [column], [new_col], sample,
                                        new_table=working_table)

        except Exception as e:
            return self._error_response(str(e), [column])

    async def is_business_day(self, table: str, schema: str, column: str,
                              backend=None, data_id=None, new_table=None) -> Dict[str, Any]:
        try:
            working_table = await self._prepare_operation_table(
                table, schema, backend=backend, data_id=data_id, new_table=new_table
            )
            new_col = self._generate_cleaned_column_name(column, "is_business_day")
            await self._add_new_column(working_table, schema, new_col, "BOOLEAN")

            q = self._qualified_table(working_table, schema)
            c = SQLIdentifierSanitizer.sanitize(column)
            n = SQLIdentifierSanitizer.sanitize(new_col)

            base_expr = await self._datetime_base_expr(working_table, schema, column)

            await self._exec(f"""
                ALTER TABLE {q}
                UPDATE "{n}" = toDayOfWeek({base_expr}) BETWEEN 1 AND 5
                WHERE 1 SETTINGS mutations_sync = 1
            """)
            sample = await self._fetch_sample(working_table, schema, [c, n])
            return self._success_response("Computed is_business_day", [column], [new_col], sample,
                                        new_table=working_table)

        except Exception as e:
            return self._error_response(str(e), [column])

    async def days_in_month(self, table: str, schema: str, column: str,
                            backend=None, data_id=None, new_table=None) -> Dict[str, Any]:
        try:
            working_table = await self._prepare_operation_table(
                table, schema, backend=backend, data_id=data_id, new_table=new_table
            )
            new_col = self._generate_cleaned_column_name(column, "days_in_month")
            await self._add_new_column(working_table, schema, new_col, "INTEGER")

            q = self._qualified_table(working_table, schema)
            c = SQLIdentifierSanitizer.sanitize(column)
            n = SQLIdentifierSanitizer.sanitize(new_col)

            base_expr = await self._datetime_base_expr(working_table, schema, column)

            await self._exec(f"""
                ALTER TABLE {q}
                UPDATE "{n}" = toDayOfMonth(toLastDayOfMonth({base_expr}))
                WHERE 1 SETTINGS mutations_sync = 1
            """)
            sample = await self._fetch_sample(working_table, schema, [c, n])
            return self._success_response("Computed days_in_month", [column], [new_col], sample,
                                        new_table=working_table)

        except Exception as e:
            return self._error_response(str(e), [column])

    async def week_of_month(self, table: str, schema: str, column: str,
                            backend=None, data_id=None, new_table=None) -> Dict[str, Any]:
        try:
            working_table = await self._prepare_operation_table(
                table, schema, backend=backend, data_id=data_id, new_table=new_table
            )
            new_col = self._generate_cleaned_column_name(column, "week_of_month")
            await self._add_new_column(working_table, schema, new_col, "INTEGER")

            q = self._qualified_table(working_table, schema)
            c = SQLIdentifierSanitizer.sanitize(column)
            n = SQLIdentifierSanitizer.sanitize(new_col)

            base_expr = await self._datetime_base_expr(working_table, schema, column)

            await self._exec(f"""
                ALTER TABLE {q}
                UPDATE "{n}" = toWeek({base_expr}) - toWeek(toStartOfMonth({base_expr})) + 1
                WHERE 1 SETTINGS mutations_sync = 1
            """)
            sample = await self._fetch_sample(working_table, schema, [c, n])
            return self._success_response("Computed week_of_month", [column], [new_col], sample,
                                        new_table=working_table)

        except Exception as e:
            return self._error_response(str(e), [column])

    # ==================================================================
    #  WAVE 2 — resampling + frequency conversion
    # ==================================================================
    async def asfreq(self, table: str, schema: str, column: str, freq: str,
                     method: str = None, backend=None, data_id=None,
                     new_table: str = None) -> Dict[str, Any]:
        try:
            safe_table = SQLIdentifierSanitizer.sanitize(table)
            safe_col = SQLIdentifierSanitizer.sanitize(column)
            safe_schema = SQLIdentifierSanitizer.sanitize(schema)
            qualified = self._qualified_table(safe_table, safe_schema)

            column_types = await self.db.get_column_types(safe_table, safe_schema)
            if safe_col not in column_types and column not in column_types:
                return self._error_response(f"Column '{column}' not found", [column], [])
            dtype = str(column_types.get(safe_col, column_types.get(column, ""))).lower()
            if not any(t in dtype for t in ("date", "time", "timestamp")):
                return self._error_response(f"Column '{column}' must be datetime-like", [column], [])
            try:
                unit = self._parse_freq(freq)
            except ValueError as exc:
                return self._error_response(str(exc), [column], [])
            if method not in (None, "ffill", "bfill"):
                return self._error_response("method must be None, 'ffill' or 'bfill'", [column], [])

            bounds = await self._fetch(f'SELECT MIN("{safe_col}") AS lo, MAX("{safe_col}") AS hi FROM {qualified}')
            lo, hi = bounds[0]["lo"], bounds[0]["hi"]
            if lo is None or hi is None:
                return self._error_response(f"Column '{column}' has no non-null values", [column], [])
            lo_s, hi_s = str(lo), str(hi)

            dupes = await self._fetchval(
                f"SELECT COUNT(*) FROM (SELECT DATE_TRUNC('{unit}', \"{safe_col}\") AS b "
                f"FROM {qualified} GROUP BY b HAVING COUNT(*) > 1) t")
            if dupes:
                # ponytail: like pandas (which raises on non-unique index), refuse
                # rather than silently duplicating grid rows; resample first.
                return self._error_response(
                    f"Column '{column}' has multiple rows per '{freq}' bucket; resample first",
                    [column], [])

            other_cols = [c for c in column_types if c != safe_col and c != column]
            safe_others = [SQLIdentifierSanitizer.sanitize(c) for c in other_cols]

            grid_start = f"DATE_TRUNC('{unit}', CAST({self._dt_literal(lo_s)} AS DateTime))"
            n_row = await self._fetchval(
                f"SELECT dateDiff('{unit}', {grid_start}, CAST({self._dt_literal(hi_s)} AS DateTime)) + 1")
            n = int(n_row or 1)
            # ponytail: cap grid size; unbounded generate over years of seconds explodes.
            if n < 1 or n > 100000:
                return self._error_response(f"asfreq grid would hold {n} buckets (cap 100000)", [column], [])
            # ponytail: unit known at build time, so emit the proven
            # add<Unit> family instead of string-unit dateAdd (see above).
            add_fn = {"second": "addSeconds", "minute": "addMinutes",
                      "hour": "addHours", "day": "addDays", "week": "addWeeks",
                      "month": "addMonths", "quarter": "addQuarters",
                      "year": "addYears"}[unit]
            grid = f"(SELECT {add_fn}({grid_start}, number) AS bucket FROM numbers({n}))"
            bucket_match = f"DATE_TRUNC('{unit}', s.\"{safe_col}\") = g.bucket"

            if method is None:
                fill_items = [f's."{c}"' for c in safe_others]
            elif method == "ffill":
                # ponytail: CH cannot decorrelate ORDER BY+LIMIT subqueries;
                # IGNORE NULLS windows probed good on this server instead.
                fill_items = [
                    f"last_value(s.\"{c}\") IGNORE NULLS OVER (ORDER BY g.bucket "
                    f"ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS \"{c}\""
                    for c in safe_others]
            else:
                fill_items = [
                    f"first_value(s.\"{c}\") IGNORE NULLS OVER (ORDER BY g.bucket "
                    f"ROWS BETWEEN CURRENT ROW AND UNBOUNDED FOLLOWING) AS \"{c}\""
                    for c in safe_others]

            output_table = await self._resolve_output_table_name(
                safe_table, safe_schema, backend=backend, data_id=data_id, new_table=new_table)
            qualified_target = f'{self.db.quote_identifier(safe_schema)}.{self.db.quote_identifier(output_table)}'
            select_list = f'g.bucket AS "{safe_col}"' + ("".join(f", {item}" for item in fill_items) if fill_items else "")
            await self._exec(
                f"CREATE TABLE {qualified_target} AS "
                f"SELECT {select_list} "
                f"FROM {grid} g LEFT JOIN {qualified} s ON {bucket_match} "
                f"ORDER BY bucket")
            sample = await self._fetch_sample(output_table, safe_schema)
            return self._success_response(f"Converted '{column}' to frequency '{freq}'",
                                          [column], [safe_col], sample, freq=freq,
                                          method=method, new_table=output_table)

        except Exception as e:
            return self._error_response(f"asfreq error: {str(e)}\n{traceback.format_exc()}", [column], [])

    # ==================================================================
    #  WAVE 1 — names, durations, parsing, filtering
    # ==================================================================
    async def day_name(self, table: str, schema: str, column: str,
                       backend=None, data_id=None, new_table=None) -> Dict[str, Any]:
        try:
            working_table = await self._prepare_operation_table(
                table, schema, backend=backend, data_id=data_id, new_table=new_table
            )
            new_col = self._generate_cleaned_column_name(column, "day_name")
            await self._add_new_column(working_table, schema, new_col, "TEXT")

            qualified = self._qualified_table(working_table, schema)
            safe_col = SQLIdentifierSanitizer.sanitize(column)
            safe_new = SQLIdentifierSanitizer.sanitize(new_col)
            base_expr = await self._datetime_base_expr(working_table, schema, column)

            # ponytail: this build rejects formatDateTime %A; DATE_FORMAT %W probed good.
            await self._exec(f"""ALTER TABLE {qualified} UPDATE "{safe_new}" = DATE_FORMAT({base_expr}, '%W') WHERE 1 SETTINGS mutations_sync = 1""")
            sample = await self._fetch_sample(working_table, schema, [safe_col, safe_new])
            return self._success_response(f"Extracted day name from '{column}'",
                                          [column], [new_col], sample, new_table=working_table)

        except Exception as e:
            return self._error_response(f"day_name error: {str(e)}", [column], [])

    async def month_name(self, table: str, schema: str, column: str,
                         backend=None, data_id=None, new_table=None) -> Dict[str, Any]:
        try:
            working_table = await self._prepare_operation_table(
                table, schema, backend=backend, data_id=data_id, new_table=new_table
            )
            new_col = self._generate_cleaned_column_name(column, "month_name")
            await self._add_new_column(working_table, schema, new_col, "TEXT")

            qualified = self._qualified_table(working_table, schema)
            safe_col = SQLIdentifierSanitizer.sanitize(column)
            safe_new = SQLIdentifierSanitizer.sanitize(new_col)
            base_expr = await self._datetime_base_expr(working_table, schema, column)

            # ponytail: same %B rejection as day_name; DATE_FORMAT %M probed good.
            await self._exec(f"""ALTER TABLE {qualified} UPDATE "{safe_new}" = DATE_FORMAT({base_expr}, '%M') WHERE 1 SETTINGS mutations_sync = 1""")
            sample = await self._fetch_sample(working_table, schema, [safe_col, safe_new])
            return self._success_response(f"Extracted month name from '{column}'",
                                          [column], [new_col], sample, new_table=working_table)

        except Exception as e:
            return self._error_response(f"month_name error: {str(e)}", [column], [])

    _DIFF_CH_UNITS = {
        "millisecond": "millisecond", "second": "second", "minute": "minute",
        "hour": "hour", "day": "day", "week": "week",
        "month": "month", "quarter": "quarter", "year": "year",
    }

    async def diff(self, table: str, schema: str, col1: str, col2: str, unit: str = "day",
                   target_col: str = None, backend=None, data_id=None,
                   new_table: str = None) -> Dict[str, Any]:
        try:
            unit = unit.lower()
            if unit not in self._DIFF_DIVISORS:
                return self._error_response(f"Unsupported diff unit: {unit}", [col1, col2], [])

            working_table = await self._prepare_operation_table(
                table, schema, backend=backend, data_id=data_id, new_table=new_table
            )
            new_col = SQLIdentifierSanitizer.sanitize(target_col) if target_col else self._generate_cleaned_column_name(f"{col1}_{col2}", f"diff_{unit}")
            await self._add_new_column(working_table, schema, new_col, "DOUBLE PRECISION")

            qualified = self._qualified_table(working_table, schema)
            safe_new = SQLIdentifierSanitizer.sanitize(new_col)
            base1 = await self._datetime_base_expr(working_table, schema, col1)
            base2 = await self._datetime_base_expr(working_table, schema, col2)
            ch_unit = self._DIFF_CH_UNITS[unit]

            await self._exec(f"""ALTER TABLE {qualified} UPDATE "{safe_new}" = dateDiff('{ch_unit}', {base1}, {base2}) WHERE 1 SETTINGS mutations_sync = 1""")
            safe_c1 = SQLIdentifierSanitizer.sanitize(col1)
            safe_c2 = SQLIdentifierSanitizer.sanitize(col2)
            sample = await self._fetch_sample(working_table, schema, columns=[safe_c1, safe_c2, safe_new])
            return self._success_response(f"Computed {unit} difference '{col1}' → '{col2}'",
                                          [col1, col2], [new_col], sample, unit=unit,
                                          new_table=working_table)

        except Exception as e:
            return self._error_response(f"diff error: {str(e)}", [col1, col2], [])

    async def to_datetime(self, table: str, schema: str, column: str, fmt: str = None,
                          errors: str = "raise", unit: str = None, tz: str = None,
                          backend=None, data_id=None, new_table=None) -> Dict[str, Any]:
        try:
            if errors not in ("raise", "coerce"):
                return self._error_response("errors must be 'raise' or 'coerce'", [column], [])

            if fmt is not None:
                return await self.strptime(table, schema, column, fmt,
                                           backend=backend, data_id=data_id, new_table=new_table)
            if unit is not None:
                unit = unit.lower()
                if unit not in ("s", "ms", "us"):
                    return self._error_response(f"Unsupported unit: {unit} (use 's', 'ms' or 'us')", [column], [])
                divisor = {"s": 1, "ms": 1000, "us": 1000000}[unit]
                working_table = await self._prepare_operation_table(
                    table, schema, backend=backend, data_id=data_id, new_table=new_table
                )
                new_col = self._generate_cleaned_column_name(column, "todatetime")
                await self._add_new_column(working_table, schema, new_col, "TIMESTAMP")

                qualified = self._qualified_table(working_table, schema)
                safe_col = SQLIdentifierSanitizer.sanitize(column)
                safe_new = SQLIdentifierSanitizer.sanitize(new_col)
                expr = f"toDateTime(CAST(\"{safe_col}\" AS Float64) / {divisor})"
                if tz:
                    expr = f"toTimezone({expr}, '{tz}')"
                await self._exec(f"""ALTER TABLE {qualified} UPDATE "{safe_new}" = {expr} WHERE 1 SETTINGS mutations_sync = 1""")
                sample = await self._fetch_sample(working_table, schema, columns=[safe_col, safe_new])
                return self._success_response("Converted epoch to datetime",
                                              [column], [new_col], sample, unit=unit,
                                              new_table=working_table)

            working_table = await self._prepare_operation_table(
                table, schema, backend=backend, data_id=data_id, new_table=new_table
            )
            new_col = self._generate_cleaned_column_name(column, "todatetime")
            # ponytail: coerce path writes NULLs; plain DateTime rejects them.
            await self._add_new_column(
                working_table, schema, new_col,
                "Nullable(DateTime)" if errors == "coerce" else "TIMESTAMP")

            qualified = self._qualified_table(working_table, schema)
            safe_col = SQLIdentifierSanitizer.sanitize(column)
            safe_new = SQLIdentifierSanitizer.sanitize(new_col)
            base_expr = await self._datetime_base_expr(working_table, schema, column)

            if errors == "coerce":
                # ponytail: base is already DateTime for date-like cols; parse strings leniently.
                col_type = await self._get_column_type(working_table, schema, column)
                if "timestamp" in col_type.lower() or "date" in col_type.lower() or "datetime" in col_type.lower():
                    expr = base_expr
                else:
                    expr = f'parseDateTimeBestEffortOrNull("{safe_col}")'
            else:
                expr = base_expr
            if tz:
                expr = f"toTimezone({expr}, '{tz}')"
            await self._exec(f"""ALTER TABLE {qualified} UPDATE "{safe_new}" = {expr} WHERE 1 SETTINGS mutations_sync = 1""")
            sample = await self._fetch_sample(working_table, schema, columns=[safe_col, safe_new])
            return self._success_response(f"Converted '{column}' to datetime",
                                          [column], [new_col], sample, new_table=working_table)

        except Exception as e:
            return self._error_response(f"to_datetime error: {str(e)}", [column], [])

    async def fromtimestamp(self, table: str, schema: str,
                            column: Optional[str] = None,
                            value: Optional[float] = None,
                            tz: Optional[str] = None,
                            backend=None, data_id=None, new_table=None) -> Dict[str, Any]:
        try:
            if column is None and value is None:
                return self._error_response("Provide either column or value")

            working_table = await self._prepare_operation_table(
                table, schema, backend=backend, data_id=data_id, new_table=new_table
            )

            source = f'"{SQLIdentifierSanitizer.sanitize(column)}"' if column else str(value)
            new_col = self._generate_cleaned_column_name(column or "value", "fromtimestamp")
            await self._add_new_column(working_table, schema, new_col, "TIMESTAMP")

            qualified = self._qualified_table(working_table, schema)
            safe_new = SQLIdentifierSanitizer.sanitize(new_col)

            expr = f"toDateTime({source})"
            if tz:
                expr = f"toTimezone({expr}, '{tz}')"

            await self._exec(f"""
                ALTER TABLE {qualified}
                UPDATE "{safe_new}" = {expr}
                WHERE 1 SETTINGS mutations_sync = 1
            """)

            cols = [column, new_col] if column else [new_col]
            sample = await self._fetch_sample(working_table, schema, columns=cols)
            return self._success_response(
                "Converted POSIX timestamp to datetime",
                [column] if column else [],
                [new_col],
                sample,
                timezone=tz,
                new_table=working_table,
            )

        except Exception as e:
            return self._error_response(f"fromtimestamp error: {str(e)}", [], [])

    async def timestamp(self, table: str, schema: str, column: str,
                        backend=None, data_id=None, new_table=None) -> Dict[str, Any]:
        try:
            working_table = await self._prepare_operation_table(
                table, schema, backend=backend, data_id=data_id, new_table=new_table
            )
            new_col = self._generate_cleaned_column_name(column, "timestamp")
            await self._add_new_column(working_table, schema, new_col, "DOUBLE PRECISION")

            qualified = self._qualified_table(working_table, schema)
            safe_col = SQLIdentifierSanitizer.sanitize(column)
            safe_new = SQLIdentifierSanitizer.sanitize(new_col)

            base_expr = await self._datetime_base_expr(working_table, schema, column)

            expr = f'toUnixTimestamp({base_expr})'

            await self._exec(f"""ALTER TABLE {qualified} UPDATE "{safe_new}" = {expr} WHERE 1 SETTINGS mutations_sync = 1""")
            sample = await self._fetch_sample(working_table, schema, columns=[safe_col, safe_new])
            return self._success_response(f"Converted '{column}' to POSIX timestamp",
                                        [column], [new_col], sample, new_table=working_table)

        except Exception as e:
            return self._error_response(f"timestamp error: {str(e)}", [column], [])

    async def strftime(self, table: str, schema: str, column: str, fmt: str,
                       backend=None, data_id=None, new_table=None) -> Dict[str, Any]:
        try:
            working_table = await self._prepare_operation_table(
                table, schema, backend=backend, data_id=data_id, new_table=new_table
            )
            new_col = self._generate_cleaned_column_name(column, "strftime")
            await self._add_new_column(working_table, schema, new_col, "TEXT")

            qualified = self._qualified_table(working_table, schema)
            safe_col = SQLIdentifierSanitizer.sanitize(column)
            safe_new = SQLIdentifierSanitizer.sanitize(new_col)

            base_expr = await self._datetime_base_expr(working_table, schema, column)

            expr = f"formatDateTime({base_expr}, '{fmt}')"

            await self._exec(f"""ALTER TABLE {qualified} UPDATE "{safe_new}" = {expr} WHERE 1 SETTINGS mutations_sync = 1""")
            sample = await self._fetch_sample(working_table, schema, [safe_col, safe_new])
            return self._success_response(f"Formatted '{column}' using '{fmt}'",
                                        [column], [new_col], sample, format=fmt,
                                        new_table=working_table)

        except Exception as e:
            return self._error_response(f"strftime error: {str(e)}", [column], [])

    async def strptime(self, table: str, schema: str, column: str, fmt: str,
                       backend=None, data_id=None, new_table=None) -> Dict[str, Any]:
        try:
            working_table = await self._prepare_operation_table(
                table, schema, backend=backend, data_id=data_id, new_table=new_table
            )
            new_col = self._generate_cleaned_column_name(column, "strptime")
            await self._add_new_column(working_table, schema, new_col, "TIMESTAMP")

            qualified = self._qualified_table(working_table, schema)
            safe_col = SQLIdentifierSanitizer.sanitize(column)
            safe_new = SQLIdentifierSanitizer.sanitize(new_col)

            expr = f"parseDateTimeBestEffort(\"{safe_col}\", '{fmt}')"

            await self._exec(f"""ALTER TABLE {qualified} UPDATE "{safe_new}" = {expr} WHERE 1 SETTINGS mutations_sync = 1""")
            sample = await self._fetch_sample(working_table, schema, [safe_col, safe_new])
            return self._success_response(f"Parsed '{column}' using format '{fmt}'",
                                        [column], [new_col], sample, format=fmt,
                                        new_table=working_table)

        except Exception as e:
            return self._error_response(f"strptime error: {str(e)}", [column], [])

    async def add_timedelta(self, table: str, schema: str, column: str, interval: str,
                            backend=None, data_id=None, new_table=None) -> Dict[str, Any]:
        try:
            working_table = await self._prepare_operation_table(
                table, schema, backend=backend, data_id=data_id, new_table=new_table
            )
            new_col = self._generate_cleaned_column_name(column, "add")
            await self._add_new_column(working_table, schema, new_col, "TIMESTAMP")

            q = self._qualified_table(working_table, schema)
            c = SQLIdentifierSanitizer.sanitize(column)
            n = SQLIdentifierSanitizer.sanitize(new_col)

            base_expr = await self._datetime_base_expr(working_table, schema, column)

            ch_interval = interval.replace("'", "")
            await self._exec(f"""ALTER TABLE {q} UPDATE "{n}" = {base_expr} + INTERVAL {ch_interval} WHERE 1 SETTINGS mutations_sync = 1""")
            sample = await self._fetch_sample(working_table, schema, [c, n])
            return self._success_response(f"Added interval '{interval}' to '{column}'",
                                        [column], [new_col], sample, interval=interval,
                                        new_table=working_table)

        except Exception as e:
            return self._error_response(str(e), [column])

    async def sub_timedelta(self, table: str, schema: str, column: str, interval: str,
                            backend=None, data_id=None, new_table=None) -> Dict[str, Any]:
        try:
            working_table = await self._prepare_operation_table(
                table, schema, backend=backend, data_id=data_id, new_table=new_table
            )
            new_col = self._generate_cleaned_column_name(column, "sub")
            await self._add_new_column(working_table, schema, new_col, "TIMESTAMP")

            q = self._qualified_table(working_table, schema)
            c = SQLIdentifierSanitizer.sanitize(column)
            n = SQLIdentifierSanitizer.sanitize(new_col)

            base_expr = await self._datetime_base_expr(working_table, schema, column)

            ch_interval = interval.replace("'", "")
            await self._exec(f"""ALTER TABLE {q} UPDATE "{n}" = {base_expr} - INTERVAL {ch_interval} WHERE 1 SETTINGS mutations_sync = 1""")
            sample = await self._fetch_sample(working_table, schema, [c, n])
            return self._success_response(f"Subtracted interval '{interval}' from '{column}'",
                                        [column], [new_col], sample, interval=interval,
                                        new_table=working_table)

        except Exception as e:
            return self._error_response(str(e), [column])

    async def replace(self, table: str, schema: str, column: str,
                      backend=None, data_id=None, new_table=None,**kwargs,) -> Dict[str, Any]:
        try:
            allowed = {"year", "month", "day", "hour", "minute", "second"}
            for k in kwargs:
                if k not in allowed:
                    return self._error_response(f"Unsupported field: {k}", [column])

            working_table = await self._prepare_operation_table(
                table, schema, backend=backend, data_id=data_id, new_table=new_table
            )
            new_col = self._generate_cleaned_column_name(column, "replace")
            await self._add_new_column(working_table, schema, new_col, "TIMESTAMP")

            q = self._qualified_table(working_table, schema)
            c = SQLIdentifierSanitizer.sanitize(column)
            n = SQLIdentifierSanitizer.sanitize(new_col)

            base_expr = await self._datetime_base_expr(working_table, schema, column)

            def part(field):
                return kwargs.get(field, f'{_CH_REPLACE_FUNC_MAP[field]}({base_expr})')

            expr = f"""
                makeDateTime(
                    {part("year")},
                    {part("month")},
                    {part("day")},
                    {part("hour")},
                    {part("minute")},
                    {part("second")}
                )
            """
            await self._exec(f"""ALTER TABLE {q} UPDATE "{n}" = {expr} WHERE 1 SETTINGS mutations_sync = 1""")
            sample = await self._fetch_sample(working_table, schema, [c, n])
            return self._success_response(f"Replaced fields in '{column}'",
                                        [column], [new_col], sample, replaced_fields=kwargs,
                                        new_table=working_table)

        except Exception as e:
            return self._error_response(str(e), [column])

    async def between(self, table: str, schema: str, column: str, start: str, end: str,
                      backend=None, data_id=None, new_table=None) -> Dict[str, Any]:
        try:
            base_expr = await self._datetime_base_expr(table, schema, column)
            where = f"{base_expr} BETWEEN CAST({self._dt_literal(start)} AS DateTime) AND CAST({self._dt_literal(end)} AS DateTime)"
            working_table = await self._prepare_filtered_table(
                table, schema, where, backend=backend, data_id=data_id, new_table=new_table
            )
            sample = await self._fetch_sample(working_table, schema)
            return self._success_response(f"Filtered '{column}' between '{start}' and '{end}'",
                                          [column], [], sample, new_table=working_table)

        except Exception as e:
            return self._error_response(f"between error: {str(e)}", [column], [])

    async def before(self, table: str, schema: str, column: str, value: str,
                     backend=None, data_id=None, new_table=None) -> Dict[str, Any]:
        try:
            base_expr = await self._datetime_base_expr(table, schema, column)
            where = f"{base_expr} < CAST({self._dt_literal(value)} AS DateTime)"
            working_table = await self._prepare_filtered_table(
                table, schema, where, backend=backend, data_id=data_id, new_table=new_table
            )
            sample = await self._fetch_sample(working_table, schema)
            return self._success_response(f"Filtered '{column}' before '{value}'",
                                          [column], [], sample, new_table=working_table)

        except Exception as e:
            return self._error_response(f"before error: {str(e)}", [column], [])

    async def after(self, table: str, schema: str, column: str, value: str,
                    backend=None, data_id=None, new_table=None) -> Dict[str, Any]:
        try:
            base_expr = await self._datetime_base_expr(table, schema, column)
            where = f"{base_expr} > CAST({self._dt_literal(value)} AS DateTime)"
            working_table = await self._prepare_filtered_table(
                table, schema, where, backend=backend, data_id=data_id, new_table=new_table
            )
            sample = await self._fetch_sample(working_table, schema)
            return self._success_response(f"Filtered '{column}' after '{value}'",
                                          [column], [], sample, new_table=working_table)

        except Exception as e:
            return self._error_response(f"after error: {str(e)}", [column], [])

    async def select_year(self, table: str, schema: str, column: str, years,
                          backend=None, data_id=None, new_table=None) -> Dict[str, Any]:
        try:
            values = ", ".join(str(int(y)) for y in (years if isinstance(years, (list, tuple)) else [years]))
            base_expr = await self._datetime_base_expr(table, schema, column)
            where = f"toYear({base_expr}) IN ({values})"
            working_table = await self._prepare_filtered_table(
                table, schema, where, backend=backend, data_id=data_id, new_table=new_table
            )
            sample = await self._fetch_sample(working_table, schema)
            return self._success_response(f"Selected years {values} from '{column}'",
                                          [column], [], sample, new_table=working_table)

        except Exception as e:
            return self._error_response(f"select_year error: {str(e)}", [column], [])

    async def select_month(self, table: str, schema: str, column: str, months,
                           backend=None, data_id=None, new_table=None) -> Dict[str, Any]:
        try:
            values = ", ".join(str(int(m)) for m in (months if isinstance(months, (list, tuple)) else [months]))
            base_expr = await self._datetime_base_expr(table, schema, column)
            where = f"toMonth({base_expr}) IN ({values})"
            working_table = await self._prepare_filtered_table(
                table, schema, where, backend=backend, data_id=data_id, new_table=new_table
            )
            sample = await self._fetch_sample(working_table, schema)
            return self._success_response(f"Selected months {values} from '{column}'",
                                          [column], [], sample, new_table=working_table)

        except Exception as e:
            return self._error_response(f"select_month error: {str(e)}", [column], [])

    async def add_offset(self, table: str, schema: str, column: str,
                         years: int = 0, quarters: int = 0, months: int = 0,
                         weeks: int = 0, days: int = 0, business_day: bool = False,
                         target_col: str = None, backend=None, data_id=None,
                         new_table: str = None) -> Dict[str, Any]:
        try:
            if business_day and any([years, quarters, months, weeks]):
                return self._error_response(
                    "business_day offsets only combine with 'days'", [column], [])

            working_table = await self._prepare_operation_table(
                table, schema, backend=backend, data_id=data_id, new_table=new_table
            )
            new_col = SQLIdentifierSanitizer.sanitize(target_col) if target_col else self._generate_cleaned_column_name(column, "offset")
            await self._add_new_column(working_table, schema, new_col, "TIMESTAMP")

            qualified = self._qualified_table(working_table, schema)
            safe_col = SQLIdentifierSanitizer.sanitize(column)
            safe_new = SQLIdentifierSanitizer.sanitize(new_col)
            base_expr = await self._datetime_base_expr(working_table, schema, column)

            if business_day:
                # ponytail: addDays proven on this server; string-unit dateAdd
                # misresolves here, so it is not used anywhere on CH.
                dow = f"toDayOfWeek({base_expr})"
                if days >= 0:
                    roll = f"(CASE WHEN {dow} = 6 THEN 2 WHEN {dow} = 7 THEN 1 ELSE 0 END)"
                    rolled = f"addDays({base_expr}, {roll})"
                    kk = f"greatest({days} - (CASE WHEN {dow} IN (6, 7) THEN 1 ELSE 0 END), 0)"
                    w = f"(toDayOfWeek({rolled}) - 1)"
                    expr = f"addDays({rolled}, ({kk}) + 2 * intDiv(({w}) + ({kk}), 5))"
                else:
                    m = abs(days)
                    rollback = f"(CASE WHEN {dow} = 6 THEN 1 WHEN {dow} = 7 THEN 2 ELSE 0 END)"
                    rolled = f"addDays({base_expr}, -{rollback})"
                    kk = f"greatest({m} - (CASE WHEN {dow} IN (6, 7) THEN 1 ELSE 0 END), 0)"
                    wrev = f"(4 - (toDayOfWeek({rolled}) - 1))"
                    expr = f"addDays({rolled}, -(({kk}) + 2 * intDiv(({wrev}) + ({kk}), 5)))"
            else:
                expr = base_expr
                if years:
                    expr = f"addYears({expr}, {years})"
                if quarters:
                    expr = f"addQuarters({expr}, {quarters})"
                if months:
                    expr = f"addMonths({expr}, {months})"
                if weeks:
                    expr = f"addWeeks({expr}, {weeks})"
                if days:
                    expr = f"addDays({expr}, {days})"
                if expr == base_expr:
                    return self._error_response("at least one offset unit must be non-zero", [column], [])

            await self._exec(f"""ALTER TABLE {qualified} UPDATE "{safe_new}" = {expr} WHERE 1 SETTINGS mutations_sync = 1""")
            sample = await self._fetch_sample(working_table, schema, columns=[safe_col, safe_new])
            return self._success_response(f"Applied offset to '{column}'",
                                          [column], [new_col], sample, new_table=working_table)

        except Exception as e:
            return self._error_response(f"add_offset error: {str(e)}", [column], [])

    async def normalize(self, table: str, schema: str, column: str,
                        backend=None, data_id=None, new_table=None) -> Dict[str, Any]:
        try:
            working_table = await self._prepare_operation_table(
                table, schema, backend=backend, data_id=data_id, new_table=new_table
            )
            new_col = self._generate_cleaned_column_name(column, "normalize")
            await self._add_new_column(working_table, schema, new_col, "TIMESTAMP")

            q = self._qualified_table(working_table, schema)
            c = SQLIdentifierSanitizer.sanitize(column)
            n = SQLIdentifierSanitizer.sanitize(new_col)

            base_expr = await self._datetime_base_expr(working_table, schema, column)

            await self._exec(f"""ALTER TABLE {q} UPDATE "{n}" = toStartOfDay({base_expr}) WHERE 1 SETTINGS mutations_sync = 1""")
            sample = await self._fetch_sample(working_table, schema, [c, n])
            return self._success_response(f"Normalized '{column}' to day",
                                        [column], [new_col], sample, new_table=working_table)

        except Exception as e:
            return self._error_response(str(e), [column])
