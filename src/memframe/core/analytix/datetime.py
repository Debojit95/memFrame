from typing import Any, Dict, List, Optional
import traceback
from datetime import datetime, UTC
import pandas as pd

from memframe.db_manager.adapters.base import DatabaseAdapter
from memframe.db_manager.adapters.duckdb import DuckDBAdapter
from memframe.db_manager.adapters.postgresql import PostgresAdapter
from memframe.db_manager.adapters.clickhouse import ClickHouseAdapter
from memframe.utils.helper import SQLIdentifierSanitizer

DT_FIELD_MAP = {
    "year": "YEAR",
    "month": "MONTH",
    "day": "DAY",
    "hour": "HOUR",
    "minute": "MINUTE",
    "second": "SECOND",
    "dayofweek": "DOW",
    "dow": "DOW",
    "dayofyear": "DOY",
    "doy": "DOY",
    "week": "WEEK",
    "weekofyear": "WEEK",
    "quarter": "QUARTER",
}


class DatetimeOps:
    """
    Core datetime operations executed directly on the database.
    Every public method creates a new transient table, adds a result
    column there, and returns a standardised response with the new table
    name (identical pattern to DataCleaningOps, ArithmeticOps, etc.).
    """

    def __init__(self, db_adapter: DatabaseAdapter):
        self.db = db_adapter

    # ------------------------------------------------------------------
    # Internal helpers (identical to other core classes)
    # ------------------------------------------------------------------
    async def _exec(self, sql: str, *args):
        return await self.db.execute(sql, *args)

    async def _fetch(self, sql: str, *args):
        return await self.db.fetch(sql, *args)

    async def _fetchval(self, sql: str, *args):
        return await self.db.fetchval(sql, *args)

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

        rows = await self._fetch(f"SELECT {column_clause} FROM {qualified}")
        records = [dict(row) for row in rows]
        return pd.DataFrame.from_records(records)

    async def _get_column_type(self, table: str, schema: str, column: str) -> str:
        types = await self.db.get_column_types(table, schema)
        return types.get(column, "TIMESTAMP")

    def _qualified_table(self, table: str, schema: str) -> str:
        safe_table = SQLIdentifierSanitizer.sanitize(table)
        safe_schema = SQLIdentifierSanitizer.sanitize(schema)
        return f'{self.db.quote_identifier(safe_schema)}.{self.db.quote_identifier(safe_table)}'

    def _generate_cleaned_column_name(self, original: str, suffix: str = "") -> str:
        name = f"dt_{original}"
        return f"{name}_{suffix}" if suffix else name

    async def _add_new_column(self, table: str, schema: str, col_name: str, col_type: str):
        qualified = self._qualified_table(table, schema)
        safe_col = SQLIdentifierSanitizer.sanitize(col_name)
        await self._exec(f'ALTER TABLE {qualified} ADD COLUMN "{safe_col}" {col_type}')

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

    def _success_response(self, message: str, involved_cols: List[str], generated_cols: List[str],
                          sample_df: pd.DataFrame, **extra) -> Dict[str, Any]:
        return {
            "is_error": False,
            "message": message,
            "error_message": None,
            "involved_cols": involved_cols,
            "generated_cols": generated_cols,
            "result": sample_df,
            **extra,
        }

    def _error_response(self, error_message: str, involved_cols: List[str] = None,
                        generated_cols: List[str] = None) -> Dict[str, Any]:
        return {
            "is_error": True,
            "message": "",
            "error_message": error_message,
            "involved_cols": involved_cols or [],
            "generated_cols": generated_cols or [],
        }

    def _unsupported_backend_error(self) -> NotImplementedError:
        return NotImplementedError(
            f"Unsupported database backend for datetime operation: {self.db.__class__.__name__}"
        )

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

            if isinstance(self.db, PostgresAdapter) or isinstance(self.db, DuckDBAdapter):
                
                working_table = await self._prepare_operation_table(
                    table, schema, backend=backend, data_id=data_id, new_table=new_table
                )

                sql_field = DT_FIELD_MAP[field]
                new_col = self._generate_cleaned_column_name(column, field)
                await self._add_new_column(working_table, schema, new_col, "INTEGER")

                qualified = self._qualified_table(working_table, schema)
                safe_col = SQLIdentifierSanitizer.sanitize(column)
                safe_new = SQLIdentifierSanitizer.sanitize(new_col)

                await self._exec(f"""
                    UPDATE {qualified}
                    SET "{safe_new}" = EXTRACT({sql_field} FROM "{safe_col}")
                """)

                sample = await self._fetch_sample(working_table, schema, columns=[safe_col, safe_new])
                msg = f"Extracted {field} from '{column}' → '{new_col}'"
                return self._success_response(msg, [column], [new_col], sample,
                                            extract_field=field, new_table=working_table)
                                            
            elif isinstance(self.db, ClickHouseAdapter):
                ch_field_map = {
                    "year": "toYear", "month": "toMonth", "day": "toDayOfMonth",
                    "hour": "toHour", "minute": "toMinute", "second": "toSecond",
                    "dayofweek": "toDayOfWeek", "dow": "toDayOfWeek",
                    "dayofyear": "toDayOfYear", "doy": "toDayOfYear",
                    "week": "toWeek", "weekofyear": "toWeek",
                    "quarter": "toQuarter"
                }
                working_table = await self._prepare_operation_table(
                    table, schema, backend=backend, data_id=data_id, new_table=new_table
                )

                sql_func = ch_field_map[field]
                new_col = self._generate_cleaned_column_name(column, field)
                await self._add_new_column(working_table, schema, new_col, "INTEGER")

                qualified = self._qualified_table(working_table, schema)
                safe_col = SQLIdentifierSanitizer.sanitize(column)
                safe_new = SQLIdentifierSanitizer.sanitize(new_col)

                # Explicitly cast to DateTime to prevent Illegal type String errors
                col_type = await self._get_column_type(working_table, schema, column)
                if "timestamp" in col_type.lower() or "date" in col_type.lower() or "datetime" in col_type.lower():
                    base_expr = f'"{safe_col}"'
                else:
                    base_expr = f'CAST("{safe_col}" AS DateTime)'

                await self._exec(f"""
                    ALTER TABLE {qualified} 
                    UPDATE "{safe_new}" = {sql_func}({base_expr}) 
                    WHERE 1
                """)

                sample = await self._fetch_sample(working_table, schema, columns=[safe_col, safe_new])
                msg = f"Extracted {field} from '{column}' → '{new_col}'"
                return self._success_response(msg, [column], [new_col], sample,
                                            extract_field=field, new_table=working_table)
            else:
                raise self._unsupported_backend_error()

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

            if isinstance(self.db, PostgresAdapter) or isinstance(self.db, DuckDBAdapter):
                working_table = await self._prepare_operation_table(
                    table, schema, backend=backend, data_id=data_id, new_table=new_table
                )

                new_col = self._generate_cleaned_column_name(column, f"ceil_{unit}")
                await self._add_new_column(working_table, schema, new_col, "TIMESTAMP")

                qualified = self._qualified_table(working_table, schema)
                safe_col = SQLIdentifierSanitizer.sanitize(column)
                safe_new = SQLIdentifierSanitizer.sanitize(new_col)
                interval = interval_map[unit]

                await self._exec(f"""
                    UPDATE {qualified}
                    SET "{safe_new}" =
                        CASE
                            WHEN "{safe_col}" = DATE_TRUNC('{unit}', "{safe_col}")
                            THEN "{safe_col}"
                            ELSE DATE_TRUNC('{unit}', "{safe_col}" + INTERVAL '{interval}')
                        END
                """)

                sample = await self._fetch_sample(working_table, schema, columns=[safe_col, safe_new])
                msg = f"Ceiled '{column}' to {unit}"
                return self._success_response(msg, [column], [new_col], sample, unit=unit,
                                            new_table=working_table)

            elif isinstance(self.db, ClickHouseAdapter):
                ch_floor_map = {
                    "year": "toStartOfYear", "quarter": "toStartOfQuarter", "month": "toStartOfMonth",
                    "week": "toMonday", "day": "toStartOfDay", "hour": "toStartOfHour",
                    "minute": "toStartOfMinute", "second": "toStartOfSecond"
                }
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
                floor_fn = ch_floor_map[unit]
                interval = ch_interval_map[unit]

                col_type = await self._get_column_type(working_table, schema, column)
                if "timestamp" in col_type.lower() or "date" in col_type.lower() or "datetime" in col_type.lower():
                    base_expr = f'"{safe_col}"'
                else:
                    base_expr = f'CAST("{safe_col}" AS DateTime)'

                await self._exec(f"""
                    ALTER TABLE {qualified}
                    UPDATE "{safe_new}" = 
                        IF({base_expr} = {floor_fn}({base_expr}), 
                           {base_expr}, 
                           {floor_fn}({base_expr} + INTERVAL {interval}))
                    WHERE 1
                """)

                sample = await self._fetch_sample(working_table, schema, columns=[safe_col, safe_new])
                msg = f"Ceiled '{column}' to {unit}"
                return self._success_response(msg, [column], [new_col], sample, unit=unit,
                                            new_table=working_table)
            else:
                raise self._unsupported_backend_error()

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

            if isinstance(self.db, PostgresAdapter) or isinstance(self.db, DuckDBAdapter):
                working_table = await self._prepare_operation_table(
                    table, schema, backend=backend, data_id=data_id, new_table=new_table
                )

                new_col = self._generate_cleaned_column_name(column, f"round_{unit}")
                await self._add_new_column(working_table, schema, new_col, "TIMESTAMP")

                qualified = self._qualified_table(working_table, schema)
                safe_col = SQLIdentifierSanitizer.sanitize(column)
                safe_new = SQLIdentifierSanitizer.sanitize(new_col)
                half_interval = half_interval_map[unit]

                await self._exec(f"""
                    UPDATE {qualified}
                    SET "{safe_new}" =
                        DATE_TRUNC(
                            '{unit}',
                            "{safe_col}" + INTERVAL '{half_interval}'
                        )
                """)

                sample = await self._fetch_sample(working_table, schema, columns=[safe_col, safe_new])
                msg = f"Rounded '{column}' to {unit}"
                return self._success_response(msg, [column], [new_col], sample, unit=unit,
                                            new_table=working_table)
                                            
            elif isinstance(self.db, ClickHouseAdapter):
                ch_floor_map = {
                    "year": "toStartOfYear", "quarter": "toStartOfQuarter", "month": "toStartOfMonth",
                    "week": "toMonday", "day": "toStartOfDay", "hour": "toStartOfHour",
                    "minute": "toStartOfMinute", "second": "toStartOfSecond"
                }
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
                floor_fn = ch_floor_map[unit]
                half_interval = ch_half_interval_map[unit]

                col_type = await self._get_column_type(working_table, schema, column)
                if "timestamp" in col_type.lower() or "date" in col_type.lower() or "datetime" in col_type.lower():
                    base_expr = f'"{safe_col}"'
                else:
                    base_expr = f'CAST("{safe_col}" AS DateTime)'

                await self._exec(f"""
                    ALTER TABLE {qualified}
                    UPDATE "{safe_new}" = {floor_fn}({base_expr} + INTERVAL {half_interval})
                    WHERE 1
                """)

                sample = await self._fetch_sample(working_table, schema, columns=[safe_col, safe_new])
                msg = f"Rounded '{column}' to {unit}"
                return self._success_response(msg, [column], [new_col], sample, unit=unit,
                                            new_table=working_table)
            else:
                raise self._unsupported_backend_error()

        except Exception as e:
            return self._error_response(f"round error: {str(e)}", [column], [])

    async def floor(self, table: str, schema: str, column: str, unit: str,
                    backend=None, data_id=None, new_table=None) -> Dict[str, Any]:
        try:
            unit = unit.lower()
            valid_units = {"year", "quarter", "month", "week", "day", "hour", "minute", "second"}
            if unit not in valid_units:
                return self._error_response(f"Unsupported floor unit: {unit}", [column], [])

            if isinstance(self.db, PostgresAdapter) or isinstance(self.db, DuckDBAdapter):
                working_table = await self._prepare_operation_table(
                    table, schema, backend=backend, data_id=data_id, new_table=new_table
                )

                new_col = self._generate_cleaned_column_name(column, f"floor_{unit}")
                await self._add_new_column(working_table, schema, new_col, "TIMESTAMP")

                qualified = self._qualified_table(working_table, schema)
                safe_col = SQLIdentifierSanitizer.sanitize(column)
                safe_new = SQLIdentifierSanitizer.sanitize(new_col)

                await self._exec(f"""
                    UPDATE {qualified}
                    SET "{safe_new}" = DATE_TRUNC('{unit}', "{safe_col}")
                """)

                sample = await self._fetch_sample(working_table, schema, columns=[safe_col, safe_new])
                msg = f"Floored '{column}' to {unit}"
                return self._success_response(msg, [column], [new_col], sample, unit=unit,
                                            new_table=working_table)
                                            
            elif isinstance(self.db, ClickHouseAdapter):
                ch_floor_map = {
                    "year": "toStartOfYear", "quarter": "toStartOfQuarter", "month": "toStartOfMonth",
                    "week": "toMonday", "day": "toStartOfDay", "hour": "toStartOfHour",
                    "minute": "toStartOfMinute", "second": "toStartOfSecond"
                }
                working_table = await self._prepare_operation_table(
                    table, schema, backend=backend, data_id=data_id, new_table=new_table
                )

                new_col = self._generate_cleaned_column_name(column, f"floor_{unit}")
                await self._add_new_column(working_table, schema, new_col, "TIMESTAMP")

                qualified = self._qualified_table(working_table, schema)
                safe_col = SQLIdentifierSanitizer.sanitize(column)
                safe_new = SQLIdentifierSanitizer.sanitize(new_col)
                floor_fn = ch_floor_map[unit]

                col_type = await self._get_column_type(working_table, schema, column)
                if "timestamp" in col_type.lower() or "date" in col_type.lower() or "datetime" in col_type.lower():
                    base_expr = f'"{safe_col}"'
                else:
                    base_expr = f'CAST("{safe_col}" AS DateTime)'

                await self._exec(f"""
                    ALTER TABLE {qualified}
                    UPDATE "{safe_new}" = {floor_fn}({base_expr})
                    WHERE 1
                """)

                sample = await self._fetch_sample(working_table, schema, columns=[safe_col, safe_new])
                msg = f"Floored '{column}' to {unit}"
                return self._success_response(msg, [column], [new_col], sample, unit=unit,
                                            new_table=working_table)
            else:
                raise self._unsupported_backend_error()

        except Exception as e:
            return self._error_response(f"floor error: {str(e)}", [column], [])

    # ==================================================================
    #  TIMEZONE OPERATIONS
    # ==================================================================
    async def tz_localize(self, table: str, schema: str, column: str, tz: str | None,
                          ambiguous: str = "raise", nonexistent: str = "raise",
                          backend=None, data_id=None, new_table=None) -> Dict[str, Any]:
        try:
            if isinstance(self.db, PostgresAdapter) or isinstance(self.db, DuckDBAdapter):
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

                if tz is None:
                    await self._exec(f"""
                        UPDATE {qualified}
                        SET "{safe_new}" = "{safe_col}"::timestamp
                    """)
                    msg = f"Removed timezone from '{column}'"
                else:
                    await self._exec(f"""
                        UPDATE {qualified}
                        SET "{safe_new}" = "{safe_col}" AT TIME ZONE '{tz}'
                    """)
                    msg = f"Localized '{column}' to timezone '{tz}'"

                sample = await self._fetch_sample(working_table, schema, columns=[safe_col, safe_new])
                return self._success_response(msg, [column], [new_col], sample, timezone=tz,
                                            warnings=warnings, new_table=working_table)

            elif isinstance(self.db, ClickHouseAdapter):
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

                col_type = await self._get_column_type(working_table, schema, column)
                if "timestamp" in col_type.lower() or "date" in col_type.lower() or "datetime" in col_type.lower():
                    base_expr = f'"{safe_col}"'
                else:
                    base_expr = f'CAST("{safe_col}" AS DateTime)'

                if tz is None:
                    await self._exec(f"""
                        ALTER TABLE {qualified}
                        UPDATE "{safe_new}" = toTimezone({base_expr}, 'UTC')
                        WHERE 1
                    """)
                    msg = f"Removed timezone from '{column}' (Defaulted to UTC)"
                else:
                    await self._exec(f"""
                        ALTER TABLE {qualified}
                        UPDATE "{safe_new}" = toTimezone({base_expr}, '{tz}')
                        WHERE 1
                    """)
                    msg = f"Localized '{column}' to timezone '{tz}'"

                sample = await self._fetch_sample(working_table, schema, columns=[safe_col, safe_new])
                return self._success_response(msg, [column], [new_col], sample, timezone=tz,
                                            warnings=warnings, new_table=working_table)
            else:
                raise self._unsupported_backend_error()

        except Exception as e:
            return self._error_response(f"tz_localize error: {str(e)}\n{traceback.format_exc()}", [column], [])

    async def tz_convert(self, table: str, schema: str, column: str, tz: str | None,
                         backend=None, data_id=None, new_table=None) -> Dict[str, Any]:
        try:
            if isinstance(self.db, PostgresAdapter) or isinstance(self.db, DuckDBAdapter):
                working_table = await self._prepare_operation_table(
                    table, schema, backend=backend, data_id=data_id, new_table=new_table
                )

                new_col = self._generate_cleaned_column_name(column, f"tzconvert_{tz or 'naive'}")
                await self._add_new_column(working_table, schema, new_col, "TIMESTAMP")

                qualified = self._qualified_table(working_table, schema)
                safe_col = SQLIdentifierSanitizer.sanitize(column)
                safe_new = SQLIdentifierSanitizer.sanitize(new_col)

                if tz is None:
                    await self._exec(f"""
                        UPDATE {qualified}
                        SET "{safe_new}" = ("{safe_col}" AT TIME ZONE 'UTC')
                    """)
                    msg = f"Converted '{column}' to UTC and removed timezone"
                else:
                    await self._exec(f"""
                        UPDATE {qualified}
                        SET "{safe_new}" = ("{safe_col}" AT TIME ZONE 'UTC') AT TIME ZONE '{tz}'
                    """)
                    msg = f"Converted '{column}' timezone to '{tz}'"

                sample = await self._fetch_sample(working_table, schema, columns=[safe_col, safe_new])
                return self._success_response(msg, [column], [new_col], sample, timezone=tz,
                                            new_table=working_table)

            elif isinstance(self.db, ClickHouseAdapter):
                working_table = await self._prepare_operation_table(
                    table, schema, backend=backend, data_id=data_id, new_table=new_table
                )

                new_col = self._generate_cleaned_column_name(column, f"tzconvert_{tz or 'naive'}")
                await self._add_new_column(working_table, schema, new_col, "TIMESTAMP")

                qualified = self._qualified_table(working_table, schema)
                safe_col = SQLIdentifierSanitizer.sanitize(column)
                safe_new = SQLIdentifierSanitizer.sanitize(new_col)

                col_type = await self._get_column_type(working_table, schema, column)
                if "timestamp" in col_type.lower() or "date" in col_type.lower() or "datetime" in col_type.lower():
                    base_expr = f'"{safe_col}"'
                else:
                    base_expr = f'CAST("{safe_col}" AS DateTime)'

                if tz is None:
                    await self._exec(f"""
                        ALTER TABLE {qualified}
                        UPDATE "{safe_new}" = toTimezone({base_expr}, 'UTC')
                        WHERE 1
                    """)
                    msg = f"Converted '{column}' to UTC and removed timezone"
                else:
                    await self._exec(f"""
                        ALTER TABLE {qualified}
                        UPDATE "{safe_new}" = toTimezone(toTimezone({base_expr}, 'UTC'), '{tz}')
                        WHERE 1
                    """)
                    msg = f"Converted '{column}' timezone to '{tz}'"

                sample = await self._fetch_sample(working_table, schema, columns=[safe_col, safe_new])
                return self._success_response(msg, [column], [new_col], sample, timezone=tz,
                                            new_table=working_table)
            else:
                raise self._unsupported_backend_error()

        except Exception as e:
            return self._error_response(f"tz_convert error: {str(e)}\n{traceback.format_exc()}", [column], [])

    # ==================================================================
    #  BOOLEAN CHECKS
    # ==================================================================
    async def is_month_start(self, table: str, schema: str, column: str,
                             backend=None, data_id=None, new_table=None) -> Dict[str, Any]:
        try:
            if isinstance(self.db, PostgresAdapter) or isinstance(self.db, DuckDBAdapter):
                working_table = await self._prepare_operation_table(
                    table, schema, backend=backend, data_id=data_id, new_table=new_table
                )
                new_col = self._generate_cleaned_column_name(column, "is_month_start")
                await self._add_new_column(working_table, schema, new_col, "BOOLEAN")

                q = self._qualified_table(working_table, schema)
                c = SQLIdentifierSanitizer.sanitize(column)
                n = SQLIdentifierSanitizer.sanitize(new_col)

                await self._exec(f"""UPDATE {q} SET "{n}" = EXTRACT(DAY FROM "{c}") = 1""")
                sample = await self._fetch_sample(working_table, schema, [c, n])
                return self._success_response("Computed is_month_start", [column], [new_col], sample,
                                            new_table=working_table)
                                            
            elif isinstance(self.db, ClickHouseAdapter):
                working_table = await self._prepare_operation_table(
                    table, schema, backend=backend, data_id=data_id, new_table=new_table
                )
                new_col = self._generate_cleaned_column_name(column, "is_month_start")
                await self._add_new_column(working_table, schema, new_col, "BOOLEAN")

                q = self._qualified_table(working_table, schema)
                c = SQLIdentifierSanitizer.sanitize(column)
                n = SQLIdentifierSanitizer.sanitize(new_col)

                col_type = await self._get_column_type(working_table, schema, column)
                if "timestamp" in col_type.lower() or "date" in col_type.lower() or "datetime" in col_type.lower():
                    base_expr = f'"{c}"'
                else:
                    base_expr = f'CAST("{c}" AS DateTime)'

                await self._exec(f"""ALTER TABLE {q} UPDATE "{n}" = toDayOfMonth({base_expr}) = 1 WHERE 1""")
                sample = await self._fetch_sample(working_table, schema, [c, n])
                return self._success_response("Computed is_month_start", [column], [new_col], sample,
                                            new_table=working_table)
            else:
                raise self._unsupported_backend_error()

        except Exception as e:
            return self._error_response(str(e), [column])

    async def is_month_end(self, table: str, schema: str, column: str,
                           backend=None, data_id=None, new_table=None) -> Dict[str, Any]:
        try:
            if isinstance(self.db, PostgresAdapter) or isinstance(self.db, DuckDBAdapter):
                working_table = await self._prepare_operation_table(
                    table, schema, backend=backend, data_id=data_id, new_table=new_table
                )
                new_col = self._generate_cleaned_column_name(column, "is_month_end")
                await self._add_new_column(working_table, schema, new_col, "BOOLEAN")

                q = self._qualified_table(working_table, schema)
                c = SQLIdentifierSanitizer.sanitize(column)
                n = SQLIdentifierSanitizer.sanitize(new_col)

                await self._exec(f"""
                    UPDATE {q}
                    SET "{n}" =
                        DATE_TRUNC('month', "{c}" + INTERVAL '1 day')
                        != DATE_TRUNC('month', "{c}")
                """)
                sample = await self._fetch_sample(working_table, schema, [c, n])
                return self._success_response("Computed is_month_end", [column], [new_col], sample,
                                            new_table=working_table)
                                            
            elif isinstance(self.db, ClickHouseAdapter):
                working_table = await self._prepare_operation_table(
                    table, schema, backend=backend, data_id=data_id, new_table=new_table
                )
                new_col = self._generate_cleaned_column_name(column, "is_month_end")
                await self._add_new_column(working_table, schema, new_col, "BOOLEAN")

                q = self._qualified_table(working_table, schema)
                c = SQLIdentifierSanitizer.sanitize(column)
                n = SQLIdentifierSanitizer.sanitize(new_col)

                col_type = await self._get_column_type(working_table, schema, column)
                if "timestamp" in col_type.lower() or "date" in col_type.lower() or "datetime" in col_type.lower():
                    base_expr = f'"{c}"'
                else:
                    base_expr = f'CAST("{c}" AS DateTime)'

                await self._exec(f"""
                    ALTER TABLE {q}
                    UPDATE "{n}" = toDayOfMonth(toLastDayOfMonth({base_expr})) = toDayOfMonth({base_expr})
                    WHERE 1
                """)
                sample = await self._fetch_sample(working_table, schema, [c, n])
                return self._success_response("Computed is_month_end", [column], [new_col], sample,
                                            new_table=working_table)
            else:
                raise self._unsupported_backend_error()

        except Exception as e:
            return self._error_response(str(e), [column])

    async def is_year_start(self, table: str, schema: str, column: str,
                            backend=None, data_id=None, new_table=None) -> Dict[str, Any]:
        try:
            if isinstance(self.db, PostgresAdapter) or isinstance(self.db, DuckDBAdapter):
                working_table = await self._prepare_operation_table(
                    table, schema, backend=backend, data_id=data_id, new_table=new_table
                )
                new_col = self._generate_cleaned_column_name(column, "is_year_start")
                await self._add_new_column(working_table, schema, new_col, "BOOLEAN")

                q = self._qualified_table(working_table, schema)
                c = SQLIdentifierSanitizer.sanitize(column)
                n = SQLIdentifierSanitizer.sanitize(new_col)

                await self._exec(f"""
                    UPDATE {q}
                    SET "{n}" =
                        EXTRACT(MONTH FROM "{c}") = 1
                        AND EXTRACT(DAY FROM "{c}") = 1
                """)
                sample = await self._fetch_sample(working_table, schema, [c, n])
                return self._success_response("Computed is_year_start", [column], [new_col], sample,
                                            new_table=working_table)

            elif isinstance(self.db, ClickHouseAdapter):
                working_table = await self._prepare_operation_table(
                    table, schema, backend=backend, data_id=data_id, new_table=new_table
                )
                new_col = self._generate_cleaned_column_name(column, "is_year_start")
                await self._add_new_column(working_table, schema, new_col, "BOOLEAN")

                q = self._qualified_table(working_table, schema)
                c = SQLIdentifierSanitizer.sanitize(column)
                n = SQLIdentifierSanitizer.sanitize(new_col)

                col_type = await self._get_column_type(working_table, schema, column)
                if "timestamp" in col_type.lower() or "date" in col_type.lower() or "datetime" in col_type.lower():
                    base_expr = f'"{c}"'
                else:
                    base_expr = f'CAST("{c}" AS DateTime)'

                await self._exec(f"""
                    ALTER TABLE {q}
                    UPDATE "{n}" = toMonth({base_expr}) = 1 AND toDayOfMonth({base_expr}) = 1
                    WHERE 1
                """)
                sample = await self._fetch_sample(working_table, schema, [c, n])
                return self._success_response("Computed is_year_start", [column], [new_col], sample,
                                            new_table=working_table)
            else:
                raise self._unsupported_backend_error()

        except Exception as e:
            return self._error_response(str(e), [column])

    async def is_year_end(self, table: str, schema: str, column: str,
                          backend=None, data_id=None, new_table=None) -> Dict[str, Any]:
        try:
            if isinstance(self.db, PostgresAdapter) or isinstance(self.db, DuckDBAdapter):
                working_table = await self._prepare_operation_table(
                    table, schema, backend=backend, data_id=data_id, new_table=new_table
                )
                new_col = self._generate_cleaned_column_name(column, "is_year_end")
                await self._add_new_column(working_table, schema, new_col, "BOOLEAN")

                q = self._qualified_table(working_table, schema)
                c = SQLIdentifierSanitizer.sanitize(column)
                n = SQLIdentifierSanitizer.sanitize(new_col)

                await self._exec(f"""
                    UPDATE {q}
                    SET "{n}" =
                        DATE_TRUNC('year', "{c}" + INTERVAL '1 day')
                        != DATE_TRUNC('year', "{c}")
                """)
                sample = await self._fetch_sample(working_table, schema, [c, n])
                return self._success_response("Computed is_year_end", [column], [new_col], sample,
                                            new_table=working_table)

            elif isinstance(self.db, ClickHouseAdapter):
                working_table = await self._prepare_operation_table(
                    table, schema, backend=backend, data_id=data_id, new_table=new_table
                )
                new_col = self._generate_cleaned_column_name(column, "is_year_end")
                await self._add_new_column(working_table, schema, new_col, "BOOLEAN")

                q = self._qualified_table(working_table, schema)
                c = SQLIdentifierSanitizer.sanitize(column)
                n = SQLIdentifierSanitizer.sanitize(new_col)

                col_type = await self._get_column_type(working_table, schema, column)
                if "timestamp" in col_type.lower() or "date" in col_type.lower() or "datetime" in col_type.lower():
                    base_expr = f'"{c}"'
                else:
                    base_expr = f'CAST("{c}" AS DateTime)'

                await self._exec(f"""
                    ALTER TABLE {q}
                    UPDATE "{n}" = toMonth({base_expr}) = 12 AND toDayOfMonth({base_expr}) = 31
                    WHERE 1
                """)
                sample = await self._fetch_sample(working_table, schema, [c, n])
                return self._success_response("Computed is_year_end", [column], [new_col], sample,
                                            new_table=working_table)
            else:
                raise self._unsupported_backend_error()

        except Exception as e:
            return self._error_response(str(e), [column])

    async def is_quarter_start(self, table: str, schema: str, column: str,
                               backend=None, data_id=None, new_table=None) -> Dict[str, Any]:
        try:
            if isinstance(self.db, PostgresAdapter) or isinstance(self.db, DuckDBAdapter):
                working_table = await self._prepare_operation_table(
                    table, schema, backend=backend, data_id=data_id, new_table=new_table
                )
                new_col = self._generate_cleaned_column_name(column, "is_quarter_start")
                await self._add_new_column(working_table, schema, new_col, "BOOLEAN")

                q = self._qualified_table(working_table, schema)
                c = SQLIdentifierSanitizer.sanitize(column)
                n = SQLIdentifierSanitizer.sanitize(new_col)

                await self._exec(f"""
                    UPDATE {q}
                    SET "{n}" =
                        DATE_TRUNC('quarter', "{c}") = DATE_TRUNC('day', "{c}")
                """)
                sample = await self._fetch_sample(working_table, schema, [c, n])
                return self._success_response("Computed is_quarter_start", [column], [new_col], sample,
                                            new_table=working_table)

            elif isinstance(self.db, ClickHouseAdapter):
                working_table = await self._prepare_operation_table(
                    table, schema, backend=backend, data_id=data_id, new_table=new_table
                )
                new_col = self._generate_cleaned_column_name(column, "is_quarter_start")
                await self._add_new_column(working_table, schema, new_col, "BOOLEAN")

                q = self._qualified_table(working_table, schema)
                c = SQLIdentifierSanitizer.sanitize(column)
                n = SQLIdentifierSanitizer.sanitize(new_col)

                col_type = await self._get_column_type(working_table, schema, column)
                if "timestamp" in col_type.lower() or "date" in col_type.lower() or "datetime" in col_type.lower():
                    base_expr = f'"{c}"'
                else:
                    base_expr = f'CAST("{c}" AS DateTime)'

                await self._exec(f"""
                    ALTER TABLE {q}
                    UPDATE "{n}" = toStartOfQuarter({base_expr}) = toStartOfDay({base_expr})
                    WHERE 1
                """)
                sample = await self._fetch_sample(working_table, schema, [c, n])
                return self._success_response("Computed is_quarter_start", [column], [new_col], sample,
                                            new_table=working_table)
            else:
                raise self._unsupported_backend_error()

        except Exception as e:
            return self._error_response(str(e), [column])

    async def is_quarter_end(self, table: str, schema: str, column: str,
                             backend=None, data_id=None, new_table=None) -> Dict[str, Any]:
        try:
            if isinstance(self.db, PostgresAdapter) or isinstance(self.db, DuckDBAdapter):
                working_table = await self._prepare_operation_table(
                    table, schema, backend=backend, data_id=data_id, new_table=new_table
                )
                new_col = self._generate_cleaned_column_name(column, "is_quarter_end")
                await self._add_new_column(working_table, schema, new_col, "BOOLEAN")

                q = self._qualified_table(working_table, schema)
                c = SQLIdentifierSanitizer.sanitize(column)
                n = SQLIdentifierSanitizer.sanitize(new_col)

                await self._exec(f"""
                    UPDATE {q}
                    SET "{n}" =
                        DATE_TRUNC('quarter', "{c}" + INTERVAL '1 day')
                        != DATE_TRUNC('quarter', "{c}")
                """)
                sample = await self._fetch_sample(working_table, schema, [c, n])
                return self._success_response("Computed is_quarter_end", [column], [new_col], sample,
                                            new_table=working_table)

            elif isinstance(self.db, ClickHouseAdapter):
                working_table = await self._prepare_operation_table(
                    table, schema, backend=backend, data_id=data_id, new_table=new_table
                )
                new_col = self._generate_cleaned_column_name(column, "is_quarter_end")
                await self._add_new_column(working_table, schema, new_col, "BOOLEAN")

                q = self._qualified_table(working_table, schema)
                c = SQLIdentifierSanitizer.sanitize(column)
                n = SQLIdentifierSanitizer.sanitize(new_col)

                col_type = await self._get_column_type(working_table, schema, column)
                if "timestamp" in col_type.lower() or "date" in col_type.lower() or "datetime" in col_type.lower():
                    base_expr = f'"{c}"'
                else:
                    base_expr = f'CAST("{c}" AS DateTime)'

                await self._exec(f"""
                    ALTER TABLE {q}
                    UPDATE "{n}" = toStartOfQuarter({base_expr} + INTERVAL 1 DAY) != toStartOfQuarter({base_expr})
                    WHERE 1
                """)
                sample = await self._fetch_sample(working_table, schema, [c, n])
                return self._success_response("Computed is_quarter_end", [column], [new_col], sample,
                                            new_table=working_table)
            else:
                raise self._unsupported_backend_error()

        except Exception as e:
            return self._error_response(str(e), [column])

    async def is_weekend(self, table: str, schema: str, column: str,
                         backend=None, data_id=None, new_table=None) -> Dict[str, Any]:
        try:
            if isinstance(self.db, PostgresAdapter) or isinstance(self.db, DuckDBAdapter):
                working_table = await self._prepare_operation_table(
                    table, schema, backend=backend, data_id=data_id, new_table=new_table
                )
                new_col = self._generate_cleaned_column_name(column, "is_weekend")
                await self._add_new_column(working_table, schema, new_col, "BOOLEAN")

                q = self._qualified_table(working_table, schema)
                c = SQLIdentifierSanitizer.sanitize(column)
                n = SQLIdentifierSanitizer.sanitize(new_col)

                await self._exec(f"""
                    UPDATE {q}
                    SET "{n}" = EXTRACT(DOW FROM "{c}") IN (0, 6)
                """)
                sample = await self._fetch_sample(working_table, schema, [c, n])
                return self._success_response("Computed is_weekend", [column], [new_col], sample,
                                            new_table=working_table)

            elif isinstance(self.db, ClickHouseAdapter):
                working_table = await self._prepare_operation_table(
                    table, schema, backend=backend, data_id=data_id, new_table=new_table
                )
                new_col = self._generate_cleaned_column_name(column, "is_weekend")
                await self._add_new_column(working_table, schema, new_col, "BOOLEAN")

                q = self._qualified_table(working_table, schema)
                c = SQLIdentifierSanitizer.sanitize(column)
                n = SQLIdentifierSanitizer.sanitize(new_col)

                col_type = await self._get_column_type(working_table, schema, column)
                if "timestamp" in col_type.lower() or "date" in col_type.lower() or "datetime" in col_type.lower():
                    base_expr = f'"{c}"'
                else:
                    base_expr = f'CAST("{c}" AS DateTime)'

                await self._exec(f"""
                    ALTER TABLE {q}
                    UPDATE "{n}" = toDayOfWeek({base_expr}) IN (6, 7)
                    WHERE 1
                """)
                sample = await self._fetch_sample(working_table, schema, [c, n])
                return self._success_response("Computed is_weekend", [column], [new_col], sample,
                                            new_table=working_table)
            else:
                raise self._unsupported_backend_error()

        except Exception as e:
            return self._error_response(str(e), [column])

    async def is_weekday(self, table: str, schema: str, column: str,
                         backend=None, data_id=None, new_table=None) -> Dict[str, Any]:
        try:
            if isinstance(self.db, PostgresAdapter) or isinstance(self.db, DuckDBAdapter):
                working_table = await self._prepare_operation_table(
                    table, schema, backend=backend, data_id=data_id, new_table=new_table
                )
                new_col = self._generate_cleaned_column_name(column, "is_weekday")
                await self._add_new_column(working_table, schema, new_col, "BOOLEAN")

                q = self._qualified_table(working_table, schema)
                c = SQLIdentifierSanitizer.sanitize(column)
                n = SQLIdentifierSanitizer.sanitize(new_col)

                await self._exec(f"""
                    UPDATE {q}
                    SET "{n}" = EXTRACT(DOW FROM "{c}") BETWEEN 1 AND 5
                """)
                sample = await self._fetch_sample(working_table, schema, [c, n])
                return self._success_response("Computed is_weekday", [column], [new_col], sample,
                                            new_table=working_table)

            elif isinstance(self.db, ClickHouseAdapter):
                working_table = await self._prepare_operation_table(
                    table, schema, backend=backend, data_id=data_id, new_table=new_table
                )
                new_col = self._generate_cleaned_column_name(column, "is_weekday")
                await self._add_new_column(working_table, schema, new_col, "BOOLEAN")

                q = self._qualified_table(working_table, schema)
                c = SQLIdentifierSanitizer.sanitize(column)
                n = SQLIdentifierSanitizer.sanitize(new_col)

                col_type = await self._get_column_type(working_table, schema, column)
                if "timestamp" in col_type.lower() or "date" in col_type.lower() or "datetime" in col_type.lower():
                    base_expr = f'"{c}"'
                else:
                    base_expr = f'CAST("{c}" AS DateTime)'

                await self._exec(f"""
                    ALTER TABLE {q}
                    UPDATE "{n}" = toDayOfWeek({base_expr}) BETWEEN 1 AND 5
                    WHERE 1
                """)
                sample = await self._fetch_sample(working_table, schema, [c, n])
                return self._success_response("Computed is_weekday", [column], [new_col], sample,
                                            new_table=working_table)
            else:
                raise self._unsupported_backend_error()

        except Exception as e:
            return self._error_response(str(e), [column])

    async def is_business_day(self, table: str, schema: str, column: str,
                              backend=None, data_id=None, new_table=None) -> Dict[str, Any]:
        try:
            if isinstance(self.db, PostgresAdapter) or isinstance(self.db, DuckDBAdapter):
                working_table = await self._prepare_operation_table(
                    table, schema, backend=backend, data_id=data_id, new_table=new_table
                )
                new_col = self._generate_cleaned_column_name(column, "is_business_day")
                await self._add_new_column(working_table, schema, new_col, "BOOLEAN")

                q = self._qualified_table(working_table, schema)
                c = SQLIdentifierSanitizer.sanitize(column)
                n = SQLIdentifierSanitizer.sanitize(new_col)

                await self._exec(f"""
                    UPDATE {q}
                    SET "{n}" = EXTRACT(DOW FROM "{c}") BETWEEN 1 AND 5
                """)
                sample = await self._fetch_sample(working_table, schema, [c, n])
                return self._success_response("Computed is_business_day", [column], [new_col], sample,
                                            new_table=working_table)

            elif isinstance(self.db, ClickHouseAdapter):
                working_table = await self._prepare_operation_table(
                    table, schema, backend=backend, data_id=data_id, new_table=new_table
                )
                new_col = self._generate_cleaned_column_name(column, "is_business_day")
                await self._add_new_column(working_table, schema, new_col, "BOOLEAN")

                q = self._qualified_table(working_table, schema)
                c = SQLIdentifierSanitizer.sanitize(column)
                n = SQLIdentifierSanitizer.sanitize(new_col)

                col_type = await self._get_column_type(working_table, schema, column)
                if "timestamp" in col_type.lower() or "date" in col_type.lower() or "datetime" in col_type.lower():
                    base_expr = f'"{c}"'
                else:
                    base_expr = f'CAST("{c}" AS DateTime)'

                await self._exec(f"""
                    ALTER TABLE {q}
                    UPDATE "{n}" = toDayOfWeek({base_expr}) BETWEEN 1 AND 5
                    WHERE 1
                """)
                sample = await self._fetch_sample(working_table, schema, [c, n])
                return self._success_response("Computed is_business_day", [column], [new_col], sample,
                                            new_table=working_table)
            else:
                raise self._unsupported_backend_error()

        except Exception as e:
            return self._error_response(str(e), [column])

    async def days_in_month(self, table: str, schema: str, column: str,
                            backend=None, data_id=None, new_table=None) -> Dict[str, Any]:
        try:
            if isinstance(self.db, PostgresAdapter) or isinstance(self.db, DuckDBAdapter):
                working_table = await self._prepare_operation_table(
                    table, schema, backend=backend, data_id=data_id, new_table=new_table
                )
                new_col = self._generate_cleaned_column_name(column, "days_in_month")
                await self._add_new_column(working_table, schema, new_col, "INTEGER")

                q = self._qualified_table(working_table, schema)
                c = SQLIdentifierSanitizer.sanitize(column)
                n = SQLIdentifierSanitizer.sanitize(new_col)

                await self._exec(f"""
                    UPDATE {q}
                    SET "{n}" =
                        EXTRACT(DAY FROM (DATE_TRUNC('month', "{c}") + INTERVAL '1 month - 1 day'))
                """)
                sample = await self._fetch_sample(working_table, schema, [c, n])
                return self._success_response("Computed days_in_month", [column], [new_col], sample,
                                            new_table=working_table)

            elif isinstance(self.db, ClickHouseAdapter):
                working_table = await self._prepare_operation_table(
                    table, schema, backend=backend, data_id=data_id, new_table=new_table
                )
                new_col = self._generate_cleaned_column_name(column, "days_in_month")
                await self._add_new_column(working_table, schema, new_col, "INTEGER")

                q = self._qualified_table(working_table, schema)
                c = SQLIdentifierSanitizer.sanitize(column)
                n = SQLIdentifierSanitizer.sanitize(new_col)

                col_type = await self._get_column_type(working_table, schema, column)
                if "timestamp" in col_type.lower() or "date" in col_type.lower() or "datetime" in col_type.lower():
                    base_expr = f'"{c}"'
                else:
                    base_expr = f'CAST("{c}" AS DateTime)'

                await self._exec(f"""
                    ALTER TABLE {q}
                    UPDATE "{n}" = toDayOfMonth(toLastDayOfMonth({base_expr}))
                    WHERE 1
                """)
                sample = await self._fetch_sample(working_table, schema, [c, n])
                return self._success_response("Computed days_in_month", [column], [new_col], sample,
                                            new_table=working_table)
            else:
                raise self._unsupported_backend_error()

        except Exception as e:
            return self._error_response(str(e), [column])

    async def week_of_month(self, table: str, schema: str, column: str,
                            backend=None, data_id=None, new_table=None) -> Dict[str, Any]:
        try:
            if isinstance(self.db, PostgresAdapter) or isinstance(self.db, DuckDBAdapter):
                working_table = await self._prepare_operation_table(
                    table, schema, backend=backend, data_id=data_id, new_table=new_table
                )
                new_col = self._generate_cleaned_column_name(column, "week_of_month")
                await self._add_new_column(working_table, schema, new_col, "INTEGER")

                q = self._qualified_table(working_table, schema)
                c = SQLIdentifierSanitizer.sanitize(column)
                n = SQLIdentifierSanitizer.sanitize(new_col)

                await self._exec(f"""
                    UPDATE {q}
                    SET "{n}" =
                        EXTRACT(WEEK FROM "{c}")
                        - EXTRACT(WEEK FROM DATE_TRUNC('month', "{c}"))
                        + 1
                """)
                sample = await self._fetch_sample(working_table, schema, [c, n])
                return self._success_response("Computed week_of_month", [column], [new_col], sample,
                                            new_table=working_table)

            elif isinstance(self.db, ClickHouseAdapter):
                working_table = await self._prepare_operation_table(
                    table, schema, backend=backend, data_id=data_id, new_table=new_table
                )
                new_col = self._generate_cleaned_column_name(column, "week_of_month")
                await self._add_new_column(working_table, schema, new_col, "INTEGER")

                q = self._qualified_table(working_table, schema)
                c = SQLIdentifierSanitizer.sanitize(column)
                n = SQLIdentifierSanitizer.sanitize(new_col)

                col_type = await self._get_column_type(working_table, schema, column)
                if "timestamp" in col_type.lower() or "date" in col_type.lower() or "datetime" in col_type.lower():
                    base_expr = f'"{c}"'
                else:
                    base_expr = f'CAST("{c}" AS DateTime)'

                await self._exec(f"""
                    ALTER TABLE {q}
                    UPDATE "{n}" = toWeek({base_expr}) - toWeek(toStartOfMonth({base_expr})) + 1
                    WHERE 1
                """)
                sample = await self._fetch_sample(working_table, schema, [c, n])
                return self._success_response("Computed week_of_month", [column], [new_col], sample,
                                            new_table=working_table)
            else:
                raise self._unsupported_backend_error()

        except Exception as e:
            return self._error_response(str(e), [column])

    async def fromtimestamp(self, table: str, schema: str,
                            column: Optional[str] = None,
                            value: Optional[float] = None,
                            tz: Optional[str] = None,
                            backend=None, data_id=None, new_table=None) -> Dict[str, Any]:
        try:
            if column is None and value is None:
                return self._error_response("Provide either column or value")

            if isinstance(self.db, PostgresAdapter) or isinstance(self.db, DuckDBAdapter):
                working_table = await self._prepare_operation_table(
                    table, schema, backend=backend, data_id=data_id, new_table=new_table
                )

                source = f'"{SQLIdentifierSanitizer.sanitize(column)}"' if column else str(value)
                new_col = self._generate_cleaned_column_name(column or "value", "fromtimestamp")
                await self._add_new_column(working_table, schema, new_col, "TIMESTAMP")

                qualified = self._qualified_table(working_table, schema)
                safe_new = SQLIdentifierSanitizer.sanitize(new_col)

                expr = f"TO_TIMESTAMP({source})"
                if tz:
                    expr = f"{expr} AT TIME ZONE '{tz}'"

                await self._exec(f"""
                    UPDATE {qualified}
                    SET "{safe_new}" = {expr}
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

            elif isinstance(self.db, ClickHouseAdapter):
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
                    WHERE 1
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
            else:
                raise self._unsupported_backend_error()

        except Exception as e:
            return self._error_response(f"fromtimestamp error: {str(e)}", [], [])


    async def timestamp(self, table: str, schema: str, column: str,
                        backend=None, data_id=None, new_table=None) -> Dict[str, Any]:
        try:
            if isinstance(self.db, PostgresAdapter) or isinstance(self.db, DuckDBAdapter):
                working_table = await self._prepare_operation_table(
                    table, schema, backend=backend, data_id=data_id, new_table=new_table
                )
                new_col = self._generate_cleaned_column_name(column, "timestamp")
                await self._add_new_column(working_table, schema, new_col, "DOUBLE PRECISION")

                qualified = self._qualified_table(working_table, schema)
                safe_col = SQLIdentifierSanitizer.sanitize(column)
                safe_new = SQLIdentifierSanitizer.sanitize(new_col)

                if isinstance(self.db, PostgresAdapter):
                    expr = f'EXTRACT(EPOCH FROM "{safe_col}")'
                elif isinstance(self.db, DuckDBAdapter):
                    expr = f'epoch("{safe_col}")'
                else:
                    raise self._unsupported_backend_error()

                await self._exec(f"""UPDATE {qualified} SET "{safe_new}" = {expr}""")
                sample = await self._fetch_sample(working_table, schema, columns=[safe_col, safe_new])
                return self._success_response(f"Converted '{column}' to POSIX timestamp",
                                            [column], [new_col], sample, new_table=working_table)

            elif isinstance(self.db, ClickHouseAdapter):
                working_table = await self._prepare_operation_table(
                    table, schema, backend=backend, data_id=data_id, new_table=new_table
                )
                new_col = self._generate_cleaned_column_name(column, "timestamp")
                await self._add_new_column(working_table, schema, new_col, "DOUBLE PRECISION")

                qualified = self._qualified_table(working_table, schema)
                safe_col = SQLIdentifierSanitizer.sanitize(column)
                safe_new = SQLIdentifierSanitizer.sanitize(new_col)

                col_type = await self._get_column_type(working_table, schema, column)
                if "timestamp" in col_type.lower() or "date" in col_type.lower() or "datetime" in col_type.lower():
                    base_expr = f'"{safe_col}"'
                else:
                    base_expr = f'CAST("{safe_col}" AS DateTime)'

                expr = f'toUnixTimestamp({base_expr})'

                await self._exec(f"""ALTER TABLE {qualified} UPDATE "{safe_new}" = {expr} WHERE 1""")
                sample = await self._fetch_sample(working_table, schema, columns=[safe_col, safe_new])
                return self._success_response(f"Converted '{column}' to POSIX timestamp",
                                            [column], [new_col], sample, new_table=working_table)
            else:
                raise self._unsupported_backend_error()

        except Exception as e:
            return self._error_response(f"timestamp error: {str(e)}", [column], [])

    async def strftime(self, table: str, schema: str, column: str, fmt: str,
                       backend=None, data_id=None, new_table=None) -> Dict[str, Any]:
        try:
            if isinstance(self.db, PostgresAdapter) or isinstance(self.db, DuckDBAdapter):
                working_table = await self._prepare_operation_table(
                    table, schema, backend=backend, data_id=data_id, new_table=new_table
                )
                new_col = self._generate_cleaned_column_name(column, "strftime")
                await self._add_new_column(working_table, schema, new_col, "TEXT")

                qualified = self._qualified_table(working_table, schema)
                safe_col = SQLIdentifierSanitizer.sanitize(column)
                safe_new = SQLIdentifierSanitizer.sanitize(new_col)

                col_type = await self._get_column_type(table, schema, column)
                if "timestamp" in col_type.lower() or "date" in col_type.lower():
                    base_expr = f'"{safe_col}"'
                else:
                    base_expr = f'CAST("{safe_col}" AS TIMESTAMP)'

                if isinstance(self.db, PostgresAdapter):
                    sql_fmt = self._convert_strftime_format(fmt)
                    expr = f"TO_CHAR({base_expr}, '{sql_fmt}')"
                elif isinstance(self.db, DuckDBAdapter):
                    expr = f"strftime('{fmt}', {base_expr})"
                else:
                    raise self._unsupported_backend_error()

                await self._exec(f"""UPDATE {qualified} SET "{safe_new}" = {expr}""")
                sample = await self._fetch_sample(working_table, schema, [safe_col, safe_new])
                return self._success_response(f"Formatted '{column}' using '{fmt}'",
                                            [column], [new_col], sample, format=fmt,
                                            new_table=working_table)

            elif isinstance(self.db, ClickHouseAdapter):
                working_table = await self._prepare_operation_table(
                    table, schema, backend=backend, data_id=data_id, new_table=new_table
                )
                new_col = self._generate_cleaned_column_name(column, "strftime")
                await self._add_new_column(working_table, schema, new_col, "TEXT")

                qualified = self._qualified_table(working_table, schema)
                safe_col = SQLIdentifierSanitizer.sanitize(column)
                safe_new = SQLIdentifierSanitizer.sanitize(new_col)

                col_type = await self._get_column_type(table, schema, column)
                if "timestamp" in col_type.lower() or "date" in col_type.lower() or "datetime" in col_type.lower():
                    base_expr = f'"{safe_col}"'
                else:
                    base_expr = f'CAST("{safe_col}" AS DateTime)'

                expr = f"formatDateTime({base_expr}, '{fmt}')"

                await self._exec(f"""ALTER TABLE {qualified} UPDATE "{safe_new}" = {expr} WHERE 1""")
                sample = await self._fetch_sample(working_table, schema, [safe_col, safe_new])
                return self._success_response(f"Formatted '{column}' using '{fmt}'",
                                            [column], [new_col], sample, format=fmt,
                                            new_table=working_table)
            else:
                raise self._unsupported_backend_error()

        except Exception as e:
            return self._error_response(f"strftime error: {str(e)}", [column], [])

    async def strptime(self, table: str, schema: str, column: str, fmt: str,
                       backend=None, data_id=None, new_table=None) -> Dict[str, Any]:
        try:
            if isinstance(self.db, PostgresAdapter) or isinstance(self.db, DuckDBAdapter):
                working_table = await self._prepare_operation_table(
                    table, schema, backend=backend, data_id=data_id, new_table=new_table
                )
                new_col = self._generate_cleaned_column_name(column, "strptime")
                await self._add_new_column(working_table, schema, new_col, "TIMESTAMP")

                qualified = self._qualified_table(working_table, schema)
                safe_col = SQLIdentifierSanitizer.sanitize(column)
                safe_new = SQLIdentifierSanitizer.sanitize(new_col)

                if isinstance(self.db, PostgresAdapter):
                    sql_fmt = self._convert_strftime_format(fmt)
                    expr = f"TO_TIMESTAMP(\"{safe_col}\", '{sql_fmt}')"
                elif isinstance(self.db, DuckDBAdapter):
                    expr = f"strptime(\"{safe_col}\", '{fmt}')"
                else:
                    raise self._unsupported_backend_error()

                await self._exec(f"""UPDATE {qualified} SET "{safe_new}" = {expr}""")
                sample = await self._fetch_sample(working_table, schema, [safe_col, safe_new])
                return self._success_response(f"Parsed '{column}' using format '{fmt}'",
                                            [column], [new_col], sample, format=fmt,
                                            new_table=working_table)

            elif isinstance(self.db, ClickHouseAdapter):
                working_table = await self._prepare_operation_table(
                    table, schema, backend=backend, data_id=data_id, new_table=new_table
                )
                new_col = self._generate_cleaned_column_name(column, "strptime")
                await self._add_new_column(working_table, schema, new_col, "TIMESTAMP")

                qualified = self._qualified_table(working_table, schema)
                safe_col = SQLIdentifierSanitizer.sanitize(column)
                safe_new = SQLIdentifierSanitizer.sanitize(new_col)

                expr = f"parseDateTimeBestEffort(\"{safe_col}\", '{fmt}')"

                await self._exec(f"""ALTER TABLE {qualified} UPDATE "{safe_new}" = {expr} WHERE 1""")
                sample = await self._fetch_sample(working_table, schema, [safe_col, safe_new])
                return self._success_response(f"Parsed '{column}' using format '{fmt}'",
                                            [column], [new_col], sample, format=fmt,
                                            new_table=working_table)
            else:
                raise self._unsupported_backend_error()

        except Exception as e:
            return self._error_response(f"strptime error: {str(e)}", [column], [])

    async def add_timedelta(self, table: str, schema: str, column: str, interval: str,
                            backend=None, data_id=None, new_table=None) -> Dict[str, Any]:
        try:
            if isinstance(self.db, PostgresAdapter) or isinstance(self.db, DuckDBAdapter):
                working_table = await self._prepare_operation_table(
                    table, schema, backend=backend, data_id=data_id, new_table=new_table
                )
                new_col = self._generate_cleaned_column_name(column, "add")
                await self._add_new_column(working_table, schema, new_col, "TIMESTAMP")

                q = self._qualified_table(working_table, schema)
                c = SQLIdentifierSanitizer.sanitize(column)
                n = SQLIdentifierSanitizer.sanitize(new_col)

                await self._exec(f"""UPDATE {q} SET "{n}" = "{c}" + INTERVAL '{interval}'""")
                sample = await self._fetch_sample(working_table, schema, [c, n])
                return self._success_response(f"Added interval '{interval}' to '{column}'",
                                            [column], [new_col], sample, interval=interval,
                                            new_table=working_table)

            elif isinstance(self.db, ClickHouseAdapter):
                working_table = await self._prepare_operation_table(
                    table, schema, backend=backend, data_id=data_id, new_table=new_table
                )
                new_col = self._generate_cleaned_column_name(column, "add")
                await self._add_new_column(working_table, schema, new_col, "TIMESTAMP")

                q = self._qualified_table(working_table, schema)
                c = SQLIdentifierSanitizer.sanitize(column)
                n = SQLIdentifierSanitizer.sanitize(new_col)

                col_type = await self._get_column_type(working_table, schema, column)
                if "timestamp" in col_type.lower() or "date" in col_type.lower() or "datetime" in col_type.lower():
                    base_expr = f'"{c}"'
                else:
                    base_expr = f'CAST("{c}" AS DateTime)'

                ch_interval = interval.replace("'", "")
                await self._exec(f"""ALTER TABLE {q} UPDATE "{n}" = {base_expr} + INTERVAL {ch_interval} WHERE 1""")
                sample = await self._fetch_sample(working_table, schema, [c, n])
                return self._success_response(f"Added interval '{interval}' to '{column}'",
                                            [column], [new_col], sample, interval=interval,
                                            new_table=working_table)
            else:
                raise self._unsupported_backend_error()

        except Exception as e:
            return self._error_response(str(e), [column])

    async def sub_timedelta(self, table: str, schema: str, column: str, interval: str,
                            backend=None, data_id=None, new_table=None) -> Dict[str, Any]:
        try:
            if isinstance(self.db, PostgresAdapter) or isinstance(self.db, DuckDBAdapter):
                working_table = await self._prepare_operation_table(
                    table, schema, backend=backend, data_id=data_id, new_table=new_table
                )
                new_col = self._generate_cleaned_column_name(column, "sub")
                await self._add_new_column(working_table, schema, new_col, "TIMESTAMP")

                q = self._qualified_table(working_table, schema)
                c = SQLIdentifierSanitizer.sanitize(column)
                n = SQLIdentifierSanitizer.sanitize(new_col)

                await self._exec(f"""UPDATE {q} SET "{n}" = "{c}" - INTERVAL '{interval}'""")
                sample = await self._fetch_sample(working_table, schema, [c, n])
                return self._success_response(f"Subtracted interval '{interval}' from '{column}'",
                                            [column], [new_col], sample, interval=interval,
                                            new_table=working_table)

            elif isinstance(self.db, ClickHouseAdapter):
                working_table = await self._prepare_operation_table(
                    table, schema, backend=backend, data_id=data_id, new_table=new_table
                )
                new_col = self._generate_cleaned_column_name(column, "sub")
                await self._add_new_column(working_table, schema, new_col, "TIMESTAMP")

                q = self._qualified_table(working_table, schema)
                c = SQLIdentifierSanitizer.sanitize(column)
                n = SQLIdentifierSanitizer.sanitize(new_col)

                col_type = await self._get_column_type(working_table, schema, column)
                if "timestamp" in col_type.lower() or "date" in col_type.lower() or "datetime" in col_type.lower():
                    base_expr = f'"{c}"'
                else:
                    base_expr = f'CAST("{c}" AS DateTime)'

                ch_interval = interval.replace("'", "")
                await self._exec(f"""ALTER TABLE {q} UPDATE "{n}" = {base_expr} - INTERVAL {ch_interval} WHERE 1""")
                sample = await self._fetch_sample(working_table, schema, [c, n])
                return self._success_response(f"Subtracted interval '{interval}' from '{column}'",
                                            [column], [new_col], sample, interval=interval,
                                            new_table=working_table)
            else:
                raise self._unsupported_backend_error()

        except Exception as e:
            return self._error_response(str(e), [column])

    async def replace(self, table: str, schema: str, column: str, 
                      backend=None, data_id=None, new_table=None,**kwargs,) -> Dict[str, Any]:
        try:
            allowed = {"year", "month", "day", "hour", "minute", "second"}
            for k in kwargs:
                if k not in allowed:
                    return self._error_response(f"Unsupported field: {k}", [column])

            if isinstance(self.db, PostgresAdapter) or isinstance(self.db, DuckDBAdapter):
                working_table = await self._prepare_operation_table(
                    table, schema, backend=backend, data_id=data_id, new_table=new_table
                )
                new_col = self._generate_cleaned_column_name(column, "replace")
                await self._add_new_column(working_table, schema, new_col, "TIMESTAMP")

                q = self._qualified_table(working_table, schema)
                c = SQLIdentifierSanitizer.sanitize(column)
                n = SQLIdentifierSanitizer.sanitize(new_col)

                def part(field):
                    return kwargs.get(field, f'EXTRACT({field.upper()} FROM "{c}")')

                expr = f"""
                    MAKE_TIMESTAMP(
                        {part("year")},
                        {part("month")},
                        {part("day")},
                        {part("hour")},
                        {part("minute")},
                        {part("second")}
                    )
                """
                await self._exec(f"""UPDATE {q} SET "{n}" = {expr}""")
                sample = await self._fetch_sample(working_table, schema, [c, n])
                return self._success_response(f"Replaced fields in '{column}'",
                                            [column], [new_col], sample, replaced_fields=kwargs,
                                            new_table=working_table)

            elif isinstance(self.db, ClickHouseAdapter):
                working_table = await self._prepare_operation_table(
                    table, schema, backend=backend, data_id=data_id, new_table=new_table
                )
                new_col = self._generate_cleaned_column_name(column, "replace")
                await self._add_new_column(working_table, schema, new_col, "TIMESTAMP")

                q = self._qualified_table(working_table, schema)
                c = SQLIdentifierSanitizer.sanitize(column)
                n = SQLIdentifierSanitizer.sanitize(new_col)

                ch_func_map = {
                    "year": "toYear", "month": "toMonth", "day": "toDayOfMonth",
                    "hour": "toHour", "minute": "toMinute", "second": "toSecond"
                }

                col_type = await self._get_column_type(working_table, schema, column)
                if "timestamp" in col_type.lower() or "date" in col_type.lower() or "datetime" in col_type.lower():
                    base_expr = f'"{c}"'
                else:
                    base_expr = f'CAST("{c}" AS DateTime)'

                def part(field):
                    return kwargs.get(field, f'{ch_func_map[field]}({base_expr})')

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
                await self._exec(f"""ALTER TABLE {q} UPDATE "{n}" = {expr} WHERE 1""")
                sample = await self._fetch_sample(working_table, schema, [c, n])
                return self._success_response(f"Replaced fields in '{column}'",
                                            [column], [new_col], sample, replaced_fields=kwargs,
                                            new_table=working_table)
            else:
                raise self._unsupported_backend_error()

        except Exception as e:
            return self._error_response(str(e), [column])

    async def normalize(self, table: str, schema: str, column: str,
                        backend=None, data_id=None, new_table=None) -> Dict[str, Any]:
        try:
            if isinstance(self.db, PostgresAdapter) or isinstance(self.db, DuckDBAdapter):
                working_table = await self._prepare_operation_table(
                    table, schema, backend=backend, data_id=data_id, new_table=new_table
                )
                new_col = self._generate_cleaned_column_name(column, "normalize")
                await self._add_new_column(working_table, schema, new_col, "TIMESTAMP")

                q = self._qualified_table(working_table, schema)
                c = SQLIdentifierSanitizer.sanitize(column)
                n = SQLIdentifierSanitizer.sanitize(new_col)

                await self._exec(f"""UPDATE {q} SET "{n}" = DATE_TRUNC('day', "{c}")""")
                sample = await self._fetch_sample(working_table, schema, [c, n])
                return self._success_response(f"Normalized '{column}' to day",
                                            [column], [new_col], sample, new_table=working_table)

            elif isinstance(self.db, ClickHouseAdapter):
                working_table = await self._prepare_operation_table(
                    table, schema, backend=backend, data_id=data_id, new_table=new_table
                )
                new_col = self._generate_cleaned_column_name(column, "normalize")
                await self._add_new_column(working_table, schema, new_col, "TIMESTAMP")

                q = self._qualified_table(working_table, schema)
                c = SQLIdentifierSanitizer.sanitize(column)
                n = SQLIdentifierSanitizer.sanitize(new_col)

                col_type = await self._get_column_type(working_table, schema, column)
                if "timestamp" in col_type.lower() or "date" in col_type.lower() or "datetime" in col_type.lower():
                    base_expr = f'"{c}"'
                else:
                    base_expr = f'CAST("{c}" AS DateTime)'

                await self._exec(f"""ALTER TABLE {q} UPDATE "{n}" = toStartOfDay({base_expr}) WHERE 1""")
                sample = await self._fetch_sample(working_table, schema, [c, n])
                return self._success_response(f"Normalized '{column}' to day",
                                            [column], [new_col], sample, new_table=working_table)
            else:
                raise self._unsupported_backend_error()

        except Exception as e:
            return self._error_response(str(e), [column])