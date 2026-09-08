from typing import Any, Dict, List, Optional
import traceback
from datetime import datetime, timezone
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
            # ponytail: "result" key keeps is_operation_response() true so the
            # ContextManager proxy raises OperationError instead of leaking dicts.
            "result": None,
        }

    def _unsupported_backend_error(self) -> NotImplementedError:
        return NotImplementedError(
            f"Unsupported database backend for datetime operation: {self.db.__class__.__name__}"
        )

    @staticmethod
    def _dt_literal(value: str) -> str:
        # ponytail: orchestrator pre-validates datetime strings; escape quotes only.
        return "'" + str(value).replace("'", "''") + "'"

    async def _datetime_base_expr(self, working_table: str, schema: str, column: str) -> str:
        """ClickHouse base expression: raw col if date-like else CAST to DateTime."""
        safe_col = SQLIdentifierSanitizer.sanitize(column)
        col_type = await self._get_column_type(working_table, schema, column)
        if "timestamp" in col_type.lower() or "date" in col_type.lower() or "datetime" in col_type.lower():
            return f'"{safe_col}"'
        return f'CAST("{safe_col}" AS DateTime)'

    async def _prepare_filtered_table(
        self,
        table: str,
        schema: str,
        where: str,
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
        await self._exec(f"CREATE TABLE {qualified_target} AS SELECT * FROM {qualified_source} WHERE {where}")
        return output_table

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
                    "week": "toISOWeek", "weekofyear": "toISOWeek",
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
                    WHERE 1 SETTINGS mutations_sync = 1
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
                    WHERE 1 SETTINGS mutations_sync = 1
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
                    WHERE 1 SETTINGS mutations_sync = 1
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

                # ponytail: tz-aware input (TIMESTAMPTZ) converts with a single
                # AT TIME ZONE; the double conversion below is only for naive.
                col_type = await self._get_column_type(working_table, schema, column)
                is_aware = "timestamptz" in col_type.lower() or "with time zone" in col_type.lower()

                if tz is None:
                    await self._exec(f"""
                        UPDATE {qualified}
                        SET "{safe_new}" = ("{safe_col}" AT TIME ZONE 'UTC')
                    """)
                    msg = f"Converted '{column}' to UTC and removed timezone"
                elif is_aware:
                    await self._exec(f"""
                        UPDATE {qualified}
                        SET "{safe_new}" = "{safe_col}" AT TIME ZONE '{tz}'
                    """)
                    msg = f"Converted '{column}' timezone to '{tz}'"
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

                await self._exec(f"""ALTER TABLE {q} UPDATE "{n}" = toDayOfMonth({base_expr}) = 1 WHERE 1 SETTINGS mutations_sync = 1""")
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
                    WHERE 1 SETTINGS mutations_sync = 1
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
                    WHERE 1 SETTINGS mutations_sync = 1
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
                    WHERE 1 SETTINGS mutations_sync = 1
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
                    WHERE 1 SETTINGS mutations_sync = 1
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
                    WHERE 1 SETTINGS mutations_sync = 1
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
                    WHERE 1 SETTINGS mutations_sync = 1
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
                    WHERE 1 SETTINGS mutations_sync = 1
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
                    WHERE 1 SETTINGS mutations_sync = 1
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
                    WHERE 1 SETTINGS mutations_sync = 1
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
                    WHERE 1 SETTINGS mutations_sync = 1
                """)
                sample = await self._fetch_sample(working_table, schema, [c, n])
                return self._success_response("Computed week_of_month", [column], [new_col], sample,
                                            new_table=working_table)
            else:
                raise self._unsupported_backend_error()

        except Exception as e:
            return self._error_response(str(e), [column])

    # ==================================================================
    #  WAVE 1 — names, durations, parsing, filtering
    # ==================================================================
    async def day_name(self, table: str, schema: str, column: str,
                       backend=None, data_id=None, new_table=None) -> Dict[str, Any]:
        try:
            if isinstance(self.db, PostgresAdapter) or isinstance(self.db, DuckDBAdapter):
                working_table = await self._prepare_operation_table(
                    table, schema, backend=backend, data_id=data_id, new_table=new_table
                )
                new_col = self._generate_cleaned_column_name(column, "day_name")
                await self._add_new_column(working_table, schema, new_col, "TEXT")

                qualified = self._qualified_table(working_table, schema)
                safe_col = SQLIdentifierSanitizer.sanitize(column)
                safe_new = SQLIdentifierSanitizer.sanitize(new_col)

                col_type = await self._get_column_type(working_table, schema, column)
                if "timestamp" in col_type.lower() or "date" in col_type.lower():
                    base_expr = f'"{safe_col}"'
                else:
                    base_expr = f'CAST("{safe_col}" AS TIMESTAMP)'

                expr = f"TO_CHAR({base_expr}, 'FMDay')" if isinstance(self.db, PostgresAdapter) else f"DAYNAME({base_expr})"
                await self._exec(f"""UPDATE {qualified} SET "{safe_new}" = {expr}""")
                sample = await self._fetch_sample(working_table, schema, [safe_col, safe_new])
                return self._success_response(f"Extracted day name from '{column}'",
                                              [column], [new_col], sample, new_table=working_table)

            elif isinstance(self.db, ClickHouseAdapter):
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
            else:
                raise self._unsupported_backend_error()

        except Exception as e:
            return self._error_response(f"day_name error: {str(e)}", [column], [])

    async def month_name(self, table: str, schema: str, column: str,
                         backend=None, data_id=None, new_table=None) -> Dict[str, Any]:
        try:
            if isinstance(self.db, PostgresAdapter) or isinstance(self.db, DuckDBAdapter):
                working_table = await self._prepare_operation_table(
                    table, schema, backend=backend, data_id=data_id, new_table=new_table
                )
                new_col = self._generate_cleaned_column_name(column, "month_name")
                await self._add_new_column(working_table, schema, new_col, "TEXT")

                qualified = self._qualified_table(working_table, schema)
                safe_col = SQLIdentifierSanitizer.sanitize(column)
                safe_new = SQLIdentifierSanitizer.sanitize(new_col)

                col_type = await self._get_column_type(working_table, schema, column)
                if "timestamp" in col_type.lower() or "date" in col_type.lower():
                    base_expr = f'"{safe_col}"'
                else:
                    base_expr = f'CAST("{safe_col}" AS TIMESTAMP)'

                expr = f"TO_CHAR({base_expr}, 'FMMonth')" if isinstance(self.db, PostgresAdapter) else f"MONTHNAME({base_expr})"
                await self._exec(f"""UPDATE {qualified} SET "{safe_new}" = {expr}""")
                sample = await self._fetch_sample(working_table, schema, [safe_col, safe_new])
                return self._success_response(f"Extracted month name from '{column}'",
                                              [column], [new_col], sample, new_table=working_table)

            elif isinstance(self.db, ClickHouseAdapter):
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
            else:
                raise self._unsupported_backend_error()

        except Exception as e:
            return self._error_response(f"month_name error: {str(e)}", [column], [])

    # ponytail: single canonical freq table for resample/asfreq (and any later
    # grid feature). Single-bucket units only; "2h"/"15min" fail loudly instead
    # of silently binning wrong. "MS" is month-start (pandas case-sensitive).
    _FREQ_ALIASES = {
        "s": "second", "second": "second", "seconds": "second",
        "t": "minute", "min": "minute", "minute": "minute", "minutes": "minute",
        "h": "hour", "hour": "hour", "hours": "hour",
        "d": "day", "day": "day", "days": "day",
        "w": "week", "week": "week", "weeks": "week",
        "m": "month", "me": "month", "month": "month", "months": "month",
        "ms": "month",
        "q": "quarter", "qe": "quarter", "quarter": "quarter", "quarters": "quarter",
        "a": "year", "y": "year", "ye": "year", "year": "year", "years": "year",
    }

    @classmethod
    def _parse_freq(cls, freq: str) -> str:
        key = str(freq).strip()
        if key == "MS":
            return "month"
        key = key.lower()
        if key not in cls._FREQ_ALIASES:
            raise ValueError(
                f"Unsupported freq: {freq!r} (use D, W, ME, QE, YE, h, min, s or full unit names)"
            )
        return cls._FREQ_ALIASES[key]

    _RESAMPLE_AGGS = {"count", "sum", "mean", "avg", "min", "max", "median", "std"}

    @classmethod
    def _normalize_agg_spec(cls, agg, value_columns):
        """Normalize agg spec to [(out_suffix, func, column|None)]; raises ValueError."""
        if isinstance(agg, str):
            funcs = [agg]
        elif isinstance(agg, (list, tuple)):
            funcs = list(agg)
        elif isinstance(agg, dict):
            specs = []
            for col, fns in agg.items():
                for fn in (fns if isinstance(fns, (list, tuple)) else [fns]):
                    fn = str(fn).lower()
                    if fn not in cls._RESAMPLE_AGGS:
                        raise ValueError(f"Unsupported agg: {fn!r}")
                    if fn == "count":
                        specs.append((f"{col}_count", "count", str(col)))
                    else:
                        specs.append((f"{col}_{fn}", fn, str(col)))
            if not specs:
                raise ValueError("agg dict must not be empty")
            return specs
        else:
            raise ValueError(f"agg must be a string, list or dict, got {agg!r}")

        normed = []
        for fn in funcs:
            fn = str(fn).lower()
            if fn not in cls._RESAMPLE_AGGS:
                raise ValueError(f"Unsupported agg: {fn!r}")
            if fn == "count":
                normed.append(("value", "count", None))
                continue
            if value_columns is None:
                raise ValueError(f"value_columns required for agg {fn!r}")
            cols = value_columns if isinstance(value_columns, (list, tuple)) else [value_columns]
            # ponytail: one function over one column keeps the legacy "value"
            # output name; anything wider gets value_<column>_<agg> names.
            if len(funcs) == 1 and len(cols) == 1:
                normed.append(("value", fn, str(cols[0])))
                continue
            for col in cols:
                normed.append((f"{col}_{fn}", fn, str(col)))
        if not normed:
            raise ValueError("agg must not be empty")
        return normed

    _DIFF_DIVISORS = {
        "millisecond": 0.001, "second": 1, "minute": 60, "hour": 3600,
        "day": 86400, "week": 604800,
        # ponytail: calendar months/quarters/years have no fixed length;
        # 30/91.25/365-day conventions, documented, pandas-grade precision later.
        "month": 2592000, "quarter": 7889400, "year": 31536000,
    }
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

            if isinstance(self.db, PostgresAdapter) or isinstance(self.db, DuckDBAdapter):
                working_table = await self._prepare_operation_table(
                    table, schema, backend=backend, data_id=data_id, new_table=new_table
                )
                new_col = SQLIdentifierSanitizer.sanitize(target_col) if target_col else self._generate_cleaned_column_name(f"{col1}_{col2}", f"diff_{unit}")
                await self._add_new_column(working_table, schema, new_col, "DOUBLE PRECISION")

                qualified = self._qualified_table(working_table, schema)
                safe_c1 = SQLIdentifierSanitizer.sanitize(col1)
                safe_c2 = SQLIdentifierSanitizer.sanitize(col2)
                safe_new = SQLIdentifierSanitizer.sanitize(new_col)
                divisor = self._DIFF_DIVISORS[unit]

                await self._exec(f"""UPDATE {qualified} SET "{safe_new}" = EXTRACT(EPOCH FROM ("{safe_c2}" - "{safe_c1}")) / {divisor}""")
                sample = await self._fetch_sample(working_table, schema, columns=[safe_c1, safe_c2, safe_new])
                return self._success_response(f"Computed {unit} difference '{col1}' → '{col2}'",
                                              [col1, col2], [new_col], sample, unit=unit,
                                              new_table=working_table)

            elif isinstance(self.db, ClickHouseAdapter):
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
            else:
                raise self._unsupported_backend_error()

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
                if isinstance(self.db, PostgresAdapter) or isinstance(self.db, DuckDBAdapter):
                    working_table = await self._prepare_operation_table(
                        table, schema, backend=backend, data_id=data_id, new_table=new_table
                    )
                    new_col = self._generate_cleaned_column_name(column, "todatetime")
                    await self._add_new_column(working_table, schema, new_col, "TIMESTAMP")

                    qualified = self._qualified_table(working_table, schema)
                    safe_col = SQLIdentifierSanitizer.sanitize(column)
                    safe_new = SQLIdentifierSanitizer.sanitize(new_col)
                    expr = f"TO_TIMESTAMP(CAST(\"{safe_col}\" AS DOUBLE PRECISION) / {divisor})"
                    if tz:
                        expr = f"{expr} AT TIME ZONE '{tz}'"
                    await self._exec(f"""UPDATE {qualified} SET "{safe_new}" = {expr}""")
                    cols = [safe_col, safe_new]
                    sample = await self._fetch_sample(working_table, schema, columns=cols)
                    return self._success_response("Converted epoch to datetime",
                                                  [column], [new_col], sample, unit=unit,
                                                  new_table=working_table)

                elif isinstance(self.db, ClickHouseAdapter):
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
                else:
                    raise self._unsupported_backend_error()

            if isinstance(self.db, PostgresAdapter) or isinstance(self.db, DuckDBAdapter):
                working_table = await self._prepare_operation_table(
                    table, schema, backend=backend, data_id=data_id, new_table=new_table
                )
                new_col = self._generate_cleaned_column_name(column, "todatetime")
                await self._add_new_column(working_table, schema, new_col, "TIMESTAMP")

                qualified = self._qualified_table(working_table, schema)
                safe_col = SQLIdentifierSanitizer.sanitize(column)
                safe_new = SQLIdentifierSanitizer.sanitize(new_col)

                if errors == "coerce":
                    if isinstance(self.db, DuckDBAdapter):
                        expr = f'TRY_CAST("{safe_col}" AS TIMESTAMP)'
                    else:
                        # ponytail: Postgres has no TRY_CAST; ISO-like guard, best-effort.
                        expr = f"""CASE WHEN TRIM("{safe_col}"::TEXT) ~ '^[0-9]{{4}}-[0-9]{{2}}-[0-9]{{2}}' THEN CAST("{safe_col}" AS TIMESTAMP) ELSE NULL END"""
                else:
                    expr = f'CAST("{safe_col}" AS TIMESTAMP)'
                if tz:
                    expr = f"({expr}) AT TIME ZONE '{tz}'"
                await self._exec(f"""UPDATE {qualified} SET "{safe_new}" = {expr}""")
                sample = await self._fetch_sample(working_table, schema, columns=[safe_col, safe_new])
                return self._success_response(f"Converted '{column}' to datetime",
                                              [column], [new_col], sample, new_table=working_table)

            elif isinstance(self.db, ClickHouseAdapter):
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
            else:
                raise self._unsupported_backend_error()

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

                await self._exec(f"""ALTER TABLE {qualified} UPDATE "{safe_new}" = {expr} WHERE 1 SETTINGS mutations_sync = 1""")
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

                await self._exec(f"""ALTER TABLE {qualified} UPDATE "{safe_new}" = {expr} WHERE 1 SETTINGS mutations_sync = 1""")
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

                await self._exec(f"""ALTER TABLE {qualified} UPDATE "{safe_new}" = {expr} WHERE 1 SETTINGS mutations_sync = 1""")
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
                await self._exec(f"""ALTER TABLE {q} UPDATE "{n}" = {base_expr} + INTERVAL {ch_interval} WHERE 1 SETTINGS mutations_sync = 1""")
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
                await self._exec(f"""ALTER TABLE {q} UPDATE "{n}" = {base_expr} - INTERVAL {ch_interval} WHERE 1 SETTINGS mutations_sync = 1""")
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
                    # ponytail: EXTRACT returns NUMERIC on Postgres; cast so
                    # MAKE_TIMESTAMP(int * 6) resolves on both backends.
                    return kwargs.get(field, f'EXTRACT({field.upper()} FROM "{c}")::INTEGER')

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
                await self._exec(f"""ALTER TABLE {q} UPDATE "{n}" = {expr} WHERE 1 SETTINGS mutations_sync = 1""")
                sample = await self._fetch_sample(working_table, schema, [c, n])
                return self._success_response(f"Replaced fields in '{column}'",
                                            [column], [new_col], sample, replaced_fields=kwargs,
                                            new_table=working_table)
            else:
                raise self._unsupported_backend_error()

        except Exception as e:
            return self._error_response(str(e), [column])

    async def between(self, table: str, schema: str, column: str, start: str, end: str,
                      backend=None, data_id=None, new_table=None) -> Dict[str, Any]:
        try:
            if isinstance(self.db, PostgresAdapter) or isinstance(self.db, DuckDBAdapter):
                safe_col = SQLIdentifierSanitizer.sanitize(column)
                where = f'CAST("{safe_col}" AS TIMESTAMP) BETWEEN CAST({self._dt_literal(start)} AS TIMESTAMP) AND CAST({self._dt_literal(end)} AS TIMESTAMP)'
                working_table = await self._prepare_filtered_table(
                    table, schema, where, backend=backend, data_id=data_id, new_table=new_table
                )
                sample = await self._fetch_sample(working_table, schema)
                return self._success_response(f"Filtered '{column}' between '{start}' and '{end}'",
                                              [column], [], sample, new_table=working_table)

            elif isinstance(self.db, ClickHouseAdapter):
                base_expr = await self._datetime_base_expr(table, schema, column)
                where = f"{base_expr} BETWEEN CAST({self._dt_literal(start)} AS DateTime) AND CAST({self._dt_literal(end)} AS DateTime)"
                working_table = await self._prepare_filtered_table(
                    table, schema, where, backend=backend, data_id=data_id, new_table=new_table
                )
                sample = await self._fetch_sample(working_table, schema)
                return self._success_response(f"Filtered '{column}' between '{start}' and '{end}'",
                                              [column], [], sample, new_table=working_table)
            else:
                raise self._unsupported_backend_error()

        except Exception as e:
            return self._error_response(f"between error: {str(e)}", [column], [])

    async def before(self, table: str, schema: str, column: str, value: str,
                     backend=None, data_id=None, new_table=None) -> Dict[str, Any]:
        try:
            if isinstance(self.db, PostgresAdapter) or isinstance(self.db, DuckDBAdapter):
                safe_col = SQLIdentifierSanitizer.sanitize(column)
                where = f'CAST("{safe_col}" AS TIMESTAMP) < CAST({self._dt_literal(value)} AS TIMESTAMP)'
                working_table = await self._prepare_filtered_table(
                    table, schema, where, backend=backend, data_id=data_id, new_table=new_table
                )
                sample = await self._fetch_sample(working_table, schema)
                return self._success_response(f"Filtered '{column}' before '{value}'",
                                              [column], [], sample, new_table=working_table)

            elif isinstance(self.db, ClickHouseAdapter):
                base_expr = await self._datetime_base_expr(table, schema, column)
                where = f"{base_expr} < CAST({self._dt_literal(value)} AS DateTime)"
                working_table = await self._prepare_filtered_table(
                    table, schema, where, backend=backend, data_id=data_id, new_table=new_table
                )
                sample = await self._fetch_sample(working_table, schema)
                return self._success_response(f"Filtered '{column}' before '{value}'",
                                              [column], [], sample, new_table=working_table)
            else:
                raise self._unsupported_backend_error()

        except Exception as e:
            return self._error_response(f"before error: {str(e)}", [column], [])

    async def after(self, table: str, schema: str, column: str, value: str,
                    backend=None, data_id=None, new_table=None) -> Dict[str, Any]:
        try:
            if isinstance(self.db, PostgresAdapter) or isinstance(self.db, DuckDBAdapter):
                safe_col = SQLIdentifierSanitizer.sanitize(column)
                where = f'CAST("{safe_col}" AS TIMESTAMP) > CAST({self._dt_literal(value)} AS TIMESTAMP)'
                working_table = await self._prepare_filtered_table(
                    table, schema, where, backend=backend, data_id=data_id, new_table=new_table
                )
                sample = await self._fetch_sample(working_table, schema)
                return self._success_response(f"Filtered '{column}' after '{value}'",
                                              [column], [], sample, new_table=working_table)

            elif isinstance(self.db, ClickHouseAdapter):
                base_expr = await self._datetime_base_expr(table, schema, column)
                where = f"{base_expr} > CAST({self._dt_literal(value)} AS DateTime)"
                working_table = await self._prepare_filtered_table(
                    table, schema, where, backend=backend, data_id=data_id, new_table=new_table
                )
                sample = await self._fetch_sample(working_table, schema)
                return self._success_response(f"Filtered '{column}' after '{value}'",
                                              [column], [], sample, new_table=working_table)
            else:
                raise self._unsupported_backend_error()

        except Exception as e:
            return self._error_response(f"after error: {str(e)}", [column], [])

    async def select_year(self, table: str, schema: str, column: str, years,
                          backend=None, data_id=None, new_table=None) -> Dict[str, Any]:
        try:
            values = ", ".join(str(int(y)) for y in (years if isinstance(years, (list, tuple)) else [years]))
            if isinstance(self.db, PostgresAdapter) or isinstance(self.db, DuckDBAdapter):
                safe_col = SQLIdentifierSanitizer.sanitize(column)
                where = f'EXTRACT(YEAR FROM CAST("{safe_col}" AS TIMESTAMP)) IN ({values})'
                working_table = await self._prepare_filtered_table(
                    table, schema, where, backend=backend, data_id=data_id, new_table=new_table
                )
                sample = await self._fetch_sample(working_table, schema)
                return self._success_response(f"Selected years {values} from '{column}'",
                                              [column], [], sample, new_table=working_table)

            elif isinstance(self.db, ClickHouseAdapter):
                base_expr = await self._datetime_base_expr(table, schema, column)
                where = f"toYear({base_expr}) IN ({values})"
                working_table = await self._prepare_filtered_table(
                    table, schema, where, backend=backend, data_id=data_id, new_table=new_table
                )
                sample = await self._fetch_sample(working_table, schema)
                return self._success_response(f"Selected years {values} from '{column}'",
                                              [column], [], sample, new_table=working_table)
            else:
                raise self._unsupported_backend_error()

        except Exception as e:
            return self._error_response(f"select_year error: {str(e)}", [column], [])

    async def select_month(self, table: str, schema: str, column: str, months,
                           backend=None, data_id=None, new_table=None) -> Dict[str, Any]:
        try:
            values = ", ".join(str(int(m)) for m in (months if isinstance(months, (list, tuple)) else [months]))
            if isinstance(self.db, PostgresAdapter) or isinstance(self.db, DuckDBAdapter):
                safe_col = SQLIdentifierSanitizer.sanitize(column)
                where = f'EXTRACT(MONTH FROM CAST("{safe_col}" AS TIMESTAMP)) IN ({values})'
                working_table = await self._prepare_filtered_table(
                    table, schema, where, backend=backend, data_id=data_id, new_table=new_table
                )
                sample = await self._fetch_sample(working_table, schema)
                return self._success_response(f"Selected months {values} from '{column}'",
                                              [column], [], sample, new_table=working_table)

            elif isinstance(self.db, ClickHouseAdapter):
                base_expr = await self._datetime_base_expr(table, schema, column)
                where = f"toMonth({base_expr}) IN ({values})"
                working_table = await self._prepare_filtered_table(
                    table, schema, where, backend=backend, data_id=data_id, new_table=new_table
                )
                sample = await self._fetch_sample(working_table, schema)
                return self._success_response(f"Selected months {values} from '{column}'",
                                              [column], [], sample, new_table=working_table)
            else:
                raise self._unsupported_backend_error()

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

            if isinstance(self.db, PostgresAdapter) or isinstance(self.db, DuckDBAdapter):
                working_table = await self._prepare_operation_table(
                    table, schema, backend=backend, data_id=data_id, new_table=new_table
                )
                new_col = SQLIdentifierSanitizer.sanitize(target_col) if target_col else self._generate_cleaned_column_name(column, "offset")
                await self._add_new_column(working_table, schema, new_col, "TIMESTAMP")

                qualified = self._qualified_table(working_table, schema)
                safe_col = SQLIdentifierSanitizer.sanitize(column)
                safe_new = SQLIdentifierSanitizer.sanitize(new_col)

                if business_day:
                    # ponytail: closed-form Mon-Fri arithmetic verified against
                    # pandas BDay (roll weekend starts, exclusive count, weekend
                    # start consumes one). No holidays — see is_business_day.
                    dow = f'EXTRACT(DOW FROM "{safe_col}")'
                    if days >= 0:
                        roll = f"(CASE WHEN {dow} = 6 THEN 2 WHEN {dow} = 0 THEN 1 ELSE 0 END)"
                        rolled = f'("{safe_col}" + {roll} * INTERVAL \'1 day\')'
                        kk = f"GREATEST({days} - (CASE WHEN {dow} IN (0, 6) THEN 1 ELSE 0 END), 0)"
                        w = f"((EXTRACT(DOW FROM {rolled}) + 6) % 7)"
                        expr = f"({rolled} + (({kk}) + 2 * FLOOR((({w}) + ({kk})) / 5)) * INTERVAL '1 day')"
                    else:
                        m = abs(days)
                        rollback = f"(CASE WHEN {dow} = 6 THEN 1 WHEN {dow} = 0 THEN 2 ELSE 0 END)"
                        rolled = f'("{safe_col}" - {rollback} * INTERVAL \'1 day\')'
                        kk = f"GREATEST({m} - (CASE WHEN {dow} IN (0, 6) THEN 1 ELSE 0 END), 0)"
                        wrev = f"(4 - ((EXTRACT(DOW FROM {rolled}) + 6) % 7))"
                        expr = f"({rolled} - (({kk}) + 2 * FLOOR((({wrev}) + ({kk})) / 5)) * INTERVAL '1 day')"
                else:
                    terms = []
                    if years:
                        terms.append(f"INTERVAL '{years} years'")
                    if quarters:
                        terms.append(f"INTERVAL '{quarters * 3} months'")
                    if months:
                        terms.append(f"INTERVAL '{months} months'")
                    if weeks:
                        terms.append(f"INTERVAL '{weeks} weeks'")
                    if days:
                        terms.append(f"INTERVAL '{days} days'")
                    if not terms:
                        return self._error_response("at least one offset unit must be non-zero", [column], [])
                    expr = f'"{safe_col}"' + "".join(f" + {t}" for t in terms)

                await self._exec(f"""UPDATE {qualified} SET "{safe_new}" = {expr}""")
                sample = await self._fetch_sample(working_table, schema, columns=[safe_col, safe_new])
                return self._success_response(f"Applied offset to '{column}'",
                                              [column], [new_col], sample, new_table=working_table)

            elif isinstance(self.db, ClickHouseAdapter):
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
            else:
                raise self._unsupported_backend_error()

        except Exception as e:
            return self._error_response(f"add_offset error: {str(e)}", [column], [])

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

                await self._exec(f"""ALTER TABLE {q} UPDATE "{n}" = toStartOfDay({base_expr}) WHERE 1 SETTINGS mutations_sync = 1""")
                sample = await self._fetch_sample(working_table, schema, [c, n])
                return self._success_response(f"Normalized '{column}' to day",
                                            [column], [new_col], sample, new_table=working_table)
            else:
                raise self._unsupported_backend_error()

        except Exception as e:
            return self._error_response(str(e), [column])

    # ==================================================================
    #  WAVE 2 — resampling + frequency conversion
    # ==================================================================
    async def resample(self, table: str, schema: str, column: str, freq: str,
                       agg: Any = "count", value_columns: Any = None,
                       group_by: Any = None, label: str = "left", closed: str = "left",
                       backend=None, data_id=None, new_table=None) -> Dict[str, Any]:
        # ponytail: moved from GeneralTableOps (ctx.resample) — time-bucketed
        # aggregation belongs here; extended with multi-agg + group_by.
        try:
            safe_table = SQLIdentifierSanitizer.sanitize(table)
            safe_col = SQLIdentifierSanitizer.sanitize(column)
            qualified = self._qualified_table(safe_table, schema)

            column_types = await self.db.get_column_types(safe_table, schema)
            if safe_col not in column_types and column not in column_types:
                return self._error_response(f"Column '{column}' not found", [column], [])
            dtype = str(column_types.get(safe_col, column_types.get(column, ""))).lower()
            if not any(t in dtype for t in ("date", "time", "timestamp")):
                return self._error_response(f"Column '{column}' must be datetime-like", [column], [])

            try:
                unit = self._parse_freq(freq)
            except ValueError as exc:
                return self._error_response(str(exc), [column], [])
            if label not in ("left", "right"):
                return self._error_response("label must be 'left' or 'right'", [column], [])
            if closed not in ("left", "right"):
                return self._error_response("closed must be 'left' or 'right'", [column], [])
            try:
                specs = self._normalize_agg_spec(agg, value_columns)
            except ValueError as exc:
                return self._error_response(str(exc), [column], [])

            groups = [SQLIdentifierSanitizer.sanitize(str(g)) for g in (
                group_by if isinstance(group_by, (list, tuple)) else ([group_by] if group_by else []))]
            if isinstance(self.db, ClickHouseAdapter):
                bucket = f"DATE_TRUNC('{unit}', \"{safe_col}\")"
                interval = f"INTERVAL 1 {unit}"
            else:
                bucket = f"DATE_TRUNC('{unit}', \"{safe_col}\")"
                interval = f"INTERVAL '1 {unit}'"
            if label == "right":
                bucket = f"{bucket} + {interval}"

            select_items = [f"{bucket} AS bucket"]
            select_items += [f'"{g}"' for g in groups]
            for out_name, func, col in specs:
                safe_out = SQLIdentifierSanitizer.sanitize(out_name)
                qcol = SQLIdentifierSanitizer.sanitize(col) if col is not None else None
                if func == "count" and col is None:
                    select_items.append(f"COUNT(*) AS \"{safe_out}\"")
                elif func == "count":
                    select_items.append(f'COUNT("{qcol}") AS "{safe_out}"')
                elif func == "mean":
                    # ponytail: MEAN() only exists on DuckDB; AVG is identical everywhere.
                    select_items.append(f'AVG("{qcol}") AS "{safe_out}"')
                elif func == "median" and isinstance(self.db, PostgresAdapter):
                    # ponytail: Postgres has no MEDIAN() aggregate.
                    select_items.append(
                        f"percentile_cont(0.5) WITHIN GROUP (ORDER BY \"{qcol}\") AS \"{safe_out}\"")
                else:
                    select_items.append(f'{func.upper()}("{qcol}") AS "{safe_out}"')
            group_items = ["bucket"] + [f'"{g}"' for g in groups]

            query = f"""
                SELECT {", ".join(select_items)}
                FROM {qualified}
                GROUP BY {", ".join(group_items)}
                ORDER BY {", ".join(group_items)}
            """
            rows = await self._fetch(query)
            records = [dict(row) for row in rows]
            df = pd.DataFrame.from_records(records)
            if not df.empty:
                df = df.rename(columns={"bucket": column})
            involved = [column] + groups
            generated = [out for out, _, _ in specs]
            return self._success_response(
                f"Resampled '{column}' by '{freq}'",
                involved, generated, df,
                result_metadata={"row_count": len(df), "freq": freq, "unit": unit,
                                 "aggregation": agg, "label": label, "closed": closed},
            )

        except Exception as e:
            return self._error_response(f"resample error: {str(e)}\n{traceback.format_exc()}", [column], [])

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

            if isinstance(self.db, ClickHouseAdapter):
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
            elif isinstance(self.db, PostgresAdapter):
                # ponytail: Postgres generate_series is set-returning natively;
                # UNNEST() takes arrays only, so the bare form lives here.
                # Grid starts at the truncated min so buckets align to midnights.
                grid = (
                    f"(SELECT generate_series(DATE_TRUNC('{unit}', CAST({self._dt_literal(lo_s)} AS TIMESTAMP)), "
                    f"CAST({self._dt_literal(hi_s)} AS TIMESTAMP), INTERVAL '1 {unit}') AS bucket)")
                bucket_match = f"DATE_TRUNC('{unit}', s.\"{safe_col}\") = g.bucket"
            else:
                # ponytail: DuckDB's generate_series returns a list, hence UNNEST.
                # Grid starts at the truncated min so buckets align to midnights.
                grid = (
                    f"(SELECT UNNEST(generate_series(DATE_TRUNC('{unit}', CAST({self._dt_literal(lo_s)} AS TIMESTAMP)), "
                    f"CAST({self._dt_literal(hi_s)} AS TIMESTAMP), INTERVAL '1 {unit}')) AS bucket)")
                bucket_match = f"DATE_TRUNC('{unit}', s.\"{safe_col}\") = g.bucket"

            if method is None:
                fill_items = [f's."{c}"' for c in safe_others]
            else:
                # ponytail: CH cannot decorrelate ORDER BY+LIMIT subqueries;
                # IGNORE NULLS windows probed good on this server instead.
                if isinstance(self.db, ClickHouseAdapter):
                    if method == "ffill":
                        fill_items = [
                            f"last_value(s.\"{c}\") IGNORE NULLS OVER (ORDER BY g.bucket "
                            f"ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS \"{c}\""
                            for c in safe_others]
                    else:
                        fill_items = [
                            f"first_value(s.\"{c}\") IGNORE NULLS OVER (ORDER BY g.bucket "
                            f"ROWS BETWEEN CURRENT ROW AND UNBOUNDED FOLLOWING) AS \"{c}\""
                            for c in safe_others]
                else:
                    # ponytail: portable correlated-subquery fill (O(n^2)); per-backend
                    # IGNORE NULLS windows later if asfreq grids prove large.
                    cmp_op, order = ("<=", "DESC") if method == "ffill" else (">=", "ASC")
                    fill_items = []
                    for c in safe_others:
                        fill_items.append(
                            f"(SELECT t2.\"{c}\" FROM {qualified} t2 "
                            f"WHERE DATE_TRUNC('{unit}', t2.\"{safe_col}\") {cmp_op} g.bucket "
                            f"AND t2.\"{c}\" IS NOT NULL "
                            f"ORDER BY DATE_TRUNC('{unit}', t2.\"{safe_col}\") {order} LIMIT 1) AS \"{c}\"")

            output_table = await self._resolve_output_table_name(
                safe_table, safe_schema, backend=backend, data_id=data_id, new_table=new_table)
            qualified_target = f'{self.db.quote_identifier(safe_schema)}.{self.db.quote_identifier(output_table)}'
            # ponytail: fill subqueries re-derive the truncated source bucket
            # per grid row (portable correlated fill, O(n^2)).
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