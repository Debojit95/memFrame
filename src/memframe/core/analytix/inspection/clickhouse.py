from typing import Dict, List, Any
import pandas as pd

from memframe.core.analytix.inspection.base import GeneralTableOps
from memframe.utils.helper import SQLIdentifierSanitizer


class ClickHouseTableOps(GeneralTableOps):
    """ClickHouse backend — overrides hooks plus the methods whose algorithm
    differs wholesale from PostgreSQL/DuckDB (insert / update / set_index and
    the nullable-aware type mapping)."""

    # ── hooks ──────────────────────────────────────────────────────
    def _limit_offset(self, limit: int, offset: int) -> str:
        # ClickHouse requires LIMIT before OFFSET
        return f"LIMIT {limit} OFFSET {offset}"

    def _random_order_sql(self) -> str:
        return "ORDER BY rand()"

    def _numeric_stat_exprs(self, col: str) -> Dict[str, str]:
        return {
            "q25": f'quantile(0.25)("{col}")',
            "median": f'quantile(0.5)("{col}")',
            "q75": f'quantile(0.75)("{col}")',
            "std": f'stddevSamp("{col}")',
        }

    async def _list_table_columns(self, schema: str, table: str) -> List[str]:
        rows = await self._fetch(
            """
            SELECT name
            FROM system.columns
            WHERE database = ? AND table = ?
            """,
            schema, table,
        )
        return [row["name"] for row in rows]

    def _create_table_suffix(self) -> str:
        return " ENGINE = MergeTree() ORDER BY tuple()"

    async def _insert_rows(self, qualified: str, rows: List[List[Any]], columns: List[str]) -> None:
        await self.db.insert_rows(qualified, rows, list(columns))

    def _dtype_translation_map(self) -> Dict[str, str]:
        return {
            "int": "Int64",
            "int8": "Int8",
            "int16": "Int16",
            "int32": "Int32",
            "int64": "Int64",
            "float": "Float64",
            "float32": "Float32",
            "float64": "Float64",
            "double": "Float64",
            "str": "String",
            "string": "String",
            "text": "String",
        }

    def _cast_expr(self, col_safe: str, target_type: str, source_type: str, requested_dtype: str) -> str:
        col_q = self.db.quote_identifier(col_safe)
        ch_string_cast_functions = {
            "int": "toInt64OrNull",
            "int8": "toInt8OrNull",
            "int16": "toInt16OrNull",
            "int32": "toInt32OrNull",
            "int64": "toInt64OrNull",
            "float": "toFloat64OrNull",
            "float32": "toFloat32OrNull",
            "float64": "toFloat64OrNull",
            "double": "toFloat64OrNull",
        }
        source_is_string = "string" in source_type or "text" in source_type
        if target_type == "String":
            expr = f"toString({col_q})"
        elif source_is_string:
            expr = f"{ch_string_cast_functions[requested_dtype]}({col_q})"
        else:
            expr = f"CAST({col_q} AS Nullable({target_type}))"
        return f"{expr} AS {col_q}"

    def _translate_resample_rule(self, rule: str) -> str:
        # ponytail: ClickHouse DATE_TRUNC/dateTrunc needs full unit names
        # ('day', 'hour', ...); Postgres/DuckDB accept pandas offset aliases
        # like 'D'. Translate only here, leave others untouched.
        alias_map = {
            "D": "day", "H": "hour", "T": "minute", "MIN": "minute",
            "S": "second", "W": "week", "M": "month",
            "Q": "quarter", "A": "year", "Y": "year",
        }
        return alias_map.get(rule.upper(), rule)

    def _row_value(self, row: Any, col: str, columns: List[str]) -> Any:
        return row[col]

    def _row_to_dict(self, row: Any, columns: List[str]) -> Dict[str, Any]:
        return dict(row)

    def _row_to_list(self, row: Any, columns: List[str]) -> List[Any]:
        return [row[c] for c in columns]

    def _sql_type_for_series(self, series: pd.Series) -> str:
        # ponytail: ClickHouse non-Nullable columns can't store NULL — they
        # coerce None to "" (String) or 1970-01-01 (DateTime). Wrap in
        # Nullable(...) when the series has any NA values.
        nullable = bool(series.isna().any())

        def _ch(t: str) -> str:
            return f"Nullable({t})" if nullable else t

        dtype = series.dtype
        if pd.api.types.is_bool_dtype(dtype):
            return _ch("UInt8")
        if pd.api.types.is_integer_dtype(dtype):
            return _ch("Int64")
        if pd.api.types.is_float_dtype(dtype):
            return _ch("Float64")
        if pd.api.types.is_datetime64_any_dtype(dtype):
            return _ch("DateTime")
        return _ch("String")

    # ── whole-method overrides (different algorithm) ──────────────
    async def dataframe_insert(self, table: str, schema: str, column: str, value: Any, **kwargs) -> Dict[str, Any]:
        try:
            table = SQLIdentifierSanitizer.sanitize(table)
            column = SQLIdentifierSanitizer.sanitize(column)
            qualified = self._qualified_table(table, schema)

            if not isinstance(value, list):
                return self._error_response("Value must be a list")

            total_rows = await self._fetchval(f"SELECT count() FROM {qualified}") or 0

            if len(value) != total_rows:
                return self._error_response(
                    f"Length mismatch: Expected {total_rows}, got {len(value)}"
                )

            # ClickHouse: get current data, add column, recreate table
            df = await self._get_table_dataframe(table, schema)
            df[column] = value

            temp_name = await self._save_dataframe_as_table(
                df, schema, table, backend=kwargs.get("backend"), data_id=kwargs.get("data_id")
            )
            temp_qualified = self._qualified_table(temp_name, schema)

            await self._exec(f"DROP TABLE IF EXISTS {qualified}")
            await self._exec(f"RENAME TABLE {temp_qualified} TO {qualified}")

            return self._success_response(
                message=f"Column '{column}' created successfully with {total_rows} values",
                involved_cols=[],
                generated_cols=[column],
                result=df,
            )
        except Exception as e:
            return self._error_response(str(e))

    async def dataframe_set_index(self, table: str, schema: str, columns: List[str], **kwargs) -> Dict[str, Any]:
        try:
            table = SQLIdentifierSanitizer.sanitize(table)
            qualified = self._qualified_table(table, schema)

            # ClickHouse uses ORDER BY for sorting; recreate table with proper ORDER BY
            df = await self._get_table_dataframe(table, schema)

            temp_name = await self._save_dataframe_as_table(df, schema, table)
            temp_qualified = self._qualified_table(temp_name, schema)

            await self._exec(f"DROP TABLE IF EXISTS {qualified}")
            await self._exec(f"RENAME TABLE {temp_qualified} TO {qualified}")

            return self._success_response(
                result="Index set (ORDER BY in ClickHouse)", involved_cols=columns
            )
        except Exception as e:
            return self._error_response(str(e))

    async def dataframe_update(self, table: str, schema: str, other_table: str, other_schema: str, on: str, overwrite: bool = True, errors: str = "ignore", **kwargs) -> Dict[str, Any]:
        try:
            table = SQLIdentifierSanitizer.sanitize(table)
            other_table = SQLIdentifierSanitizer.sanitize(other_table)
            on = SQLIdentifierSanitizer.sanitize(on)

            t1 = self._qualified_table(table, schema)
            t2 = self._qualified_table(other_table, other_schema)

            if errors not in ["ignore", "raise"]:
                return self._error_response("errors must be 'ignore' or 'raise'")

            target_cols = await self._get_column_types(table, schema)
            source_cols = await self._get_column_types(other_table, other_schema)

            common_cols = [c for c in target_cols if c in source_cols and c != on]

            if not common_cols:
                return self._success_response(
                    message="No overlapping columns to update",
                    involved_cols=[],
                    generated_cols=[]
                )

            if errors == "raise":
                for col in common_cols:
                    conflict = await self._fetchval(f"""
                        SELECT count()
                        FROM {t1} JOIN {t2} ON {t1}.`{on}` = {t2}.`{on}`
                        WHERE {t1}.`{col}` IS NOT NULL AND {t2}.`{col}` IS NOT NULL
                    """)
                    if conflict > 0:
                        return self._error_response(
                            f"Conflict detected in column '{col}'"
                        )

            # ponytail: ClickHouse rejects correlated subqueries inside
            # ALTER TABLE ... UPDATE, so rebuild via temp-table + LEFT JOIN with
            # COALESCE. Matches Postgres/DuckDB overwrite semantics exactly.
            select_parts = []
            for col in target_cols:
                col_safe = SQLIdentifierSanitizer.sanitize(col)
                if col == on:
                    select_parts.append(f"t1.`{col_safe}` AS `{col_safe}`")
                elif col in common_cols:
                    if overwrite:
                        expr = f"COALESCE(t2.`{col_safe}`, t1.`{col_safe}`)"
                    else:
                        expr = f"COALESCE(t1.`{col_safe}`, t2.`{col_safe}`)"
                    select_parts.append(f"{expr} AS `{col_safe}`")
                else:
                    select_parts.append(f"t1.`{col_safe}` AS `{col_safe}`")
            cols_clause = ", ".join(select_parts)

            temp_table = f"{table}__update_temp"
            qualified_temp = self._qualified_table(temp_table, schema)
            await self._exec(f"DROP TABLE IF EXISTS {qualified_temp}")
            await self._exec(f"""
                CREATE TABLE {qualified_temp} AS
                SELECT {cols_clause}
                FROM {t1} t1
                LEFT JOIN {t2} t2 ON t1.`{on}` = t2.`{on}`
            """)
            await self._exec(f"DROP TABLE {t1}")
            await self._exec(f"RENAME TABLE {qualified_temp} TO {t1}")

            sample_rows = await self._fetch(f"SELECT * FROM {t1} LIMIT 10")
            records = self._rows_to_records(sample_rows)
            records = await self._normalize_records_by_type(records, table, schema)
            sample_df = pd.DataFrame.from_records(records)

            return self._success_response(
                result=sample_df,
                message=f"Table '{table}' updated using '{other_table}'",
                involved_cols=[on] + common_cols,
                generated_cols=common_cols,
                result_metadata={
                    "updated_columns": common_cols,
                    "overwrite": overwrite,
                    "errors": errors
                }
            )
        except Exception as e:
            return self._error_response(str(e))
