"""
ClickHouse cleaning operations.

Every method where ClickHouse diverges from the DuckDB default (which lives in
base.py) is overridden here with the original ClickHouse-specific branch copied
verbatim, so behaviour is preserved exactly.
"""

import traceback
from typing import Any, Dict, List, Optional

from memframe.core.analytix.cleaning.base import DataCleaningOps, _sql_literal
from memframe.utils.helper import SQLIdentifierSanitizer
from memframe.core.analytix._response import fail, ok


class ClickHouseCleaningOps(DataCleaningOps):

    # ------------------------------------------------------------------
    # Numeric
    # ------------------------------------------------------------------
    def _numeric_target_for(self, pg_type: str) -> str:
        ch_map = {"SMALLINT": "Int16", "INTEGER": "Int32", "BIGINT": "Int64", "FLOAT": "Float64"}
        return ch_map.get(pg_type, "Decimal(18, 6)")

    async def numeric_fillna(
        self,
        table: str,
        schema: str,
        column: str,
        value: Any = None,
        mode: str = "mean",
        backend=None,
        data_id: Optional[str] = None,
        new_table: Optional[str] = None,
    ) -> Dict[str, Any]:
        try:
            original = table
            table = await self._prepare_column_operation_table(
                table, schema, [column], backend=backend, data_id=data_id, new_table=new_table,
            )
            mode = mode.upper()

            suffix_map = {
                "CONSTANT": f"constant_filled_{value}",
                "MEAN": "mean_filled",
                "AVG": "mean_filled",
                "AVERAGE": "mean_filled",
                "MEDIAN": "median_filled",
                "MODE": "mode_filled",
                "STD": "std_filled",
                "VAR": "var_filled",
                "VARIANCE": "var_filled",
                "MIN": "min_filled",
                "MAX": "max_filled",
                "BFILL": "bfill_filled",
                "FFILL": "ffill_filled"
            }

            if mode not in suffix_map:
                return fail(f"Unsupported mode: {mode}")

            new_col = self._generate_cleaned_column_name(column, suffix_map[mode])
            col_type = await self._get_column_type(table, schema, column)
            await self._add_new_column(table, schema, new_col, col_type)

            qualified = self._qualified_table(table, schema)
            safe_col = SQLIdentifierSanitizer.sanitize(column)
            safe_new = SQLIdentifierSanitizer.sanitize(new_col)

            if mode == "CONSTANT" and value is None:
                return fail("Value must be provided for CONSTANT mode")

            async def _apply(tq):
                if mode == "CONSTANT":
                    converted = _sql_literal(value)
                    await self._exec(
                        f'ALTER TABLE {tq} UPDATE "{safe_new}" = COALESCE("{safe_col}", {converted}) WHERE 1'
                    )
                    return value
                elif mode in ["FFILL", "BFILL"]:
                    raise self._unsupported_backend_error()
                else:
                    stat_map = {
                        "MEAN": f'AVG("{safe_col}")',
                        "AVG": f'AVG("{safe_col}")',
                        "AVERAGE": f'AVG("{safe_col}")',
                        "MEDIAN": f'quantile(0.5)("{safe_col}")',
                        "MODE": f'(SELECT "{safe_col}" FROM {tq} WHERE "{safe_col}" IS NOT NULL GROUP BY "{safe_col}" ORDER BY COUNT(*) DESC LIMIT 1)',
                        "STD": f'stddevPop("{safe_col}")',
                        "VAR": f'varPop("{safe_col}")',
                        "VARIANCE": f'varPop("{safe_col}")',
                        "MIN": f'min("{safe_col}")',
                        "MAX": f'max("{safe_col}")',
                    }
                    stat_expr = stat_map[mode]
                    await self._exec(f"""
                        ALTER TABLE {tq}
                        UPDATE "{safe_new}" = COALESCE("{safe_col}", (SELECT COALESCE({stat_expr}, 0) FROM {tq} WHERE "{safe_col}" IS NOT NULL))
                        WHERE 1
                    """)
                    return await self._fetchval(f"""
                        SELECT {stat_expr}
                        FROM {tq}
                        WHERE "{safe_col}" IS NOT NULL
                    """)

            fill_value = await _apply(qualified)

            await self._add_new_column_if_not_exists(original, schema, new_col, col_type)
            await _apply(self._qualified_table(original, schema))

            null_count = await self._fetchval(
                f'SELECT COUNT(*) FROM {qualified} WHERE "{safe_col}" IS NULL'
            ) or 0

            sample = await self._fetch_data(table, schema, columns=[safe_col, safe_new])
            msg = f"Filled {null_count} null values in '{column}' using {mode} ({fill_value})"

            return ok(
                msg, [column], [new_col], sample, fill_mode=mode, fill_value=fill_value, new_table=table,
            )

        except Exception as e:
            return fail(
                f"numeric_fillna error: {str(e)}\n{traceback.format_exc()}", [column], [],
            )

    async def numeric_enforce_range(
        self,
        table: str,
        schema: str,
        column: str,
        min_value: int | float = None,
        max_value: int | float = None,
        backend=None,
        data_id: Optional[str] = None,
        new_table: Optional[str] = None,
    ) -> Dict[str, Any]:
        try:
            original = table
            table = await self._prepare_operation_table(
                table, schema, backend=backend, data_id=data_id, new_table=new_table,
            )
            new_col = self._generate_cleaned_column_name(column, f"range_{min_value}_{max_value}")
            col_type = await self._get_column_type(table, schema, column)
            await self._add_new_column(table, schema, new_col, col_type)

            qualified = self._qualified_table(table, schema)
            safe_col = SQLIdentifierSanitizer.sanitize(column)
            safe_new = SQLIdentifierSanitizer.sanitize(new_col)

            case_parts = []
            if min_value is not None:
                case_parts.append(f'WHEN "{safe_col}" < {min_value} THEN NULL')
            if max_value is not None:
                case_parts.append(f'WHEN "{safe_col}" > {max_value} THEN NULL')
            case_expr = f"CASE {' '.join(case_parts)} ELSE \"{safe_col}\" END" if case_parts else f'"{safe_col}"'

            async def _apply(tq):
                await self._exec(f'ALTER TABLE {tq} UPDATE "{safe_new}" = {case_expr} WHERE 1')

            await _apply(qualified)

            await self._add_new_column_if_not_exists(original, schema, new_col, col_type)
            await _apply(self._qualified_table(original, schema))

            affected = 0
            if min_value is not None or max_value is not None:
                cond = []
                if min_value is not None:
                    cond.append(f'"{safe_col}" < {min_value}')
                if max_value is not None:
                    cond.append(f'"{safe_col}" > {max_value}')
                where_clause = " OR ".join(cond)
                affected = await self._fetchval(
                    f'SELECT COUNT(*) FROM {qualified} WHERE {where_clause}'
                ) or 0

            sample = await self._fetch_data(table, schema, columns=[safe_col, safe_new])
            msg = f"Enforced range on '{column}': {affected} values set to NULL"
            return ok(msg, [column], [new_col], sample, new_table=table)

        except Exception as e:
            return fail(f"numeric_enforce_range error: {str(e)}\n{traceback.format_exc()}")

    async def numeric_drop_outliers_zscore(
        self,
        table: str,
        schema: str,
        column: str,
        z_thresh: float = 3.0,
        backend=None,
        data_id: Optional[str] = None,
        new_table: Optional[str] = None,
    ) -> Dict[str, Any]:
        try:
            original = table
            table = await self._prepare_operation_table(
                table, schema, backend=backend, data_id=data_id, new_table=new_table,
            )
            new_col = self._generate_cleaned_column_name(column, f"zscore_filtered_{z_thresh}")
            col_type = await self._get_column_type(table, schema, column)
            await self._add_new_column(table, schema, new_col, col_type)

            qualified = self._qualified_table(table, schema)
            safe_col = SQLIdentifierSanitizer.sanitize(column)
            safe_new = SQLIdentifierSanitizer.sanitize(new_col)

            async def _apply(tq):
                await self._exec(f"""
                    ALTER TABLE {tq}
                    UPDATE "{safe_new}" = CASE
                        WHEN ABS(("{safe_col}" - (SELECT AVG("{safe_col}") FROM {tq} WHERE "{safe_col}" IS NOT NULL)) /
                             NULLIF((SELECT stddevPop("{safe_col}") FROM {tq} WHERE "{safe_col}" IS NOT NULL), 0)) > {z_thresh} THEN NULL
                        ELSE "{safe_col}"
                    END
                    WHERE 1
                """)

            await _apply(qualified)

            await self._add_new_column_if_not_exists(original, schema, new_col, col_type)
            await _apply(self._qualified_table(original, schema))

            outlier_count = await self._fetchval(f"""
                SELECT COUNT(*)
                FROM {qualified}
                WHERE "{safe_col}" IS NOT NULL
                  AND ABS(("{safe_col}" - (SELECT AVG("{safe_col}") FROM {qualified} WHERE "{safe_col}" IS NOT NULL)) /
                         NULLIF((SELECT stddevPop("{safe_col}") FROM {qualified} WHERE "{safe_col}" IS NOT NULL), 0)) > {z_thresh}
            """) or 0

            sample = await self._fetch_data(table, schema, columns=[safe_col, safe_new])
            msg = f"Removed {outlier_count} outliers from '{column}' using Z-score (threshold={z_thresh})"
            return ok(msg, [column], [new_col], sample, new_table=table)

        except Exception as e:
            return fail(f"numeric_drop_outliers_zscore error: {str(e)}\n{traceback.format_exc()}")

    async def numeric_convert_text(
        self,
        table: str,
        schema: str,
        column: str,
        backend=None,
        data_id: Optional[str] = None,
        new_table: Optional[str] = None,
    ) -> Dict[str, Any]:
        try:
            original = table
            table = await self._prepare_operation_table(
                table, schema, backend=backend, data_id=data_id, new_table=new_table,
            )

            qualified = self._qualified_table(table, schema)
            safe_col = SQLIdentifierSanitizer.sanitize(column)

            cleaned_expr = f"""nullIf(replaceRegexpAll(toString("{safe_col}"), '[^0-9.+-]', ''), '')"""
            numeric_check = f"match({cleaned_expr}, '^[+-]?([0-9]+([.][0-9]*)?|[.][0-9]+)$')"

            target_type = await self._detect_numeric_target(qualified, cleaned_expr)

            async def _apply(tq):
                # ponytail: CH has no ALTER COLUMN TYPE ... USING, so rebuild
                # in place via a temp table (same idiom as compress_rare).
                temp_table = f"{table}__temp"
                qualified_temp = self._qualified_table(temp_table, schema)
                await self._exec(f"DROP TABLE IF EXISTS {qualified_temp}")
                await self._exec(f"""
                    CREATE TABLE {qualified_temp} AS
                    SELECT * REPLACE (
                        CAST(CASE
                            WHEN {numeric_check}
                                THEN {cleaned_expr}
                            ELSE NULL
                        END AS {target_type}) AS "{safe_col}"
                    )
                    FROM {tq}
                """)
                await self._exec(f"DROP TABLE {tq}")
                await self._exec(f"RENAME TABLE {qualified_temp} TO {tq}")

            await _apply(qualified)

            # Mirror the in-place conversion onto the original table
            await _apply(self._qualified_table(original, schema))

            converted = await self._fetchval(
                f'SELECT COUNT(*) FROM {qualified} WHERE "{safe_col}" IS NOT NULL'
            ) or 0

            sample = await self._fetch_data(table, schema, columns=[safe_col])
            msg = f"Converted {converted} text values in '{column}' to numeric"
            return ok(msg, [column], [], sample, new_table=table)

        except Exception as e:
            return fail(f"numeric_convert_text error: {str(e)}\n{traceback.format_exc()}")

    # ------------------------------------------------------------------
    # Categorical
    # ------------------------------------------------------------------
    async def categorical_fillna(
        self,
        table: str,
        schema: str,
        column: str,
        mode: str = "mode",
        value: Optional[Any] = None,
        mapping: Optional[Dict[Any, Any]] = None,
        backend=None,
        data_id: Optional[str] = None,
        new_table: Optional[str] = None,
    ) -> Dict[str, Any]:
        try:
            original = table
            table = await self._prepare_column_operation_table(
                table, schema, [column], backend=backend, data_id=data_id, new_table=new_table,
            )
            mode = mode.upper()

            valid_modes = {"CONSTANT", "MODE", "MAP", "BFILL", "FFILL"}
            if mode not in valid_modes:
                return fail(f"Unsupported mode: {mode}")

            suffix_map = {
                "CONSTANT": "constant_filled",
                "MODE": "mode_filled",
                "MAP": "mapped",
                "BFILL": "bfill_filled",
                "FFILL": "ffill_filled"
            }

            new_col = self._generate_cleaned_column_name(column, suffix_map[mode])
            col_type = await self._get_column_type(table, schema, column)
            await self._add_new_column(table, schema, new_col, col_type)

            qualified = self._qualified_table(table, schema)
            safe_col = SQLIdentifierSanitizer.sanitize(column)
            safe_new = SQLIdentifierSanitizer.sanitize(new_col)

            fill_value = None

            if mode == "CONSTANT" and value is None:
                return fail("Value must be provided for CONSTANT mode")
            if mode == "MAP" and not mapping:
                return fail("Mapping must be provided for MAP mode")

            async def _apply(tq):
                if mode == "CONSTANT":
                    val_str = str(value).replace("'", "''")
                    await self._exec(
                        f'ALTER TABLE {tq} UPDATE "{safe_new}" = COALESCE("{safe_col}", \'{val_str}\') WHERE 1'
                    )
                    return value
                elif mode in ["FFILL", "BFILL"]:
                    raise self._unsupported_backend_error()
                elif mode == "MODE":
                    await self._exec(f"""
                        ALTER TABLE {tq}
                        UPDATE "{safe_new}" = COALESCE(
                            "{safe_col}",
                            (SELECT "{safe_col}" FROM {tq} WHERE "{safe_col}" IS NOT NULL GROUP BY "{safe_col}" ORDER BY COUNT(*) DESC LIMIT 1)
                        ) WHERE 1
                    """)
                    return await self._fetchval(f"""
                        SELECT "{safe_col}" AS mode_value
                        FROM {tq}
                        WHERE "{safe_col}" IS NOT NULL
                        GROUP BY "{safe_col}"
                        ORDER BY COUNT(*) DESC
                        LIMIT 1
                    """)
                else:  # MAP
                    case_parts = []
                    for old, new in mapping.items():
                        old_esc = str(old).replace("'", "''")
                        new_esc = str(new).replace("'", "''")
                        case_parts.append(f'WHEN "{safe_col}" = \'{old_esc}\' THEN \'{new_esc}\'')
                    case_expr = f"CASE {' '.join(case_parts)} ELSE \"{safe_col}\" END"
                    await self._exec(f"""
                        ALTER TABLE {tq}
                        UPDATE "{safe_new}" = COALESCE({case_expr}, "{safe_col}")
                        WHERE 1
                    """)
                    return f"{len(mapping)} mappings applied"

            fill_value = await _apply(qualified)

            await self._add_new_column_if_not_exists(original, schema, new_col, col_type)
            await _apply(self._qualified_table(original, schema))

            null_count = await self._fetchval(
                f'SELECT COUNT(*) FROM {qualified} WHERE "{safe_col}" IS NULL'
            ) or 0

            sample = await self._fetch_data(table, schema, columns=[safe_col, safe_new])
            msg = f"Processed '{column}' using {mode} (fill={fill_value}), affected {null_count} nulls"

            return ok(
                msg, [column], [new_col], sample, fill_mode=mode, fill_value=fill_value, new_table=table,
            )

        except Exception as e:
            return fail(
                f"categorical_fillna error: {str(e)}\n{traceback.format_exc()}", [column], [],
            )

    async def categorical_map_values(
        self,
        table: str,
        schema: str,
        column: str,
        mapping: Dict[Any, Any],
        backend=None,
        data_id: Optional[str] = None,
        new_table: Optional[str] = None,
    ) -> Dict[str, Any]:
        try:
            original = table
            table = await self._prepare_operation_table(
                table, schema, backend=backend, data_id=data_id, new_table=new_table,
            )
            if not mapping:
                return fail("No mapping provided")

            new_col = self._generate_cleaned_column_name(column, f"mapped_{'_'.join(list(mapping.keys()))}")
            col_type = await self._get_column_type(table, schema, column)
            await self._add_new_column(table, schema, new_col, col_type)

            qualified = self._qualified_table(table, schema)
            safe_col = SQLIdentifierSanitizer.sanitize(column)
            safe_new = SQLIdentifierSanitizer.sanitize(new_col)

            case_parts = []
            for old, new in mapping.items():
                old_esc = str(old).replace("'", "''")
                new_esc = str(new).replace("'", "''")
                case_parts.append(f'WHEN "{safe_col}" = \'{old_esc}\' THEN \'{new_esc}\'')
            case_expr = f"CASE {' '.join(case_parts)} ELSE \"{safe_col}\" END"

            async def _apply(tq):
                await self._exec(f'ALTER TABLE {tq} UPDATE "{safe_new}" = {case_expr} WHERE 1')

            await _apply(qualified)

            await self._add_new_column_if_not_exists(original, schema, new_col, col_type)
            await _apply(self._qualified_table(original, schema))

            sample = await self._fetch_data(table, schema, columns=[safe_col, safe_new])
            msg = f"Mapped {len(mapping)} value categories in '{column}'"
            return ok(msg, [column], [new_col], sample, new_table=table)

        except Exception as e:
            return fail(f"categorical_map_values error: {str(e)}\n{traceback.format_exc()}")

    async def categorical_filter_invalid(
        self,
        table: str,
        schema: str,
        column: str,
        valid_values: List[Any],
        backend=None,
        data_id: Optional[str] = None,
        new_table: Optional[str] = None,
    ) -> Dict[str, Any]:
        try:
            original = table
            table = await self._prepare_operation_table(
                table, schema, backend=backend, data_id=data_id, new_table=new_table,
            )
            if not valid_values:
                return fail("No valid_values provided")

            new_col = self._generate_cleaned_column_name(column, f"valid_values_{'_'.join(valid_values)}")
            col_type = await self._get_column_type(table, schema, column)
            await self._add_new_column(table, schema, new_col, col_type)

            qualified = self._qualified_table(table, schema)
            safe_col = SQLIdentifierSanitizer.sanitize(column)
            safe_new = SQLIdentifierSanitizer.sanitize(new_col)

            escaped = [f"'{str(v).replace(chr(39), chr(39)+chr(39))}'" for v in valid_values]
            in_list = ",".join(escaped)

            async def _apply(tq):
                await self._exec(f"""
                    ALTER TABLE {tq}
                    UPDATE "{safe_new}" = CASE WHEN "{safe_col}" IN ({in_list}) THEN "{safe_col}" ELSE NULL END
                    WHERE 1
                """)

            await _apply(qualified)

            await self._add_new_column_if_not_exists(original, schema, new_col, col_type)
            await _apply(self._qualified_table(original, schema))

            invalid = await self._fetchval(f"""
                SELECT COUNT(*) FROM {qualified}
                WHERE "{safe_col}" IS NOT NULL AND "{safe_col}" NOT IN ({in_list})
            """) or 0

            sample = await self._fetch_data(table, schema, columns=[safe_col, safe_new])
            msg = f"Set {invalid} invalid values in '{column}' to NULL. Valid values: {valid_values[:10]}..."
            return ok(msg, [column], [new_col], sample, new_table=table)

        except Exception as e:
            return fail(f"categorical_filter_invalid error: {str(e)}\n{traceback.format_exc()}")

    async def categorical_compress_rare(
        self,
        table: str,
        schema: str,
        column: str,
        min_count: int = 10,
        other_label: str = "other",
        backend=None,
        data_id: Optional[str] = None,
        new_table: Optional[str] = None,
    ) -> Dict[str, Any]:
        try:
            original = table
            table = await self._prepare_operation_table(
                table, schema, backend=backend, data_id=data_id, new_table=new_table,
            )
            new_col = self._generate_cleaned_column_name(column, f"compressed_{min_count}_{other_label}")
            col_type = await self._get_column_type(table, schema, column)
            await self._add_new_column(table, schema, new_col, col_type)

            qualified = self._qualified_table(table, schema)
            safe_col = SQLIdentifierSanitizer.sanitize(column)
            safe_new = SQLIdentifierSanitizer.sanitize(new_col)

            other_esc = other_label.replace("'", "''")

            async def _apply(tq):
                # ponytail: ClickHouse has no correlated subqueries inside
                # ALTER TABLE ... UPDATE, so the prior per-row COUNT collapsed
                # to a total and rare categories were never replaced. Rebuild
                # via a grouped freq LEFT JOIN (same idiom as the stat-groupby
                # ClickHouse branch below).
                await self._exec(f'ALTER TABLE {tq} DROP COLUMN "{safe_new}"')
                temp_table = f"{table}__temp"
                qualified_temp = self._qualified_table(temp_table, schema)
                await self._exec(f"DROP TABLE IF EXISTS {qualified_temp}")
                await self._exec(f"""
                    CREATE TABLE {qualified_temp} AS
                    SELECT t.*,
                        CASE
                            WHEN freq.cnt IS NULL OR freq.cnt < {min_count} THEN '{other_esc}'
                            ELSE t."{safe_col}"
                        END AS "{safe_new}"
                    FROM {tq} t
                    LEFT JOIN (
                        SELECT "{safe_col}", COUNT(*) AS cnt
                        FROM {tq}
                        WHERE "{safe_col}" IS NOT NULL
                        GROUP BY "{safe_col}"
                    ) freq ON t."{safe_col}" = freq."{safe_col}"
                """)
                await self._exec(f"DROP TABLE {tq}")
                await self._exec(f"RENAME TABLE {qualified_temp} TO {tq}")

            await _apply(qualified)

            await self._add_new_column_if_not_exists(original, schema, new_col, col_type)
            await _apply(self._qualified_table(original, schema))

            rare_rows = await self._fetch(f"""
                SELECT DISTINCT "{safe_col}"
                FROM {qualified}
                WHERE "{safe_col}" IS NOT NULL
                GROUP BY "{safe_col}"
                HAVING COUNT(*) < {min_count}
            """)
            rare_count = len(rare_rows)

            sample = await self._fetch_data(table, schema, columns=[safe_col, safe_new])
            msg = f"Compressed {rare_count} rare categories in '{column}' to '{other_label}' (min_count={min_count})"
            return ok(msg, [column], [new_col], sample, new_table=table)

        except Exception as e:
            return fail(f"categorical_compress_rare error: {str(e)}\n{traceback.format_exc()}")

    # ------------------------------------------------------------------
    # Datetime
    # ------------------------------------------------------------------
    async def datetime_fillna(
        self,
        table: str,
        schema: str,
        column: str,
        mode: str = "mean",
        value: Optional[Any] = None,
        backend=None,
        data_id: Optional[str] = None,
        new_table: Optional[str] = None,
    ) -> Dict[str, Any]:
        try:
            original = table
            table = await self._prepare_column_operation_table(
                table, schema, [column], backend=backend, data_id=data_id, new_table=new_table,
            )
            mode = mode.upper()

            valid_modes = {"CONSTANT", "MIN", "MAX", "MEAN", "MEDIAN", "MODE", "NOW", "FFILL", "BFILL"}
            if mode not in valid_modes:
                return fail(f"Unsupported mode: {mode}")

            suffix_map = {
                "CONSTANT": "constant_filled",
                "MIN": "min_filled",
                "MAX": "max_filled",
                "MEAN": "mean_filled",
                "MEDIAN": "median_filled",
                "MODE": "mode_filled",
                "NOW": "now_filled",
                "BFILL": "bfill_filled",
                "FFILL": "ffill_filled"
            }

            new_col = self._generate_cleaned_column_name(column, suffix_map[mode])
            await self._add_new_column(table, schema, new_col, "TIMESTAMP")

            qualified = self._qualified_table(table, schema)
            safe_col = SQLIdentifierSanitizer.sanitize(column)
            safe_new = SQLIdentifierSanitizer.sanitize(new_col)

            fill_value = None

            if mode == "CONSTANT" and value is None:
                return fail("Value must be provided for CONSTANT mode")

            async def _apply(tq):
                if mode == "CONSTANT":
                    await self._exec(
                        f'ALTER TABLE {tq} UPDATE "{safe_new}" = COALESCE("{safe_col}", CAST(\'{value}\' AS DateTime)) WHERE 1'
                    )
                    return value
                elif mode == "NOW":
                    await self._exec(
                        f'ALTER TABLE {tq} UPDATE "{safe_new}" = COALESCE("{safe_col}", now()) WHERE 1'
                    )
                    return "CURRENT_TIMESTAMP"
                elif mode in ["FFILL", "BFILL"]:
                    raise self._unsupported_backend_error()
                else:
                    stat_map = {
                        "MIN": f'min("{safe_col}")',
                        "MAX": f'max("{safe_col}")',
                        "MEAN": f'toDateTime(toUInt32(avg(toUnixTimestamp("{safe_col}"))))',
                        "MEDIAN": f'toDateTime(toUInt32(quantile(0.5)(toUnixTimestamp("{safe_col}"))))',
                        "MODE": f'(SELECT "{safe_col}" FROM {tq} WHERE "{safe_col}" IS NOT NULL GROUP BY "{safe_col}" ORDER BY COUNT(*) DESC LIMIT 1)'
                    }
                    stat_expr = stat_map[mode]
                    await self._exec(f"""
                        ALTER TABLE {tq}
                        UPDATE "{safe_new}" = COALESCE("{safe_col}", (SELECT {stat_expr} FROM {tq} WHERE "{safe_col}" IS NOT NULL))
                        WHERE 1
                    """)
                    return await self._fetchval(f"""
                        SELECT {stat_expr}
                        FROM {tq}
                        WHERE "{safe_col}" IS NOT NULL
                    """)

            fill_value = await _apply(qualified)

            await self._add_new_column_if_not_exists(original, schema, new_col, "TIMESTAMP")
            await _apply(self._qualified_table(original, schema))

            null_count = await self._fetchval(
                f'SELECT COUNT(*) FROM {qualified} WHERE "{safe_col}" IS NULL'
            ) or 0

            sample = await self._fetch_data(table, schema, columns=[safe_col, safe_new])
            msg = f"Filled {null_count} null values in '{column}' using {mode} ({fill_value})"

            return ok(
                msg, [column], [new_col], sample, fill_mode=mode, fill_value=fill_value, new_table=table,
            )

        except Exception as e:
            return fail(
                f"datetime_fillna error: {str(e)}\n{traceback.format_exc()}", [column], [],
            )

    async def datetime_fix_invalid(
        self,
        table: str,
        schema: str,
        column: str,
        backend=None,
        data_id: Optional[str] = None,
        new_table: Optional[str] = None,
    ) -> Dict[str, Any]:
        try:
            original = table
            table = await self._prepare_operation_table(
                table, schema, backend=backend, data_id=data_id, new_table=new_table,
            )
            new_col = self._generate_cleaned_column_name(column, "fixed")
            await self._add_new_column(table, schema, new_col, "DATE")

            qualified = self._qualified_table(table, schema)
            safe_col = SQLIdentifierSanitizer.sanitize(column)
            safe_new = SQLIdentifierSanitizer.sanitize(new_col)

            text_expr = f'toString("{safe_col}")'

            async def _apply(tq):
                await self._exec(f"""
                    ALTER TABLE {tq}
                    UPDATE "{safe_new}" = CASE
                        WHEN {text_expr} = '0000-00-00' THEN NULL
                        ELSE "{safe_col}"
                    END
                    WHERE 1
                """)

            await _apply(qualified)

            await self._add_new_column_if_not_exists(original, schema, new_col, "DATE")
            await _apply(self._qualified_table(original, schema))

            invalid = await self._fetchval(
                f"SELECT COUNT(*) FROM {qualified} WHERE {text_expr} = '0000-00-00'"
            ) or 0

            sample = await self._fetch_data(table, schema, columns=[safe_col, safe_new])
            msg = f"Fixed {invalid} invalid dates in '{column}'"
            return ok(msg, [column], [new_col], sample, new_table=table)

        except Exception as e:
            return fail(f"datetime_fix_invalid error: {str(e)}\n{traceback.format_exc()}")

    async def datetime_remove_out_of_range(
        self,
        table: str,
        schema: str,
        column: str,
        min_dt: Optional[str] = None,
        max_dt: Optional[str] = None,
        backend=None,
        data_id: Optional[str] = None,
        new_table: Optional[str] = None,
    ) -> Dict[str, Any]:
        try:
            original = table
            table = await self._prepare_operation_table(
                table, schema, backend=backend, data_id=data_id, new_table=new_table,
            )
            new_col = self._generate_cleaned_column_name(column, "range_filtered")
            new_col_type = "Nullable(Date)"
            await self._add_new_column(table, schema, new_col, new_col_type)

            if min_dt is None and max_dt is None:
                min_dt, max_dt = "1900-01-01", "2100-01-01"

            qualified = self._qualified_table(table, schema)
            safe_col = SQLIdentifierSanitizer.sanitize(column)
            safe_new = SQLIdentifierSanitizer.sanitize(new_col)

            case_parts = []
            if min_dt:
                case_parts.append(f'WHEN "{safe_col}" < \'{min_dt}\'::DATE THEN NULL')
            if max_dt:
                case_parts.append(f'WHEN "{safe_col}" > \'{max_dt}\'::DATE THEN NULL')
            async def _apply(tq):
                col_q = self.db.quote_identifier(safe_col)
                new_q = self.db.quote_identifier(safe_new)
                ch_case_parts = []
                if min_dt:
                    ch_case_parts.append(f"WHEN {col_q} < toDate('{min_dt}') THEN NULL")
                if max_dt:
                    ch_case_parts.append(f"WHEN {col_q} > toDate('{max_dt}') THEN NULL")
                ch_case_expr = f"CASE {' '.join(ch_case_parts)} ELSE toDate({col_q}) END" if ch_case_parts else f"toDate({col_q})"
                await self._exec(
                    f"ALTER TABLE {tq} UPDATE {new_q} = {ch_case_expr} "
                    "WHERE 1 SETTINGS mutations_sync = 1"
                )

            await _apply(qualified)

            await self._add_new_column_if_not_exists(original, schema, new_col, new_col_type)
            await _apply(self._qualified_table(original, schema))

            affected = 0
            if case_parts:
                ch_cond = []
                col_q = self.db.quote_identifier(safe_col)
                if min_dt:
                    ch_cond.append(f"{col_q} < toDate('{min_dt}')")
                if max_dt:
                    ch_cond.append(f"{col_q} > toDate('{max_dt}')")
                ch_where = " OR ".join(ch_cond)
                affected = await self._fetchval(
                    f'SELECT COUNT(*) FROM {qualified} WHERE {ch_where}'
                ) or 0

            sample = await self._fetch_data(table, schema, columns=[safe_col, safe_new])
            msg = f"Set {affected} out-of-range dates in '{column}' to NULL"
            return ok(msg, [column], [new_col], sample, new_table=table)

        except Exception as e:
            return fail(f"datetime_remove_out_of_range error: {str(e)}\n{traceback.format_exc()}")

    # ------------------------------------------------------------------
    # Groupby cleaning
    # ------------------------------------------------------------------
    async def numeric_fillna_groupby(
        self,
        table: str,
        schema: str,
        column: str,
        group_cols: Optional[List[str]] = None,
        value: Any = None,
        mode: str = "mean",
        backend=None,
        data_id: Optional[str] = None,
        new_table: Optional[str] = None,
    ) -> Dict[str, Any]:
        try:
            original = table
            involved_cols = [*(group_cols or []), column]
            table = await self._prepare_column_operation_table(
                table, schema, involved_cols, backend=backend, data_id=data_id, new_table=new_table,
            )
            mode = mode.upper()

            suffix_map = {
                "CONSTANT": f"constant_filled_{value}",
                "MEAN": "mean_filled",
                "AVG": "mean_filled",
                "AVERAGE": "mean_filled",
                "MEDIAN": "median_filled",
                "MODE": "mode_filled",
                "STD": "std_filled",
                "VAR": "var_filled",
                "VARIANCE": "var_filled",
                "MIN": "min_filled",
                "MAX": "max_filled",
                "BFILL": "bfill_filled",
                "FFILL": "ffill_filled"
            }

            if mode not in suffix_map:
                return fail(f"Unsupported mode: {mode}")

            new_col = self._generate_cleaned_column_name(column, suffix_map[mode])
            col_type = await self._get_column_type(table, schema, column)
            await self._add_new_column(table, schema, new_col, col_type)

            qualified = self._qualified_table(table, schema)
            safe_col = SQLIdentifierSanitizer.sanitize(column)
            safe_new = SQLIdentifierSanitizer.sanitize(new_col)
            safe_group_cols = [SQLIdentifierSanitizer.sanitize(item) for item in group_cols] if group_cols else []

            if group_cols:
                group_cols = [SQLIdentifierSanitizer.sanitize(c) for c in group_cols]
                group_expr = ", ".join([f'"{c}"' for c in group_cols])
                join_cond = " AND ".join([
                    f'(t."{c}" = g."{c}" OR (t."{c}" IS NULL AND g."{c}" IS NULL))'
                    for c in group_cols
                ])

            if mode == "CONSTANT" and value is None:
                return fail("Value must be provided")

            async def _apply(tq):
                if mode == "CONSTANT":
                    converted = _sql_literal(value)
                    await self._exec(
                        f'ALTER TABLE {tq} UPDATE "{safe_new}" = COALESCE("{safe_col}", {converted}) WHERE 1'
                    )
                    return value
                elif mode in ["FFILL", "BFILL"]:
                    raise self._unsupported_backend_error()
                else:
                    stat_map = {
                        "MEAN": f'AVG("{safe_col}")',
                        "AVG": f'AVG("{safe_col}")',
                        "AVERAGE": f'AVG("{safe_col}")',
                        "MEDIAN": f'quantile(0.5)("{safe_col}")',
                        "MODE": f'(SELECT "{safe_col}" FROM {tq} WHERE "{safe_col}" IS NOT NULL GROUP BY "{safe_col}" ORDER BY COUNT(*) DESC LIMIT 1)',
                        "STD": f'stddevPop("{safe_col}")',
                        "VAR": f'varPop("{safe_col}")',
                        "VARIANCE": f'varPop("{safe_col}")',
                        "MIN": f'min("{safe_col}")',
                        "MAX": f'max("{safe_col}")',
                    }
                    stat_expr = stat_map[mode]

                    if group_cols:
                        # ClickHouse doesn't support UPDATE FROM, so recreate the table
                        await self._exec(f'ALTER TABLE {tq} DROP COLUMN "{safe_new}"')
                        temp_table = f"{table}__temp"
                        await self._exec(f"DROP TABLE IF EXISTS {self._qualified_table(temp_table, schema)}")

                        global_stat_expr = stat_expr

                        await self._exec(f"""
                            CREATE TABLE {self._qualified_table(temp_table, schema)} AS
                            SELECT t.*,
                                COALESCE(
                                    t."{safe_col}",
                                    g.val,
                                    (SELECT {global_stat_expr} FROM {tq})
                                ) AS "{safe_new}"
                            FROM {tq} t
                            LEFT JOIN (
                                SELECT {group_expr}, {stat_expr} AS val
                                FROM {tq}
                                GROUP BY {group_expr}
                            ) g ON {join_cond}
                        """)
                        await self._exec(f"DROP TABLE {tq}")
                        await self._exec(f"RENAME TABLE {self._qualified_table(temp_table, schema)} TO {tq}")
                    else:
                        await self._exec(f"""
                            ALTER TABLE {tq}
                            UPDATE "{safe_new}" = COALESCE("{safe_col}", (SELECT COALESCE({stat_expr}, 0) FROM {tq}))
                            WHERE 1
                        """)

                    return mode

            fill_value = await _apply(qualified)

            await self._add_new_column_if_not_exists(original, schema, new_col, col_type)
            await _apply(self._qualified_table(original, schema))

            null_count = await self._fetchval(
                f'SELECT COUNT(*) FROM {qualified} WHERE "{safe_col}" IS NULL'
            ) or 0

            sample = await self._fetch_data(
                table, schema,
                columns=safe_group_cols + [safe_col, safe_new]
            )

            msg = f"Filled {null_count} nulls in '{column}' using {mode} (grouped={bool(group_cols)})"

            return ok(
                msg,
                involved_cols,
                [new_col],
                sample,
                fill_mode=mode,
                fill_value=fill_value,
                new_table=table,
            )

        except Exception as e:
            return fail(
                f"numeric_fillna_groupby error: {str(e)}\n{traceback.format_exc()}", [column], [],
            )

    async def categorical_fillna_groupby(
        self,
        table: str,
        schema: str,
        column: str,
        group_cols: Optional[List[str]] = None,
        mode: str = "mode",
        backend=None,
        data_id: Optional[str] = None,
        new_table: Optional[str] = None,
    ) -> Dict[str, Any]:
        try:
            original = table
            involved_cols = [*(group_cols or []), column]
            table = await self._prepare_column_operation_table(
                table, schema, involved_cols, backend=backend, data_id=data_id, new_table=new_table,
            )
            mode = mode.upper()

            valid_modes = {"MODE", "BFILL", "FFILL"}
            if mode not in valid_modes:
                return fail(f"Unsupported mode: {mode}")

            suffix_map = {
                "MODE": "mode_filled",
                "BFILL": "bfill_filled",
                "FFILL": "ffill_filled"
            }

            new_col = self._generate_cleaned_column_name(column, suffix_map[mode])
            col_type = await self._get_column_type(table, schema, column)
            await self._add_new_column(table, schema, new_col, col_type)

            qualified = self._qualified_table(table, schema)
            safe_col = SQLIdentifierSanitizer.sanitize(column)
            safe_new = SQLIdentifierSanitizer.sanitize(new_col)
            safe_group_cols = [SQLIdentifierSanitizer.sanitize(c) for c in group_cols] if group_cols else []

            if group_cols:
                group_cols = [SQLIdentifierSanitizer.sanitize(c) for c in group_cols]
                group_expr = ", ".join([f'"{c}"' for c in group_cols])
                join_cond = " AND ".join([
                    f'(t."{c}" = m."{c}" OR (t."{c}" IS NULL AND m."{c}" IS NULL))'
                    for c in group_cols
                ])

            fill_value = None

            async def _apply(tq):
                if mode in ["FFILL", "BFILL"]:
                    raise self._unsupported_backend_error()
                else:  # MODE
                    if group_cols:
                        # ClickHouse doesn't support UPDATE FROM, recreate table via JOIN
                        await self._exec(f'ALTER TABLE {tq} DROP COLUMN "{safe_new}"')
                        temp_table = f"{table}__temp"
                        qualified_temp = self._qualified_table(temp_table, schema)
                        await self._exec(f"DROP TABLE IF EXISTS {qualified_temp}")

                        await self._exec(f"""
                            CREATE TABLE {qualified_temp} AS
                            SELECT t.*,
                                COALESCE(t."{safe_col}", m.mode_val) AS "{safe_new}"
                            FROM {tq} t
                            LEFT JOIN (
                                SELECT {group_expr}, "{safe_col}" AS mode_val
                                FROM (
                                    SELECT {group_expr}, "{safe_col}",
                                        COUNT(*) AS cnt,
                                        ROW_NUMBER() OVER (
                                            PARTITION BY {group_expr}
                                            ORDER BY COUNT(*) DESC
                                        ) AS rn
                                    FROM {tq}
                                    WHERE "{safe_col}" IS NOT NULL
                                    GROUP BY {group_expr}, "{safe_col}"
                                ) sub
                                WHERE rn = 1
                            ) m ON {join_cond}
                        """)
                        await self._exec(f"DROP TABLE {tq}")
                        await self._exec(f"RENAME TABLE {qualified_temp} TO {tq}")
                    else:
                        await self._exec(f"""
                            ALTER TABLE {tq}
                            UPDATE "{safe_new}" = COALESCE(
                                "{safe_col}",
                                (SELECT "{safe_col}" FROM {tq} WHERE "{safe_col}" IS NOT NULL GROUP BY "{safe_col}" ORDER BY COUNT(*) DESC LIMIT 1)
                            ) WHERE 1
                        """)

                    return "group_mode" if group_cols else "mode"

            fill_value = await _apply(qualified)

            await self._add_new_column_if_not_exists(original, schema, new_col, col_type)
            await _apply(self._qualified_table(original, schema))

            null_count = await self._fetchval(
                f'SELECT COUNT(*) FROM {qualified} WHERE "{safe_col}" IS NULL'
            ) or 0

            sample = await self._fetch_data(
                table, schema,
                columns=safe_group_cols + [safe_col, safe_new]
            )

            msg = f"Processed '{column}' using {mode} (grouped={bool(group_cols)}), affected {null_count} nulls"

            return ok(
                msg,
                involved_cols,
                [new_col],
                sample,
                fill_mode=mode,
                fill_value=fill_value,
                new_table=table,
            )

        except Exception as e:
            return fail(
                f"categorical_fillna_groupby error: {str(e)}\n{traceback.format_exc()}", [column], [],
            )

    async def datetime_fillna_groupby(
        self,
        table: str,
        schema: str,
        column: str,
        group_cols: Optional[List[str]] = None,
        mode: str = "mean",
        backend=None,
        data_id: Optional[str] = None,
        new_table: Optional[str] = None,
    ) -> Dict[str, Any]:
        try:
            original = table
            involved_cols = [*(group_cols or []), column]
            table = await self._prepare_column_operation_table(
                table, schema, involved_cols, backend=backend, data_id=data_id, new_table=new_table,
            )
            mode = mode.upper()

            valid_modes = {"MIN", "MAX", "MEAN", "MEDIAN", "MODE", "FFILL", "BFILL"}
            if mode not in valid_modes:
                return fail(f"Unsupported mode: {mode}")

            suffix_map = {
                "MIN": "min_filled",
                "MAX": "max_filled",
                "MEAN": "mean_filled",
                "MEDIAN": "median_filled",
                "MODE": "mode_filled",
                "BFILL": "bfill_filled",
                "FFILL": "ffill_filled"
            }

            new_col = self._generate_cleaned_column_name(column, suffix_map[mode])
            await self._add_new_column(table, schema, new_col, "TIMESTAMP")

            qualified = self._qualified_table(table, schema)
            safe_col = SQLIdentifierSanitizer.sanitize(column)
            safe_new = SQLIdentifierSanitizer.sanitize(new_col)
            safe_group_cols = [SQLIdentifierSanitizer.sanitize(c) for c in group_cols] if group_cols else []

            if group_cols:
                group_cols = [SQLIdentifierSanitizer.sanitize(c) for c in group_cols]
                group_expr = ", ".join([f'"{c}"' for c in group_cols])

                join_cond = " AND ".join([
                    f'(t."{c}" = g."{c}" OR (t."{c}" IS NULL AND g."{c}" IS NULL))'
                    for c in group_cols
                ])

                partition = f'PARTITION BY {group_expr}'
            else:
                partition = ""

            fill_value = None

            async def _apply(tq):
                if mode in ["FFILL", "BFILL"]:
                    # ponytail: ClickHouse has no LAST/FIRST_VALUE IGNORE NULLS,
                    # so use the cumulative-non-null-count "islands and gaps"
                    # idiom — each row's island id is the count of non-null
                    # values seen so far (asc for FFILL, desc for BFILL). Every
                    # island has exactly one non-null anchor; null rows inherit
                    # its value. Rebuild via temp table (same idiom as the
                    # stat-groupby ClickHouse branch below).
                    order_dir = "DESC" if mode == "BFILL" else "ASC"
                    if safe_group_cols:
                        fm_select = ", ".join(f'"{c}"' for c in safe_group_cols) + ", "
                        fm_group = "GROUP BY " + ", ".join(f'"{c}"' for c in safe_group_cols) + ", _island_id"
                        fm_join = " AND ".join(
                            [f'b."{c}" = fm."{c}"' for c in safe_group_cols]
                            + ["b._island_id = fm._island_id"]
                        )
                    else:
                        fm_select = ""
                        fm_group = "GROUP BY _island_id"
                        fm_join = "b._island_id = fm._island_id"

                    await self._exec(f'ALTER TABLE {tq} DROP COLUMN "{safe_new}"')
                    temp_table = f"{table}__temp"
                    qualified_temp = self._qualified_table(temp_table, schema)
                    await self._exec(f"DROP TABLE IF EXISTS {qualified_temp}")
                    await self._exec(f"""
                        CREATE TABLE {qualified_temp} AS
                        WITH base AS (
                            SELECT t.*, ROW_NUMBER() OVER () AS __idx
                            FROM {tq} t
                        ),
                        with_island AS (
                            SELECT b.*,
                                countIf(isNotNull("{safe_col}")) OVER (
                                    {partition}
                                    ORDER BY __idx {order_dir}
                                    ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
                                ) AS _island_id
                            FROM base b
                        ),
                        fill_map AS (
                            SELECT
                                {fm_select}
                                _island_id,
                                any("{safe_col}") AS fill_val
                            FROM with_island
                            WHERE "{safe_col}" IS NOT NULL
                            {fm_group}
                        )
                        SELECT b.*,
                            COALESCE(b."{safe_col}", fm.fill_val) AS "{safe_new}"
                        FROM with_island b
                        LEFT JOIN fill_map fm ON {fm_join}
                        ORDER BY b.__idx
                    """)
                    await self._exec(f"DROP TABLE {tq}")
                    await self._exec(f"RENAME TABLE {qualified_temp} TO {tq}")
                    return mode
                else:
                    stat_map = {
                        "MIN": f'min("{safe_col}")',
                        "MAX": f'max("{safe_col}")',
                        "MEAN": f'toDateTime(toUInt32(avg(toUnixTimestamp("{safe_col}"))))',
                        "MEDIAN": f'toDateTime(toUInt32(quantile(0.5)(toUnixTimestamp("{safe_col}"))))',
                        "MODE": f'(SELECT "{safe_col}" FROM {tq} WHERE "{safe_col}" IS NOT NULL GROUP BY "{safe_col}" ORDER BY COUNT(*) DESC LIMIT 1)'
                    }
                    stat_expr = stat_map[mode]

                    if group_cols:
                        await self._exec(f'ALTER TABLE {tq} DROP COLUMN "{safe_new}"')
                        temp_table = f"{table}__temp"
                        qualified_temp = self._qualified_table(temp_table, schema)
                        await self._exec(f"DROP TABLE IF EXISTS {qualified_temp}")

                        await self._exec(f"""
                            CREATE TABLE {qualified_temp} AS
                            SELECT t.*,
                                COALESCE(
                                    t."{safe_col}",
                                    g.val,
                                    (SELECT {stat_expr} FROM {tq})
                                ) AS "{safe_new}"
                            FROM {tq} t
                            LEFT JOIN (
                                SELECT {group_expr}, {stat_expr} AS val
                                FROM {tq}
                                GROUP BY {group_expr}
                            ) g ON {join_cond}
                        """)
                        await self._exec(f"DROP TABLE {tq}")
                        await self._exec(f"RENAME TABLE {qualified_temp} TO {tq}")
                        return None
                    else:
                        await self._exec(f"""
                            ALTER TABLE {tq}
                            UPDATE "{safe_new}" = COALESCE("{safe_col}", (SELECT {stat_expr} FROM {tq} WHERE "{safe_col}" IS NOT NULL))
                            WHERE 1
                        """)
                        return mode

            fill_value = await _apply(qualified)

            await self._add_new_column_if_not_exists(original, schema, new_col, "TIMESTAMP")
            await _apply(self._qualified_table(original, schema))

            null_count = await self._fetchval(
                f'SELECT COUNT(*) FROM {qualified} WHERE "{safe_col}" IS NULL'
            ) or 0

            sample = await self._fetch_data(
                table, schema,
                columns=safe_group_cols + [safe_col, safe_new]
            )

            msg = f"Filled {null_count} nulls in '{column}' using {mode} (grouped={bool(group_cols)})"

            return ok(
                msg,
                involved_cols,
                [new_col],
                sample,
                fill_mode=mode,
                fill_value=fill_value,
                new_table=table,
            )

        except Exception as e:
            return fail(
                f"datetime_fillna_groupby error: {str(e)}\n{traceback.format_exc()}", [column], [],
            )
