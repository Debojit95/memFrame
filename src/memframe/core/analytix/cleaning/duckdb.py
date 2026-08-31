"""
DuckDB cleaning operations.

The DuckDB-specific branch of every cleaning method is copied verbatim from the
original monolith; base.py keeps only shared infrastructure. Behaviour is
preserved exactly.
"""

from typing import Any, Dict, List, Optional
import traceback

from memframe.core.analytix.cleaning.base import DataCleaningOps, _sql_literal
from memframe.utils.helper import SQLIdentifierSanitizer
from memframe.core.analytix._response import fail, ok


class DuckDBCleaningOps(DataCleaningOps):

    # ------------------------------------------------------------------
    # Numeric
    # ------------------------------------------------------------------
    def _numeric_target_for(self, pg_type: str) -> str:
        return {
            "SMALLINT": "SMALLINT",
            "INTEGER": "INTEGER",
            "BIGINT": "BIGINT",
            "FLOAT": "DOUBLE",
        }.get(pg_type, "NUMERIC")

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
                        f'UPDATE {tq} SET "{safe_new}" = COALESCE("{safe_col}", {converted})'
                    )
                    return value

                elif mode in ["FFILL", "BFILL"]:
                    row_id = "rowid"

                    if mode == "FFILL":
                        window_expr = f'''
                            LAST_VALUE("{safe_col}" IGNORE NULLS)
                            OVER (
                                ORDER BY __idx
                                ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
                            )
                        '''
                    else:
                        window_expr = f'''
                            FIRST_VALUE("{safe_col}" IGNORE NULLS)
                            OVER (
                                ORDER BY __idx
                                ROWS BETWEEN CURRENT ROW AND UNBOUNDED FOLLOWING
                            )
                        '''

                    await self._exec(f"""
                        WITH base AS (
                            SELECT *,
                                ROW_NUMBER() OVER () AS __idx,
                                {row_id} AS __rid
                            FROM {tq}
                        ),
                        filled AS (
                            SELECT *,
                                {window_expr} AS filled_val
                            FROM base
                        )
                        UPDATE {tq} t
                        SET "{safe_new}" = COALESCE(t."{safe_col}", f.filled_val)
                        FROM filled f
                        WHERE t.{row_id} = f.__rid
                    """)

                    return mode

                else:
                    stat_map = {
                        "MEAN": f'AVG("{safe_col}")',
                        "AVG": f'AVG("{safe_col}")',
                        "AVERAGE": f'AVG("{safe_col}")',
                        "MEDIAN": f'PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY "{safe_col}")',
                        "MODE": f"""
                            (SELECT "{safe_col}"
                            FROM {tq}
                            WHERE "{safe_col}" IS NOT NULL
                            GROUP BY "{safe_col}"
                            ORDER BY COUNT(*) DESC
                            LIMIT 1)
                        """,
                        "STD": f'STDDEV_POP("{safe_col}")',
                        "VAR": f'VAR_POP("{safe_col}")',
                        "VARIANCE": f'VAR_POP("{safe_col}")',
                        "MIN": f'MIN("{safe_col}")',
                        "MAX": f'MAX("{safe_col}")',
                    }

                    stat_expr = stat_map[mode]

                    await self._exec(f"""
                        WITH stat_val AS (
                            SELECT COALESCE({stat_expr}, 0) AS val
                            FROM {tq}
                            WHERE "{safe_col}" IS NOT NULL
                        )
                        UPDATE {tq}
                        SET "{safe_new}" = COALESCE("{safe_col}", (SELECT val FROM stat_val))
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
                await self._exec(f'UPDATE {tq} SET "{safe_new}" = {case_expr}')

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
                    WITH stats AS (
                        SELECT AVG("{safe_col}") AS mean, STDDEV_POP("{safe_col}") AS sd
                        FROM {tq}
                        WHERE "{safe_col}" IS NOT NULL
                    )
                    UPDATE {tq}
                    SET "{safe_new}" = CASE
                        WHEN ABS(("{safe_col}" - stats.mean) / NULLIF(stats.sd, 0)) > {z_thresh} THEN NULL
                        ELSE "{safe_col}"
                    END
                    FROM stats
                """)

            await _apply(qualified)

            await self._add_new_column_if_not_exists(original, schema, new_col, col_type)
            await _apply(self._qualified_table(original, schema))

            outlier_count = await self._fetchval(f"""
                WITH stats AS (
                    SELECT AVG("{safe_col}") AS mean, STDDEV_POP("{safe_col}") AS sd
                    FROM {qualified}
                    WHERE "{safe_col}" IS NOT NULL
                )
                SELECT COUNT(*)
                FROM {qualified}, stats
                WHERE "{safe_col}" IS NOT NULL
                  AND ABS(("{safe_col}" - stats.mean) / NULLIF(stats.sd, 0)) > {z_thresh}
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

            cleaned_expr = f"""NULLIF(REGEXP_REPLACE(CAST("{safe_col}" AS VARCHAR), '[^0-9.+-]', '', 'g'), '')"""
            numeric_check = f"REGEXP_MATCHES({cleaned_expr}, '^[+-]?([0-9]+([.][0-9]*)?|[.][0-9]+)$')"

            target_type = await self._detect_numeric_target(qualified, cleaned_expr)

            async def _apply(tq):
                await self._exec(f"""
                    ALTER TABLE {tq}
                    ALTER COLUMN "{safe_col}" TYPE {target_type}
                    USING (CASE
                        WHEN {numeric_check}
                            THEN CAST({cleaned_expr} AS {target_type})
                        ELSE NULL
                    END)
                """)

            await _apply(qualified)

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
                        f'UPDATE {tq} SET "{safe_new}" = COALESCE("{safe_col}", \'{val_str}\')'
                    )
                    return value

                elif mode in ["FFILL", "BFILL"]:
                    row_id = "rowid"

                    if mode == "FFILL":
                        window_expr = f'''
                            LAST_VALUE("{safe_col}" IGNORE NULLS)
                            OVER (
                                ORDER BY __idx
                                ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
                            )
                        '''
                    else:
                        window_expr = f'''
                            FIRST_VALUE("{safe_col}" IGNORE NULLS)
                            OVER (
                                ORDER BY __idx
                                ROWS BETWEEN CURRENT ROW AND UNBOUNDED FOLLOWING
                            )
                        '''

                    await self._exec(f"""
                        WITH base AS (
                            SELECT *,
                                ROW_NUMBER() OVER () AS __idx,
                                {row_id} AS __rid
                            FROM {tq}
                        ),
                        filled AS (
                            SELECT *,
                                {window_expr} AS filled_val
                            FROM base
                        )
                        UPDATE {tq} t
                        SET "{safe_new}" = COALESCE(t."{safe_col}", f.filled_val)
                        FROM filled f
                        WHERE t.{row_id} = f.__rid
                    """)

                    return mode

                elif mode == "MODE":
                    await self._exec(f"""
                        WITH mode_val AS (
                            SELECT "{safe_col}" AS mode_value
                            FROM {tq}
                            WHERE "{safe_col}" IS NOT NULL
                            GROUP BY "{safe_col}"
                            ORDER BY COUNT(*) DESC
                            LIMIT 1
                        )
                        UPDATE {tq}
                        SET "{safe_new}" = COALESCE("{safe_col}", (SELECT mode_value FROM mode_val))
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
                        UPDATE {tq}
                        SET "{safe_new}" = COALESCE({case_expr}, "{safe_col}")
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
                await self._exec(f'UPDATE {tq} SET "{safe_new}" = {case_expr}')

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
                    UPDATE {tq}
                    SET "{safe_new}" = CASE WHEN "{safe_col}" IN ({in_list}) THEN "{safe_col}" ELSE NULL END
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
                await self._exec(f"""
                    WITH freq AS (
                        SELECT "{safe_col}", COUNT(*) AS cnt
                        FROM {tq}
                        WHERE "{safe_col}" IS NOT NULL
                        GROUP BY "{safe_col}"
                    )
                    UPDATE {tq} AS tgt
                    SET "{safe_new}" = CASE
                        WHEN freq.cnt < {min_count} THEN '{other_esc}'
                        ELSE tgt."{safe_col}"
                    END
                    FROM freq
                    WHERE tgt."{safe_col}" = freq."{safe_col}"
                """)

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
                        f'UPDATE {tq} SET "{safe_new}" = COALESCE("{safe_col}", ?::TIMESTAMP)',
                        str(value),
                    )
                    return value

                elif mode == "NOW":
                    await self._exec(
                        f'UPDATE {tq} SET "{safe_new}" = COALESCE("{safe_col}", CURRENT_TIMESTAMP)'
                    )
                    return "CURRENT_TIMESTAMP"

                elif mode in ["FFILL", "BFILL"]:
                    row_id = "rowid"

                    if mode == "FFILL":
                        window_expr = f'''
                            LAST_VALUE("{safe_col}" IGNORE NULLS)
                            OVER (
                                ORDER BY __idx
                                ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
                            )
                        '''
                    else:
                        window_expr = f'''
                            FIRST_VALUE("{safe_col}" IGNORE NULLS)
                            OVER (
                                ORDER BY __idx
                                ROWS BETWEEN CURRENT ROW AND UNBOUNDED FOLLOWING
                            )
                        '''

                    await self._exec(f"""
                        WITH base AS (
                            SELECT *,
                                ROW_NUMBER() OVER () AS __idx,
                                {row_id} AS __rid
                            FROM {tq}
                        ),
                        filled AS (
                            SELECT *,
                                {window_expr} AS filled_val
                            FROM base
                        )
                        UPDATE {tq} t
                        SET "{safe_new}" = COALESCE(t."{safe_col}", f.filled_val)
                        FROM filled f
                        WHERE t.{row_id} = f.__rid
                    """)

                    return mode

                else:
                    stat_map = {
                        "MIN": f'MIN("{safe_col}")',
                        "MAX": f'MAX("{safe_col}")',
                        "MEAN": f'TO_TIMESTAMP(AVG(EPOCH("{safe_col}")))',
                        "MEDIAN": f'TO_TIMESTAMP(MEDIAN(EPOCH("{safe_col}")))',
                        "MODE": f"""
                            (
                                SELECT "{safe_col}"
                                FROM {tq}
                                WHERE "{safe_col}" IS NOT NULL
                                GROUP BY "{safe_col}"
                                ORDER BY COUNT(*) DESC
                                LIMIT 1
                            )
                        """
                    }

                    stat_expr = stat_map[mode]

                    await self._exec(f"""
                        WITH stat_val AS (
                            SELECT {stat_expr} AS val
                            FROM {tq}
                            WHERE "{safe_col}" IS NOT NULL
                        )
                        UPDATE {tq}
                        SET "{safe_new}" = COALESCE("{safe_col}", (SELECT val FROM stat_val))
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

            text_expr = f'CAST("{safe_col}" AS VARCHAR)'

            async def _apply(tq):
                await self._exec(f"""
                    UPDATE {tq}
                    SET "{safe_new}" = CASE
                        WHEN {text_expr} = '0000-00-00' THEN NULL
                        ELSE "{safe_col}"
                    END
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
            new_col_type = "DATE"
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
            case_expr = f"CASE {' '.join(case_parts)} ELSE \"{safe_col}\" END" if case_parts else f'"{safe_col}"'

            async def _apply(tq):
                await self._exec(f'UPDATE {tq} SET "{safe_new}" = {case_expr}')

            await _apply(qualified)

            await self._add_new_column_if_not_exists(original, schema, new_col, new_col_type)
            await _apply(self._qualified_table(original, schema))

            affected = 0
            if case_parts:
                cond = []
                if min_dt:
                    cond.append(f'"{safe_col}" < \'{min_dt}\'::DATE')
                if max_dt:
                    cond.append(f'"{safe_col}" > \'{max_dt}\'::DATE')
                where_clause = " OR ".join(cond)
                affected = await self._fetchval(
                    f'SELECT COUNT(*) FROM {qualified} WHERE {where_clause}'
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
                        f'UPDATE {tq} SET "{safe_new}" = COALESCE("{safe_col}", {converted})'
                    )
                    return value

                elif mode in ["FFILL", "BFILL"]:
                    row_id = "rowid"
                    partition = f'PARTITION BY {group_expr}' if group_cols else ""

                    if mode == "FFILL":
                        window_expr = f'''
                            LAST_VALUE("{safe_col}" IGNORE NULLS)
                            OVER (
                                {partition}
                                ORDER BY __idx
                                ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
                            )
                        '''
                    else:
                        window_expr = f'''
                            FIRST_VALUE("{safe_col}" IGNORE NULLS)
                            OVER (
                                {partition}
                                ORDER BY __idx
                                ROWS BETWEEN CURRENT ROW AND UNBOUNDED FOLLOWING
                            )
                        '''

                    await self._exec(f"""
                        WITH base AS (
                            SELECT *,
                                ROW_NUMBER() OVER () AS __idx,
                                {row_id} AS __rid
                            FROM {tq}
                        ),
                        filled AS (
                            SELECT *,
                                {window_expr} AS filled_val
                            FROM base
                        )
                        UPDATE {tq} t
                        SET "{safe_new}" = COALESCE(t."{safe_col}", f.filled_val)
                        FROM filled f
                        WHERE t.{row_id} = f.__rid
                    """)

                    return mode

                else:
                    stat_map = {
                        "MEAN": f'AVG(CASE WHEN "{safe_col}" IS NOT NULL THEN "{safe_col}" END)',
                        "AVG": f'AVG(CASE WHEN "{safe_col}" IS NOT NULL THEN "{safe_col}" END)',
                        "AVERAGE": f'AVG(CASE WHEN "{safe_col}" IS NOT NULL THEN "{safe_col}" END)',
                        "MEDIAN": f'PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY "{safe_col}")',
                        "MODE": f"""
                            (SELECT "{safe_col}"
                            FROM {tq}
                            WHERE "{safe_col}" IS NOT NULL
                            GROUP BY "{safe_col}"
                            ORDER BY COUNT(*) DESC
                            LIMIT 1)
                        """,
                        "STD": f'STDDEV_POP("{safe_col}")',
                        "VAR": f'VAR_POP("{safe_col}")',
                        "VARIANCE": f'VAR_POP("{safe_col}")',
                        "MIN": f'MIN("{safe_col}")',
                        "MAX": f'MAX("{safe_col}")',
                    }

                    stat_expr = stat_map[mode]

                    if group_cols:
                        await self._exec(f"""
                            WITH grouped AS (
                                SELECT {group_expr},
                                    {stat_expr} AS val
                                FROM {tq}
                                GROUP BY {group_expr}
                            ),
                            global_stat AS (
                                SELECT {stat_expr} AS val FROM {tq}
                            )
                            UPDATE {tq} t
                            SET "{safe_new}" = COALESCE(
                                t."{safe_col}",
                                g.val,
                                (SELECT val FROM global_stat)
                            )
                            FROM grouped g
                            WHERE {join_cond}
                        """)
                    else:
                        await self._exec(f"""
                            WITH stat_val AS (
                                SELECT COALESCE({stat_expr}, 0) AS val
                                FROM {tq}
                            )
                            UPDATE {tq}
                            SET "{safe_new}" = COALESCE("{safe_col}", (SELECT val FROM stat_val))
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
                partition = f'PARTITION BY {group_expr}'
            else:
                partition = ""

            fill_value = None

            async def _apply(tq):
                if mode in ["FFILL", "BFILL"]:
                    row_id = "rowid"

                    if mode == "FFILL":
                        window_expr = f'''
                            LAST_VALUE("{safe_col}" IGNORE NULLS)
                            OVER (
                                {partition}
                                ORDER BY __idx
                                ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
                            )
                        '''
                    else:
                        window_expr = f'''
                            FIRST_VALUE("{safe_col}" IGNORE NULLS)
                            OVER (
                                {partition}
                                ORDER BY __idx
                                ROWS BETWEEN CURRENT ROW AND UNBOUNDED FOLLOWING
                            )
                        '''

                    await self._exec(f"""
                        WITH base AS (
                            SELECT *,
                                ROW_NUMBER() OVER () AS __idx,
                                {row_id} AS __rid
                            FROM {tq}
                        ),
                        filled AS (
                            SELECT *,
                                {window_expr} AS filled_val
                            FROM base
                        )
                        UPDATE {tq} t
                        SET "{safe_new}" = COALESCE(t."{safe_col}", f.filled_val)
                        FROM filled f
                        WHERE t.{row_id} = f.__rid
                    """)

                    return mode

                else:  # MODE
                    if group_cols:
                        await self._exec(f"""
                            WITH mode_vals AS (
                                SELECT {group_expr},
                                    "{safe_col}" AS mode_val
                                FROM (
                                    SELECT {group_expr},
                                        "{safe_col}",
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
                            )
                            UPDATE {tq} t
                            SET "{safe_new}" = COALESCE(t."{safe_col}", m.mode_val)
                            FROM mode_vals m
                            WHERE {join_cond}
                        """)
                    else:
                        await self._exec(f"""
                            WITH mode_val AS (
                                SELECT "{safe_col}" AS mode_value
                                FROM {tq}
                                WHERE "{safe_col}" IS NOT NULL
                                GROUP BY "{safe_col}"
                                ORDER BY COUNT(*) DESC
                                LIMIT 1
                            )
                            UPDATE {tq}
                            SET "{safe_new}" = COALESCE("{safe_col}", (SELECT mode_value FROM mode_val))
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
                    row_id = "rowid"

                    if mode == "FFILL":
                        window_expr = f'''
                            LAST_VALUE("{safe_col}" IGNORE NULLS)
                            OVER (
                                {partition}
                                ORDER BY __idx
                                ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
                            )
                        '''
                    else:
                        window_expr = f'''
                            FIRST_VALUE("{safe_col}" IGNORE NULLS)
                            OVER (
                                {partition}
                                ORDER BY __idx
                                ROWS BETWEEN CURRENT ROW AND UNBOUNDED FOLLOWING
                            )
                        '''

                    await self._exec(f"""
                        WITH base AS (
                            SELECT *,
                                ROW_NUMBER() OVER () AS __idx,
                                {row_id} AS __rid
                            FROM {tq}
                        ),
                        filled AS (
                            SELECT *,
                                {window_expr} AS filled_val
                            FROM base
                        )
                        UPDATE {tq} t
                        SET "{safe_new}" = COALESCE(t."{safe_col}", f.filled_val)
                        FROM filled f
                        WHERE t.{row_id} = f.__rid
                    """)

                    return mode

                else:
                    stat_map = {
                        "MIN": f'MIN("{safe_col}")',
                        "MAX": f'MAX("{safe_col}")',
                        "MEAN": f'TO_TIMESTAMP(AVG(EPOCH("{safe_col}")))',
                        "MEDIAN": f'TO_TIMESTAMP(MEDIAN(EPOCH("{safe_col}")))',
                        "MODE": f"""
                            (
                                SELECT "{safe_col}"
                                FROM {tq}
                                WHERE "{safe_col}" IS NOT NULL
                                GROUP BY "{safe_col}"
                                ORDER BY COUNT(*) DESC
                                LIMIT 1
                            )
                        """
                    }

                    stat_expr = stat_map[mode]

                    if group_cols:
                        await self._exec(f"""
                            WITH grouped AS (
                                SELECT {group_expr},
                                    {stat_expr} AS val
                                FROM {tq}
                                GROUP BY {group_expr}
                            ),
                            global_stat AS (
                                SELECT COALESCE({stat_expr}, CURRENT_TIMESTAMP) AS val
                                FROM {tq}
                            )
                            UPDATE {tq} t
                            SET "{safe_new}" = COALESCE(
                                t."{safe_col}",
                                g.val,
                                (SELECT val FROM global_stat)
                            )
                            FROM grouped g
                            WHERE {join_cond}
                        """)
                    else:
                        await self._exec(f"""
                            WITH stat_val AS (
                                SELECT {stat_expr} AS val
                                FROM {tq}
                                WHERE "{safe_col}" IS NOT NULL
                            )
                            UPDATE {tq}
                            SET "{safe_new}" = COALESCE("{safe_col}", (SELECT val FROM stat_val))
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
