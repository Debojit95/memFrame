import traceback
from typing import Any, Dict, List, Optional
from memframe.core.analytix.cleaning.base import _sql_literal
from memframe.core.analytix._response import fail, ok
from memframe.utils.helper import SQLIdentifierSanitizer

from memframe.core.analytix.cleaning.duckdb import DuckDBCleaningOps


class DuckDBGroupByCleaningOps(DuckDBCleaningOps):
    """DuckDB backend group-by fillna ops (moved from cleaning)."""

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
                    row_id = self._rowid_expr()
                    partition = f'PARTITION BY {group_expr}' if group_cols else ""

                    if mode == "FFILL":
                        window_expr = f'''
                            {self._ffill_fn(safe_col)}
                            OVER (
                                {partition}
                                ORDER BY __idx
                                ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
                            )
                        '''
                    else:
                        window_expr = f'''
                            {self._bfill_fn(safe_col)}
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
                    row_id = self._rowid_expr()

                    if mode == "FFILL":
                        window_expr = f'''
                            {self._ffill_fn(safe_col)}
                            OVER (
                                {partition}
                                ORDER BY __idx
                                ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
                            )
                        '''
                    else:
                        window_expr = f'''
                            {self._bfill_fn(safe_col)}
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
                    row_id = self._rowid_expr()

                    if mode == "FFILL":
                        window_expr = f'''
                            {self._ffill_fn(safe_col)}
                            OVER (
                                {partition}
                                ORDER BY __idx
                                ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
                            )
                        '''
                    else:
                        window_expr = f'''
                            {self._bfill_fn(safe_col)}
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
