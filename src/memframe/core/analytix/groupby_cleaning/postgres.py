import traceback
from typing import Any, Dict, List, Optional
from memframe.core.analytix._response import fail, ok
from memframe.utils.helper import SQLIdentifierSanitizer
from memframe.core.analytix.cleaning.postgres import PostgresCleaningOps
from memframe.core.analytix.groupby_cleaning.duckdb import DuckDBGroupByCleaningOps


class PostgresGroupByCleaningOps(PostgresCleaningOps, DuckDBGroupByCleaningOps):
    """PostgreSQL backend group-by fillna ops (moved from cleaning)."""

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
                table,
                schema,
                involved_cols,
                backend=backend,
                data_id=data_id,
                new_table=new_table,
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
                    if mode == "FFILL":
                        window_expr = f'''
                            MAX("{safe_col}") OVER (
                                {partition}
                                ORDER BY __idx
                                ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
                            )
                        '''
                    else:
                        window_expr = f'''
                            MIN("{safe_col}") OVER (
                                {partition}
                                ORDER BY __idx
                                ROWS BETWEEN CURRENT ROW AND UNBOUNDED FOLLOWING
                            )
                        '''

                    temp_table = f"{table}__fill_temp"
                    qualified_temp = self._qualified_table(temp_table, schema)
                    cols = ", ".join(f'"{c}"' for c in (safe_group_cols + [safe_col]))
                    bare_name = f"{tq}".split(".", 1)[-1]
                    await self._exec(f"DROP TABLE IF EXISTS {qualified_temp}")
                    await self._exec(f"""
                        CREATE TABLE {qualified_temp} AS
                        SELECT {cols}, COALESCE("{safe_col}", filled_val) AS "{safe_new}"
                        FROM (
                            WITH base AS (
                                SELECT *,
                                    ROW_NUMBER() OVER (ORDER BY ctid) AS __idx
                                FROM {tq}
                            ),
                            filled AS (
                                SELECT *, {window_expr} AS filled_val
                                FROM base
                            )
                            SELECT * FROM filled
                        ) sub
                        ORDER BY __idx
                    """)
                    await self._exec(f"DROP TABLE {tq}")
                    await self._exec(f"ALTER TABLE {qualified_temp} RENAME TO {bare_name}")

                    return mode

                else:
                    stat_map = {
                        "MIN": f'MIN("{safe_col}")',
                        "MAX": f'MAX("{safe_col}")',
                        "MEAN": f'TO_TIMESTAMP(AVG(EXTRACT(EPOCH FROM "{safe_col}")))',
                        "MEDIAN": f'''
                            TO_TIMESTAMP(
                                PERCENTILE_CONT(0.5)
                                WITHIN GROUP (ORDER BY EXTRACT(EPOCH FROM "{safe_col}"))
                            )
                        ''',
                        "MODE": f'''
                            (
                                SELECT "{safe_col}"
                                FROM {tq}
                                WHERE "{safe_col}" IS NOT NULL
                                GROUP BY "{safe_col}"
                                ORDER BY COUNT(*) DESC
                                LIMIT 1
                            )
                        ''',
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

            # Mirror the new column onto the original table (in-place mutation)
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
