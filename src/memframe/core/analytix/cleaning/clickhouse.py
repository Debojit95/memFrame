import traceback

from typing import Any, Dict, Optional

from memframe.core.analytix.cleaning.base import DataCleaningOps, _sql_literal
from memframe.utils.helper import SQLIdentifierSanitizer
from memframe.core.analytix._response import fail, ok


class ClickHouseCleaningOps(DataCleaningOps):
    """ClickHouse backend.

    Inherits the four operations whose only dialect difference is the
    in-place UPDATE form (`_update_stmt` -> ALTER TABLE ... UPDATE ... WHERE 1)
    and text casts (`_text_expr` -> toString). The remaining ten operations
    use ClickHouse-specific strategies (mutations with SETTINGS,
    quantile()/stddevPop aggregates, temp-table rebuilds instead of
    UPDATE ... FROM) and are kept as explicit overrides below.
    """

    def _numeric_target_for(self, pg_type: str) -> str:
        return {
            "SMALLINT": "Int16",
            "INTEGER": "Int32",
            "BIGINT": "Int64",
            "FLOAT": "Float64",
        }.get(pg_type, "Decimal(18, 6)")

    def _text_expr(self, col: str) -> str:
        return f'toString("{col}")'

    def _update_stmt(self, tq: str, set_expr: str) -> str:
        return f"ALTER TABLE {tq} UPDATE {set_expr} WHERE 1"

    def _regex_match(self, expr: str, pattern: str) -> str:
        # ponytail: base REGEXP_MATCHES is DuckDB/PG-only; match() is the CH
        # spelling (also fixes numeric_convert_text's predicate on CH).
        return f"match({expr}, '{pattern}')"

    def _regex_replace(self, expr: str, pattern: str, replacement: str) -> str:
        return f"replaceRegexpAll({expr}, '{pattern}', '{replacement}')"

    # ------------------------------------------------------------------
    # Numeric
    # ------------------------------------------------------------------
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

    def _double_cast(self, expr: str) -> str:
        return f"CAST({expr} AS Float64)"

    async def numeric_interpolate(
        self,
        table: str,
        schema: str,
        column: str,
        method: str = "linear",
        limit: Optional[int] = None,
        limit_direction: str = "forward",
        limit_area: Optional[str] = None,
        order_by: Any = None,
        backend=None,
        data_id: Optional[str] = None,
        new_table: Optional[str] = None,
    ) -> Dict[str, Any]:
        # ponytail: window functions are illegal in ALTER TABLE ... UPDATE, so
        # rebuild via temp tables (same idiom as the islands FFILL branch).
        # __idx is the interpolation sequence (order_by or physical); __pos
        # pins the original physical order so rebuilds never reorder rows.
        try:
            original = table
            col_type = await self._get_column_type(table, schema, column)
            try:
                direction, area, order_cols = self._validate_interpolate(
                    column, method, limit, limit_direction, limit_area, col_type, order_by
                )
            except ValueError as ve:
                return fail(str(ve), [column], [])
            if order_cols:
                available = await self.db.get_column_types(table, schema)
                missing = [c for c in order_cols if SQLIdentifierSanitizer.sanitize(c) not in available]
                if missing:
                    return fail(f"Order-by column(s) not found: {missing}", [column], [])
            table = await self._prepare_column_operation_table(
                table, schema, [column, *order_cols], backend=backend, data_id=data_id, new_table=new_table,
            )

            new_col = self._generate_cleaned_column_name(column, "interpolated")
            safe_col = SQLIdentifierSanitizer.sanitize(column)
            safe_new = SQLIdentifierSanitizer.sanitize(new_col)
            case = self._interpolate_case(safe_col, limit, direction, area)
            if order_cols:
                order_term = ", ".join(f'"{SQLIdentifierSanitizer.sanitize(c)}" ASC' for c in order_cols)
                idx_window = f"ROW_NUMBER() OVER (ORDER BY {order_term})"
            else:
                idx_window = "ROW_NUMBER() OVER ()"

            qualified = self._qualified_table(table, schema)
            temp_table = f"{table}__interp_temp"
            qualified_temp = self._qualified_table(temp_table, schema)
            await self._exec("DROP TABLE IF EXISTS {}".format(qualified_temp))
            await self._exec(f"""
                CREATE TABLE {qualified_temp} AS
                WITH base_pos AS (
                    SELECT *, ROW_NUMBER() OVER () AS __pos
                    FROM {qualified}
                ),
                base AS (
                    SELECT *, {idx_window} AS __idx
                    FROM base_pos
                ),
                isl AS (
                    SELECT b.*,
                        countIf(isNotNull("{safe_col}")) OVER (
                            ORDER BY __idx ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
                        ) AS _isl_f,
                        countIf(isNotNull("{safe_col}")) OVER (
                            ORDER BY __idx DESC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
                        ) AS _isl_b
                    FROM base b
                ),
                anch AS (
                    SELECT s.*,
                        first_value("{safe_col}") OVER (PARTITION BY _isl_f ORDER BY __idx) AS _pv,
                        first_value(__idx) OVER (PARTITION BY _isl_f ORDER BY __idx) AS _pp,
                        first_value("{safe_col}") OVER (PARTITION BY _isl_b ORDER BY __idx DESC) AS _nv,
                        first_value(__idx) OVER (PARTITION BY _isl_b ORDER BY __idx DESC) AS _np
                    FROM isl s
                )
                SELECT __pos, "{safe_col}", {case} AS "{safe_new}"
                FROM anch ORDER BY __pos
            """)
            await self._exec(f"DROP TABLE {qualified}")
            await self._exec(f"RENAME TABLE {qualified_temp} TO {qualified}")

            # apply to the original (multi-column) table via positional join;
            # the join keys are stored columns — a CTE referencing the outer
            # scope is a correlated subquery, which CH rejects in JOINs.
            original_qualified = self._qualified_table(original, schema)
            ocols = [c for c in await self.db.get_column_types(original, schema) if c != safe_new]
            ocols_clause = ", ".join(f'r."{SQLIdentifierSanitizer.sanitize(c)}"' for c in ocols)
            orig_temp = f"{original}__interp_temp"
            orig_temp_qualified = self._qualified_table(orig_temp, schema)
            await self._exec("DROP TABLE IF EXISTS {}".format(orig_temp_qualified))
            await self._exec(f"""
                CREATE TABLE {orig_temp_qualified} AS
                SELECT {ocols_clause}, c."{safe_new}" AS "{safe_new}"
                FROM (SELECT *, ROW_NUMBER() OVER () AS __idx FROM {original_qualified}) r
                LEFT JOIN {qualified} c ON c.__pos = r.__idx
                ORDER BY r.__idx
            """)
            await self._exec(f"DROP TABLE {original_qualified}")
            await self._exec(f"RENAME TABLE {orig_temp_qualified} TO {original_qualified}")

            null_count = await self._fetchval(
                f'SELECT COUNT(*) FROM {qualified} WHERE "{safe_col}" IS NULL'
            ) or 0

            sample = await self._fetch_data(table, schema, columns=[safe_col, safe_new])
            msg = f"Interpolated {null_count} null values in '{column}' using linear"
            if order_cols:
                msg += f" ordered by {order_cols}"

            return ok(
                msg, [column, *order_cols], [new_col], sample, fill_mode="LINEAR", fill_value="linear",
                new_table=table, order_by=order_cols or None,
            )

        except Exception as e:
            return fail(
                f"numeric_interpolate error: {str(e)}\n{traceback.format_exc()}", [column], [],
            )

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
