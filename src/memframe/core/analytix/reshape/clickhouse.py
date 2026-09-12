from typing import List

from memframe.core.analytix.reshape.base import ReshapingOps


class ClickHouseReshapingOps(ReshapingOps):
    """ClickHouse backend — system.columns, *If aggregations, stateless transpose."""

    def _safe_numeric_expr(self, column: str) -> str:
        col_q = self._quote_identifier(column)
        # Cast to String first to support both numeric and string columns safely
        return f"toFloat64OrNull(toString({col_q}))"

    async def _get_table_columns(self, table: str, schema: str) -> List[str]:
        rows = await self._fetch(
            f"""
            SELECT name AS column_name
            FROM system.columns
            WHERE table = {self._quote_literal(table)}
              AND database = {self._quote_literal(schema)}
            ORDER BY position
            """
        )
        return [r["column_name"] for r in rows]

    def _build_explode_sql(
        self,
        q: str,
        schema: str,
        new_table: str,
        safe_cols: List[str],
        base_cols: List[str],
    ) -> str:
        array_aliases = []
        for col in safe_cols:
            col_q = self._quote_identifier(col)
            # Use nested replaceAll to safely strip brackets and quotes without regex
            clean_expr = f"replaceAll(replaceAll(replaceAll(replaceAll(CAST(base.{col_q} AS String), '[', ''), ']', ''), '\\'', ''), '\"', '')"

            array_expr = f"""
            CASE
                WHEN base.{col_q} IS NULL THEN emptyArrayString()
                WHEN startsWith(trim(CAST(base.{col_q} AS String)), '[') THEN
                    arrayMap(x -> trim(x), splitByString(',', {clean_expr}))
                ELSE [CAST(base.{col_q} AS String)]
            END
            """
            array_aliases.append(f"{array_expr} AS {col}_arr")

        # To handle multiple columns with different array lengths safely,
        # we generate an index array up to the max length and extract elements by index.
        if len(safe_cols) == 1:
            idx_expr = f"range(1, length({safe_cols[0]}_arr) + 1)"
        else:
            idx_expr = f"range(1, greatest({', '.join([f'length({col}_arr)' for col in safe_cols])}) + 1)"

        array_aliases.append(f"arrayJoin({idx_expr}) AS arr_idx")

        sub_select = []
        if base_cols:
            sub_select.append(", ".join(f"base.{self._quote_identifier(c)}" for c in base_cols))
        sub_select.extend(array_aliases)

        select_cols = []
        if base_cols:
            select_cols.append(", ".join(f"sub.{self._quote_identifier(c)}" for c in base_cols))
        for col in safe_cols:
            # Elements out of bounds will default to '' (empty string), matching DuckDB/Postgres padding
            select_cols.append(f"sub.{col}_arr[sub.arr_idx] AS {self._quote_identifier(col)}")

        return f"""
        CREATE TABLE "{schema}"."{new_table}" AS
        SELECT
            {", ".join(select_cols)}
        FROM (
            SELECT
                {", ".join(sub_select)}
            FROM {q} AS base
        ) AS sub
        """

    def _count_all_expr(self, conditions_str: str) -> str:
        return f"countIf({conditions_str})"

    def _filtered_agg_expr(self, func: str, value_expr: str, conditions_str: str) -> str:
        ch_func_map = {"SUM": "sumIf", "AVG": "avgIf", "MIN": "minIf", "MAX": "maxIf"}
        ch_func = ch_func_map.get(func, "sumIf")
        return f"{ch_func}({value_expr}, {conditions_str})"

    def _normalize_ratio_expr(self, filtered_expr: str, denominator: str) -> str:
        return f"toFloat64({filtered_expr}) / NULLIF({denominator}, 0)"

    async def _transpose_unpivot_source(self, q, schema, table, cols):
        # ClickHouse HTTP is stateless, so TEMPORARY tables don't persist across requests.
        # We use a single query with arrayJoin to avoid intermediate tables.

        # 1. Build arrays of column names and Nullable(String) casted values
        col_names_arr = "[" + ", ".join(f"'{c}'" for c in cols) + "]"
        col_vals_arr = "[" + ", ".join(f'CAST({self._quote_identifier(c)} AS Nullable(String))' for c in cols) + "]"

        # 2. Build the unpivot subquery
        unpivot_sql = f"""
        SELECT
            tupleElement(zipped, 1) AS column_name,
            __rowid__,
            tupleElement(zipped, 2) AS value
        FROM (
            SELECT
                __rowid__,
                arrayJoin(arrayZip({col_names_arr}, {col_vals_arr})) AS zipped
            FROM (
                SELECT
                    ROW_NUMBER() OVER () AS __rowid__,
                    {", ".join(self._quote_identifier(c) for c in cols)}
                FROM {q}
            )
        )
        """

        # 3. Fetch distinct row ids to build the pivot
        row_ids = await self._fetch(f"SELECT DISTINCT __rowid__ FROM ({unpivot_sql}) ORDER BY __rowid__")
        return row_ids, f"({unpivot_sql})"

    def _pct_rank_expr(self, base_rank: str, partition_clause=None) -> str:
        if partition_clause:
            return f"toFloat64(({base_rank})) / NULLIF(COUNT(*) OVER (PARTITION BY {partition_clause}), 0)"
        return f"toFloat64(({base_rank})) / NULLIF(COUNT(*) OVER (), 0)"
