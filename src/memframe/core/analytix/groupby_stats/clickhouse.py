from __future__ import annotations

from typing import List

from memframe.core.analytix.groupby_stats.base import GroupByStatsOps
from memframe.utils.helper import SQLIdentifierSanitizer


class ClickHouseGroupByStatsOps(GroupByStatsOps):
    """ClickHouse backend.

    Dialect hooks (native ``stddevPop``/``varPop``, ``topK`` mode,
    ``toUnixTimestamp``) plus two structural overrides: MergeTree CTAS
    and the create-swap-``EXCHANGE`` map-back, because asynchronous
    ``UPDATE`` mutations can return stale data.
    """

    _count_fn = "count()"

    def _std_expr(self, qcol: str) -> str:
        return f"stddevPop({qcol})"

    def _var_expr(self, qcol: str) -> str:
        return f"varPop({qcol})"

    def _sem_expr(self, qcol: str) -> str:
        return f"stddevPop({qcol}) / sqrt(count({qcol}))"

    def _product_expr(self, qcol: str) -> str:
        # ClickHouse canonical names; case-insensitive but using
        # camelCase for clarity.
        return f"exp(sum(ln(nullIf({qcol}, 0))))"

    def _median_expr(self, qcol: str) -> str:
        return f"median({qcol})"

    def _mode_expr(self, qcol: str) -> str:
        # ClickHouse does not have a built-in exact MODE() aggregate.
        # topK(N)(col) returns an array of the N most frequent values
        # (Space-Saving algorithm, approximate but very good in practice).
        # [1] extracts the first (most frequent) element (1-based index).
        return f"topK(1)({qcol})[1]"

    def _elapsed_seconds_expr(self, qcol: str) -> str:
        # ClickHouse: use toUnixTimestamp for epoch extraction
        return f"toUnixTimestamp(MAX({qcol})) - toUnixTimestamp(MIN({qcol}))"

    def _create_table_sql(self, output_table: str, schema: str, select_sql: str) -> str:
        output_qualified = self._qualified_table(
            SQLIdentifierSanitizer.sanitize(output_table),
            SQLIdentifierSanitizer.sanitize(schema),
        )
        return f"""
                CREATE TABLE {output_qualified}
                ENGINE = MergeTree()
                ORDER BY tuple()
                AS
                {select_sql}
                """

    async def _apply_map(
        self,
        table: str,
        schema: str,
        group_table: str,
        safe_group_cols: List[str],
        new_columns: List[str],
        backend,
        data_id: str,
        sig: str,
    ) -> None:
        orig_q = self._qualified_table(table, schema)
        group_q = self._qualified_table(group_table, schema)
        cond = " AND ".join(
            f'o.{self.db.quote_identifier(c)} = g.{self.db.quote_identifier(c)}'
            for c in safe_group_cols
        )
        feats = ", ".join(
            f'g.{self.db.quote_identifier(c)} AS {self.db.quote_identifier(c)}'
            for c in new_columns
        )
        tmp = SQLIdentifierSanitizer.sanitize(f"{table}__map_tmp")
        tmp_q = self._qualified_table(tmp, schema)
        await self._exec(f"DROP TABLE IF EXISTS {tmp_q}")
        await self._exec(
            f"""CREATE TABLE {tmp_q}
                ENGINE = MergeTree()
                ORDER BY tuple()
                AS SELECT o.*, {feats}
                FROM {orig_q} o LEFT JOIN {group_q} g ON {cond}"""
        )
        await self._exec(f"EXCHANGE TABLES {orig_q} AND {tmp_q}")
        await self._exec(f"DROP TABLE {tmp_q}")
        await self._record_map_marker(table, schema, backend, data_id, sig)
