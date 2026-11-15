from __future__ import annotations

from typing import List

from memframe.core.analytix.groupby_stats.base import GroupByStatsOps


class PostgresGroupByStatsOps(GroupByStatsOps):
    """PostgreSQL backend — percentile median and epoch extraction; all else inherited."""

    def _median_expr(self, qcol: str) -> str:
        return f"PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY {qcol})"

    def _mode_expr(self, qcol: str) -> str:
        return f"MODE() WITHIN GROUP (ORDER BY {qcol})"

    def _elapsed_seconds_expr(self, qcol: str) -> str:
        return f"EXTRACT(EPOCH FROM (MAX({qcol}) - MIN({qcol})))"

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
        quote = self.db.quote_identifier
        orig_q = self._qualified_table(table, schema)
        group_q = self._qualified_table(group_table, schema)
        cond = " AND ".join(
            f"{orig_q}.{quote(c)} = g.{quote(c)}" for c in safe_group_cols
        )
        types = await self.db.get_column_types(group_table, schema) or {}
        for col in new_columns:
            col_type = types.get(col)
            if not col_type:
                raise ValueError(
                    f"Cannot resolve type for mapped column '{col}'"
                )
            await self._exec(
                f"ALTER TABLE {orig_q} ADD COLUMN {quote(col)} {col_type}"
            )
        assignments = ", ".join(
            f"{quote(c)} = g.{quote(c)}" for c in new_columns
        )
        await self._exec(
            f"UPDATE {orig_q} SET {assignments} FROM {group_q} g WHERE {cond}"
        )
        await self._record_map_marker(table, schema, backend, data_id, sig)
