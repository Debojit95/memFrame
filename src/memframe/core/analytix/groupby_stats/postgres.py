from __future__ import annotations

from typing import List

from memframe.core.analytix.groupby_stats.base import GroupByStatsOps
from memframe.utils.helper import SQLIdentifierSanitizer


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
            f"""CREATE TABLE {tmp_q} AS
                SELECT o.*, {feats}
                FROM {orig_q} o LEFT JOIN {group_q} g ON {cond}"""
        )
        await self._exec(f"DROP TABLE {orig_q}")
        await self._exec(
            f"ALTER TABLE {tmp_q} RENAME TO {self.db.quote_identifier(SQLIdentifierSanitizer.sanitize(table))}"
        )
        await self._record_map_marker(table, schema, backend, data_id, sig)
