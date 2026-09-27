from __future__ import annotations

from memframe.core.analytix.groupby_window.base import GroupbyWindowOps
from memframe.utils.helper import SQLIdentifierSanitizer


class PostgresGroupbyWindowOps(GroupbyWindowOps):
    """PostgreSQL backend — map-back swaps tables; all else inherited."""


    async def _apply_map(
        self, table, schema, group_table, new_columns, backend, data_id, sig,
    ):
        try:
            orig_cols = list(await self.db.get_column_types(table, schema) or {})
        except Exception:
            orig_cols = []
        if not orig_cols:
            raise ValueError(f"map_feature: could not list columns of {schema}.{table}")
        orig_q = self._qualified_table(table, schema)
        group_q = self._qualified_table(group_table, schema)
        cond = " AND ".join(
            f'o.{self.db.quote_identifier(c)} = g.{self.db.quote_identifier(c)}'
            for c in orig_cols
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

