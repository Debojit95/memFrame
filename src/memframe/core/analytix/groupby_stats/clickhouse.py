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

    async def _after_drop_columns(self, table: str, schema: str) -> None:
        # ponytail: DROP COLUMN is an async MergeTree mutation; poll until
        # none are pending so the rebuild sees the drop. Row-shape
        # agnostic (adapters disagree on dicts vs tuples).
        import asyncio

        qualified = self._qualified_table(table, schema)
        parts = qualified.replace("`", "").replace('"', "").split(".")
        table_name_only = parts[-1] if len(parts) > 1 else parts[0]
        database = parts[0] if len(parts) > 1 else "currentDatabase()"
        for _ in range(120):
            rows = await self._fetch(
                "SELECT count() AS pending FROM system.mutations "
                f"WHERE database = '{database}' "
                f"AND table = '{table_name_only}' "
                "AND is_done = 0"
            )
            if not rows:
                return
            first = rows[0]
            pending = first.get("pending") if isinstance(first, dict) else first[0]
            if not pending:
                return
            await asyncio.sleep(1)

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
        source_types = await self.db.get_column_types(table, schema) or {}
        if any(
            "Nullable" in str(source_types.get(c, ""))
            for c in safe_group_cols
        ):
            # Join engine keys cannot be Nullable; keep LEFT JOIN semantics.
            await self._apply_map_swap(
                table, schema, group_table,
                safe_group_cols, new_columns,
            )
        else:
            await self._apply_map_join(
                table, schema, group_table,
                safe_group_cols, new_columns, backend,
            )
        await self._record_map_marker(table, schema, backend, data_id, sig)

    async def _apply_map_join(
        self,
        table: str,
        schema: str,
        group_table: str,
        safe_group_cols: List[str],
        new_columns: List[str],
        backend,
    ) -> None:
        quote = self.db.quote_identifier
        orig_q = self._qualified_table(table, schema)
        group_q = self._qualified_table(group_table, schema)
        group_types = await self.db.get_column_types(group_table, schema) or {}
        cache_schema = getattr(backend, "cache_schema", schema)
        map_table = SQLIdentifierSanitizer.sanitize(f"{table}__map_join")
        map_q = self._qualified_table(map_table, cache_schema)
        map_ref = f"{cache_schema}.{map_table}".replace("'", "''")
        keys = ", ".join(quote(c) for c in safe_group_cols)
        key_defs = ", ".join(
            f"{quote(c)} {group_types.get(c) or 'String'}"
            for c in safe_group_cols
        )
        feat_defs = ", ".join(
            f"{quote(c)} {group_types.get(c) or 'Float64'}"
            for c in new_columns
        )
        await self._exec(f"DROP TABLE IF EXISTS {map_q}")
        try:
            await self._exec(
                f"CREATE TABLE {map_q} ({key_defs}, {feat_defs}) "
                f"ENGINE = Join(ANY, LEFT, {keys})"
            )
            await self._exec(
                f"INSERT INTO {map_q} SELECT "
                f"{', '.join(quote(c) for c in safe_group_cols + new_columns)} "
                f"FROM {group_q}"
            )
            for col in new_columns:
                await self._exec(
                    f"ALTER TABLE {orig_q} ADD COLUMN {quote(col)} "
                    f"{group_types.get(col) or 'Float64'}"
                )
            assignments = ", ".join(
                f"{quote(c)} = joinGet('{map_ref}', "
                f"'{c.replace(chr(39), chr(39) * 2)}', {keys})"
                for c in new_columns
            )
            await self._exec(
                f"ALTER TABLE {orig_q} UPDATE {assignments} "
                f"WHERE 1 SETTINGS mutations_sync = 1"
            )
        finally:
            await self._exec(f"DROP TABLE IF EXISTS {map_q}")

    async def _apply_map_swap(
        self,
        table: str,
        schema: str,
        group_table: str,
        safe_group_cols: List[str],
        new_columns: List[str],
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
