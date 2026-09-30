from memframe.core.analytix.filter.filter_II.base import DataFilteringOps


class ClickHouseFilteringOps(DataFilteringOps):
    """ClickHouse backend — MergeTree engine clause for CTAS."""

    def _engine_clause(self) -> str:
        # ClickHouse requires ENGINE + ORDER BY for MergeTree
        return "ENGINE = MergeTree() ORDER BY tuple()"

    def _flag_column_type(self) -> str:
        return "Bool"

    async def _fill_flag_column(
        self, qualified: str, flag_col: str, where_clause: str, params: list
    ) -> None:
        # ponytail: UPDATE is an async MergeTree mutation — ALTER UPDATE
        # syntax plus a wait (below) instead of plain UPDATE.
        await self._exec(
            f"ALTER TABLE {qualified} UPDATE "
            f"{self.db.quote_identifier(flag_col)} = "
            f"COALESCE(({where_clause}), FALSE)",
            *params,
        )

    async def _after_source_mutation(self, table: str, schema: str) -> None:
        # ponytail: poll system.mutations until the flag UPDATE is visible.
        # Row-shape agnostic (adapters disagree on dicts vs tuples).
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
