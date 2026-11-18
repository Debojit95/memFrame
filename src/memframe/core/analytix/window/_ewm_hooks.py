"""Shared EWM backend hooks for ungrouped and grouped window engines."""

from typing import List

from memframe.core.analytix._generated import swap_table_into_source
from memframe.utils.helper import SQLIdentifierSanitizer


class PostgresEWMHooks:
    """PostgreSQL-specific EWM hooks."""

    def _is_missing_sql(self, expr: str) -> str:
        f = self._ewm_cast_float(expr)
        return f"({expr} IS NULL OR {f} = CAST('NaN' AS FLOAT8))"


class ClickHouseEWMHooks:
    """ClickHouse-specific EWM hooks.

    ClickHouse cannot run synchronous UPDATE mutations, so the feature table
    is joined to the source and exchanged atomically.
    """

    @staticmethod
    def _ewm_value(value) -> str:
        return f"CAST({float(value)!r} AS Float64)"

    def _ewm_cast_float(self, expr: str) -> str:
        return f"CAST({expr} AS Float64)"

    def _ewm_float_type(self) -> str:
        return "Float64"

    def _ewm_int_type(self) -> str:
        return "Int64"

    def _ewm_uses_transaction(self) -> bool:
        return False

    def _is_missing_sql(self, expr: str) -> str:
        return f"({expr} IS NULL OR isNaN({self._ewm_cast_float(expr)}))"

    def _ewm_create_state(self, qualified: str, select_sql: str) -> str:
        return (
            f"CREATE TABLE {qualified} ENGINE = MergeTree() ORDER BY tuple() "
            f"AS {select_sql}"
        )

    def _pop_std_agg(self) -> str:
        return "stddevPop"

    def _count_expr(self, column_expr: str) -> str:
        # ClickHouse rewrites count(nullable) into a missing virtual column
        # inside a window CTAS; sum(if(isNull(...))) is equivalent and stable.
        return f"sum(if(isNull({column_expr}), 0, 1))"

    async def _merge_features(
        self,
        table: str,
        schema: str,
        select_sql: str,
        columns: List[str],
        column_types: List[str],
        backend=None,
        data_id=None,
    ) -> None:
        base = await self._resolve_output_table_name(
            table, schema, backend=backend, data_id=data_id
        )
        feat = SQLIdentifierSanitizer.sanitize(f"{base}__feat")
        wide = SQLIdentifierSanitizer.sanitize(f"{base}__wide")
        q_src = self._qualified_table(table, schema)
        q_feat = self._qualified_table(feat, schema)
        q_wide = self._qualified_table(wide, schema)
        feats = ", ".join(
            f"s.{self.db.quote_identifier(c)} AS {self.db.quote_identifier(c)}"
            for c in columns
        )
        rid = self.db.quote_identifier(self._row_id_col)
        try:
            await self._exec(
                f"CREATE TABLE {q_feat} ENGINE = MergeTree() ORDER BY tuple() AS "
                f"{select_sql}"
            )
            await self._exec(
                f"CREATE TABLE {q_wide} ENGINE = MergeTree() ORDER BY tuple() AS "
                f"SELECT o.*, {feats} FROM {q_src} AS o "
                f"LEFT JOIN {q_feat} AS s ON o.{rid} = s.{rid}"
            )
            await swap_table_into_source(
                self.db, backend, schema, table, wide, generated=columns
            )
        finally:
            await self._exec(f"DROP TABLE IF EXISTS {q_feat}")
            await self._exec(f"DROP TABLE IF EXISTS {q_wide}")
