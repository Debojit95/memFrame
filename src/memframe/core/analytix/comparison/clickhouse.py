from typing import Any, Dict, Optional
import traceback

from memframe.core.analytix.comparison.base import ComparisonOps
from memframe.utils.helper import SQLIdentifierSanitizer


class ClickHouseComparisonOps(ComparisonOps):
    """ClickHouse backend.

    Dialect hooks (MergeTree engine clause, DateTime64 casts) plus one
    structural override: single CTAS with the computed column instead of
    clone → add → update, because UPDATE is an async mutation.
    """

    def _engine_clause(self) -> str:
        return "ENGINE = MergeTree() ORDER BY tuple() "

    def _datetime_cast(self) -> str:
        # ClickHouse: DateTime64 with microsecond precision + Nullable
        return "Nullable(DateTime64(6))"

    async def _compare_columns(
        self,
        table: str,
        schema: str,
        col1: str,
        col2: str,
        operator: str,
        col1_expr: Optional[str] = None,
        col2_expr: Optional[str] = None,
        backend=None,
        data_id: Optional[str] = None,
        new_table: Optional[str] = None,
    ) -> Dict[str, Any]:
        try:
            safe_col1 = SQLIdentifierSanitizer.sanitize(col1)
            safe_col2 = SQLIdentifierSanitizer.sanitize(col2)

            left_expr = col1_expr if col1_expr else self.db.quote_identifier(safe_col1)
            right_expr = col2_expr if col2_expr else self.db.quote_identifier(safe_col2)
            sql_operator = self._sql_operator(operator)

            new_col = self._generate_cleaned_column_name(col1, operator, col2)
            safe_new = SQLIdentifierSanitizer.sanitize(new_col)

            # ──────────────────────────────────────────────────────────
            # ClickHouse: Single CTAS with computed column
            # Avoids ALTER+UPDATE because UPDATE is an async mutation
            # ──────────────────────────────────────────────────────────
            qualified_source = self._qualified_table(table, schema)
            select_query = f"""
                SELECT *, ({left_expr} {sql_operator} {right_expr}) AS {self.db.quote_identifier(safe_new)}
                FROM {qualified_source}
            """
            working_table = await self._materialize_query_as_table(
                select_query, table, schema,
                backend=backend, data_id=data_id, new_table=new_table
            )

            # Sample result from the working table
            sample = await self._fetch_data(
                working_table, schema, columns=[safe_col1, safe_col2, safe_new]
            )

            return self._success_response(
                f"Compared {col1} {operator} {col2} → {new_col}",
                sample,
                col1=col1,
                col2=col2,
                operator=operator,
                new_column=new_col,
                new_table=working_table,
            )

        except Exception as e:
            return self._error_response(
                f"compare error: {str(e)}\n{traceback.format_exc()}"
            )
