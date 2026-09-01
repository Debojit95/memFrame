from typing import Any, List, Tuple

from memframe.core.analytix.selection.base import DataSelectionOps


class PostgresSelectionOps(DataSelectionOps):
    """PostgreSQL backend — dialect hooks only; all operations inherited."""

    def _list_columns_query(self, qualified: str, schema: str, table: str) -> Tuple[str, list]:
        return (
            "SELECT column_name FROM information_schema.columns "
            "WHERE table_schema = $1 AND table_name = $2",
            [schema, table],
        )

    def _list_column_types_query(self, qualified: str, schema: str, table: str) -> Tuple[str, list]:
        return (
            "SELECT column_name, data_type FROM information_schema.columns "
            "WHERE table_schema = $1 AND table_name = $2",
            [schema, table],
        )

    def _column_name(self, row) -> str:
        return self._row_get(row, "column_name", 0)

    def _column_type(self, row) -> str:
        return self._row_get(row, "data_type", 1)

    def _serialize_asof_where(self, normalized: Any) -> Any:
        return normalized

    def _iloc_join_clause(self, row_pos_list: List[int], ord_list: List[int]) -> str:
        idx_arr = "ARRAY[" + ", ".join(map(str, row_pos_list)) + "]::int[]"
        ord_arr = "ARRAY[" + ", ".join(map(str, ord_list)) + "]::int[]"
        return f"""
        JOIN (
            SELECT * FROM UNNEST({idx_arr}, {ord_arr}) AS v(idx, ord)
        ) v ON t._rn = v.idx
        ORDER BY v.ord
        """
