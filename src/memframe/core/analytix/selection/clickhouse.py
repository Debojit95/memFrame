from typing import Any, List, Tuple

from memframe.core.analytix.selection.base import DataSelectionOps


class ClickHouseSelectionOps(DataSelectionOps):
    """ClickHouse backend — dialect hooks only; all operations inherited."""

    def _list_columns_query(self, qualified: str, schema: str, table: str) -> Tuple[str, list]:
        ph = self.db.placeholder
        return (
            f"SELECT name FROM system.columns "
            f"WHERE database = {ph(1)} AND table = {ph(2)}",
            [schema, table],
        )

    def _list_column_types_query(self, qualified: str, schema: str, table: str) -> Tuple[str, list]:
        ph = self.db.placeholder
        return (
            f"SELECT name, type FROM system.columns "
            f"WHERE database = {ph(1)} AND table = {ph(2)}",
            [schema, table],
        )

    def _column_name(self, row) -> str:
        return self._row_get(row, "name", 0)

    def _column_type(self, row) -> str:
        return self._row_get(row, "type", 1)

    def _serialize_asof_where(self, normalized: Any) -> Any:
        return normalized

    def _iloc_join_clause(self, row_pos_list: List[int], ord_list: List[int]) -> str:
        idx_arr = "[" + ", ".join(map(str, row_pos_list)) + "]"
        ord_arr = "[" + ", ".join(map(str, ord_list)) + "]"
        return f"""
        JOIN (
            SELECT idx, ord
            FROM (SELECT {idx_arr} AS idx_arr, {ord_arr} AS ord_arr)
            ARRAY JOIN idx_arr AS idx, ord_arr AS ord
        ) v ON t._rn = v.idx
        ORDER BY v.ord
        """
