"""
ClickHouse selection operations.

Overrides the column-introspection hooks (system.columns) and the iloc
positional JOIN clause (ARRAY JOIN over inline arrays). Like DuckDB, it
serializes date/datetime asof values to ISO strings via the base default.
"""

from memframe.core.analytix.selection.base import DataSelectionOps


class ClickHouseSelectionOps(DataSelectionOps):
    def _list_columns_query(self, qualified, schema, table):
        ph = self.db.placeholder
        return (
            f"SELECT name FROM system.columns "
            f"WHERE database = {ph(1)} AND table = {ph(2)}",
            [schema, table],
        )

    def _list_column_types_query(self, qualified, schema, table):
        ph = self.db.placeholder
        return (
            f"SELECT name, type FROM system.columns "
            f"WHERE database = {ph(1)} AND table = {ph(2)}",
            [schema, table],
        )

    def _column_name(self, row):
        return self._row_get(row, "name", 0)

    def _column_type(self, row):
        return self._row_get(row, "type", 1)

    def _iloc_join_clause(self, row_pos_list, ord_list):
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
