"""
PostgreSQL selection operations.

Overrides the column-introspection hooks (information_schema), the asof value
serialization (Postgres accepts native date/datetime values, so they pass
through unchanged), and the iloc positional JOIN clause (UNNEST(...) AS v(...)).
"""

from memframe.core.analytix.selection.base import DataSelectionOps


class PostgresSelectionOps(DataSelectionOps):
    def _list_columns_query(self, qualified, schema, table):
        return (
            "SELECT column_name FROM information_schema.columns "
            "WHERE table_schema = $1 AND table_name = $2",
            [schema, table],
        )

    def _list_column_types_query(self, qualified, schema, table):
        return (
            "SELECT column_name, data_type FROM information_schema.columns "
            "WHERE table_schema = $1 AND table_name = $2",
            [schema, table],
        )

    def _column_name(self, row):
        return self._row_get(row, "column_name", 0)

    def _column_type(self, row):
        return self._row_get(row, "data_type", 1)

    def _serialize_asof_where(self, normalized):
        return normalized

    def _iloc_join_clause(self, row_pos_list, ord_list):
        idx_arr = "ARRAY[" + ", ".join(map(str, row_pos_list)) + "]::int[]"
        ord_arr = "ARRAY[" + ", ".join(map(str, ord_list)) + "]::int[]"
        return f"""
        JOIN (
            SELECT * FROM UNNEST({idx_arr}, {ord_arr}) AS v(idx, ord)
        ) v ON t._rn = v.idx
        ORDER BY v.ord
        """
