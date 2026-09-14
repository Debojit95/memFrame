from memframe.core.analytix.merging.base import DataMergeOps


class ClickHouseMergeOps(DataMergeOps):
    """ClickHouse backend — MergeTree output and DateTime-aware key auto-cast."""

    def _auto_cast_join_columns(self, l_col: str, r_col: str, l_type, r_type):
        # ClickHouse uses DateTime/DateTime64 for timestamps, Date/Date32 for
        # dates. "DATE" inside "DATETIME" must not count as a date-only type.
        l_type_upper = str(l_type).upper()
        r_type_upper = str(r_type).upper()

        l_is_timestamp = ("TIMESTAMP" in l_type_upper or "DATETIME" in l_type_upper)
        r_is_timestamp = ("TIMESTAMP" in r_type_upper or "DATETIME" in r_type_upper)

        l_is_date_only = ("DATE" in l_type_upper and not l_is_timestamp)
        r_is_date_only = ("DATE" in r_type_upper and not r_is_timestamp)

        if l_is_timestamp and r_is_date_only:
            l_col = f"CAST({l_col} AS Date)"
        elif l_is_date_only and r_is_timestamp:
            r_col = f"CAST({r_col} AS Date)"

        return l_col, r_col

    def _create_table_as(self, schema: str, new_table: str, select_sql: str) -> str:
        return (
            f"CREATE TABLE {self.db.quote_identifier(schema)}.{self.db.quote_identifier(new_table)}\n"
            f"ENGINE = MergeTree()\nORDER BY tuple()\nAS\n"
            f"{select_sql}"
        )
