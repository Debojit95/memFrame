from memframe.core.analytix.sorting.base import DataSortingOps


class ClickHouseSortingOps(DataSortingOps):
    """ClickHouse backend — IS NULL sentinel ordering + MergeTree CREATE."""

    def _order_term(self, quoted_col: str, direction: str, na_position: str) -> str:
        """
        Build a single ORDER BY term for ClickHouse.

        ClickHouse does NOT support NULLS FIRST / NULLS LAST.
        Its defaults are:  ASC → NULLs last,  DESC → NULLs first
        (identical to PostgreSQL / DuckDB defaults).

        So we only need an IS NULL sentinel when the user explicitly
        requests the **non-default** position:

          na_position="first" + ASC  →  non-default  →  need sentinel
          na_position="last"  + DESC →  non-default  →  need sentinel
          otherwise                    →  default      →  plain column

        Sentinel logic:
          (col IS NULL) DESC  → 1 (NULL) first, 0 (non-NULL) after
          (col IS NULL) ASC   → 0 (non-NULL) first, 1 (NULL) after
        """
        is_non_default = (
            (na_position == "first" and direction == "ASC") or
            (na_position == "last" and direction == "DESC")
        )

        if is_non_default:
            if na_position == "first":
                null_sort = f"({quoted_col} IS NULL) DESC"
            else:
                null_sort = f"({quoted_col} IS NULL) ASC"
            return f"{null_sort}, {quoted_col} {direction}"
        else:
            return f"{quoted_col} {direction}"

    def _create_sort_table_sql(
        self,
        qualified_new: str,
        select_clause: str,
        qualified: str,
        order_sql: str,
    ) -> str:
        # ClickHouse requires ENGINE + ORDER BY for MergeTree
        return f"""
            CREATE TABLE {qualified_new}
            ENGINE = MergeTree()
            ORDER BY tuple()
            AS SELECT {select_clause}
            FROM {qualified}
            ORDER BY {order_sql}
        """
