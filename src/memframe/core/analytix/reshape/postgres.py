from typing import List

from memframe.core.analytix.reshape.base import ReshapingOps


class PostgresReshapingOps(ReshapingOps):
    """PostgreSQL backend — numeric-cast guard + LATERAL explode; rest inherited."""

    def _safe_numeric_expr(self, column: str) -> str:
        col_q = self._quote_identifier(column)
        numeric_pattern = r"^[+-]?([0-9]+([.][0-9]*)?|[.][0-9]+)([eE][+-]?[0-9]+)?$"
        return (
            f"CASE WHEN trim({col_q}::text) ~ {self._quote_literal(numeric_pattern)} "
            f"THEN {col_q}::DOUBLE PRECISION ELSE NULL END"
        )

    def _build_explode_sql(
        self,
        q: str,
        schema: str,
        new_table: str,
        safe_cols: List[str],
        base_cols: List[str],
    ) -> str:
        lateral_parts = []
        exploded_select_cols = []

        base_select = ", ".join(
            f'base.{self._quote_identifier(c)}' for c in base_cols
        )

        for col in safe_cols:
            col_q = self._quote_identifier(col)
            part = f"""
            unnest(
                CASE
                    WHEN base.{col_q} IS NULL THEN ARRAY[]::text[]
                    WHEN base.{col_q}::text LIKE '[%' THEN
                        string_to_array(
                            replace(
                                replace(
                                    replace(
                                        replace(base.{col_q}::text, '[', ''),
                                    ']', ''),
                                '''', ''),
                            '"', ''),
                            ','
                        )
                    ELSE ARRAY[base.{col_q}::text]
                END
            ) AS {col_q}
            """
            lateral_parts.append(part)
            exploded_select_cols.append(f"exploded.{col_q}")

        lateral_sql = ",\n".join(lateral_parts)
        select_sql = ", ".join(
            [part for part in (base_select, ", ".join(exploded_select_cols)) if part]
        )

        return f"""
        CREATE TABLE "{schema}"."{new_table}" AS
        SELECT
            {select_sql}
        FROM {q} AS base,
        LATERAL (
            SELECT {lateral_sql}
        ) AS exploded
        """
