from __future__ import annotations

from typing import Any

import pandas as pd

from src.db_manager.adapters.base import DatabaseAdapter
from src.utils.helper import SQLIdentifierSanitizer

try:
    import plotly.express as px
except ImportError:
    px = None


class LinePlotCore:
    """
    Core line plotting engine.

    Fetches data from the active table and delegates to plotly.express.line.
    """

    def __init__(self, db_adapter: DatabaseAdapter):
        self.db = db_adapter

    def _qualified_table(self, table: str, schema: str) -> str:
        safe_table = SQLIdentifierSanitizer.sanitize(table, allow_qualified=False)
        safe_schema = SQLIdentifierSanitizer.sanitize(schema, allow_qualified=False)
        return f"{self.db.quote_identifier(safe_schema)}.{self.db.quote_identifier(safe_table)}"

    def _ensure_plotly(self) -> None:
        if px is None:
            raise ImportError(
                "Plotly is required for plotting. Install it with: pip install plotly"
            )

    def _collect_column_value(self, value: Any, out: set[str]) -> None:
        if isinstance(value, str) and value:
            out.add(SQLIdentifierSanitizer.sanitize(value, allow_qualified=False))
            return
        if isinstance(value, (list, tuple, set)):
            for item in value:
                if isinstance(item, str) and item:
                    out.add(SQLIdentifierSanitizer.sanitize(item, allow_qualified=False))

    def _extract_requested_columns(
        self,
        x: Any = None,
        y: Any = None,
        line_group: Any = None,
        color: Any = None,
        line_dash: Any = None,
        symbol: Any = None,
        hover_name: Any = None,
        hover_data: Any = None,
        text: Any = None,
        facet_row: Any = None,
        facet_col: Any = None,
        error_x: Any = None,
        error_x_minus: Any = None,
        error_y: Any = None,
        error_y_minus: Any = None,
        animation_frame: Any = None,
        animation_group: Any = None,
        custom_data: Any = None,
    ) -> list[str] | None:
        requested: set[str] = set()
        for value in (
            x,
            y,
            line_group,
            color,
            line_dash,
            symbol,
            hover_name,
            text,
            facet_row,
            facet_col,
            error_x,
            error_x_minus,
            error_y,
            error_y_minus,
            animation_frame,
            animation_group,
            custom_data,
        ):
            self._collect_column_value(value, requested)

        if isinstance(hover_data, dict):
            self._collect_column_value(list(hover_data.keys()), requested)
        else:
            self._collect_column_value(hover_data, requested)

        return sorted(requested) if requested else None

    async def _fetch_dataframe(
        self,
        table: str,
        schema: str,
        columns: list[str] | None = None,
    ) -> pd.DataFrame:
        qualified = self._qualified_table(table, schema)
        column_types = await self.db.get_column_types(table, schema)
        available_columns = list(column_types.keys())

        if columns is None:
            selected = available_columns
        else:
            selected = [col for col in columns if col in column_types]
            if not selected:
                selected = available_columns

        if selected:
            quoted_cols = [
                self.db.quote_identifier(
                    SQLIdentifierSanitizer.sanitize(col, allow_qualified=False)
                )
                for col in selected
            ]
            column_clause = ", ".join(quoted_cols)
        else:
            column_clause = "*"

        rows = await self.db.fetch(f"SELECT {column_clause} FROM {qualified}")
        records = [dict(row) for row in rows]
        if not records and selected:
            return pd.DataFrame(columns=selected)
        return pd.DataFrame.from_records(records)

    async def line(
        self,
        table: str,
        schema: str,
        *,
        x: Any = None,
        y: Any = None,
        line_group: Any = None,
        color: Any = None,
        line_dash: Any = None,
        symbol: Any = None,
        hover_name: Any = None,
        hover_data: Any = None,
        text: Any = None,
        facet_row: Any = None,
        facet_col: Any = None,
        error_x: Any = None,
        error_x_minus: Any = None,
        error_y: Any = None,
        error_y_minus: Any = None,
        animation_frame: Any = None,
        animation_group: Any = None,
        custom_data: Any = None,
        **kwargs: Any,
    ):
        """
        Build a line plot using plotly.express.line.

        Parameters match px.line except data_frame, which is provided automatically.
        """
        self._ensure_plotly()

        if "data_frame" in kwargs:
            raise ValueError(
                "Do not pass 'data_frame' to ops.line(); it is derived from the active context."
            )

        requested_columns = self._extract_requested_columns(
            x=x,
            y=y,
            line_group=line_group,
            color=color,
            line_dash=line_dash,
            symbol=symbol,
            hover_name=hover_name,
            hover_data=hover_data,
            text=text,
            facet_row=facet_row,
            facet_col=facet_col,
            error_x=error_x,
            error_x_minus=error_x_minus,
            error_y=error_y,
            error_y_minus=error_y_minus,
            animation_frame=animation_frame,
            animation_group=animation_group,
            custom_data=custom_data,
        )
        df = await self._fetch_dataframe(table=table, schema=schema, columns=requested_columns)
        return px.line(
            df,
            x=x,
            y=y,
            line_group=line_group,
            color=color,
            line_dash=line_dash,
            symbol=symbol,
            hover_name=hover_name,
            hover_data=hover_data,
            text=text,
            facet_row=facet_row,
            facet_col=facet_col,
            error_x=error_x,
            error_x_minus=error_x_minus,
            error_y=error_y,
            error_y_minus=error_y_minus,
            animation_frame=animation_frame,
            animation_group=animation_group,
            custom_data=custom_data,
            **kwargs,
        )


__all__ = ["LinePlotCore"]
