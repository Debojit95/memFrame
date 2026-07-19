from __future__ import annotations

from typing import Any

import pandas as pd

from db_manager.adapters.base import DatabaseAdapter
from utils.helper import SQLIdentifierSanitizer

try:
    import plotly.express as px
except ImportError:
    px = None


class PiePlotCore:
    """Core pie chart engine – fetches data and delegates to plotly.express.pie."""

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
        names: Any = None,
        values: Any = None,
        color: Any = None,
        facet_row: Any = None,
        facet_col: Any = None,
        hover_name: Any = None,
        hover_data: Any = None,
        custom_data: Any = None,
        **_,
    ) -> list[str] | None:
        requested: set[str] = set()
        for value in (names, values, color, facet_row, facet_col, hover_name, custom_data):
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

    async def pie(
        self,
        table: str,
        schema: str,
        *,
        names: Any = None,
        values: Any = None,
        color: Any = None,
        facet_row: Any = None,
        facet_col: Any = None,
        facet_col_wrap: int = 0,
        facet_row_spacing: float | None = None,
        facet_col_spacing: float | None = None,
        color_discrete_sequence: Any = None,
        color_discrete_map: Any = None,
        hover_name: Any = None,
        hover_data: Any = None,
        custom_data: Any = None,
        category_orders: Any = None,
        labels: Any = None,
        title: str | None = None,
        subtitle: str | None = None,
        template: str | None = None,
        width: int | None = None,
        height: int | None = None,
        opacity: float | None = None,
        hole: float | None = None,
        **kwargs: Any,
    ) -> Any:
        """Build a pie chart using plotly.express.pie."""
        self._ensure_plotly()

        if "data_frame" in kwargs:
            raise ValueError(
                "Do not pass 'data_frame' to ops.pie(); it is derived from the active context."
            )

        requested_columns = self._extract_requested_columns(
            names=names,
            values=values,
            color=color,
            facet_row=facet_row,
            facet_col=facet_col,
            hover_name=hover_name,
            hover_data=hover_data,
            custom_data=custom_data,
        )
        df = await self._fetch_dataframe(table=table, schema=schema, columns=requested_columns)

        return px.pie(
            df,
            names=names,
            values=values,
            color=color,
            facet_row=facet_row,
            facet_col=facet_col,
            facet_col_wrap=facet_col_wrap,
            facet_row_spacing=facet_row_spacing,
            facet_col_spacing=facet_col_spacing,
            color_discrete_sequence=color_discrete_sequence,
            color_discrete_map=color_discrete_map,
            hover_name=hover_name,
            hover_data=hover_data,
            custom_data=custom_data,
            category_orders=category_orders,
            labels=labels,
            title=title,
            subtitle=subtitle,
            template=template,
            width=width,
            height=height,
            opacity=opacity,
            hole=hole,
            **kwargs,
        )


__all__ = ["PiePlotCore"]