"""Zero-dependency HTML renderer for a DashboardDesign.

Produces a single self-contained HTML page (Plotly figures inline, tables as
HTML, metrics as styled cards) using only ``plotly`` (already a core dep).
No Dash / web server required.
"""

from __future__ import annotations

from html import escape
from typing import Any, Dict, List

import plotly.express as px
from plotly.graph_objects import Figure

from memframe.dashboard.models import (
    ChartType,
    DashboardDesign,
    FigureDesign,
    MetricDesign,
    TableDesign,
    WidgetDesign,
)


def _pack_rows(widgets: List[WidgetDesign]) -> List[List[WidgetDesign]]:
    """Greedily group widgets into rows: at most 2 widgets and span<=12 per row."""
    ordered = sorted(widgets, key=lambda w: w.result_index)
    rows: List[List[WidgetDesign]] = []
    current: List[WidgetDesign] = []
    span = 0
    for w in ordered:
        if current and (len(current) >= 2 or span + w.col_span > 12):
            rows.append(current)
            current = []
            span = 0
        current.append(w)
        span += w.col_span
    if current:
        rows.append(current)
    return rows


def _apply_layout(fig: Figure, d: FigureDesign) -> None:
    fig.update_layout(
        title=d.title or None,
        width=d.width,
        height=d.height,
        xaxis_title=d.xaxis_title,
        yaxis_title=d.yaxis_title,
        legend_title=d.legend_title,
        showlegend=d.show_legend,
    )
    try:
        scale = getattr(px.colors.sequential, d.color_scale, None)
        if scale is None:
            scale = getattr(px.colors.qualitative, d.color_scale, None)
        if scale:
            fig.update_layout(colorway=scale)
    except Exception:
        pass


def _build_figure(df: Any, d: FigureDesign) -> Figure:
    cols = list(df.columns)
    x = d.x or (cols[0] if cols else None)
    y = d.y or (cols[1] if len(cols) > 1 else None)
    ct = d.chart_type
    if ct == ChartType.BAR:
        fig = px.bar(df, x=x, y=y)
    elif ct == ChartType.LINE:
        fig = px.line(df, x=x, y=y)
    elif ct == ChartType.SCATTER:
        fig = px.scatter(df, x=x, y=y)
    elif ct == ChartType.PIE:
        fig = px.pie(df, names=x, values=y)
    elif ct == ChartType.HISTOGRAM:
        fig = px.histogram(df, x=(x or (cols[0] if cols else None)))
    elif ct == ChartType.BOX:
        fig = px.box(df, x=x, y=y)
    else:
        fig = px.bar(df, x=x, y=y)
    return fig


def _fmt(val: Any, md: MetricDesign) -> str:
    if isinstance(val, (int, float)):
        try:
            s = format(val, f".{md.decimal_places}f")
        except Exception:
            s = str(val)
        return f"{md.prefix or ''}{s}{md.suffix or ''}"
    return f"{md.prefix or ''}{val}{md.suffix or ''}"


def _render_metric(result: Any, widget: WidgetDesign) -> str:
    md = widget.metric_design or MetricDesign()
    title = escape(widget.title or "")
    if isinstance(result, dict) and len(result) == 1:
        body = f"<div class='memframe-card-value' style='font-size:{md.font_size}px'>{_fmt(next(iter(result.values())), md)}</div>"
    elif isinstance(result, dict):
        body = "".join(
            f"<div class='memframe-metric-row'><span>{escape(str(k))}</span><b>{_fmt(v, md)}</b></div>"
            for k, v in result.items()
        )
    else:
        body = f"<div class='memframe-card-value' style='font-size:{md.font_size}px'>{_fmt(result, md)}</div>"
    return (
        f"<div class='memframe-cell'>"
        f"<div class='memframe-card memframe-{md.color}'>"
        f"<div class='memframe-card-title'>{title}</div>"
        f"<div class='memframe-card-body'>{body}</div>"
        f"</div></div>"
    )


def _render_cell(widget: WidgetDesign, result: Any, plotly_included: List[bool]) -> str:
    if widget.kind == "plot":
        if isinstance(result, Figure):
            fig = result
            if widget.plot_design:
                _apply_layout(fig, widget.plot_design)
        elif hasattr(result, "to_html") and hasattr(result, "to_dict"):
            fig = _build_figure(result, widget.plot_design or FigureDesign())
            if widget.plot_design:
                _apply_layout(fig, widget.plot_design)
        else:
            return "<div class='memframe-cell'><em>Unsupported plot input</em></div>"
        inc = not plotly_included[0]
        if inc:
            plotly_included[0] = True
        html = fig.to_html(include_plotlyjs=inc, full_html=False, config={"responsive": True})
        return f"<div class='memframe-cell'>{html}</div>"

    if widget.kind == "table":
        df = result
        max_rows = (widget.table_design or TableDesign()).max_rows
        view = df.head(max_rows) if max_rows else df
        tbl = view.to_html(index=False, classes="memframe-table", border=0)
        return f"<div class='memframe-cell'>{tbl}</div>"

    if widget.kind == "metric":
        return _render_metric(result, widget)

    return f"<div class='memframe-cell'><p>{escape(str(result))}</p></div>"


_PAGE_TEMPLATE = """<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="utf-8"/>
<meta name="viewport" content="width=device-width, initial-scale=1"/>
<title>{title}</title>
<style>
:root {{ --bg:#ffffff; --fg:#1a1a1a; --card:#f4f6f8; --muted:#6b7280; --border:#e5e7eb; }}
[data-theme="dark"] {{ --bg:#111827; --fg:#e5e7eb; --card:#1f2937; --muted:#9ca3af; --border:#374151; }}
body {{ background:var(--bg); color:var(--fg); font-family: system-ui, sans-serif; margin:0; padding:24px; }}
h1 {{ font-size:28px; margin:0 0 20px; }}
.memframe-row {{ display:grid; grid-template-columns:repeat(12,1fr); gap:16px; margin-bottom:16px; }}
.memframe-cell {{ min-width:0; }}
.memframe-table {{ border-collapse:collapse; width:100%; font-size:13px; }}
.memframe-table th, .memframe-table td {{ border:1px solid var(--border); padding:6px 8px; text-align:left; }}
.memframe-card {{ background:var(--card); border-radius:10px; padding:16px 18px; }}
.memframe-card-title {{ color:var(--muted); font-size:13px; margin-bottom:6px; }}
.memframe-card-body {{ font-weight:600; }}
.memframe-card-value {{ font-weight:700; line-height:1.1; }}
.memframe-metric-row {{ display:flex; justify-content:space-between; padding:2px 0; }}
</style>
</head>
<body data-theme="{theme}">
<h1>{title}</h1>
{rows}
</body>
</html>
"""


def render_html(items: List[Dict[str, Any]], design: DashboardDesign) -> str:
    """Render a DashboardDesign + its raw result items into an HTML string."""
    rows = _pack_rows(design.widgets)
    plotly_included = [False]
    body_rows = []
    for row in rows:
        cells = []
        for w in row:
            result = items[w.result_index]["result"]
            cells.append(_render_cell(w, result, plotly_included))
        body_rows.append(f"<div class='memframe-row'>{''.join(cells)}</div>")
    return _PAGE_TEMPLATE.format(
        title=escape(design.dashboard_title),
        theme=design.global_theme,
        rows="".join(body_rows),
    )
