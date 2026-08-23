"""Single-figure Plotly canvas renderer for a DashboardDesign.

Composes the whole dashboard as ONE responsive Plotly figure (subplots/domains)
so it fills the viewport. Widget types are fixed by the result type:
DataFrame -> table, Plotly Figure -> plot, dict/number -> metric (KPI).
Uses only ``plotly`` (already a core dep). No Dash / web server required.
"""

from __future__ import annotations

from typing import Any, Dict, List, Optional

import plotly.express as px
import plotly.graph_objects as go
from plotly.subplots import make_subplots

from memframe.dashboard.models import (
    ChartType,
    DashboardDesign,
    FigureDesign,
    MetricDesign,
    TableDesign,
    WidgetDesign,
)

_CARTESIAN_TYPES = {
    "bar", "scatter", "box", "violin", "histogram", "heatmap", "contour",
    "area", "line", "funnel", "waterfall", "ohlc", "candlestick",
}
_DOMAIN_TYPES = {"pie", "sunburst", "treemap", "funnelarea", "table", "indicator"}


def _pack_rows(widgets: List[WidgetDesign]) -> List[List[WidgetDesign]]:
    """Group widgets into rows of at most 2, never mixing a metric with a visual.

    Metrics are kept in their own (compact) rows so a KPI never shares a tall
    chart row and wastes vertical space.
    """
    ordered = sorted(widgets, key=lambda w: w.result_index)
    rows: List[List[WidgetDesign]] = []
    current: List[WidgetDesign] = []
    cur_cat: Optional[str] = None

    def _cat(w: WidgetDesign) -> str:
        return "metric" if w.kind == "metric" else "visual"

    for w in ordered:
        cat = _cat(w)
        if current and (len(current) >= 2 or cat != cur_cat):
            rows.append(current)
            current = []
            cur_cat = None
        current.append(w)
        cur_cat = cat
    if current:
        rows.append(current)
    return rows


def _is_figure(obj: Any) -> bool:
    return hasattr(obj, "to_plotly_json") and hasattr(obj, "to_html")


def _is_dataframe(obj: Any) -> bool:
    return hasattr(obj, "columns") and hasattr(obj, "to_html")


def _trace_grid_type(trace: Any) -> str:
    t = getattr(trace, "type", "scatter") or "scatter"
    if t in _CARTESIAN_TYPES:
        return "xy"
    if t in _DOMAIN_TYPES:
        return "domain"
    # scene / polar / map traces -> domain cell best-effort
    return "domain"


def _cell_type(widget: WidgetDesign, result: Any) -> str:
    """Plotly subplot cell type for a widget: 'xy' | 'domain' | 'table' | 'indicator'."""
    kind = widget.kind
    if kind == "table":
        return "table"
    if kind == "metric":
        return "table" if (isinstance(result, dict) and len(result) > 1) else "indicator"
    # kind == "plot"
    if _is_dataframe(result):
        return "xy"  # px chart is cartesian
    if _is_figure(result):
        traces = list(result.data)
        if traces and all(_trace_grid_type(t) == "xy" for t in traces):
            return "xy"
        return "domain"
    return "xy"


def _apply_layout(fig: go.Figure, d: FigureDesign) -> None:
    fig.update_layout(
        width=d.width,
        height=d.height,
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


def _build_figure(df: Any, d: FigureDesign) -> go.Figure:
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


def _df_to_table(df: Any, max_rows: Optional[int]) -> go.Table:
    view = df.head(max_rows) if max_rows else df
    return go.Table(
        header=dict(values=list(view.columns)),
        cells=dict(values=[view[c].tolist() for c in view.columns]),
    )


def _result_to_table(res: Any, max_rows: Optional[int]) -> go.Table:
    """Build a go.Table from a DataFrame, dict, list, or single scalar value."""
    if _is_dataframe(res):
        return _df_to_table(res, max_rows)
    if isinstance(res, dict):
        keys = list(res.keys())
        return go.Table(
            header=dict(values=keys),
            cells=dict(values=[[res[k] for k in keys]]),
        )
    if isinstance(res, (list, tuple)):
        vals = list(res)[:max_rows] if max_rows else list(res)
        return go.Table(
            header=dict(values=["value"]),
            cells=dict(values=[vals]),
        )
    # str / bool / other scalar -> single-cell table
    return go.Table(
        header=dict(values=["value"]),
        cells=dict(values=[[res]]),
    )


def _fmt_prefix_suffix(md: MetricDesign) -> Dict[str, str]:
    return {"prefix": md.prefix or "", "suffix": md.suffix or ""}


def _metric_trace(result: Any, widget: WidgetDesign) -> go.Trace:
    md = widget.metric_design or MetricDesign()
    if isinstance(result, dict) and len(result) == 1:
        val = next(iter(result.values()))
    elif isinstance(result, dict):
        keys = list(result.keys())
        return go.Table(
            header=dict(values=keys),
            cells=dict(values=[_fmt_prefix_suffix(md).get("prefix", "") + str(v) + _fmt_prefix_suffix(md).get("suffix", "") for v in result.values()]),
        )
    else:
        val = result
    number = dict(
        valueformat=f".{md.decimal_places}f",
        font=dict(size=md.font_size),
        **_fmt_prefix_suffix(md),
    )
    return go.Indicator(
        mode="number",
        value=float(val) if isinstance(val, (int, float)) else 0,
        number=number,
        title={"text": widget.title},
        domain=dict(y=[0.25, 0.85]),
    )


def _add_plot_traces(fig: go.Figure, widget: WidgetDesign, result: Any, row: int, col: int) -> None:
    if _is_dataframe(result):
        chart = _build_figure(result, widget.plot_design or FigureDesign())
        _apply_layout(chart, widget.plot_design or FigureDesign())
        for t in chart.data:
            fig.add_trace(t, row=row, col=col)
        pd = widget.plot_design
        if pd and pd.xaxis_title:
            fig.update_xaxes(title_text=pd.xaxis_title, row=row, col=col)
        if pd and pd.yaxis_title:
            fig.update_yaxes(title_text=pd.yaxis_title, row=row, col=col)
        return
    # pre-built figure
    src = result
    for t in src.data:
        fig.add_trace(t, row=row, col=col)
    # best-effort axis titles from source layout
    try:
        if getattr(src.layout, "xaxis", None) and getattr(src.layout.xaxis, "title", None):
            fig.update_xaxes(title_text=src.layout.xaxis.title.text, row=row, col=col)
        if getattr(src.layout, "yaxis", None) and getattr(src.layout.yaxis, "title", None):
            fig.update_yaxes(title_text=src.layout.yaxis.title.text, row=row, col=col)
    except Exception:
        pass


def _widget_title(widget: WidgetDesign) -> str:
    if widget.title:
        return widget.title
    if widget.kind == "plot" and widget.plot_design and widget.plot_design.title:
        return widget.plot_design.title
    return ""


def render_html(items: List[Dict[str, Any]], design: DashboardDesign) -> str:
    """Render a DashboardDesign + its raw result items into a single-figure HTML page."""
    fig = build_canvas(items, design)
    bg = "#111827" if design.global_theme == "dark" else "#ffffff"
    has_visual = any(w.kind in ("plot", "table") for w in design.widgets)
    if not has_visual:
        # ponytail: a KPI-only dashboard must not be stretched to fill the whole
        # viewport (a tiny number in a giant empty cell). Size it to its content
        # and center it instead.
        R = max(1, len(_pack_rows(design.widgets)))
        fig.update_layout(
            height=max(160, 150 * R),
            autosize=False,
            margin=dict(l=20, r=20, t=50, b=20),
        )
        fig_html = fig.to_html(include_plotlyjs=True, full_html=False, config={"responsive": True})
        return _PAGE_TEMPLATE_KPI.format(title=design.dashboard_title, bg=bg, figure=fig_html)
    fig_html = fig.to_html(include_plotlyjs=True, full_html=False, config={"responsive": True})
    return _PAGE_TEMPLATE.format(title=design.dashboard_title, bg=bg, figure=fig_html)


def build_canvas(items: List[Dict[str, Any]], design: DashboardDesign) -> go.Figure:
    """Compose the whole dashboard as ONE Plotly figure (subplots/domains)."""
    # ponytail: coerce kind from the actual result type so a DataFrame is never
    # charted, even if the design carries a wrong kind.
    for w in design.widgets:
        res = items[w.result_index]["result"]
        if _is_dataframe(res):
            w.kind = "table"
        elif _is_figure(res):
            w.kind = "plot"
        elif isinstance(res, (int, float)):
            w.kind = "metric"
        elif isinstance(res, (dict, list, str, bool)):
            # ponytail: dict -> metric (single KPI) or table (multi-key); list /
            # str / bool -> table. Renderer resolves dict single- vs multi-key.
            w.kind = "table" if isinstance(res, (list, str, bool)) else "metric"

    rows = _pack_rows(design.widgets)
    R = len(rows)
    specs: List[List[Optional[Dict[str, Any]]]] = []
    titles: List[str] = []
    cell_map: List[tuple] = []  # (widget, result, row, col)
    for ri, row in enumerate(rows, start=1):
        if len(row) == 1:
            w = row[0]
            res = items[w.result_index]["result"]
            specs.append([{"type": _cell_type(w, res), "colspan": 2}, None])
            titles.append(_widget_title(w))
            titles.append("")
            cell_map.append((w, res, ri, 1))
        else:
            spec_row: List[Optional[Dict[str, Any]]] = []
            for ci, w in enumerate(row, start=1):
                res = items[w.result_index]["result"]
                spec_row.append({"type": _cell_type(w, res)})
                titles.append(_widget_title(w))
                cell_map.append((w, res, ri, ci))
            specs.append(spec_row)

    # ponytail: metric-only rows get a thin weight so a KPI strip doesn't eat a
    # full chart-height row. Weights are relative; Plotly normalizes them.
    row_heights = [
        0.15 if all(w.kind == "metric" for w in row) else 1.0 for row in rows
    ]
    fig = make_subplots(
        rows=R,
        cols=2,
        specs=specs,
        subplot_titles=titles,
        row_heights=row_heights,
        vertical_spacing=0.05,
        horizontal_spacing=0.05,
    )

    for w, res, r, c in cell_map:
        if w.kind == "table":
            fig.add_trace(_result_to_table(res, (w.table_design or TableDesign()).max_rows), row=r, col=c)
        elif w.kind == "metric":
            fig.add_trace(_metric_trace(res, w), row=r, col=c)
        else:  # plot
            _add_plot_traces(fig, w, res, r, c)

    template = "plotly_dark" if design.global_theme == "dark" else "plotly_white"
    fig.update_layout(
        template=template,
        title=design.dashboard_title or None,
        autosize=True,
        margin=dict(l=20, r=20, t=50, b=20),
        showlegend=True,
    )
    return fig


_PAGE_TEMPLATE = """<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="utf-8"/>
<meta name="viewport" content="width=device-width, initial-scale=1"/>
<title>{title}</title>
<style>
html, body {{ margin:0; padding:0; width:100vw; height:100vh; overflow:hidden; background:{bg}; }}
#memframe-canvas {{ width:100vw; height:100vh; }}
</style>
</head>
<body>
<div id="memframe-canvas">{figure}</div>
</body>
</html>
"""

_PAGE_TEMPLATE_KPI = """<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="utf-8"/>
<meta name="viewport" content="width=device-width, initial-scale=1"/>
<title>{title}</title>
<style>
html, body {{ margin:0; padding:0; width:100vw; height:100vh; overflow:hidden; background:{bg}; }}
#memframe-canvas {{ width:100vw; height:100vh; display:flex; align-items:center; justify-content:center; }}
#memframe-kpi {{ width:min(90vw, 760px); }}
</style>
</head>
<body>
<div id="memframe-canvas"><div id="memframe-kpi">{figure}</div></div>
</body>
</html>
"""


_GUARDRAIL_TEMPLATE = """<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="utf-8"/>
<meta name="viewport" content="width=device-width, initial-scale=1"/>
<title>{title}</title>
<style>
html, body {{ margin:0; padding:0; width:100vw; height:100vh; overflow:hidden; }}
#memframe-guardrail {{
  width:100vw; height:100vh; display:flex; align-items:center; justify-content:center;
  background:#0f172a; font-family:system-ui, -apple-system, Segoe UI, Roboto, sans-serif;
}}
#memframe-guardrail .card {{
  width:min(90vw, 720px); padding:32px 36px; border-radius:14px;
  background:#1e293b; color:#e2e8f0; box-shadow:0 10px 40px rgba(0,0,0,.35);
}}
#memframe-guardrail h2 {{ margin:0 0 12px; font-size:20px; color:#f87171; }}
#memframe-guardrail p {{ margin:8px 0; line-height:1.5; font-size:15px; }}
#memframe-guardrail .reason {{ color:#fbbf24; font-weight:600; }}
#memframe-guardrail .hint {{ color:#94a3b8; font-size:13px; }}
</style>
</head>
<body>
<div id="memframe-guardrail">
  <div class="card">
    <h2>{title}</h2>
    <p>Execution was stopped before planning.</p>
    <p class="reason">{reason}</p>
    <p class="hint">{hint}</p>
  </div>
</div>
</body>
</html>
"""


def render_guardrail_blocked(reason: str, title: str = "Query blocked by guardrail") -> str:
    """Return a full-screen, themed HTML page explaining why a query was stopped."""
    hint = "Rephrase your request so it refers to the active dataset's columns."
    safe_reason = (reason or "Query blocked by the guardrail.").replace("{", "{{").replace("}", "}}")
    return _GUARDRAIL_TEMPLATE.format(title=title, reason=safe_reason, hint=hint)

