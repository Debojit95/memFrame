import pandas as pd
import plotly.express as px
import plotly.graph_objects as go

from memframe.dashboard import DashboardManager
from memframe.dashboard import render as render_mod
from memframe.dashboard.models import (
    ChartType,
    DashboardDesign,
    FigureDesign,
    MetricDesign,
    WidgetDesign,
)


def _df():
    return pd.DataFrame({"region": ["A", "B", "C"], "sales": [10, 20, 30]})


def test_models_constraints():
    w = WidgetDesign(result_index=0, kind="plot", col_span=6)
    assert w.col_span == 6
    d = DashboardDesign(widgets=[w])
    assert d.global_theme == "light"


def test_summarize_covers_kinds():
    dm = DashboardManager()
    dm.add("sales plot", px.bar(_df(), x="region", y="sales"))
    dm.add("raw table", _df())
    dm.add("metric dict", {"conversion": 0.42})
    dm.add("scalar", 123)
    s = dm.summarize()
    assert len(s) == 4
    assert any("Plotly figure" in x for x in s)
    assert any("DataFrame" in x for x in s)
    assert any("dict with keys" in x for x in s)
    assert any("scalar" in x for x in s)


def test_pack_rows_max_two():
    widgets = [
        WidgetDesign(result_index=0, kind="plot", col_span=6),
        WidgetDesign(result_index=1, kind="plot", col_span=6),
        WidgetDesign(result_index=2, kind="table", col_span=12),
    ]
    rows = render_mod._pack_rows(widgets)
    assert len(rows) == 2
    assert [len(r) for r in rows] == [2, 1]


def test_render_html_with_figure_table_metric():
    df = _df()
    items = [
        {"title": "chart", "result": px.bar(df, x="region", y="sales")},
        {"title": "tbl", "result": df},
        {"title": "metric", "result": 0.42},
    ]
    design = DashboardDesign(
        dashboard_title="Sales",
        widgets=[
            WidgetDesign(
                result_index=0,
                kind="plot",
                col_span=6,
                plot_design=FigureDesign(title="Sales by region", chart_type=ChartType.KEEP_EXISTING),
            ),
            WidgetDesign(result_index=1, kind="table", col_span=6),
            WidgetDesign(
                result_index=2,
                kind="metric",
                col_span=12,
                metric_design=MetricDesign(suffix="%", decimal_places=1),
            ),
        ],
    )
    html = render_mod.render_html(items, design)
    assert "Sales by region" in html
    assert "0.4%" in html
    assert html.count("<div class='memframe-row'>") == 2
    assert "plotly" in html.lower()


def test_render_auto_chart_from_df():
    df = _df()
    items = [{"title": "raw", "result": df}]
    design = DashboardDesign(
        widgets=[
            WidgetDesign(
                result_index=0,
                kind="plot",
                col_span=12,
                plot_design=FigureDesign(chart_type=ChartType.BAR, x="region", y="sales"),
            )
        ]
    )
    html = render_mod.render_html(items, design)
    assert "plotly" in html.lower()


def test_render_existing_figure_layout():
    fig = go.Figure()
    fig.add_bar(x=["A", "B"], y=[1, 2])
    items = [{"title": "f", "result": fig}]
    design = DashboardDesign(
        widgets=[
            WidgetDesign(
                result_index=0,
                kind="plot",
                col_span=12,
                plot_design=FigureDesign(title="My Chart", width=900, height=450),
            )
        ]
    )
    html = render_mod.render_html(items, design)
    assert "My Chart" in html


def test_manager_render_end_to_end():
    df = _df()
    dm = DashboardManager()
    dm.add("chart", px.bar(df, x="region", y="sales"))
    dm.add("table", df)
    design = DashboardDesign(
        dashboard_title="Demo",
        widgets=[
            WidgetDesign(
                result_index=0,
                kind="plot",
                col_span=6,
                plot_design=FigureDesign(title="C", chart_type=ChartType.KEEP_EXISTING),
            ),
            WidgetDesign(result_index=1, kind="table", col_span=6),
        ],
    )
    html = dm.render(design)
    assert "Demo" in html
    assert html.count("<div class='memframe-row'>") == 1


def test_summarize_is_data_light_dataframe():
    # ponytail: the dashboard LLM must only ever see a compact summary, never
    # the raw data values (token cost + privacy).
    df = pd.DataFrame({"A": range(10000), "B": [f"val_{i}" for i in range(10000)]})
    dm = DashboardManager()
    dm.add("Big", df)
    s = dm.summarize()[0]
    assert "10000 rows" in s
    assert "val_9999" not in s
    assert "val_5000" not in s
    assert len(s) < 1000


def test_summarize_is_data_light_figure():
    fig = go.Figure()
    fig.add_trace(go.Scatter(x=[1, 2, 3], y=[9.87654321, 1.23456789, 4.56789123]))
    dm = DashboardManager()
    dm.add("Fig", fig)
    s = dm.summarize()[0]
    # preview reports trace type / point counts only, never raw coordinates
    assert "9.87654321" not in s
    assert "scatter" in s
    assert len(s) < 1000


def test_dashboard_naming_convention():
    # ponytail: lock the a*/sync wrapper convention used across MemFrame.
    import inspect

    from memframe import MemFrame

    assert inspect.iscoroutinefunction(MemFrame.adashboard)
    assert not inspect.iscoroutinefunction(MemFrame.dashboard)
