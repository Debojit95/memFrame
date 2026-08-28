import asyncio
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


def test_render_single_figure_canvas():
    # ponytail: the whole dashboard must be ONE Plotly figure (the "canvas"),
    # not one embedded figure per widget.
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
    assert "0.42" in html
    assert "plotly" in html.lower()
    # exactly one plotly graph div => a single composed figure
    assert html.count('class="plotly-graph-div"') == 1
    # full-screen viewport wrapper
    assert "100vh" in html
    # composed figure has exactly one trace per widget (bar / table / indicator)
    fig = render_mod.build_canvas(items, design)
    kinds = [t.type for t in fig.data]
    assert kinds == ["bar", "table", "indicator"]


def test_dataframe_renders_as_table():
    # ponytail: a DataFrame result must render as a Plotly TABLE, never a chart.
    # Here the design wrongly says kind='plot' for a DataFrame — the renderer
    # must coerce it to a table and emit no cartesian chart.
    df = _df()
    items = [{"title": "raw", "result": df}]
    design = DashboardDesign(
        widgets=[
            WidgetDesign(
                result_index=0,
                kind="plot",  # intentionally wrong to lock the bug fix
                col_span=12,
                plot_design=FigureDesign(chart_type=ChartType.BAR, x="region", y="sales"),
            )
        ]
    )
    fig = render_mod.build_canvas(items, design)
    kinds = [t.type for t in fig.data]
    assert kinds == ["table"]


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
    fig = render_mod.build_canvas(items, design)
    assert [t.type for t in fig.data] == ["bar"]


def test_render_metric_multikey_dict_as_table():
    items = [{"title": "breakdown", "result": {"a": 1, "b": 2, "c": 3}}]
    design = DashboardDesign(
        widgets=[WidgetDesign(result_index=0, kind="metric", col_span=12)]
    )
    fig = render_mod.build_canvas(items, design)
    assert [t.type for t in fig.data] == ["table"]


def test_render_scalar_metric():
    items = [{"title": "rate", "result": 0.42}]
    design = DashboardDesign(
        widgets=[
            WidgetDesign(
                result_index=0,
                kind="metric",
                col_span=12,
                metric_design=MetricDesign(suffix="%", decimal_places=1),
            )
        ]
    )
    fig = render_mod.build_canvas(items, design)
    assert [t.type for t in fig.data] == ["indicator"]


def test_render_list_as_table():
    # ponytail: a list sub-query result must render as a table, not crash.
    items = [{"title": "vals", "result": [1, 2, 3]}]
    design = DashboardDesign(widgets=[WidgetDesign(result_index=0, kind="metric", col_span=12)])
    fig = render_mod.build_canvas(items, design)
    assert [t.type for t in fig.data] == ["table"]


def test_render_string_as_table():
    items = [{"title": "note", "result": "all good"}]
    design = DashboardDesign(widgets=[WidgetDesign(result_index=0, kind="metric", col_span=12)])
    fig = render_mod.build_canvas(items, design)
    assert [t.type for t in fig.data] == ["table"]


def test_metric_rows_are_compact(monkeypatch):
    # ponytail: a metric-only row must get a thin row weight, not a full
    # chart-height row. Spy on make_subplots to capture the row_heights arg.
    captured = {}
    orig = render_mod.make_subplots

    def _spy(*a, **k):
        captured.update(k)
        return orig(*a, **k)

    monkeypatch.setattr(render_mod, "make_subplots", _spy)
    df = _df()
    items = [
        {"title": "kpi", "result": 0.42},
        {"title": "chart", "result": px.bar(df, x="region", y="sales")},
    ]
    design = DashboardDesign(
        widgets=[
            WidgetDesign(result_index=0, kind="metric", col_span=12),
            WidgetDesign(
                result_index=1,
                kind="plot",
                col_span=12,
                plot_design=FigureDesign(chart_type=ChartType.KEEP_EXISTING),
            ),
        ]
    )
    render_mod.build_canvas(items, design)
    assert captured.get("row_heights") == [0.15, 1.0]


def test_render_only_metrics_is_centered():
    # ponytail: a KPI-only dashboard must not be stretched full-viewport.
    items = [{"title": "a", "result": 1}, {"title": "b", "result": 2}]
    design = DashboardDesign(
        widgets=[
            WidgetDesign(result_index=0, kind="metric", col_span=6),
            WidgetDesign(result_index=1, kind="metric", col_span=6),
        ]
    )
    html = render_mod.render_html(items, design)
    assert "memframe-kpi" in html
    assert "100vh" in html


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
    assert html.count('class="plotly-graph-div"') == 1


def test_manager_design_coerces_dataframe_to_table():
    # ponytail: design() must flip a DataFrame result to kind='table' even if the
    # (stubbed) agent returned kind='plot'.
    import memframe_ai.agents.dashboard as dash_mod

    df = _df()
    dm = DashboardManager()
    dm.add("raw", df)
    real = dash_mod.DashboardAgent

    class _Stub:
        def __init__(self, settings):
            pass

        async def design(self, summaries):
            return DashboardDesign(
                widgets=[WidgetDesign(result_index=0, kind="plot", col_span=12)]
            )

    dash_mod.DashboardAgent = _Stub
    try:
        plan = asyncio.run(dm.design(object()))
    finally:
        dash_mod.DashboardAgent = real
    assert plan.widgets[0].kind == "table"


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


def test_show_displays_native_figure_in_notebook(monkeypatch):
    # ponytail: in a notebook (Colab/Jupyter) show() must return the native Plotly
    # figure so the kernel auto-displays it via the plotly extension's LOCAL
    # plotly.js (no CDN). It must NOT call fig.show() (which embeds the blocked
    # CDN script and crashes the kernel) -- this is the 0.2.2 behaviour.
    import memframe.utils.plot_renderer as _pr

    monkeypatch.setattr(_pr, "in_notebook", lambda: True)

    shown = []
    monkeypatch.setattr(
        "plotly.graph_objects.Figure.show",
        lambda self, *a, **k: shown.append(self),
    )

    dm = DashboardManager()
    dm.add("sales plot", px.bar(_df(), x="region", y="sales"))
    design = DashboardDesign(
        dashboard_title="Test",
        global_theme="light",
        widgets=[WidgetDesign(result_index=0, kind="plot", col_span=6)],
    )

    result = dm.show(design=design)

    assert isinstance(result, go.Figure)
    assert len(shown) == 0


def test_adashboard_returns_figure_in_notebook(monkeypatch):
    # ponytail: in a notebook adashboard() must return the native Plotly figure,
    # not the HTML string, so the cell renders it via its mimebundle.
    import types

    from memframe.db_manager.context import ContextManager
    import memframe.dashboard.manager as _mgr

    import json

    ctx = ContextManager(types.SimpleNamespace(), "d1")
    fig = px.bar(_df(), x="region", y="sales")
    spec = json.loads(fig.to_json())
    resp = {
        "guardrail_blocked": False,
        "plots": [{"title": "p", "spec": spec}],
        "results": [],
        "values": [],
    }

    async def fake_achat(sentence):
        return resp

    monkeypatch.setattr(ContextManager, "achat", staticmethod(fake_achat))
    monkeypatch.setattr(
        "memframe_ai.entrypoints._get_settings", lambda memframe: object()
    )

    design = DashboardDesign(
        dashboard_title="T",
        widgets=[WidgetDesign(result_index=0, kind="plot", col_span=6)],
    )

    async def fake_design(settings):
        return design

    monkeypatch.setattr(_mgr.DashboardManager, "design", staticmethod(fake_design))
    monkeypatch.setattr("memframe.utils.plot_renderer.in_notebook", lambda: True)

    # show=False: we only assert the RETURN value, not the display side effects.
    result = asyncio.run(ctx.adashboard("q", show=False))
    assert isinstance(result, go.Figure)


def test_adashboard_returns_html_in_terminal(monkeypatch):
    # ponytail: outside a notebook adashboard() still returns the self-contained
    # HTML string (terminals have no mimebundle renderer).
    import types

    from memframe.db_manager.context import ContextManager
    import memframe.dashboard.manager as _mgr

    import json

    ctx = ContextManager(types.SimpleNamespace(), "d1")
    fig = px.bar(_df(), x="region", y="sales")
    spec = json.loads(fig.to_json())
    resp = {
        "guardrail_blocked": False,
        "plots": [{"title": "p", "spec": spec}],
        "results": [],
        "values": [],
    }

    async def fake_achat(sentence):
        return resp

    monkeypatch.setattr(ContextManager, "achat", staticmethod(fake_achat))
    monkeypatch.setattr(
        "memframe_ai.entrypoints._get_settings", lambda memframe: object()
    )

    design = DashboardDesign(
        dashboard_title="T",
        widgets=[WidgetDesign(result_index=0, kind="plot", col_span=6)],
    )

    async def fake_design(settings):
        return design

    monkeypatch.setattr(_mgr.DashboardManager, "design", staticmethod(fake_design))
    monkeypatch.setattr("memframe.utils.plot_renderer.in_notebook", lambda: False)

    result = asyncio.run(ctx.adashboard("q", show=False))
    assert isinstance(result, str)
    assert "<html" in result.lower()


def test_in_colab_false_by_default():
    # ponytail: the Colab-gated path must stay off in normal test/CI environments.
    from memframe.utils.plot_renderer import in_colab

    assert in_colab() is False


def test_adashboard_returns_figure_in_colab(monkeypatch):
    # ponytail: in Colab adashboard() must return the native Plotly figure (so
    # Colab renders it via its native plotly.js), not inline HTML (which embeds a
    # ~3MB bundle and crashes the runtime).
    import types

    import memframe.dashboard.manager as _mgr
    from memframe.db_manager.context import ContextManager

    import json

    ctx = ContextManager(types.SimpleNamespace(), "d1")
    fig = px.bar(_df(), x="region", y="sales")
    spec = json.loads(fig.to_json())
    resp = {
        "guardrail_blocked": False,
        "plots": [{"title": "p", "spec": spec}],
        "results": [],
        "values": [],
    }

    async def fake_achat(sentence):
        return resp

    monkeypatch.setattr(ContextManager, "achat", staticmethod(fake_achat))
    monkeypatch.setattr(
        "memframe_ai.entrypoints._get_settings", lambda memframe: object()
    )
    design = DashboardDesign(
        dashboard_title="T",
        widgets=[WidgetDesign(result_index=0, kind="plot", col_span=6)],
    )

    async def fake_design(settings):
        return design

    monkeypatch.setattr(_mgr.DashboardManager, "design", staticmethod(fake_design))
    monkeypatch.setattr("memframe.utils.plot_renderer.in_notebook", lambda: True)
    monkeypatch.setattr("memframe.utils.plot_renderer.in_colab", lambda: True)

    result = asyncio.run(ctx.adashboard("q", show=False))
    assert isinstance(result, go.Figure)


def test_show_displays_figure_in_colab(monkeypatch):
    # ponytail: in Colab show() must return the native Plotly figure so the kernel
    # auto-displays it via the plotly extension's LOCAL plotly.js (no CDN). It must
    # NOT call fig.show() (which embeds the blocked CDN script and crashes the
    # kernel); this matches the 0.2.2 behaviour.
    import memframe.utils.plot_renderer as _pr

    monkeypatch.setattr(_pr, "in_notebook", lambda: True)
    monkeypatch.setattr(_pr, "in_colab", lambda: True)

    shown = []
    monkeypatch.setattr(
        "plotly.graph_objects.Figure.show",
        lambda self, *a, **k: shown.append(self),
    )

    dm = DashboardManager()
    dm.add("sales plot", px.bar(_df(), x="region", y="sales"))
    design = DashboardDesign(
        dashboard_title="Test",
        global_theme="light",
        widgets=[WidgetDesign(result_index=0, kind="plot", col_span=6)],
    )
    result = dm.show(design=design)
    assert isinstance(result, go.Figure)
    assert len(shown) == 0

