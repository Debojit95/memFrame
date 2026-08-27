"""DashboardManager: collect query results, design a layout via the AI agent,
and render it as a zero-dependency HTML dashboard.
"""

from __future__ import annotations

from typing import Any, Dict, List, Optional, Tuple

from memframe.dashboard import render as _render
from memframe.dashboard.models import ChartType, DashboardDesign


def _is_figure(obj: Any) -> bool:
    return hasattr(obj, "to_plotly_json") and hasattr(obj, "to_html")


def _is_dataframe(obj: Any) -> bool:
    return hasattr(obj, "columns") and hasattr(obj, "to_html")


def _summarize_item(index: int, title: str, result: Any) -> str:
    label = f"Result {index} [{title}]"
    if _is_figure(result):
        spec = result.to_plotly_json()
        try:
            from memframe_ai.tools.plot import plot_spec_preview

            prev = plot_spec_preview(spec)
            return (
                f"{label}: Plotly figure — traces={prev['traces']}, "
                f"types={prev['trace_types']}, points={prev['points']}, "
                f"title={prev.get('title')}"
            )
        except Exception:
            return f"{label}: Plotly figure"
    if _is_dataframe(result):
        df = result
        sample = df.head(2).to_dict("records")
        return (
            f"{label}: DataFrame {len(df)} rows x {len(df.columns)} cols; "
            f"columns={list(df.columns)}; sample={sample}"
        )
    if isinstance(result, dict):
        return f"{label}: dict with keys={list(result.keys())[:20]}"
    if isinstance(result, (list, tuple)):
        return f"{label}: list of length {len(result)}"
    if isinstance(result, (int, float, str, bool)):
        return f"{label}: scalar {type(result).__name__} = {result!r}"
    return f"{label}: {type(result).__name__}"


class DashboardManager:
    """Collect results, design a layout with the AI agent, render to HTML."""

    def __init__(self) -> None:
        self._items: List[Dict[str, Any]] = []

    def add(self, title: str, result: Any, description: Optional[str] = None) -> "DashboardManager":
        self._items.append({"title": title, "result": result, "description": description})
        return self

    def collect(self, items: List[Tuple[str, Any]]) -> "DashboardManager":
        for title, result in items:
            self.add(title, result)
        return self

    def summarize(self) -> List[str]:
        return [_summarize_item(i, it["title"], it["result"]) for i, it in enumerate(self._items)]

    async def design(self, settings: Any) -> DashboardDesign:
        # ponytail: lazy import keeps the core lib free of the pydantic-ai extra.
        from memframe_ai.agents.dashboard import DashboardAgent

        agent = DashboardAgent(settings)
        plan = await agent.design(self.summarize())
        # ponytail: coerce kind from the ACTUAL result type so a DataFrame can
        # never be charted and a pre-built figure is never shown as a table,
        # even if the LLM mislabels it.
        for w in plan.widgets:
            res = self._items[w.result_index]["result"]
            if _is_dataframe(res):
                w.kind = "table"
                w.plot_design = None
            elif _is_figure(res):
                w.kind = "plot"
                if w.plot_design:
                    w.plot_design.chart_type = ChartType.KEEP_EXISTING
            elif isinstance(res, (int, float)):
                w.kind = "metric"
            elif isinstance(res, (dict, list, str, bool)):
                w.kind = "table" if isinstance(res, (list, str, bool)) else "metric"
        return plan

    def render(self, design: DashboardDesign) -> str:
        return _render.render_html(self._items, design)

    def save(self, design: DashboardDesign, filename: str = "dashboard.html") -> str:
        html = self.render(design)
        with open(filename, "w", encoding="utf-8") as fh:
            fh.write(html)
        return filename

    def render_figure(self, design: "DashboardDesign"):
        """The composed dashboard figure for native notebook display."""
        return _render.render_figure(self._items, design)

    def show(
        self,
        design: Optional["DashboardDesign"] = None,
        html: Optional[str] = None,
        filename: str = "dashboard.html",
    ) -> None:
        """Env-agnostic display (notebook inline / browser) of the dashboard.

        Pass a pre-rendered ``html`` string, or a ``design`` to render first.
        In a live notebook (Colab/Jupyter/VSCode) the composed Plotly figure is
        displayed natively so it isn't broken by HTML ``<script>`` sanitization.
        The native-figure path never falls back to ``display(HTML(...))`` — the
        dashboard HTML embeds a Plotly ``<script>`` that Jupyter frontends strip
        or crash on.
        """
        from memframe.utils.plot_renderer import in_notebook, smart_show

        if html is None:
            if design is None:
                raise ValueError("show() requires either `design` or `html`.")
            html = self.render(design)

        if design is not None and in_notebook():
            # ponytail: notebook -> show the native figure via its mimebundle.
            # On any display failure we return (not fall back to HTML), because
            # display(HTML(...)) on the embedded Plotly script crashes the kernel.
            try:
                from IPython.display import display

                display(self.render_figure(design))
            except Exception:
                pass
            return

        smart_show(html, filename)
