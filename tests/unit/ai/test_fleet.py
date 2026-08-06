import asyncio
from types import SimpleNamespace

import pandas as pd
import pytest
from pydantic_ai.toolsets import FunctionToolset

from memframe.main import MemFrame
from memframe_ai.agents import (
    SPECIALISTS,
    AnalyticsAgent,
    _make_delegate,
    _plot_tool,
)
from memframe_ai.agents.analytics import _orchestrator_prompt, _specialist_prompt
from memframe_ai.config import AISettings
from memframe_ai.sessions import store
from memframe_ai.tools import arithmetic, clean, context, inspect, select, stats, upload

_EXPECTED = {
    "context",
    "inspect",
    "select",
    "clean",
    "stats",
    "arithmetic",
    "plot_bar",
    "plot_line",
    "plot_pie",
    "plot_scatter",
    "plot_scatter_3d",
    "plot_bar_polar",
}


@pytest.fixture
def session():
    m = MemFrame(connection_type="local", connection_params={"db_path": ":memory:"})
    asyncio.run(m.aconnect())
    df = pd.DataFrame({"name": ["a", "b", "a", "c"], "age": [20, 30, 25, 40]})
    ops = asyncio.run(m.aupload_df(df, filename="demo"))
    s = store.create("f1", ops=ops, settings=AISettings(api_key="k"))
    try:
        yield s
    finally:
        asyncio.run(m.aclose())


def test_specialist_set(session):
    fleet = AnalyticsAgent(session, AISettings(api_key="k"))
    assert set(fleet.specialist_agents()) == _EXPECTED


def test_specialists_require_tool_calls():
    for name in ("clean", "arithmetic", "inspect"):
        sp = _specialist_prompt(name)
        assert "MUST actually call" in sp
        assert "run_code" in sp
        assert name in sp
        assert "PRESERVE THE FULL DATASET" in sp
        assert "never drop, rename, or collapse any column" in sp


def test_orchestrator_prevents_repeats():
    sp = _orchestrator_prompt("t", "upload")
    assert isinstance(sp, str)
    assert "EXACTLY ONCE" in sp
    assert "Do not re-run" in sp
    assert "were not asked to remove" in sp


def test_plot_specialists_expose_one_tool(session):
    for name in ("plot_bar", "plot_pie", "plot_scatter_3d"):
        plot_type = SPECIALISTS[name][0]
        assert plot_type == name.removeprefix("plot_")
        tool = _plot_tool(session, plot_type)
        assert list(FunctionToolset(tools=[tool]).tools) == [plot_type]
        assert tool.__doc__


def test_multitool_specialist_names(session):
    modules = {
        "context": context.tools(session) + upload.tools(session),
        "inspect": inspect.tools(session),
        "select": select.tools(session),
        "clean": clean.tools(session),
        "stats": stats.tools(session),
        "arithmetic": arithmetic.tools(session),
    }
    for name, fns in modules.items():
        assert set(FunctionToolset(tools=fns).tools) == {f.__name__ for f in fns}


def test_orchestrator_builds(session):
    fleet = AnalyticsAgent(session, AISettings(api_key="k"))
    assert fleet.orchestrator().name == "orchestrator"


class _FakeAgent:
    def __init__(self, output):
        self._output = output

    async def run(self, instruction, **kwargs):
        if isinstance(self._output, Exception):
            raise self._output
        return SimpleNamespace(output=self._output)


def test_make_delegate_name_and_result():
    d = _make_delegate(_FakeAgent("done"), "select", "pick columns")
    assert d.__name__ == "run_select"
    assert "pick columns" in d.__doc__
    assert asyncio.run(d("x")) == "done"


def test_make_delegate_error_path():
    d = _make_delegate(_FakeAgent(RuntimeError("boom")), "stats", "counts")
    out = asyncio.run(d("x"))
    assert "stats agent failed" in out
    assert "boom" in out
