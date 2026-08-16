import asyncio

import pandas as pd
import pytest
from pydantic_ai.toolsets import FunctionToolset

from memframe.main import MemFrame
from memframe_ai.agents import (
    SPECIALISTS,
    AnalyticsAgent,
    _plot_tool,
)
from memframe_ai.agents.analytics import _specialist_prompt
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


def test_plot_specialists_expose_one_tool(session):
    for name in ("plot_bar", "plot_pie", "plot_scatter_3d"):
        plot_type = SPECIALISTS[name][0]
        assert plot_type == name.removeprefix("plot_")
        tool = _plot_tool(session, plot_type)
        assert list(FunctionToolset(tools=[tool]).tools) == [plot_type]
        assert tool.__doc__


def test_chat_resets_subquery_results_between_calls(session):
    class _FakePlanner:
        def __init__(self, settings):
            pass

        async def plan_with_dependencies(self, prompt, context):
            return None

    agent = AnalyticsAgent(session, AISettings(api_key="k"))
    agent._planner = _FakePlanner(AISettings(api_key="k"))

    session.record_subquery_result("stale", {"ok": True, "result": [{"x": 1}]})
    assert session.subquery_results

    asyncio.run(agent.achat("does nothing"))

    assert session.subquery_results == []
    assert session.results == []


def test_package_returns_full_plot_spec(session):
    agent = AnalyticsAgent(session, AISettings(api_key="k"))
    full_spec = {"data": [{"type": "bar", "x": ["a", "b"], "y": [1, 2]}], "layout": {}}
    session.add_plot("p1", "My Title", full_spec, None)

    result = {"ok": True, "plot_id": "p1", "title": "My Title", "spec": full_spec, "spec_preview": {"traces": 1}}
    session.record_subquery_result("bar(x=a, y=b)", result)

    packaged = agent._package()
    plot_entry = packaged["plots"][0]
    assert plot_entry["spec"] == full_spec
    assert plot_entry["spec_preview"]
    assert "blocks" not in packaged
    assert packaged["answer"] == "bar(x=a, y=b): ✓ (plot shown)"
    assert packaged["error"] is None


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


def test_fmt_subquery_branches():
    from memframe_ai.agents.analytics import _fmt_subquery

    assert _fmt_subquery("bar(x=a)", {"ok": True, "plot_id": "p1", "title": "T", "spec": {"data": []}}) == "bar(x=a): ✓ (plot shown)"
    assert _fmt_subquery("fillna(c)", {"ok": True, "result": [{"a": 1}, {"a": 2}]}) == "fillna(c): ✓ (table shown)"
    assert _fmt_subquery("add(a,b)", {"ok": True, "result": 42}) == "add(a,b): ✓ 42"
    assert _fmt_subquery("describe()", {"ok": True, "result": {"mean": 1.0}}) == "describe(): ✓ {'mean': 1.0}"
    assert _fmt_subquery("head(n=2)", {"ok": True}) == "head(n=2): ✓"
    assert _fmt_subquery("x", {"ok": False, "hint": "boom"}) == "x: ✗ boom"
    assert _fmt_subquery("x", {"ok": False, "message": "nope"}) == "x: ✗ nope"


def test_package_answer_joins_subqueries(session):
    agent = AnalyticsAgent(session, AISettings(api_key="k"))
    session.record_subquery_result("fillna(c)", {"ok": True, "result": [{"a": 1}]})
    session.record_subquery_result("bar(x=a, y=b)", {"ok": True, "plot_id": "p1", "title": "T", "spec": {}})
    packaged = agent._package()
    assert packaged["answer"] == (
        "fillna(c): ✓ (table shown)\n............\n"
        "bar(x=a, y=b): ✓ (plot shown)"
    )


def test_package_returns_full_result(session):
    """_package exposes the full DataFrame result(s), not a capped sample."""
    agent = AnalyticsAgent(session, AISettings(api_key="k"))
    full_df = pd.DataFrame({"cleaned_total_cases": [1.0, 2.0, 3.0, 4.0]})
    session.add_result(full_df)
    session.record_subquery_result(
        "fillna(total_cases, mode=median)",
        {"ok": True, "result": [{"cleaned_total_cases": 1.0}]},
    )

    packaged = agent._package()
    assert isinstance(packaged["result"], pd.DataFrame)
    assert packaged["result"].equals(full_df)
    assert packaged["results"] == [full_df]
    assert (
        packaged["answer"]
        == "fillna(total_cases, mode=median): ✓ (table shown)"
    )