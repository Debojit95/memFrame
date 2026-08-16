import asyncio

import pandas as pd
import pytest

from memframe.main import MemFrame
from memframe_ai.config import AISettings
from memframe_ai.sessions import store
from memframe_ai.tools import arithmetic, clean, context, inspect, plot, select, stats, upload


@pytest.fixture
def session():
    m = MemFrame(connection_type="local", connection_params={"db_path": ":memory:"})
    asyncio.run(m.aconnect())
    df = pd.DataFrame(
        {"name": ["a", "b", "a", "c"], "age": [20, 30, 25, 40], "score": [1.0, 2.0, None, 4.0]}
    )
    ops = asyncio.run(m.aupload_df(df, filename="demo"))
    s = store.create("t1", ops=ops, settings=AISettings(api_key="k"))
    try:
        yield s
    finally:
        asyncio.run(m.aclose())


def _tool(session, name):
    tools = {}
    for module in (context, upload, inspect, select, clean, stats, arithmetic, plot):
        tools.update({f.__name__: f for f in module.tools(session)})
    return tools[name]


def test_head_returns_records(session):
    r = asyncio.run(_tool(session, "head")(n=2))
    assert r["ok"] is True
    assert len(r["result"]) == 2
    assert set(r["result"][0]) == {"name", "age", "score"}


def test_where_advances_active_table(session):
    base = session.table
    r = asyncio.run(_tool(session, "where")(cond="age > 24"))
    assert r["ok"] is True
    assert r["new_table"] is not None
    assert session.table != base
    assert r["active_table"] == session.table


def test_dropna_advances(session):
    r = asyncio.run(_tool(session, "dropna")(how="any"))
    assert r["ok"] is True
    assert session.table.endswith("__op_1")


def test_unknown_plot_type_hint(session):
    r = asyncio.run(_tool(session, "plot")(plot_type="nope", x="name"))
    assert r["ok"] is False
    assert "bar" in r["hint"]


def test_plot_stores_spec(session):
    r = asyncio.run(_tool(session, "plot")(plot_type="bar", x="name", y="age"))
    assert r["ok"] is True
    assert r["plot_id"] in session.plots
    assert "spec" not in r
    assert r["spec_preview"]["traces"] == 1
    assert r["spec_preview"]["points"] == 4
    assert session.plots[r["plot_id"]]["spec"]["data"]


def test_normalize_stashes_full_df_result():
    """The USER gets the full DataFrame; the MODEL only ever sees a capped sample."""
    m = MemFrame(connection_type="local", connection_params={"db_path": ":memory:"})
    asyncio.run(m.aconnect())
    big = pd.DataFrame({"x": range(30), "y": range(30, 60)})
    ops = asyncio.run(m.aupload_df(big, filename="big"))
    s = store.create("t2", ops=ops, settings=AISettings(api_key="k"))
    try:
        asyncio.run(s.ensure())
        r = asyncio.run(_tool(s, "head")(n=30))
        assert r["ok"] is True
        # user stash: full 30-row frame, both columns
        assert len(s.results) == 1
        assert len(s.results[0]) == 30
        assert list(s.results[0].columns) == ["x", "y"]
        # model path: still capped at max_output_rows (20)
        assert len(r["result"]) == 20
        s.reset_results()
    finally:
        asyncio.run(m.aclose())


def test_inspect_tools_do_not_advance(session):
    asyncio.run(session.ensure())
    base = session.table
    asyncio.run(_tool(session, "head")(n=2))
    asyncio.run(_tool(session, "describe")())
    asyncio.run(_tool(session, "null_analysis")())
    assert session.table == base


def test_subtract_creates_new_column_and_advances(session):
    base = session.table
    r = asyncio.run(_tool(session, "subtract")("age", 5, target_col="net"))
    assert r["ok"] is True
    assert r["new_table"] is not None
    assert session.table != base
    assert session.table == r["active_table"]
    assert sorted(row["net"] for row in r["result"]) == [15, 20, 25, 35]
