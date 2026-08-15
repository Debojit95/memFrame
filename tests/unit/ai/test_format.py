import asyncio
import json

from memframe_ai.format import (
    classify_block,
    render_block,
    render_blocks,
    records_to_md_table,
)


def test_records_to_md_table():
    rows = [{"A": 1, "B": "x"}, {"A": 2, "B": "y"}]
    table = records_to_md_table(rows)
    assert "| A | B |" in table
    assert "| 1 | x |" in table
    assert "| 2 | y |" in table


def test_records_to_md_table_empty():
    assert records_to_md_table([]) == "(empty result)"


def test_classify_df():
    b = classify_block(
        "head(n=2)",
        {"ok": True, "result": [{"A": 1}, {"A": 2}]},
    )
    assert b["type"] == "df"
    assert b["query"] == "head(n=2)"
    assert b["rows"] == 2
    assert b["columns"] == ["A"]


def test_classify_plot():
    b = classify_block(
        "bar(x=G)",
        {"ok": True, "plot_id": "abc", "title": "t", "spec_preview": {"traces": 1}},
    )
    assert b["type"] == "plot"
    assert b["plot_id"] == "abc"
    assert b["spec"] == {"traces": 1}
    assert b["error"] is None


def test_classify_error():
    b = classify_block("fillna(C)", {"ok": False, "hint": "boom"})
    assert b["type"] == "error"
    assert b["message"] == "boom"


def test_classify_dict():
    b = classify_block("value_counts(D)", {"ok": True, "result": {"zoom": 5}})
    assert b["type"] == "dict"
    assert b["value"] == {"zoom": 5}


def test_render_block_and_blocks():
    blocks = [
        {"query": "q1", "type": "df", "records": [{"A": 1}], "columns": ["A"], "rows": 1},
        {"query": "q2", "type": "error", "message": "boom"},
    ]
    out = render_blocks(blocks)
    assert "User query = q1" in out
    assert "User query = q2" in out
    assert "[Error] boom" in out
    assert "User query = q2\nResponse:" in out


def test_render_plot_block():
    out = render_block({"query": "q", "type": "plot", "plot_id": "abc", "title": "T", "spec": {}, "error": None})
    assert "[Plot] T (id: abc)" in out


def test_recorder_wraps_tool():
    from memframe_ai.agents.analytics import _recorded
    from memframe_ai.sessions import Session

    session = Session(session_id="s", ops=None, memframe=None, settings=None)

    async def fake_tool(n: int = 1) -> dict:
        return {"ok": True, "result": [{"n": n}]}

    wrapped = _recorded(session, fake_tool)
    assert wrapped.__name__ == "fake_tool"
    assert asyncio.run(wrapped(n=2)) == {"ok": True, "result": [{"n": 2}]}
    assert session.blocks[0]["query"] == "fake_tool(n=2)"
    assert session.blocks[0]["type"] == "df"


def test_reset_blocks():
    from memframe_ai.sessions import Session

    session = Session(session_id="s", ops=None, memframe=None, settings=None)
    session.record_block({"query": "q", "type": "dict", "value": 1})
    session.reset_blocks()
    assert session.blocks == []


def test_plot_spec_is_json_safe():
    from memframe.main import MemFrame
    from memframe_ai.config import AISettings
    from memframe_ai.sessions import store
    import pandas as pd

    m = MemFrame(connection_type="local", connection_params={"db_path": ":memory:"})
    asyncio.run(m.aconnect())
    df = pd.DataFrame({"G": ["a", "b", "a"], "C": [1.0, 2.0, 3.0]})
    ops = asyncio.run(m.aupload_df(df, filename="demo"))
    s = store.create("p1", ops=ops, settings=AISettings(api_key="k"))

    from memframe_ai.tools import plot

    try:
        fn = {f.__name__: f for f in plot.tools(s)}["plot"]
        res = asyncio.run(fn(plot_type="bar", x="G", y="C", title="T"))
        assert res["ok"] is True
        json.dumps(res["spec_preview"])  # must not raise
        json.dumps(s.plots[res["plot_id"]]["spec"])  # full spec stays JSON-safe in session
    finally:
        asyncio.run(m.aclose())
