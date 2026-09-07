import asyncio

import pandas as pd

from memframe.wrappers.analytix.datetime import DateTimeWrapper
from memframe_ai.sessions import Session
from memframe_ai.tools import inspect as inspect_tools
from memframe_ai.wrappers import SessionWrappers


class _FakeOps:
    """Minimal session stand-in exposing only `.ops` for wrapper binding."""

    def __init__(self):
        self.ops = type("Ops", (), {"memframe": None, "_data_id": None})()


class _RecordingDatetime:
    """Stub DateTimeWrapper recording aresample calls."""

    def __init__(self):
        self.calls = []

    async def aresample(self, **kwargs):
        self.calls.append(kwargs)
        return {
            "is_error": False,
            "message": "resampled",
            "result": pd.DataFrame({"ts": pd.to_datetime(["2023-01-01"]), "value": [2]}),
            "new_table": "stub_table",
        }


class _FakeSession:
    """Session stub satisfying tools.inspect.resample + normalize()."""

    def __init__(self):
        self.wrappers = type("W", (), {})()
        self.wrappers.inspection = None
        self.wrappers.datetime = _RecordingDatetime()
        self.settings = None
        self.results = []
        self.table = "stub_table"

    def add_result(self, df):
        self.results.append(df)

    async def advance_table(self, new_table):
        self.table = new_table


def test_session_wrappers_exposes_datetime():
    wrappers = SessionWrappers.__new__(SessionWrappers)
    SessionWrappers.__init__(wrappers, _FakeOps())
    assert isinstance(wrappers.datetime, DateTimeWrapper)


def test_inspect_resample_tool_routes_to_datetime_wrapper():
    session = _FakeSession()
    fns = {f.__name__: f for f in inspect_tools.tools(session)}
    assert "resample" in fns

    out = asyncio.run(fns["resample"](time_column="ts", rule="D", agg="SUM", value_column="sales"))

    assert out["ok"] is True
    assert session.wrappers.datetime.calls == [
        {
            "column": "ts",
            "freq": "D",
            "agg": "SUM",
            "value_columns": "sales",
            "label": "left",
            "closed": "left",
        }
    ]
    assert session.table == "stub_table"


def test_session_wrappers_property_caches_datetime_binding():
    session = Session.__new__(Session)
    session.ops = type("Ops", (), {"memframe": None, "_data_id": None})()
    first = session.wrappers
    assert isinstance(first.datetime, DateTimeWrapper)
    assert session.wrappers is first
