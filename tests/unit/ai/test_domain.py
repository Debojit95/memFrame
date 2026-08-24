import asyncio

import pandas as pd
import pytest

from memframe.main import MemFrame
from memframe_ai.config import AISettings
from memframe_ai.sessions import store


@pytest.fixture
def session():
    m = MemFrame(connection_type="local", connection_params={"db_path": ":memory:"})
    asyncio.run(m.aconnect())
    df = pd.DataFrame(
        {
            "dt": pd.to_datetime(["2023-01-01 10:00:00", "2024-06-15 23:30:00", "2025-03-01 00:00:00"]),
            "num": [1, 2, 100],
            "cat": ["zoom", "meet", "zoom"],
        }
    )
    ops = asyncio.run(m.aupload_df(df, filename="demo"))
    s = store.create("d1", ops=ops, settings=AISettings(api_key="k"))
    try:
        yield s
    finally:
        asyncio.run(m.aclose())


def test_domain_context_contains_profiles(session):
    ctx = asyncio.run(session.domain_context())
    assert "ACTIVE TABLE CONTEXT" in ctx
    assert "dt [datetime]" in ctx
    assert "RANGE 2023-01-01" in ctx and "2025-03-01" in ctx
    assert "%Y-%m-%d %H:%M:%S" in ctx
    assert "num [numeric]" in ctx
    assert "RANGE 1 to 100" in ctx
    assert "cat [categorical]" in ctx
    assert "'zoom'" in ctx and "'meet'" in ctx


def test_domain_context_contains_preview(session):
    ctx = asyncio.run(session.domain_context())
    assert "DATA PREVIEW (first 5 rows):" in ctx
    assert "zoom" in ctx and "meet" in ctx
    assert "100" in ctx
    assert "| dt" in ctx and "| num" in ctx and "| cat" in ctx
    assert "3 rows shown)" in ctx


def test_domain_context_cached_for_same_table(session):
    first = asyncio.run(session.domain_context())
    second = asyncio.run(session.domain_context())
    assert first == second
    assert first is second


def test_domain_context_force_refresh_rebuilds(session):
    first = asyncio.run(session.domain_context())
    second = asyncio.run(session.domain_context(force_refresh=True))
    assert first == second


def test_domain_context_reflects_advance_table(session):
    asyncio.run(session.ensure())
    adapter = session.adapter
    asyncio.run(adapter.execute(
        'CREATE TABLE "memframe_transient"."other_tbl" (num BIGINT, cat VARCHAR)'
    ))
    asyncio.run(session.advance_table("other_tbl"))
    after = asyncio.run(session.domain_context(force_refresh=True))
    assert "other_tbl" in after
    assert "num [numeric]" in after and "cat [categorical]" in after