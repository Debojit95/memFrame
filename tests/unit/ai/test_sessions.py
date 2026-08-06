import asyncio

import pandas as pd
import pytest

from memframe.main import MemFrame
from memframe_ai.config import AISettings
from memframe_ai.sessions import store


@pytest.fixture
def uploaded():
    m = MemFrame(connection_type="local", connection_params={"db_path": ":memory:"})
    asyncio.run(m.aconnect())
    df = pd.DataFrame({"name": ["a", "b", "a"], "age": [20, 30, 25]})
    ops = asyncio.run(m.aupload_df(df, filename="demo"))
    settings = AISettings(api_key="k")
    try:
        yield ops, settings
    finally:
        asyncio.run(m.aclose())


def test_create_and_resolve_active_table(uploaded):
    ops, settings = uploaded
    session = store.create("s1", ops=ops, settings=settings)
    try:
        asyncio.run(session.ensure())
        assert session.table is not None
        assert session.schema == "upload"
        assert session.adapter is not None
        assert store.get("s1") is session
    finally:
        store.drop("s1")


def test_advance_table_to_transient(uploaded):
    ops, settings = uploaded
    session = store.create("s2", ops=ops, settings=settings)
    try:
        asyncio.run(session.ensure())
        base = session.table
        asyncio.run(session.advance_table(base + "__op_9"))
        assert session.table == base + "__op_9"
    finally:
        store.drop("s2")


def test_missing_active_dataset_raises():
    m = MemFrame(connection_type="local", connection_params={"db_path": ":memory:"})
    asyncio.run(m.aconnect())
    try:
        ops = m._ops()
        session = store.create("s3", ops=ops, settings=AISettings(api_key="k"))
        with pytest.raises(Exception):
            asyncio.run(session.ensure())
    finally:
        asyncio.run(m.aclose())
