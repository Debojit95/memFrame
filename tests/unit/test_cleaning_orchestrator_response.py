import asyncio

import pandas as pd
import pytest

from memframe.main import MemFrame
from memframe.wrappers.analytix.cleaning import CleaningWrapper
from memframe.core.orchestrator.analytix.groupby_fillna import GroupByFillnaOrchestrator


@pytest.fixture
def cleaning_context():
    memframe = MemFrame(
        connection_type="local",
        connection_params={"db_path": ":memory:"},
    )
    asyncio.run(memframe.aconnect())
    try:
        yield memframe.upload_df(
            pd.DataFrame({"category": ["a", "b"]}),
            filename="cleaning_orchestrator_response",
        )
    finally:
        asyncio.run(memframe.aclose())


def test_cleaning_validation_error_is_canonical(cleaning_context):
    response = CleaningWrapper(cleaning_context).fillna("category", method="mean")

    assert response["is_error"] is True
    assert response["error_message"]
    assert response["involved_cols"] == ["category"]
    assert response["result"] is None


def test_groupby_validation_error_is_canonical(cleaning_context):
    response = asyncio.run(GroupByFillnaOrchestrator(cleaning_context).groupby_fillna("category", group_cols=[]))

    assert response["is_error"] is True
    assert response["error_message"] == "group_cols must be provided for groupby_fillna"
    assert response["result"] is None


def test_groupby_fillna_repeat_keeps_base_table():
    mf = MemFrame(
        connection_type="local", connection_params={"db_path": ":memory:"}
    )
    asyncio.run(mf.aconnect())
    try:
        ctx = mf.upload_df(
            pd.DataFrame({"segment": ["a", "a", "b"], "z": [1.0, None, 3.0]}),
            filename="groupby_fillna_repeat",
        )
        did = ctx._data_id

        first = ctx.groupby("segment").fillna("z", method="mean")
        second = ctx.groupby("segment").fillna("z", method="mean")
        assert first["is_error"] is False
        assert second["is_error"] is False

        uploads = asyncio.run(mf._backend.fetch(
            "SELECT table_name FROM information_schema.tables "
            "WHERE table_schema = 'memframe_upload'"
        ))
        names = {r[0] for r in uploads}
        assert f"{did}__base" in names
        assert did in names

        cache_tables = asyncio.run(mf._backend.fetch(
            "SELECT table_name FROM information_schema.tables "
            "WHERE table_schema = 'memframe_cache'"
        ))
        assert cache_tables == []

        row = asyncio.run(mf._backend.fetch_row(
            f"SELECT cache_kind, ref_table, owns_table "
            f"FROM {mf._backend.cache_registry_table} "
            f"WHERE data_id = {mf._backend.placeholder(1)} "
            "AND operation_type = 'method_call' ORDER BY opidx DESC LIMIT 1",
            did,
        ))
        assert row[0] == "ref"
        assert row[1] == f"{did}__base"
        assert not row[2]
    finally:
        asyncio.run(mf.aclose())
