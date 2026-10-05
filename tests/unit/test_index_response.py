import asyncio

import pandas as pd
import pytest

from memframe.core.orchestrator.analytix.index import IndexOrchestrator
from memframe.main import MemFrame


@pytest.fixture
def index_context():
    memframe = MemFrame(
        connection_type="local",
        connection_params={"db_path": ":memory:"},
    )
    asyncio.run(memframe.aconnect())
    try:
        yield memframe.upload_df(
            pd.DataFrame(
                {
                    "month": [1, 4, 7, 10],
                    "year": [2012, 2014, 2013, 2014],
                    "sale": [55, 40, 84, 31],
                }
            ),
            filename="index_response",
        )
    finally:
        asyncio.run(memframe.aclose())


def _orch(ctx):
    return IndexOrchestrator(ctx)


def test_set_and_get_index(index_context):
    orch = _orch(index_context)
    response = asyncio.run(orch.set_index("month"))
    assert response["is_error"] is False
    assert response["index_cols"] == ["month"]

    response = asyncio.run(orch.get_index())
    assert response["is_error"] is False
    assert response["result"] == {
        "index_cols": ["month"],
        "synthetic": False,
        "values": [1, 4, 7, 10],
    }


def test_synthetic_index_without_set(index_context):
    response = asyncio.run(_orch(index_context).get_index())
    assert response["is_error"] is False
    assert response["result"]["synthetic"] is True
    assert response["result"]["values"] == [0, 1, 2, 3]


def test_set_index_unknown_column(index_context):
    response = asyncio.run(_orch(index_context).set_index("nope"))
    assert response["is_error"] is True
    assert "not found" in response["error_message"]


def test_reindex_plain_and_fill(index_context):
    orch = _orch(index_context)
    asyncio.run(orch.set_index("month"))

    response = asyncio.run(orch.reindex([1, 2, 4]))
    assert response["is_error"] is False
    assert response["result"]["month"].tolist() == [1, 2, 4]
    assert response["result"]["sale"].isna().tolist() == [False, True, False]

    response = asyncio.run(orch.reindex([1, 2, 4], fill_value=0))
    assert response["is_error"] is False
    assert response["result"]["sale"].tolist() == [55, 0, 40]


def test_reindex_columns(index_context):
    orch = _orch(index_context)
    asyncio.run(orch.set_index("month"))
    response = asyncio.run(orch.reindex(columns=["sale", "nope"], fill_value=-1))
    assert response["is_error"] is False
    assert list(response["result"].columns) == ["sale", "nope"]
    assert (response["result"]["nope"] == -1).all()


def test_reindex_ffill_bfill_limit(index_context):
    orch = _orch(index_context)
    asyncio.run(orch.set_index("month"))

    response = asyncio.run(orch.reindex([1, 2, 3, 4, 7], method="ffill"))
    assert response["is_error"] is False
    assert response["result"]["sale"].tolist() == [55, 55, 55, 40, 84]

    response = asyncio.run(orch.reindex([1, 2, 3, 4, 7], method="ffill", limit=1))
    assert response["is_error"] is False
    assert response["result"]["sale"].isna().tolist() == [False, False, True, False, False]

    response = asyncio.run(orch.reindex([1, 2, 3, 4, 7], method="bfill"))
    assert response["is_error"] is False
    assert response["result"]["sale"].tolist() == [55, 40, 40, 40, 84]


def test_reindex_nearest(index_context):
    orch = _orch(index_context)
    asyncio.run(orch.set_index("month"))
    response = asyncio.run(orch.reindex([2, 9], method="nearest"))
    assert response["is_error"] is False
    assert response["result"]["sale"].tolist() == [55, 31]


def test_reindex_method_guards(index_context):
    orch = _orch(index_context)
    asyncio.run(orch.set_index("month"))

    response = asyncio.run(orch.reindex([4, 1, 7], method="ffill"))
    assert response["is_error"] is True
    assert "monotonic" in response["error_message"]

    response = asyncio.run(orch.reindex([1, 2], method="nope"))
    assert response["is_error"] is True
    assert "method" in response["error_message"]

    response = asyncio.run(orch.reindex([1, 2], limit=1))
    assert response["is_error"] is True
    assert "limit" in response["error_message"]


def test_multi_index_round_trip(index_context):
    orch = _orch(index_context)
    response = asyncio.run(orch.set_index(["year", "month"]))
    assert response["is_error"] is False

    response = asyncio.run(orch.get_index())
    assert response["result"]["values"] == [(2012, 1), (2013, 7), (2014, 4), (2014, 10)]

    response = asyncio.run(orch.reindex([(2012, 1), (2014, 5), (2014, 10)], fill_value=0))
    assert response["is_error"] is False
    assert response["result"]["sale"].tolist() == [55, 0, 31]

    response = asyncio.run(orch.reindex([(2012, 1), 5], fill_value=0))
    assert response["is_error"] is True
    assert "width" in response["error_message"]


def test_reset_index_level(index_context):
    orch = _orch(index_context)
    asyncio.run(orch.set_index(["year", "month"]))

    response = asyncio.run(orch.reset_index(level="year"))
    assert response["is_error"] is False
    assert response["result"]["index_cols"] == ["month"]

    response = asyncio.run(orch.reset_index())
    assert response["is_error"] is False
    assert response["result"]["index_cols"] == []

    response = asyncio.run(orch.reindex([1, 2]))
    assert response["is_error"] is True
    assert "set_index" in response["error_message"]


def test_reindex_like(index_context):
    memframe = index_context.memframe
    other = memframe.upload_df(
        pd.DataFrame({"month": [4, 7], "sale": [400, 700]}),
        filename="index_other",
    )
    asyncio.run(_orch(other).set_index("month"))

    orch = _orch(index_context)
    asyncio.run(orch.set_index("month"))
    response = asyncio.run(orch.reindex_like(other._data_id, fill_value=-1))
    assert response["is_error"] is False
    assert response["result"]["sale"].tolist() == [40, 84]
