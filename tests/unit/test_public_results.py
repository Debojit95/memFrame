import asyncio

import pandas as pd
import pytest

from memframe.exceptions import OperationError
from memframe.main import MemFrame


@pytest.fixture
def context():
    memframe = MemFrame(
        connection_type="local",
        connection_params={"db_path": ":memory:"},
    )
    asyncio.run(memframe.aconnect())
    try:
        yield memframe.upload_df(
            pd.DataFrame(
                {
                    "id": [1, 2, 3],
                    "value": [10.0, 20.0, 30.0],
                    "category": ["a", "a", "b"],
                }
            ),
            filename="public_results",
        )
    finally:
        asyncio.run(memframe.aclose())


def test_public_inspection_returns_dataframe(context):
    result = context.head(n=2)

    assert isinstance(result, pd.DataFrame)
    assert result.shape == (2, 3)


def test_public_stats_returns_scalar_and_dict(context):
    assert context.mean("value") == pytest.approx(20.0)
    assert context.proportions("category") == {
        "a": pytest.approx(2 / 3),
        "b": pytest.approx(1 / 3),
    }


def test_public_selection_returns_scalar(context):
    assert context.at(2, "value", index_column="id") == 20


def test_public_streaming_returns_iterator(context):
    iterator = context.full_table(chunk_size=2)

    assert hasattr(iterator, "__aiter__")


def test_public_errors_raise_operation_error(context):
    with pytest.raises(OperationError, match="Unknown column"):
        context.loc("1:2", columns=["missing"])
