import pandas as pd
import pytest

from memframe.db_manager.context import ContextManager
from memframe.exceptions import ConfigurationError


def test_dict_creates_upload_dataset(connected_memframe):
    ctx = connected_memframe.DataFrame(
        {"id": [1, 2, 3], "name": ["a", "b", "c"]}
    )

    assert isinstance(ctx, ContextManager)
    assert ctx._data_id
    head = ctx.head(n=10).sort_values("id").reset_index(drop=True)
    assert head["id"].tolist() == [1, 2, 3]
    assert head["name"].tolist() == ["a", "b", "c"]
    assert any(
        row["data_id"] == ctx._data_id
        for row in connected_memframe.list_tables()
    )


def test_dataframe_input_with_columns_and_dtype(connected_memframe):
    frame = pd.DataFrame({"a": [1, 2], "b": [3, 4]})
    ctx = connected_memframe.DataFrame(
        frame, columns=["b", "a"], dtype="float64"
    )

    assert list(ctx.columns) == ["b", "a"]
    assert ctx.head(n=10)["b"].tolist() == [3.0, 4.0]


def test_context_input_copies_to_new_dataset(connected_memframe):
    source = connected_memframe.upload_df(
        pd.DataFrame({"a": [1, 2]}), filename="df_constructor_source"
    )
    copied = connected_memframe.DataFrame(source)

    assert copied._data_id != source._data_id
    assert copied.head(n=10).sort_values("a")["a"].tolist() == [1, 2]
    assert source.head(n=10)["a"].tolist() == [1, 2]


def test_index_alignment_is_not_persisted(connected_memframe):
    ctx = connected_memframe.DataFrame(
        {"a": pd.Series([2, 3], index=[2, 3])},
        index=[0, 1, 2, 3],
    )

    values = ctx.head(n=10)["a"].tolist()
    assert sum(pd.isna(value) for value in values) == 2
    assert sorted(value for value in values if not pd.isna(value)) == [2, 3]
    assert "index" not in ctx.columns


def test_filename_and_dtypes_extensions(connected_memframe):
    ctx = connected_memframe.DataFrame(
        {"id": [1, 2]},
        filename="constructed.csv",
        dtypes={"id": "BIGINT"},
    )

    assert any(
        row["filename"] == "constructed.csv"
        for row in connected_memframe.list_tables()
    )
    assert ctx.head(n=10)["id"].tolist() == [1, 2]


def test_empty_input_raises(connected_memframe):
    with pytest.raises(ConfigurationError):
        connected_memframe.DataFrame(None)
    with pytest.raises(ConfigurationError):
        connected_memframe.DataFrame(pd.DataFrame())
