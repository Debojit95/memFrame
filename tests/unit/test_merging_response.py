"""Behavioral unit tests for the merge/join/concat wrapper stack.

Runs against an in-memory DuckDB. Successful ``MergeWrapper`` calls return a
live ``ContextManager`` bound to the new output table (chainable); error
envelopes are returned unchanged as dicts. ``ContextManager`` dispatch
(``left.merge(...)``) passes the context through unwrapped.
"""

import asyncio

import pandas as pd
import pytest

from memframe.core.analytix.merging import make_merge_ops
from memframe.core.orchestrator.analytix.merging import MergeOrchestrator
from memframe.db_manager.context import ContextManager
from memframe.exceptions import DataNotFound
from memframe.main import MemFrame
from memframe.wrappers.analytix.merging import MergeAccessor, MergeWrapper


def _left_df():
    return pd.DataFrame(
        {
            "id": [1, 2, 3],
            "name": ["a", "b", "c"],
            "val": [10, 20, 30],
        }
    )


def _right_df():
    return pd.DataFrame(
        {
            "id": [2, 3, 4],
            "score": [200, 300, 400],
            "val": [1.0, 2.0, 3.0],
        }
    )


def _connect(deep_cache=None):
    mf = MemFrame(
        connection_type="local",
        connection_params={"db_path": ":memory:"},
        deep_cache=deep_cache,
    )
    asyncio.run(mf.aconnect())
    return mf


@pytest.fixture
def merge_contexts():
    mf = _connect()
    try:
        left = mf.upload_df(_left_df(), filename="merge_left")
        right = mf.upload_df(_right_df(), filename="merge_right")
        yield left, right, mf
    finally:
        asyncio.run(mf.aclose())


@pytest.fixture
def merge_nocache_contexts():
    mf = _connect(deep_cache=False)
    try:
        left = mf.upload_df(_left_df(), filename="merge_nocache_left")
        right = mf.upload_df(_right_df(), filename="merge_nocache_right")
        yield left, right, mf
    finally:
        asyncio.run(mf.aclose())


def _frame(ctx):
    """Read the full contents of a dataset context as a DataFrame."""
    df = ctx.full_table()
    assert isinstance(df, pd.DataFrame)
    return df


# ── merge ────────────────────────────────────────────────────────────


def test_merge_inner_returns_context(merge_contexts):
    left, right, _ = merge_contexts
    merged = MergeWrapper(left).merge(right, on="id")
    assert isinstance(merged, ContextManager)
    result = _frame(merged)
    # ponytail: merge suffixes every overlapping column, keys included
    assert set(result["id_x"]) == {2, 3}
    assert "val_x" in result.columns
    assert "val_y" in result.columns


def test_merge_public_api_returns_context(merge_contexts):
    left, right, _ = merge_contexts
    merged = left.merge(right, on="id")
    assert isinstance(merged, ContextManager)
    assert set(_frame(merged)["id_x"]) == {2, 3}


def test_merge_left_right_outer(merge_contexts):
    left, right, _ = merge_contexts
    wrapper = MergeWrapper(left)
    assert set(_frame(wrapper.merge(right, how="left", on="id"))["id_x"]) == {1, 2, 3}
    assert set(_frame(wrapper.merge(right, how="right", on="id"))["id_y"]) == {2, 3, 4}
    outer = _frame(wrapper.merge(right, how="outer", on="id"))
    assert set(outer["id_y"].dropna()) == {2, 3, 4}
    assert set(outer["id_x"].dropna()) == {1, 2, 3}


def test_merge_left_on_right_on(merge_contexts):
    left, right, _ = merge_contexts
    merged = MergeWrapper(left).merge(right, left_on="id", right_on="id")
    assert isinstance(merged, ContextManager)
    assert set(_frame(merged)["id_x"]) == {2, 3}


def test_merge_custom_suffixes(merge_contexts):
    left, right, _ = merge_contexts
    merged = MergeWrapper(left).merge(right, on="id", suffixes=("_l", "_r"))
    assert {"val_l", "val_r"}.issubset(_frame(merged).columns)


def test_merge_anti_joins(merge_contexts):
    left, right, _ = merge_contexts
    wrapper = MergeWrapper(left)
    assert set(_frame(wrapper.merge(right, how="left_anti", on="id"))["id_x"]) == {1}
    assert set(_frame(wrapper.merge(right, how="right_anti", on="id"))["id_y"]) == {4}


def test_merge_unknown_how(merge_contexts):
    left, right, _ = merge_contexts
    response = MergeWrapper(left).merge(right, how="nonsense", on="id")
    assert response["is_error"] is True
    assert "Unsupported join type" in response["error_message"]


def test_merge_missing_keys(merge_contexts):
    left, right, _ = merge_contexts
    response = MergeWrapper(left).merge(right)
    assert response["is_error"] is True
    assert "Must provide 'on'" in response["error_message"]


def test_merge_mismatched_keys(merge_contexts):
    left, right, _ = merge_contexts
    response = MergeWrapper(left).merge(
        right, left_on=["id"], right_on=["id", "val"]
    )
    assert response["is_error"] is True
    assert "match length" in response["error_message"]


def test_merge_bad_chunk_size(merge_contexts):
    left, right, _ = merge_contexts
    response = MergeWrapper(left).merge(right, on="id", chunk_size=0)
    assert response["is_error"] is True
    assert "chunk_size must be > 0" in response["error_message"]


def test_merge_chunk_size_returns_context(merge_contexts):
    # ponytail: chunk_size no longer shapes the public return — the output
    # table is fully materialized either way.
    left, right, _ = merge_contexts
    merged = MergeWrapper(left).merge(right, on="id", chunk_size=2)
    assert isinstance(merged, ContextManager)
    assert set(_frame(merged)["id_x"]) == {2, 3}


def test_merge_cache_replay_returns_context(merge_contexts):
    left, right, _ = merge_contexts
    first = MergeWrapper(left).merge(right, on="id")
    second = MergeWrapper(left).merge(right, on="id")
    assert isinstance(second, ContextManager)
    pd.testing.assert_frame_equal(
        _frame(second).sort_values("id_x").reset_index(drop=True),
        _frame(first).sort_values("id_x").reset_index(drop=True),
    )


def test_merge_chained_as_left(merge_contexts):
    left, right, _ = merge_contexts
    merged = MergeWrapper(left).merge(right, on="id")
    chained = MergeWrapper(merged).merge(right, left_on="id_x", right_on="id")
    assert isinstance(chained, ContextManager)
    assert set(_frame(chained)["id_x"]) == {2, 3}


def test_merge_chained_as_right(merge_contexts):
    left, right, _ = merge_contexts
    merged = MergeWrapper(left).merge(right, on="id")
    chained = MergeWrapper(left).merge(merged, left_on="id", right_on="id_x")
    assert isinstance(chained, ContextManager)
    assert set(_frame(chained)["id_x"]) == {2, 3}


def test_merge_dangling_table_raises(merge_nocache_contexts):
    # ponytail: MemFrame(deep_cache=False) force-drops merge outputs, so the
    # returned context dangles and resolves with a clear error on use.
    left, right, _ = merge_nocache_contexts
    merged = MergeWrapper(left).merge(right, on="id")
    assert isinstance(merged, ContextManager)
    with pytest.raises(DataNotFound):
        merged.full_table()


def test_merge_direct_call_style(merge_contexts):
    left, right, _ = merge_contexts
    merged = MergeWrapper(left)(right, on="id")
    assert isinstance(merged, ContextManager)
    assert set(_frame(merged)["id_x"]) == {2, 3}


# ── join ─────────────────────────────────────────────────────────────


def test_join_returns_context(merge_contexts):
    left, right, _ = merge_contexts
    merged = MergeWrapper(left).join(right, on="id")
    assert isinstance(merged, ContextManager)
    result = _frame(merged)
    assert set(result["id"]) == {1, 2, 3}
    assert "val" in result.columns
    assert "val_right" in result.columns


def test_join_custom_suffixes(merge_contexts):
    left, right, _ = merge_contexts
    merged = MergeWrapper(left).join(right, on="id", lsuffix="_l", rsuffix="_r")
    assert {"val_l", "val_r"}.issubset(_frame(merged).columns)


def test_join_on_common_columns(merge_contexts):
    left, right, _ = merge_contexts
    merged = MergeWrapper(left).join(right)
    assert isinstance(merged, ContextManager)
    assert len(_frame(merged)) >= 1


def test_join_unknown_how(merge_contexts):
    left, right, _ = merge_contexts
    response = MergeWrapper(left).join(right, how="nonsense", on="id")
    assert response["is_error"] is True
    assert "Unsupported join type" in response["error_message"]


# ── concat ───────────────────────────────────────────────────────────


def test_concat_axis0_outer(merge_contexts):
    left, right, _ = merge_contexts
    merged = MergeWrapper(left).concat([right], axis=0, join="outer")
    assert isinstance(merged, ContextManager)
    result = _frame(merged)
    assert len(result) == 6
    assert {"id", "name", "val", "score"}.issubset(result.columns)


def test_concat_axis0_inner(merge_contexts):
    left, right, _ = merge_contexts
    merged = MergeWrapper(left).concat([right], axis=0, join="inner")
    assert isinstance(merged, ContextManager)
    result = _frame(merged)
    assert len(result) == 6
    assert set(result.columns) == {"id", "val"}


def test_concat_axis0_ignore_index(merge_contexts):
    left, right, _ = merge_contexts
    merged = MergeWrapper(left).concat(
        [right], axis=0, join="outer", ignore_index=True
    )
    assert isinstance(merged, ContextManager)
    assert "__index__" in _frame(merged).columns


def test_concat_axis1(merge_contexts):
    left, right, _ = merge_contexts
    merged = MergeWrapper(left).concat([right], axis=1, join="outer")
    assert isinstance(merged, ContextManager)
    result = _frame(merged)
    assert len(result) == 3
    assert "id_1" in result.columns


def test_concat_too_few_tables(merge_contexts):
    left, _, _ = merge_contexts
    response = MergeWrapper(left).concat([], axis=0)
    assert response["is_error"] is True
    assert "at least 2 tables" in response["error_message"]


def test_concat_bad_axis(merge_contexts):
    left, right, _ = merge_contexts
    response = MergeWrapper(left).concat([right], axis=2)
    assert response["is_error"] is True
    assert "axis must be 0 or 1" in response["error_message"]


def test_concat_bad_join(merge_contexts):
    left, right, _ = merge_contexts
    response = MergeWrapper(left).concat([right], join="left")
    assert response["is_error"] is True
    assert "join must be" in response["error_message"]


# ── public API / core failure / orchestrator ─────────────────────────


def test_merge_public_api_error_stays_dict(merge_contexts):
    left, right, _ = merge_contexts
    result = left.merge(right, how="nonsense", on="id")
    assert isinstance(result, dict)
    assert result["is_error"] is True


def test_merge_core_unsupported_backend():
    with pytest.raises(NotImplementedError):
        make_merge_ops(object())


def test_orchestrator_from_context(merge_contexts):
    left, _, mf = merge_contexts
    orchestrator = MergeOrchestrator.from_context(mf, left._data_id)
    assert isinstance(orchestrator, MergeOrchestrator)
    assert orchestrator._data_id == left._data_id


def test_merge_accessor_alias():
    assert MergeAccessor is MergeWrapper
