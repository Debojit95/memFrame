"""Phase-3 data-path regression tests: cache signatures, CSV retry, plot cap."""

import asyncio

import pandas as pd
import pytest

from memframe.cache.cache_manager import CacheManager
from memframe.main import MemFrame


# ── cache signature: content hash prevents same-shape collisions ─────────


def test_signature_distinguishes_same_shape_frames():
    m = CacheManager()
    a = m._signature((pd.DataFrame({"x": [1, 2, 3]}),))
    b = m._signature((pd.DataFrame({"x": [9, 9, 9]}),))
    assert a != b


def test_signature_stable_for_equal_frames():
    m = CacheManager()
    a = m._signature((pd.DataFrame({"x": [1, 2, 3]}),))
    b = m._signature((pd.DataFrame({"x": [1, 2, 3]}),))
    assert a == b


def test_signature_distinguishes_different_index():
    m = CacheManager()
    a = m._signature((pd.DataFrame({"x": [1, 2]}, index=[0, 1]),))
    b = m._signature((pd.DataFrame({"x": [1, 2]}, index=[1, 0]),))
    assert a != b


def test_series_signature_includes_content():
    m = CacheManager()
    a = m._signature((pd.Series([1, 2], name="s"),))
    b = m._signature((pd.Series([2, 1], name="s"),))
    assert a != b
    assert m._signature((pd.Series([1, 2], name="s"),)) == a


def test_unhashable_content_falls_back_gracefully():
    m = CacheManager()
    df = pd.DataFrame({"x": [[1], [2], [3]]})  # list cells: no stable hash
    sig = m._signature((df,))
    assert "__dataframe__" in sig  # shape-only fallback, no crash


# ── CSV typed-stream retry must not re-insert flushed chunks ─────────────


def test_csv_retry_does_not_duplicate_flushed_chunks(tmp_path):
    # Bad value sits at row 55000: after the first 50k chunk is flushed the
    # parse fails, the column widens, and the file is re-read. Without the
    # flush-skip, the re-read re-inserts the first 50k rows (~110k total).
    path = tmp_path / "big.csv"
    with open(path, "w") as fh:
        fh.write("num,label\n")
        for i in range(60000):
            num = "__-333333333333333333333333333__" if i == 55000 else str(i % 100)
            fh.write(f"{num},r{i}\n")

    m = MemFrame(connection_type="local", connection_params={"db_path": ":memory:"})
    try:
        asyncio.run(m.aconnect())
        ctx = asyncio.run(m.aupload_csv(str(path)))
        table, schema = asyncio.run(ctx._get_active_context())
        count = asyncio.run(
            m._backend.fetchval(f'SELECT COUNT(*) FROM "{schema}"."{table}"')
        )
        assert count == 60000
    finally:
        asyncio.run(m.aclose())


# ── plot fetch cap ────────────────────────────────────────────────────────


def test_bar_plot_caps_fetched_rows():
    from memframe.utils.plot_renderer import suppress_inline_display

    df = pd.DataFrame(
        {"cat": [f"c{i % 7}" for i in range(10050)], "v": list(range(10050))}
    )
    m = MemFrame(connection_type="local", connection_params={"db_path": ":memory:"})
    try:
        asyncio.run(m.aconnect())
        ctx = asyncio.run(m.aupload_df(df, filename="cap"))
        with suppress_inline_display():
            fig = ctx.bar(x="cat", y="v")  # wrapper exposes a sync form
        total_points = sum(len(trace.x) for trace in fig.data)
        assert 0 < total_points <= 10000
    finally:
        asyncio.run(m.aclose())
