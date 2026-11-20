"""Grouped-window integration matrix for the added parameters."""

from __future__ import annotations
if __name__ == "__main__":
    from _bootstrap import direct

    direct(__file__)


import asyncio
import itertools

import numpy as np
import pandas as pd
import pytest

import _window_helpers as h
from memframe.wrappers.analytix.groupby_window import GroupByWindowStatsWrapper

_counter = itertools.count()


def _wrapper(memframe, frame, tag):
    ctx = memframe.upload_df(frame, filename=f"g{tag}{next(_counter)}")
    return GroupByWindowStatsWrapper(ctx)


def _roll(wrapper, **kwargs):
    return asyncio.run(wrapper.arolling(**kwargs))


def _expand(wrapper, **kwargs):
    return asyncio.run(wrapper.aexpanding(**kwargs))


def test_grouped_min_periods_matrix(connected_memframe, backend_config):
    frame = h.grouped_df()
    for func in ("sum", "mean", "count", "std"):
        for mp in (None, 1, 3):
            wrapper = _wrapper(connected_memframe, frame, "mp")
            kwargs = {} if mp is None else {"min_periods": mp}
            response = _roll(
                wrapper, column="x", window=3, func=func, group_cols="g",
                order_by="o", **kwargs,
            )
            got = h.grouped_result_of(response)
            expected = h.pandas_grouped_roll(
                frame, "x", func, window=3,
                min_periods=3 if mp is None else mp,
            )
            h.assert_values(got[h.column_of(response)], expected)


def test_grouped_center_closed(connected_memframe, backend_config):
    frame = h.grouped_df()
    for func in ("sum", "mean"):
        wrapper = _wrapper(connected_memframe, frame, "cc")
        for closed in (None, "right", "left", "both", "neither"):
            for center in (False, True):
                kwargs = {"center": center}
                if closed:
                    kwargs["closed"] = closed
                response = _roll(
                    wrapper, column="x", window=4, func=func,
                    group_cols="g", order_by="o", min_periods=1, **kwargs,
                )
                got = h.grouped_result_of(response)
                expected = h.pandas_grouped_roll(
                    frame, "x", func, window=4, min_periods=1, **kwargs
                )
                h.assert_values(got[h.column_of(response)], expected)


def test_grouped_ddof(connected_memframe, backend_config):
    frame = h.grouped_df()
    for func in ("std", "var"):
        for ddof in (0, 1, 2):
            wrapper = _wrapper(connected_memframe, frame, "dd")
            response = _roll(
                wrapper, column="x", window=4, func=func, group_cols="g",
                order_by="o", min_periods=1,
                func_kwargs={func: {"ddof": ddof}},
            )
            got = h.grouped_result_of(response)
            expected = h.pandas_grouped_roll(
                frame, "x", func, window=4, min_periods=1,
                method_kwargs={"ddof": ddof},
            )
            h.assert_values(got[h.column_of(response)], expected)

            response = _expand(
                wrapper, column="x", func=func, group_cols="g",
                order_by="o", min_periods=1,
                func_kwargs={func: {"ddof": ddof}},
            )
            got = h.grouped_result_of(response)
            expected = h.pandas_grouped_roll(
                frame, "x", func, window=None, min_periods=1,
                method_kwargs={"ddof": ddof},
            )
            h.assert_values(got[h.column_of(response)], expected)


def test_grouped_rank_params(connected_memframe, backend_config):
    frame = h.grouped_df()
    wrapper = _wrapper(connected_memframe, frame, "rk")
    for method in ("average", "min", "max"):
        for ascending in (True, False):
            for pct in (False, True):
                response = _roll(
                    wrapper, column="x", window=3, func="rank",
                    group_cols="g", order_by="o", min_periods=1,
                    func_kwargs={"rank": {
                        "method": method,
                        "ascending": ascending,
                        "pct": pct,
                    }},
                )
                got = h.grouped_result_of(response)
                expected = h.pandas_grouped_roll(
                    frame, "x", "rank", window=3, min_periods=1,
                    method_kwargs={
                        "method": method,
                        "ascending": ascending,
                        "pct": pct,
                    },
                )
                h.assert_values(got[h.column_of(response)], expected)


def test_grouped_interpolation(connected_memframe, backend_config):
    frame = h.grouped_df()
    for interpolation in ("lower", "higher", "nearest", "midpoint"):
        wrapper = _wrapper(connected_memframe, frame, "qi")
        for q in (0.25, 0.5, 0.75):
            response = _roll(
                wrapper, column="x", window=3, func="quantile", q=q,
                group_cols="g", order_by="o", min_periods=1,
                func_kwargs={"quantile": {
                    "interpolation": interpolation,
                }},
            )
            got = h.grouped_result_of(response)
            expected = h.pandas_grouped_quantile(
                frame, "x", 3, q, interpolation, min_periods=1
            )
            h.assert_values(got[h.column_of(response)], expected)


def test_grouped_time_matrix(connected_memframe, backend_config):
    frame = h.grouped_df()
    wrapper = _wrapper(connected_memframe, frame, "tw")
    for window, pandas_window in (("3D", "3D"), ("36h", "36h")):
        for func in ("sum", "mean", "count"):
            response = _roll(
                wrapper, column="x", window=window, func=func,
                group_cols="g", order_by="t",
            )
            got = (
                response["result"]
                .sort_values(["g", "t"], kind="mergesort")
                .reset_index(drop=True)
            )
            expected = h.pandas_grouped_time(
                frame, "x", pandas_window, func
            )
            h.assert_values(got[h.column_of(response)], expected)


def test_grouped_time_closed_center(connected_memframe, backend_config):
    frame = h.grouped_df()
    wrapper = _wrapper(connected_memframe, frame, "twcc")
    for closed in (None, "left", "both"):
        for center in (False, True):
            kwargs = {"center": center}
            if closed:
                kwargs["closed"] = closed
            response = _roll(
                wrapper, column="x", window="2D", func="mean",
                group_cols="g", order_by="t", min_periods=1, **kwargs,
            )
            got = (
                response["result"]
                .sort_values(["g", "t"], kind="mergesort")
                .reset_index(drop=True)
            )
            expected = h.pandas_grouped_time(
                frame, "x", "2D", "mean", min_periods=1, **kwargs
            )
            h.assert_values(got[h.column_of(response)], expected)


def test_grouped_time_quantile_median_skew(connected_memframe, backend_config):
    frame = h.grouped_df()
    wrapper = _wrapper(connected_memframe, frame, "tws")
    response = _roll(
        wrapper, column="x", window="3D", func="quantile", q=0.5,
        group_cols="g", order_by="t", min_periods=1,
        func_kwargs={"quantile": {"interpolation": "nearest"}},
    )
    got = response["result"].sort_values(
        ["g", "t"], kind="mergesort"
    ).reset_index(drop=True)
    expected = h.pandas_grouped_time_quantile(
        frame, "x", "3D", 0.5, "nearest", min_periods=1
    )
    h.assert_values(got[h.column_of(response)], expected)

    for func in ("median", "skew", "kurt"):
        response = _roll(
            wrapper, column="x", window="3D", func=func,
            group_cols="g", order_by="t", min_periods=1,
        )
        got = response["result"].sort_values(
            ["g", "t"], kind="mergesort"
        ).reset_index(drop=True)
        expected = h.pandas_grouped_time(
            frame, "x", "3D", func, min_periods=1
        )
        h.assert_values(
            got[h.column_of(response)], expected, atol=1e-9
        )


def test_grouped_time_first_last_types(connected_memframe, backend_config):
    frame = h.grouped_df()
    frame["d"] = frame["t"]
    wrapper = _wrapper(connected_memframe, frame, "twfl")
    for func in ("first", "last"):
        response = _roll(
            wrapper, column="d", window="3D", func=func,
            group_cols="g", order_by="t", min_periods=1,
        )
        got = response["result"].sort_values(
            ["g", "t"], kind="mergesort"
        ).reset_index(drop=True)
        column = h.column_of(response)
        assert pd.api.types.is_datetime64_any_dtype(got[column])
        h.assert_datetimes(
            got[column],
            h.manual_grouped_time_first_last(frame, func, "3D", col="d"),
        )

        response = _roll(
            wrapper, column="s", window="3D", func=func,
            group_cols="g", order_by="t", min_periods=1,
        )
        got = response["result"].sort_values(
            ["g", "t"], kind="mergesort"
        ).reset_index(drop=True)
        h.assert_texts(
            got[h.column_of(response)],
            h.manual_grouped_time_first_last(frame, func, "3D", col="s"),
        )


def test_grouped_win_type(connected_memframe, backend_config):
    frame = h.grouped_df()
    for win_type in ("triang", "hann", "boxcar"):
        wrapper = _wrapper(connected_memframe, frame, "wt")
        for func in ("sum", "mean"):
            response = _roll(
                wrapper, column="x", window=4, func=func, group_cols="g",
                order_by="o", min_periods=1, win_type=win_type,
            )
            got = h.grouped_result_of(response)
            expected = h.pandas_grouped_roll(
                frame, "x", func, window=4, min_periods=1,
                win_type=win_type,
            )
            h.assert_values(
                got[h.column_of(response)], expected, atol=1e-9
            )


def test_grouped_dtype_routing(connected_memframe, backend_config):
    frame = h.grouped_df()
    wrapper = _wrapper(connected_memframe, frame, "dtr")
    for func in (
        "sum", "mean", "min", "max", "count", "std", "var", "quantile",
        "median", "sem", "rank", "skew", "kurt", "nunique", "first", "last",
    ):
        response = _roll(
            wrapper, column="x", window=4, func=func, group_cols="g",
            order_by="o", min_periods=1,
        )
        h.assert_ok(response)

    for func in ("min", "max", "count", "nunique", "rank", "first", "last"):
        response = _roll(
            wrapper, column="s", window=3, func=func, group_cols="g",
            order_by="o", min_periods=1,
        )
        h.assert_ok(response)

    response = _roll(
        wrapper, column="s", window=3,
        func=["mean", "sum", "std", "var", "quantile", "median", "sem",
              "skew", "kurt"],
        group_cols="g", order_by="o",
    )
    assert response["is_error"] is True


def test_grouped_func_kwargs_multi(connected_memframe, backend_config):
    frame = h.grouped_df()
    wrapper = _wrapper(connected_memframe, frame, "fkm")
    response = _roll(
        wrapper, column="x", window=4, func=["std", "quantile", "rank"],
        group_cols="g", order_by="o", min_periods=1,
        func_kwargs={
            "std": {"ddof": 0},
            "quantile": {"interpolation": "nearest"},
            "rank": {"method": "max", "pct": True},
        },
    )
    h.assert_ok(response)
    assert response["successful_funcs"] == ["std", "quantile", "rank"]
    got = h.grouped_result_of(response)
    columns = response["new_columns"]

    h.assert_values(
        got[columns[0]],
        h.pandas_grouped_roll(
            frame, "x", "std", window=4, min_periods=1,
            method_kwargs={"ddof": 0},
        ),
    )
    h.assert_values(
        got[columns[1]],
        h.pandas_grouped_quantile(
            frame, "x", 4, 0.5, "nearest", min_periods=1
        ),
    )
    h.assert_values(
        got[columns[2]],
        h.pandas_grouped_roll(
            frame, "x", "rank", window=4, min_periods=1,
            method_kwargs={"method": "max", "pct": True},
        ),
    )


def test_grouped_same_column_rerun(connected_memframe, backend_config):
    frame = h.grouped_df()
    wrapper = _wrapper(connected_memframe, frame, "rerun")
    first = _roll(
        wrapper, column="x", window=3, func="sum", group_cols="g",
        order_by="o", min_periods=1, closed="right",
    )
    got = h.grouped_result_of(first)
    h.assert_values(
        got[h.column_of(first)],
        h.pandas_grouped_roll(frame, "x", "sum", window=3, min_periods=1),
    )

    second = _roll(
        wrapper, column="x", window=3, func="sum", group_cols="g",
        order_by="o", min_periods=1, closed="left",
    )
    got = h.grouped_result_of(second)
    h.assert_values(
        got[h.column_of(second)],
        h.pandas_grouped_roll(
            frame, "x", "sum", window=3, min_periods=1, closed="left"
        ),
    )
    generated = [
        c for c in second["result"].columns if c.startswith("x_rolling_sum")
    ]
    assert len(generated) == 1


def test_grouped_multikey_and_no_order(connected_memframe, backend_config):
    frame = h.two_key_grouped_df()
    wrapper = _wrapper(connected_memframe, frame, "mk")
    response = _roll(
        wrapper, column="x", window=3, func="mean", group_cols=["g1", "g2"],
        order_by="o", min_periods=1,
    )
    h.assert_ok(response)
    assert h.column_of(response).endswith("_by_g1_g2")
    got = response["result"].sort_values(
        ["g1", "g2", "o"], kind="mergesort"
    ).reset_index(drop=True)
    expected = h.pandas_grouped_roll(
        frame, "x", "mean", window=3, keys=("g1", "g2"), min_periods=1
    )
    h.assert_values(got[h.column_of(response)], expected)

    # no order_by: natural upload order inside each group
    natural = h.numeric_df()
    natural["g"] = np.where(natural["o"] <= 3, "a", "b")
    wrapper = _wrapper(connected_memframe, natural, "noorder")
    response = _roll(
        wrapper, column="x", window=2, func="mean", group_cols="g",
        min_periods=1,
    )
    h.assert_ok(response)

    expected = natural.assign(__e=np.nan)
    for _, grp in natural.groupby("g", sort=False):
        vals = grp["x"].rolling(2, min_periods=1).mean()
        expected.loc[grp.index, "__e"] = vals.to_numpy()
    # the preview keeps `_mf_idx` (physical upload) order
    h.assert_values(
        response["result"][h.column_of(response)],
        expected["__e"].to_numpy(),
    )

