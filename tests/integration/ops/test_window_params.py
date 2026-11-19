"""Backend-parametrized integration matrix for the rolling/expanding params."""

from __future__ import annotations

import itertools

import numpy as np
import pandas as pd
import pytest

import _window_helpers as h
from memframe.wrappers.analytix.window import WindowWrapper

WIN_TYPES = [
    "boxcar", "triang", "bartlett", "parzen", "bohman", "blackman",
    "blackmanharris", "barthann", "nuttall", "hann", "hamming", "cosine",
]

_counter = itertools.count()


def _wrapper(memframe, frame, tag):
    ctx = memframe.upload_df(frame, filename=f"{tag}{next(_counter)}")
    return WindowWrapper(ctx)


def test_rolling_min_periods_matrix(connected_memframe, backend_config):
    frame = h.numeric_df()
    for func in ("sum", "mean", "min", "max", "count", "std", "var"):
        for mp in (None, 0, 1, 3):
            wrapper = _wrapper(connected_memframe, frame, "mp")
            kwargs = {} if mp is None else {"min_periods": mp}
            response = wrapper.rolling(
                column="x", window=3, func=func, order_by="o", **kwargs
            )
            got = h.result_of(response)
            expected = h.pandas_row(
                frame, 3, func, min_periods=3 if mp is None else mp
            )
            h.assert_values(got[h.column_of(response)], expected)


@pytest.mark.parametrize("mp", [None, 1])
def test_rolling_first_last_rows(connected_memframe, backend_config, mp):
    frame = h.numeric_df()
    ordered = frame.sort_values("o", kind="mergesort")
    for func in ("first", "last"):
        wrapper = _wrapper(connected_memframe, frame, "fl")
        kwargs = {} if mp is None else {"min_periods": mp}
        response = wrapper.rolling(
            column="x", window=3, func=func, order_by="o", **kwargs
        )
        got = h.result_of(response)
        expected = h.manual_first_last(ordered["x"], 3, func, mp)
        h.assert_values(got[h.column_of(response)], expected)


def test_rolling_min_periods_above_window_is_rejected(
    connected_memframe, backend_config
):
    wrapper = _wrapper(connected_memframe, h.numeric_df(), "mperr")
    response = wrapper.rolling(
        column="x", window=3, func="mean", order_by="o", min_periods=4
    )
    assert "min_periods" in h.error_text(response)


def test_rolling_center_closed_matrix(connected_memframe, backend_config):
    frame = h.numeric_df()
    for func in ("sum", "mean", "count", "std", "var"):
        wrapper = _wrapper(connected_memframe, frame, "cc")
        for closed in (None, "right", "left", "both", "neither"):
            for center in (False, True):
                kwargs = {"center": center}
                if closed:
                    kwargs["closed"] = closed
                response = wrapper.rolling(
                    column="x", window=4, func=func, order_by="o",
                    min_periods=1, **kwargs,
                )
                got = h.result_of(response)
                expected = h.pandas_row(
                    frame, 4, func, min_periods=1, **kwargs,
                )
                h.assert_values(got[h.column_of(response)], expected)


def test_rolling_center_closed_specials(connected_memframe, backend_config):
    frame = h.numeric_df()
    for func in ("median", "sem", "nunique", "skew", "kurt"):
        wrapper = _wrapper(connected_memframe, frame, "ccs")
        for closed in ("right", "left", "both", "neither"):
            response = wrapper.rolling(
                column="x", window=4, func=func, order_by="o",
                min_periods=1, closed=closed,
            )
            got = h.result_of(response)
            if func == "nunique":
                expected = h.pandas_row_apply(
                    frame, 4, lambda v: v.nunique(),
                    min_periods=1, closed=closed,
                )
            else:
                expected = h.pandas_row(
                    frame, 4, func, min_periods=1, closed=closed
                )
            h.assert_values(
                got[h.column_of(response)], expected, atol=1e-9
            )


def test_rolling_rank_params(connected_memframe, backend_config):
    frame = h.numeric_df()
    wrapper = _wrapper(connected_memframe, frame, "rank")
    for method in ("average", "min", "max"):
        for ascending in (True, False):
            for pct in (False, True):
                response = wrapper.rolling(
                    column="x", window=3, func="rank", order_by="o",
                    min_periods=1,
                    func_kwargs={"rank": {
                        "method": method,
                        "ascending": ascending,
                        "pct": pct,
                    }},
                )
                got = h.result_of(response)
                expected = h.pandas_row_rank(
                    frame, 3, method, ascending, pct, min_periods=1,
                )
                h.assert_values(got[h.column_of(response)], expected)


def test_rolling_rank_first_and_dense(connected_memframe, backend_config):
    frame = h.numeric_df()
    ordered = frame.sort_values("o", kind="mergesort")
    wrapper = _wrapper(connected_memframe, frame, "rankfd")
    for method in ("first", "dense"):
        response = wrapper.rolling(
            column="x", window=3, func="rank", order_by="o",
            min_periods=1,
            func_kwargs={"rank": {"method": method}},
        )
        got = h.result_of(response)
        expected = h.manual_rank(ordered["x"], 3, method)
        h.assert_values(got[h.column_of(response)], expected)


def test_quantile_interpolation_matrix(connected_memframe, backend_config):
    frame = h.numeric_df()
    for interpolation in ("linear", "lower", "higher", "nearest", "midpoint"):
        wrapper = _wrapper(connected_memframe, frame, "qi")
        for q in (0.1, 0.25, 0.5, 0.75, 0.9):
            for window in (1, 2, 3):
                response = wrapper.rolling(
                    column="x", window=window, func="quantile", q=q,
                    order_by="o", min_periods=1,
                    func_kwargs={"quantile": {
                        "interpolation": interpolation,
                    }},
                )
                got = h.result_of(response)
                expected = h.pandas_row_quantile(
                    frame, window, q, interpolation, min_periods=1,
                )
                h.assert_values(got[h.column_of(response)], expected)


def test_quantile_interpolation_edgecases(connected_memframe, backend_config):
    frame = h.numeric_df()
    wrapper = _wrapper(connected_memframe, frame, "qie")

    # integer position: every mode equals the linear result
    values = {}
    for interpolation in ("linear", "lower", "higher", "nearest", "midpoint"):
        response = wrapper.rolling(
            column="x", window=3, func="quantile", q=0.5, order_by="o",
            min_periods=3,
            func_kwargs={"quantile": {"interpolation": interpolation}},
        )
        got = h.result_of(response)
        values[interpolation] = got[h.column_of(response)]
    for interpolation in ("lower", "higher", "nearest", "midpoint"):
        h.assert_values(values[interpolation], values["linear"])

    # nearest half-to-even ties (n = 3): q=0.25 -> lower, q=0.75 -> higher
    for q in (0.25, 0.75):
        wrapper = _wrapper(connected_memframe, frame, "qie2")
        response = wrapper.rolling(
            column="x", window=3, func="quantile", q=q, order_by="o",
            min_periods=3,
            func_kwargs={"quantile": {"interpolation": "nearest"}},
        )
        got = h.result_of(response)
        expected = h.pandas_row_quantile(
            frame, 3, q, "nearest", min_periods=3,
        )
        h.assert_values(got[h.column_of(response)], expected)

    # q = 0 / 1 -> min / max (pandas rejects the endpoints for rolling quantile)
    for q, func in ((0.0, "min"), (1.0, "max")):
        wrapper = _wrapper(connected_memframe, frame, "qie3")
        response = wrapper.rolling(
            column="x", window=4, func="quantile", q=q, order_by="o",
            min_periods=1,
            func_kwargs={"quantile": {"interpolation": "lower"}},
        )
        got = h.result_of(response)
        expected = h.pandas_row(frame, 4, func, min_periods=1)
        h.assert_values(got[h.column_of(response)], expected)


def test_interpolation_rejects_unknown_mode(connected_memframe, backend_config):
    wrapper = _wrapper(connected_memframe, h.numeric_df(), "qierr")
    response = wrapper.rolling(
        column="x", window=3, func="quantile", order_by="o",
        func_kwargs={"quantile": {"interpolation": "bogus"}},
    )
    assert "interpolation" in h.error_text(response)


def test_normalised_median_paths(connected_memframe, backend_config):
    frame = h.numeric_df()
    wrapper = _wrapper(connected_memframe, frame, "med")

    response = wrapper.rolling(
        column="x", window=4, func="median", order_by="o"
    )
    got = h.result_of(response)
    h.assert_values(
        got[h.column_of(response)], h.pandas_row(frame, 4, "median")
    )

    response = wrapper.expanding(
        column="x", func="median", order_by="o", min_periods=1
    )
    got = h.result_of(response)
    h.assert_values(
        got[h.column_of(response)],
        h.pandas_expanding_row(frame, "median", min_periods=1),
    )

    response = wrapper.rolling(
        column="x", window="3D", func="median", order_by="t", min_periods=1
    )
    got = h.result_of(response, by="t")
    h.assert_values(
        got[h.column_of(response)],
        h.pandas_time_oracle(frame, "x", "3D", "median", min_periods=1),
    )


def test_skew_kurt_edges(connected_memframe, backend_config):
    constant = pd.DataFrame({"x": [5.0, 5.0, 5.0, 5.0], "o": [1, 2, 3, 4]})
    wrapper = _wrapper(connected_memframe, constant, "skc")
    for func, window in (("skew", 3), ("kurt", 4)):
        response = wrapper.rolling(
            column="x", window=window, func=func, order_by="o",
            min_periods=1,
        )
        got = h.result_of(response)
        h.assert_values(
            got[h.column_of(response)],
            h.pandas_row(constant, window, func, min_periods=1),
            atol=1e-9,
        )

    near_zero = pd.DataFrame(
        {"x": [1.0, 1.0 + 1e-9, 1.0 - 1e-9], "o": [1, 2, 3]}
    )
    wrapper = _wrapper(connected_memframe, near_zero, "skz")
    response = wrapper.rolling(
        column="x", window=3, func="skew", order_by="o", min_periods=3
    )
    got = h.result_of(response)
    h.assert_values(
        got[h.column_of(response)],
        h.pandas_row(near_zero, 3, "skew", min_periods=3),
        atol=1e-9,
    )

    frame = h.numeric_df()
    wrapper = _wrapper(connected_memframe, frame, "ske")
    for func, window in (("skew", 3), ("kurt", 4)):
        response = wrapper.rolling(
            column="x", window=window, func=func, order_by="o",
            min_periods=1,
        )
        got = h.result_of(response)
        h.assert_values(
            got[h.column_of(response)],
            h.pandas_row(frame, window, func, min_periods=1),
            atol=1e-9,
        )


def test_win_type_sum_mean_sweep(connected_memframe, backend_config):
    frame = h.numeric_df()
    for win_type in WIN_TYPES:
        wrapper = _wrapper(connected_memframe, frame, "wt")
        for func in ("sum", "mean"):
            response = wrapper.rolling(
                column="x", window=4, func=func, order_by="o",
                min_periods=1, win_type=win_type,
            )
            got = h.result_of(response)
            expected = h.pandas_row(
                frame, 4, func, min_periods=1, win_type=win_type
            )
            h.assert_values(got[h.column_of(response)], expected, atol=1e-9)


def test_win_type_rejections(connected_memframe, backend_config):
    frame = h.numeric_df()
    cases = [
        ({"func": "std", "win_type": "hann"}, "win_type"),
        ({"func": "var", "win_type": "hann"}, "win_type"),
        ({"func": "mean", "win_type": "gaussian"}, "win_type"),
        ({"func": "mean", "win_type": "hann", "center": True}, "win_type"),
        ({"func": "mean", "win_type": "hann", "closed": "left"}, "win_type"),
        ({"func": "mean", "win_type": "hann", "window": "3D"}, "time-based"),
        ({"func": "mean", "win_type": "hann", "window": 513}, "512"),
    ]
    for kwargs, needle in cases:
        wrapper = _wrapper(connected_memframe, frame, "wterr")
        call = {"column": "x", "window": 4, "order_by": "o"}
        call.update(kwargs)
        response = wrapper.rolling(**call)
        assert needle in h.error_text(response)


def test_time_window_matrix(connected_memframe, backend_config):
    frame = h.numeric_df()
    wrapper = _wrapper(connected_memframe, frame, "tw")
    for window, pandas_window in (
        ("3D", "3D"),
        ("36h", "36h"),
        (pd.Timedelta(days=2), "2D"),
    ):
        for func in ("sum", "mean", "min", "max", "count", "std", "var"):
            response = wrapper.rolling(
                column="x", window=window, func=func, order_by="t"
            )
            got = h.result_of(response, by="t")
            expected = h.pandas_time_oracle(
                frame, "x", pandas_window, func
            )
            h.assert_values(got[h.column_of(response)], expected)


def test_time_window_closed_center(connected_memframe, backend_config):
    frame = h.numeric_df()
    wrapper = _wrapper(connected_memframe, frame, "twcc")
    for func in ("sum", "mean", "count"):
        for closed in (None, "right", "left", "both", "neither"):
            for center in (False, True):
                kwargs = {"center": center}
                if closed:
                    kwargs["closed"] = closed
                response = wrapper.rolling(
                    column="x", window="2D", func=func, order_by="t",
                    min_periods=1, **kwargs,
                )
                got = h.result_of(response, by="t")
                expected = h.pandas_time_oracle(
                    frame, "x", "2D", func, min_periods=1, **kwargs
                )
                h.assert_values(got[h.column_of(response)], expected)


def test_time_window_specials(connected_memframe, backend_config):
    frame = h.numeric_df()
    wrapper = _wrapper(connected_memframe, frame, "tws")
    for func in ("quantile", "median", "sem", "rank", "skew", "kurt"):
        response = wrapper.rolling(
            column="x", window="3D", func=func, order_by="t",
            min_periods=1,
        )
        got = h.result_of(response, by="t")
        if func == "quantile":
            expected = h.pandas_time_oracle(
                frame, "x", "3D", "median", min_periods=1
            )
        else:
            expected = h.pandas_time_oracle(
                frame, "x", "3D", func, min_periods=1
            )
        h.assert_values(
            got[h.column_of(response)], expected, atol=1e-9
        )

    response = wrapper.rolling(
        column="x", window="3D", func="nunique", order_by="t", min_periods=1
    )
    got = h.result_of(response, by="t")
    expected = h.pandas_time_apply(
        frame, "x", "3D", lambda v: v.nunique(), min_periods=1
    )
    h.assert_values(got[h.column_of(response)], expected)


def test_time_window_first_last_types(connected_memframe, backend_config):
    frame = h.numeric_df()
    frame["d"] = frame["t"]
    wrapper = _wrapper(connected_memframe, frame, "twfl")
    for func in ("first", "last"):
        expected = h.manual_time_first_last(frame, func, "3D", col="d")
        response = wrapper.rolling(
            column="d", window="3D", func=func, order_by="t", min_periods=1
        )
        got = h.result_of(response, by="t")
        column = h.column_of(response)
        assert pd.api.types.is_datetime64_any_dtype(got[column])
        h.assert_datetimes(got[column], expected)

        expected = h.manual_time_first_last(frame, func, "3D", col="s")
        response = wrapper.rolling(
            column="s", window="3D", func=func, order_by="t", min_periods=1
        )
        got = h.result_of(response, by="t")
        h.assert_texts(got[h.column_of(response)], expected)


def test_time_window_errors(connected_memframe, backend_config):
    frame = h.numeric_df()
    cases = [
        ({"func": "mean", "order_by": None}, "order_by"),
        ({"func": "mean", "order_by": ["t"]}, "single datetime"),
        ({"func": "mean", "order_by": "x"}, "datetime"),
        ({"func": "mean", "order_by": "t", "win_type": "hann"}, "time-based"),
    ]
    for kwargs, needle in cases:
        wrapper = _wrapper(connected_memframe, frame, "twerr")
        call = {"column": "x", "window": "3D"}
        call.update(kwargs)
        response = wrapper.rolling(**call)
        assert needle in h.error_text(response)


def test_dtype_routing_numeric(connected_memframe, backend_config):
    frame = h.numeric_df()
    wrapper = _wrapper(connected_memframe, frame, "dtn")
    for func in (
        "sum", "mean", "min", "max", "count", "std", "var", "quantile",
        "median", "sem", "rank", "skew", "kurt", "nunique", "first", "last",
    ):
        response = wrapper.rolling(
            column="x", window=4, func=func, order_by="o", min_periods=1
        )
        h.assert_ok(response)
        got = h.result_of(response)
        assert h.column_of(response) in got.columns


def test_dtype_routing_datetime(connected_memframe, backend_config):
    frame = h.numeric_df()
    wrapper = _wrapper(connected_memframe, frame, "dtd")
    for func in (
        "min", "max", "mean", "median", "mode", "count", "nunique",
        "rank", "first", "last",
    ):
        response = wrapper.rolling(
            column="t", window=4, func=func, order_by="o", min_periods=1
        )
        h.assert_ok(response)
        got = h.result_of(response)
        column = h.column_of(response)
        if func in ("min", "max", "mean", "median", "mode", "first", "last"):
            assert pd.api.types.is_datetime64_any_dtype(got[column]), func

    response = wrapper.rolling(
        column="t", window=4, func=["max", "sum", "std", "var", "quantile",
                                    "sem", "skew", "kurt"],
        order_by="o", min_periods=1,
    )
    assert response["is_partial"] is True
    skipped = response["skipped_funcs"]
    for name in ("sum", "std", "var", "quantile", "sem", "skew", "kurt"):
        assert name in skipped


def test_dtype_routing_text(connected_memframe, backend_config):
    frame = h.numeric_df()
    wrapper = _wrapper(connected_memframe, frame, "dtt")
    for func in ("min", "max", "count", "nunique", "rank", "first", "last"):
        response = wrapper.rolling(
            column="s", window=3, func=func, order_by="o", min_periods=1
        )
        h.assert_ok(response)

    response = wrapper.rolling(
        column="s", window=3,
        func=["mean", "sum", "std", "var", "quantile", "median", "sem",
              "skew", "kurt"],
        order_by="o",
    )
    assert response["is_error"] is True
    assert "categorical" in response["error_message"]


def test_func_kwargs_multi(connected_memframe, backend_config):
    frame = h.numeric_df()
    wrapper = _wrapper(connected_memframe, frame, "fkm")
    response = wrapper.rolling(
        column="x", window=4, func=["std", "quantile", "rank"],
        order_by="o", min_periods=1,
        func_kwargs={
            "std": {"ddof": 0},
            "quantile": {"interpolation": "nearest"},
            "rank": {"method": "max", "pct": True},
        },
    )
    h.assert_ok(response)
    assert response["successful_funcs"] == ["std", "quantile", "rank"]
    got = h.result_of(response)
    columns = response["new_columns"]
    ordered = frame.sort_values("o", kind="mergesort")
    h.assert_values(
        got[columns[0]],
        ordered["x"].rolling(4, min_periods=1).std(ddof=0),
    )
    h.assert_values(
        got[columns[1]],
        h.pandas_row_quantile(frame, 4, 0.5, "nearest", min_periods=1),
    )
    h.assert_values(
        got[columns[2]],
        h.pandas_row_rank(frame, 4, "max", True, True, min_periods=1),
    )


def test_partial_failure_envelope(connected_memframe, backend_config):
    wrapper = _wrapper(connected_memframe, h.numeric_df(), "pfe")
    response = wrapper.rolling(
        column="x", window=3, func=["mean", "frobnicate"], order_by="o"
    )
    assert response["is_error"] is False
    assert response["is_partial"] is True
    assert "frobnicate" in response["skipped_funcs"]
    assert response["successful_funcs"] == ["mean"]


def test_same_column_rerun(connected_memframe, backend_config):
    frame = h.numeric_df()
    wrapper = _wrapper(connected_memframe, frame, "rerun")
    first = wrapper.rolling(
        column="x", window=3, func="sum", order_by="o",
        min_periods=1, closed="right",
    )
    got = h.result_of(first)
    h.assert_values(
        got[h.column_of(first)],
        h.pandas_row(frame, 3, "sum", min_periods=1),
    )

    second = wrapper.rolling(
        column="x", window=3, func="sum", order_by="o",
        min_periods=1, closed="left",
    )
    got = h.result_of(second)
    h.assert_values(
        got[h.column_of(second)],
        h.pandas_row(frame, 3, "sum", min_periods=1, closed="left"),
    )
    generated = [
        c for c in second["result"].columns if c.startswith("x_rolling_sum")
    ]
    assert len(generated) == 1


def test_column_names_and_dtypes(connected_memframe, backend_config):
    frame = h.numeric_df()
    template = "x"

    def run(order_by="o", **kwargs):
        wrapper = _wrapper(connected_memframe, frame, "names")
        return wrapper.rolling(column=template, order_by=order_by, **kwargs)

    response = run(window=3, func="mean")
    assert h.column_of(response) == "x_rolling_mean_w3"
    h.assert_ok(response)

    response = run(window=3, func="quantile", q=0.5)
    assert h.column_of(response) == "x_rolling_q0.5_w3"
    h.assert_ok(response)

    response = run(window=3, func="median")
    assert h.column_of(response) == "x_rolling_median_w3"

    response = run(window=4, func="skew")
    assert h.column_of(response) == "x_rolling_skew_w4"

    response = run(window=4, func="kurt")
    assert h.column_of(response) == "x_rolling_kurt_w4"

    response = run(window="3D", func="mean", order_by="t")
    assert h.column_of(response) == "x_rolling_mean_w259200s"

    wrapper = _wrapper(connected_memframe, frame, "names2")
    response = wrapper.expanding(
        column="x", func="mean", order_by="o", min_periods=1
    )
    assert h.column_of(response) == "x_expanding_mean"

    response = run(window=3, func="count", min_periods=1)
    got = h.result_of(response)
    dtype = got[h.column_of(response)].dtype
    assert np.issubdtype(dtype, np.number)

    response = wrapper.rolling(
        column="t", window=3, func="first", order_by="o", min_periods=1
    )
    got = h.result_of(response)
    assert pd.api.types.is_datetime64_any_dtype(got[h.column_of(response)])
