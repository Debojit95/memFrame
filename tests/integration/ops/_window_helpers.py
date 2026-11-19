"""Shared fixtures and exact-comparison helpers for window integration tests."""

from __future__ import annotations

import numpy as np
import pandas as pd

ORDER = "o"


# ----------------------------------------------------------------------
# Data
# ----------------------------------------------------------------------
def numeric_df() -> pd.DataFrame:
    """8 rows in a deliberately shuffled physical order.

    Ordered by ``o`` the value column is ``[5, None, 3, 1, 8, 8, 8, 3]``:
    a null, two ties (3) and three identical values (8,8,8) so that
    ``skew``/``kurt`` and interpolation modes are distinguishable.
    """
    rows = [
        # x,     o, t,            s
        (1.0, 4, "2024-01-04", None),
        (8.0, 7, "2024-01-06", "d"),
        (5.0, 1, "2024-01-01", "a"),
        (8.0, 5, "2024-01-04", "c"),
        (None, 2, "2024-01-01", "b"),
        (3.0, 8, "2024-01-08", "d"),
        (3.0, 3, "2024-01-02", "b"),
        (8.0, 6, "2024-01-04", "c"),
    ]
    return pd.DataFrame(
        {
            "x": [r[0] for r in rows],
            "o": [r[1] for r in rows],
            "t": pd.to_datetime([r[2] for r in rows]),
            "s": [r[3] for r in rows],
        }
    )


def grouped_df() -> pd.DataFrame:
    """``numeric_df`` split into unequal groups (3 / 5 rows)."""
    frame = numeric_df()
    order = frame["o"].to_numpy()
    frame["g"] = np.where(order <= 3, "a", "b")
    return frame


def two_key_grouped_df() -> pd.DataFrame:
    frame = grouped_df()
    frame["g1"] = frame["g"]
    frame["g2"] = np.where(frame["o"] % 2 == 0, "even", "odd")
    return frame


def window_series_df() -> pd.DataFrame:
    return pd.DataFrame({"x": [10.0, 20.0, 30.0, 40.0, 50.0], "o": [1, 2, 3, 4, 5]})


# ----------------------------------------------------------------------
# Result helpers
# ----------------------------------------------------------------------
def assert_ok(response):
    assert response["is_error"] is False, (
        response.get("failed_funcs") or response.get("error_message")
    )
    assert isinstance(response["result"], pd.DataFrame)


def column_of(response, index: int = 0) -> str:
    cols = response.get("new_columns")
    if not cols:
        cols = [response["new_column"]]
    return cols[index]


def result_of(response, by=ORDER) -> pd.DataFrame:
    assert_ok(response)
    return response["result"].sort_values(by).reset_index(drop=True)


def grouped_result_of(response, keys=("g",)) -> pd.DataFrame:
    assert_ok(response)
    return (
        response["result"]
        .sort_values([*keys, ORDER])
        .reset_index(drop=True)
    )


def floats(values) -> np.ndarray:
    return pd.array(pd.Series(values)).to_numpy(dtype="float64", na_value=np.nan)


def assert_values(actual, expected, *, rtol=1e-9, atol=1e-10):
    a, e = floats(actual), floats(expected)
    assert a.shape == e.shape, f"shape {a.shape} != {e.shape}"
    np.testing.assert_array_equal(
        np.isnan(a), np.isnan(e), err_msg="NaN masks differ"
    )
    mask = ~np.isnan(a)
    np.testing.assert_allclose(a[mask], e[mask], rtol=rtol, atol=atol)


def assert_datetimes(actual, expected):
    a = pd.to_datetime(pd.Series(actual)).to_numpy(dtype="datetime64[ns]")
    e = pd.to_datetime(pd.Series(expected)).to_numpy(dtype="datetime64[ns]")
    np.testing.assert_array_equal(np.isnat(a), np.isnat(e))
    mask = ~np.isnat(a)
    np.testing.assert_array_equal(a[mask].astype("int64"), e[mask].astype("int64"))


def assert_texts(actual, expected):
    def norm(values):
        return [None if pd.isna(v) else str(v) for v in pd.Series(values)]

    assert norm(actual) == norm(expected)


def error_text(response) -> str:
    assert response["is_error"] is True
    failed = response.get("failed_funcs") or {}
    return " ".join(str(v) for v in failed.values()) or str(
        response.get("error_message")
    )


# ----------------------------------------------------------------------
# Oracles
# ----------------------------------------------------------------------
def pandas_roll(series, window, func, **kwargs):
    return getattr(series.rolling(window, **kwargs), func)()


def pandas_row(frame, window, func, value="x", by=ORDER, **kwargs):
    """Row-window oracle aligned to the engine's order-by sequence."""
    ordered = frame.sort_values(by, kind="mergesort")
    return getattr(ordered[value].rolling(window, **kwargs), func)()


def pandas_row_apply(frame, window, fn, value="x", by=ORDER, **kwargs):
    ordered = frame.sort_values(by, kind="mergesort")
    return ordered[value].rolling(window, **kwargs).apply(fn, raw=False)


def pandas_row_quantile(frame, window, q, interpolation, value="x",
                        by=ORDER, **kwargs):
    ordered = frame.sort_values(by, kind="mergesort")
    return ordered[value].rolling(window, **kwargs).quantile(
        q, interpolation=interpolation
    )


def pandas_row_rank(frame, window, method, ascending, pct, value="x",
                    by=ORDER, **kwargs):
    ordered = frame.sort_values(by, kind="mergesort")
    return ordered[value].rolling(window, **kwargs).rank(
        method=method, ascending=ascending, pct=pct
    )


def pandas_expanding(series, func, **kwargs):
    return getattr(series.expanding(**kwargs), func)()


def pandas_expanding_row(frame, func, value="x", by=ORDER, **kwargs):
    ordered = frame.sort_values(by, kind="mergesort")
    return getattr(ordered[value].expanding(**kwargs), func)()


def pandas_quantile(series, window, q, interpolation, **kwargs):
    return series.rolling(window, **kwargs).quantile(
        q, interpolation=interpolation
    )


def pandas_rank(series, window, method, ascending, pct, **kwargs):
    return series.rolling(window, **kwargs).rank(
        method=method, ascending=ascending, pct=pct
    )


def pandas_grouped_roll(frame, value, func, window=None, keys=("g",),
                        method_kwargs=None, **kwargs):
    keys = list(keys)
    ordered = frame.sort_values([*keys, ORDER], kind="mergesort")
    parts = []
    for _, grp in ordered.groupby(keys, sort=False):
        roll = (
            grp[value].rolling(window, **kwargs)
            if window is not None
            else grp[value].expanding(**kwargs)
        )
        parts.append(getattr(roll, func)(**(method_kwargs or {})))
    return pd.concat(parts).to_numpy()


def _time_sorted(frame, values):
    return (
        frame.assign(__expected=values)
        .sort_values("t", kind="mergesort")["__expected"]
        .to_numpy()
    )


def pandas_time_oracle(frame, value, window, func, **kwargs):
    """pandas time-window oracle in ``t``-sorted (stable) row order."""
    ordered = frame.sort_values("t", kind="mergesort")
    out = getattr(ordered.rolling(window, on="t", **kwargs)[value], func)()
    return out.to_numpy()


def pandas_time_apply(frame, value, window, fn, **kwargs):
    ordered = frame.sort_values("t", kind="mergesort")
    out = ordered.rolling(window, on="t", **kwargs)[value].apply(
        fn, raw=False
    )
    return out.to_numpy()


def pandas_grouped_time(frame, value, window, func, keys=("g",), **kwargs):
    keys = list(keys)
    ordered = frame.sort_values([*keys, "t"], kind="mergesort")
    parts = []
    for _, grp in ordered.groupby(keys, sort=False):
        inner = grp.sort_values("t", kind="mergesort")
        roll = getattr(
            inner.set_index("t", drop=False).rolling(window, **kwargs)[value],
            func,
        )
        parts.append(roll())
    return pd.concat(parts).to_numpy()


def pandas_grouped_quantile(frame, value, window, q, interpolation,
                            keys=("g",), **kwargs):
    keys = list(keys)
    ordered = frame.sort_values([*keys, ORDER], kind="mergesort")
    parts = []
    for _, grp in ordered.groupby(keys, sort=False):
        parts.append(
            grp[value].rolling(window, **kwargs).quantile(
                q, interpolation=interpolation
            )
        )
    return pd.concat(parts).to_numpy()


def pandas_grouped_time_quantile(frame, value, window, q, interpolation,
                                 keys=("g",), **kwargs):
    keys = list(keys)
    ordered = frame.sort_values([*keys, "t"], kind="mergesort")
    parts = []
    for _, grp in ordered.groupby(keys, sort=False):
        inner = grp.sort_values("t", kind="mergesort")
        parts.append(
            inner.set_index("t", drop=False).rolling(
                window, **kwargs
            )[value].quantile(q, interpolation=interpolation)
        )
    return pd.concat(parts).to_numpy()


def manual_grouped_time_first_last(frame, func, window, keys=("g",), col="x"):
    keys = list(keys)
    ordered = frame.sort_values([*keys, "t"], kind="mergesort")
    parts = []
    for _, grp in ordered.groupby(keys, sort=False):
        parts.append(manual_time_first_last(grp, func, window, col=col))
    return np.concatenate(parts)


def manual_first_last(series, window, func, min_periods=None):
    """FIRST_VALUE/LAST_VALUE semantics for a right-closed row frame."""
    values = list(series)
    minp = window if min_periods is None else min_periods
    out = []
    for i in range(len(values)):
        lo = max(0, i - window + 1)
        frame = values[lo : i + 1]
        observed = sum(1 for v in frame if v is not None and v == v)
        if observed < minp:
            out.append(np.nan)
            continue
        out.append(frame[0] if func == "first" else frame[-1])
    return np.asarray(out, dtype=object)


def manual_time_first_last(frame, func, window, col="x"):
    order = np.argsort(frame["t"].to_numpy(), kind="mergesort")
    ts = frame["t"].to_numpy()[order]
    xs = frame[col].to_numpy(dtype=object)[order]
    offset = np.timedelta64(pd.Timedelta(window).to_timedelta64())
    out = []
    for i in range(len(ts)):
        mask = (ts > ts[i] - offset) & (ts <= ts[i]) & (
            np.arange(len(ts)) <= i
        )
        picked = xs[np.flatnonzero(mask)]
        out.append(picked[0] if func == "first" else picked[-1])
    return np.asarray(out, dtype=object)


def manual_rank(series, window, method, ascending=True, pct=False):
    values = list(series)
    out = []
    for i in range(len(values)):
        lo = max(0, i - window + 1)
        frame = [v for v in values[lo : i + 1] if v == v]
        cur = values[i]
        if cur != cur:
            out.append(np.nan)
            continue
        less = sum(1 for v in frame if (v < cur if ascending else v > cur))
        equal = sum(1 for v in frame if v == cur)
        if method == "average":
            rank = less + (equal + 1) / 2.0
        elif method == "min":
            rank = less + 1
        elif method == "max":
            rank = less + equal
        elif method == "first":
            before = sum(
                1
                for j in range(lo, i)
                if values[j] == cur
            )
            rank = less + before + 1
        else:  # dense
            upper = [
                v
                for v in frame
                if (v <= cur if ascending else v >= cur)
            ]
            rank = len(set(upper))
        out.append(rank / len(frame) if pct else rank)
    return np.asarray(out, dtype=float)
