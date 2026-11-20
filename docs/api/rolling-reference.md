# Rolling Reference

This page is the complete parameter and semantics reference for rolling
windows in memFrame, both ungrouped (`ctx.rolling(...)`) and grouped
(`ctx.groupby(...).rolling(...)`).

## Parameters

| Parameter | Type | Behavior |
| --- | --- | --- |
| `window` | `int`, `datetime.timedelta`, `pandas.Timedelta`, or duration `str` | Row window when an int; time window when a duration. A row window includes the current row (`window=3` → 2 preceding + current). A time window includes observations in the duration ending at the current row. |
| `min_periods` | `int` or `None` | Minimum observations in the frame before a value is emitted. Row windows default to the window size; time windows default to `1`. Must satisfy `0 <= min_periods`; row windows also require `min_periods <= window`. |
| `center` | `bool` | Centre the frame on the current row. Row windows shift by `(window - 1) // 2` rows; time windows shift by `window / 2`. Default `False`. |
| `win_type` | `str` or `None` | Apply scipy window weights to `sum`/`mean`. See [Window types](#window-types). Default `None` (unweighted). |
| `order_by` | `str`, `list[str]`, or `None` | The memFrame equivalent of pandas' `on`. For row windows it defines the frame sequence; for time windows it is the clock and must be a single datetime column. `None` uses the immutable upload order (`_mf_idx`). |
| `closed` | `str` or `None` | Frame edge inclusivity: `right` (default, `(first, last]`), `left` (`[first, last)`), `both` (`[first, last]`), `neither` (`(first, last)`). Row and time windows. |
| `step` | — | Not exposed by memFrame. Passing it raises `TypeError`. |
| `method` | — | Not exposed by memFrame. Passing it raises `TypeError`. |

`func_kwargs` carries per-function arguments such as
`{"std": {"ddof": 0}, "rank": {"method": "dense"}}`.

## Window forms

### Row windows

An integer `window` counts rows, including the current row. `closed` selects
which edges of that row range are included; `center` shifts the range.

```python
ctx.rolling(column="sales", window=3, func="mean", order_by="date")
ctx.rolling(column="sales", window=3, func="mean", center=True)
ctx.rolling(column="sales", window=3, func="mean", closed="both")
```

### Time windows

A `datetime.timedelta`, `pandas.Timedelta`, or duration string creates a
time-based window. The clock is `order_by`, which must be a single datetime
column.

```python
import datetime

ctx.rolling(column="sales", window="3D", func="mean", order_by="date")
ctx.rolling(column="sales", window=datetime.timedelta(days=3),
            func="mean", order_by="date")
ctx.rolling(column="sales", window="36h", func="mean", order_by="event_time")
```

Duration strings are parsed as time deltas. Fixed durations work:

| Form | Meaning |
| --- | --- |
| `"1s"`, `"30s"` | seconds |
| `"1min"`, `"30min"` | minutes |
| `"1h"`, `"36h"` | hours |
| `"1D"`, `"3D"` | days |
| `"1h30min"`, `"1.5h"` | composite / fractional hours |
| `"2W"` | 14 days (a duration, not a calendar week) |

Non-fixed calendar aliases are not durations and are rejected:
`"B"`, `"ME"`, `"M"`, `"Q"`, `"Y"`, and a bare `"W"`.

A pandas `BaseIndexer` is not accepted; `window` must be an int, timedelta, or
duration string.

## `min_periods`

- Row window, `min_periods=None` → the window size.
- Time window, `min_periods=None` → `1`.
- `min_periods=0` emits a value even for an empty frame where the aggregate is
  defined.
- Row windows reject `min_periods > window`.
- Negative or non-integer values are rejected.

## `center` and `closed`

`center=False` labels the frame at its right edge. `center=True` shifts the
frame over the ordered sequence by `(window - 1) // 2` rows (or `window / 2`
for time windows).

`closed` controls which edges belong to the frame:

| `closed` | Row window | Time window |
| --- | --- | --- |
| `right` (default) | `window - 1` preceding + current | `(t - window, t]` |
| `left` | `window` preceding, current excluded | `[t - window, t)` |
| `both` | `window` preceding + current | `[t - window, t]` |
| `neither` | `window - 1` preceding, current excluded | `(t - window, t)` |

`window=1` with `closed="neither"` is an empty frame and is rejected.

## Window types

`win_type` applies scipy window weights to `sum` and `mean` over the default
frame (`center=False`, `closed="right"`). Weighted frames are capped at 512
rows.

Supported: `boxcar`, `triang`, `bartlett`, `parzen`, `bohman`, `blackman`,
`blackmanharris`, `barthann`, `nuttall`, `hann`, `hamming`, `cosine`.

These match the weights returned by
[`scipy.signal.windows`](https://docs.scipy.org/doc/scipy/reference/signal.windows.html)
with `fftbins=False`.

Window types that require extra parameters (`gaussian`, `exponential`,
`tukey`, `kaiser`, `taylor`, `lanczos`, …) are not supported. Weighted
`std`/`var`, time windows, `center=True`, and non-default `closed` are also
rejected.

```python
ctx.rolling(column="sales", window=5, func="mean", order_by="date",
            win_type="triang")
```

## Grouped rolling

`ctx.groupby(*group_cols).rolling(...)` applies the same parameters inside
each partition:

```python
result = ctx.groupby("region").rolling(
    3, order_by="date", min_periods=1,
).mean("sales")

result = ctx.groupby("region").rolling(
    "3D", order_by="date", closed="both",
).sum("sales")
```

- Rows are partitioned by the group columns first, then ordered by `order_by`
  inside each group.
- Ties are broken by `_mf_idx`, the immutable upload order.
- `order_by=None` uses `_mf_idx` inside each group.
- A group's window never includes rows from another group.
- Time windows use the same datetime `order_by` rule per group.
- The generated column is joined back by `_mf_idx`, so the result stays in
  upload order. Sort the result yourself for display.
