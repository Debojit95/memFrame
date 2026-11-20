# Window

> **Example data:** copy the setup cell from [Example Data](example-data.md) into Colab once, then run the examples below. Examples use that `ctx`; local frames are shown inline.

Source: `src/memframe/wrappers/analytix/window.py`

`WindowWrapper` is the public rolling / expanding / exponentially-weighted
interface exposed through a `ContextManager`. Every operation compiles to
backend-native SQL and runs in-engine across DuckDB, PostgreSQL, and
ClickHouse; each call adds its generated column(s) to the source upload
table and records a source ref that the deep cache stores.

Users normally call window methods directly on a dataset context returned by an
upload operation. There are two equivalent entry points:

- a direct call — `ctx.rolling(...)`, `ctx.expanding(...)`,
  `ctx.ewm(...)` — with `order_by` passed as an argument;
- a pandas-style fluent builder — `ctx.on("sales").rolling(7).mean(...)`.

```python
# ctx is defined in the Example Data setup

rolling = ctx.rolling(column="sales", window=7, func="mean", order_by="date")
expanding = ctx.expanding(column="sales", func="sum", order_by="date")
smoothed = ctx.ewm(column="sales", span=3, func="mean", order_by="date")
fluent = ctx.on("sales").rolling(7).mean(order_by="date")
```

```python
# ctx is defined in the Example Data setup

rolling = await ctx.arolling(column="sales", window=7, func="mean", order_by="date")
expanding = await ctx.aexpanding(column="sales", func="sum", order_by="date")
smoothed = await ctx.aewm(column="sales", span=3, func="mean", order_by="date")
```

`window` is top-level (`ctx.rolling`, not `ctx.dt.*`) — `dt` is reserved
for `DateTimeWrapper`.

The lower-level files are implementation details:

- `src/memframe/core/analytix/window/` holds the engine: `base.py` is the
  shared `WindowOps` with DuckDB-flavoured defaults plus dialect hooks
  (`_row_id_col`, `_std_agg`/`_var_agg`, `_datetime_epoch_expr`,
  `_median_epoch_sql`, `_from_epoch_expr`, and the EWM block/missing-value
  hooks); `duckdb.py` /
  `postgres.py` / `clickhouse.py` override those hooks (including the
  correlated quantile/nunique/rank SQL and ClickHouse spellings);
  `factory.py` dispatches `make_window_ops(adapter)` on `isinstance`.
- `src/memframe/core/orchestrator/analytix/window.py` resolves the active
  dataset context, detects the column dtype, maps the requested functions to
  the dtype-appropriate engine methods, and applies `@record_call`.
- `src/memframe/wrappers/analytix/window.py` exposes the synchronous and
  asynchronous public methods plus the fluent builders.

## Public API

### Rolling

Each rolling method has a synchronous and asynchronous form. Direct rolling
takes `(column, window, func, order_by=None, q=0.5, min_periods=None,
center=False, closed=None, win_type=None, func_kwargs=None)`; the named
shorthands fix `func`. Builders take the same frame options at construction:
`ctx.on("sales").rolling(7, min_periods=1, win_type="hann").mean()`.

| Synchronous (via builder or `rolling`) | Purpose |
| --- | --- |
| `rolling(column, window, func, order_by=None, q=0.5, min_periods=None, center=False, closed=None, win_type=None, func_kwargs=None)` / `await arolling(...)` | Generic — `func` may be a string or list |
| `.rolling(column, window).sum(order_by=None)` / `.asum(...)` | Rolling sum |
| `.mean(...)` / `.amean(...)` | Rolling mean |
| `.min(...)` / `.amin(...)` | Rolling minimum |
| `.max(...)` / `.amax(...)` | Rolling maximum |
| `.count(...)` / `.acount(...)` | Rolling non-null count |
| `.std(...)` / `.astd(...)` | Rolling sample standard deviation (`ddof=1`) |
| `.var(...)` / `.avar(...)` | Rolling sample variance |
| `.quantile(q=0.5, order_by=None, interpolation='linear')` / `.aquantile(...)` | Rolling quantile |
| `.median(...)` / `.amedian(...)` | Rolling median |
| `.sem(...)` / `.asem(...)` | Rolling standard error |
| `.rank(method='average', ascending=True, pct=False)` / `.arank(...)` | Rolling rank within the window |
| `.skew(...)` / `.askew(...)` | Rolling skewness (needs 3 observations) |
| `.kurt(...)` / `.akurt(...)` | Rolling excess kurtosis (needs 4 observations) |
| `.nunique(...)` / `.anunique(...)` | Rolling distinct count |
| `.first(...)` / `.afirst(...)` | First value in the window |
| `.last(...)` / `.alast(...)` | Last value in the window |

### Expanding

Expanding methods mirror rolling but have no `window`; they add `min_periods`
(default `1`). Expansion runs from the first row to the current row.

| Synchronous | Purpose |
| --- | --- |
| `expanding(column, func, order_by=None, q=0.5, min_periods=1, func_kwargs=None)` / `await aexpanding(...)` | Generic |
| `.expanding(column, min_periods=1).sum(order_by=None)` / `.asum(...)` | Running sum |
| `.mean(...)` / `.amean(...)` | Running mean |
| `.min(...)` / `.amin(...)` | Running minimum |
| `.max(...)` / `.amax(...)` | Running maximum |
| `.count(...)` / `.acount(...)` | Running non-null count |
| `.std(...)` / `.astd(...)` | Running sample standard deviation |
| `.var(...)` / `.avar(...)` | Running sample variance |
| `.quantile(q=0.5, order_by=None, interpolation='linear')` / `.aquantile(...)` | Running quantile |
| `.median(...)` / `.amedian(...)` | Running median |
| `.sem(...)` / `.asem(...)` | Running standard error |
| `.rank(method='average', ascending=True, pct=False)` / `.arank(...)` | Running rank |
| `.skew(...)` / `.askew(...)` | Running skewness |
| `.kurt(...)` / `.akurt(...)` | Running excess kurtosis |
| `.nunique(...)` / `.anunique(...)` | Running distinct count |
| `.first(...)` / `.afirst(...)` | Running first value |
| `.last(...)` / `.alast(...)` | Running last value |

### EWM

Exponentially-weighted operations are numeric-only and support four functions.

| Synchronous | Purpose |
| --- | --- |
| `ewm(column, com=None, span=None, halflife=None, alpha=None, adjust=True, ignore_na=False, min_periods=0, func="mean", order_by=None)` / `await aewm(...)` | Generic EWM |
| `.ewm(column, ...).mean(...)` / `.amean(...)` | EWM mean |
| `.sum(...)` / `.asum(...)` | EWM sum |
| `.std(...)` / `.astd(...)` | EWM standard deviation |
| `.var(...)` / `.avar(...)` | EWM variance |

## Usage Overview

```python
# ctx is defined in the Example Data setup

sample = ctx.rolling(column="sales", window=7, func="mean", order_by="date")
sample = ctx.rolling(column="sales", window=7, func=["sum", "mean"], order_by="date")
sample = ctx.expanding(column="sales", func="sum", order_by="date", min_periods=3)
sample = ctx.ewm(column="sales", span=3, func="mean", order_by="date")
sample = ctx.on("sales").rolling(7).mean(order_by="date")
```

```python
# ctx is defined in the Example Data setup

sample = await ctx.arolling(column="sales", window=7, func="mean", order_by="date")
sample = await ctx.aexpanding(column="sales", func="sum", order_by="date", min_periods=3)
sample = await ctx.aewm(column="sales", span=3, func="mean", order_by="date")
```

`order_by` is optional — omit it to use the natural `_mf_idx` upload order:

```python
sample = ctx.rolling(column="sales", window=7, func="mean")
sample = ctx.expanding(column="sales", func="sum", min_periods=3)
sample = ctx.ewm(column="sales", span=3, func="mean")
```

## Common Parameters

| Parameter | Type | Description |
| --- | --- | --- |
| `column` | `str` | Column to window over. |
| `window` | `int`, `timedelta`, or offset `str` | Rolling window: rows when an int (**inclusive of the current row**, `window=3` → 2 preceding + current), or a duration (`"3D"`, `"36h"`, `timedelta(days=3)`) for time-based windows. |
| `func` | `str` or `list[str]` | Aggregation(s) to apply. A list chains them into a single output table. |
| `order_by` | `str`, `list[str]`, or `None` | Row ordering for the window. `None` (default) uses the immutable upload order (`_mf_idx`). A list orders by multiple columns. |
| `q` | `float` | Quantile in `[0, 1]` for `quantile` (default `0.5`). |
| `center` | `bool` | Rolling only — centre the frame on the current row: row windows shift by `(window - 1) // 2` rows, time windows by `window / 2` (default `False`). |
| `func_kwargs` | `dict` or `None` | Per-function keyword arguments, e.g. `{"std": {"ddof": 0}, "rank": {"method": "dense"}}`. |
| `closed` | `str` or `None` | Rolling only — which frame edges are included: `right` (default), `left`, `both`, `neither` (row and time windows). |
| `win_type` | `str` or `None` | Rolling only — apply scipy window weights. Supported: `boxcar`, `triang`, `bartlett`, `parzen`, `bohman`, `blackman`, `blackmanharris`, `barthann`, `nuttall`, `hann`, `hamming`, `cosine`. Weighted `sum`/`mean` only. |
| `min_periods` | `int` or `None` | Minimum observations in the frame before a value is emitted. Rolling default `None` → `window` (pandas parity); expanding default `1`; EWM default `0`. Must satisfy `0 <= min_periods`; row windows also require `<= window` (time windows have no upper bound). |

`order_by` accepts a single column or a list:

```python
result = ctx.rolling(column="sales", window=7, func="mean", order_by="date")
result = ctx.rolling(
    column="sales", window=7, func="mean", order_by=["region", "date"]
)
```

Every generated column is computed in SQL and merged back into the source by
`_mf_idx`; the paragraphs below describe the ordering the window itself uses.

### How the window runs internally

The same shape is used for rolling, expanding, and EWM:

```sql
-- no order_by
AVG("sales") OVER (ORDER BY "_mf_idx"
                   ROWS BETWEEN 6 PRECEDING AND CURRENT ROW)

-- order_by="date"
AVG("sales") OVER (ORDER BY "date", "_mf_idx"
                   ROWS BETWEEN 6 PRECEDING AND CURRENT ROW)
```

- **No `order_by` is not "unordered".** The engine always has an order key:
  `_mf_idx`, the immutable index assigned at upload. It survives later sorts,
  filters, and merges, so a no-order window is stable across repeated runs and
  matches pandas' "row order of the frame as it was uploaded".
- **`order_by` is ordered *inside the OVER clause* only.** It decides which
  rows fall into each frame; it never rewrites the table. `_mf_idx` is
  appended to the key list so equal `order_by` values have a deterministic
  order (upload order wins ties). `center` shifts the frame over that ordered
  sequence, and `closed` picks which of its edges are included.
- **The output keeps the upload order.** Each generated value is joined back
  to the source row by `_mf_idx`, so a table ordered by `date` still comes
  back in upload order — `order_by` changes *which rows are aggregated*, not
  *how the result is laid out*. Sort the result yourself if you want it
  presented by `date`.
- **Time windows use the `order_by` column as the clock.** With a duration
  window (`"3D"`), the frame is `(t − window, t]` over the values of the
  `order_by` timestamp, so `order_by` is required to be datetime-typed.

The pandas equivalent mirrors this exactly: compute on the frame ordered by
`order_by` (or the frame as-is when it is omitted), then restore the original
row order:

```python
ordered = df.sort_values("date", kind="mergesort")   # skip when order_by is None
expected = (
    ordered["sales"].rolling(7).mean()
    .reindex(df.index)                               # back to the original row order
)
```

For large frames, pass `preview_rows=N` to `MemFrame`/`mf.DataFrame`: the
source still receives every generated value, only the returned preview is
capped.

> **Ordered vs. unordered:** every example below is shown twice — once with
> `order_by` and once without. Both forms are fully deterministic; they differ
> only in which column defines the frame sequence (`order_by` vs. upload
> order).

## Aggregation Parameters

Method-level pandas parameters are available on the builders, and through
`func_kwargs` for the generic `func=` path:

```python
ctx.on("sales").rolling(7, min_periods=1).std(ddof=0)
ctx.on("sales").rolling(7).rank(method="dense", ascending=False, pct=True)
ctx.rolling(
    column="sales", window=7, func=["std", "quantile"],
    func_kwargs={"std": {"ddof": 0}, "quantile": {"q": 0.9}},
)
```

Supported per-function parameters: `ddof` for `std`/`var`/`sem`, `q` for
`quantile`/`median`, and `method`/`ascending`/`pct` for `rank`.

`quantile` also accepts `interpolation`, matching the pandas signature:
`"linear"` (default), `"lower"`, `"higher"`, `"nearest"`, and `"midpoint"`.
`"linear"` runs natively; the other modes compute the position
`q * (count - 1)` and pick the element(s) around it, exactly like pandas
(see the `quantile` section).

## Time-Based Windows

Pass a duration instead of a row count to window by time. A datetime
`order_by` column is required, and `min_periods` defaults to `1`:

```python
ctx.rolling(column="sales", window="3D", func="mean", order_by="date")
ctx.rolling(column="sales", window=pd.Timedelta("36h"), func="sum",
                order_by="date")
ctx.on("sales").rolling(datetime.timedelta(days=3)).mean(order_by="date")
```

- The frame defaults to `(t - window, t]` — a strict left edge, like pandas.
  `closed` selects the edges: `right` (default), `left`, `both`, `neither`.
  Rows that share a timestamp are ordered by row position, so ties are not
  grouped (again matching pandas).
- `center=True` shifts both edges by `window / 2`, so the frame becomes
  `(t - window/2, t + window/2]` (edges still follow `closed`). Centred frames
  look ahead, so later rows are included.
- Supported aggregations: `sum`, `mean`, `min`, `max`, `count`, `std`, `var`
  (with `ddof`), the specials — `quantile`, `median`, `sem`, `rank`,
  `nunique`, `skew`, `kurt`, and the datetime `mean`/`median`/`mode` — plus
  `first`/`last`. `count` is gated on the rows in the frame.
- `first` / `last` return the value of the earliest / latest row in the frame
  (by position), including when that value is null. They are MemFrame extras
  (pandas' `Rolling` has no `first`/`last`) and keep the column's type, so
  datetime and text columns come back unchanged.
- `min_periods` defaults to `1`; set it explicitly to require more
  observations.
- Still rejected for time windows: `win_type`.
- Generated columns carry the duration tag, e.g.
  `sales_rolling_mean_w259200s` for three days.

## Weighted Windows

`win_type` applies scipy-compatible window weights to a rolling `sum` or
`mean` (`boxcar` is the unweighted frame; pass `None` for that):

```python
ctx.rolling(column="sales", window=5, func="mean", order_by="date",
                win_type="triang")
ctx.on("sales").rolling(5, win_type="hann").mean(order_by="date")
```

- Supported weights: `boxcar`, `triang`, `bartlett`, `parzen`, `bohman`,
  `blackman`, `blackmanharris`, `barthann`, `nuttall`, `hann`, `hamming`,
  `cosine` — the same arrays `scipy.signal.get_window(..., fftbins=False)`
  returns.
- `gaussian`, `exponential`, `tukey`, `kaiser`, `taylor`, `lanczos` need extra
  window parameters and raise `NotImplementedError`.
- Weighted `std`/`var` raise: pandas computes them with rotating weights, which
  is a different contract.
- Weighted frames use the default shape only (`center=False`,
  `closed="right"`), and the window is capped at 512 rows (the weighted scan
  runs as a weights join).

## Type Support

The orchestrator samples the target column and infers one of three kinds, then
maps each requested function to the dtype-appropriate engine method:

| Function | numeric | datetime | categorical |
| --- | --- | --- | --- |
| `sum`, `std`, `var`, `sem`, `quantile`, `skew`, `kurt` | ✅ | — | — |
| `mean`, `median` | ✅ | ✅ | — |
| `min`, `max`, `count`, `nunique`, `rank`, `first`, `last` | ✅ | ✅ | ✅ |
| `mode` | — | ✅ | — |

Datetime columns route `min`/`max` through the generic value aggregation, while
`mean`/`median`/`mode` are computed over the column's epoch value and converted
back to a timestamp (or `DATE` for date-only columns). Requesting an
unsupported function for a detected dtype returns a canonical error envelope
rather than raising; EWM on a non-numeric column is rejected the same way.

---

## Rolling

A rolling window of size `w = window` covers the rows from `w - 1` preceding
the current row through the current row. `min_periods` defaults to `window`
(pandas parity), so the first `w - 1` rows are `NULL`. Pass
`min_periods=1` to aggregate whatever is available instead:

```python
rows = ctx.rolling(column="sales", window=3, func="mean", order_by="date")
partial = ctx.rolling(
    column="sales", window=3, func="mean", order_by="date", min_periods=1,
)
```

`count` is gated on the rows in the frame rather than on the non-null
observations, matching pandas. `min_periods=0` additionally returns `0` for an
empty `sum` window.

`center` and `closed` change the frame itself, with the same offsets pandas
uses (`w = window`):

| `closed` | frame (`center=False`) | frame (`center=True`) |
| --- | --- | --- |
| `right` / `None` | `w-1 PRECEDING … CURRENT ROW` | shift forward `(w-1)//2` |
| `left` | `w PRECEDING … 1 PRECEDING` | shift forward `(w-1)//2` |
| `both` | `w PRECEDING … CURRENT ROW` | shift forward `(w-1)//2` |
| `neither` | `w-1 PRECEDING … 1 PRECEDING` | shift forward `(w-1)//2` |

```python
ctx.rolling(column="sales", window=3, func="mean", order_by="date", center=True)
ctx.rolling(column="sales", window=3, func="sum", order_by="date", closed="both")
```

`closed="neither"` needs `window >= 2` — with `window=1` the frame would hold no
rows and SQL cannot express it. `rank` always ranks the **current** row inside
the frame; pandas ranks the frame's last row, which only differs under
`center=True`.

All examples in this section use a numeric frame
`sales = [10, 20, 30, 40, 50]`, `date` starting at `2024-01-01` (the
`example` dataset), `window = 3`.

### `sum`

Total of the values in the window (nulls ignored). Result column
`sales_rolling_sum_w3`.

```python
# ordered by a column
result = ctx.rolling(column="sales", window=3, func="sum", order_by="date")

# no order_by — natural `_mf_idx` order
result = ctx.rolling(column="sales", window=3, func="sum")
```

Both forms → `[NULL, NULL, 60, 90, 120]`.

### `mean`

Average of the values in the window. Result column `sales_rolling_mean_w3`.

```python
result = ctx.rolling(column="sales", window=3, func="mean", order_by="date")
result = ctx.rolling(column="sales", window=3, func="mean")
```

Both forms → `[NULL, NULL, 20, 30, 40]`.

### `min`

Smallest value in the window. Result column `sales_rolling_min_w3`.

```python
result = ctx.rolling(column="sales", window=3, func="min", order_by="date")
result = ctx.rolling(column="sales", window=3, func="min")
```

Both forms → `[NULL, NULL, 10, 20, 30]`.

### `max`

Largest value in the window. Result column `sales_rolling_max_w3`.

```python
result = ctx.rolling(column="sales", window=3, func="max", order_by="date")
result = ctx.rolling(column="sales", window=3, func="max")
```

Both forms → `[NULL, NULL, 30, 40, 50]`.

### `count`

Number of non-null values in the window. Result column
`sales_rolling_count_w3`.

```python
result = ctx.rolling(column="sales", window=3, func="count", order_by="date")
result = ctx.rolling(column="sales", window=3, func="count")
```

Both forms → `[NULL, NULL, 3, 3, 3]`.

### `std`

**Sample** standard deviation (`ddof=1`) over the window. Pass `ddof=0` for the
population figure, or any other integer for the unbiased form. Result column
`sales_rolling_std_w3`.

```python
result = ctx.rolling(column="sales", window=3, func="std", order_by="date")
result = ctx.rolling(column="sales", window=3, func="std")
```

Both forms → `[NULL, NULL, 10, 10, 10]`.

### `var`

**Sample** variance (`ddof=1`) over the window. A single-row window yields
`NULL`. `ddof` is configurable per call. Result column `sales_rolling_var_w3`.

```python
result = ctx.rolling(column="sales", window=3, func="var", order_by="date")
result = ctx.rolling(column="sales", window=3, func="var")
```

Both forms → `[NULL, NULL, 100, 100, 100]`.

### `quantile`

Interpolated quantile `q` (default `0.5`, the median) over the window and gated
by the same `min_periods` rule as the other functions. Result column
`sales_rolling_q0.5_w3`.

Parameters: `column`, `window`, `q` (default `0.5`), `order_by`, and
`interpolation`:

| `interpolation` | value |
| --- | --- |
| `linear` (default) | interpolates between the two neighbours |
| `lower` | element at `floor(q * (n - 1))` |
| `higher` | element at `ceil(q * (n - 1))` |
| `midpoint` | average of the lower and higher elements |
| `nearest` | closest element; exact halves round to the even index |

`q` accepts `[0, 1]` (`0` → minimum, `1` → maximum). When `q * (n - 1)` lands
on an integer every mode returns the same element, and a single observation
returns that value. Set the mode through the builder or `func_kwargs`:

```python
ctx.on("sales").rolling(7).quantile(q=0.9, interpolation="higher")
ctx.rolling(
    column="sales", window=7, func="quantile", q=0.9, order_by="date",
    func_kwargs={"quantile": {"interpolation": "nearest"}},
)
```

```python
result = ctx.rolling(column="sales", window=3, func="quantile", q=0.5, order_by="date")
result = ctx.rolling(column="sales", window=3, func="quantile", q=0.5)
```

Both forms → `[NULL, NULL, 20, 30, 40]`.

### `median`

Median of the window (quantile `0.5`, linear interpolation). Result column
`sales_rolling_median_w3`.

```python
result = ctx.rolling(column="sales", window=3, func="median", order_by="date")
result = ctx.rolling(column="sales", window=3, func="median")
```

Both forms → `[NULL, NULL, 20, 30, 40]`.

### `sem`

Standard error of the mean, matching pandas: the sample standard deviation
divided by `sqrt(count - ddof)` (`ddof=1` by default). Result column
`sales_rolling_sem_w3`.

```python
result = ctx.rolling(column="sales", window=3, func="sem", order_by="date")
result = ctx.rolling(column="sales", window=3, func="sem")
```

Both forms → `[NULL, NULL, 7.071, 7.071, 7.071]`.

### `rank`

Rank of the current row inside the window. `method` is `average` (default),
`min`, `max`, `first`, or `dense`; `ascending=False` ranks descending and
`pct=True` divides by the non-null count. A null current value yields `NULL`.
Result column `sales_rolling_rank_w3`.

```python
result = ctx.rolling(column="sales", window=3, func="rank", order_by="date")
result = ctx.rolling(column="sales", window=3, func="rank")
```

Both forms → `[NULL, NULL, 3, 3, 3]`.

### `nunique`

Number of distinct values in the window. Result column
`sales_rolling_nunique_w3`.

```python
result = ctx.rolling(column="sales", window=3, func="nunique", order_by="date")
result = ctx.rolling(column="sales", window=3, func="nunique")
```

Both forms → `[NULL, NULL, 3, 3, 3]`.

### `skew` / `kurt`

Bias-corrected sample skewness and excess kurtosis, following pandas. `skew`
needs at least 3 observations and `kurt` at least 4; windows that are shorter
than that (or whose variance is effectively zero) return `NULL`. A window
where every value is identical returns `0` for `skew` and `-3` for `kurt`.
Result columns `sales_rolling_skew_w3` / `sales_rolling_kurt_w3`.

```python
result = ctx.rolling(column="sales", window=3, func="skew", order_by="date")
result = ctx.on("sales").rolling(3).kurt(order_by="date")
```

They are stateless closed forms, so they also work with `center`, `closed`,
time windows, and per group.

### `first`

First value in the window (the value `w - 1` rows back, or the first row of the
table). Result column `sales_rolling_first_w3`.

```python
result = ctx.rolling(column="sales", window=3, func="first", order_by="date")
result = ctx.rolling(column="sales", window=3, func="first")
```

Both forms → `[NULL, NULL, 10, 20, 30]`.

### `last`

Last value in the window — always the current row. Result column
`sales_rolling_last_w3`.

```python
result = ctx.rolling(column="sales", window=3, func="last", order_by="date")
result = ctx.rolling(column="sales", window=3, func="last")
```

Both forms → `[NULL, NULL, 30, 40, 50]`.

### Multiple functions

Passing a list computes every function in one pass and writes all generated
columns to the source:

```python
result = ctx.rolling(
    column="sales", window=3, func=["sum", "mean"], order_by="date"
)
result = ctx.rolling(column="sales", window=3, func=["sum", "mean"])
```

The response reports `new_columns`, `successful_funcs`, `skipped_funcs`,
`failed_funcs`, and `is_partial`. Unknown functions and functions unsupported
for the detected dtype are skipped (partial result); if every requested
function fails, an error envelope is returned instead.

### Datetime rolling

`min`, `max`, `mean`, `median`, and `mode` are supported on datetime columns.
`min`/`max` compare the timestamps directly; `mean`/`median`/`mode` convert to
epoch seconds, aggregate, then convert back. On a date frame
`date = [Jan 1, Jan 2, Jan 3, Jan 4, Jan 5]`, `window = 3`:

```python
result = ctx.rolling(column="date", window=3, func="median", order_by="date")
result = ctx.rolling(column="date", window=3, func="median")
```

Both forms → `[NULL, NULL, Jan 2, Jan 3, Jan 4]`.

```python
result = ctx.rolling(column="date", window=3, func="min", order_by="date")
result = ctx.rolling(column="date", window=3, func="min")
```

Both forms → `[NULL, NULL, Jan 1, Jan 2, Jan 3]`.

Date-only columns are cast back to a date so the result keeps the original
granularity; timestamp columns keep their time component.

### Rank and nunique details

`nunique` is a native windowed distinct count on DuckDB. PostgreSQL and
ClickHouse use a correlated `COUNT(DISTINCT …)` (`countDistinct` on
ClickHouse) over the narrow ordered base — there is no Python fallback.
`rank` ranks the current row inside the frame (see the frame notes above).

---

## Expanding

Expanding operations run from the first row through the current row. There is
no `window`; `min_periods` (default `1`) gates the early rows. All examples use
`sales = [10, 20, 30, 40, 50]`.

### `sum`

Running total. Result column `sales_expanding_sum`.

```python
result = ctx.expanding(column="sales", func="sum", order_by="date")
result = ctx.expanding(column="sales", func="sum")
```

Both forms → `[10, 30, 60, 100, 150]`.

### `mean`

Running average. Result column `sales_expanding_mean`.

```python
result = ctx.expanding(column="sales", func="mean", order_by="date")
result = ctx.expanding(column="sales", func="mean")
```

Both forms → `[10, 15, 20, 25, 30]`.

`min_periods` suppresses output until enough non-null values have been seen:

```python
result = ctx.expanding(
    column="sales", func="mean", order_by="date", min_periods=3
)
result = ctx.expanding(column="sales", func="mean", min_periods=3)
```

Both forms → `[NULL, NULL, 20, 25, 30]`.

### `min`

Running minimum. Result column `sales_expanding_min`.

```python
result = ctx.expanding(column="sales", func="min", order_by="date")
result = ctx.expanding(column="sales", func="min")
```

Both forms → `[10, 10, 10, 10, 10]`.

### `max`

Running maximum. Result column `sales_expanding_max`.

```python
result = ctx.expanding(column="sales", func="max", order_by="date")
result = ctx.expanding(column="sales", func="max")
```

Both forms → `[10, 20, 30, 40, 50]`.

### `count`

Running non-null count. Result column `sales_expanding_count`.

```python
result = ctx.expanding(column="sales", func="count", order_by="date")
result = ctx.expanding(column="sales", func="count")
```

Both forms → `[1, 2, 3, 4, 5]`.

### `std`

Running **sample** standard deviation. The first row is `NULL`. Result column
`sales_expanding_std`.

```python
result = ctx.expanding(column="sales", func="std", order_by="date")
result = ctx.expanding(column="sales", func="std")
```

Both forms → `[NULL, 7.071, 10, 12.91, 15.811]`.

### `var`

Running **sample** variance. The first row is `NULL`. Result column
`sales_expanding_var`.

```python
result = ctx.expanding(column="sales", func="var", order_by="date")
result = ctx.expanding(column="sales", func="var")
```

Both forms → `[NULL, 50, 100, 166.667, 250]`.

### `quantile`

Running interpolated quantile `q` (default `0.5`). Result column
`sales_expanding_q0.5`.

Parameters: `column`, `q` (default `0.5`), `min_periods`, `order_by`.

```python
result = ctx.expanding(column="sales", func="quantile", q=0.5, order_by="date")
result = ctx.expanding(column="sales", func="quantile", q=0.5)
```

Both forms → `[10, 15, 20, 25, 30]`; with `min_periods=3` →
`[NULL, NULL, 20, 25, 30]`.

### `median`

Running median. Result column `sales_expanding_median`.

```python
result = ctx.expanding(column="sales", func="median", order_by="date")
result = ctx.expanding(column="sales", func="median")
```

Both forms → `[10, 15, 20, 25, 30]`.

### `sem`

Running standard error of the mean, matching pandas: sample standard deviation
over `sqrt(count - ddof)`. Result column `sales_expanding_sem`.

```python
result = ctx.expanding(column="sales", func="sem", order_by="date")
result = ctx.expanding(column="sales", func="sem")
```

Both forms → `[NULL, 7.071, 7.071, 7.454, 7.905]`.

### `rank`

Running rank of the current row. `method`, `ascending`, and `pct` work as in
the rolling case. Result column `sales_expanding_rank`.

```python
result = ctx.expanding(column="sales", func="rank", order_by="date")
result = ctx.expanding(column="sales", func="rank")
```

Both forms → `[1, 2, 3, 4, 5]`.

### `nunique`

Running distinct count. Result column `sales_expanding_nunique`.

```python
result = ctx.expanding(column="sales", func="nunique", order_by="date")
result = ctx.expanding(column="sales", func="nunique")
```

Both forms → `[1, 2, 3, 4, 5]`.

### `skew` / `kurt`

Running bias-corrected skewness and excess kurtosis. Both return `NULL` until
enough observations exist (3 for `skew`, 4 for `kurt`); identical values give
`0` / `-3`. Result columns `sales_expanding_skew` / `sales_expanding_kurt`.

```python
result = ctx.expanding(column="sales", func="skew", order_by="date")
result = ctx.on("sales").expanding().kurt(order_by="date")
```

### `first`

First value seen so far (the first row of the table). Result column
`sales_expanding_first`.

```python
result = ctx.expanding(column="sales", func="first", order_by="date")
result = ctx.expanding(column="sales", func="first")
```

Both forms → `[10, 10, 10, 10, 10]`.

### `last`

Last value seen so far — the current row. Result column
`sales_expanding_last`.

```python
result = ctx.expanding(column="sales", func="last", order_by="date")
result = ctx.expanding(column="sales", func="last")
```

Both forms → `[10, 20, 30, 40, 50]`.

### Datetime expanding

`min`, `max`, `mean`, `median`, and `mode` are supported. On a date frame
`date = [Jan 1, Jan 2, Jan 3, Jan 4, Jan 5]`:

```python
result = ctx.expanding(column="date", func="mean", order_by="date", min_periods=2)
result = ctx.expanding(column="date", func="mean", min_periods=2)
```

Both forms → `[NULL, Jan 1 12:00, Jan 2, Jan 2 12:00, Jan 3]`.

---

## EWM

Exponentially-weighted operations are numeric-only and compute a smoothing
factor from exactly one of `com`, `span`, `halflife`, or `alpha`:

| Parameter | Relation |
| --- | --- |
| `alpha` | used directly, must be in `(0, 1]` |
| `com` | `α = 1 / (1 + com)`, `com >= 0` |
| `span` | `α = 2 / (span + 1)`, `span >= 1` |
| `halflife` | `α = 1 - exp(-ln(2) / halflife)`, `halflife > 0` |

If none is provided, `α` defaults to `0.5`. Providing more than one raises.
EWM is computed in-engine with a blockwise SQL scan: a narrow
`(_mf_idx, value, row number)` table is sorted once, block-boundary state is
carried by a recursive CTE in a small side table, and the per-row weighted
sums run in SQL. The
generated columns are merged back by `_mf_idx`; no full-source clone, Python
per-row loop, or `VALUES` payload is built. Result columns are named
`<column>_ewm_<func>`.

Block size adapts to `alpha` (bounded weights); values are accurate to normal
double precision for `alpha <= 0.9`, with relative variance/std error growing
toward ~1e-7 for `alpha` close to 1. For very large frames, pass
`preview_rows=N`; the source still receives every value.

Examples use `sales = [10, 20, 30, 40, 50]`.

### `mean`

Exponentially-weighted mean.

```python
result = ctx.ewm(column="sales", span=3, func="mean", order_by="date")
result = ctx.ewm(column="sales", span=3, func="mean")
```

Both forms → `[10, 16.667, 24.286, 32.667, 41.613]`.

```python
# halflife + recursive form
result = ctx.ewm(column="sales", halflife=2, func="mean", order_by="date", adjust=False)
result = ctx.ewm(column="sales", halflife=2, func="mean", adjust=False)
```

Both forms → `[10, 12.929, 17.929, 24.393, 31.893]`.

### `sum`

Exponentially-weighted sum. Result column `sales_ewm_sum`.

```python
result = ctx.ewm(column="sales", span=3, func="sum", order_by="date")
result = ctx.ewm(column="sales", span=3, func="sum")
```

Both forms → `[10, 25, 42.5, 61.25, 80.625]`.

### `std`

Exponentially-weighted standard deviation. Result column `sales_ewm_std`.

```python
result = ctx.ewm(column="sales", span=3, func="std", order_by="date")
result = ctx.ewm(column="sales", span=3, func="std")
```

Both forms → `[NULL, 7.071, 9.636, 11.772, 13.452]`.

### `var`

Exponentially-weighted variance. Result column `sales_ewm_var`.

```python
result = ctx.ewm(column="sales", span=3, func="var", order_by="date")
result = ctx.ewm(column="sales", span=3, func="var")
```

Both forms → `[NULL, 50, 92.857, 138.571, 180.968]`.

### Multiple functions and shared options

`func` may be a list, and the weighting options apply to every function:

```python
result = ctx.ewm(
    column="sales", halflife=5, func=["mean", "std"], order_by="date",
    adjust=True, ignore_na=False, min_periods=0,
)
result = ctx.ewm(
    column="sales", halflife=5, func=["mean", "std"],
    adjust=True, ignore_na=False, min_periods=0,
)
```

- `adjust=True` normalizes by the decaying weights (pandas `adjust=True`);
  `adjust=False` is the recursive form.
- `ignore_na=True` drops nulls before weighting; `ignore_na=False` keeps their
  position and decays across them.
- `min_periods` suppresses output until that many non-null values are seen.

---

## Fluent Builder

`ctx.on(column)` returns a builder mirroring the pandas chained style. Every
terminal method accepts the same `order_by` (optional) as its direct form.

```python
# with order_by
ctx.on("sales").rolling(7).sum(order_by="date")
ctx.on("sales").rolling(7).mean(order_by="date")
ctx.on("sales").rolling(7).std(order_by="date")
ctx.on("sales").rolling(7).quantile(q=0.5, order_by="date")
ctx.on("sales").expanding(min_periods=3).mean(order_by="date")
ctx.on("sales").ewm(span=3).mean(order_by="date")

# without order_by — natural `_mf_idx` order
ctx.on("sales").rolling(7).sum()
ctx.on("sales").rolling(7).mean()
ctx.on("sales").rolling(7).std()
ctx.on("sales").rolling(7).quantile(q=0.5)
ctx.on("sales").expanding(min_periods=3).mean()
ctx.on("sales").ewm(span=3).mean()
```

`rolling` / `expanding` / `ewm` also work directly on the wrapper and return a
builder when `func` is omitted:

```python
ctx.rolling(column="sales", window=7).mean(order_by="date")
ctx.rolling(column="sales", window=7).mean()
ctx.expanding(column="sales").sum(order_by="date")
ctx.expanding(column="sales").sum()
```

Each builder exposes the named shorthands above plus a generic
`apply(func, order_by=None)` / `await aapply(...)` escape hatch:

```python
ctx.on("sales").rolling(7).apply("mean", order_by="date")
ctx.on("sales").rolling(7).apply("mean")
```

## Return Values and Errors

On a `ContextManager`, public methods return the resulting **DataFrame**
directly. Calling the wrapper class directly (or using a builder's terminal
method) returns the raw operation envelope:

```python
{
    "is_error": False,
    "message": "Rolling mean on 'sales' (window=2)",
    "result": <pandas.DataFrame>,
    "new_table": "ab12CD",                   # source table (ref target)
    "new_columns": ["sales_rolling_mean_w2"],
    "new_column": "sales_rolling_mean_w2",   # single-function calls only
    "mapped_table": "ab12CD",                # source now holds the generated column
    "mapped_columns": ["sales_rolling_mean_w2"],
    "window": 2,
    # multi-function calls also include:
    # "successful_funcs", "skipped_funcs", "failed_funcs", "is_partial", "dtype"
}
```

Invalid operations surface as `OperationError` when the envelope is unwrapped
(e.g. via a `ContextManager`): an unknown function, a function unsupported for
the detected dtype, an EWM on a non-numeric column, an invalid alpha, or a
missing `backend`/`data_id`. When inspecting the raw envelope, check
`is_error` / `error_message` instead.

## Generated Tables

Every window operation **adds its generated column(s) to the source table**
and returns a narrow preview containing only the involved and generated
columns; the deep cache stores a ref to the source.

- **PostgreSQL / DuckDB:** rolling and expanding aggregations add the
  generated column(s) to the source and backfill them in one windowed
  `UPDATE` keyed by `_mf_idx` — the source is never cloned or swapped.
  EWM builds narrow `ordered`/`blocks`/`state` temps, runs the blockwise
  feature SQL, and uses the same backfill by `_mf_idx`. Specials
  (`quantile`/`median`, `sem`, `rank`, `nunique`, `skew`/`kurt`,
  `first`/`last`, datetime stats) use a narrow ordered base and the same
  backfill.
- **ClickHouse:** each operation builds a narrow feature table, joins it to
  the source by `_mf_idx`, and atomically swaps the result into the source
  with `EXCHANGE TABLES`; the temporary feature/wide tables are dropped.
  EWM follows the same pattern for its blockwise feature table.

The envelope gains `mapped_table` (the source table name) and `mapped_columns`
(the generated column names). The cache ref points at the source, so a
cached replay returns the same columns as the first call.

A repeated identical call reuses/replaces the same generated columns; a
different call leaves the other derived columns intact. If a generated name
already exists from a foreign source, the call fails and the source is left
unchanged.

Window operations apply `@record_call`. The generated columns are written to
the source and recorded as a cache `ref`; a hit replays the involved/generated
columns from the source without recomputing. `deep_cache=False` skips the ref
row but still mutates the source.

## Backend Behavior

Window operations support DuckDB, PostgreSQL, and ClickHouse:

- Identifiers are sanitized and quoted before SQL is generated.
- `order_by` may be a single column, a list, or omitted (natural `_mf_idx` order).
- Backend dialect differences (quantile interpolation/indexing,
  stddev/variance names, distinct counts, epoch handling, and ordered value
  extraction) are handled per backend.
- The per-backend SQL is locked by
  `tests/unit/test_window_sql_fingerprint.py` (36 scenarios × 3 backends).

## Errors

Window methods raise `OperationError` (once unwrapped) for validation or
backend failures:

- Unknown function, or a function unsupported for the detected dtype.
- EWM requested on a non-numeric column.
- Invalid EWM parameters (more than one of `com`/`span`/`halflife`/`alpha`, or
  an out-of-range value).
- Missing or renamed `column` / `order_by`.

## API Reference

::: memframe.wrappers.analytix.window.WindowWrapper
    options:
      show_root_heading: true
      show_root_full_path: true
      members:
        - arolling
        - rolling
        - aexpanding
        - expanding
        - aewm
        - ewm
        - "on"

::: memframe.wrappers.analytix.window.WindowBuilderWrapper
    options:
      show_root_heading: true
      show_root_full_path: true
      members:
        - rolling
        - expanding
        - ewm

::: memframe.wrappers.analytix.window.RollingWindowBuilderWrapper
    options:
      show_root_heading: true
      show_root_full_path: true

::: memframe.wrappers.analytix.window.ExpandingWindowBuilderWrapper
    options:
      show_root_heading: true
      show_root_full_path: true

::: memframe.wrappers.analytix.window.EWMWindowBuilderWrapper
    options:
      show_root_heading: true
      show_root_full_path: true
