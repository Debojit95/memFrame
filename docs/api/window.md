# Window

Source: `src/memframe/wrappers/analytix/window.py`

`WindowWrapper` is the public rolling / expanding / exponentially-weighted
interface exposed through a `ContextManager`. Every operation compiles to
backend-native SQL and runs in-engine across DuckDB, PostgreSQL, and
ClickHouse; each call writes a new transient table (`<table>__op_<n>`) and the
source upload table is never mutated in place.

Users normally call window methods directly on a dataset context returned by an
upload operation. There are two equivalent entry points:

- a direct call — `dataset.rolling(...)`, `dataset.expanding(...)`,
  `dataset.ewm(...)` — with `order_by` passed as an argument;
- a pandas-style fluent builder — `dataset.on("sales").rolling(7).mean(...)`.

```python
dataset = mf.upload_df(frame)

rolling = dataset.rolling(column="sales", window=7, func="mean", order_by="date")
expanding = dataset.expanding(column="sales", func="sum", order_by="date")
smoothed = dataset.ewm(column="sales", span=3, func="mean", order_by="date")
fluent = dataset.on("sales").rolling(7).mean(order_by="date")
```

```python
dataset = await mf.aupload_df(frame)

rolling = await dataset.arolling(column="sales", window=7, func="mean", order_by="date")
expanding = await dataset.aexpanding(column="sales", func="sum", order_by="date")
smoothed = await dataset.aewm(column="sales", span=3, func="mean", order_by="date")
```

`window` is top-level (`dataset.rolling`, not `dataset.dt.*`) — `dt` is reserved
for `DateTimeWrapper`.

The lower-level files are implementation details:

- `src/memframe/core/analytix/window/` holds the engine: `base.py` is the
  shared `WindowOps` with DuckDB-flavoured defaults plus dialect hooks
  (`_row_id_col`, `_std_agg`/`_var_agg`, `_datetime_epoch_expr`,
  `_median_epoch_sql`, `_from_epoch_expr`, `_ewm_row_types`); `duckdb.py` /
  `postgres.py` / `clickhouse.py` override those hooks (and the structurally
  divergent operations — PostgreSQL's correlated-subquery quantile and Python
  `nunique` fallback, ClickHouse's single-pass windows); `factory.py` dispatches
  `make_window_ops(adapter)` on `isinstance`.
- `src/memframe/core/orchestrator/analytix/window.py` resolves the active
  dataset context, detects the column dtype, maps the requested functions to
  the dtype-appropriate engine methods, and applies `@record_call`.
- `src/memframe/wrappers/analytix/window.py` exposes the synchronous and
  asynchronous public methods plus the fluent builders.

## Public API

### Rolling

Each rolling method has a synchronous and asynchronous form. Direct rolling
takes `(column, window, func, order_by=None, q=0.5)`; the named shorthands fix
`func`.

| Synchronous (via builder or `rolling`) | Purpose |
| --- | --- |
| `rolling(column, window, func, order_by=None, q=0.5)` / `await arolling(...)` | Generic — `func` may be a string or list |
| `.rolling(column, window).sum(order_by=None)` / `.asum(...)` | Rolling sum |
| `.mean(...)` / `.amean(...)` | Rolling mean |
| `.min(...)` / `.amin(...)` | Rolling minimum |
| `.max(...)` / `.amax(...)` | Rolling maximum |
| `.count(...)` / `.acount(...)` | Rolling non-null count |
| `.std(...)` / `.astd(...)` | Rolling population standard deviation |
| `.var(...)` / `.avar(...)` | Rolling sample variance |
| `.quantile(q=0.5, order_by=None)` / `.aquantile(...)` | Rolling quantile |
| `.sem(...)` / `.asem(...)` | Rolling standard error |
| `.rank(...)` / `.arank(...)` | Rolling rank within the window |
| `.nunique(...)` / `.anunique(...)` | Rolling distinct count |
| `.first(...)` / `.afirst(...)` | First value in the window |
| `.last(...)` / `.alast(...)` | Last value in the window |

### Expanding

Expanding methods mirror rolling but have no `window`; they add `min_periods`
(default `1`). Expansion runs from the first row to the current row.

| Synchronous | Purpose |
| --- | --- |
| `expanding(column, func, order_by=None, q=0.5, min_periods=1)` / `await aexpanding(...)` | Generic |
| `.expanding(column).sum(min_periods=1)` / `.asum(...)` | Running sum |
| `.mean(...)` / `.amean(...)` | Running mean |
| `.min(...)` / `.amin(...)` | Running minimum |
| `.max(...)` / `.amax(...)` | Running maximum |
| `.count(...)` / `.acount(...)` | Running non-null count |
| `.std(...)` / `.astd(...)` | Running sample standard deviation |
| `.var(...)` / `.avar(...)` | Running sample variance |
| `.quantile(q=0.5, min_periods=1)` / `.aquantile(...)` | Running quantile |
| `.sem(...)` / `.asem(...)` | Running standard error |
| `.rank(...)` / `.arank(...)` | Running rank |
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
dataset = mf.upload_df(frame)

sample = dataset.rolling(column="sales", window=7, func="mean", order_by="date")
sample = dataset.rolling(column="sales", window=7, func=["sum", "mean"], order_by="date")
sample = dataset.expanding(column="sales", func="sum", order_by="date", min_periods=3)
sample = dataset.ewm(column="sales", span=3, func="mean", order_by="date")
sample = dataset.on("sales").rolling(7).mean(order_by="date")
```

```python
dataset = await mf.aupload_df(frame)

sample = await dataset.arolling(column="sales", window=7, func="mean", order_by="date")
sample = await dataset.aexpanding(column="sales", func="sum", order_by="date", min_periods=3)
sample = await dataset.aewm(column="sales", span=3, func="mean", order_by="date")
```

`order_by` is optional — omit it to use physical row order:

```python
sample = dataset.rolling(column="sales", window=7, func="mean")
sample = dataset.expanding(column="sales", func="sum", min_periods=3)
sample = dataset.ewm(column="sales", span=3, func="mean")
```

## Common Parameters

| Parameter | Type | Description |
| --- | --- | --- |
| `column` | `str` | Column to window over. |
| `window` | `int` | Rolling window size in rows, **inclusive of the current row** (`window=3` → 2 preceding + current). Must be `>= 1`. |
| `func` | `str` or `list[str]` | Aggregation(s) to apply. A list chains them into a single output table. |
| `order_by` | `str`, `list[str]`, or `None` | Row ordering for the window. `None` (default) uses physical row order. A list orders by multiple columns. |
| `q` | `float` | Quantile in `[0, 1]` for `quantile` (default `0.5`). |
| `min_periods` | `int` | Expanding/EWM only — minimum non-null count before a value is emitted. |

`order_by` accepts a single column or a list:

```python
result = dataset.rolling(column="sales", window=7, func="mean", order_by="date")
result = dataset.rolling(
    column="sales", window=7, func="mean", order_by=["region", "date"]
)
```

With no `order_by`, rows are processed in physical storage order (using each
backend's physical row identifier). Some paths materialize a temporary ordering
column; ClickHouse has no stable row id, so a synthetic row number is generated
in a subquery.

> **Ordered vs. unordered:** every example below is shown twice — once with
> `order_by` and once without. On a freshly uploaded frame the physical order
> matches insertion order, so both forms produce the same values; they diverge
> only once rows have been reordered (for example by a prior sort or merge).

## Type Support

The orchestrator samples the target column and infers one of three kinds, then
maps each requested function to the dtype-appropriate engine method:

| Function | numeric | datetime | categorical |
| --- | --- | --- | --- |
| `sum`, `mean`, `std`, `var`, `quantile`, `sem` | ✅ | — | — |
| `min`, `max`, `count`, `nunique`, `rank`, `first`, `last` | ✅ | ✅ | ✅ |
| `median`, `mode` | — | ✅ | — |

Datetime columns route `min`/`max` through the generic value aggregation, while
`mean`/`median`/`mode` are computed over the column's epoch value and converted
back to a timestamp (or `DATE` for date-only columns). Requesting an
unsupported function for a detected dtype returns a canonical error envelope
rather than raising; EWM on a non-numeric column is rejected the same way.

---

## Rolling

A rolling window of size `w = window` covers the rows from `w - 1` preceding
the current row through the current row. For the first rows the window is
partial (fewer than `w` rows) and the aggregation runs over whatever is
available — except `quantile`, which returns `NULL` until the window is full.

All examples in this section use a numeric frame
`sales = [10, 20, 30, 40, 50]`, `day = [1, 2, 3, 4, 5]`, `window = 3`.

### `sum`

Total of the values in the window (nulls ignored). Result column
`sales_rolling_sum_w3`.

```python
# ordered by a column
result = dataset.rolling(column="sales", window=3, func="sum", order_by="day")

# no order_by — physical row order
result = dataset.rolling(column="sales", window=3, func="sum")
```

Both forms → `[10, 30, 60, 90, 120]`.

### `mean`

Average of the values in the window. Result column `sales_rolling_mean_w3`.

```python
result = dataset.rolling(column="sales", window=3, func="mean", order_by="day")
result = dataset.rolling(column="sales", window=3, func="mean")
```

Both forms → `[10, 15, 20, 30, 40]`.

### `min`

Smallest value in the window. Result column `sales_rolling_min_w3`.

```python
result = dataset.rolling(column="sales", window=3, func="min", order_by="day")
result = dataset.rolling(column="sales", window=3, func="min")
```

Both forms → `[10, 10, 10, 20, 30]`.

### `max`

Largest value in the window. Result column `sales_rolling_max_w3`.

```python
result = dataset.rolling(column="sales", window=3, func="max", order_by="day")
result = dataset.rolling(column="sales", window=3, func="max")
```

Both forms → `[10, 20, 30, 40, 50]`.

### `count`

Number of non-null values in the window. Result column
`sales_rolling_count_w3`.

```python
result = dataset.rolling(column="sales", window=3, func="count", order_by="day")
result = dataset.rolling(column="sales", window=3, func="count")
```

Both forms → `[1, 2, 3, 3, 3]`.

### `std`

**Population** standard deviation over the window. Result column
`sales_rolling_std_w3`.

```python
result = dataset.rolling(column="sales", window=3, func="std", order_by="day")
result = dataset.rolling(column="sales", window=3, func="std")
```

Both forms → `[0, 5, 8.165, 8.165, 8.165]`.

### `var`

**Sample** variance over the window. A single-row window yields `NULL`.
Result column `sales_rolling_var_w3`.

```python
result = dataset.rolling(column="sales", window=3, func="var", order_by="day")
result = dataset.rolling(column="sales", window=3, func="var")
```

Both forms → `[NULL, 50, 100, 100, 100]`.

### `quantile`

Interpolated quantile `q` (default `0.5`, the median) over the window. Unlike
the other rolling functions, it returns `NULL` until the window holds at least
`window` non-null values. Result column `sales_rolling_q0.5_w3`.

Parameters: `column`, `window`, `q` (default `0.5`), `order_by`.

```python
result = dataset.rolling(column="sales", window=3, func="quantile", q=0.5, order_by="day")
result = dataset.rolling(column="sales", window=3, func="quantile", q=0.5)
```

Both forms → `[NULL, NULL, 20, 30, 40]`.

### `sem`

Standard error of the mean: the **sample** standard deviation divided by the
square root of the non-null count over the window. Result column
`sales_rolling_sem_w3`.

```python
result = dataset.rolling(column="sales", window=3, func="sem", order_by="day")
result = dataset.rolling(column="sales", window=3, func="sem")
```

Both forms → `[NULL, 5, 5.774, 5.774, 5.774]`.

### `rank`

Number of rows in the window whose value is `<=` the current value (so the
largest value in a full window gets rank `w`). Result column
`sales_rolling_rank_w3`.

```python
result = dataset.rolling(column="sales", window=3, func="rank", order_by="day")
result = dataset.rolling(column="sales", window=3, func="rank")
```

Both forms → `[1, 2, 3, 3, 3]`.

### `nunique`

Number of distinct values in the window. Result column
`sales_rolling_nunique_w3`.

```python
result = dataset.rolling(column="sales", window=3, func="nunique", order_by="day")
result = dataset.rolling(column="sales", window=3, func="nunique")
```

Both forms → `[1, 2, 3, 3, 3]`.

### `first`

First value in the window (the value `w - 1` rows back, or the first row of the
table). Result column `sales_rolling_first_w3`.

```python
result = dataset.rolling(column="sales", window=3, func="first", order_by="day")
result = dataset.rolling(column="sales", window=3, func="first")
```

Both forms → `[10, 10, 10, 20, 30]`.

### `last`

Last value in the window — always the current row. Result column
`sales_rolling_last_w3`.

```python
result = dataset.rolling(column="sales", window=3, func="last", order_by="day")
result = dataset.rolling(column="sales", window=3, func="last")
```

Both forms → `[10, 20, 30, 40, 50]`.

### Multiple functions

Passing a list chains each function onto the table produced by the previous
one, so all result columns land in a single transient table:

```python
result = dataset.rolling(
    column="sales", window=3, func=["sum", "mean"], order_by="day"
)
result = dataset.rolling(column="sales", window=3, func=["sum", "mean"])
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
result = dataset.rolling(column="date", window=3, func="median", order_by="day")
result = dataset.rolling(column="date", window=3, func="median")
```

Both forms → `[Jan 1, Jan 1 12:00, Jan 2, Jan 3, Jan 4]`.

```python
result = dataset.rolling(column="date", window=3, func="min", order_by="day")
result = dataset.rolling(column="date", window=3, func="min")
```

Both forms → `[Jan 1, Jan 1, Jan 1, Jan 2, Jan 3]`.

Date-only columns are cast back to a date so the result keeps the original
granularity; timestamp columns keep their time component.

### Rank and nunique details

`nunique` is a native windowed distinct count on DuckDB and ClickHouse. On
**PostgreSQL** there is no windowed distinct-count, so it falls back to a
Python pass over the ordered values and writes the counts back in chunks.

---

## Expanding

Expanding operations run from the first row through the current row. There is
no `window`; `min_periods` (default `1`) gates the early rows. All examples use
`sales = [10, 20, 30, 40, 50]`.

### `sum`

Running total. Result column `sales_expanding_sum`.

```python
result = dataset.expanding(column="sales", func="sum", order_by="day")
result = dataset.expanding(column="sales", func="sum")
```

Both forms → `[10, 30, 60, 100, 150]`.

### `mean`

Running average. Result column `sales_expanding_mean`.

```python
result = dataset.expanding(column="sales", func="mean", order_by="day")
result = dataset.expanding(column="sales", func="mean")
```

Both forms → `[10, 15, 20, 25, 30]`.

`min_periods` suppresses output until enough non-null values have been seen:

```python
result = dataset.expanding(
    column="sales", func="mean", order_by="day", min_periods=3
)
result = dataset.expanding(column="sales", func="mean", min_periods=3)
```

Both forms → `[NULL, NULL, 20, 25, 30]`.

### `min`

Running minimum. Result column `sales_expanding_min`.

```python
result = dataset.expanding(column="sales", func="min", order_by="day")
result = dataset.expanding(column="sales", func="min")
```

Both forms → `[10, 10, 10, 10, 10]`.

### `max`

Running maximum. Result column `sales_expanding_max`.

```python
result = dataset.expanding(column="sales", func="max", order_by="day")
result = dataset.expanding(column="sales", func="max")
```

Both forms → `[10, 20, 30, 40, 50]`.

### `count`

Running non-null count. Result column `sales_expanding_count`.

```python
result = dataset.expanding(column="sales", func="count", order_by="day")
result = dataset.expanding(column="sales", func="count")
```

Both forms → `[1, 2, 3, 4, 5]`.

### `std`

Running **sample** standard deviation. The first row is `NULL`. Result column
`sales_expanding_std`.

```python
result = dataset.expanding(column="sales", func="std", order_by="day")
result = dataset.expanding(column="sales", func="std")
```

Both forms → `[NULL, 7.071, 10, 12.91, 15.811]`.

### `var`

Running **sample** variance. The first row is `NULL`. Result column
`sales_expanding_var`.

```python
result = dataset.expanding(column="sales", func="var", order_by="day")
result = dataset.expanding(column="sales", func="var")
```

Both forms → `[NULL, 50, 100, 166.667, 250]`.

### `quantile`

Running interpolated quantile `q` (default `0.5`). Result column
`sales_expanding_q0.5`.

Parameters: `column`, `q` (default `0.5`), `min_periods`, `order_by`.

```python
result = dataset.expanding(column="sales", func="quantile", q=0.5, order_by="day")
result = dataset.expanding(column="sales", func="quantile", q=0.5)
```

Both forms → `[10, 15, 20, 25, 30]`; with `min_periods=3` →
`[NULL, NULL, 20, 25, 30]`.

### `sem`

Running standard error of the mean (sample standard deviation over the square
root of the non-null count). Result column `sales_expanding_sem`.

```python
result = dataset.expanding(column="sales", func="sem", order_by="day")
result = dataset.expanding(column="sales", func="sem")
```

Both forms → `[NULL, 5, 5.774, 6.455, 7.071]`.

### `rank`

Running rank — the count of rows seen so far whose value is `<=` the current
value. Result column `sales_expanding_rank`.

```python
result = dataset.expanding(column="sales", func="rank", order_by="day")
result = dataset.expanding(column="sales", func="rank")
```

Both forms → `[1, 2, 3, 4, 5]`.

### `nunique`

Running distinct count. Result column `sales_expanding_nunique`.

```python
result = dataset.expanding(column="sales", func="nunique", order_by="day")
result = dataset.expanding(column="sales", func="nunique")
```

Both forms → `[1, 2, 3, 4, 5]`.

### `first`

First value seen so far (the first row of the table). Result column
`sales_expanding_first`.

```python
result = dataset.expanding(column="sales", func="first", order_by="day")
result = dataset.expanding(column="sales", func="first")
```

Both forms → `[10, 10, 10, 10, 10]`.

### `last`

Last value seen so far — the current row. Result column
`sales_expanding_last`.

```python
result = dataset.expanding(column="sales", func="last", order_by="day")
result = dataset.expanding(column="sales", func="last")
```

Both forms → `[10, 20, 30, 40, 50]`.

### Datetime expanding

`min`, `max`, `mean`, `median`, and `mode` are supported. On a date frame
`date = [Jan 1, Jan 2, Jan 3, Jan 4, Jan 5]`:

```python
result = dataset.expanding(column="date", func="mean", order_by="day", min_periods=2)
result = dataset.expanding(column="date", func="mean", min_periods=2)
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
Values are computed in NumPy (O(n)) and materialized into the result table.
Result columns are named `<column>_ewm_<func>`.

Examples use `sales = [10, 20, 30, 40, 50]`.

### `mean`

Exponentially-weighted mean.

```python
result = dataset.ewm(column="sales", span=3, func="mean", order_by="day")
result = dataset.ewm(column="sales", span=3, func="mean")
```

Both forms → `[10, 16.667, 24.286, 32.667, 41.613]`.

```python
# halflife + recursive form
result = dataset.ewm(column="sales", halflife=2, func="mean", order_by="day", adjust=False)
result = dataset.ewm(column="sales", halflife=2, func="mean", adjust=False)
```

Both forms → `[10, 12.929, 17.929, 24.393, 31.893]`.

### `sum`

Exponentially-weighted sum. Result column `sales_ewm_sum`.

```python
result = dataset.ewm(column="sales", span=3, func="sum", order_by="day")
result = dataset.ewm(column="sales", span=3, func="sum")
```

Both forms → `[10, 25, 42.5, 61.25, 80.625]`.

### `std`

Exponentially-weighted standard deviation. Result column `sales_ewm_std`.

```python
result = dataset.ewm(column="sales", span=3, func="std", order_by="day")
result = dataset.ewm(column="sales", span=3, func="std")
```

Both forms → `[NULL, 7.071, 9.636, 11.772, 13.452]`.

### `var`

Exponentially-weighted variance. Result column `sales_ewm_var`.

```python
result = dataset.ewm(column="sales", span=3, func="var", order_by="day")
result = dataset.ewm(column="sales", span=3, func="var")
```

Both forms → `[NULL, 50, 92.857, 138.571, 180.968]`.

### Multiple functions and shared options

`func` may be a list, and the weighting options apply to every function:

```python
result = dataset.ewm(
    column="sales", halflife=5, func=["mean", "std"], order_by="day",
    adjust=True, ignore_na=False, min_periods=0,
)
result = dataset.ewm(
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

`dataset.on(column)` returns a builder mirroring the pandas chained style. Every
terminal method accepts the same `order_by` (optional) as its direct form.

```python
# with order_by
dataset.on("sales").rolling(7).sum(order_by="date")
dataset.on("sales").rolling(7).mean(order_by="date")
dataset.on("sales").rolling(7).std(order_by="date")
dataset.on("sales").rolling(7).quantile(q=0.5, order_by="date")
dataset.on("sales").expanding(min_periods=3).mean(order_by="date")
dataset.on("sales").ewm(span=3).mean(order_by="date")

# without order_by — physical row order
dataset.on("sales").rolling(7).sum()
dataset.on("sales").rolling(7).mean()
dataset.on("sales").rolling(7).std()
dataset.on("sales").rolling(7).quantile(q=0.5)
dataset.on("sales").expanding(min_periods=3).mean()
dataset.on("sales").ewm(span=3).mean()
```

`rolling` / `expanding` / `ewm` also work directly on the wrapper and return a
builder when `func` is omitted:

```python
dataset.rolling(column="sales", window=7).mean(order_by="date")
dataset.rolling(column="sales", window=7).mean()
dataset.expanding(column="sales").sum(order_by="date")
dataset.expanding(column="sales").sum()
```

Each builder exposes the named shorthands above plus a generic
`apply(func, order_by=None)` / `await aapply(...)` escape hatch:

```python
dataset.on("sales").rolling(7).apply("mean", order_by="date")
dataset.on("sales").rolling(7).apply("mean")
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
    "new_table": "ab12CD__op_3",
    "new_columns": ["sales_rolling_mean_w2"],
    "new_column": "sales_rolling_mean_w2",   # single-function calls only
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

Every window operation is non-destructive to the source upload table. It
creates a new transient table holding the source rows plus the generated
column(s), then returns a preview of it:

- **PostgreSQL / DuckDB:** the source is cloned, each result column is added,
  then the window values are written back per physical row. Some shapes
  (quantile, rank, nunique, ewm) build the result in a single pass instead.
- **ClickHouse:** each operation builds the result table in a single pass —
  window functions are computed inline. EWM stages its values in a temporary
  in-memory table, then joins it in.

Window operations apply `@record_call`, so repeat calls are recorded in the
transient registry (L1). With `MemFrame(deep_cache=True)` the result tables are
persisted and replayed on a hit (L2).

## Backend Behavior

Window operations support DuckDB, PostgreSQL, and ClickHouse:

- Identifiers are sanitized and quoted before SQL is generated.
- `order_by` may be a single column, a list, or omitted (physical order).
- Backend dialect differences (quantile, stddev/variance, and distinct-count
  implementations) are handled per backend; PostgreSQL's windowed distinct-count
  falls back to Python.
- The per-backend SQL is locked by
  `tests/unit/test_window_sql_fingerprint.py` (21 scenarios × 3 backends).

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
