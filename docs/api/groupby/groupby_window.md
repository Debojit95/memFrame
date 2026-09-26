# GroupBy Window

Source: `src/memframe/wrappers/analytix/groupby_window.py`

`GroupByWindowStatsWrapper` is the public partitioned-window interface
exposed through a `ContextManager`. It provides
pandas-`groupby().rolling()`-like analytics: rolling fixed-size windows
and expanding unbounded windows computed **per group** (`PARTITION BY`
the group columns), in-engine across DuckDB, PostgreSQL, and ClickHouse.
Each call writes a new transient table (`<table>__op_<n>`) and the source
upload table is never mutated in place.

Users normally call grouped window methods through the unified `groupby`
builder on a dataset context returned by an upload operation. There are
two equivalent entry points:

- a direct call — `dataset.groupby("region").arolling(...)` — with the
  column, window, function, and group columns passed as arguments
  (async; sync via `rolling(...)` on the stats wrapper);
- a pandas-style fluent builder — `dataset.groupby("region").rolling(3,
  order_by="month").mean("sales")`.

```python
dataset = mf.upload_df(frame)

result = dataset.groupby("region").rolling(3, order_by="month").mean("sales")
result = dataset.groupby("region").expanding(order_by="month").sum("sales")
```

```python
dataset = await mf.aupload_df(frame)

result = await dataset.groupby("region").arolling(
    column="sales", window=3, func="mean",
    group_cols="region", order_by="month",
)
```

!!! note
    A flat `dataset.rolling(...)` resolves to the ungrouped
    `WindowWrapper.rolling(column, window, func, ...)` (it is registered
    first and takes no group columns). The grouped form above is only
    reachable through the `groupby(...)` builder or
    `GroupByWindowStatsWrapper` directly — the same arrangement as
    grouped `event_rate` on the Stats page.

The lower-level files are implementation details:

- `src/memframe/core/analytix/groupby_window.py` builds and executes
  backend-specific SQL in a single `GroupbyWindowOps` class that extends
  `WindowOps` (`isinstance` branches for DuckDB / PostgreSQL /
  ClickHouse).
- `src/memframe/core/orchestrator/analytix/groupby_window.py` resolves
  the active dataset context, detects the column dtype, maps requested
  functions to dtype-appropriate engine methods, chains multi-function
  requests into one transient table, and applies `@record_call`.
- `src/memframe/wrappers/analytix/groupby_window.py` exposes the
  synchronous and asynchronous builder methods plus the direct API.
- `src/memframe/wrappers/analytix/groupby.py` is the unified `GroupBy`
  facade that routes `ctx.groupby(*columns)` calls to the stats,
  cumulative, and window builders.

## Public API

### Direct

| Synchronous | Asynchronous | Purpose |
| --- | --- | --- |
| `rolling(column, window, func, group_cols, order_by=None, q=0.5)` | `await arolling(...)` | Grouped rolling window; `func` is a string or list |
| `expanding(column, func, group_cols, order_by=None, q=0.5, min_periods=1)` | `await aexpanding(...)` | Grouped expanding window |
| `ewm(column, group_cols, order_by=None, com/span/halflife/alpha, ...)` | `await aewm(...)` | Grouped EWM (see note below) |

`group_cols` accepts a single column name or a list of names.

### Builder

`groupby(*columns)` returns the unified `GroupBy`; `.rolling(window,
order_by)` / `.expanding(min_periods, order_by)` / `.ewm(...)` return
fluent builders. Each terminal method has a synchronous and asynchronous
form (`mean` / `amean`, …):

| Rolling / Expanding terminals | Purpose |
| --- | --- |
| `sum(column)` / `mean(column)` | Running sum / mean within each group |
| `min(column)` / `max(column)` | Running minimum / maximum |
| `count(column)` | Running non-null count |
| `std(column)` / `var(column)` | Running sample stddev / variance |
| `quantile(column, q=0.5)` | Running quantile |
| `sem(column)` | Running standard error |
| `rank(column)` | Running rank within the window |
| `nunique(column)` | Running distinct count |
| `first(column)` / `last(column)` | First / last value in the window |
| `agg(column, funcs)` | Multi-aggregation chained into one table |

!!! note
    Grouped EWM (`ewm`) is currently unavailable: the engine path
    depends on helpers (`_ensure_row_id`, `_alpha_from_params`,
    `_compute_ewm_array`) that did not survive the window core split.
    Rolling and expanding are fully supported; EWM needs an engine port.

## Usage Overview

```python
dataset = mf.upload_df(frame)

sample = dataset.groupby("region").rolling(3, order_by="month").mean("sales")
sample = dataset.groupby("region").expanding(order_by="month").sum("sales")
sample = dataset.groupby("region").rolling(3, order_by="month").agg("sales", ["sum", "mean"])
```

```python
dataset = await mf.aupload_df(frame)

sample = await dataset.groupby("region").arolling(
    column="sales", window=3, func="mean",
    group_cols="region", order_by="month",
)
sample = await dataset.groupby("region").aexpanding(
    column="sales", func="sum", group_cols="region", order_by="month",
)
```

`order_by` is optional — omit it to use physical row order:

```python
sample = dataset.groupby("region").rolling(3).mean("sales")
sample = dataset.groupby("region").expanding().sum("sales")
```

## Common Parameters

| Parameter | Type | Description |
| --- | --- | --- |
| `column` | `str` | Value column the window runs over. |
| `group_cols` | `str` or `list[str]` | Column(s) partitioning the window. At least one is required. |
| `window` | `int` | Rolling window size in rows (`ROWS BETWEEN window-1 PRECEDING AND CURRENT ROW`). |
| `func` | `str` or `list[str]` | Aggregation(s): `sum`, `mean`/`avg`, `min`, `max`, `count`, `std`/`stddev`/`stddev_samp`, `var`/`variance`/`var_samp`, `quantile`, `sem`, `rank`, `nunique`, `first`/`first_value`, `last`/`last_value`. |
| `order_by` | `str`, `list[str]`, or `None` | Row ordering inside each group. `None` (default) uses physical row order. |
| `q` | `float` | Quantile level for `quantile` (default `0.5`). |
| `min_periods` | `int` | Minimum rows before an expanding value is produced (default `1`). |
| `new_table` | `str` or `None` | Explicit name for the output table (chaining). Auto-generated as `<table>__op_<n>` when omitted. |

With no `order_by`, rows accumulate in physical storage order
(PostgreSQL `ctid`, DuckDB `rowid`). ClickHouse has no stable row id, so
a synthetic `_mf_row_num` (`ROW_NUMBER() OVER()`) is generated in a
subquery to give the window a deterministic order.

Result columns are named
`<column>_rolling_<stat>_w<window>_by_<groups>` (rolling) or
`<column>_expanding_<stat>_by_<groups>` (expanding); duplicate requests
get a numeric suffix (`_2`, …).

## Multicolumn Group-By

Pass several group columns to partition by their combination — the
window restarts at every distinct key tuple:

```python
result = dataset.groupby("region", "month").rolling(3, order_by="day").sum("sales")
# column: sales_rolling_sum_w3_by_region_month
```

```python
result = await dataset.groupby("region", "month").aexpanding(
    column="sales", func="mean",
    group_cols=["region", "month"], order_by="day",
)
```

Direct calls accept the same shapes — a single name or a list:

```python
result = dataset.arolling(
    column="sales", window=3, func=["sum", "mean"],
    group_cols=["region", "month"], order_by="day",
)
```

Multi-aggregation (`func` as a list, or builder `.agg(column, funcs)`)
chains every requested function into **one** transient table, each
reading the previous call's output:

```python
result = dataset.groupby("region").rolling(3, order_by="month").agg(
    "sales", ["sum", "mean", "std"]
)
# columns: sales_rolling_sum_w3_by_region,
#          sales_rolling_mean_w3_by_region,
#          sales_rolling_std_w3_by_region
```

## Rolling

Each rolling call computes `AGG(col) OVER (PARTITION BY <groups> ORDER
BY <order> ROWS BETWEEN window-1 PRECEDING AND CURRENT ROW)` and appends
the result column(s) to a copy of the source rows:

```python
result = dataset.groupby("region").rolling(3, order_by="month").mean("sales")
result = dataset.groupby("region").rolling(7, order_by="month").quantile("sales", q=0.9)
```

`func` also accepts backend spellings directly (`AVG`, `STDDEV_SAMP`,
`first_value`, …); unknown names fail the whole call with `is_error
True`, while dtype-unsupported names are reported per-function in
`skipped_funcs`/`failed_funcs` with `is_partial True`.

## Expanding

Expanding mirrors rolling with an unbounded frame (first row of each
group through the current row) plus `min_periods`:

```python
result = dataset.groupby("region").expanding(order_by="month").sum("sales")
result = dataset.groupby("region").expanding(order_by="month", min_periods=3).mean("sales")
```

## Specials: quantile, nunique, rank, sem

These use backend-specific execution but identical builder signatures:

- `quantile`: DuckDB windowed `QUANTILE_CONT`; PostgreSQL and
  ClickHouse fall back to a correlated subquery over a `ROW_NUMBER()`
  base (`PERCENTILE_CONT … WITHIN GROUP` / `quantile(q)(…)`).
- `nunique`: DuckDB windowed `COUNT(DISTINCT …)`; PostgreSQL and
  ClickHouse use the correlated-subquery form (`countDistinct` on
  ClickHouse).
- `rank`: correlated `COUNT(*)` subquery on all backends.
- `sem`: windowed `stddev / sqrt(count)` (`stddevSamp` on ClickHouse).

The orchestrator detects the value column dtype (numeric / datetime /
categorical) and maps each requested function to the dtype-appropriate
engine method; datetime columns support `min`/`max`/`mean`/`median`/
`mode`/`count`/`nunique`/`rank`/`first`/`last` via epoch conversion.

## Return Values and Errors

Calling the builder (or the wrapper class) directly returns the raw
operation envelope; single-function calls also carry `new_column`:

```python
{
    "is_error": False,
    "message": "Rolling mean on 'sales' grouped by ['region'] (window=2)",
    "result": <pandas.DataFrame>,
    "new_table": "ab12CD__op_3",
    "new_columns": ["sales_rolling_mean_w2_by_region"],
    "new_column": "sales_rolling_mean_w2_by_region",   # single-function calls only
    "window": 2,
    "group_cols": ["region"],
    # multi-function calls also include:
    # "successful_funcs", "skipped_funcs", "failed_funcs", "is_partial", "dtype"
}
```

On a `ContextManager`, public methods return the resulting **DataFrame**
directly; invalid operations surface as `OperationError` when unwrapped.
An unknown function, a function unsupported for the detected dtype, or a
missing column fails the call (`message == ""` on the raw envelope).

## Generated Tables

Every grouped window operation is non-destructive to the source upload
table. It creates a new transient table holding the source rows plus the
generated column(s), then returns a preview of it:

- **PostgreSQL / DuckDB:** `CREATE TABLE … AS SELECT *,
  <window> AS <target> FROM …` (quantile / nunique / rank use a
  `ROW_NUMBER()` `__base` subquery; no-`order_by` calls stage a rowid
  table first).
- **ClickHouse:** `CREATE TABLE … ENGINE = MergeTree() ORDER BY tuple()
  AS SELECT …` — windows computed inline; without `order_by` the source
  is wrapped in a `_mf_row_num` subquery whose helper column is excluded
  from the output.

Operations apply `@record_call`, so repeat calls are recorded in the
transient registry.

## Backend Behavior

Grouped windows support DuckDB, PostgreSQL, and ClickHouse:

- Identifiers are sanitized and quoted before SQL is generated.
- `group_cols` and `order_by` may each be a single column, a list, or
  (for `order_by`) omitted for physical order.
- Backend dialect differences (sample stddev/variance names,
  quantile/nunique/rank execution shapes, datetime epoch handling) are
  handled per backend.
- The per-backend SQL is locked by
  `tests/unit/test_groupby_window_sql_fingerprint.py` (6 scenarios × 3
  backends).

## Errors

Grouped window methods raise `OperationError` (once unwrapped) for
validation or backend failures:

- No group-by column, no aggregation function, or an invalid function entry.
- Unknown rolling function, or a function unsupported for the detected dtype.
- Missing or renamed `column` / `order_by` / group column (raised out of
  dtype detection).
- Missing `backend` / `data_id` at the ops layer, or an unsupported backend.

## API Reference

::: memframe.wrappers.analytix.groupby_window.GroupByWindowStatsWrapper
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
        - groupby

::: memframe.wrappers.analytix.groupby_window.GroupByWindowWrapper
    options:
      show_root_heading: true
      show_root_full_path: true
      members:
        - rolling
        - expanding
        - ewm

::: memframe.wrappers.analytix.groupby_window.GroupByRollingWrapper
    options:
      show_root_heading: true
      show_root_full_path: true

::: memframe.wrappers.analytix.groupby_window.GroupByExpandingWrapper
    options:
      show_root_heading: true
      show_root_full_path: true

::: memframe.wrappers.analytix.groupby_window.GroupByEWMWrapper
    options:
      show_root_heading: true
      show_root_full_path: true
