# GroupBy Window

Source: `src/memframe/wrappers/analytix/groupby_window.py`

`GroupByWindowStatsWrapper` is the public partitioned-window interface
exposed through a `ContextManager`. It provides
pandas-`groupby().rolling()`-like analytics: rolling fixed-size windows
and expanding unbounded windows computed **per group** (`PARTITION BY`
the group columns), in-engine across DuckDB, PostgreSQL, and ClickHouse.
Each call adds the generated column(s) to the source upload table and
returns a separate narrow result table (`<table>__op_<n>`) containing only
the involved and generated columns. The narrow table is what the deep
cache stores.

Users normally call grouped window methods through the unified `groupby`
builder on a dataset context returned by an upload operation. There are
two equivalent entry points:

- a direct call — `dataset.groupby("region").arolling(...)` — with the
  column, window, function, and group columns passed as arguments
  (async; sync via `rolling(...)` on the stats wrapper);
- a pandas-style fluent builder — `dataset.groupby("region").rolling(3,
  order_by="month").mean("sales")`.

```python
dataset = mf.DataFrame(frame)

result = dataset.groupby("region").rolling(3, order_by="month").mean("sales")
result = dataset.groupby("region").expanding(order_by="month").sum("sales")
```

```python
dataset = await mf.aDataFrame(frame)

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

- `src/memframe/core/analytix/groupby_window/` builds and executes
  backend-specific SQL in `GroupbyWindowOps` and its DuckDB / PostgreSQL /
  ClickHouse subclasses. Grouped EWM reuses the ungrouped blockwise EWM
  helpers in `core/analytix/window/base.py`.
- `src/memframe/core/orchestrator/analytix/groupby_window.py` resolves
  the active dataset context, detects the column dtype, maps requested
  functions to dtype-appropriate engine methods, chains multi-function
  requests into one cache table, and applies `@record_call`.
- `src/memframe/wrappers/analytix/groupby_window.py` exposes the
  synchronous and asynchronous builder methods plus the direct API.
- `src/memframe/wrappers/analytix/groupby.py` is the unified `GroupBy`
  facade that routes `ctx.groupby(*columns)` calls to the stats,
  cumulative, and window builders.

## Public API

### Direct

| Synchronous | Asynchronous | Purpose |
| --- | --- | --- |
| `rolling(column, window, func, group_cols, order_by=None, q=0.5, min_periods=None, center=False, closed=None, win_type=None)` | `await arolling(...)` | Grouped rolling window; `func` is a string or list |
| `expanding(column, func, group_cols, order_by=None, q=0.5, min_periods=1)` | `await aexpanding(...)` | Grouped expanding window |
| `ewm(column, group_cols, order_by=None, com/span/halflife/alpha, ...)` | `await aewm(...)` | Grouped EWM (see note below) |

`group_cols` accepts a single column name or a list of names.

### Builder

`groupby(*columns)` returns the unified `GroupBy`; `.rolling(window,
order_by, min_periods=None, center=False, closed=None)` /
`.expanding(min_periods, order_by)` / `.ewm(...)` return
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
    Grouped EWM (`ewm`) uses the same blockwise SQL engine as ungrouped EWM,
    partitioned by the group columns. See the EWM section below.

## Usage Overview

```python
dataset = mf.DataFrame(frame)

sample = dataset.groupby("region").rolling(3, order_by="month").mean("sales")
sample = dataset.groupby("region").expanding(order_by="month").sum("sales")
sample = dataset.groupby("region").rolling(3, order_by="month").agg("sales", ["sum", "mean"])
```

```python
dataset = await mf.aDataFrame(frame)

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
| `window` | `int`, `timedelta`, or offset `str` | Rolling window: rows (`window-1 PRECEDING AND CURRENT ROW`) or a duration (`"3D"`) with a datetime `order_by`. |
| `func` | `str` or `list[str]` | Aggregation(s): `sum`, `mean`/`avg`, `min`, `max`, `count`, `std`/`stddev`/`stddev_samp`, `var`/`variance`/`var_samp`, `quantile`, `median`, `sem`, `rank`, `nunique`, `first`/`first_value`, `last`/`last_value`. |
| `order_by` | `str`, `list[str]`, or `None` | Row ordering inside each group. `None` (default) uses the immutable upload order (`_mf_idx`). |
| `q` | `float` | Quantile level for `quantile` (default `0.5`). |
| `func_kwargs` | `dict` or `None` | Per-function keyword arguments (`ddof`, rank `method`/`ascending`/`pct`, `q`). |
| `center` | `bool` | Rolling only — centre the frame on the current row (default `False`). |
| `closed` | `str` or `None` | Rolling only — `right` (default), `left`, `both`, `neither`. |
| `win_type` | `str` or `None` | Rolling only — scipy window weights (`boxcar`, `triang`, `hann`, …) for weighted `sum`/`mean`. |
| `min_periods` | `int` or `None` | Minimum observations before a value is produced. Rolling default `None` → `window`; expanding default `1`. Must satisfy `0 <= min_periods <= window`. |
| `new_table` | `str` or `None` | Kept for compatibility; the result is written to the source and `new_table` resolves to the source table. |
| `map_feature` | `bool` | Deprecated/ignored. Generated columns are always written back to the source; the envelope carries `mapped_table` / `mapped_columns`. `new_table` is the source table (the ref target). |

With no `order_by`, each group is ordered by the immutable upload index
(`_mf_idx`) and the window runs in that natural order. With an explicit
`order_by`, rows are grouped first, then ordered inside each group by
`order_by..., _mf_idx`; the additional `_mf_idx` key makes ties deterministic.

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
chains every requested function into **one** cache table, each
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
the result column(s) to a copy of the source rows.

`min_periods` defaults to `window` (pandas parity), so the first `window - 1`
rows of each group are `NULL`. Pass `min_periods=1` to aggregate the partial
window instead:

```python
result = dataset.groupby("region").rolling(3, order_by="month").mean("sales")
result = dataset.groupby("region").rolling(3, order_by="month", min_periods=1).mean("sales")
result = dataset.groupby("region").rolling(7, order_by="month").quantile("sales", q=0.9)
```

`center=True` shifts the frame forward `(window - 1) // 2` rows and `closed`
selects the included edges (`right` default, `left`, `both`, `neither`); both
apply per group. `closed="neither"` needs `window >= 2`.

`std`/`var`/`sem` take `ddof` (default `1`, matching pandas), and `rank` takes
`method` (`average` default, `min`, `max`, `first`, `dense`), `ascending`, and
`pct`:

```python
dataset.groupby("region").rolling(7, order_by="month").std("sales", ddof=0)
dataset.groupby("region").rolling(7, order_by="month").rank("sales", method="dense")
```

`win_type` applies scipy window weights per group for weighted `sum`/`mean`:

```python
dataset.groupby("region").rolling(7, order_by="month", win_type="triang").mean("sales")
```

Passing a duration (`window="3D"` / `timedelta`) windows by time per group
instead of by row count; a datetime `order_by` is required and `min_periods`
defaults to `1`. `closed` (`right`/`left`/`both`/`neither`) applies, and the
specials (`quantile`, `median`, `sem`, `rank`, `nunique`, datetime stats) work
per group too:

```python
dataset.groupby("region").rolling("3D", order_by="date").mean("sales")
dataset.groupby("region").rolling("3D", order_by="date").quantile("sales", q=0.9)
dataset.groupby("region").rolling("2D", order_by="date", closed="both").sum("sales")
```

`center`, `win_type`, and `first`/`last` remain unsupported for time windows.

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

## EWM

Grouped EWM runs the same blockwise SQL engine as the ungrouped `ewm`,
partitioned by the group columns:

```python
result = dataset.groupby("region").ewm(span=3, order_by="month").mean("sales")
result = dataset.groupby("region").ewm(
    span=3, order_by="month", adjust=False,
).agg("sales", ["mean", "var"])
```

- `order_by=None` uses natural upload order (`_mf_idx`) **inside each group**.
- Explicit `order_by` is applied per group, with `_mf_idx` as a deterministic
  tie-breaker.
- Supported functions: `mean`, `sum`, `std`, `var`; options: `com`, `span`,
  `halflife`, `alpha`, `adjust`, `ignore_na`, `min_periods`.
- Values follow the unified ungrouped contract:
  - `adjust=False` `sum` equals the EWMA state (same values as `mean`);
  - `adjust=False` `var/std` need two valid points, so the first row is NULL;
  - `adjust=True` + `ignore_na=False` decays state across null rows.
- Result columns: `<column>_ewm_<func>_by_<groups>`.

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

## Source Write-Back

Every grouped window call writes its generated column(s) back to the source
table by replacing the source with the row-aligned window result, then creates
the narrow `involved + generated` result table that `@record_call` stores.

```python
result = dataset.groupby("region").rolling(3, order_by="month").agg(
    "sales", ["mean"]
)
# source now has: region, month, sales, sales_rolling_mean_w3_by_region
# result["new_table"] has: sales, region, month, sales_rolling_mean_w3_by_region
```

Rules:

- A repeated identical call reuses the same generated column(s); the source
  keeps exactly one copy. `deep_cache=False` recomputes but writes no ref row.
- The `map_feature` parameter is retained for compatibility but ignored:
  write-back is always on.
- If the column name already exists from anything else, the call fails and
  the source is left unchanged — drop or rename it first.
- Backend swaps keep the original table name: DuckDB `CREATE OR REPLACE`,
  PostgreSQL create-swap-rename, ClickHouse atomic `EXCHANGE TABLES`.

## Return Values and Errors

Calling the builder (or the wrapper class) directly returns the raw
operation envelope; single-function calls also carry `new_column`:

```python
{
    "is_error": False,
    "message": "Rolling mean on 'sales' grouped by ['region'] (window=2)",
    "result": <pandas.DataFrame>,
    "new_table": "ab12CD",                     # source table (ref target)
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

Every grouped window operation writes its generated column(s) to the source
upload table; the deep cache stores a ref to the source:

- **PostgreSQL / DuckDB:** grouped rolling/expanding compute a narrow
  `(_mf_idx, <windows>)` feature query and backfill the source with one
  `UPDATE` keyed by `_mf_idx`. The source is never cloned or swapped.
  Specials (quantile / nunique / rank / datetime) use the same backfill over
  a narrow ordered base.
- **ClickHouse:** grouped rolling/expanding build a narrow feature table,
  join it to the source by `_mf_idx`, and atomically swap it in with
  `EXCHANGE TABLES`; the temp feature/wide tables are dropped.

Operations apply `@record_call`, so repeat calls are recorded in the
cache registry.

## Backend Behavior

Grouped windows support DuckDB, PostgreSQL, and ClickHouse:

- Identifiers are sanitized and quoted before SQL is generated.
- `group_cols` and `order_by` may each be a single column, a list, or
  (for `order_by`) omitted for natural `_mf_idx` order inside each group.
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
