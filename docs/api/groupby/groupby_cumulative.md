# GroupBy Cumulative

Source: `src/memframe/wrappers/analytix/groupby_cumulative.py`

`GroupByCumulativeWrapper` is the public group-wise running-computation
interface exposed through a `ContextManager`. It provides
pandas-`groupby().cumsum()`-like analytics: cumulative sum, product,
minimum, maximum, mean, count, standard deviation, and variance, each
computed over a window partitioned by the group columns and ordered by a
column you choose (or the physical row order by default).

Users normally call group-by cumulative methods through the unified
`groupby` builder on a dataset context returned by an upload operation:

```python
dataset = mf.upload_df(frame)

result = dataset.groupby("region").cumsum("revenue", order_col="month")
result = dataset.groupby("region").cummean("score")
```

```python
dataset = await mf.aupload_df(frame)

result = await dataset.groupby("region").acumsum("revenue", order_col="month")
```

!!! note
    A flat `dataset.cumsum(...)` resolves to the ungrouped
    `CumulativeWrapper.cumsum(column, order_col, target_col)` (it is
    registered first and takes no group columns). The grouped form above
    is only reachable through the `groupby(...)` builder or
    `GroupByCumulativeWrapper` directly — the same arrangement as
    grouped `event_rate` on the Stats page.

The lower-level files are implementation details:

- `src/memframe/core/analytix/groupby_cumulative.py` builds and executes
  backend-specific SQL in a single `GroupbyCumulativeOps` class
  (`isinstance` branches for DuckDB / PostgreSQL / ClickHouse).
- `src/memframe/core/orchestrator/analytix/groupby_cumulative.py`
  resolves the active dataset context and applies `@record_call`.
- `src/memframe/wrappers/analytix/groupby_cumulative.py` exposes the
  synchronous and asynchronous builder methods plus the direct async API.
- `src/memframe/wrappers/analytix/groupby.py` is the unified `GroupBy`
  facade that routes `ctx.groupby(*columns)` calls to the stats,
  cumulative, and (future) window builders.

## Public API

Each operation has a synchronous and asynchronous form. All take
`(column, order_col=None, target_col=None)`:

| Synchronous | Asynchronous | Purpose |
| --- | --- | --- |
| `cumsum(column, order_col=None, target_col=None)` | `await acumsum(...)` | Group-wise running sum |
| `cumprod(column, order_col=None, target_col=None)` | `await acumprod(...)` | Group-wise running product |
| `cummax(column, order_col=None, target_col=None)` | `await acummax(...)` | Group-wise running maximum |
| `cummin(column, order_col=None, target_col=None)` | `await acummin(...)` | Group-wise running minimum |
| `cummean(column, order_col=None, target_col=None)` | `await acummean(...)` | Group-wise running mean |
| `cumcount(column, order_col=None, target_col=None)` | `await acumcount(...)` | Group-wise running non-null count |
| `cumstd(column, order_col=None, target_col=None)` | `await acumstd(...)` | Group-wise running population stddev |
| `cumvar(column, order_col=None, target_col=None)` | `await acumvar(...)` | Group-wise running population variance |

Public builder methods return the raw operation envelope (see Return
Values below). Invalid operations surface `is_error` envelopes (or raise
`OperationError` once unwrapped via a `ContextManager`).

## Usage Overview

```python
dataset = mf.upload_df(frame)

sample = dataset.groupby("region").cumsum("revenue", order_col="month")
sample = dataset.groupby("region").cummean("score", target_col="running_avg")
sample = dataset.groupby("region", "month").cumcount("user_id")
```

```python
dataset = await mf.aupload_df(frame)

sample = await dataset.groupby("region").acumsum("revenue", order_col="month")
sample = await dataset.groupby("region").acummean("score")
```

`order_col` is optional — omit it to use physical row order:

```python
sample = dataset.groupby("region").cumsum("revenue")
sample = dataset.groupby("region").cummean("score")
```

## Common Parameters

| Parameter | Type | Description |
| --- | --- | --- |
| `column` | `str` | Numeric column to accumulate. |
| `order_col` | `str`, `list[str]`, or `None` | Row ordering inside each group. `None` (default) uses physical row order. A list orders by multiple columns. |
| `target_col` | `str` or `None` | Name for the result column. Auto-generated as `cum_<column>_<op>_by_<groups>` with `_order_by_<orders>` appended when `order_col` is given (e.g. `cum_revenue_sum_by_region_order_by_month`). |

`group_cols` are fixed at builder creation (`groupby("region", "month")`)
and accept one or more columns.

With no `order_col`, rows accumulate in physical storage order
(PostgreSQL `ctid`, DuckDB `rowid`). ClickHouse has no stable row id, so
a synthetic `_mf_row_num` (`ROW_NUMBER() OVER()`) is generated in a
subquery to give the window a deterministic order, then stripped from
the output via `SELECT * EXCEPT(_mf_row_num)` so chained operations stay
clean.

## Running Sum and Product

### `cumsum`

Running sum within each group via `SUM(col) OVER (PARTITION BY …
ORDER BY … ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)`.

```python
result = dataset.groupby("region").cumsum("revenue", order_col="month")
```

```python
result = await dataset.groupby("region").acumsum("revenue", order_col="month")
```

### `cumprod`

Running product within each group via `EXP(SUM(LN(NULLIF(col, 0)))
OVER ({window_spec}))`. Zeros are skipped rather than zeroing the tail.

```python
result = dataset.groupby("region").cumprod("growth_factor", order_col="month")
```

Negative inputs abort the whole operation with `is_error True` (engines
reject `LN` of a negative). Restrict `cumprod` to non-negative columns.

## Running Min, Max, and Mean

### `cummax`, `cummin`

Running extrema within each group via `MAX(col)` / `MIN(col)` over the
same partitioned window frame.

```python
result = dataset.groupby("region").cummax("price", order_col="day")
result = dataset.groupby("region").cummin("price", order_col="day")
```

### `cummean`

Running mean within each group via `AVG(col)`.

```python
result = dataset.groupby("region").cummean("score", target_col="running_avg")
```

## Running Count, Stddev, and Variance

### `cumcount`

Running non-null count within each group via `COUNT(col)`.

```python
result = dataset.groupby("region").cumcount("user_id", order_col="signup_date")
```

The result column is `BIGINT` on PostgreSQL/DuckDB and `Int64` on
ClickHouse (pass `target_type` through the direct API to override).

### `cumstd`, `cumvar`

Running **population** (not sample) statistics within each group:
`STDDEV_POP(col)` / `VAR_POP(col)` on PostgreSQL and DuckDB,
`stddevPop(col)` / `varPop(col)` on ClickHouse.

```python
result = dataset.groupby("region").cumstd("score", order_col="month")
result = dataset.groupby("region").cumvar("score", order_col="month")
```

## Return Values and Errors

Calling the builder (or the wrapper class) directly returns the raw
operation envelope:

```python
{
    "is_error": False,
    "message": "Cumulative sum of 'revenue' grouped by [region] ...",
    "result": <pandas.DataFrame>,
    "new_table": "ab12CD__op_3",
    "new_columns": ["cum_revenue_sum_by_region"],
    "group_cols": ["region"],
    "operation": "sum",
}
```

Invalid operations return `is_error` envelopes (and raise
`OperationError` once unwrapped via a `ContextManager`): a missing
column, a missing `backend`/`data_id`, or an unsupported backend.

## Generated Tables

Every group-by cumulative operation is non-destructive to the source
upload table. It creates a new transient table holding the source rows
plus the result column, then returns a preview of it:

- **PostgreSQL / DuckDB:** `CREATE TABLE … AS SELECT *,
  <window> AS <target> FROM …` with `PARTITION BY` + physical-row
  fallback ordering.
- **ClickHouse:** the same single-pass shape with `ENGINE = MergeTree()
  ORDER BY tuple()`; without `order_col` the source is wrapped in a
  `ROW_NUMBER()` subquery whose helper column is excluded from the
  output.

Operations apply `@record_call`, so repeat calls are recorded in the
transient registry.

## Backend Behavior

Group-by cumulative supports DuckDB, PostgreSQL, and ClickHouse:

- Identifiers are sanitized and quoted before SQL is generated.
- `group_cols` may be a single column or a list; at least one is required.
- `order_col` may be a single column, a list, or omitted (physical order;
  synthetic `_mf_row_num` on ClickHouse).
- `STDDEV_POP`/`VAR_POP` map to ClickHouse-native `stddevPop`/`varPop`.

## Errors

Group-by cumulative methods surface `is_error` envelopes (or raise
`OperationError` once unwrapped) for validation or backend failures:

- Missing or renamed `column` / `order_col` / group column.
- `cumprod` on a column containing negatives fails the whole operation.
- Missing `backend` / `data_id` at the ops layer, or an unsupported backend.

## API Reference

::: memframe.wrappers.analytix.groupby_cumulative.GroupByCumulativeWrapper
    options:
      show_root_heading: true
      show_root_full_path: true
      members:
        - groupby
        - cumsum
        - cumprod
        - cummax
        - cummin
        - cummean
        - cumcount
        - cumstd
        - cumvar

::: memframe.wrappers.analytix.groupby_cumulative.GroupByCumulativeBuilderWrapper
    options:
      show_root_heading: true
      show_root_full_path: true
      members:
        - acumsum
        - cumsum
        - acumprod
        - cumprod
        - acummax
        - cummax
        - acummin
        - cummin
        - acummean
        - cummean
        - acumcount
        - cumcount
        - acumstd
        - cumstd
        - acumvar
        - cumvar
