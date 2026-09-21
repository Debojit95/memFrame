# Cumulative

Source: `src/memframe/wrappers/analytix/cumulative.py`

`CumulativeWrapper` is the public cumulative interface exposed through a
`ContextManager`. It provides pandas-`expanding()`-like running computations:
cumulative sum, product, minimum, maximum, mean, count, standard deviation,
and variance, each computed over a window ordered by a column you choose (or
the physical row order by default).

Users normally call cumulative methods directly on a dataset context returned
by an upload operation:

```python
dataset = mf.upload_df(frame)
result = dataset.cumsum("revenue", order_col="month")
```

```python
dataset = await mf.aupload_df(frame)
result = await dataset.acumsum("revenue", order_col="month")
```

`cumulative` is top-level (`dataset.cumsum`, not `dataset.dt.*`) — `dt` is
reserved for `DateTimeWrapper`.

The lower-level files are implementation details:

- `src/memframe/core/analytix/cumulative/` builds and executes backend-specific
  SQL (`base.py` holds the DuckDB-flavoured shared engine plus dialect hooks,
  `postgres.py`/`clickhouse.py` override per backend, `factory.py` dispatches
  `make_cumulative_ops(adapter)` on `isinstance`).
- `src/memframe/core/orchestrator/analytix/cumulative.py` resolves the active
  dataset context and passes persistence metadata.
- `src/memframe/wrappers/analytix/cumulative.py` exposes synchronous and
  asynchronous public methods.

## Public API

Every cumulative operation has synchronous and asynchronous forms. All take
`(column, order_col=None, target_col=None)`:

| Synchronous | Asynchronous | Purpose |
| --- | --- | --- |
| `cumsum(column, order_col=None, target_col=None)` | `await acumsum(...)` | Running sum |
| `cumprod(column, order_col=None, target_col=None)` | `await acumprod(...)` | Running product |
| `cummax(column, order_col=None, target_col=None)` | `await acummax(...)` | Running maximum |
| `cummin(column, order_col=None, target_col=None)` | `await acummin(...)` | Running minimum |
| `cummean(column, order_col=None, target_col=None)` | `await acummean(...)` | Running mean |
| `cumcount(column, order_col=None, target_col=None)` | `await acumcount(...)` | Running non-null count |
| `cumstd(column, order_col=None, target_col=None)` | `await acumstd(...)` | Running population stddev |
| `cumvar(column, order_col=None, target_col=None)` | `await acumvar(...)` | Running population variance |

Public methods return the resulting DataFrame directly. Invalid operations
raise `OperationError`.

## Usage Overview

```python
dataset = mf.upload_df(frame)

sample = dataset.cumsum("revenue", order_col="month")
sample = dataset.cummean("score", target_col="running_avg")
sample = dataset.cumcount("user_id")
```

```python
dataset = await mf.aupload_df(frame)

sample = await dataset.acumsum("revenue", order_col="month")
sample = await dataset.acummean("score", target_col="running_avg")
```

`order_col` is optional — omit it to use physical row order:

```python
sample = dataset.cumsum("revenue")
sample = dataset.cummean("score")
sample = dataset.cumcount("user_id")
```

## Common Parameters

| Parameter | Type | Description |
| --- | --- | --- |
| `column` | `str` | Numeric column to accumulate. |
| `order_col` | `str`, `list[str]`, or `None` | Row ordering for the window. `None` (default) uses physical row order. A list orders by multiple columns. |
| `target_col` | `str` or `None` | Name for the result column. Auto-generated as `cum_<column>_<op>` if omitted (e.g. `cum_revenue_sum`). |

`order_col` accepts a single column or a list:

```python
result = dataset.cumsum("revenue", order_col="month")
result = dataset.cumsum("revenue", order_col=["region", "month"])
```

With no `order_col`, rows accumulate in physical storage order (PostgreSQL
`ctid`, DuckDB `rowid`). ClickHouse has no stable row id, so a synthetic
`_ch_rowid` (`ROW_NUMBER() OVER()`) is generated in a subquery to give the
window a deterministic order.

## Running Sum and Product

### `cumsum`

Running sum via `SUM(col) OVER (ORDER BY … ROWS BETWEEN UNBOUNDED PRECEDING
AND CURRENT ROW)`.

```python
result = dataset.cumsum("revenue", order_col="month")
```

```python
result = await dataset.acumsum("revenue", order_col="month", target_col="ytd")
```

Example `x=[1.0, 2.0, 3.0]` → `cum_x_sum=[1.0, 3.0, 6.0]`.

### `cumprod`

Running product via `EXP(SUM(LN(NULLIF(col, 0))) OVER (…))`. Zeros are
skipped rather than zeroing the tail (`x=[2.0, 0.0, 4.0]` →
`cum_x_prod≈[2.0, 2.0, 8.0]`, floating-point dust applies).

```python
result = dataset.cumprod("growth_factor", order_col="month")
```

```python
result = await dataset.acumprod("growth_factor", order_col="month")
```

Negative inputs abort the whole operation with `is_error True` (engines
reject `LN` of a negative — e.g. DuckDB's *"cannot take logarithm of a
negative number"*). Restrict `cumprod` to non-negative columns.

## Running Min, Max, and Mean

### `cummax`, `cummin`

Running extrema via `MAX(col)` / `MIN(col)` over the same window frame.

```python
result = dataset.cummax("price", order_col="day")
result = dataset.cummin("price", order_col="day")
```

```python
result = await dataset.acummax("price", order_col="day")
result = await dataset.acummin("price", order_col="day")
```

### `cummean`

Running mean via `AVG(col)`.

```python
result = dataset.cummean("score", target_col="running_avg")
```

```python
result = await dataset.acummean("score", target_col="running_avg")
```

`NULL` values are ignored by the aggregation but still occupy a row, so a
`NULL` row repeats the previous running value (`x=[2.0, 0.0, -3.0, NULL,
4.0]` → `cum_x_mean=[2.0, 1.0, -0.33, -0.33, 0.75]`).

## Running Count, Stddev, and Variance

### `cumcount`

Running non-null count via `COUNT(col)`. Unlike the other ops, `NULL` rows
do not advance the count (`x=[2.0, 0.0, -3.0, NULL, 4.0]` →
`cum_x_count=[1, 2, 3, 3, 4]`).

```python
result = dataset.cumcount("user_id", order_col="signup_date")
```

```python
result = await dataset.acumcount("user_id", order_col="signup_date")
```

The result column is `BIGINT` on PostgreSQL/DuckDB and `UInt64` on
ClickHouse.

### `cumstd`, `cumvar`

Running **population** (not sample) statistics: `STDDEV_POP(col)` /
`VAR_POP(col)` on PostgreSQL and DuckDB, `stddevPop(col)` / `varPop(col)`
on ClickHouse.

```python
result = dataset.cumstd("score", order_col="month")
result = dataset.cumvar("score", order_col="month")
```

```python
result = await dataset.acumstd("score", order_col="month")
result = await dataset.acumvar("score", order_col="month")
```

A single-row frame yields `0.0` (population denominator `n`, not `n-1`).

## Return Values and Errors

Public cumulative methods return the resulting DataFrame directly. Generated
table names and operation metadata remain internal to cache and AI layers.
Invalid operations raise `OperationError`. A negative column passed to
`cumprod`, or a missing column, surfaces this way.

## Generated Tables

Every cumulative operation is non-destructive to the source upload table.
Each operation creates a new transient table (`<table>__op_<n>`) holding the
source rows plus the result column, then returns a sample of it:

- **PostgreSQL / DuckDB:** clone via `CREATE TABLE … AS SELECT *`, `ADD
  COLUMN` for the target, then `UPDATE … FROM (SELECT ctid/rowid,
  <window> AS val …) WHERE t.<rid> = s.<rid>`.
- **ClickHouse:** a single `CREATE TABLE … ENGINE = MergeTree() ORDER BY
  tuple() AS SELECT *, <window> AS <target> FROM …` — the window is computed
  inline because asynchronous `UPDATE` mutations can return stale data.

Operations are recorded with `deep_cache=True`, so repeat calls replay the
persisted transient table instead of recomputing.

## Backend Behavior

Cumulative supports DuckDB, PostgreSQL, and ClickHouse adapters:

- Identifiers are sanitized and quoted before SQL is generated.
- `order_col` may be a single column, a list, or omitted (physical order;
  synthetic `_ch_rowid` on ClickHouse).
- `STDDEV_POP`/`VAR_POP` map to ClickHouse-native `stddevPop`/`varPop`;
  `cumcount` maps `BIGINT` to `UInt64` on ClickHouse.
- The per-backend SQL is locked by
  `tests/unit/test_cumulative_sql_fingerprint.py` (24 scenarios × 3
  backends).

## Errors

Cumulative methods raise `OperationError` for backend or validation
failures:

- `cumprod` on a column containing negatives fails the whole operation
  (`LN` of a negative is an engine error, not per-row `NULL`).
- Missing columns fail with the canonical error envelope.
- Unsupported backends raise `OperationError`.

## API Reference

::: memframe.wrappers.analytix.cumulative.CumulativeWrapper
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
