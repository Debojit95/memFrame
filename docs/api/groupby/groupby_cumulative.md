# GroupBy Cumulative

> **Example data:** copy the setup cell from [Example Data](../example-data.md) into Colab once, then run the examples below. Examples use that `ctx`; local frames are shown inline.

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
# ctx is defined in the Example Data setup

result = ctx.groupby("region").cumsum("sales", order_col="date")
result = ctx.groupby("region").cummean("score")
```

```python
# ctx is defined in the Example Data setup

result = await ctx.groupby("region").acumsum("sales", order_col="date")
```

!!! note
    A flat `ctx.cumsum(...)` resolves to the ungrouped
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
# ctx is defined in the Example Data setup

sample = ctx.groupby("region").cumsum("sales", order_col="date")
sample = ctx.groupby("region").cummean("score", target_col="running_avg")
sample = ctx.groupby("region", "date").cumcount("id")
```

```python
# ctx is defined in the Example Data setup

sample = await ctx.groupby("region").acumsum("sales", order_col="date")
sample = await ctx.groupby("region").acummean("score")
```

`order_col` is optional — omit it to use physical row order:

```python
sample = ctx.groupby("region").cumsum("sales")
sample = ctx.groupby("region").cummean("score")
```

## Common Parameters

| Parameter | Type | Description |
| --- | --- | --- |
| `column` | `str` | Numeric column to accumulate. |
| `order_col` | `str`, `list[str]`, or `None` | Row ordering inside each group. `None` (default) uses physical row order. A list orders by multiple columns. |
| `target_col` | `str` or `None` | Name for the result column. Auto-generated as `cum_<column>_<op>_by_<groups>` with `_order_by_<orders>` appended when `order_col` is given (e.g. `cum_sales_sum_by_region_order_by_date`). |
| `map_feature` | `bool` | Deprecated/ignored. The running-value column is always written back to the source; the envelope carries `mapped_table` / `mapped_columns`. `new_table` is the source table (the ref target). |

`group_cols` are fixed at builder creation (`groupby("region", "date")`)
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
result = ctx.groupby("region").cumsum("sales", order_col="date")
```

```python
result = await ctx.groupby("region").acumsum("sales", order_col="date")
```

### `cumprod`

Running product within each group via `EXP(SUM(LN(NULLIF(col, 0)))
OVER ({window_spec}))`. Zeros are skipped rather than zeroing the tail.

```python
result = ctx.groupby("region").cumprod("sales", order_col="date")
```

Negative inputs abort the whole operation with `is_error True` (engines
reject `LN` of a negative). Restrict `cumprod` to non-negative columns.

## Running Min, Max, and Mean

### `cummax`, `cummin`

Running extrema within each group via `MAX(col)` / `MIN(col)` over the
same partitioned window frame.

```python
result = ctx.groupby("region").cummax("sales", order_col="date")
result = ctx.groupby("region").cummin("sales", order_col="date")
```

### `cummean`

Running mean within each group via `AVG(col)`.

```python
result = ctx.groupby("region").cummean("score", target_col="running_avg")
```

## Running Count, Stddev, and Variance

### `cumcount`

Running non-null count within each group via `COUNT(col)`.

```python
result = ctx.groupby("region").cumcount("id", order_col="date")
```

The result column is `BIGINT` on PostgreSQL/DuckDB and `Int64` on
ClickHouse (pass `target_type` through the direct API to override).

### `cumstd`, `cumvar`

Running **population** (not sample) statistics within each group:
`STDDEV_POP(col)` / `VAR_POP(col)` on PostgreSQL and DuckDB,
`stddevPop(col)` / `varPop(col)` on ClickHouse.

```python
result = ctx.groupby("region").cumstd("score", order_col="date")
result = ctx.groupby("region").cumvar("score", order_col="date")
```

## Source Write-Back

Every cumulative call writes its running-value column back to the source
table in place (DuckDB/PostgreSQL) or by atomically exchanging the computed
result (ClickHouse). The deep cache stores a source ref, not a copy.

```python
result = ctx.groupby("region").cumsum("sales")
# source now has: region, sales, cum_sales_sum_by_region
# result["new_table"] is the source (ref target)
```

Rules:

- DuckDB/PostgreSQL add the generated column and backfill it with one
  `UPDATE ... FROM` over a narrow `(_mf_idx, <window>)` query. No wide result
  table and no source replacement are created.
- ClickHouse computes the running column in a single CTAS and swaps the
  result into the source with `EXCHANGE TABLES`.
- A repeated identical call reuses the same generated column; the source
  keeps exactly one copy. `deep_cache=False` recomputes but writes no ref row.
- The `map_feature` parameter is retained for compatibility but ignored:
  write-back is always on.
- If the column name already exists from anything else, the call fails and
  the source is left unchanged — drop or rename it first.

## Return Values and Errors

Calling the builder (or the wrapper class) directly returns the raw
operation envelope:

```python
{
    "is_error": False,
    "message": "Cumulative sum of 'sales' grouped by [region] ...",
    "result": <pandas.DataFrame>,
    "new_table": "ab12CD",          # source table (ref target)
    "new_columns": ["cum_sales_sum_by_region"],
    "group_cols": ["region"],
    "operation": "sum",
}
```

Invalid operations return `is_error` envelopes (and raise
`OperationError` once unwrapped via a `ContextManager`): a missing
column, a missing `backend`/`data_id`, or an unsupported backend.

## Generated Tables

Every group-by cumulative operation writes its result column to the source
upload table; the deep cache stores a source ref.

- **PostgreSQL / DuckDB:** `ALTER TABLE … ADD COLUMN` plus one
  `UPDATE … FROM (SELECT _mf_idx, <window> AS <target> FROM source)` keyed by
  `_mf_idx`, with `PARTITION BY` + physical-row fallback ordering. The source
  is never cloned or swapped.
- **ClickHouse:** a single-pass CTAS with `ENGINE = MergeTree()
  ORDER BY tuple()` computes the column, then `EXCHANGE TABLES` swaps it into
  the source; without `order_col` the source is wrapped in a `ROW_NUMBER()`
  subquery whose helper column is excluded from the output.

Operations apply `@record_call`, so repeat calls are recorded in the
cache registry.

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

## Large datasets

For 10M+ rows:

- Use a file-backed DuckDB (`db_path="memframe.duckdb"`), not `:memory:`.
- Pass `temp_directory` and `memory_limit` so DuckDB can spill instead of
  exhausting Colab RAM:

```python
mf = MemFrame(
    connection_type="local",
    connection_params={
        "db_path": "memframe.duckdb",
        "temp_directory": "/content/duckdb_tmp",
        "memory_limit": "8GB",
    },
    preview_rows=1000,
)
```

- `preview_rows` caps the returned DataFrame. The generated column still
  covers every source row; the source (and cache ref) holds the full result.
- Group-by cumulative and grouped-window operations update the source in
  place and cache a source ref, so no wide result copy is materialized.
- Benchmark the cold path with:

```bash
uv run python scripts/bench_groupby_memory.py --op cumsum --rows 5000000 --preview-rows 10
```

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
