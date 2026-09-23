# GroupBy Stats

Source: `src/memframe/wrappers/analytix/groupby_stats.py`

`GroupByStatsWrapper` is the public group-by aggregation interface exposed
through a `ContextManager`. It provides pandas-`groupby().agg()`-like
analytics: pick grouping column(s), map value columns to lists of statistics,
and get one row per group. Every operation compiles to backend-native SQL and
runs in-engine across DuckDB, PostgreSQL, and ClickHouse; each call writes a
new transient table (`<table>__op_<n>`) and the source upload table is never
mutated in place.

Users normally call group-by methods directly on a dataset context returned
by an upload operation. There are two equivalent entry points:

- a direct call — `dataset.agg(group_cols, agg_dict)` — with the grouping
  columns passed as an argument;
- a pandas-style fluent builder — `dataset.groupby("region").sum("sales")`.

```python
dataset = mf.upload_df(frame)

result = dataset.agg(group_cols=["region"], agg_dict={"sales": ["sum", "mean"]})
result = dataset.groupby("region").sum("sales")
```

```python
dataset = await mf.aupload_df(frame)

result = await dataset.aagg(group_cols="region", agg_dict={"sales": ["sum"]})
result = await dataset.groupby("region").aagg({"sales": ["mean"]})
```

`groupby` is top-level (`dataset.groupby`, not `dataset.dt.*`) — `dt` is
reserved for `DateTimeWrapper`.

The lower-level files are implementation details:

- `src/memframe/core/analytix/groupby_stats.py` builds and executes
  backend-specific SQL in a single `GroupByStatsOps` class (`isinstance`
  branches for DuckDB / PostgreSQL / ClickHouse).
- `src/memframe/core/orchestrator/analytix/groupby_stats.py` resolves the
  active dataset context and applies `@record_call`.
- `src/memframe/wrappers/analytix/groupby_stats.py` exposes synchronous and
  asynchronous public methods plus the fluent builder.

## Public API

### Direct

| Synchronous | Asynchronous | Purpose |
| --- | --- | --- |
| `agg(group_cols, agg_dict, new_table=None)` | `await aagg(...)` | Group-by aggregation over an explicit column mapping |
| `event_rate(group_cols, datetime_col, unit="day", new_table=None)` | `await aevent_rate(...)` | Grouped event rate over a time unit |

`group_cols` accepts a single column name or a list of names. `agg_dict`
maps a value column to a list of statistics, e.g.
`{"sales": ["sum", "mean"]}`.

### Builder

`groupby(*columns)` returns a `GroupByWrapper` holding the group columns.
Each terminal method has a synchronous and asynchronous form:

| Synchronous | Asynchronous | Purpose |
| --- | --- | --- |
| `agg(agg_dict, new_table=None)` | `await aagg(...)` | Generic — any column → stats mapping |
| `sum(column)` | `await asum(...)` | Group-wise sum |
| `mean(column)` | `await amean(...)` | Group-wise mean |
| `min(column)` | `await amin(...)` | Group-wise minimum |
| `max(column)` | `await amax(...)` | Group-wise maximum |
| `count(column)` | `await acount(...)` | Group-wise non-null count |
| `median(column)` | `await amedian(...)` | Group-wise median |
| `mode(column)` | `await amode(...)` | Group-wise mode |
| `std(column)` | `await astd(...)` | Group-wise population standard deviation |
| `var(column)` | `await avar(...)` | Group-wise population variance |
| `sem(column)` | `await asem(...)` | Group-wise standard error of the mean |
| `nunique(column)` | `await anunique(...)` | Group-wise distinct count |
| `range(column)` | `await arange(...)` | Group-wise range (`max - min`) |
| `product(column)` | `await aproduct(...)` | Group-wise product |
| `event_rate(datetime_col, unit="day", new_table=None)` | `await aevent_rate(...)` | Grouped event rate |

## Usage Overview

```python
dataset = mf.upload_df(frame)

sample = dataset.agg(group_cols="region", agg_dict={"sales": ["sum", "mean"]})
sample = dataset.groupby("region").agg({"sales": ["sum"], "qty": ["max"]})
sample = dataset.groupby("region").mean("sales")
```

```python
dataset = await mf.aupload_df(frame)

sample = await dataset.aagg(group_cols=["region"], agg_dict={"sales": ["sum"]})
sample = await dataset.groupby("region").amean("sales")
```

Multi-column grouping works in both entry points:

```python
result = dataset.agg(
    group_cols=["region", "month"],
    agg_dict={"sales": ["sum"]},
)
result = dataset.groupby("region", "month").sum("sales")
```

## Common Parameters

| Parameter | Type | Description |
| --- | --- | --- |
| `group_cols` | `str` or `list[str]` | Column(s) to group by. At least one is required. |
| `agg_dict` / `agg_spec` | `dict[str, list[str]]` | Value column → list of statistics (e.g. `{"sales": ["sum", "mean"]}`). |
| `datetime_col` | `str` | Datetime column for `event_rate`. |
| `unit` | `str` | Rate unit for `event_rate`: `second`, `minute`, `hour`, `day` (default), or `week`. |
| `new_table` | `str` or `None` | Explicit name for the output table. Auto-generated as `<table>__op_<n>` (chained) when omitted. |

## Aggregation Statistics

Result columns are named `<column>_<stat>` (e.g. `sales_sum`):

- `count` → `COUNT(col)`; `sum` → `SUM(col)`; `min` / `max` → `MIN(col)` /
  `MAX(col)`; `avg` and `mean` → `AVG(col)`; `nunique` →
  `COUNT(DISTINCT col)`; `range` → `MAX(col) - MIN(col)`.

```python
result = dataset.groupby("region").agg({"sales": ["sum", "mean", "nunique"]})
```

### Spread: `std`, `var`, `sem`

Population (not sample) statistics: `STDDEV_POP(col)` / `VAR_POP(col)` on
PostgreSQL and DuckDB, `stddevPop(col)` / `varPop(col)` on ClickHouse.
`sem` is `stddev_pop / SQRT(COUNT(col))`.

```python
result = dataset.groupby("region").std("score")
result = dataset.groupby("region").var("score")
result = dataset.groupby("region").sem("score")
```

### `product`

Group-wise product via `EXP(SUM(LN(NULLIF(col, 0))))` (ClickHouse:
`exp(sum(ln(nullIf(col, 0))))`). Zeros are skipped rather than zeroing the
group.

```python
result = dataset.groupby("region").product("growth_factor")
```

### `median`, `mode`

Backend-specific implementations: `median` uses
`PERCENTILE_CONT(0.5) WITHIN GROUP` on PostgreSQL, `MEDIAN(col)` on
DuckDB, and `median(col)` on ClickHouse. `mode` uses `MODE() WITHIN
GROUP` on PostgreSQL, `MODE(col)` on DuckDB, and `topK(1)(col)[1]`
(approximate) on ClickHouse.

```python
result = dataset.groupby("region").median("sales")
result = dataset.groupby("region").mode("sales")
```

## Event Rate

Grouped event rate per time unit. Returns one row per group with `cnt`
(group size) and `event_rate`. Groups whose timestamps are all identical
(or a single row) yield `0.0` instead of dividing by zero.

```python
result = dataset.groupby("region").event_rate("ts", unit="day")
result = dataset.aevent_rate(
    group_cols=["region"], datetime_col="ts", unit="hour"
)
```

!!! note
    A flat `dataset.event_rate(...)` resolves to the ungrouped
    `StatsWrapper.event_rate(column, unit)` (it is registered first). The
    grouped form above is only reachable through the `groupby(...)`
    builder or `GroupByStatsWrapper` directly.

## Return Values and Errors

On a `ContextManager`, public methods return the resulting **DataFrame**
directly. Calling the wrapper class directly (or using a builder's terminal
method) returns the raw operation envelope:

```python
{
    "is_error": False,
    "message": "Group-by aggregation on ['sales']",
    "result": <pandas.DataFrame>,
    "new_table": "ab12CD__op_1",
    "new_columns": ["sales_sum", "sales_mean"],
    "group_cols": ["region"],
    "agg_spec": {"sales": ["sum", "mean"]},
}
```

Invalid operations surface as `OperationError` when the envelope is
unwrapped (e.g. via a `ContextManager`): a missing group-by column, an
empty `agg_dict`, an unknown statistic, or a missing value column. When
inspecting the raw envelope, check `is_error` / `error_message` instead.

## Generated Tables

Every group-by operation is non-destructive to the source upload table. It
creates a new transient table holding one row per group, then returns a
preview of it:

- **PostgreSQL / DuckDB:** `CREATE TABLE … AS SELECT <groups>,
  <aggregates> … GROUP BY …`.
- **ClickHouse:** `CREATE TABLE … ENGINE = MergeTree() ORDER BY tuple() AS
  SELECT … GROUP BY …`.

Output names chain via the transient registry (`<table>__op_<n>`); an
explicit `new_table` is sanitized and deduplicated with a numeric suffix on
collision. Operations apply `@record_call`, so repeat calls are recorded in
the transient registry.

## Backend Behavior

Group-by stats support DuckDB, PostgreSQL, and ClickHouse:

- Identifiers are sanitized and quoted before SQL is generated.
- `group_cols` may be a single column or a list; at least one is required.
- Backend dialect differences (`std`/`var`/`sem`, `product`, `median`,
  `mode`, event-rate epoch extraction) are handled per backend.
- The per-backend SQL is locked by
  `tests/unit/test_groupby_stats_sql_fingerprint.py` (6 scenarios × 3
  backends).

## Errors

Group-by methods raise `OperationError` (once unwrapped) for validation or
backend failures:

- No group-by column, or an empty `agg_dict` / no valid aggregate
  expressions.
- Unknown statistic name.
- Missing group, value, or datetime column.
- Missing `backend` / `data_id` at the ops layer, or an unsupported backend.

## API Reference

::: memframe.wrappers.analytix.groupby_stats.GroupByStatsWrapper
    options:
      show_root_heading: true
      show_root_full_path: true
      members:
        - aagg
        - agg
        - aevent_rate
        - event_rate
        - groupby

::: memframe.wrappers.analytix.groupby_stats.GroupByWrapper
    options:
      show_root_heading: true
      show_root_full_path: true
      members:
        - aagg
        - agg
        - asum
        - sum
        - amean
        - mean
        - amin
        - min
        - amax
        - max
        - acount
        - count
        - amedian
        - median
        - amode
        - mode
        - astd
        - std
        - avar
        - var
        - asem
        - sem
        - anunique
        - nunique
        - arange
        - range
        - aproduct
        - product
        - aevent_rate
        - event_rate
