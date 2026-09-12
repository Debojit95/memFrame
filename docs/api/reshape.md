# Reshape

Source: `src/memframe/wrappers/analytix/reshape.py`

`ReshapingWrapper` is the public reshape interface exposed through a
`ContextManager`. It provides pandas-like `explode`, `melt`, `pivot`,
`pivot_table`, `crosstab`, `transpose`, `rank`, and `groupby_rank` APIs backed
by backend-native SQL across DuckDB, PostgreSQL, and ClickHouse.

Users normally call reshape directly on a dataset context returned by an upload
operation:

```python
dataset = mf.upload_df(frame)
result = dataset.melt(id_vars=["id"], value_vars=["score"])
```

The same methods are available asynchronously:

```python
dataset = await mf.aupload_df(frame)
result = await dataset.amelt(id_vars=["id"], value_vars=["score"])
```

The lower-level files are implementation details:

- `src/memframe/core/analytix/reshape.py` builds and executes backend-specific SQL.
- `src/memframe/core/orchestrator/analytix/reshape.py` resolves the active dataset
  context and passes persistence metadata (with `deep_cache` support so reshaped
  tables are replayable).
- `src/memframe/wrappers/analytix/reshape.py` exposes synchronous and asynchronous
  public methods.

## Public API

Every reshape operation has synchronous and asynchronous forms:

| Synchronous | Asynchronous | Purpose |
| --- | --- | --- |
| `explode(column, chunk_size=None)` | `await aexplode(...)` | Explode list-like values into separate rows |
| `melt(id_vars, value_vars, var_name="variable", value_name="value", chunk_size=None)` | `await amelt(...)` | Unpivot columns into long format |
| `pivot(index, columns, values, chunk_size=None)` | `await apivot(...)` | Pivot data from long to wide format (no duplicates) |
| `pivot_table(index=None, columns=None, values=None, aggfunc="mean", fill_value=None, chunk_size=None)` | `await apivot_table(...)` | Pivot with aggregation over duplicate combinations |
| `crosstab(index, columns, values=None, aggfunc=None, margins=False, normalize=False, chunk_size=None)` | `await acrosstab(...)` | Cross-tabulation frequency/aggregation table |
| `transpose(chunk_size=None)` | `await atranspose(...)` | Transpose rows and columns |
| `rank(columns, method="average", na_option="keep", ascending=True, pct=False, chunk_size=None)` | `await arank(...)` | Rank values within selected columns |
| `groupby_rank(groupby, columns, method="average", ascending=True, na_option="keep", pct=False, chunk_size=None)` | `await agroupby_rank(...)` | Rank values within groups |

Public methods return the operation value directly (a `DataFrame` or an async
iterator). Invalid operations raise `OperationError`.

## Usage Overview

```python
dataset = mf.upload_df(frame)

exploded = dataset.explode(column="tags")
long = dataset.melt(id_vars=["id"], value_vars=["score"])
wide = dataset.pivot(index="id", columns="region", values="score")
agg = dataset.pivot_table(index="region", values="score", aggfunc="mean")
freq = dataset.crosstab(index="region", columns="segment")
transposed = dataset.transpose()
ranked = dataset.rank(columns="score")
grouped = dataset.groupby_rank(groupby="region", columns="score")
```

```python
dataset = await mf.aupload_df(frame)

long = await dataset.amelt(id_vars=["id"], value_vars=["score"])
iterator = await dataset.amelt(id_vars=["id"], value_vars=["score"], chunk_size=1000)
async for chunk in iterator:
    process(chunk)
```

Every reshape materializes a new transient table (`<table>__op_<n>`) and leaves
the source upload table untouched. The public return is a sample `DataFrame`
(or an async iterator when `chunk_size` is set); `new_table` metadata remains
available to the cache layer.

## Parameters

`explode`:

| Parameter | Type | Description |
| --- | --- | --- |
| `column` | `str` or `list[str]` | Column(s) with list-like values (`"[a, b]"`, plain scalars, or nulls) to explode into rows. |
| `chunk_size` | `int` or `None` | If set, return an async iterator of `DataFrame` chunks instead of a single sample. |

`melt`:

| Parameter | Type | Description |
| --- | --- | --- |
| `id_vars` | `list[str]` or `None` | Columns to keep as identifiers. Defaults to `[]`. |
| `value_vars` | `list[str]` or `None` | Columns to unpivot. Defaults to all non-`id_vars` columns. |
| `var_name` | `str` | Name of the new variable column. Defaults to `"variable"`. |
| `value_name` | `str` | Name of the new value column. Must not already exist in the table. Defaults to `"value"`. |
| `chunk_size` | `int` or `None` | Chunked streaming, as above. |

Validation: `value_vars` must be non-empty after resolution; every
`id_vars + value_vars` column must exist.

`pivot`:

| Parameter | Type | Description |
| --- | --- | --- |
| `index` | `str` or `list[str]` | Row identifier column(s). |
| `columns` | `str` or `list[str]` | Column(s) whose distinct values become new columns. |
| `values` | `str` or `list[str]` | Value column(s) to spread (`MAX(CASE ...)` per cell). |
| `chunk_size` | `int` or `None` | Chunked streaming, as above. |

`pivot` requires unique `index + columns` combinations — duplicates return an
error directing you to `pivot_table`.

`pivot_table`:

| Parameter | Type | Description |
| --- | --- | --- |
| `index` | `str`, `list[str]`, or `None` | Group-by row identifier(s). |
| `columns` | `str`, `list[str]`, or `None` | Column(s) whose distinct values become new columns. |
| `values` | `str` or `list[str]` | Value column(s) to aggregate. Required. |
| `aggfunc` | `str` or `dict` | Aggregation per value column (`"mean"` maps to `AVG`); a `dict` maps column → function. Defaults to `"mean"`. |
| `fill_value` | scalar or `None` | `COALESCE` replacement for null aggregates. Defaults to `None`. |
| `chunk_size` | `int` or `None` | Chunked streaming, as above. |

`crosstab`:

| Parameter | Type | Description |
| --- | --- | --- |
| `index` | `str` or `list[str]` | Row grouping column(s). |
| `columns` | `str` or `list[str]` | Column(s) whose distinct values become new columns. |
| `values` | `str`, `list[str]`, or `None` | With `None`, counts occurrences; otherwise aggregates the value column(s). |
| `aggfunc` | `str` or `None` | Aggregation (`SUM`/`AVG`/`MIN`/`MAX`; `"mean"` maps to `AVG`). Defaults to `"SUM"` when `values` is given. |
| `margins` | `bool` | Append an `All` row with column totals. Defaults to `False`. |
| `normalize` | `bool` or `"all"` | Divide cells by the grand total (proportions). Defaults to `False`. |
| `chunk_size` | `int` or `None` | Chunked streaming, as above. |

`transpose` takes no column arguments — it unpivots every column into
`column_name` rows and pivots original row positions into numbered columns.

`rank` / `groupby_rank`:

| Parameter | Type | Description |
| --- | --- | --- |
| `columns` | `str` or `list[str]` | Column(s) to rank; each gains a `<col>_rank` column. |
| `groupby` | `str` or `list[str]` | (`groupby_rank` only) Partition column(s) for per-group ranks. |
| `method` | `"average"`, `"min"`, `"max"`, `"dense"`, or `"first"` | Tie-breaking, mirroring pandas. Defaults to `"average"`. |
| `na_option` | `"keep"`, `"top"`, or `"bottom"` | Null placement (`NULLS FIRST`/`LAST`, or `CASE`-guarded null ranks for `"keep"`). Defaults to `"keep"`. |
| `ascending` | `bool` | Rank direction. Defaults to `True`. |
| `pct` | `bool` | Divide ranks by the (partition) row count. Defaults to `False`. |
| `chunk_size` | `int` or `None` | Chunked streaming, as above. |

## Chunked Streaming

When `chunk_size` is set, the result contains `iterator` (an `async_generator` of
`DataFrame` chunks) and `new_table`, with `result=None`. Iterate it:

```python
iterator = await dataset.amelt(id_vars=["id"], value_vars=["score"], chunk_size=1000)
async for chunk in iterator:
    print(chunk.head())
```

The iterator transparently handles the cache's transient-table move (upload
schema → `memframe_transient`) that happens between table creation and
consumption.

## Generated Tables

Every reshape is non-destructive to the source upload table:

1. Generates a transient name `<table>__op_<n>` via the registry.
2. `CREATE TABLE <new> AS <reshape SELECT>` (per-op SQL below).
3. Non-chunked: sample via `SELECT *` from the new table; chunked: `LIMIT/OFFSET` pages.
4. `deep_cache=True` persists the reshaped clone for replay.

## Backend Behavior

Reshape supports DuckDB, PostgreSQL, and ClickHouse:

- Identifiers are sanitized via `SQLIdentifierSanitizer` and quoted per backend (`"` vs `` ` ``).
- `explode`: DuckDB uses `UNNEST` over a `str_split`/`list_value` expression with `EXCLUDE`; PostgreSQL unnests via `LATERAL` + `string_to_array`; ClickHouse strips brackets with nested `replaceAll`, splits with `splitByString`, and fans out with an `arrayJoin` index array (safe for multi-column arrays of differing lengths).
- `melt` / `pivot` / `pivot_table`: `UNION ALL` / `MAX(CASE ...)` / `AVG(CASE ...)` shapes shared across backends; numeric text is coerced per backend (`TRY_CAST` on DuckDB, regex-guarded cast on PostgreSQL, `toFloat64OrNull` on ClickHouse).
- `crosstab`: `COUNT(*) FILTER (WHERE ...)` on DuckDB/PostgreSQL vs `countIf` / `sumIf` / `avgIf` / `minIf` / `maxIf` on ClickHouse.
- `transpose`: DuckDB/PostgreSQL stage through `TEMP TABLE`s with `ROW_NUMBER()`; ClickHouse is stateless over HTTP, so it unpivots in one query with `arrayJoin(arrayZip(...))`.
- `rank` / `groupby_rank`: `RANK` / `DENSE_RANK` / `ROW_NUMBER` window functions shared across backends; `pct` casts per backend (`CAST(... AS DOUBLE PRECISION)` vs `toFloat64`).

## Errors

Reshape raises `OperationError` for validation or backend failures:

- `value_vars cannot be empty` — melt with no columns to unpivot.
- `Column '<col>' does not exist` — melt with an unknown column.
- `value_name '<name>' already exists in table` — melt target collision.
- `Duplicate index/column combinations found. Use pivot_table instead.` — pivot with non-unique cells.
- `values must be provided` — pivot_table without value columns.
- `Unsupported aggfunc type` — pivot_table with a non-str/dict aggregator.
- `No distinct column values found` / `No column values found` — pivot/crosstab on empty column domains.
- `No columns found` / `No rows found` — transpose on an empty table.
- `Unsupported method: <method>` — rank with an unknown tie-breaker.
- `Unsupported database backend for reshape operation` — unknown adapter.

## API Reference

::: memframe.wrappers.analytix.reshape.ReshapingWrapper
    options:
      show_root_heading: true
      show_root_full_path: true
      members:
        - aexplode
        - explode
        - amelt
        - melt
        - apivot
        - pivot
        - apivot_table
        - pivot_table
        - acrosstab
        - crosstab
        - atranspose
        - transpose
        - arank
        - rank
        - agroupby_rank
        - groupby_rank
