# Sorting

> **Example data:** copy the setup cell from [Example Data](example-data.md) into Colab once, then run the examples below. Examples use that `ctx`; local frames are shown inline.

Source: `src/memframe/wrappers/analytix/sorting.py`

`SortingWrapper` is the public sorting interface exposed through a
`ContextManager`. It provides a pandas-like `sort_values` API backed by
SQL `ORDER BY` across DuckDB, PostgreSQL, and ClickHouse.

Sorting does not rewrite the source table. A preview-only sort is executed as
a bounded Top-N query and cached as a small result payload. Sorts that need
chunking or a full result use a narrow `(position, _mf_idx)` permutation table
and join it back to the source. The public return is an ordered `DataFrame` (or
an async iterator when `chunk_size` is set). Pass `return_ctx=True` to
materialize a new sorted dataset with its own `data_id` and `ContextManager`.
That dataset is registered in `memframe_csv_registry`; the call is not cached,
so every call creates a fresh context.

Users normally call sorting directly on a dataset context returned by an upload
operation:

```python
# ctx is defined in the Example Data setup
result = ctx.sort_values(by="score")
```

The same method is available asynchronously:

```python
# ctx is defined in the Example Data setup
result = await ctx.asort_values(by="score", ascending=False)
```

The lower-level files are implementation details:

- `src/memframe/core/analytix/sorting/` builds and executes backend-specific SQL.
- `src/memframe/core/orchestrator/analytix/sorting.py` resolves the active dataset
  context and passes persistence metadata (with `deep_cache` so sorted tables are
  replayable).
- `src/memframe/wrappers/analytix/sorting.py` exposes synchronous and asynchronous
  public methods.

## Public API

Every sorting operation has synchronous and asynchronous forms:

| Synchronous | Asynchronous | Purpose |
| --- | --- | --- |
| `sort_values(by, ascending=True, na_position="last", columns="*", chunk_size=None, return_ctx=False)` | `await asort_values(...)` | Sort rows by one or more columns |

Public methods return the operation value directly (a `DataFrame` or an async
iterator). With `return_ctx=True`, they return a `ContextManager` bound
to a new sorted dataset. Invalid operations raise `OperationError`.

## Usage Overview

```python
# ctx is defined in the Example Data setup

sample = ctx.sort_values(by="score")
sample = ctx.sort_values(by=["region", "score"], ascending=[True, False])
nulls_first = ctx.sort_values(by="score", na_position="first")
subset = ctx.sort_values(by="score", columns=["region", "score"])
sorted_dataset = ctx.sort_values(by="score", return_ctx=True)
```

```python
# ctx is defined in the Example Data setup

sample = await ctx.asort_values(by="score", ascending=False)
iterator = await ctx.asort_values(by="score", chunk_size=1000)
async for chunk in iterator:
    process(chunk)
```

Preview-only sorts execute `SELECT ... ORDER BY ... LIMIT preview_rows` and
cache the resulting preview payload. Chunked and full-result sorts materialize
only a narrow `<table>__op_<n>` order table in `memframe_cache`; replay joins
that table to the source by `_mf_idx`. The public return is a sample
`DataFrame` (or an async iterator when `chunk_size` is set); `new_table`
metadata remains available to the cache layer.

## Parameters

| Parameter | Type | Description |
| --- | --- | --- |
| `by` | `str` or `list[str]` | Column(s) to sort by. |
| `ascending` | `bool` or `list[bool]` | Sort direction per column. A scalar `True`/`False` is broadcast. Defaults to `True`. |
| `na_position` | `"first"` or `"last"` | Where nulls sort. Defaults to `"last"`. |
| `columns` | `str`, `list[str]`, or `"*"` | Columns to include in the result. `by` columns are always included even when a subset is requested. A `str` like `"name"` is treated as `["name"]`. Defaults to `"*"` (all columns). |
| `chunk_size` | `int` or `None` | If set, return an async iterator of `DataFrame` chunks of this size instead of a single sample. Must be a positive `int`. |

Validation:

- `ascending` length must match `by` length.
- `na_position` must be `"first"` or `"last"`.
- `chunk_size` must be a positive `int` (`bool` is rejected; `chunk_size=1` is allowed).

Failed validation returns `is_error True` internally and raises `OperationError`
to the caller via `ContextManager` dispatch.

## Null Handling

PostgreSQL and DuckDB use native `NULLS FIRST` / `NULLS LAST` in the generated
`ORDER BY`. ClickHouse has no `NULLS FIRST/LAST` — its defaults are `ASC → NULLS LAST`,
`DESC → NULLS FIRST` (matching Postgres/DuckDB), so the engine adds an
`(col IS NULL)` sentinel only when the requested position is non-default:

- `na_position="first"` + `ASC` → `(col IS NULL) DESC, col ASC`
- `na_position="last"` + `DESC` → `(col IS NULL) ASC, col DESC`

All other combinations use plain `col ASC/DESC`.

## Chunked Streaming

When `chunk_size` is set, the result contains `iterator` (an `async_generator` of
`DataFrame` chunks) and `chunk_size`, with `result=None`. Iterate it:

```python
result = ctx.sort_values(by="score", chunk_size=1000)
# result["iterator"] is async iterable; result["result"] is None
async for chunk in result["iterator"]:
    sink(chunk)
```

Or via the public `ContextManager` path (`await ctx.asort_values(...)`
unwraps `result` vs `iterator` automatically `src/memframe/core/analytix/_response.py:49`):

```python
iterator = await ctx.asort_values(by="score", chunk_size=1000)
async for chunk in iterator:
    print(chunk.head())
```

The iterator transparently handles the cache's cache-table move (upload
schema → `memframe_cache`) that happens between table creation and
consumption.

## Generated Tables

Every `sort_values` is non-destructive to the source upload table:

1. Resolves `column_types` for `by` and `columns`.
2. Preview-only: runs `SELECT <columns> FROM <source> ORDER BY <order_sql> LIMIT <preview_rows>` and caches the small result payload.
3. Chunked/full: creates a narrow `<table>__op_<n>` permutation containing only `position` and `_mf_idx`.
4. Result reads join the source to the permutation and order by `position`; previews join only the first `preview_rows` permutation entries.
5. `return_ctx=True` materializes a new sorted dataset and registry entry.

## Backend Behavior

Sorting supports DuckDB, PostgreSQL, and ClickHouse:

- Identifiers are sanitized via `SQLIdentifierSanitizer` and quoted per backend (`"` vs `` ` ``).
- `ORDER BY` terms are built from the same logical spec on all backends; only the null sentinel logic diverges (see above).
- ClickHouse `MERGE TREE` ordering: the table's `ORDER BY tuple()` is a no-op for arbitrary user sorts, so the `ORDER BY <sort_cols>` inside the `SELECT` carries the actual ordering (standard pattern for `CREATE TABLE … AS SELECT … ORDER BY` on ClickHouse).

## Errors

Sorting raises `OperationError` for validation or backend failures:

- `backend and data_id required` — internal invariant, should not surface in normal use (missing active dataset).
- `Length of 'ascending' must match 'by'`
- `na_position must be 'first' or 'last'`
- `chunk_size must be a positive integer`
- `Unsupported database backend for sorting operation` — unknown adapter.

## API Reference

::: memframe.wrappers.analytix.sorting.SortingWrapper
    options:
      show_root_heading: true
      show_root_full_path: true
      members:
        - asort_values
        - sort_values
