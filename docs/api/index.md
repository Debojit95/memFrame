# Index

Source: `src/memframe/wrappers/analytix/index.py`

`IndexWrapper` is the public index interface exposed through a
`ContextManager`. It provides pandas-like `set_index`, `reset_index`, `index`,
`reindex`, and `reindex_like` backed by backend-native SQL across DuckDB,
PostgreSQL, and ClickHouse. SQL tables have no row labels, so the index is
**metadata-only** — a JSON list of key columns in
`memframe_csv_registry.index_cols`, never DDL. `set_index`/`reset_index`
only read/write that metadata; `reindex`/`reindex_like` are read-time
`LEFT JOIN`s against the key.

Users normally call index methods directly on a dataset context returned by
an upload operation:

```python
dataset = mf.upload_df(frame)
dataset.set_index("month")
labels = dataset.index
```

The same methods are available asynchronously:

```python
dataset = await mf.aupload_df(frame)
await dataset.aset_index("month")
aligned = await dataset.areindex([1, 2, 4])
```

The lower-level files are implementation details:

- `src/memframe/core/analytix/index/` builds and executes backend-specific SQL
  (`base.py` holds all logic; `duckdb.py` / `postgres.py` / `clickhouse.py`
  inherit unchanged — the fill SQL is portable).
- `src/memframe/core/orchestrator/analytix/index.py` resolves the active dataset
  context and passes persistence metadata (metadata writes are signature-only
  in the cache; `reindex` reads persist under `deep_cache`).
- `src/memframe/wrappers/analytix/index.py` exposes synchronous and asynchronous
  public methods.

## Public API

Every index operation has synchronous and asynchronous forms (`ctx.index` is
the property form of `get_index`, returning bare label values):

| Synchronous | Asynchronous | Purpose |
| --- | --- | --- |
| `set_index(keys, append=False, drop=True, verify_integrity=False)` | `await aset_index(...)` | Track key column(s) as the logical index |
| `reset_index(level=None, drop=False, names=None)` | `await areset_index(...)` | Clear the tracked index, or a subset of levels |
| `get_index(limit=None)` / `index` | `await aget_index(...)` | Read back index columns and labels |
| `reindex(labels, ...)` | `await areindex(...)` | Conform rows/columns to new labels |
| `reindex_like(other, ...)` | `await areindex_like(...)` | Conform to another dataset's index and columns |

Public methods return the operation value directly (a `DataFrame`, label
list, or dict). Invalid operations raise `OperationError`.

## Usage Overview

```python
dataset = mf.upload_df(frame)
dataset.set_index("month")

labels = dataset.index                    # [1, 4, 7, 10]
aligned = dataset.reindex([1, 2, 4])      # gap months -> NaN
filled = dataset.reindex([1, 2, 4], fill_value=0)
dataset.reset_index()                     # back to synthetic RangeIndex
```

```python
dataset = await mf.aupload_df(frame)
await dataset.aset_index(["year", "month"])
aligned = await dataset.areindex([(2012, 1), (2014, 5)], fill_value=0)
filled = await dataset.areindex([1, 2, 3, 4], method="ffill", limit=1)
nearest = await dataset.areindex([2, 9], method="nearest")
remaining = await dataset.areset_index(level="year")  # keep ["month"]
```

Multi-key index and fill methods:

```python
await orch.set_index(["year", "month"])
await orch.reindex([(2012, 1), (2014, 5)], fill_value=0)
await orch.reindex([1, 2, 3, 4], method="ffill", limit=1)
await orch.reindex([2, 9], method="nearest")
await orch.reset_index(level="year")     # keep ["month"]
```

## Parameters

| Parameter | Ops | Description |
| --- | --- | --- |
| `keys` | `set_index` | Column name or list. MultiIndex = list of 2+ columns (labels are tuples). `columns=` is accepted as an alias. |
| `append` | `set_index` | `True` extends the tracked keys, `False` replaces them. Defaults to `False`. |
| `drop` | `set_index` / `reset_index` | Recorded, not executed — key columns never leave the table; future projections may hide them. Defaults to `True` / `False`. |
| `verify_integrity` | `set_index` | One `COUNT(*)` vs `COUNT(DISTINCT keys)` probe; fails on duplicates. Defaults to `False`. |
| `level` | `reset_index` | `None` clears all levels; a name or list removes a subset. |
| `names` | `reset_index` | `{old: new}` column mapping applied via `RENAME COLUMN` before clearing. |
| `labels` / `index` / `columns` / `axis` | `reindex` | New labels: `labels` + `axis` (`0`/`"index"`, `1`/`"columns"`), or `index=`/`columns=` keywords. Both axes at once is allowed. |
| `method` | `reindex` | `None` (gaps stay missing), `"ffill"`/`"pad"`, `"bfill"`/`"backfill"`, `"nearest"`. Row axis only; needs monotonic labels. |
| `fill_value` | `reindex` | Scalar for still-missing cells (applied after any method fill). Defaults to `None` (NULL). |
| `limit` | `reindex` | Max consecutive fills for `ffill`/`bfill`. Needs a fill method. |
| `other` | `reindex_like` | The other dataset's `data_id` string. Same key columns required; labels and columns are taken from it. Thin wrapper over `reindex`. |

Validation (each returns `is_error True` internally):

- Unknown key/column names; empty label lists.
- `method` outside `ffill/pad`, `bfill/backfill`, `nearest`.
- `method` with non-monotonic labels; `limit` without a fill method.
- Label width != index width; `reindex` with no index set (`Call set_index(keys) first`).
- `reindex_like` against an unknown `data_id`, an index-less dataset, or mismatched key columns.

## Backend Behavior

Index ops support DuckDB, PostgreSQL, and ClickHouse with one shared
implementation:

- Wanted labels are inlined as a portable `UNION ALL` derived table
  (`SELECT ? AS "k", ? AS "__ord" UNION ALL ...`) with bind params, then
  `LEFT JOIN`ed to the base table on null-safe key equality, ordered by the
  input ordinal. Read-only — no table is created or altered (except `reset_index(names=...)`, which runs `RENAME COLUMN`).
- `ffill`/`bfill` use the islands idiom — `COUNT(col)` over the ordering plus
  `MAX(col)` per island — exact on all three backends (no `IGNORE NULLS`
  needed). `limit` caps the per-island run via `ROW_NUMBER`.
- `nearest` (single numeric/datetime key only) fetches one `DISTINCT` key list
  and maps gap labels to the closest present key in Python (ties go to the
  predecessor), then reuses the plain join path.
- Identifiers are sanitized via `SQLIdentifierSanitizer` and quoted per backend
  (`"` vs `` ` ``).
- `reindex` on the column axis is a projection: missing columns render as
  `fill_value` (or `NULL`), extras are dropped, order follows the request.

Ceilings that fail loudly today: multi-key `nearest`, `tolerance`,
`limit` with `nearest`, and pandas `level=` broadcast — ask when a real query
needs them.

## Errors

Failures return `is_error True` with an `error_message` (raised as
`OperationError` once the `ContextManager` wrapper lands):

- `No index set. Call set_index(keys) first.`
- `method='...' needs monotonically increasing/decreasing labels.`
- `Label width N != index width M.` / `MultiIndex needs tuple labels ...`
- `Index mismatch: self [...] vs other [...] (same key columns required).`
- `method='nearest' needs a numeric or datetime key column.`
- `backend and data_id required` — internal invariant, should not surface in normal use.

## API Reference

::: memframe.wrappers.analytix.index.IndexWrapper
    options:
      show_root_heading: true
      show_root_full_path: true
      members:
        - aset_index
        - set_index
        - areset_index
        - reset_index
        - aget_index
        - get_index
        - areindex
        - reindex
        - areindex_like
        - reindex_like
