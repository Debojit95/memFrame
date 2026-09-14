# Merging

Source: `src/memframe/wrappers/analytix/merging.py`

`MergeWrapper` is the public merging interface exposed through a `ContextManager`.
It provides pandas-like `merge`, `join`, and `concat` APIs backed by
backend-native SQL across DuckDB, PostgreSQL, and ClickHouse.

The other dataset is another dataset context returned by an upload (or
`set_active`) — merging does not accept a DataFrame directly. Both datasets must
live in the same backend and upload schema.

Users normally call merging directly on a dataset context returned by an upload
operation:

```python
left = mf.upload_df(left_frame, filename="left")
right = mf.upload_df(right_frame, filename="right")

merged = left.merge(right, on="id")
```

The same methods are available asynchronously:

```python
left = await mf.aupload_df(left_frame, filename="left")
right = await mf.aupload_df(right_frame, filename="right")

merged = await left.amerge(right, on="id")
```

The lower-level files are implementation details:

- `src/memframe/core/analytix/merging/` builds and executes backend-specific SQL
  (`DataMergeOps` in `base.py` plus per-backend modules for DuckDB, PostgreSQL,
  and ClickHouse).
- `src/memframe/core/orchestrator/analytix/merging.py` resolves the active dataset
  context and the right-hand dataset's table.
- `src/memframe/wrappers/analytix/merging.py` exposes synchronous and asynchronous
  public methods.

## Public API

Every merging operation has synchronous and asynchronous forms:

| Synchronous | Asynchronous | Purpose |
| --- | --- | --- |
| `merge(right_ops, how="inner", on=None, left_on=None, right_on=None, suffixes=("_x", "_y"), chunk_size=None)` | `await amerge(...)` | Key-based join into a new table |
| `join(right_ops, how="left", on=None, lsuffix="", rsuffix="", chunk_size=None)` | `await ajoin(...)` | Join with index-style suffixes |
| `concat(other_ops_list, axis=0, join="outer", ignore_index=False, chunk_size=None)` | `await aconcat(...)` | Stack or side-by-side concat multiple datasets |

The wrapper instance is also callable as a shortcut for `merge`:

```python
merged = left.merge(right, on="id")     # preferred
merged = left(right, on="id")           # __call__ shorthand
```

## Usage Overview

```python
left = mf.upload_df(left_frame, filename="left")
right = mf.upload_df(right_frame, filename="right")

# Inner merge on a shared key
merged = left.merge(right, on="id")

# Different key names on each side
merged = left.merge(right, left_on="id", right_on="customer_id")

# Outer merge with custom overlap suffixes
merged = left.merge(right, how="outer", on="id", suffixes=("_left", "_right"))

# left/right anti-joins keep rows with no match on the other side
only_left = left.merge(right, how="left_anti", on="id")
only_right = left.merge(right, how="right_anti", on="id")

# Join defaults to a left join and suffixes the right-side overlap with "_right"
joined = left.join(right, on="id")
joined = left.join(right, on="id", lsuffix="_l", rsuffix="_r")

# Concat two or more datasets (the active dataset is always first)
stacked = left.concat([right], axis=0, join="outer")
side_by_side = left.concat([right], axis=1, join="outer")
```

Async and chained forms (every call returns a live context):

```python
left = await mf.aupload_df(left_frame, filename="left")
right = await mf.aupload_df(right_frame, filename="right")

merged = await left.amerge(right, on="id")
merged.head()

chained = merged.merge(right, left_on="id_x", right_on="id")
```

## Parameters

`merge`:

| Parameter | Type | Description |
| --- | --- | --- |
| `right_ops` | `ContextManager` | The right-hand dataset context. |
| `how` | `str` | Join type: `"inner"`, `"left"`, `"right"`, `"outer"`, `"cross"`, `"left_anti"`, or `"right_anti"`. Defaults to `"inner"`. |
| `on` | `str`, `list[str]`, or `None` | Shared key column(s), applied to both sides. Mutually exclusive with `left_on`/`right_on`. |
| `left_on` | `str`, `list[str]`, or `None` | Left key column(s) when the names differ. |
| `right_on` | `str`, `list[str]`, or `None` | Right key column(s) when the names differ. Must match `left_on` in length. |
| `suffixes` | `tuple[str, str]` | Suffixes for overlapping columns (including the join keys). Defaults to `("_x", "_y")`. |
| `chunk_size` | `int` or `None` | Accepted for validation only; the output table is fully materialized and a live context is returned regardless. Must be `> 0`. |

`join`:

| Parameter | Type | Description |
| --- | --- | --- |
| `right_ops` | `ContextManager` | The right-hand dataset context. |
| `how` | `str` | Join type, as above. Defaults to `"left"`. |
| `on` | `str`, `list[str]`, or `None` | Key column(s) applied to both sides. When `None`, the common columns are used automatically. |
| `lsuffix` | `str` | Suffix for left-side overlapping (non-key) columns. Defaults to `""`. |
| `rsuffix` | `str` | Suffix for right-side overlapping columns. Defaults to `""`, which is rendered as `"_right"`. |
| `chunk_size` | `int` or `None` | As above — validated, but the return is always a live context. |

`concat`:

| Parameter | Type | Description |
| --- | --- | --- |
| `other_ops_list` | `list[ContextManager]` | Datasets to concatenate; the active dataset is prepended automatically. At least one other dataset is required (2 tables total). |
| `axis` | `0` or `1` | `0` stacks rows (`UNION ALL`); `1` places columns side by side (row-number join). Defaults to `0`. |
| `join` | `"outer"` or `"inner"` | Column alignment: `"outer"` unions all columns (`NULL`-filled); `"inner"` keeps only common columns. Defaults to `"outer"`. |
| `ignore_index` | `bool` | Axis-0 only: prepend a `ROW_NUMBER()` `__index__` column. Defaults to `False`. |
| `chunk_size` | `int` or `None` | As above — validated, but the return is always a live context. |

## Join Semantics

Supported join types: `inner`, `left`, `right`, `outer`, `cross`, `left_anti`,
and `right_anti`. `cross` ignores the join condition and produces the Cartesian
product. `left_anti` / `right_anti` keep rows from one side that have no match
on the other.

Key resolution:

- `merge`: provide `on`, or both `left_on` and `right_on`. Omitting both is an error.
- `join`: with `on=None`, the common columns of both tables are used as the key;
  if there are none, the operation errors.

Column naming differs between the two:

- `merge` suffixes **every** overlapping column, including the join keys
  (`id`, `val` → `id_x`, `id_y`, `val_x`, `val_y` with the default suffixes).
- `join` keeps the left column name unchanged and suffixes the right-side overlap
  with `rsuffix`, falling back to `"_right"` when `rsuffix` is empty.

## Timestamp / Date Auto-Cast

When a join key is a timestamp on one side and a date on the other, the SQL casts
the timestamp side to a date so the comparison type-checks:

- DuckDB and PostgreSQL match on the `TIMESTAMP` / `DATE` type substrings.
- ClickHouse matches its `DateTime` / `DateTime64` types against a date-only
  `Date` / `Date32` (the `DATE` substring inside `DATETIME` is explicitly excluded).

## Return Values and Errors

Success returns a live `ContextManager` bound to the new output table — not a
`DataFrame`. It is chainable like any dataset context (`merged.head()`,
`merged.merge(other)`, or as the `right_ops` of another merge). `chunk_size`
does not change the public return type: the output table is fully materialized
either way.

Merging differs from the other operation groups in how errors surface through
`ContextManager`: because the error payloads omit the `result` key, the generic
response unwrapping in `src/memframe/core/analytix/_response.py` leaves them as
raw dictionaries instead of raising `OperationError`. Concretely:

```python
# Success: a live dataset context on the merged table
merged = left.merge(right, on="id")
merged.head()
chained = merged.merge(other, left_on="id_x", right_on="id")

# Error: the raw envelope dict, not a raised exception
response = left.merge(right, how="nonsense", on="id")
assert response["is_error"] is True
assert response["error_message"]
```

Error messages:

- `backend and data_id required` — internal invariant; missing active dataset/backend.
- `chunk_size must be > 0`
- `Must provide 'on' or both 'left_on' and 'right_on'`
- `left_on and right_on must match length`
- `Unsupported join type: <how>`
- `No columns found in left table: <table>` / `No columns found in right table: <table>`
- `No common columns for join` — `join` with no shared columns.
- `Need at least 2 tables to concat`
- `axis must be 0 or 1`
- `join must be 'outer' or 'inner'`
- `No columns available after join resolution` — `axis=0, join="inner"` with no common columns.
- `Unsupported database backend for merge operation` — unknown adapter.

## Generated Tables

Every merge, join, and concat materializes a new transient table
(`<left_table>__op_<n>`) and leaves the source upload tables untouched:

1. Resolves the output name via the transient registry (with a dedupe suffix on collision).
2. `CREATE TABLE <new> AS <join/union SELECT>`.
3. Persists the table in the transient schema and records it in the transient
   registry, so the returned context stays live and replays work. Outputs are
   dropped with the parent dataset (`delete_table`) or on cache clear. Sides may
   live in different schemas (e.g. a merged transient table plus an upload
   table) — each side is qualified with its own schema.
4. `MemFrame(deep_cache=False)` disables persistence globally; a merged context
   created under it raises `DataNotFound` on use because its table was dropped.

## Backend Behavior

Merging supports DuckDB, PostgreSQL, and ClickHouse:

- Identifiers are sanitized via `SQLIdentifierSanitizer` and quoted per backend
  (`"` vs `` ` ``).
- DuckDB and PostgreSQL share one code path: plain `CREATE TABLE … AS SELECT …`.
- ClickHouse uses its own path with `ENGINE = MergeTree() ORDER BY tuple()`; the
  `SELECT` shapes are otherwise identical.
- `concat(axis=1)` numbers rows with `ROW_NUMBER() OVER ()` in each input and
  joins them on that row number (`FULL OUTER JOIN` for `"outer"`, `INNER JOIN`
  for `"inner"`).
- Overlapping output column names are de-duplicated with `_1`, `_2`, … suffixes.

## API Reference

::: memframe.wrappers.analytix.merging.MergeWrapper
    options:
      show_root_heading: true
      show_root_full_path: true
      members:
        - amerge
        - merge
        - ajoin
        - join
        - aconcat
        - concat
