# Selection

Source: `src/wrappers/analytix/selection.py`

`SelectionWrapper` is the public selection interface exposed through a
`ContextManager` as `.select`. It provides pandas-like row and column access
against the active backend table, including scalar lookups, column retrieval,
integer-position selection, conditional replacement, and dtype-based column
selection.

Users normally call these methods on an uploaded or connected dataset context.
The lower-level files are implementation details:

- `src/core/analytix/selection.py` builds and executes backend-specific SQL.
- `src/core/orchestrator/analytix/selection.py` resolves the active dataset
  context and creates transient-table operations.
- `src/wrappers/analytix/selection.py` exposes synchronous and asynchronous
  public methods.

## Public API

Every selection operation has synchronous and asynchronous forms:

| Synchronous | Asynchronous | Purpose |
| --- | --- | --- |
| `asof(where, on, subset=None, chunk_size=None)` | `await aasof(...)` | Last row at or before reference values |
| `at(row_label, column_label, index_column=None)` | `await aat(...)` | Scalar by row label and column name |
| `iat(row_position, column_label, order_by)` | `await aiat(...)` | Scalar by integer row position |
| `get(keys, default=None)` | `await aget(...)` | One or more columns |
| `loc(row_selector, columns=None, index_column=None, chunk_size=None)` | `await aloc(...)` | Named-column wrapper over iloc-style row selection |
| `iloc(row_indexer=None, col_indexer=None, columns=None)` | `await ailoc(...)` | Integer-location row and column selection |
| `where(cond, other=None, chunk_size=None)` | `await awhere(...)` | Conditional replacement |
| `select_dtypes(include=None, exclude=None, chunk_size=None)` | `await aselect_dtypes(...)` | Columns by database type category |
| `take(indices, axis=0)` | `await atake(...)` | Rows or columns by integer indices |

All methods return a dictionary with `is_error`, `message`, and either
`result`, `value`, `iterator`, or error details.

## Usage Overview

```python
dataset = mf.upload_csv("data/sales.csv")

result = dataset.get(["customer_id", "amount"])
if not result["is_error"]:
    frame = result["result"]
```

```python
dataset = await mf.aupload_csv("data/sales.csv")

result = await dataset.aget(["customer_id", "amount"])
if not result["is_error"]:
    frame = result["result"]
```

Selection methods are also available through the context forwarding behavior,
so `dataset.get(...)` resolves to `dataset.get(...)` when no context
attribute shadows that name.

## Scalar Access

### `at`

`at` returns one value by matching a row label in an index column and reading a
named column. If `index_column` is omitted, selection uses `id` when present,
otherwise the first table column.

```python
value = dataset.at(
    row_label=103,
    column_label="name",
    index_column="id",
)
```

```python
value = await dataset.aat(
    row_label=103,
    column_label="name",
    index_column="id",
)
```

The scalar is returned under `value`.

### `iat`

`iat` returns one value by 0-based row position and column name. Pass
`order_by` to make row numbering deterministic.

```python
value = dataset.iat(
    row_position=2,
    column_label="score",
    order_by="id",
)
```

```python
value = await dataset.aiat(
    row_position=2,
    column_label="score",
    order_by=["id"],
)
```

### `asof`

`asof` returns the last row whose `on` column is less than or equal to each
reference value in `where`. A scalar `where` returns one row as a pandas
Series; a list returns a DataFrame.

```python
row = dataset.asof(
    where="2023-03-15",
    on="join_date",
    subset=["score"],
)
```

```python
rows = await dataset.aasof(
    where=["2023-03-15", "2023-04-10"],
    on="join_date",
)
```

When `subset` is provided, matching rows must have non-null values in those
columns.

## Column Retrieval

`get` returns the requested columns as a DataFrame. Missing columns are added
with `default`.

```python
columns = dataset.get(keys=["name", "score"])
```

```python
columns = await dataset.aget(
    keys=["name", "missing_column"],
    default="MISSING",
)
```

## DataFrame Selection

### `iloc`

`iloc` selects rows and columns by integer position. Indexers may be integers,
lists, slices, boolean masks, or slice strings such as `"0:3"`. Negative
integer indices are supported.

```python
result = dataset.iloc(
    row_indexer=[0, 3],
    col_indexer=[1, 2],
)
```

```python
result = await dataset.ailoc(
    row_indexer="1:4",
    col_indexer="0:2",
)
```

You can also pass both indexers as a tuple:

```python
result = dataset.iloc(row_indexer=("1:4", "0:2"))
```

When both row and column indexers resolve to a single cell, the response
contains `value` instead of `result`.

### `loc`

The public wrapper accepts `row_selector` plus optional `columns`, then routes
the request through the same integer-position path used by `iloc`. Use integer
positions, slices, boolean masks, slice strings, or tuple selectors for rows.
Use `columns` when you want to select columns by name.

```python
result = dataset.loc(
    row_selector="0:3",
    columns=["name", "score"],
)
```

```python
result = await dataset.aloc(
    row_selector=([0, 2, 4], ["name", "score"]),
)
```

`columns` must be `None`, `"*"`, or a list/tuple of column names. `chunk_size`
is currently rejected by the public `loc` wrapper. The public wrapper does not
support SQL `WHERE` strings as row selectors.

### `take`

`take` is a convenience method over `iloc`.

```python
rows = dataset.take(indices=[0, 2], axis=0)
columns = dataset.take(indices=[1, 3], axis=1)
```

```python
rows = await dataset.atake(indices=[0, 2], axis=0)
columns = await dataset.atake(indices=[1, 3], axis=1)
```

Use `axis=0` for rows and `axis=1` for columns.

## Transforming Selections

### `where`

`where` applies a SQL condition across all columns. Values in rows that match
`cond` are kept. Values in rows that do not match are replaced with `other`, or
`NULL` when `other` is omitted.

```python
result = dataset.where(cond="score > 85")
```

```python
result = await dataset.awhere(
    cond="score > 85",
    other=0,
)
```

### `select_dtypes`

`select_dtypes` keeps columns whose backend type maps to the requested
categories.

```python
numeric = dataset.select_dtypes(include="numeric")
without_text = dataset.select_dtypes(exclude="categorical")
```

```python
dates = await dataset.aselect_dtypes(
    include=["date", "timestamp"],
)
```

Supported categories are `numeric`, `categorical`, `date`, `timestamp`, and
`other`. At least one of `include` or `exclude` must be provided.

## Return Value Format

Selection methods return dictionaries. Common keys are:

| Key | Type | Description |
| --- | --- | --- |
| `is_error` | `bool` | `True` when the operation failed |
| `message` | `str` | Human-readable operation summary |
| `error_message` | `str` or `None` | Error details when `is_error` is true |
| `result` | `pd.DataFrame`, `pd.Series`, or `None` | DataFrame or row result |
| `value` | scalar or `None` | Scalar result for `at`, `iat`, and single-cell `iloc` |
| `new_table` | `str` or `None` | Transient table name created by table-producing operations |
| `iterator` | async generator or `None` | Chunk iterator when supported and `chunk_size` is set |
| `selected_columns` | `list` or `None` | Columns selected by `select_dtypes` |

Always check `is_error` before consuming `result` or `value`.

## Transient Tables

Table-producing methods create transient tables when called through a connected
context. These tables are stored in the `transient` schema and tracked in the
transient registry. The response includes the bare transient table name in
`new_table`.

`where`, `select_dtypes`, and the core `asof`/`loc` paths support streaming
with `chunk_size` by returning `iterator`. The public `loc` wrapper currently
returns an error when `chunk_size` is provided.

## Backend Behavior

Selection supports DuckDB and PostgreSQL adapters:

- DuckDB uses `PRAGMA table_info`, quoted identifiers, `ARRAY`/`UNNEST`
  positional joins, and backend-specific SQL placeholders.
- PostgreSQL uses `information_schema.columns`, typed `UNNEST` arrays,
  quoted identifiers, and PostgreSQL placeholders.

Column names are sanitized and quoted before SQL is generated. Methods that
accept column names resolve them against the live table schema.

## Errors

Selection methods catch exceptions and return `is_error=True` instead of
raising in normal public use.

- Missing columns or missing row labels return `KeyError` details.
- Out-of-bounds positions return `IndexError` details.
- Invalid selector shapes return `ValueError` or `TypeError` details.
- Unsupported `axis` values in `take` return an error response.
- Calling `loc` with `chunk_size` returns an error response.

## API Reference

::: src.wrappers.analytix.selection.SelectionWrapper
    options:
      show_root_heading: true
      show_root_full_path: true
      members:
        - aasof
        - asof
        - aat
        - at
        - aiat
        - iat
        - aget
        - get
        - aloc
        - loc
        - awhere
        - where
        - aselect_dtypes
        - select_dtypes
        - ailoc
        - iloc
        - atake
        - take
