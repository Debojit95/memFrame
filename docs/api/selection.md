# Selection

Source: `src/wrappers/analytix/selection.py`

`SelectionWrapper` is the public selection interface exposed through a
`ContextManager`. It provides pandas-like row and column access against the
active backend table, including scalar lookup, column retrieval,
integer-location selection, and dtype-based column selection.

Users normally call selection methods directly on a dataset context returned by
an upload operation:

```python
dataset = mf.upload_df(frame)
result = dataset.iloc(row_indexer="0:5", col_indexer="0:3")
```

The same methods are also available from `dataset.select`.

The lower-level files are implementation details:

- `src/core/analytix/selection.py` builds and executes backend-specific SQL.
- `src/core/orchestrator/analytix/selection.py` resolves the active dataset
  context, normalizes public indexers, and maps column names to positions.
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
| `iloc(row_indexer=None, col_indexer=None, columns=None, index_column=None)` | `await ailoc(...)` | Integer/label-location row and column selection, or raw SQL `WHERE` |
| `select_dtypes(include=None, exclude=None, chunk_size=None)` | `await aselect_dtypes(...)` | Columns by database type category |

Public methods return DataFrames, Series, scalars, dictionaries, or iterators
directly. Invalid operations raise `OperationError`.

## Usage Overview

```python
dataset = mf.upload_csv("data/sales.csv")

frame = dataset.get(["customer_id", "amount"])
```

```python
dataset = await mf.aupload_csv("data/sales.csv")

frame = await dataset.ailoc(row_indexer="0:10", columns=["amount"])
```

Selection methods are exposed directly through context forwarding. You can use
`dataset.iloc(...)` or the explicit `dataset.select.iloc(...)` form.

## Scalar Access

### `at`

`at` returns one value by matching a row label in an index column and reading a
named column. If `index_column` is omitted, selection uses `id` when present,
otherwise the first table column.

```python
result = dataset.at(
    row_label=103,
    column_label="name",
    index_column="id",
)
print(result)
```

```python
result = await dataset.aat(
    row_label=103,
    column_label="name",
    index_column="id",
)
```

Parameters:

| Parameter | Type | Description |
| --- | --- | --- |
| `row_label` | any | Value matched against the index column. |
| `column_label` | `str` | Column whose scalar value should be returned. |
| `index_column` | `str` or `None` | Column used to locate the row. Defaults to `id` if present, otherwise the first column. |

Return behavior:

- On success, the scalar is returned directly.
- Missing `column_label`, missing `index_column`, or missing `row_label`
  raises `OperationError`.

### `iat`

`iat` returns one value by 0-based row position and column name. It uses
`ROW_NUMBER() OVER (ORDER BY ...)`, so `order_by` is required for deterministic
row numbering.

```python
result = dataset.iat(
    row_position=2,
    column_label="score",
    order_by="id",
)
```

```python
result = await dataset.aiat(
    row_position=2,
    column_label="score",
    order_by=["id"],
)
```

Parameters:

| Parameter | Type | Description |
| --- | --- | --- |
| `row_position` | `int` | Zero-based row position after ordering. |
| `column_label` | `str` | Column whose scalar value should be returned. |
| `order_by` | `str` or `list[str]` | Column or columns used to order rows before position lookup. |

Out-of-range row positions raise `OperationError` with `IndexError` details.

### `asof`

`asof` returns the last row whose `on` column is less than or equal to each
reference value in `where`. A scalar `where` returns one row as a pandas
Series; a list returns a DataFrame.

```python
result = dataset.asof(
    where="2023-03-15",
    on="join_date",
    subset=["score"],
)
```

```python
result = await dataset.aasof(
    where=["2023-03-15", "2023-04-10"],
    on="join_date",
)
```

Parameters:

| Parameter | Type | Description |
| --- | --- | --- |
| `where` | `str`, timestamp-like, or `list` | Reference value or values. Each value is converted with `pandas.Timestamp`. |
| `on` | `str` | Timestamp/date column used for `<= where` matching and descending order. |
| `subset` | `str`, `list[str]`, or `None` | Columns that must be non-null in a matched row. Defaults to all columns. |
| `chunk_size` | `int` or `None` | Accepted by the public wrapper and passed to the core operation. |

When `subset` is provided, only rows with non-null values in those columns are
eligible for matching.

## Column Retrieval

### `get`

`get` returns requested columns as a DataFrame. Missing columns are added with
`default`.

```python
result = dataset.get(keys=["name", "score"])
```

```python
result = await dataset.aget(
    keys=["name", "missing_column"],
    default="MISSING",
)
```

Parameters:

| Parameter | Type | Description |
| --- | --- | --- |
| `keys` | `str` or `list[str]` | Column name or column names to retrieve. |
| `default` | any | Value used for requested columns that do not exist. |

If none of the requested columns exist, the result is a DataFrame containing the
requested keys filled with `default`.

## DataFrame Selection

### `iloc`

`iloc` selects rows and columns by integer position, by label list, or by a raw
SQL `WHERE` clause. It can return either a DataFrame or a scalar.

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

```python
result = dataset.iloc(row_indexer=("1:4", "0:2"))
```

```python
# Raw SQL WHERE (read-only against the active table)
result = dataset.iloc(row_indexer="age > 24 AND city = 'NYC'")
```

```python
# Label-list selection against an index column
result = dataset.iloc(
    row_indexer=["a", "c", "d"],
    index_column="name",
)
```

Parameters:

| Parameter | Type | Description |
| --- | --- | --- |
| `row_indexer` | `int`, `list[int]`, `slice`, boolean list, slice string, tuple, `list[str]`, `str`, or `None` | Row selector. Integer/positional forms select by row position. A `list[str]` selects rows whose `index_column` value is in the list. A `str` is treated as a raw SQL `WHERE` clause evaluated against the active table (read-only; a single statement — `;` is rejected). `None` selects all rows. A tuple must be `(rows, cols)`. |
| `col_indexer` | `int`, `list[int]`, `slice`, `list[str]`, slice string, or `None` | Column selector by position, or by names when a string/list of strings is provided. `None` selects all columns. |
| `columns` | `str`, `list[str]`, `tuple[str, ...]`, or `None` | Named-column alternative to `col_indexer`. Cannot be combined with `col_indexer`. |
| | | |
| `index_column` | `str` or `None` | Column used to resolve a `list[str]` `row_indexer` to matching rows. Required when `row_indexer` is a label list. |

Supported selector forms:

| Form | Example | Behavior |
| --- | --- | --- |
| Integer | `2` | Select one row or column. Negative indices are supported. |
| Integer list | `[0, 2, 4]` | Select positions in the given order. |
| Slice | `slice(1, 4)` | Select a Python slice. |
| Slice string | `"1:4"` | Parsed as `slice(1, 4)`. |
| Boolean mask | `[True, False, True]` | Select rows/columns where mask is true. Length must match the axis. |
| Tuple style | `("0:3", "1:4")` | Provides row and column indexers together. |
| Named columns | `columns=["name"]` | Converts names to column positions. |
| Label list | `row_indexer=["a", "c"], index_column="name"` | Selects rows whose `index_column` value is in the list. |
| Raw SQL `WHERE` | `row_indexer="age > 24"` | Evaluated as a SQL `WHERE` against the active table (read-only). |

When both row and column indexers resolve to a single cell, the response
contains the scalar under `result`.

### `select_dtypes`

`select_dtypes` keeps columns whose backend type maps to requested categories.

```python
numeric = dataset.select_dtypes(include="numeric")
without_text = dataset.select_dtypes(exclude="categorical")
```

```python
dates = await dataset.aselect_dtypes(
    include=["date", "timestamp"],
)
```

Parameters:

| Parameter | Type | Description |
| --- | --- | --- |
| `include` | `str`, `list[str]`, or `None` | Categories to include. |
| `exclude` | `str`, `list[str]`, or `None` | Categories to exclude after include filtering. |
| `chunk_size` | `int` or `None` | If provided, the result can be returned as an async chunk iterator. |

Supported categories:

| Category | Typical backend types |
| --- | --- |
| `numeric` | integer, bigint, decimal, numeric, real, float, double |
| `categorical` | varchar, char, text, string-like types |
| `date` | date |
| `timestamp` | timestamp, timestamptz, datetime |
| `other` | Any type not matched by the above categories |

At least one of `include` or `exclude` must be provided. The selected DataFrame
is returned directly.

## Return Values and Errors

Public selection methods return DataFrames, Series, scalars, dictionaries, or
async iterators directly. Generated-table metadata remains internal to cache
and AI execution. Failed operations raise `OperationError`.

## Generated Tables

Table-producing selection methods create generated tables internally when
called through a connected context.

`iloc` and `select_dtypes` commonly create generated tables.
`at`, `iat`, `get`, and scalar `asof` are read-oriented operations.

When `chunk_size` is supported, the method returns an async iterator.

## Backend Behavior

Selection supports DuckDB and PostgreSQL adapters:

- DuckDB uses `PRAGMA table_info`, quoted identifiers, `ARRAY`/`UNNEST`
  positional joins, and backend-specific SQL placeholders.
- PostgreSQL uses `information_schema.columns`, typed `UNNEST` arrays, quoted
  identifiers, and PostgreSQL placeholders.
- Column names are sanitized and quoted before SQL is generated.
- Methods that accept column names resolve them against the live table schema.
- Integer-position selection uses SQL row numbering and does not imply a stable
  row order unless the operation explicitly orders rows.

## Errors

Selection methods raise `OperationError` for invalid input or backend failures.

- Missing columns or missing row labels return `KeyError` details.
- Out-of-bounds positions return `IndexError` details.
- Invalid selector shapes return `ValueError` or `TypeError` details.
- Boolean masks must match the selected axis length.
- `iloc` rejects simultaneous `col_indexer` and `columns`.
- `iloc` rejects a `str` row_indexer (raw `WHERE`) when backend context is missing, and a `list[str]` row_indexer when `index_column` is missing.
- `select_dtypes` requires at least one of `include` or `exclude`.

## API Reference

::: memframe.wrappers.analytix.selection.SelectionWrapper
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
        - aselect_dtypes
        - select_dtypes
        - ailoc
        - iloc
