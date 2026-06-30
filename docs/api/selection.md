# Selection

Source: `src/wrappers/analytix/selection.py`

`SelectionWrapper` is the public selection interface exposed through a
`ContextManager`. It provides pandas-like row and column access against the
active backend table, including scalar lookup, column retrieval,
integer-location selection, conditional replacement, dtype-based column
selection, and row/column taking.

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
| `loc(row_selector, columns=None, index_column=None, chunk_size=None)` | `await aloc(...)` | Named-column wrapper over iloc-style row selection |
| `iloc(row_indexer=None, col_indexer=None, columns=None)` | `await ailoc(...)` | Integer-location row and column selection |
| `where(cond, other=None, chunk_size=None)` | `await awhere(...)` | Conditional replacement |
| `select_dtypes(include=None, exclude=None, chunk_size=None)` | `await aselect_dtypes(...)` | Columns by database type category |
| `take(indices, axis=0)` | `await atake(...)` | Rows or columns by integer indices |

All methods return a dictionary with `is_error`, `message`, `error_message`,
and either `result`, `value`, `iterator`, or method-specific metadata.

## Usage Overview

```python
dataset = mf.upload_csv("data/sales.csv")

result = dataset.get(["customer_id", "amount"])
if not result["is_error"]:
    frame = result["result"]
```

```python
dataset = await mf.aupload_csv("data/sales.csv")

result = await dataset.ailoc(row_indexer="0:10", columns=["amount"])
if not result["is_error"]:
    frame = result["result"]
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
print(result["value"])
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

- On success, the scalar is returned under `value`.
- Missing `column_label`, missing `index_column`, or missing `row_label`
  returns `is_error=True`.

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

Out-of-range row positions return `is_error=True` with `IndexError` details.

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

`iloc` selects rows and columns by integer position. It can return either a
DataFrame or a scalar.

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

Parameters:

| Parameter | Type | Description |
| --- | --- | --- |
| `row_indexer` | `int`, `list[int]`, `slice`, boolean list, slice string, tuple, or `None` | Row selector. `None` selects all rows. A tuple must be `(rows, cols)`. |
| `col_indexer` | `int`, `list[int]`, `slice`, `list[str]`, slice string, or `None` | Column selector by position, or by names when a string/list of strings is provided. `None` selects all columns. |
| `columns` | `str`, `list[str]`, `tuple[str, ...]`, or `None` | Named-column alternative to `col_indexer`. Cannot be combined with `col_indexer`. |

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

When both row and column indexers resolve to a single cell, the response
contains `value` instead of `result`.

### `loc`

The public `loc` wrapper accepts row selectors plus optional named columns, then
routes the request through the same integer-position path used by `iloc`.

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

Parameters:

| Parameter | Type | Description |
| --- | --- | --- |
| `row_selector` | int/list/slice/boolean mask/slice string/tuple | Row selector passed through the iloc-style path. A tuple must be `(rows, columns)`. |
| `columns` | `list[str]`, tuple of strings, `"*"`, or `None` | Named columns to select. `None` or `"*"` selects all columns. |
| `index_column` | `str` or `None` | Accepted for compatibility. When the wrapper succeeds, it is reported as ignored. |
| `chunk_size` | `int` or `None` | Currently rejected by the public wrapper. |

Current limitations:

- `columns` must be `None`, `"*"`, or a list/tuple of column names.
- Public `loc` does not support SQL `WHERE` strings as row selectors.
- Passing `chunk_size` returns an error response.

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

Parameters:

| Parameter | Type | Description |
| --- | --- | --- |
| `indices` | `list[int]` | Row or column positions to select. Negative row indices are supported by `iloc`. |
| `axis` | `0` or `1` | `0` selects rows; `1` selects columns. |

Invalid `axis` values return `is_error=True`.

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

Parameters:

| Parameter | Type | Description |
| --- | --- | --- |
| `cond` | `str` | SQL boolean expression evaluated per row. |
| `other` | scalar, SQL expression, or `None` | Replacement for rows that do not satisfy `cond`. `None` becomes SQL `NULL`. |
| `chunk_size` | `int` or `None` | If supported by the core path, returns an async chunk iterator instead of a DataFrame sample. |

The operation creates a generated table and returns `new_table` when backend
context is available.

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

At least one of `include` or `exclude` must be provided. The response includes
`selected_columns`.

## Return Value Format

Selection methods return dictionaries. Common keys are:

| Key | Type | Description |
| --- | --- | --- |
| `is_error` | `bool` | `True` when the operation failed. |
| `message` | `str` | Human-readable operation summary. |
| `error_message` | `str` or `None` | Error details when `is_error` is true. |
| `result` | `pd.DataFrame`, `pd.Series`, or `None` | DataFrame or row result for non-scalar operations. |
| `value` | scalar or `None` | Scalar result for `at`, `iat`, and single-cell `iloc`. |
| `new_table` | `str` or `None` | Generated/transient table name for table-producing operations. |
| `iterator` | async generator or `None` | Chunk iterator when supported and `chunk_size` is set. |
| `selected_columns` | `list` or `None` | Columns selected by `select_dtypes`. |
| `row_indices` | `list` or absent | Row positions resolved by `iloc`/`take`. |
| `col_indices` | `list` or absent | Column positions resolved by `iloc`/`take`. |

Always check `is_error` before consuming `result`, `value`, or `iterator`.

## Generated Tables

Table-producing selection methods create generated tables when called through a
connected context. The response includes the generated table name under
`new_table`.

`iloc`, `take`, `where`, and `select_dtypes` commonly create generated tables.
`at`, `iat`, `get`, and scalar `asof` are read-oriented operations.

When `chunk_size` is supported, the response contains `iterator` and omits the
full DataFrame result.

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

Selection methods catch exceptions and return `is_error=True` in normal public
use.

- Missing columns or missing row labels return `KeyError` details.
- Out-of-bounds positions return `IndexError` details.
- Invalid selector shapes return `ValueError` or `TypeError` details.
- Boolean masks must match the selected axis length.
- `iloc` rejects simultaneous `col_indexer` and `columns`.
- `loc` rejects invalid `columns`, tuple shape errors, and `chunk_size`.
- `select_dtypes` requires at least one of `include` or `exclude`.
- `take` requires `axis` to be `0` or `1`.

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
