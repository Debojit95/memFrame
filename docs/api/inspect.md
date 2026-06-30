# Inspect

Source: `src/wrappers/analytix/inspect.py`

`TableOpsWrapper` is the public inspection and table-utility interface exposed
through a `ContextManager`. It provides pandas-like methods for previewing
rows, reading schema metadata, computing summaries, applying lightweight table
utilities, and retrieving compact table properties from the active backend
table.

Users normally call inspect methods directly on a dataset context returned by
an upload operation:

```python
dataset = mf.upload_df(frame)
result = dataset.head(n=5)
```

The same methods are also available from `dataset.inspect`.

The lower-level files are implementation details:

- `src/core/analytix/table_ops.py` builds and executes backend-specific SQL.
- `src/core/orchestrator/analytix/table_ops.py` resolves the active dataset
  context and delegates work to the core table engine.
- `src/wrappers/analytix/inspect.py` exposes synchronous and asynchronous
  public methods.

## Public API

Every inspect operation has synchronous and asynchronous forms:

| Synchronous | Asynchronous | Purpose |
| --- | --- | --- |
| `head(n=10, columns=None)` | `await ahead(...)` | First rows |
| `tail(n=10, columns=None)` | `await atail(...)` | Last rows |
| `sample(n=10, columns=None, random_state=None)` | `await asample(...)` | Random rows |
| `info()` | `await ainfo()` | Per-column table information |
| `describe(columns=None)` | `await adescribe(...)` | Numeric descriptive statistics |
| `null_analysis(columns=None)` | `await anull_analysis(...)` | Null distribution by column |
| `corr(columns=None, method="pearson")` | `await acorr(...)` | Numeric correlation matrix |
| `full_table(columns=None, chunk_size=None)` | `await afull_table(...)` | Full table or chunk iterator |
| `astype(columns=None, dtypes=None, dtype_map=None)` | `await aastype(...)` | Cast selected columns |
| `insert(column, value)` | `await ainsert(...)` | Add a column from a value list |
| `map(func, na_action=None, columns=None, datetime_action="skip")` | `await amap(...)` | Apply SQL expression to values |
| `rename(columns)` | `await arename(...)` | Rename columns |
| `set_index(columns)` | `await aset_index(...)` | Add a primary-key index |
| `reset_index()` | `await areset_index()` | Recreate an `id` index column |
| `update(on, other_table, other_schema="upload", overwrite=True, errors="ignore")` | `await aupdate(...)` | Update from another table |
| `resample(time_column, rule, agg="COUNT", value_column=None, label="left", closed="left")` | `await aresample(...)` | Time-series aggregation |
| `axes()` | `await aaxes()` | Row and column axes |
| `columns()` | `await acolumns()` | Column labels |
| `dtypes()` | `await adtypes()` | Column database types |
| `first_valid_index()` | `await afirst_valid_index()` | First row with non-null values |
| `memory_usage()` | `await amemory_usage()` | Backend table memory usage |
| `ndim()` | `await andim()` | Number of dimensions |
| `shape()` | `await ashape()` | Row and column count |
| `size()` | `await asize()` | Total element count |
| `values()` | `await avalues()` | Table values as nested lists |
| `items()` | `await aitems()` | Column/value iterator result |
| `iterrows()` | `await aiterrows()` | Row iterator result |
| `itertuples(index=True)` | `await aitertuples(...)` | Tuple-style row iterator result |

All methods return a dictionary with `is_error`, `message`, `error_message`,
and either `result`, `iterator`, or method-specific metadata.

## Usage Overview

```python
dataset = mf.upload_csv("data/sales.csv")

preview = dataset.head(n=5, columns=["customer_id", "amount"])
if not preview["is_error"]:
    frame = preview["result"]
```

```python
dataset = await mf.aupload_csv("data/sales.csv")

summary = await dataset.adescribe(columns=["amount"])
if not summary["is_error"]:
    frame = summary["result"]
```

Inspect methods are exposed directly through context forwarding. You can use
`dataset.head(...)` or the explicit `dataset.inspect.head(...)` form.

## Row Preview

### `head`

`head` returns the first `n` rows from the active table.

```python
result = dataset.head(n=10)
```

```python
result = await dataset.ahead(
    n=5,
    columns=["customer_id", "amount"],
)
```

Parameters:

| Parameter | Type | Description |
| --- | --- | --- |
| `n` | `int` | Maximum number of rows to return. Defaults to `10`. |
| `columns` | `list[str]` or `None` | Optional columns to include. Invalid names are ignored; if none are valid, all columns are used. |

The response includes a DataFrame under `result` and row/column metadata under
`result_metadata`.

### `tail`

`tail` returns the last `n` rows by counting total rows and applying an offset.

```python
result = dataset.tail(n=5)
```

```python
result = await dataset.atail(n=5, columns=["name", "score"])
```

Parameters:

| Parameter | Type | Description |
| --- | --- | --- |
| `n` | `int` | Maximum number of rows to return. Defaults to `10`. |
| `columns` | `list[str]` or `None` | Optional columns to include. |

### `sample`

`sample` returns random rows using backend `RANDOM()` ordering.

```python
result = dataset.sample(n=10, random_state=42)
```

```python
result = await dataset.asample(
    n=10,
    columns=["name", "score"],
)
```

Parameters:

| Parameter | Type | Description |
| --- | --- | --- |
| `n` | `int` | Number of random rows to return. Defaults to `10`. |
| `columns` | `list[str]` or `None` | Optional columns to include. |
| `random_state` | `int` or `None` | Optional seed. PostgreSQL applies it through `setseed`; DuckDB currently accepts but does not use it for deterministic sampling. |

### `full_table`

`full_table` returns all rows as a DataFrame unless `chunk_size` is provided.

```python
result = dataset.full_table(columns=["id", "name"])
```

```python
result = await dataset.afull_table(chunk_size=1000)
async for chunk in result["iterator"]:
    ...
```

Parameters:

| Parameter | Type | Description |
| --- | --- | --- |
| `columns` | `list[str]` or `None` | Optional columns to include. |
| `chunk_size` | `int` or `None` | Positive row count per chunk. When set, response contains `iterator` instead of `result`. |

Invalid or non-positive `chunk_size` returns an error response.

## Summary Methods

### `info`

`info` returns one row per column with database type, null counts, non-null
counts, null percentage, and distinct count.

```python
result = dataset.info()
```

```python
result = await dataset.ainfo()
```

Parameters: none.

Return behavior:

- `result` is a DataFrame with one row per column.
- `new_table` contains the generated summary table name when persistence
  context is available.
- `result_metadata.table_info` contains table-level metadata such as row count,
  column count, and backend size information.

### `describe`

`describe` computes numeric statistics: `count`, `mean`, `std`, `min`, `25%`,
`50%`, `75%`, and `max`.

```python
result = dataset.describe()
```

```python
result = await dataset.adescribe(columns=["amount", "score"])
```

Parameters:

| Parameter | Type | Description |
| --- | --- | --- |
| `columns` | `list[str]` or `None` | Numeric columns to summarize. If omitted, numeric columns are discovered from the backend schema. |

The result DataFrame has a `statistic` column plus one column per summarized
numeric column. If no numeric columns are available, the method returns
`is_error=True`.

### `null_analysis`

`null_analysis` reports whether selected columns contain null values and their
missing percentages.

```python
result = dataset.null_analysis(columns=["score", "note"])
```

```python
result = await dataset.anull_analysis()
```

Parameters:

| Parameter | Type | Description |
| --- | --- | --- |
| `columns` | `list[str]`, `"*"`, or `None` | Columns to analyze. `None`, `"*"`, or `["*"]` analyzes all columns. |

The result DataFrame is indexed by column name when data is available and
contains `contains_null` and `percent_missing` columns.

### `corr`

`corr` computes a numeric correlation matrix.

```python
result = dataset.corr(columns=["amount", "score"])
```

```python
result = await dataset.acorr()
```

Parameters:

| Parameter | Type | Description |
| --- | --- | --- |
| `columns` | `list[str]`, `"*"`, or `None` | Numeric columns to include. Defaults to all numeric columns. |
| `method` | `str` | Accepted by the wrapper. The current core implementation computes Pearson correlation with SQL `CORR`. |

At least two numeric columns are required. Metadata includes the analyzed
columns and up to ten strong correlations.

## Table Utilities

### `astype`

`astype` casts selected columns and returns only the casted columns.

```python
result = dataset.astype(
    dtype_map={"amount": "float64", "customer_id": "str"},
)
```

```python
result = await dataset.aastype(
    columns=["amount"],
    dtypes=["float64"],
)
```

Parameters:

| Parameter | Type | Description |
| --- | --- | --- |
| `columns` | `list[str]` or `None` | Columns to cast when `dtype_map` is not provided. |
| `dtypes` | `list[str]` or `None` | Target dtypes matching `columns` by position. |
| `dtype_map` | `dict[str, str]` or `None` | Direct mapping of column name to target dtype. Takes precedence over `columns`/`dtypes`. |

Supported dtype aliases:

| Alias group | Examples | SQL target |
| --- | --- | --- |
| Integer | `int`, `int8`, `int16`, `int32` | `INTEGER` |
| Big integer | `int64` | `BIGINT` |
| Float | `float`, `float32` | `FLOAT` |
| Double | `float64`, `double` | `DOUBLE` |
| Text | `str`, `string`, `text` | `TEXT` |

### `insert`

`insert` adds a new text column and fills it from a list of values. The list
length must match the row count.

```python
result = dataset.insert(
    column="segment",
    value=["retail", "enterprise", "retail"],
)
```

```python
result = await dataset.ainsert(
    column="segment",
    value=["retail", "enterprise", "retail"],
)
```

Parameters:

| Parameter | Type | Description |
| --- | --- | --- |
| `column` | `str` | New column name. |
| `value` | `list` | Values assigned row-by-row. Must match row count. |

### `map`

`map` applies a SQL expression to compatible columns. Use `x` as the placeholder
for the current column expression.

```python
result = dataset.map(
    func="x * 100",
    columns=["conversion_rate"],
)
```

```python
result = await dataset.amap(
    func="UPPER(x)",
    columns=["segment"],
    na_action="ignore",
)
```

Parameters:

| Parameter | Type | Description |
| --- | --- | --- |
| `func` | `str` | SQL expression using `x` as the current column placeholder. |
| `na_action` | `str` or `None` | `"ignore"` preserves nulls; `None` applies the expression normally. |
| `columns` | `list[str]`, `"*"`, or `None` | Columns to map. Defaults to all columns. |
| `datetime_action` | `str` | How datetime columns are handled. Defaults to `"skip"`. |

Supported `datetime_action` values:

| Value | Behavior |
| --- | --- |
| `skip` | Skip datetime columns. |
| `cast_string` | Cast datetime values to text before applying `func`. |
| `extract_epoch` | Apply `func` to epoch seconds. |
| `keep` | Return datetime column unchanged. |
| `error` | Return an error if a datetime column is selected. |

Numeric columns accept arithmetic expressions. String columns are only applied
when the expression uses string-safe functions such as `UPPER`, `LOWER`,
`LENGTH`, or `TRIM`. Boolean columns are auto-cast to integer for arithmetic
expressions.

### `rename`

`rename` renames columns in place and returns a current table preview.

```python
result = dataset.rename(columns={"old_name": "new_name"})
```

```python
result = await dataset.arename(columns={"old_name": "new_name"})
```

Parameters:

| Parameter | Type | Description |
| --- | --- | --- |
| `columns` | `dict[str, str]` | Mapping of old column names to new names. |

### `set_index` and `reset_index`

`set_index` adds a primary-key constraint over selected columns. `reset_index`
recreates an `id` index column.

```python
result = dataset.set_index(columns=["customer_id"])
```

```python
result = await dataset.areset_index()
```

`set_index` parameters:

| Parameter | Type | Description |
| --- | --- | --- |
| `columns` | `list[str]` | Columns used for the primary key. |

### `update`

`update` updates rows from another backend table using a key column.

```python
result = dataset.update(
    on="customer_id",
    other_table="customer_updates",
    other_schema="upload",
)
```

```python
result = await dataset.aupdate(
    on="customer_id",
    other_table="customer_updates",
    overwrite=True,
    errors="ignore",
)
```

Parameters:

| Parameter | Type | Description |
| --- | --- | --- |
| `on` | `str` | Key column used to match rows. |
| `other_table` | `str` | Table containing update values. |
| `other_schema` | `str` | Schema for `other_table`. Defaults to `"upload"`. |
| `overwrite` | `bool` | Whether matched values should overwrite existing values. |
| `errors` | `str` | Error handling mode passed to the core update implementation. Defaults to `"ignore"`. |

### `resample`

`resample` groups timestamp data into time buckets and applies an aggregate.

```python
result = dataset.resample(
    time_column="event_time",
    rule="day",
    agg="COUNT",
)
```

```python
result = await dataset.aresample(
    time_column="event_time",
    rule="month",
    agg="SUM",
    value_column="amount",
)
```

Parameters:

| Parameter | Type | Description |
| --- | --- | --- |
| `time_column` | `str` | Date/timestamp column used for bucketing. |
| `rule` | `str` | Time bucket rule passed to the core SQL implementation. |
| `agg` | `str` | Aggregate function. Defaults to `"COUNT"`. |
| `value_column` | `str` or `None` | Column aggregated for value-based aggregates. |
| `label` | `str` | Bucket labeling option. Defaults to `"left"`. |
| `closed` | `str` | Bucket boundary option. Defaults to `"left"`. |

## Property Methods

These methods return compact dictionary payloads under `result`:

| Method | Async | Result key |
| --- | --- | --- |
| `axes()` | `aaxes()` | `{"axes": [row_index, columns]}` |
| `columns()` | `acolumns()` | `{"columns": [...]}` |
| `dtypes()` | `adtypes()` | `{"dtypes": {column: db_type}}` |
| `first_valid_index()` | `afirst_valid_index()` | `{"first_valid_index": 0}` or `None` |
| `memory_usage()` | `amemory_usage()` | `{"memory_bytes": value}` |
| `ndim()` | `andim()` | `{"ndim": 2}` |
| `shape()` | `ashape()` | `{"shape": (rows, columns)}` |
| `size()` | `asize()` | `{"size": rows * columns}` |
| `values()` | `avalues()` | `{"values": [[...], ...]}` |

```python
print(dataset.shape()["result"]["shape"])
print(dataset.columns()["result"]["columns"])
```

```python
shape = await dataset.ashape()
dtypes = await dataset.adtypes()
```

Parameters: none for these property methods.

## Iterator Methods

`items`, `iterrows`, and `itertuples` mirror pandas iterator-style APIs. The
wrapper exposes synchronous and asynchronous forms:

```python
items = dataset.items()
rows = dataset.iterrows()
tuples = dataset.itertuples(index=False)
```

```python
items = await dataset.aitems()
rows = await dataset.aiterrows()
tuples = await dataset.aitertuples(index=False)
```

Parameters:

| Method | Parameter | Type | Description |
| --- | --- | --- | --- |
| `items` | none | - | Returns column-oriented iterator payload from the core engine. |
| `iterrows` | none | - | Returns row-oriented iterator payload from the core engine. |
| `itertuples` | `index` | `bool` | Whether tuple rows include an index value. Defaults to `True`. |

The exact payload is returned under `result` or iterator-specific response
fields from the core table engine.

## Return Value Format

Inspect methods return dictionaries. Common keys are:

| Key | Type | Description |
| --- | --- | --- |
| `is_error` | `bool` | `True` when the operation failed. |
| `message` | `str` | Human-readable operation summary. |
| `error_message` | `str` or `None` | Error details when `is_error` is true. |
| `result` | Any | DataFrame, scalar-like dict, or iterator payload. |
| `iterator` | async generator or `None` | Chunk iterator from `full_table`. |
| `involved_cols` | `list` | Columns read or analyzed. |
| `generated_cols` | `list` | Columns produced by the operation. |
| `new_table` | `str` or `None` | Generated table name for logged summary outputs. |
| `result_metadata` | `dict` | Row counts, selected columns, and method-specific metadata. |

Always check `is_error` before consuming `result` or `iterator`.

## Generated Tables

Some inspect methods create generated summary tables when method-call logging
receives backend context. This is most visible for `info`, `describe`,
`null_analysis`, and `corr`, which return `new_table` and saved-table metadata.

Preview methods such as `head`, `tail`, `sample`, and unchunked `full_table`
are read-oriented and return a DataFrame sample directly.

## Backend Behavior

Inspect supports DuckDB and PostgreSQL adapters:

- Both backends use quoted identifiers and schema-aware table names.
- Schema discovery is delegated to the active database adapter.
- `describe` uses backend-specific percentile functions.
- `corr` uses SQL `CORR` over numeric columns.
- `sample` uses backend `RANDOM()` ordering.
- PostgreSQL can report relation memory usage; DuckDB currently returns `None`
  for `memory_usage`.
- Column and table identifiers are sanitized before SQL is generated.

## Errors

Inspect methods catch exceptions and return `is_error=True` in normal public
use.

- Invalid `chunk_size` in `full_table` returns an error response.
- `describe` returns an error when no numeric columns are available.
- `corr` returns an error when fewer than two numeric columns are available.
- `astype` returns an error for missing columns or unsupported dtype aliases.
- `astype` requires either `dtype_map` or matching `columns` and `dtypes`.
- `insert` returns an error when `value` is not a list or its length does not
  match the row count.
- `map` returns an error when `func` is not a SQL expression string or when no
  selected columns are compatible with the expression.
- `rename`, `set_index`, `reset_index`, `update`, and `resample` can return
  backend SQL errors when identifiers or constraints are invalid.

## API Reference

::: src.wrappers.analytix.inspect.TableOpsWrapper
    options:
      show_root_heading: true
      show_root_full_path: true
      members:
        - ahead
        - head
        - atail
        - tail
        - asample
        - sample
        - ainfo
        - info
        - adescribe
        - describe
        - anull_analysis
        - null_analysis
        - acorr
        - corr
        - afull_table
        - full_table
        - aastype
        - astype
        - ainsert
        - insert
        - amap
        - map
        - arename
        - rename
        - aset_index
        - set_index
        - areset_index
        - reset_index
        - aupdate
        - update
        - aresample
        - resample
        - aaxes
        - axes
        - acolumns
        - columns
        - adtypes
        - dtypes
        - afirst_valid_index
        - first_valid_index
        - amemory_usage
        - memory_usage
        - andim
        - ndim
        - ashape
        - shape
        - asize
        - size
        - avalues
        - values
        - aitems
        - items
        - aiterrows
        - iterrows
        - aitertuples
        - itertuples
