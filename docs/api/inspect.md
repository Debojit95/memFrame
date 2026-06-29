# Inspect

Source: `src/wrappers/analytix/inspect.py`

`TableOpsWrapper` is the public inspection and table-utility interface exposed
through a `ContextManager`. It provides pandas-like methods for previewing
rows, reading schema metadata, computing summaries, and applying a small set of
table-level transformations against the active backend table.

Users normally call these methods directly on a dataset context returned by an
upload operation:

```python
dataset = mf.upload_df(frame)
result = dataset.head()
```

The lower-level files are implementation details:

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

All methods return a dictionary with `is_error`, `message`, and either
`result`, `iterator`, or error details.

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

Inspect methods are exposed directly through the context forwarding behavior.
The same methods are also available from `dataset.inspect`.

## Row Preview

### `head`

`head` returns the first `n` rows. If `columns` is provided, only valid column
names from that list are selected.

```python
result = dataset.head(n=10)
```

```python
result = await dataset.ahead(
    n=5,
    columns=["customer_id", "amount"],
)
```

### `tail`

`tail` returns the last `n` rows by counting the table and applying an offset.

```python
result = dataset.tail(n=5)
```

```python
result = await dataset.atail(n=5)
```

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

`random_state` is applied through PostgreSQL `setseed` for PostgreSQL. DuckDB
accepts the parameter but does not currently use it to seed sampling.

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

When chunking is enabled, the response contains `iterator` instead of `result`.

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

The result is also saved as a generated table when the method-call logger
receives backend context.

### `describe`

`describe` computes numeric statistics: `count`, `mean`, `std`, `min`, `25%`,
`50%`, `75%`, and `max`.

```python
result = dataset.describe()
```

```python
result = await dataset.adescribe(columns=["amount", "score"])
```

If `columns` is omitted, numeric columns are discovered from the backend schema.

### `null_analysis`

`null_analysis` reports whether each selected column contains nulls and the
percentage of missing values.

```python
result = dataset.null_analysis(columns=["score", "note"])
```

```python
result = await dataset.anull_analysis()
```

Pass `None`, `"*"`, or `["*"]` to analyze all columns.

### `corr`

`corr` computes a Pearson correlation matrix for numeric columns.

```python
result = dataset.corr(columns=["amount", "score"])
```

```python
result = await dataset.acorr()
```

At least two numeric columns are required. The `method` parameter is accepted by
the wrapper, but the core implementation currently computes Pearson
correlation.

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

Supported dtype aliases are integer, float, double, and text/string aliases.

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

`datetime_action` controls datetime columns: `skip`, `cast_string`,
`extract_epoch`, `keep`, or `error`.

### `rename`

`rename` renames columns in place and returns a current table preview.

```python
result = dataset.rename(columns={"old_name": "new_name"})
```

```python
result = await dataset.arename(columns={"old_name": "new_name"})
```

### `set_index` and `reset_index`

`set_index` adds a primary-key constraint over the selected columns.
`reset_index` recreates an `id` index column.

```python
result = dataset.set_index(columns=["customer_id"])
```

```python
result = await dataset.areset_index()
```

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

### `resample`

`resample` groups timestamp data by a time bucket and aggregate.

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

## Property Methods

These methods return compact dictionary payloads under `result`:

| Method | Result key |
| --- | --- |
| `axes()` | `{"axes": [row_index, columns]}` |
| `columns()` | `{"columns": [...]}` |
| `dtypes()` | `{"dtypes": {column: db_type}}` |
| `first_valid_index()` | `{"first_valid_index": 0}` or `None` |
| `memory_usage()` | `{"memory_bytes": value}` |
| `ndim()` | `{"ndim": 2}` |
| `shape()` | `{"shape": (rows, columns)}` |
| `size()` | `{"size": rows * columns}` |
| `values()` | `{"values": [[...], ...]}` |

```python
print(dataset.shape()["result"]["shape"])
print(dataset.columns()["result"]["columns"])
```

```python
shape = await dataset.ashape()
dtypes = await dataset.adtypes()
```

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

The exact payload is returned under `result` or iterator-specific response
fields from the core table engine.

## Return Value Format

Inspect methods return dictionaries. Common keys are:

| Key | Type | Description |
| --- | --- | --- |
| `is_error` | `bool` | `True` when the operation failed |
| `message` | `str` | Human-readable operation summary |
| `error_message` | `str` or `None` | Error details when `is_error` is true |
| `result` | Any | DataFrame, scalar-like dict, or iterator payload |
| `iterator` | async generator or `None` | Chunk iterator from `full_table` |
| `involved_cols` | `list` | Columns read or analyzed |
| `generated_cols` | `list` | Columns produced by the operation |
| `new_table` | `str` or `None` | Generated table name for logged summary outputs |
| `result_metadata` | `dict` | Row counts, selected columns, and method-specific metadata |

Always check `is_error` before consuming `result` or `iterator`.

## Backend Behavior

Inspect supports DuckDB and PostgreSQL adapters:

- Both backends use quoted identifiers and schema-aware table names.
- Schema discovery is delegated to the active database adapter.
- `describe` uses backend-specific percentile functions.
- `corr` uses SQL `CORR` over numeric columns.
- PostgreSQL can report relation memory usage; DuckDB currently returns
  `None` for `memory_usage`.

Column and table identifiers are sanitized before SQL is generated.

## Errors

Inspect methods catch exceptions and return `is_error=True` in normal public
use.

- Invalid `chunk_size` in `full_table` returns an error response.
- `describe` returns an error when no numeric columns are available.
- `corr` returns an error when fewer than two numeric columns are available.
- `astype` returns an error for missing columns or unsupported dtype aliases.
- `insert` returns an error when `value` is not a list or its length does not
  match the row count.
- `map` returns an error when no selected columns are compatible with the SQL
  expression.

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
