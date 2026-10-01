# Filter

Source: `src/memframe/wrappers/analytix/filter.py`

`FilteringWrapper` is the public filtering interface exposed through a
`ContextManager`. It filters *rows*: a string expression is parsed into a
predicate tree, compiled to a SQL `WHERE` clause, and the matching rows are
written to a freshly created transient table — the source table is never
mutated. Unlike comparison, which adds a boolean column, filtering returns
only the rows that satisfy the expression.

```python
dataset = mf.upload_df(frame)
result = dataset.filter("salary >= 50000 && status == 'active'")
```

The lower-level files are implementation details:

- `src/memframe/core/analytix/filter/filter_I.py` defines the predicate tree
  and compiles it to parameterized SQL per backend.
- `src/memframe/core/analytix/filter/filter_II/` executes the filter as
  `CREATE TABLE ... AS SELECT ... WHERE ...` (with a `MergeTree` engine clause
  on ClickHouse) and returns a sample or a chunked iterator.
- `src/memframe/core/orchestrator/analytix/filter.py` resolves the active dataset
  context, parses string expressions, and passes persistence metadata.
- `src/memframe/wrappers/analytix/filter.py` exposes synchronous and asynchronous
  public methods.

## Public API

| Synchronous | Asynchronous | Purpose |
| --- | --- | --- |
| `filter(expression, columns="*", chunk_size=None, create_flag=False)` | `await afilter(...)` | Keep rows satisfying a string expression |

Public methods return the resulting DataFrame directly — or, when `chunk_size`
is given, a dict carrying an async `iterator` of DataFrames (see
[Chunked streaming](#chunked-streaming)).

## Query Syntax

Comparisons use `>`, `<`, `>=`, `<=`, `==`, `!=`; combine them with `&&`/`||`
(or the `and`/`or` keywords) and parentheses; quote string literals; write
dates as `YYYY-MM-DD` (or `YYYY-MM-DD HH:MM:SS`); extract datetime fields with
`col.dt.<field>` (`year`, `month`, `day`, `hour`, `minute`, `second`,
`dayofweek`/`dow`, `dayofyear`/`doy`, `week`/`weekofyear`, `quarter`):

```python
result = dataset.filter("salary >= 50000 && status == 'active'")
```

```python
hired_2024 = dataset.filter("hired_at.dt.year == 2024")
#    salary  status   hired_at
# 0   60000  active 2024-03-01
# 1   80000    left 2024-06-01
```

Relative time is expressed against the database clock:

```python
recent = dataset.filter("hired_at >= '2024-03-01' && hired_at <= '2024-06-01'")
```

## Cross-Dtype Filtering

Unlike `compare()`, filtering imposes no same-type restriction — numeric,
categorical, and datetime conditions mix freely in one expression:

```python
result = dataset.filter(
    "salary >= 50000 && status == 'active' && hired_at >= '2024-01-01'"
)
#    salary  status   hired_at
# 0   60000  active 2024-03-01
```

## Column Subsets

Pass `columns` to project the result instead of returning `*`:

```python
lean = dataset.filter("salary >= 50000", columns=["name", "salary"])
```

## Chunked Streaming

Pass `chunk_size` to stream the result as an async iterator of DataFrames
instead of materializing one sample:

```python
response = dataset.filter("salary >= 50000", chunk_size=1000)
async for chunk in response["iterator"]:
    process(chunk)
```

The iterator reads the transient table lazily, so streaming requires
`deep_cache=True` on the `MemFrame` — in default signature-only mode the table
is dropped when the call returns and the first pull fails.

## Flag Column

Pass `create_flag=True` to write a boolean mask in place onto the **source
table**: matching rows read `True`, everything else `False` (null-predicate
rows read `False`, not `NULL`). The filtered result itself is unchanged.

```python
dataset.filter("salary >= 50000", create_flag=True)
print(dataset.head(n=10))
#    salary  status  filter_flag
# 0   40000  active        False
# 1   60000  active         True
# 2   80000    left         True
# 3   55000  active         True
```

Rules:

- The flag is only written for a **proper subset**. Empty and full-table
  matches skip it (`flag_column` is `None`) — a constant column is useless.
- The column name is fixed (`filter_flag`, auto-suffixed to `filter_flag_1`,
  … on collision). Re-filtering the same dataset therefore adds a new column
  next to the stale one rather than overwriting it.
- The source table is mutated: later ops on the dataset see the flag column.
  The raw response dict carries the name under `flag_column`; the public
  `dataset.filter(...)` unwraps to a bare DataFrame, so read it via
  `FilteringWrapper` when you need the name programmatically.

## Errors

- Unparseable strings raise `ParseError` (e.g. `dataset.filter("salary ??? 5")`
  raises `ParseError: Expected comparison operator, got '???'`).
- Backend and validation failures surface as the canonical error dict
  (`is_error=True` with `error_message`).
- Unsupported backends raise `NotImplementedError`.

## Caching

`filter` applies `@record_call`, so calls are recorded in the transient
registry like every other orchestrated operation. With `deep_cache=True` the
result table persists and repeat calls replay it; otherwise the call is
signature-logged only.

## API Reference

::: memframe.wrappers.analytix.filter.FilteringWrapper
    options:
      show_root_heading: true
      show_root_full_path: true
      members:
        - afilter
        - filter
