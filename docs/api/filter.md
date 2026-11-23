# Filter

> **Example data:** copy the setup cell from [Example Data](example-data.md) into Colab once, then run the examples below. Examples use that `ctx`; local frames are shown inline.

Source: `src/memframe/wrappers/analytix/filter.py`

`FilteringWrapper` is the public filtering interface exposed through a
`ContextManager`. It filters *rows*: a string expression is parsed into a
predicate tree, compiled to a SQL `WHERE` clause, and the matching
`_mf_idx` values are stored as a selection in `memframe_cache`. The result is
a view over the source joined to that selection — no full copy of the source
is created. The source is only mutated when `create_flag=True`.

```python
# ctx is defined in the Example Data setup
result = ctx.filter("salary >= 55000 && region == 'North'")
```

The lower-level files are implementation details:

- `src/memframe/core/analytix/filter/filter_I.py` defines the predicate tree
  and compiles it to parameterized SQL per backend.
- `src/memframe/core/analytix/filter/filter_II/` compiles the predicate,
  writes a `(position, _mf_idx)` selection table to `memframe_cache`, and
  returns the matching rows from a source join (or a chunked iterator).
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
result = ctx.filter("salary >= 55000 && region == 'North'")
```

```python
hired_2024 = ctx.filter("date.dt.year == 2024")
```

Relative time is expressed against the database clock:

```python
recent = ctx.filter("date >= '2024-01-02' && date <= '2024-01-03'")
```

## Cross-Dtype Filtering

Unlike `compare()`, filtering imposes no same-type restriction — numeric,
categorical, and datetime conditions mix freely in one expression:

```python
result = ctx.filter(
    "salary >= 55000 && region == 'North' && date >= '2024-01-01'"
)
```

## Column Subsets

Pass `columns` to project the result instead of returning `*`:

```python
lean = ctx.filter("salary >= 50000", columns=["region", "salary"])
```

## Chunked Streaming

Pass `chunk_size` to stream the result as an async iterator of DataFrames
instead of materializing one sample:

```python
response = ctx.filter("salary >= 50000", chunk_size=1000)
async for chunk in response["iterator"]:
    process(chunk)
```

The iterator reads the selection table lazily, so streaming requires
`deep_cache=True` on the `MemFrame` — in default signature-only mode the
selection is dropped when the call returns and the first pull fails.

## Flag Column

Pass `create_flag=True` to write a boolean mask in place onto the **source
table**: matching rows read `True`, everything else `False` (null-predicate
rows read `False`, not `NULL`). The filtered result itself is unchanged.

```python
ctx.filter("salary >= 50000", create_flag=True)
print(ctx.head(n=10))
#    salary  region  filter_flag_salary_gte_50000
# 0   40000  active                       False
# 1   60000  active                        True
# 2   80000    left                        True
# 3   55000  active                        True
```

Rules:

- The flag is only written for a **proper subset**. Empty and full-table
  matches skip it (`flag_column` is `None`) — a constant column is useless.
- The column name is derived from the filter query (`filter_flag_<query>`):
  operators become words and punctuation collapses to underscores, as in
  `"B > 15"` → `filter_flag_b_gt_15`. Predicate objects use their structural
  signature. Distinct queries therefore get distinct columns instead of
  `filter_flag`, `filter_flag_1`, …; a real collision still falls back to a
  numeric suffix.
- The source table is mutated: later ops on the dataset see the flag column.
  The raw response dict carries the name under `flag_column`; the public
  `ctx.filter(...)` unwraps to a bare DataFrame, so read it via
  `FilteringWrapper` when you need the name programmatically.

## Errors

- Unparseable strings raise `ParseError` (e.g. `ctx.filter("salary ??? 5")`
  raises `ParseError: Expected comparison operator, got '???'`).
- Backend and validation failures surface as the canonical error dict
  (`is_error=True` with `error_message`).
- Unsupported backends raise `NotImplementedError`.

## Caching

`filter` applies `@record_call`, so calls are recorded in the cache
registry like every other orchestrated operation. With `deep_cache=True` the
result is a source **ref**:

```text
cache_kind  = 'ref'
ref_schema  = 'memframe_upload'
ref_table   = '<data_id>__base'
order_schema= 'memframe_cache'
order_table = '<data_id>__base__op_<n>'
order_kind  = 'filter'
```

The order table contains only `position` and `_mf_idx`; replay joins the
source against it and orders by `position`. No filtered copy is stored.
`create_flag=True` adds the flag column name to `generated_cols`, marks the
entry as mutating, and bumps the cache epoch so older entries are invalidated.

With `deep_cache=False` the selection table is dropped after the result is
materialized (streaming iterators require `deep_cache=True`).

## API Reference

::: memframe.wrappers.analytix.filter.FilteringWrapper
    options:
      show_root_heading: true
      show_root_full_path: true
      members:
        - afilter
        - filter
