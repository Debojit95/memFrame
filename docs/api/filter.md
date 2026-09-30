# Filter

Source: `src/memframe/wrappers/analytix/filter.py`

`FilteringWrapper` is the public filtering interface exposed through a
`ContextManager`. It filters *rows*: a predicate tree is compiled to a SQL
`WHERE` clause and the matching rows are written to a freshly created
transient table — the source table is never mutated. Unlike comparison, which
adds a boolean column, filtering returns only the rows that satisfy the
predicate.

Predicate factories are imported directly from the core module:

```python
from memframe.core.analytix.filter import F

dataset = mf.upload_df(frame)
result = dataset.filter(F.num.gte("salary", 50000))
```

The lower-level files are implementation details:

- `src/memframe/core/analytix/filter/filter_I.py` defines the predicate tree
  (`Predicate`, `Num`/`Cat`/`Time` factories, `F` accessor) and compiles it to
  parameterized SQL per backend.
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
| `filter(predicate, columns="*", chunk_size=None)` | `await afilter(...)` | Keep rows satisfying a predicate object or string expression |

Public methods return the resulting DataFrame directly — or, when `chunk_size`
is given, a dict carrying an async `iterator` of DataFrames (see
[Chunked streaming](#chunked-streaming)).

## Predicate-Based Filtering

Build predicates with the `F` accessor and combine them with `&` (AND),
`|` (OR), `~` (NOT), and `^` (XOR):

```python
from memframe.core.analytix.filter import F

selected = dataset.filter(
    F.num.gte("salary", 50000) & F.cat.eq("status", "active")
)
```

### Numeric (`F.num`)

| Factory | Meaning |
| --- | --- |
| `gt(col, val)` / `gte(col, val)` | `col > val` / `col >= val` |
| `lt(col, val)` / `lte(col, val)` | `col < val` / `col <= val` |
| `eq(col, val)` / `ne(col, val)` | `col = val` / `col != val` |
| `between(col, low, high)` | `col >= low AND col <= high` |
| `outside(col, low, high)` | `col < low OR col > high` |
| `gt_col(col1, col2)` / `lt_col(col1, col2)` | Column-vs-column comparison |
| `delta_gt(col1, col2, threshold)` | `(col1 - col2) > threshold` |

```python
well_paid = dataset.filter(F.num.between("salary", 50000, 100000))
```

### Categorical (`F.cat`)

| Factory | Meaning |
| --- | --- |
| `eq(col, value)` / `ne(col, value)` | Exact match / mismatch |
| `in_(col, values)` / `not_in(col, values)` | `IN` / `NOT IN` membership |
| `contains(col, text)` | `%text%` (`starts_with` / `ends_with` narrow it) |
| `ilike(col, pattern)` | Case-insensitive `LIKE` |
| `regex(col, pattern)` / `not_regex(col, pattern)` | Regex match (case-sensitive by default) |
| `is_null(col)` / `not_null(col)` | `IS NULL` / `IS NOT NULL` |
| `length_gt(col, n)` / `length_lt` / `length_eq` | `LENGTH(col)` comparisons |
| `matches_any_pattern(col, patterns)` | OR of several `contains` |

```python
named = dataset.filter(F.cat.starts_with("name", "A"))
```

### Datetime (`F.time`)

| Factory | Meaning |
| --- | --- |
| `before(col, ts)` / `after(col, ts)` | `<` / `>` a timestamp |
| `on_or_before` / `on_or_after` / `eq` / `ne` | `<=` / `>=` / `=` / `!=` |
| `between(col, start, end)` | Timestamp range (`outside` inverts it) |
| `last_days(col, n)` (+ hours/minutes/seconds/months/years) | Relative to now, e.g. `col >= NOW() - INTERVAL 'n days'` |
| `year_eq(col, v)` (+ month/day/hour/minute/second variants) | `EXTRACT` field comparisons |
| `month_between` / `day_between` / `hour_between` | `EXTRACT` field ranges |
| `is_weekday(col)` / `is_weekend(col)` | Day-of-week sets (also `is_monday`, `is_friday`, `dow_eq`) |
| `is_business_hours(col, 9, 17)` | `EXTRACT(HOUR ...)` range |
| `before_tz(col, ts, tz)` (+ after/on_or_before/on_or_after/eq/between variants) | Timezone-aware comparisons |

```python
recent = dataset.filter(F.time.last_days("hired_at", 1000))
#    salary  status   hired_at
# 0   60000  active 2024-03-01
# 1   80000    left 2024-06-01
```

## Query-Based Filtering

Pass a single string expression instead of predicate objects. Comparisons use
`>`, `<`, `>=`, `<=`, `==`, `!=`; combine them with `&&`/`||` (or the `and`/`or`
keywords) and parentheses; quote string literals; write dates as
`YYYY-MM-DD` (or `YYYY-MM-DD HH:MM:SS`); extract datetime fields with
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

Pick predicates when the filter is built programmatically (factories compose
with `&`/`|`), strings for compact hand-written queries.

## Cross-Dtype Filtering

Unlike `compare()`, filtering imposes no same-type restriction — each predicate
is independent, so numeric, categorical, and datetime conditions mix freely in
one call:

```python
result = dataset.filter(
    F.num.gte("salary", 50000)
    & F.cat.eq("status", "active")
    & F.time.on_or_after("hired_at", "2024-01-01")
)
#    salary  status   hired_at
# 0   60000  active 2024-03-01
```

The equivalent single string:

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
lean = dataset.filter(F.num.gte("salary", 50000), columns=["name", "salary"])
```

## Chunked Streaming

Pass `chunk_size` to stream the result as an async iterator of DataFrames
instead of materializing one sample:

```python
response = dataset.filter(F.num.gte("salary", 50000), chunk_size=1000)
async for chunk in response["iterator"]:
    process(chunk)
```

The iterator reads the transient table lazily, so streaming requires
`deep_cache=True` on the `MemFrame` — in default signature-only mode the table
is dropped when the call returns and the first pull fails.

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
