# Comparison

Source: `src/memframe/wrappers/analytix/comparison.py`

`ComparisonWrapper` is the public comparison interface exposed through a
`ContextManager`. It compares two columns element-wise with a SQL comparison
operator and writes the boolean result into a new column (`cmp_<col1>_<op>_<col2>`)
on a freshly created transient table — the source table is never mutated.

Users normally call comparison methods directly on a dataset context returned by
an upload operation:

```python
dataset = mf.upload_df(frame)
result = dataset.compare("salary >= bonus")
```

The lower-level files are implementation details:

- `src/memframe/core/analytix/comparison.py` builds and executes backend-specific SQL
  (clone → add column → update on DuckDB/PostgreSQL, single CTAS on ClickHouse).
- `src/memframe/core/orchestrator/analytix/comparison.py` resolves the active dataset
  context, detects the column family, and passes persistence metadata.
- `src/memframe/wrappers/analytix/comparison.py` exposes synchronous and asynchronous
  public methods.

## Public API

| Synchronous | Asynchronous | Purpose |
| --- | --- | --- |
| `compare(expression)` | `await acompare(expression)` | Predicate-based comparison from a string expression |
| `compare(col1, col2, operator)` | `await acompare(col1, col2, operator)` | Query-based comparison from explicit operands |

Public methods return the resulting DataFrame directly. Both columns must belong
to the same type family (numeric, categorical, or datetime), otherwise a
datatype-mismatch error is returned.

Supported operators: `==`, `!=`, `>`, `<`, `>=`, `<=`.

## Predicate-Based Comparison

Pass a single string expression — a predicate of the form `<column> <operator>
<column>`:

```python
dataset = mf.upload_df(frame)

sample = dataset.compare("salary >= bonus")
```

```python
dataset = await mf.aupload_df(frame)

sample = await dataset.acompare("status == expected_status")
```

The expression is parsed by `src/memframe/utils/str_compare_parser.py`.
Column names containing spaces can be quoted (`"\"total income\" >= bonus"`).

## Query-Based Comparison

Pass the two columns and the operator as three separate arguments:

```python
dataset = mf.upload_df(frame)

sample = dataset.compare("salary", "bonus", ">=")
```

```python
dataset = await mf.aupload_df(frame)

sample = await dataset.acompare("hired_at", "review_at", "<")
```

Both forms route identically; pick the expression form for compact,
readable predicates and the three-argument form when the operands or the
operator are computed programmatically.

## Type Routing

The orchestrator samples both columns and routes to the matching core path:

- **Numeric** (`INTEGER`, `FLOAT`, …) — direct SQL comparison, e.g.
  `dataset.compare("salary > bonus")`.
- **Categorical** (`VARCHAR`, `TEXT`, …) — direct SQL comparison, e.g.
  `dataset.compare("status == expected_status")`.
- **Datetime** (`TIMESTAMP`, `DATE`, …) — both sides are cast to a native
  timestamp type first (`TIMESTAMPTZ` on DuckDB/PostgreSQL,
  `Nullable(DateTime64(6))` on ClickHouse), so text-stored timestamps compare
  correctly, e.g. `dataset.compare("hired_at < review_at")`.

## Result Column

The result column is named `cmp_<col1>_<op>_<col2>`, where `<op>` is
`eq`, `ne`, `gt`, `lt`, `ge`, or `le`:

| Expression | Result column |
| --- | --- |
| `A == B` | `cmp_A_eq_B` |
| `A != B` | `cmp_A_ne_B` |
| `A > B` | `cmp_A_gt_B` |
| `A < B` | `cmp_A_lt_B` |
| `A >= B` | `cmp_A_ge_B` |
| `A <= B` | `cmp_A_le_B` |

The returned DataFrame contains the two compared columns plus the result column:

```python
sample = dataset.compare("A >= B")
print(sample)
#    A  B  cmp_A_ge_B
# 0  1  2       False
# 1  5  4        True
```

## Errors

Comparison methods return the canonical error envelope (`is_error=True` with
`error_message`) for:

- Unparseable expressions (`compare("not a comparison")`).
- Invalid operators (`compare("A", "B", "===")` — must be one of `==`, `!=`,
  `>`, `<`, `>=`, `<=`).
- Datatype mismatches (`compare("salary", "status", "==")` — both columns must
  share a type family).
- Unsupported backends raise `NotImplementedError`.

## Caching

`compare` applies `@record_call`, so repeat calls are recorded in the
transient registry like every other orchestrated operation. With
`deep_cache=True` the result table is persisted and replayed on a repeat call;
otherwise the call is signature-logged only.

## API Reference

::: memframe.wrappers.analytix.comparison.ComparisonWrapper
    options:
      show_root_heading: true
      show_root_full_path: true
      members:
        - acompare
        - compare
