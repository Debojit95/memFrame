# memFrame

The `memFrame` API is the pandas-style surface you call on a dataset context
returned by an upload (or by `register_tables` / `set_active`). Every method
compiles to backend-native SQL and runs in-engine across DuckDB, PostgreSQL,
and ClickHouse — your data never leaves the database — and each call writes a
new transient table (`<table>__op_<n>`) without mutating the source upload.

Every operation has a synchronous and an asynchronous twin: drop the `a`
prefix for sync (`head`) and use `await dataset.ahead(...)` for async. Methods
are dispatched lazily through the `ContextManager`, so you call them directly
on the dataset context:

```python
dataset = mf.upload_df(frame)

preview = dataset.head()
filled = dataset.fillna("score", method="mean")
totals = dataset.groupby("region").sum("revenue")
rolling = dataset.rolling(column="sales", window=7, func="mean", order_by="date")
```

```python
dataset = await mf.aupload_df(frame)

preview = await dataset.ahead()
filled = await dataset.afillna("score", method="mean")
totals = await dataset.agroupby("region").sum("revenue")
rolling = await dataset.arolling(column="sales", window=7, func="mean", order_by="date")
```

## Domains

| Domain | What it covers |
| --- | --- |
| [Inspect](inspect.md) | Shape, dtypes, head/tail, value counts, describe-style summaries, sampling. |
| [Clean](cleaning.md) | Null filling (global, group-wise, forward/backward), outlier handling, type conversion, duplicate removal. |
| [Arithmetic](arithmetic.md) | Element-wise arithmetic, rounding, clipping, percentage change, column-to-column operations. |
| [Cumulative](cumulative.md) | Running sum/product/min/max/mean/count/std/var over an ordered column. |
| [Window](window.md) | Rolling, expanding, and exponentially-weighted (EWM) operations, plus a fluent `on(column)` builder. |
| [Selection](selection.md) | Column/row selection, `loc`/`iloc`/`at`, boolean and `where`-style filtering. |
| [Stats](stats.md) | Grouped aggregations, correlation, skew/kurtosis, frequency and cross-tabulation. |
| [Datetime](datetime.md) | Datetime part extraction, flooring/rounding, timezone conversion, date arithmetic. |
| [Sorting](sorting.md) | `sort_values` with multi-column and mixed ascending/descending order. |
| [Reshape](reshape.md) | `explode`, `melt`, `pivot`, `pivot_table`, `crosstab`, `transpose`, rank. |
| [Merge](merging.md) | `merge`/`join`/`concat` across dataset contexts on shared keys. |
| [Transform](transform.md) | Feature engineering: scaling, binning, polynomial/interaction, encodings, cyclical features. |

For charts, see the [Plots](bar.md) pages. For connection, ingestion, and
dataset management, see the [DB Manager](connector.md) pages.

## Return values

On a dataset context, public methods return the resulting **pandas DataFrame**
directly. Calling the underlying wrapper classes (or a fluent builder's terminal
method) returns the raw operation envelope instead — `{is_error, result,
new_table, ...}`. Invalid operations surface as `OperationError` once the
envelope is unwrapped.
