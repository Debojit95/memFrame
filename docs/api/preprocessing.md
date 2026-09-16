# Transform

Source: `src/memframe/wrappers/analytix/preprocessing.py`

`PreprocessingWrapper` (Transform — `transform` in `sklearn`/`pandas` sense) is the public feature-engineering interface exposed
through a `ContextManager`. It provides sklearn/pandas-style numeric scaling,
binning, categorical encoding, and cyclical datetime features, compiled to
backend-native SQL across DuckDB, PostgreSQL, and ClickHouse.

Users normally call preprocessing directly on a dataset context returned by an
upload operation:

```python
dataset = mf.upload_df(frame)
scaled = dataset.scale(column="age")
```

```python
dataset = await mf.aupload_df(frame)
encoded = await dataset.aonehot(column="city", max_categories=10)
```

The lower-level files are implementation details:

- `src/memframe/core/analytix/preprocessing.py` builds and executes
  backend-specific SQL (single file, `isinstance` branches per backend).
- `src/memframe/core/orchestrator/analytix/preprocessing.py` resolves the
  active dataset context and passes persistence metadata (`deep_cache`).
- `src/memframe/wrappers/analytix/preprocessing.py` exposes synchronous and
  asynchronous public methods.

## Public API

Every operation has synchronous and asynchronous forms:

| Synchronous | Asynchronous | Purpose |
| --- | --- | --- |
| `scale(column)` | `await ascale(...)` | Z-score standard scaling |
| `minmax(column)` | `await aminmax(...)` | Min-max scaling to [0, 1] |
| `bin(column, bins=5, strategy="uniform")` | `await abin(...)` | Uniform or quantile binning |
| `poly(column, degree=2)` | `await apoly(...)` | Polynomial features up to degree |
| `interact(column1, column2)` | `await ainteract(...)` | Product interaction term |
| `onehot(column, max_categories=10)` | `await aonehot(...)` | One-hot encode top categories |
| `label_encode(column)` | `await alabel_encode(...)` | Frequency-ranked integer labels |
| `frequency_encode(column)` | `await afrequency_encode(...)` | Category frequency encoding |
| `target_encode(column, target_column)` | `await atarget_encode(...)` | Smoothed target-mean encoding |
| `binarize(column, value=None, condition=None)` | `await abinarize(...)` | 0/1 column from value or SQL condition |
| `cyclical_encode(column, features)` | `await acyclical_encode(...)` | Sin/cos features (`month`, `dow`, `hour`) |
| `get_dummies(column, max_categories=10)` | `await aget_dummies(...)` | Alias for `onehot` |
| `cut(column, bins=5, strategy="uniform")` | `await acut(...)` | Alias for `bin` |
| `qcut(column, bins=5)` | `await aqcut(...)` | Quantile binning |
| `robust_scale(column, quantile_range=(25,75))` | `await arobust_scale(...)` | Robust scaling `(x-median)/IQR` |
| `maxabs_scale(column)` | `await amaxabs_scale(...)` | MaxAbs scaling `x / max(|x|)` |
| `normalize(column, norm="l2")` | `await anormalize(...)` | Single-col sign (multi-col deferred) |
| `log_transform(column, base="e", epsilon=0)` | `await alog_transform(...)` | Log `ln`/`log10` with `x+eps>0` else NULL |

Public methods return the operation value directly (usually a sample
DataFrame). Invalid operations raise `OperationError`.

## Notes

- Generated columns are prefixed `transformed_` (e.g.
  `transformed_age_standardized`).
- PostgreSQL/DuckDB copy the source table then `UPDATE`; ClickHouse builds a
  new `MergeTree() ORDER BY tuple()` table via CTAS.
- Operations create transient tables under the active `data_id` when
  `deep_cache` is on, so they replay like other analytix ops.

## API Reference

::: memframe.wrappers.analytix.preprocessing.PreprocessingWrapper
    options:
      show_root_heading: true
      show_root_full_path: true
      members:
        - ascale
        - scale
        - aminmax
        - minmax
        - abin
        - bin
        - apoly
        - poly
        - ainteract
        - interact
        - aonehot
        - onehot
        - alabel_encode
        - label_encode
        - afrequency_encode
        - frequency_encode
        - atarget_encode
        - target_encode
        - abinarize
        - binarize
        - acyclical_encode
        - cyclical_encode
        - aget_dummies
        - get_dummies
        - acut
        - cut
        - aqcut
        - qcut
        - arobust_scale
        - robust_scale
        - arobust
        - robust
        - amaxabs_scale
        - maxabs_scale
        - amaxabs
        - maxabs
        - anormalize
        - normalize
        - alog_transform
        - log_transform
        - alog
        - log
