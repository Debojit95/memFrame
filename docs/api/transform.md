# Transform

Source: `src/memframe/wrappers/analytix/transform.py`

`TransformWrapper` is the public feature-engineering interface exposed
through a `ContextManager`. It provides sklearn/pandas-style numeric scaling,
binning, polynomial expansion, interaction terms, categorical encoding,
quantile/power transforms, and cyclical datetime features, compiled to
backend-native SQL across DuckDB, PostgreSQL, and ClickHouse. Every call
creates a new transient table (`<table>__op_<n>` via `deep_cache`) and
returns a sample DataFrame; the source table is never mutated in place.

Users normally call transform directly on a dataset context returned by an
upload operation:

```python
dataset = mf.upload_df(frame)
scaled = dataset.scale(column="age")
robust = dataset.robust_scale(column="salary", quantile_range=(25, 75))
encoded = dataset.onehot(column="city", max_categories=10)
cyclical = dataset.cyclical_encode(column="event_time", features=["month", "dow"])
```

```python
dataset = await mf.aupload_df(frame)
scaled = await dataset.ascale(column="age")
robust = await dataset.arobust_scale(column="salary")
logd = await dataset.alog_transform(column="price", base="e", epsilon=1)
encoded = await dataset.aonehot(column="city", max_categories=10)
```

`transform` is top-level (`dataset.scale`, not `dataset.dt.*`) — `dt` is reserved for `DateTimeWrapper` (`floor/ceil/round` would otherwise collide with `ArithmeticWrapper`).

The lower-level files are implementation details:

- `src/memframe/core/analytix/transform/base.py` holds the DuckDB-flavoured
  shared engine (helpers `_exec`/`_fetch`/`_fetch_sample`/`_qualified_table`/`_generate_cleaned_column_name`/`_add_new_column`/`_count_non_null`/`_ch_create_table_as` + `_success`/`_error`).
- `src/memframe/core/analytix/transform/postgres.py` / `clickhouse.py` / `duckdb.py`
  override where SQL differs (`PERCENTILE_CONT` vs `quantile` vs `quantiles`, `CAST` for PG ints, CTAS `MergeTree() ORDER BY tuple()` for CH).
- `src/memframe/core/analytix/transform/factory.py` dispatches `make_transform_ops(adapter)` on `isinstance`.
- `src/memframe/core/orchestrator/analytix/transform.py` resolves the
  active dataset context (`_get_active_context`) and passes persistence metadata (`backend`/`data_id` via `record_call(deep_cache=True)`).
- `src/memframe/wrappers/analytix/transform.py` exposes synchronous (`scale`) and asynchronous (`ascale`) public methods via `async_to_sync`.

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
| `quantile_transform(column, output="uniform")` | `await aquantile_transform(...)` | Quantile to uniform `[0,1]` via rank |
| `power_transform(column, method="yeo-johnson")` | `await apower_transform(...)` | Power `sign*pow(|x|,0.5)` / Box-Cox |
| `ordinal_encode(column)` | `await aordinal_encode(...)` | Ordinal alphabetical `0..n-1` |

Public methods return the resulting DataFrame directly (sample, 10 rows by default via `_fetch_sample`). Invalid operations raise `OperationError` (envelope `is_error True`, `result None`, `involved_cols`/`generated_cols` populated).

## Usage Overview

Choosing a transform (like `datetime`’s `## Usage Overview`):

```python
# Numeric: pick a scaler by data shape
scaled = dataset.scale(column="age")                 # bell-shaped, mean 0, std 1
robust = dataset.robust_scale(column="salary")       # heavy outliers → median/IQR, not mean/std
maxabs = dataset.maxabs_scale(column="sparse_feat")  # sparse, already centered → [-1,1]
normed = dataset.normalize(column="value")           # single-col sign; multi-col L2 deferred
logd = dataset.log_transform(column="price", epsilon=1)  # right-skewed → log1p

# Binning vs quantile
binned = dataset.bin(column="age", bins=5, strategy="uniform")  # equal-width
qcut = dataset.qcut(column="score", bins=4)                      # equal-frequency
qt = dataset.quantile_transform(column="score", output="uniform") # rank → [0,1]
pt = dataset.power_transform(column="score", method="yeo-johnson") # gaussianize

# Categorical: cardinality matters
dummies = dataset.onehot(column="city", max_categories=10)  # low-cardinality → k binary cols
labelled = dataset.label_encode(column="city")               # high-cardinality, frequency-ranked
ordinal = dataset.ordinal_encode(column="size")              # ordered categories (S<M<L) → alphabetical
freq = dataset.frequency_encode(column="city")               # count/n
target = dataset.target_encode(column="city", target_column="price")  # high-cardinality + target
binary = dataset.binarize(column="city", value="London")     # one-vs-all

# Datetime cyclical
cyclical = dataset.cyclical_encode(column="event_time", features=["month", "dow", "hour"])
```

```python
# Async equivalents
scaled = await dataset.ascale(column="age")
robust = await dataset.arobust_scale(column="salary", quantile_range=(10, 90))
logd = await dataset.alog_transform(column="price", base="10", epsilon=0)
```

Most transform methods materialize a new transient table (`<table>__op_<n>`) internally. The public return is the sample DataFrame; `new_table`, `involved_cols`, `generated_cols` remain internal to cache/AI.

## Numeric Scaling

All numeric scalers create a `DOUBLE PRECISION` column `transformed_<col>_<suffix>` and populate via `UPDATE` (PG/DuckDB) or `SELECT … CASE` (CH CTAS). Nulls stay `NULL`.

### `scale`

Z-score standard scaling, like `sklearn.preprocessing.StandardScaler`: `z = (x - mean) / std` (`STDDEV_POP`, `AVG`).

```python
result = dataset.scale(column="age")
result = dataset.scale(column="salary")
```

```python
result = await dataset.ascale(column="age")
result = await dataset.ascale(column="salary")
```

Parameters:

| Parameter | Type | Description |
| --- | --- | --- |
| `column` | `str` | Numeric column to scale. Must exist, type `INTEGER`/`FLOAT`/`DOUBLE` (text → use `clean.to_numeric` first). |

Generated: `transformed_<column>_standardized` (`DOUBLE PRECISION`). Example `age=[20,30,40]` → `mean=30,std=8.16` → `[-1.22,0,1.22]`.

Backend: PG/DuckDB `WITH stats AS (SELECT AVG(col) AS mean, STDDEV_POP(col) AS std FROM qualified WHERE col IS NOT NULL) UPDATE qualified SET new = CASE WHEN col IS NULL THEN NULL ELSE (col - stats.mean)/NULLIF(stats.std,0) END FROM stats`; CH `CROSS JOIN (SELECT avg(col) AS mean, stddevPop(col) AS std FROM source WHERE col IS NOT NULL)`.

Errors: unsupported backend → `NotImplementedError` wrapped as `is_error True`; missing column → `KeyError` → `is_error` envelope.

### `minmax`

Min-max to `[0,1]`, like `sklearn.preprocessing.MinMaxScaler`: `(x - min)/(max - min)`.

```python
result = dataset.minmax(column="age")
result = dataset.minmax(column="score")
```

```python
result = await dataset.aminmax(column="score")
```

Parameters:

| Parameter | Type | Description |
| --- | --- | --- |
| `column` | `str` | Numeric column. |

Generated `transformed_<column>_minmax`. `min==max` → `NULLIF(...,0)` → `NULL` (avoid div-zero).

Backend: PG needs `CAST(col AS DOUBLE PRECISION)` and `CAST((max-min) AS DOUBLE PRECISION)` to avoid integer division (`10/40=0` else). DuckDB and CH already promote.

### `robust_scale`

Robust scaling `(x - median)/IQR`, like `sklearn.preprocessing.RobServer` with `quantile_range=(25,75)` (IQR). Robust to outliers.

```python
result = dataset.robust_scale(column="salary", quantile_range=(25, 75))
result = dataset.robust_scale(column="price", quantile_range=(10, 90))
```

```python
result = await dataset.arobust_scale(column="salary", quantile_range=(25, 75))
```

Parameters:

| Parameter | Type | Description |
| --- | --- | --- |
| `column` | `str` | Numeric column. |
| `quantile_range` | `tuple[int,int]` | Low/high percentiles, default `(25,75)`. Must satisfy `0<low<high<100`. |

Generated `transformed_<column>_robust`. `IQR=0` → `NULL`.

Backend: PG `PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY col)` + `PERCENTILE_CONT(0.25/0.75)`; DuckDB `quantile(col, [0.25,0.5,0.75]) AS qs` then `qs[2]` median, `qs[3]-qs[1]` IQR (list 1-indexed); CH `quantiles(0.25,0.5,0.75)(col) AS qs` → `qs[2]`/`qs[3]-qs[1]` via nested `SELECT`.

### `maxabs_scale`

MaxAbs `x / max(|x|)` → `[-1,1]`, like `sklearn.preprocessing.MaxAbsScaler` (sparse-safe).

```python
result = dataset.maxabs_scale(column="value")
result = dataset.maxabs_scale(column="sparse_feat")
```

```python
result = await dataset.amaxabs_scale(column="value")
```

Parameters:

| Parameter | Type | Description |
| --- | --- | --- |
| `column` | `str` | Numeric column. |

Generated `transformed_<column>_maxabs`. `max_abs=0` → `NULL`.

Backend: `MAX(ABS(CAST(col AS DOUBLE PRECISION)))` + `NULLIF`.

### `normalize`

Single-column sign `x / |x|` (`-1`, `0`, `1`), placeholder for `sklearn.preprocessing.Normalizer` row-wise `x/||x||`. Multi-column `L1`/`L2` across a feature matrix is deferred.

```python
result = dataset.normalize(column="value", norm="l2")
result = dataset.normalize(column="value", norm="l1")
```

```python
result = await dataset.anormalize(column="value", norm="l2")
```

Parameters:

| Parameter | Type | Description |
| --- | --- | --- |
| `column` | `str` | Numeric column. |
| `norm` | `str` | `l1` or `l2` (currently single-col `sign`, `norm` ignored but validated). |

Generated `transformed_<column>_normalized` (`-1`/`0`/`1`, `NULL` preserved). Future: `norm` will switch to per-row vector norm when a `columns=[...]` API lands.

## Binning

### `bin`

Binning (`pd.cut` / `sklearn.preprocessing.KBinsDiscretizer` `strategy="uniform"`/`"quantile"`):

```python
result = dataset.bin(column="age", bins=5, strategy="uniform")
result = dataset.bin(column="score", bins=4, strategy="quantile")
```

```python
result = await dataset.abin(column="age", bins=3, strategy="quantile")
```

Parameters:

| Parameter | Type | Description |
| --- | --- | --- |
| `column` | `str` | Numeric column. |
| `bins` | `int` | `>=2`, default `5`. |
| `strategy` | `str` | `uniform` (equal-width `MIN`/`MAX`/`bin_width`) or `quantile` (equal-frequency via `PERCENTILE_CONT`/`quantile`/`quantiles` + `CASE`/`multiIf`). |

Uniform: `LEAST(bins, FLOOR((x - min)/NULLIF(bin_width,0))+1)` → `bin_1`…`bin_N`. Quantile: pre-computed cut points `quantiles[i] ≤ x < quantiles[i+1]` (last bin `≤`). `NULL` → `missing`. Generated `transformed_<column>_binned` (`TEXT`).

Backend: PG `percentile_cont(array[...]) WITHIN GROUP`, DuckDB `quantile(col, [...])`, CH `quantiles(...)` array + `multiIf`.

Errors: unknown `strategy` → `is_error True` `"Unknown binning strategy: ..."`, `bins<2` not validated but yields single bin.

### `cut`, `qcut`, `get_dummies`

Aliases:

```python
result = dataset.cut(column="age", bins=3)          # → bin uniform
result = dataset.qcut(column="age", bins=4)         # → bin quantile
result = dataset.get_dummies(column="city")         # → onehot
```

```python
result = await dataset.acut(column="age", bins=3)
result = await dataset.aqcut(column="age", bins=4)
result = await dataset.aget_dummies(column="city", max_categories=5)
```

Parameters same as `bin`/`onehot`. `qcut` is `bin(..., strategy="quantile")`.

## Polynomial & Interaction

### `poly`

Polynomial expansion `pow2`…`pow{degree}`, like `sklearn.preprocessing.PolynomialFeatures(degree, include_bias=False)` single-feature case:

```python
result = dataset.poly(column="age", degree=3)  # → transformed_age_pow2, pow3
result = dataset.poly(column="x", degree=2)
```

```python
result = await dataset.apoly(column="age", degree=2)
```

Parameters:

| Parameter | Type | Description |
| --- | --- | --- |
| `column` | `str` | Numeric column. |
| `degree` | `int` | `>=2`, default `2`. |

Generated `transformed_<column>_pow{d}` for `d=2..degree` (`DOUBLE PRECISION`, `POWER(col,d)`, `NULL` preserved). `degree=2` → one col, `degree=5` → 4 cols.

### `interact`

Product interaction `col1 * col2`:

```python
result = dataset.interact(column1="age", column2="income")
result = dataset.interact(column1="price", column2="quantity")
```

```python
result = await dataset.ainteract(column1="age", column2="income")
```

Parameters:

| Parameter | Type | Description |
| --- | --- | --- |
| `column1` | `str` | Numeric column. |
| `column2` | `str` | Numeric column. |

Generated `transformed_<col1>_<col2>_interaction` (`DOUBLE PRECISION`, `NULL` if either `NULL`). `Rows with both non-null: min(cnt1,cnt2)` in message.

### `log_transform`

Log with `epsilon` guard, like `sklearn.preprocessing.FunctionTransformer(np.log1p)` / `PowerTransformer` log branch:

```python
result = dataset.log_transform(column="price", base="e", epsilon=1)   # ln(x+1)
result = dataset.log_transform(column="price", base="10", epsilon=0)  # log10(x)
result = dataset.log_transform(column="count", base="e", epsilon=0.5)
```

```python
result = await dataset.alog_transform(column="price", base="e", epsilon=0)
```

Parameters:

| Parameter | Type | Description |
| --- | --- | --- |
| `column` | `str` | Numeric column. |
| `base` | `str` | `e` (`LN`) or `10`/`ten` (`LOG`/`log10`). |
| `epsilon` | `float` | Added before log, default `0`. `x+eps` must be `>0`. |

`x IS NULL` or `x+eps <=0` → `NULL` (not error). Generated `transformed_<column>_log`.

Backend: PG/DuckDB `LN(CAST(col AS DOUBLE)+eps)` vs `LOG(...)`, CH `log(col+eps)` vs `log10(col+eps)`.

## Quantile & Power

### `quantile_transform`

Rank to uniform `[0,1]`, like `sklearn.preprocessing.QuantileTransformer(output_distribution="uniform")` (Gaussian `normal` via `erfinv` deferred, currently same rank):

```python
result = dataset.quantile_transform(column="price", output="uniform")
result = dataset.quantile_transform(column="score", output="normal")  # currently uniform
```

```python
result = await dataset.aquantile_transform(column="price", output="uniform")
```

Parameters:

| Parameter | Type | Description |
| --- | --- | --- |
| `column` | `str` | Numeric column. |
| `output` | `str` | `uniform` (or `normal` — currently same `rank/(n-1)`). |

Formula `(rank - 1)/(n - 1)` (`ROW_NUMBER() OVER (ORDER BY col)-1`, CH `rank() OVER (ORDER BY col)`). `n=1` → `NULLIF(n-1,0)` → `NULL`. Generated `transformed_<column>_quantile`.

### `power_transform`

Power `sign*pow(|x|,0.5)` (`yeo-johnson` λ=0.5) / Box-Cox `(pow(x,0.5)-1)/0.5` for `x>0` else `NULL`, simplified `sklearn.preprocessing.PowerTransformer` (MLE for `λ` deferred):

```python
result = dataset.power_transform(column="price", method="yeo-johnson")
result = dataset.power_transform(column="price", method="box-cox")  # requires x>0
```

```python
result = await dataset.apower_transform(column="price", method="yeo-johnson")
```

Parameters:

| Parameter | Type | Description |
| --- | --- | --- |
| `column` | `str` | Numeric column. |
| `method` | `str` | `yeo-johnson` (any `x`) or `box-cox` (`x>0` else `NULL`). |

Generated `transformed_<column>_power`.

## Categorical Encoding

### `onehot`

One-hot `0/1` per top category, like `pd.get_dummies` / `sklearn.preprocessing.OneHotEncoder(handle_unknown="ignore", max_categories=10)`:

```python
result = dataset.onehot(column="city", max_categories=10)
result = dataset.get_dummies(column="city", max_categories=5)
result = dataset.onehot(column="product", max_categories=3)  # top-3 only
```

```python
result = await dataset.aonehot(column="city", max_categories=10)
result = await dataset.aget_dummies(column="city")
```

Parameters:

| Parameter | Type | Description |
| --- | --- | --- |
| `column` | `str` | Categorical column. |
| `max_categories` | `int` | `>=1`, default `10`. Top-N by `COUNT(*) DESC`. |

Generated `transformed_<column>_<sanitized_value>` (`INTEGER` `0/1`, `sanitized` replaces `[^a-zA-Z0-9_]` with `_`, collisions merged via `IN` list). `NULL` → `0` in all cols.

### `label_encode`

Frequency-ranked `0..n-1`, like `sklearn.preprocessing.LabelEncoder` ordered by `COUNT(*) DESC` (not alphabetical):

```python
result = dataset.label_encode(column="city")
result = dataset.label_encode(column="product")
```

```python
result = await dataset.alabel_encode(column="city")
```

Parameters:

| Parameter | Type | Description |
| --- | --- | --- |
| `column` | `str` | Categorical. |

Generated `transformed_<column>_label` (`INTEGER`, `ROW_NUMBER() OVER (ORDER BY COUNT(*) DESC)-1`). Ties arbitrary.

### `ordinal_encode`

Alphabetical `0..n-1`, like `sklearn.preprocessing.OrdinalEncoder`:

```python
result = dataset.ordinal_encode(column="size")  # S<M<L → 0,1,2 if sorted
result = dataset.ordinal_encode(column="city")
```

```python
result = await dataset.aordinal_encode(column="city")
```

Parameters:

| Parameter | Type | Description |
| --- | --- | --- |
| `column` | `str` | Categorical. |

Generated `transformed_<column>_ordinal` (`ROW_NUMBER() OVER (ORDER BY col)-1`). No `COUNT` ordering.

### `frequency_encode`

`COUNT(*)/n` per category, like `category_encoders.CountEncoder(normalize=True)`:

```python
result = dataset.frequency_encode(column="city")
```

```python
result = await dataset.afrequency_encode(column="city")
```

Parameters:

| Parameter | Type | Description |
| --- | --- | --- |
| `column` | `str` | Categorical. |

Generated `transformed_<column>_freq` (`DOUBLE PRECISION`, `CAST(COUNT(*) AS Float64)/ (SELECT COUNT(*) WHERE col IS NOT NULL)`).

### `target_encode`

Smoothed target mean `(cat_mean*cnt + global_mean*10)/(cnt+10)`, like `sklearn.preprocessing.TargetEncoder` / `category_encoders.TargetEncoder(smoothing=10)`:

```python
result = dataset.target_encode(column="city", target_column="price")
result = dataset.target_encode(column="product", target_column="sales")
```

Parameters:

| Parameter | Type | Description |
| --- | --- | --- |
| `column` | `str` | Categorical. |
| `target_column` | `str` | Numeric target (must be `NOT NULL` for stats). |

Generated `transformed_<column>_target` (`DOUBLE PRECISION`, `NULL` if `col IS NULL`). Smoothing `10` is fixed (future: expose `smoothing` param).

### `binarize`

`0/1` via `value` or `condition`, like `sklearn.preprocessing.Binarizer(threshold)`:

```python
result = dataset.binarize(column="city", value="London")   # city='London' →1
result = dataset.binarize(column="age", condition="> 30")  # age>30 →1
result = dataset.binarize(column="flag")                   # NOT NULL →1
```

```python
result = await dataset.abinarize(column="city", value="London")
result = await dataset.abinarize(column="age", condition="> 30")
```

Parameters:

| Parameter | Type | Description |
| --- | --- | --- |
| `column` | `str` | Column. |
| `value` | `any` or `None` | `col = value` →1 else 0 (sanitized `is_<value>` suffix). |
| `condition` | `str` or `None` | SQL suffix `> 30`, `<= 100`, `IS NOT NULL` → `col <condition>` →1. |

Generated `transformed_<column>_is_<sanitized_value>` / `transformed_<column>_<sanitized_cond>` / `transformed_<column>_binary` (`INTEGER`).

## Datetime

### `cyclical_encode`

Sin/cos for `month`/`dow`/`hour`, for tree/linear models that need circular continuity:

```python
result = dataset.cyclical_encode(column="event_time", features=["month", "dow"])
result = dataset.cyclical_encode(column="event_time", features=["hour"])
result = dataset.cyclical_encode(column="event_time", features=["month", "dow", "hour"])
```

```python
result = await dataset.acyclical_encode(column="event_time", features=["month", "dow"])
```

Parameters:

| Parameter | Type | Description |
| --- | --- | --- |
| `column` | `str` | Datetime (`TIMESTAMP`/`DATE`/`TEXT` cast to `DateTime64(6)` on CH). |
| `features` | `list[str]` | `month` (12), `dow` (7, `toDayOfWeek`), `hour` (24). Unknown features skipped. |

Generated `transformed_<column>_<feat>_sin` + `cos` (`DOUBLE PRECISION`, `sin(2*pi*extract/period)`, `NULL` if `col IS NULL`). `month` via `EXTRACT(MONTH)` vs `toMonth(CAST(col AS Nullable(DateTime64(6))))`, etc. Requires `features` non-empty or returns `is_error True`.

## Return Values and Errors

Public transform methods return the resulting DataFrame directly (10-row sample via `LIMIT 10`). `OperationError` is raised for the `is_error` envelope (`error_message`, `involved_cols`, `generated_cols`, `result None`):

* Unknown `strategy` for `bin` → `"Unknown binning strategy: {strategy}"` (`is_error True`, `involved_cols=[col]`).
* `quantiles` failure (`NULL` result) → `"Failed to compute quantiles for numeric_bin."` / `"Empty quantile values returned"`.
* `cyclical_encode` without `features` → `"datetime_cyclical_encode requires 'features' parameter"`.
* `box-cox` on `x+eps <=0` → row `NULL` (not error, filtered per-row).
* Unsupported backend → `NotImplementedError` wrapped as `is_error True` (`"Unsupported database backend for transform operation: ..."`) — DuckDB/Postgres/ClickHouse only.
* Missing column → `KeyError` → `is_error`.

Envelope always keeps `result` key (`None` on error) so `is_operation_response`/`unwrap_response` treat errors canonically.

## Generated Tables

Every transform operation is non-destructive to the source upload table. Each operation:

1. Clones the source table into a new transient table (`<table>__op_<n>`, `deep_cache` via `transient_registry` `MAX(opidx)+1` or timestamp fallback), or `CREATE TABLE … AS SELECT *` for ClickHouse `MergeTree()`.
2. Adds result column(s) `transformed_*` (`ADD COLUMN IF NOT EXISTS <col> DOUBLE PRECISION/INTEGER/TEXT` on PG/DuckDB, CTAS `MergeTree() ORDER BY tuple()` on CH).
3. Populates via `UPDATE … FROM stats` (PG/DuckDB) or `SELECT … CASE` (CH) — single `UPDATE` for scalers, `CASE` per `bin` quantile, `POWER` loop for `poly`, `LEFT JOIN` + `_ch_join_key` alias for `label`/`frequency`/`target`/`ordinal` on CH.
4. Returns sample DataFrame (`_fetch_sample` 10 rows, `involved_cols` + `generated_cols`); `new_table` name is in the envelope (`result` unwrapped hides it, use `CacheManager` or `?` to inspect).

## Backend Behavior

Transform supports DuckDB, PostgreSQL, and ClickHouse:

* Identifiers sanitized via `SQLIdentifierSanitizer` and quoted (`"` vs `` ` ``).
* Aggregates: `AVG`/`STDDEV_POP` vs `stddevPop`, `MIN`/`MAX` vs `min`/`max`, `POWER` vs `pow`, `LN`/`LOG` vs `log`/`log10`, `PERCENTILE_CONT` (PG) vs `quantile([...])` (DuckDB) vs `quantiles(...)` array (CH), `ROW_NUMBER()` vs `rank()`, `toMonth`/`toDayOfWeek`/`toHour` (CH casts to `Nullable(DateTime64(6))` for `String` timestamps).
* PG integer `min`/`max` uses `CAST(col AS DOUBLE PRECISION)` to avoid integer division (same for `bin` uniform `bin_width`); CH already promotes.
* CH `LEFT JOIN` collisions (`category_col` in `source.*` + `ranked` subquery with same `col`) use `_ch_join_key` alias (`SELECT col AS _ch_join_key … ON source.col = ranked._ch_join_key`) to avoid `source.category_col` prefix (fixed for `label`/`frequency`/`target`/`ordinal` — `onehot` avoids join collision via `CASE` per value).
* CH uses `ENGINE=MergeTree() ORDER BY tuple()` for all new tables; `UPDATE` on PG/DuckDB is `UPDATE qualified SET … FROM stats`, on CH is `ALTER TABLE … UPDATE … WHERE 1 SETTINGS mutations_sync=1` only for cleaning (transform uses CTAS, so `mutations_sync` not needed).
* CH `quantiles` failure returns `is_error True` (same as PG/DuckDB `percentile_cont`).

## Errors

Transform methods raise `OperationError` for validation or backend failures. Check `error_message`, `involved_cols`, `generated_cols`:

* `bin` unknown `strategy` → `is_error True`.
* `quantile_transform` with `n=1` → `NULLIF(n-1,0)` → `NULL` rows (not error).
* `power_transform` `box-cox` on `x<=0` → `NULL` per row.
* `cyclical_encode` empty `features` → `is_error True`.
* Missing column / unsupported backend → `KeyError`/`NotImplementedError` → `is_error`.

## API Reference

::: memframe.wrappers.analytix.transform.TransformWrapper
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
        - aquantile_transform
        - quantile_transform
        - aquantile
        - quantile
        - apower_transform
        - power_transform
        - apower
        - power
        - aordinal_encode
        - ordinal_encode
        - aordinal
        - ordinal
