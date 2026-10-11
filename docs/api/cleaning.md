# Clean

Source: `src/memframe/wrappers/analytix/cleaning.py`

`CleaningWrapper` (Clean — formerly Cleaning) is the public cleaning interface exposed through a
`ContextManager`. It provides pandas-like methods for filling missing values,
cleaning numeric/categorical/datetime columns, dropping missing or duplicate
data, and generating basic data-quality reports.

Users normally call cleaning methods directly on a dataset context returned by
an upload operation:

```python
dataset = mf.upload_df(frame)
result = dataset.fillna(column="salary", method="mean")
```

The same methods are also available from `dataset.clean`.

The lower-level files are implementation details:

- `src/core/analytix/cleaning.py` builds and executes backend-specific SQL.
- `src/core/orchestrator/analytix/cleaning.py` resolves the active dataset
  context, detects column type hints, and passes persistence metadata.
- `src/wrappers/analytix/cleaning.py` exposes synchronous and asynchronous
  public methods.

## Public API

Every cleaning operation has synchronous and asynchronous forms:

| Synchronous | Asynchronous | Purpose |
| --- | --- | --- |
| `fillna(column, value=None, method="mean", mapping=None, dtype=None)` | `await afillna(...)` | Fill missing values |
| `interpolate(column, method="linear", limit=None, limit_direction="forward", limit_area=None, order_by=None, dtype=None)` | `await ainterpolate(...)` | Fill missing values by linear interpolation |
| `replace(column=None, to_replace=None, value=None, regex=False)` | `await areplace(...)` | Replace values in one, several, or all columns |
| `clip(column, lower=None, upper=None)` | `await aclip(...)` | Null values outside numeric bounds |
| `drop_outliers(column, z_thresh=3.0)` | `await adrop_outliers(...)` | Null z-score outliers |
| `to_numeric(column)` | `await ato_numeric(...)` | Convert text-like values to numeric |
| `map_values(column, mapping)` | `await amap_values(...)` | Map categorical values |
| `filter_valid(column, valid_values)` | `await afilter_valid(...)` | Null values outside an allowed set |
| `compress_rare(column, min_count=10, other_label="other")` | `await acompress_rare(...)` | Group rare categories |
| `fix_dates(column)` | `await afix_dates(...)` | Null known invalid date strings |
| `clip_dates(column, min_dt=None, max_dt=None)` | `await aclip_dates(...)` | Null dates outside a range |
| `groupby_fillna(column, group_cols, value=None, method="mean", dtype=None)` | `await agroupby_fillna(...)` | Fill missing values by group |
| `dropna(axis=0, how="any", thresh=None)` | `await adropna(...)` | Drop rows or columns with missing values |
| `drop(columns=None, axis=0, index=None)` | `await adrop(...)` | Drop rows or columns |
| `isna()` | `await aisna()` | Boolean null mask |
| `notna()` | `await anotna()` | Boolean non-null mask |
| `drop_duplicates(subset=None, keep="first")` | `await adrop_duplicates(...)` | Remove duplicate rows |
| `duplicated(subset=None, keep="first")` | `await aduplicated(...)` | Flag duplicate rows with a boolean column |

Public methods return the operation value directly: usually a DataFrame, report
dictionary, mask, or `None`. Invalid operations raise `OperationError`.

## Usage Overview

```python
dataset = mf.upload_csv("data/employees.csv")

cleaned_sample = dataset.fillna(column="salary", method="mean")
```

```python
dataset = await mf.aupload_csv("data/employees.csv")

cleaned_sample = await dataset.afilter_valid(
    column="department",
    valid_values=["Sales", "Engineering", "Finance"],
)
```

Most cleaning methods materialize a new operation table internally. The public
return value is the sample DataFrame; table metadata remains available to the
cache and AI execution layers.

## Missing Values

### `fillna`

`fillna` fills missing values in one column. The orchestrator samples the
column, detects whether it is numeric, categorical, or datetime, and routes the
operation to the matching core implementation. Pass `dtype` to override
detection.

```python
result = dataset.fillna(
    column="salary",
    method="mean",
)
```

```python
result = await dataset.afillna(
    column="department",
    method="constant",
    value="Unknown",
    dtype="categorical",
)
```

Parameters:

| Parameter | Type | Description |
| --- | --- | --- |
| `column` | `str` | Column to clean. |
| `value` | any | Replacement used by `method="constant"`. |
| `method` | `str` | Fill strategy. Defaults to `"mean"`. |
| `mapping` | `dict` or `None` | Mapping used by `method="map"` for categorical columns. |
| `dtype` | `str` or `None` | Optional override: `"numeric"`, `"categorical"`, or `"datetime"`. |

Supported methods depend on detected or supplied dtype:

| Dtype | Methods |
| --- | --- |
| `numeric` | `mean`, `avg`, `average`, `median`, `mode`, `constant`, `std`, `var`,  `min`, `max`, `ffill`, `bfill` |
| `categorical` | `constant`, `mode`, `map`, `ffill`, `bfill` |
| `datetime` | `constant`, `min`, `max`, `mean`, `median`, `mode`, `now`, `ffill`, `bfill` |

Numeric `mean`/`median`/`std`/`var`/`min`/`max` methods are rejected for
categorical columns.

### `groupby_fillna`

`groupby_fillna` fills missing values using statistics or fill behavior within
groups.

```python
result = dataset.groupby_fillna(
    column="salary",
    group_cols=["department"],
    method="mean",
)
```

```python
result = await dataset.agroupby_fillna(
    column="category",
    group_cols=["department"],
    method="mode",
    dtype="categorical",
)
```

Parameters:

| Parameter | Type | Description |
| --- | --- | --- |
| `column` | `str` | Column to fill. |
| `group_cols` | `list[str]` | Columns used for grouping. Required. |
| `value` | any | Replacement value for `method="constant"` where supported. |
| `method` | `str` | Group-wise fill strategy. Defaults to `"mean"`. |
| `dtype` | `str` or `None` | Optional dtype override: `"numeric"`, `"categorical"`, or `"datetime"`. |

Supported group methods:

| Dtype | Methods |
| --- | --- |
| `numeric` | `mean`, `avg`, `average`, `median`, `mode`, `constant`, `std`, `var`, `min`, `max`, `ffill`, `bfill` |
| `categorical` | `mode`, `ffill`, `bfill` |
| `datetime` | `min`, `max`, `mean`, `median`, `mode`, `ffill`, `bfill` |

`group_cols` must be non-empty.

### `interpolate`

`interpolate` fills missing values in a numeric column by linear
interpolation along row order (`a + (b−a)·(p−p₀)/(p₁−p₀)` between the
surrounding valid values). Only `method="linear"` is supported.
Interior gaps interpolate; edge NaNs fill flat from
the nearest valid value according to `limit_direction` (`forward` fills
trailing, `backward` fills leading, `both` fills both; leading NaNs stay NaN
under the default `forward`).

```python
result = dataset.interpolate(column="temp")
```

```python
result = await dataset.ainterpolate(
    column="temp",
    limit=1,
    limit_direction="both",
    limit_area="inside",
)
```

Parameters:

| Parameter | Type | Description |
| --- | --- | --- |
| `column` | `str` | Numeric column to fill. |
| `method` | `str` | Must be `"linear"`. Anything else fails loudly. |
| `limit` | `int` or `None` | Max consecutive NaNs to fill, counted from the fill edge per direction. Must be positive. |
| `limit_direction` | `str` | `"forward"` (default), `"backward"`, or `"both"`. |
| `limit_area` | `str` or `None` | `None` (default, no restriction), `"inside"` (gaps only), `"outside"` (edges only). |
| `order_by` | `str`, `list[str]`, or `None` | Column(s) sequencing rows before interpolating (single or list, `ASC`, `NULLS LAST`). Omitted means physical row order. The ordering is temporary: values compute along the sequence but rows keep their original positions. |
| `dtype` | `str` or `None` | Optional override: only `"numeric"` is accepted; anything else fails. |

The result column is `cleaned_<column>_interpolated` (always floating point —
pandas upcasts the same way). Non-numeric columns fail; pass
`dtype="numeric"` only to override a misdetected numeric column.

```python
result = dataset.interpolate(column="temp", order_by="ts")
```

`order_by` accepts one column or a list. Duplicate order keys make ties
nondeterministic — pass a unique key when it matters. Unknown columns fail
before any table is created.

### `replace`

`replace` substitutes values in one column, a list of columns, or all columns
(`column=None`, the pandas default). `to_replace` takes pandas' scalar, list,
flat-dict, nested-dict, and pattern forms; `value` is the replacement (`None`
writes `NULL`); `regex=True` matches patterns on text columns instead of exact
equality.

```python
result = dataset.replace(column="status", to_replace={"a": "b", "y": "z"})
```

```python
result = await dataset.areplace(to_replace=r"^ba.$", value="new", regex=True)
result = await dataset.areplace(to_replace={"A": {0: 100, 4: 400}})
```

Parameters:

| Parameter | Type | Description |
| --- | --- | --- |
| `column` | `str`, `list[str]`, or `None` | Target column(s). `None` means all columns (generated `cleaned_` columns excluded). |
| `to_replace` | scalar, list, dict, pattern, or `None` | Values/patterns to find. Dicts follow pandas' rule: with `value` given, keys are column names; without it, `{old: new}` (or nested `{col: {old: new}}`). |
| `value` | scalar, list, or `None` | Replacement. Lists must match `to_replace` length; `None` writes `NULL`. |
| `regex` | `bool`, pattern, or list/dict of patterns | `True` interprets string entries as patterns; a pattern (or list/dict) implies it with `to_replace=None`. |

Each replaced column gains a `cleaned_<column>_replaced` column; numerics
always match exactly, even under `regex=True`. Replacements are cast to the
column's native type (a value that cannot live there fails loudly — scope
`column=` to dodge frame-wide mismatches), except regex replacements, which
stay raw text so backreferences pass through. Frame-wide regex skips non-text
columns, mirroring pandas; naming a non-text column explicitly fails.
`inplace` is not offered — operations return new columns, never rewrites.

### `dropna`

`dropna` drops rows or columns based on missing values.

```python
rows = dataset.dropna(axis=0, how="any")
columns = dataset.dropna(axis=1, thresh=0.1)
```

```python
rows = await dataset.adropna(axis=0, how="all")
```

Parameters:

| Parameter | Type | Description |
| --- | --- | --- |
| `axis` | `0`, `1`, `"index"`, or `"columns"` | `0`/`"index"` drops rows; `1`/`"columns"` drops columns. |
| `how` | `"any"` or `"all"` | Used when `thresh` is not provided. `"any"` drops if any value is missing; `"all"` drops only if all values are missing. |
| `thresh` | `int`, `float`, or `None` | Minimum non-null count. A float between 0 and 1 is treated as a maximum allowed null fraction. |

When `thresh` is provided, `how` is ignored.

### `isna` and `notna`

`isna` and `notna` return boolean masks as generated tables.

```python
null_mask = dataset.isna()
valid_mask = dataset.notna()
```

```python
null_mask = await dataset.aisna()
valid_mask = await dataset.anotna()
```

## Numeric Cleaning

### `clip`

`clip` creates a cleaned numeric column where values outside optional lower and
upper bounds become `NULL`. This is range enforcement, not pandas-style
clamping to the boundary value.

```python
result = dataset.clip(column="salary", lower=0, upper=250000)
```

```python
result = await dataset.aclip(column="score", lower=0, upper=100)
```

Parameters:

| Parameter | Type | Description |
| --- | --- | --- |
| `column` | `str` | Numeric column to clean. |
| `lower` | `int`, `float`, or `None` | Values below this are set to `NULL`. |
| `upper` | `int`, `float`, or `None` | Values above this are set to `NULL`. |

### `drop_outliers`

`drop_outliers` creates a cleaned column where values with absolute z-score
greater than `z_thresh` become `NULL`.

```python
result = dataset.drop_outliers(column="salary", z_thresh=3.0)
```

```python
result = await dataset.adrop_outliers(column="amount", z_thresh=2.5)
```

### `to_numeric`

`to_numeric` converts a text-like column to a numeric type **in place**,
stripping non-numeric characters from its values. The target column type
(integer, float, or decimal) is auto-detected from a sample of the cleaned
values. Invalid or empty numeric tokens become `NULL`.

```python
result = dataset.to_numeric(column="price_text")
```

```python
result = await dataset.ato_numeric(column="amount_raw")
```

## Categorical Cleaning

### `map_values`

`map_values` creates a cleaned column by replacing values using a mapping
dictionary. Unmapped values keep their original value.

```python
result = dataset.map_values(
    column="status",
    mapping={"Y": "yes", "N": "no"},
)
```

```python
result = await dataset.amap_values(
    column="segment",
    mapping={"SMB": "small_business"},
)
```

### `filter_valid`

`filter_valid` keeps values that appear in `valid_values` and sets all other
non-null values to `NULL`.

```python
result = dataset.filter_valid(
    column="department",
    valid_values=["Sales", "Engineering", "Finance"],
)
```

```python
result = await dataset.afilter_valid(
    column="state",
    valid_values=["CA", "NY", "TX"],
)
```

`valid_values` must be non-empty.

### `compress_rare`

`compress_rare` replaces categories whose frequency is less than `min_count`
with `other_label`.

```python
result = dataset.compress_rare(
    column="city",
    min_count=10,
    other_label="other",
)
```

```python
result = await dataset.acompress_rare(
    column="category",
    min_count=5,
    other_label="rare",
)
```

## Datetime Cleaning

### `fix_dates`

`fix_dates` handles known invalid date strings such as `0000-00-00` by setting
the cleaned value to `NULL`.

```python
result = dataset.fix_dates(column="signup_date")
```

```python
result = await dataset.afix_dates(column="event_date")
```

### `clip_dates`

`clip_dates` creates a cleaned date column where dates outside the supplied
range become `NULL`.

```python
result = dataset.clip_dates(
    column="signup_date",
    min_dt="2020-01-01",
    max_dt="2025-12-31",
)
```

```python
result = await dataset.aclip_dates(column="event_date", min_dt="2023-01-01")
```

Parameters:

| Parameter | Type | Description |
| --- | --- | --- |
| `column` | `str` | Date or timestamp column to clean. |
| `min_dt` | `str` or `None` | Minimum allowed date, inclusive. |
| `max_dt` | `str` or `None` | Maximum allowed date, inclusive. |

If both bounds are omitted, the core operation defaults to `1900-01-01` through
`2100-01-01`.

## Row and Column Removal

### `drop`

`drop` removes rows or columns and materializes the result as a generated table.

```python
rows_removed = dataset.drop(axis=0, index=[0, 3, 5])
cols_removed = dataset.drop(axis=1, columns=["notes", "raw_value"])
```

```python
result = await dataset.adrop(axis=1, columns=["temporary_column"])
```

Parameters:

| Parameter | Type | Description |
| --- | --- | --- |
| `columns` | `list[str]` or `None` | Columns to drop when `axis=1`. |
| `axis` | `0`, `1`, `"index"`, or `"columns"` | Drop rows with `0`/`"index"`; drop columns with `1`/`"columns"`. |
| `index` | `list[int]` or `None` | Zero-based row positions to drop when `axis=0`. |

### `drop_duplicates`

`drop_duplicates` removes duplicate rows using SQL window functions.

```python
result = dataset.drop_duplicates(subset=["email"], keep="first")
```

```python
result = await dataset.adrop_duplicates(
    subset=["customer_id", "event_time"],
    keep=False,
)
```

Parameters:

| Parameter | Type | Description |
| --- | --- | --- |
| `subset` | `list[str]` or `None` | Columns used to identify duplicates. Defaults to all columns. |
| `keep` | `"first"`, `"last"`, or `False` | Which duplicate row to keep. `False` keeps only rows with no duplicates. |

### `duplicated`

`duplicated` flags duplicate rows without removing them: same subset/keep
semantics as `drop_duplicates`, but the generated table keeps every row plus
a `duplicated` boolean column. `NULL`s group together (pandas' `NaN == NaN`
rule) on all backends, and first/last follow physical row order.

```python
result = dataset.duplicated(subset=["email"], keep=False)
```

```python
result = await dataset.aduplicated(keep="last")
```

Parameters:

| Parameter | Type | Description |
| --- | --- | --- |
| `subset` | `str`, `list[str]`, or `None` | Columns used to identify duplicates. Defaults to all columns. |
| `keep` | `"first"`, `"last"`, or `False` | Which occurrence is not flagged. `False` flags every row of a duplicate set. |

## Return Values and Errors

Public cleaning methods return the underlying DataFrame, dictionary, mask, or
other simple result. Failed operations raise `OperationError`. The internal
response envelope still carries `new_table`, column metadata, and fill metrics.

## Generated Tables

Most cleaning operations are non-destructive to the source upload table. They
create a generated table internally, usually in the same active schema (or database on ClickHouse).

Column-cleaning operations usually:

1. Copy the source table to a generated operation table.
2. Add a generated column such as `cleaned_salary_mean_filled`.
3. Populate the generated column while preserving the original column.
4. Return a sample DataFrame containing the source and generated columns.

Row/column operations such as `dropna`, `drop`, `isna`, `notna`, and
`drop_duplicates` materialize a query result as the generated table.

## Backend Behavior

Cleaning supports DuckDB and PostgreSQL adapters:

- Identifiers are sanitized and quoted before SQL is generated.
- PostgreSQL uses `ctid` for row-wise update joins where needed.
- DuckDB uses `rowid` for row-wise update joins where needed.
- Some forward/backward fill implementations differ by backend because DuckDB
  supports `IGNORE NULLS` window expressions and PostgreSQL uses fallback
  window expressions. `interpolate` instead uses the portable islands idiom
  (`COUNT` + `FIRST_VALUE` windows) on all backends; only the float cast
  differs (`DOUBLE` / `DOUBLE PRECISION` / `Float64`). ClickHouse rebuilds via
  temp tables because window functions are illegal in `ALTER TABLE ... UPDATE`.
- Date and percentile expressions use backend-specific SQL where necessary.

## Errors

Cleaning methods raise `OperationError` for invalid input or backend failures.

- `fillna(method="constant")` requires `value`.
- `fillna(method="map")` requires `mapping`.
- Numeric fill methods such as `mean`, `median`, `std`, `var`, `min`, and
  `max` are rejected for categorical columns.
- `groupby_fillna` requires `group_cols`.
- `dropna` rejects invalid `axis`, invalid `how`, and non-positive `thresh`.
- `drop` requires `index` for row drops and `columns` for column drops.
- `drop_duplicates` requires `keep` to be `"first"`, `"last"`, or `False`.
- `duplicated` requires `keep` to be `"first"`, `"last"`, or `False`, and
  `subset` to be a label or list of known columns.
- `filter_valid` requires a non-empty `valid_values` list.
- `interpolate` requires `method="linear"`, a numeric column, a positive
  `limit`, `limit_direction` in `"forward"`/`"backward"`/`"both"`, and
  `limit_area` in `None`/`"inside"`/`"outside"`.
- `replace` rejects length-mismatched lists, `value` dicts (use nested
  `to_replace`), mixed nested/flat dicts, regex on explicitly named non-text
  columns, and values that cannot cast to the column type.

## API Reference

::: memframe.wrappers.analytix.cleaning.CleaningWrapper
    options:
      show_root_heading: true
      show_root_full_path: true
      members:
        - afillna
        - fillna
        - ainterpolate
        - interpolate
        - areplace
        - replace
        - aclip
        - clip
        - adrop_outliers
        - drop_outliers
        - ato_numeric
        - to_numeric
        - amap_values
        - map_values
        - afilter_valid
        - filter_valid
        - acompress_rare
        - compress_rare
        - afix_dates
        - fix_dates
        - aclip_dates
        - clip_dates
        - agroupby_fillna
        - groupby_fillna
        - adropna
        - dropna
        - adrop
        - drop
        - aisna
        - isna
        - anotna
        - notna
        - adrop_duplicates
        - drop_duplicates
        - aduplicated
        - duplicated
