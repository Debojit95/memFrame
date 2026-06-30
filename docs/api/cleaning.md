# Cleaning

Source: `src/wrappers/analytix/cleaning.py`

`CleaningWrapper` is the public cleaning interface exposed through a
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
| `data_quality_missing_values(columns)` | `await adata_quality_missing_values(...)` | Missing-value counts |
| `data_quality_completeness_score(columns)` | `await adata_quality_completeness_score(...)` | Completeness percentages |
| `comprehensive_numeric_summary(columns)` | `await acomprehensive_numeric_summary(...)` | Numeric summary report |
| `statistical_profile_report(columns)` | `await astatistical_profile_report(...)` | Combined profile report |

All methods return a dictionary with `is_error`, `message`, `error_message`,
`result`, and column metadata such as `involved_cols` and `generated_cols`.

## Usage Overview

```python
dataset = mf.upload_csv("data/employees.csv")

result = dataset.fillna(column="salary", method="mean")
if not result["is_error"]:
    cleaned_sample = result["result"]
    next_table = result["new_table"]
```

```python
dataset = await mf.aupload_csv("data/employees.csv")

result = await dataset.afilter_valid(
    column="department",
    valid_values=["Sales", "Engineering", "Finance"],
)
if not result["is_error"]:
    cleaned_sample = result["result"]
```

Most cleaning methods materialize a new operation table and return its table
name under `new_table`. The response sample usually includes the original
column and a generated `cleaned_<column>...` column.

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
| `numeric` | `mean`, `avg`, `average`, `median`, `mode`, `constant`, `std`, `var`, `variance`, `min`, `max`, `ffill`, `bfill` |
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
| `numeric` | `mean`, `avg`, `average`, `median`, `mode`, `constant`, `std`, `var`, `variance`, `min`, `max`, `ffill`, `bfill` |
| `categorical` | `mode`, `ffill`, `bfill` |
| `datetime` | `min`, `max`, `mean`, `median`, `mode`, `ffill`, `bfill` |

`group_cols` must be non-empty.

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

`to_numeric` strips non-numeric characters from a text-like column and casts
valid numeric tokens to `NUMERIC`.

```python
result = dataset.to_numeric(column="price_text")
```

```python
result = await dataset.ato_numeric(column="amount_raw")
```

Invalid or empty numeric tokens become `NULL`.

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

## Data Quality Reports

### `data_quality_missing_values`

`data_quality_missing_values` returns per-column `total`, `non_null`,
`missing`, and `missing_pct` values.

```python
result = dataset.data_quality_missing_values(
    columns=["salary", "department"],
)
```

```python
result = await dataset.adata_quality_missing_values(
    columns=["salary", "department"],
)
```

### `data_quality_completeness_score`

`data_quality_completeness_score` returns completeness percentages for each
requested column.

```python
result = dataset.data_quality_completeness_score(columns=["salary"])
```

```python
result = await dataset.adata_quality_completeness_score(columns=["salary"])
```

### `comprehensive_numeric_summary`

`comprehensive_numeric_summary` generates numeric summaries for up to the first
20 requested columns.

```python
result = dataset.comprehensive_numeric_summary(columns=["salary", "bonus"])
```

```python
result = await dataset.acomprehensive_numeric_summary(columns=["salary", "bonus"])
```

### `statistical_profile_report`

`statistical_profile_report` combines completeness scoring and numeric summary
output into one response.

```python
result = dataset.statistical_profile_report(columns=["salary", "bonus"])
```

```python
result = await dataset.astatistical_profile_report(columns=["salary", "bonus"])
```

## Return Value Format

Cleaning methods return dictionaries. Common keys are:

| Key | Type | Description |
| --- | --- | --- |
| `is_error` | `bool` | `True` when the operation failed |
| `message` | `str` | Human-readable operation summary |
| `error_message` | `str` or `None` | Error details when `is_error` is true |
| `result` | `pd.DataFrame` or other payload | Sample of the generated table, boolean mask, or report payload |
| `involved_cols` | `list` | Source columns used by the operation |
| `generated_cols` | `list` | Cleaned/generated columns produced by the operation |
| `new_table` | `str` or `None` | Generated operation table name |
| `fill_mode` | `str` or absent | Fill strategy used by fill operations |
| `fill_value` | any or absent | Value/statistic used by fill operations |

Always check `is_error` before consuming `result` or `new_table`.

## Generated Tables

Most cleaning operations are non-destructive to the source upload table. They
create a generated table, usually in the same active schema, and return the
table name under `new_table`.

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
  window expressions.
- Date and percentile expressions use backend-specific SQL where necessary.

## Errors

Cleaning methods catch exceptions and return `is_error=True` in normal public
use.

- `fillna(method="constant")` requires `value`.
- `fillna(method="map")` requires `mapping`.
- Numeric fill methods such as `mean`, `median`, `std`, `var`, `min`, and
  `max` are rejected for categorical columns.
- `groupby_fillna` requires `group_cols`.
- `dropna` rejects invalid `axis`, invalid `how`, and non-positive `thresh`.
- `drop` requires `index` for row drops and `columns` for column drops.
- `drop_duplicates` requires `keep` to be `"first"`, `"last"`, or `False`.
- `filter_valid` requires a non-empty `valid_values` list.

## API Reference

::: src.wrappers.analytix.cleaning.CleaningWrapper
    options:
      show_root_heading: true
      show_root_full_path: true
      members:
        - afillna
        - fillna
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
        - adata_quality_missing_values
        - data_quality_missing_values
        - adata_quality_completeness_score
        - data_quality_completeness_score
        - acomprehensive_numeric_summary
        - comprehensive_numeric_summary
        - astatistical_profile_report
        - statistical_profile_report
