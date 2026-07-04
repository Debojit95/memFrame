# Stats

Source: `src/wrappers/analytix/stats.py`

`StatsWrapper` is the public statistics interface exposed through a
`ContextManager`. It provides pandas-like scalar statistics, numeric
distribution metrics, correlation and covariance matrices, categorical
association metrics, and datetime event summaries for the active backend table.

Users normally call stats methods directly on a dataset context returned by an
upload operation:

```python
dataset = mf.upload_df(frame)
result = dataset.mean(column="salary")
```

The same methods are also available from `dataset.stats`.

The lower-level files are implementation details:

- `src/core/analytix/stats.py` builds and executes backend-specific SQL.
- `src/core/orchestrator/analytix/stats.py` resolves the active dataset
  context, detects column families, and routes calls to numeric, categorical,
  or datetime implementations.
- `src/wrappers/analytix/stats.py` exposes synchronous and asynchronous public
  methods.

## Public API

Every stats operation has synchronous and asynchronous forms:

| Synchronous | Asynchronous | Purpose |
| --- | --- | --- |
| `count(column)` | `await acount(...)` | Count non-null values |
| `min(column)` | `await amin(...)` | Minimum numeric value or earliest datetime |
| `max(column)` | `await amax(...)` | Maximum numeric value or latest datetime |
| `mode(column, top_n=1)` | `await amode(...)` | Most frequent value or values |
| `unique(column)` | `await aunique(...)` | Distinct non-null values |
| `nunique(column)` | `await anunique(...)` | Number of distinct non-null values |
| `value_counts(column, top_n=10)` | `await avalue_counts(...)` | Top value frequencies |
| `mean(column)` | `await amean(...)` | Numeric mean or mean datetime |
| `median(column)` | `await amedian(...)` | Numeric median or median datetime |
| `sum(column)` | `await asum(...)` | Numeric sum |
| `std(column)` | `await astd(...)` | Population standard deviation |
| `var(column)` | `await avar(...)` | Population variance |
| `sem(column)` | `await asem(...)` | Standard error of the mean |
| `mad(column)` | `await amad(...)` | Mean absolute deviation |
| `iqr(column)` | `await aiqr(...)` | Interquartile range |
| `range(column)` | `await arange(...)` | Numeric range, `max - min` |
| `skew(column)` | `await askew(...)` | Skewness |
| `kurtosis(column)` | `await akurtosis(...)` | Kurtosis |
| `entropy(column)` | `await aentropy(...)` | Entropy from value frequencies |
| `quantile(column, q=None)` | `await aquantile(...)` | One or more quantiles |
| `autocorr(column, lag=1)` | `await aautocorr(...)` | Autocorrelation at a lag |
| `coefficient_of_variation(column)` | `await acoefficient_of_variation(...)` | Standard deviation divided by mean |
| `outliers_iqr(column)` | `await aoutliers_iqr(...)` | Outlier values by the IQR rule |
| `outliers_zscore(column, threshold=3.0)` | `await aoutliers_zscore(...)` | Outlier values by z-score |
| `corr(columns=None)` | `await acorr(...)` | Numeric correlation matrix |
| `cov(columns=None)` | `await acov(...)` | Numeric covariance matrix |
| `proportions(column)` | `await aproportions(...)` | Category proportions |
| `chi_square(column1, column2)` | `await achi_square(...)` | Chi-square association statistic |
| `cramers_v(column1, column2)` | `await acramers_v(...)` | Cramer's V association score |
| `theil_u(column1, column2)` | `await atheil_u(...)` | Theil's U asymmetric association score |
| `mutual_information(column1, column2)` | `await amutual_information(...)` | Mutual information |
| `datetime_diff(column)` | `await adatetime_diff(...)` | Consecutive datetime differences |
| `time_delta_stats(column)` | `await atime_delta_stats(...)` | Summary stats over datetime deltas |
| `event_rate(column, unit="day")` | `await aevent_rate(...)` | Event rate per time unit |
| `time_unit_counts(column, unit="day")` | `await atime_unit_counts(...)` | Counts grouped by a datetime part |
| `weekday_weekend_counts(column)` | `await aweekday_weekend_counts(...)` | Weekday versus weekend counts |
| `holiday_counts(column)` | `await aholiday_counts(...)` | New Year and Christmas counts |

All methods return a dictionary with `is_error`, `message`, `error_message`,
`result`, and column metadata such as `involved_cols` and `generated_cols`.
Matrix operations such as `corr` and `cov` may also return `new_table`.

## Usage Overview

```python
dataset = mf.upload_csv("data/employees.csv")

result = dataset.mean(column="salary")
if not result["is_error"]:
    print(result["result"])
```

```python
dataset = await mf.aupload_csv("data/events.csv")

result = await dataset.acorr(columns=["amount", "discount", "tax"])
if not result["is_error"]:
    matrix = result["result"]
```

Stats methods are exposed directly through context forwarding. You can use
`dataset.mean(...)` or the explicit `dataset.stats.mean(...)` form.

## Response Shape

Successful scalar methods return the computed value under `result`:

```python
{
    "is_error": False,
    "message": "Mean of 'salary': 72500.0000",
    "error_message": None,
    "involved_cols": [],
    "generated_cols": [],
    "result": 72500.0,
}
```

Failed operations return `is_error=True` and place the backend or validation
message under `error_message`.

Most stats methods ignore `NULL` values. Counts use non-null counts unless the
method description says otherwise.

## Dtype Routing

The orchestrator samples the target column and detects one of three column
families: `numeric`, `categorical`, or `datetime`.

The following methods route automatically:

| Method | Routing behavior |
| --- | --- |
| `count` | Numeric, categorical, or datetime non-null count |
| `min`, `max` | Datetime min/max for datetime columns; numeric min/max otherwise |
| `mode`, `unique`, `value_counts` | Categorical implementation for categorical columns; numeric implementation otherwise |
| `nunique` | Numeric, categorical, or datetime distinct count |
| `mean`, `median` | Datetime mean/median for datetime columns; numeric mean/median otherwise |

Numeric-only methods should be used on numeric columns. Categorical association
methods should be used on categorical columns. Datetime methods should be used
on date or timestamp columns.

## Common Parameters

| Parameter | Type | Description |
| --- | --- | --- |
| `column` | `str` | Column to analyze. Null values are ignored by most operations. |
| `column1` | `str` | First categorical column used in pairwise association methods. |
| `column2` | `str` | Second categorical column used in pairwise association methods. |
| `columns` | `list[str]` or `None` | Candidate numeric columns for matrix operations. If omitted, all backend columns are considered and non-numeric columns are filtered out. |
| `top_n` | `int` | Maximum number of values to return for ranked frequency operations. |
| `q` | `list[float]` or `None` | Quantiles to compute. Values should be between `0` and `1`. Defaults to `[0.25, 0.5, 0.75]`. |
| `lag` | `int` | Row lag used by autocorrelation. Defaults to `1`. |
| `threshold` | `float` | Z-score threshold for outlier detection. Defaults to `3.0`. |
| `unit` | `str` | Time unit for datetime grouping or rates. Supported values depend on the method. |

## Scalar Statistics

### `count`

`count` returns the number of non-null values in a column.

```python
result = dataset.count(column="salary")
```

```python
result = await dataset.acount(column="department")
```

Parameters:

| Parameter | Type | Description |
| --- | --- | --- |
| `column` | `str` | Column whose non-null values should be counted. |

### `min` and `max`

`min` and `max` return the smallest and largest non-null values. Datetime
columns are routed to datetime implementations, returning the earliest or
latest datetime.

```python
lowest = dataset.min(column="salary")
highest = dataset.max(column="salary")
```

```python
first_login = await dataset.amin(column="last_login")
last_login = await dataset.amax(column="last_login")
```

Parameters:

| Parameter | Type | Description |
| --- | --- | --- |
| `column` | `str` | Numeric or datetime column to summarize. |

### `mean` and `median`

`mean` and `median` summarize numeric columns. For datetime columns, values are
converted to epoch seconds, averaged or medianed, then converted back to a date
or timestamp.

```python
avg_salary = dataset.mean(column="salary")
median_salary = dataset.median(column="salary")
```

```python
avg_login = await dataset.amean(column="last_login")
median_login = await dataset.amedian(column="last_login")
```

Parameters:

| Parameter | Type | Description |
| --- | --- | --- |
| `column` | `str` | Numeric or datetime column to summarize. |

### `mode`

`mode` returns the most frequent non-null value. When `top_n=1`, `result`
contains a scalar value. When `top_n` is greater than `1`, `result` contains a
list of values in descending frequency order.

```python
top_department = dataset.mode(column="department")
top_scores = dataset.mode(column="score", top_n=3)
```

```python
result = await dataset.amode(column="department", top_n=5)
```

Parameters:

| Parameter | Type | Description |
| --- | --- | --- |
| `column` | `str` | Column whose most frequent values should be returned. |
| `top_n` | `int` | Number of modes to return. Defaults to `1`. |

### `unique` and `nunique`

`unique` returns the distinct non-null values. `nunique` returns only the
number of distinct non-null values.

```python
values = dataset.unique(column="department")
count = dataset.nunique(column="department")
```

```python
values = await dataset.aunique(column="department")
count = await dataset.anunique(column="department")
```

Parameters:

| Parameter | Type | Description |
| --- | --- | --- |
| `column` | `str` | Column whose distinct values should be computed. |

### `value_counts`

`value_counts` returns a dictionary of the most common non-null values and
their counts.

```python
result = dataset.value_counts(column="department", top_n=10)
```

```python
result = await dataset.avalue_counts(column="department", top_n=5)
```

Parameters:

| Parameter | Type | Description |
| --- | --- | --- |
| `column` | `str` | Column whose values should be counted. |
| `top_n` | `int` | Maximum number of value-count pairs to return. Defaults to `10`. |

## Numeric Statistics

Numeric statistics are intended for numeric columns. They return scalar values,
dictionaries, or lists depending on the operation.

```python
total = dataset.sum(column="salary")
std = dataset.std(column="salary")
outliers = dataset.outliers_iqr(column="salary")
```

```python
quantiles = await dataset.aquantile(column="salary", q=[0.1, 0.5, 0.9])
```

### Numeric Method Details

| Method | Parameters | Result |
| --- | --- | --- |
| `sum` | `column` | Sum of non-null values. |
| `std` | `column` | Population standard deviation using `STDDEV_POP`. |
| `var` | `column` | Population variance using `VAR_POP`. |
| `sem` | `column` | Sample standard deviation divided by square root of non-null count. |
| `mad` | `column` | Mean absolute deviation from the column mean. |
| `iqr` | `column` | 75th percentile minus 25th percentile. |
| `range` | `column` | Maximum minus minimum. |
| `skew` | `column` | Skewness using backend `SKEWNESS`. |
| `kurtosis` | `column` | Kurtosis using backend `KURTOSIS`. |
| `entropy` | `column` | Entropy calculated from value-count probabilities. |
| `quantile` | `column`, `q=None` | Dictionary keyed as `p_25`, `p_50`, etc. |
| `autocorr` | `column`, `lag=1` | Correlation between the current value and the lagged value. |
| `coefficient_of_variation` | `column` | Population standard deviation divided by average. |
| `outliers_iqr` | `column` | List of values outside `Q1 - 1.5 * IQR` or `Q3 + 1.5 * IQR`. |
| `outliers_zscore` | `column`, `threshold=3.0` | List of values whose absolute z-score exceeds `threshold`. |

Parameters:

| Parameter | Type | Description |
| --- | --- | --- |
| `column` | `str` | Numeric column to analyze. |
| `q` | `list[float]` or `None` | Quantiles for `quantile`. Defaults to `[0.25, 0.5, 0.75]`. |
| `lag` | `int` | Offset used by `autocorr`. Defaults to `1`. |
| `threshold` | `float` | Z-score cutoff used by `outliers_zscore`. Defaults to `3.0`. |

## Matrix Statistics

### `corr`

`corr` computes a pairwise correlation matrix for numeric columns. If
`columns` is omitted, all backend columns are considered and non-numeric
columns are filtered out by the orchestrator.

```python
result = dataset.corr(columns=["salary", "bonus", "tax"])
matrix = result["result"]
```

```python
result = await dataset.acorr()
```

Parameters:

| Parameter | Type | Description |
| --- | --- | --- |
| `columns` | `list[str]` or `None` | Candidate columns. Only columns detected as numeric are included. |

Return behavior:

- `result` contains a pandas DataFrame correlation matrix.
- `new_table` contains the transient table name when persistence context is
  available.
- The generated backend table stores the matrix in long format with
  `column1`, `column2`, and `value`.

### `cov`

`cov` computes a pairwise sample covariance matrix for numeric columns.

```python
result = dataset.cov(columns=["salary", "bonus", "tax"])
```

```python
result = await dataset.acov()
```

Parameters:

| Parameter | Type | Description |
| --- | --- | --- |
| `columns` | `list[str]` or `None` | Candidate columns. Only columns detected as numeric are included. |

Return behavior matches `corr`, except values are computed with sample
covariance.

## Categorical Statistics

Categorical stats operate on non-null category values.

### `proportions`

`proportions` returns category shares as a dictionary where each value is
`count / non_null_total`.

```python
result = dataset.proportions(column="department")
```

```python
result = await dataset.aproportions(column="department")
```

Parameters:

| Parameter | Type | Description |
| --- | --- | --- |
| `column` | `str` | Categorical column to summarize. |

### Pairwise Association Methods

```python
chi2 = dataset.chi_square(column1="department", column2="region")
v = dataset.cramers_v(column1="department", column2="region")
u = dataset.theil_u(column1="department", column2="region")
mi = dataset.mutual_information(column1="department", column2="region")
```

```python
result = await dataset.acramers_v(
    column1="department",
    column2="region",
)
```

Parameters:

| Parameter | Type | Description |
| --- | --- | --- |
| `column1` | `str` | First categorical column. Rows with nulls in either column are ignored. |
| `column2` | `str` | Second categorical column. Rows with nulls in either column are ignored. |

Method behavior:

| Method | Result |
| --- | --- |
| `chi_square` | Dictionary with `chi2` and `df`. |
| `cramers_v` | Scalar Cramer's V value. |
| `theil_u` | Scalar Theil's U value for `column1 -> column2`. This measure is asymmetric. |
| `mutual_information` | Scalar mutual information value. |

## Datetime Statistics

Datetime stats operate on non-null date or timestamp values.

### `datetime_diff`

`datetime_diff` sorts the column and returns the differences in seconds between
consecutive non-null values.

```python
result = dataset.datetime_diff(column="last_login")
```

```python
result = await dataset.adatetime_diff(column="last_login")
```

Parameters:

| Parameter | Type | Description |
| --- | --- | --- |
| `column` | `str` | Datetime column to analyze. |

The response `result` is a list of second differences.

### `time_delta_stats`

`time_delta_stats` computes summary statistics over consecutive datetime
differences.

```python
result = dataset.time_delta_stats(column="last_login")
```

```python
result = await dataset.atime_delta_stats(column="last_login")
```

Parameters:

| Parameter | Type | Description |
| --- | --- | --- |
| `column` | `str` | Datetime column to analyze. |

The response `result` is a dictionary with keys such as `cnt`, `min_d`,
`max_d`, `avg_d`, `median_d`, and `std_d`, measured in seconds.

### `event_rate`

`event_rate` returns the number of non-null events divided by the time span
between the minimum and maximum timestamp.

```python
result = dataset.event_rate(column="created_at", unit="day")
```

```python
result = await dataset.aevent_rate(column="created_at", unit="hour")
```

Parameters:

| Parameter | Type | Description |
| --- | --- | --- |
| `column` | `str` | Datetime column to analyze. |
| `unit` | `str` | Rate denominator. Supported values are `second`, `minute`, `hour`, `day`, and `week`. Invalid values default to `day`. |

### `time_unit_counts`

`time_unit_counts` groups events by a date part and returns counts per part.

```python
result = dataset.time_unit_counts(column="created_at", unit="month")
```

```python
result = await dataset.atime_unit_counts(column="created_at", unit="dow")
```

Parameters:

| Parameter | Type | Description |
| --- | --- | --- |
| `column` | `str` | Datetime column to analyze. |
| `unit` | `str` | Date part to extract. Supported values are `hour`, `day`, `month`, `year`, `dow`, and `quarter`. Invalid values default to `day`. |

### `weekday_weekend_counts`

`weekday_weekend_counts` returns a dictionary with `weekday` and `weekend`
counts.

```python
result = dataset.weekday_weekend_counts(column="created_at")
```

```python
result = await dataset.aweekday_weekend_counts(column="created_at")
```

Parameters:

| Parameter | Type | Description |
| --- | --- | --- |
| `column` | `str` | Datetime column to analyze. |

### `holiday_counts`

`holiday_counts` returns counts for a small built-in holiday check. Currently
it classifies January 1 as `New Year`, December 25 as `Christmas`, and all
other dates as `non-holiday`.

```python
result = dataset.holiday_counts(column="created_at")
```

```python
result = await dataset.aholiday_counts(column="created_at")
```

Parameters:

| Parameter | Type | Description |
| --- | --- | --- |
| `column` | `str` | Datetime column to analyze. |

## Sync and Async Usage

Use async methods inside async code:

```python
result = await dataset.amean(column="salary")
```

Use sync methods from normal synchronous code:

```python
result = dataset.mean(column="salary")
```

Do not call sync methods from inside a running event loop. In async functions,
use the matching async method instead.
