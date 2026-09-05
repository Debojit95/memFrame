# Datetime

Source: `src/memframe/wrappers/analytix/datetime.py`

`DateTimeWrapper` is the public datetime interface exposed through
`ContextManager.dt`. It provides pandas-like methods for extracting date parts,
rounding to calendar units, timezone localization and conversion, boolean
calendar checks, timestamp conversion, string formatting and parsing, timedelta
arithmetic, and normalization.

Users normally call datetime methods via the `dt` accessor on a dataset context
returned by an upload operation:

```python
dataset = mf.upload_df(frame)
result = dataset.dt.year("event_time")
```

```python
dataset = await mf.aupload_df(frame)
result = await dataset.dt.ayear("event_time")
```

`datetime` is `dt`-only (`dataset.dt.*`) — not `dataset.year` — to avoid
collision with `ArithmeticWrapper` (`floor/ceil/round/add/sub`). Non-datetime
operations remain at the top level.

The lower-level files are implementation details:

- `src/memframe/core/analytix/datetime.py` builds and executes backend-specific SQL.
- `src/memframe/core/orchestrator/analytix/datetime.py` resolves the active
  dataset context and passes persistence metadata.
- `src/memframe/wrappers/analytix/datetime.py` exposes synchronous and
  asynchronous public methods.

## Public API

Every datetime operation has synchronous and asynchronous forms:

| Synchronous | Asynchronous | Purpose |
| --- | --- | --- |
| `extract(column, field)` | `await aextract(...)` | Extract a datetime field |
| `year(column)` | `await ayear(...)` | Extract year |
| `month(column)` | `await amonth(...)` | Extract month |
| `day(column)` | `await aday(...)` | Extract day-of-month |
| `hour(column)` | `await ahour(...)` | Extract hour |
| `minute(column)` | `await aminute(...)` | Extract minute |
| `second(column)` | `await asecond(...)` | Extract second |
| `dayofweek(column)` | `await adayofweek(...)` | Extract day-of-week |
| `dayofyear(column)` | `await adayofyear(...)` | Extract day-of-year |
| `week(column)` | `await aweek(...)` | Extract week-of-year |
| `quarter(column)` | `await aquarter(...)` | Extract quarter |
| `floor(column, unit)` | `await afloor(...)` | Floor to calendar unit |
| `ceil(column, unit)` | `await aceil(...)` | Ceil to calendar unit |
| `round(column, unit)` | `await around(...)` | Round to calendar unit |
| `tz_localize(column, tz, ambiguous="raise", nonexistent="raise")` | `await atz_localize(...)` | Localize naive datetimes |
| `tz_convert(column, tz)` | `await atz_convert(...)` | Convert timezone |
| `is_month_start(column)` | `await ais_month_start(...)` | Flag month start |
| `is_month_end(column)` | `await ais_month_end(...)` | Flag month end |
| `is_year_start(column)` | `await ais_year_start(...)` | Flag year start |
| `is_year_end(column)` | `await ais_year_end(...)` | Flag year end |
| `is_quarter_start(column)` | `await ais_quarter_start(...)` | Flag quarter start |
| `is_quarter_end(column)` | `await ais_quarter_end(...)` | Flag quarter end |
| `days_in_month(column)` | `await adays_in_month(...)` | Days in month |
| `is_weekend(column)` | `await ais_weekend(...)` | Flag weekend |
| `is_weekday(column)` | `await ais_weekday(...)` | Flag weekday |
| `is_business_day(column)` | `await ais_business_day(...)` | Flag business day |
| `week_of_month(column)` | `await aweek_of_month(...)` | Week-of-month |
| `timestamp(column)` | `await atimestamp(...)` | Convert to Unix timestamp |
| `from_timestamp(column=None, value=None, tz=None)` | `await afrom_timestamp(...)` | Convert Unix timestamp to datetime |
| `strftime(column, fmt)` | `await astrftime(...)` | Format datetime to string |
| `strptime(column, fmt)` | `await astrptime(...)` | Parse string to datetime |
| `add(column, interval)` | `await aadd(...)` | Add interval |
| `sub(column, interval)` | `await asub(...)` | Subtract interval |
| `replace(column, **kwargs)` | `await areplace(...)` | Replace datetime components |
| `normalize(column)` | `await anormalize(...)` | Normalize to midnight |

Public methods return the resulting DataFrame directly. Invalid operations
raise `OperationError`.

## Usage Overview

```python
dataset = mf.upload_df(frame)

sample = dataset.dt.year("event_time")
sample = dataset.dt.floor("event_time", "day")
sample = dataset.dt.is_weekend("event_time")
```

```python
dataset = await mf.aupload_df(frame)

sample = await dataset.dt.ayear("event_time")
sample = await dataset.dt.afloor("event_time", "month")
sample = await dataset.dt.ais_weekend("event_time")
```

## Extract

### `extract`

Generic extractor. `field` must be one of the supported keys:

| Field | Aliases | Description |
| --- | --- | --- |
| `year` | — | Year |
| `month` | — | Month `1..12` |
| `day` | — | Day-of-month |
| `hour` | — | Hour `0..23` |
| `minute` | — | Minute |
| `second` | — | Second |
| `dayofweek` | `dow` | Day-of-week |
| `dayofyear` | `doy` | Day-of-year |
| `week` | `weekofyear` | Week-of-year |
| `quarter` | — | Quarter `1..4` |

```python
result = dataset.dt.extract("event_time", "year")
result = dataset.dt.extract("event_time", "dow")
```

```python
result = await dataset.dt.aextract("event_time", "quarter")
```

Parameters:

| Parameter | Type | Description |
| --- | --- | --- |
| `column` | `str` | Datetime column. |
| `field` | `str` | Field to extract (see table). |

Unsupported fields return `is_error True` (`"Unsupported datetime field"`).

### `year`, `month`, `day`, `hour`, `minute`, `second`, `dayofweek`, `dayofyear`, `week`, `quarter`

Shortcuts for `extract`:

```python
result = dataset.dt.year("event_time")
result = dataset.dt.month("signup_date")
result = dataset.dt.dayofweek("created_at")
result = dataset.dt.quarter("closed_at")
```

```python
result = await dataset.dt.ayear("event_time")
result = await dataset.dt.amonth("signup_date")
```

Parameters:

| Parameter | Type | Description |
| --- | --- | --- |
| `column` | `str` | Datetime column. |

Generated columns are named `dt_<column>_<field>` (e.g. `dt_event_time_year`).

## Rounding

### `floor`, `ceil`, `round`

Round datetimes to a calendar unit. Supported `unit` values are `year`,
`quarter`, `month`, `week`, `day`, `hour`, `minute`, `second`.

```python
result = dataset.dt.floor("event_time", "day")
result = dataset.dt.ceil("event_time", "month")
result = dataset.dt.round("event_time", "hour")
```

```python
result = await dataset.dt.afloor("event_time", "week")
result = await dataset.dt.aceil("event_time", "year")
result = await dataset.dt.around("event_time", "day")
```

Parameters:

| Parameter | Type | Description |
| --- | --- | --- |
| `column` | `str` | Datetime column. |
| `unit` | `str` | Calendar unit to round to. |

Unsupported units return `is_error True`. DuckDB and PostgreSQL use
`DATE_TRUNC`/`INTERVAL`; ClickHouse uses `toStartOfDay`/`toStartOfMonth` etc.

## Timezone

### `tz_localize`

Localize naive datetimes to a timezone. `tz=None` removes timezone (casts to
timestamp).

```python
result = dataset.dt.tz_localize("event_time", "UTC")
result = dataset.dt.tz_localize("event_time", None)
```

```python
result = await dataset.dt.atz_localize("event_time", "America/New_York")
```

Parameters:

| Parameter | Type | Description |
| --- | --- | --- |
| `column` | `str` | Datetime column. |
| `tz` | `str` or `None` | Target timezone or `None` to strip. |
| `ambiguous` | `str` | `"raise"` (default); other values warn but are not fully supported in SQL engines. |
| `nonexistent` | `str` | `"raise"` (default); other values warn similarly. |

`ambiguous`/`nonexistent` other than `"raise"` produce a warnings list.

### `tz_convert`

Convert timezone-aware datetimes. `tz=None` converts to UTC and strips.

```python
result = dataset.dt.tz_convert("event_time", "UTC")
result = dataset.dt.tz_convert("event_time", "Asia/Kolkata")
```

```python
result = await dataset.dt.atz_convert("event_time", "UTC")
```

Parameters:

| Parameter | Type | Description |
| --- | --- | --- |
| `column` | `str` | Datetime column. |
| `tz` | `str` or `None` | Target timezone. |

## Boolean Checks

All boolean checks create a `BOOLEAN` column `dt_<column>_<suffix>` and return
a sample DataFrame.

### `is_month_start`, `is_month_end`, `is_year_start`, `is_year_end`

```python
result = dataset.dt.is_month_start("event_time")
result = dataset.dt.is_month_end("event_time")
result = dataset.dt.is_year_start("event_time")
result = dataset.dt.is_year_end("event_time")
```

### `is_quarter_start`, `is_quarter_end`

```python
result = dataset.dt.is_quarter_start("event_time")
result = dataset.dt.is_quarter_end("event_time")
```

### `is_weekend`, `is_weekday`, `is_business_day`

`is_business_day` is currently `is_weekday` (`DOW 1..5`).

```python
result = dataset.dt.is_weekend("event_time")
result = dataset.dt.is_weekday("event_time")
result = dataset.dt.is_business_day("event_time")
```

### `days_in_month`, `week_of_month`

```python
result = dataset.dt.days_in_month("event_time")
result = dataset.dt.week_of_month("event_time")
```

Parameters:

| Parameter | Type | Description |
| --- | --- | --- |
| `column` | `str` | Datetime column. |

## Timestamp Conversion

### `timestamp`

Convert datetime to Unix timestamp (`DOUBLE PRECISION`).

```python
result = dataset.dt.timestamp("event_time")
```

```python
result = await dataset.dt.atimestamp("event_time")
```

Uses `EXTRACT(EPOCH ...)` on PostgreSQL, `epoch()` on DuckDB,
`toUnixTimestamp()` on ClickHouse.

### `from_timestamp`

Convert Unix timestamp (column or scalar) to datetime.

```python
result = dataset.dt.from_timestamp(column="epoch_col", tz="UTC")
result = dataset.dt.from_timestamp(value=1704067200, tz="UTC")
```

```python
result = await dataset.dt.afrom_timestamp(column="epoch_col")
```

Parameters:

| Parameter | Type | Description |
| --- | --- | --- |
| `column` | `str` or `None` | Column holding Unix timestamps. |
| `value` | `float` or `None` | Scalar Unix timestamp if `column` is omitted. |
| `tz` | `str` or `None` | Target timezone. |

One of `column` or `value` is required. Uses `TO_TIMESTAMP` / `toDateTime`
plus `toTimezone` on ClickHouse.

## Formatting

### `strftime`

Format datetime to string (`TEXT`).

```python
result = dataset.dt.strftime("event_time", "%Y-%m-%d")
```

```python
result = await dataset.dt.astrftime("event_time", "%Y-%m-%d")
```

PostgreSQL converts via `_convert_strftime_format` (`%Y→YYYY`), DuckDB uses
`strftime()`, ClickHouse `formatDateTime()`. Non-timestamp columns are cast to
`TIMESTAMP`/`DateTime` first.

### `strptime`

Parse string column to datetime (`TIMESTAMP`) using a format.

```python
result = dataset.dt.strptime("date_str", "%Y-%m-%d")
```

```python
result = await dataset.dt.astrptime("date_str", "%Y-%m-%d")
```

PostgreSQL `TO_TIMESTAMP(col, sql_fmt)`, DuckDB `strptime()`, ClickHouse
`parseDateTimeBestEffort()`.

Parameters:

| Parameter | Type | Description |
| --- | --- | --- |
| `column` | `str` | Column to format/parse. |
| `fmt` | `str` | Python `strftime` pattern. |

## Timedelta

### `add`, `sub`

Add or subtract an interval string.

```python
result = dataset.dt.add("event_time", "1 day")
result = dataset.dt.sub("event_time", "1 hour")
```

```python
result = await dataset.dt.aadd("event_time", "7 day")
result = await dataset.dt.asub("event_time", "30 minute")
```

Parameters:

| Parameter | Type | Description |
| --- | --- | --- |
| `column` | `str` | Datetime column. |
| `interval` | `str` | Interval literal (e.g. `"1 day"`, `"3 month"`). |

Uses `INTERVAL '...'` on DuckDB/PostgreSQL and `INTERVAL ...` with
`toTimezone` handling on ClickHouse.

## Replacement and Normalization

### `replace`

Replace one or more datetime components in place (`year`, `month`, `day`,
`hour`, `minute`, `second`).

```python
result = dataset.dt.replace("event_time", year=2000, month=1)
```

```python
result = await dataset.dt.areplace("event_time", hour=0, minute=0)
```

Parameters:

| Parameter | Type | Description |
| --- | --- | --- |
| `column` | `str` | Datetime column. |
| `**kwargs` | `int` | Components to replace: `year`, `month`, `day`, `hour`, `minute`, `second`. |

Unsupported keys return `is_error True`. Uses `MAKE_TIMESTAMP` / `makeDateTime`
with `EXTRACT` / `toYear` fallbacks.

### `normalize`

Normalize to midnight (`00:00:00`) of the same day.

```python
result = dataset.dt.normalize("event_time")
```

```python
result = await dataset.dt.anormalize("event_time")
```

`DATE_TRUNC('day')` on DuckDB/Postgres, `toStartOfDay()` on ClickHouse.

## Return Values and Errors

Public datetime methods return the resulting DataFrame directly. Generated table
names and `result_metadata` remain internal to cache and AI layers. Invalid
operations raise `OperationError`:

- Unsupported `field` for `extract` → `"Unsupported datetime field: ..."`
- Unsupported `unit` for `floor/ceil/round` → `"Unsupported ceil/round/floor unit: ..."`
- Unsupported `replace` keys → `"Unsupported field: ..."`
- `from_timestamp` without `column` or `value` → `"Provide either column or value"`
- Unsupported backend → `NotImplementedError` wrapped as `is_error True`

## Generated Tables

Every datetime operation is non-destructive to the source upload table. Each operation:

1. Clones the source table into a new transient table (`<table>__op_<n>`).
2. Adds a result column (e.g. `dt_event_time_year`, `dt_ts_floor_day`).
3. Populates it with backend-specific SQL (`EXTRACT`/`DATE_TRUNC`/ClickHouse `to*`).
4. Returns the sample DataFrame directly; table metadata remains internal.

## Backend Behavior

Datetime supports DuckDB, PostgreSQL, and ClickHouse adapters:

- Identifiers are sanitized via `SQLIdentifierSanitizer` and quoted (`"` vs `` ` ``).
- `EXTRACT(YEAR/MONTH/...)` on DuckDB/Postgres vs `toYear/toMonth/toDayOfMonth/toHour/...` on ClickHouse (with `CAST(col AS DateTime)` for text columns).
- `floor`/`ceil`/`round` use `DATE_TRUNC` + `INTERVAL` vs `toStartOf*` + `IF` on ClickHouse.
- Timezone uses `AT TIME ZONE` vs `toTimezone`.
- String formatting uses `TO_CHAR`/`strftime` vs `formatDateTime`.
- ClickHouse uses `ALTER TABLE ... UPDATE ... WHERE 1` instead of `UPDATE`.

## Errors

Datetime methods raise `OperationError` for validation or backend failures.
Invalid inputs produce `is_error True` with `involved_cols`/`generated_cols`
populated where applicable.

## API Reference

::: memframe.wrappers.analytix.datetime.DateTimeWrapper
    options:
      show_root_heading: true
      show_root_full_path: true
      members:
        - aextract
        - extract
        - ayear
        - year
        - amonth
        - month
        - aday
        - day
        - ahour
        - hour
        - aminute
        - minute
        - asecond
        - second
        - adayofweek
        - dayofweek
        - adayofyear
        - dayofyear
        - aweek
        - week
        - aquarter
        - quarter
        - afloor
        - floor
        - aceil
        - ceil
        - around
        - round
        - atz_localize
        - tz_localize
        - atz_convert
        - tz_convert
        - ais_month_start
        - is_month_start
        - ais_month_end
        - is_month_end
        - ais_year_start
        - is_year_start
        - ais_year_end
        - is_year_end
        - ais_quarter_start
        - is_quarter_start
        - ais_quarter_end
        - is_quarter_end
        - adays_in_month
        - days_in_month
        - ais_weekend
        - is_weekend
        - ais_weekday
        - is_weekday
        - ais_business_day
        - is_business_day
        - aweek_of_month
        - week_of_month
        - atimestamp
        - timestamp
        - afrom_timestamp
        - from_timestamp
        - astrftime
        - strftime
        - astrptime
        - strptime
        - aadd
        - add
        - asub
        - sub
        - areplace
        - replace
        - anormalize
        - normalize
