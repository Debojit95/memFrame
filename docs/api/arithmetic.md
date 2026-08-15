# Arithmetic

Source: `src/memframe/wrappers/analytix/arithmetic.py`

`ArithmeticWrapper` is the public arithmetic interface exposed through a
`ContextManager`. It provides pandas-like methods for binary arithmetic (add,
subtract, multiply, divide, modulo, power), unary transforms (absolute, negate,
round, ceil, floor, truncate), exp/log/root operations, trigonometric functions,
and complex operations (weighted sum, percentage change, normalize range).

Users normally call arithmetic methods directly on a dataset context returned by
an upload operation:

```python
dataset = mf.upload_df(frame)
result = dataset.add("salary", "bonus", "total_income")
```

The same methods are also available from `dataset.arithmetic`.

The lower-level files are implementation details:

- `src/memframe/core/analytix/arithmetic.py` builds and executes backend-specific SQL.
- `src/memframe/core/orchestrator/analytix/arithmetic.py` resolves the active dataset
  context and passes persistence metadata.
- `src/memframe/wrappers/analytix/arithmetic.py` exposes synchronous and asynchronous
  public methods.

## Public API

Every arithmetic operation has synchronous and asynchronous forms:

| Synchronous | Asynchronous | Purpose |
| --- | --- | --- |
| `add(col1, col2, target_col=None)` | `await aadd(...)` | Add two columns or scalar values |
| `subtract(col1, col2, target_col=None)` | `await asubtract(...)` | Subtract second operand from first |
| `sub(col1, col2, target_col=None)` | `await asub(...)` | Alias for subtract |
| `mul(col1, col2, target_col=None)` | `await amul(...)` | Multiply two operands |
| `div(col1, col2, target_col=None)` | `await adiv(...)` | Divide first operand by second |
| `mod(col1, col2, target_col=None)` | `await amod(...)` | Modulo of first by second |
| `pow(col1, col2, target_col=None)` | `await apow(...)` | First operand to power of second |
| `abs(column, target_col=None)` | `await aabs(...)` | Absolute value |
| `negate(column, target_col=None)` | `await anegate(...)` | Negate values |
| `round(column, digits=0, target_col=None)` | `await around(...)` | Round to given digits |
| `ceil(column, target_col=None)` | `await aceil(...)` | Ceiling |
| `floor(column, target_col=None)` | `await afloor(...)` | Floor |
| `truncate(column, digits=0, target_col=None)` | `await atruncate(...)` | Truncate to given digits |
| `exp(column, target_col=None)` | `await aexp(...)` | Exponential transform |
| `log(column, target_col=None)` | `await alog(...)` | Natural logarithm |
| `log10(column, target_col=None)` | `await alog10(...)` | Base-10 logarithm |
| `sqrt(column, target_col=None)` | `await asqrt(...)` | Square root |
| `sin(column, target_col=None)` | `await asin(...)` | Sine |
| `cos(column, target_col=None)` | `await acos(...)` | Cosine |
| `tan(column, target_col=None)` | `await atan(...)` | Tangent |
| `asin(column, target_col=None)` | `await aasin(...)` | Arcsine |
| `acos(column, target_col=None)` | `await aacos(...)` | Arccosine |
| `atan(column, target_col=None)` | `await aatan(...)` | Arctangent |
| `atan2(col1, col2, target_col=None)` | `await aatan2(...)` | Two-argument arctangent |
| `weighted_sum(col1, col2, weight1=1, weight2=1, target_col=None)` | `await aweighted_sum(...)` | Weighted sum of two operands |
| `percentage_change(old_col, new_col, target_col=None)` | `await apercentage_change(...)` | Percentage change |
| `normalize_range(column, target_col=None)` | `await anormalize_range(...)` | Min-max normalize |

Public methods return the resulting DataFrame directly. Invalid operations
raise `OperationError`.

## Usage Overview

```python
dataset = mf.upload_df(frame)

sample = dataset.add("salary", "bonus", "total_income")
```

```python
dataset = await mf.aupload_df(frame)

normalized_sample = await dataset.anormalize_range("salary", "normalized_salary")
```

## Binary Operations

### `add`, `subtract`, `sub`, `mul`, `div`

These methods combine two columns or a column with a scalar.

```python
result = dataset.add("salary", "bonus", "total_income")
result = dataset.mul("salary", 2, "double_salary")
result = dataset.div("revenue", "units", "revenue_per_unit")
```

```python
result = await dataset.asubtract("revenue", "cost", "profit")
result = await dataset.amul("price", "quantity", "line_total")
```

Parameters:

| Parameter | Type | Description |
| --- | --- | --- |
| `col1` | `str`, `float`, or `int` | First operand (column name or scalar). |
| `col2` | `str`, `float`, or `int` | Second operand (column name or scalar). |
| `target_col` | `str` or `None` | Name for the result column. Auto-generated if omitted. |

Numeric-looking text columns are automatically cast to numeric before the
operation.

### `mod`

Modulo between two operands. Uses `MOD()` on PostgreSQL and DuckDB, `modulo()`
on ClickHouse.

```python
result = dataset.mod("amount", 100, "remainder")
```

```python
result = await dataset.amod("hours", "8", "overtime_mod")
```

### `pow`

Raises the first operand to the power of the second. Uses `POWER()` on
PostgreSQL and DuckDB, `pow()` on ClickHouse.

```python
result = dataset.pow("radius", 2, "area_factor")
```

```python
result = await dataset.apow("distance", "3", "cubic_distance")
```

## Unary Operations

### `abs`, `negate`

```python
result = dataset.abs("negative_vals", "positive_vals")
result = dataset.negate("temperature", "inverted_temp")
```

```python
result = await dataset.aabs("change", "abs_change")
```

### `round`, `ceil`, `floor`, `truncate`

`round` rounds to the given number of decimal places. `ceil` and `floor` each
return the nearest integer. `truncate` truncates toward zero to the given
number of digits.

```python
result = dataset.round("price", 2, "rounded_price")
result = dataset.ceil("score", "ceil_score")
result = dataset.floor("score", "floor_score")
result = dataset.truncate("float_vals", 2, "truncated")
```

```python
result = await dataset.around("value", 0, "rounded_int")
```

Parameters:

| Parameter | Type | Description |
| --- | --- | --- |
| `column` | `str` | Numeric column to transform. |
| `digits` | `int` | Number of decimal places (for `round` and `truncate`). Defaults to 0. |
| `target_col` | `str` or `None` | Name for the result column. |

On PostgreSQL, `ROUND(double, integer)` does not exist — the column is cast to
`NUMERIC` internally.

## Exp / Log / Root

### `exp`, `log`, `log10`, `sqrt`

```python
result = dataset.exp("rate", "exp_rate")
result = dataset.log("salary", "log_salary")
result = dataset.log10("salary", "log10_salary")
result = dataset.sqrt("variance", "std_dev")
```

```python
result = await dataset.aexp("growth", "exp_growth")
result = await dataset.alog10("value", "log10_val")
```

Natural logarithm uses `LN()` on PostgreSQL and DuckDB, `log()` on ClickHouse.
Base-10 log uses `LOG()` (one-arg) on PostgreSQL, `LOG10()` on DuckDB,
`log10()` on ClickHouse.
Negative or zero inputs produce `NULL`.

## Trigonometric Functions

### `sin`, `cos`, `tan`

Standard trigonometric functions operating on radian values.

```python
result = dataset.sin("angle", "sin_angle")
result = dataset.cos("angle", "cos_angle")
result = dataset.tan("angle", "tan_angle")
```

### `asin`, `acos`, `atan`

Inverse trigonometric functions.

```python
result = dataset.asin("ratio", "angle_rad")
result = dataset.acos("ratio", "angle_rad")
result = dataset.atan("ratio", "angle_rad")
```

### `atan2`

Two-argument arctangent of `col1 / col2`.

```python
result = dataset.atan2("y_coord", "x_coord", "angle")
```

```python
result = await dataset.aatan2("numerator", "denominator", "theta")
```

## Complex Operations

### `weighted_sum`

Computes `(col1 * weight1 + col2 * weight2) / (weight1 + weight2)`.

```python
result = dataset.weighted_sum("math", "science", 0.7, 0.3, "final_score")
```

```python
result = await dataset.aweighted_sum("exam1", "exam2", 0.5, 0.5, "average")
```

### `percentage_change`

Computes `((new - old) / |old|) * 100`.

```python
result = dataset.percentage_change("old_price", "new_price", "pct_change")
```

```python
result = await dataset.apercentage_change("last_year", "this_year", "yoy_growth")
```

### `normalize_range`

Applies min-max normalization: `(value - min) / (max - min)`.

```python
result = dataset.normalize_range("salary", "normalized_salary")
```

```python
result = await dataset.anormalize_range("distance", "scaled_distance")
```

## Return Values and Errors

Public arithmetic methods return the resulting DataFrame directly. Generated
table names and expression metadata remain internal to cache and AI layers.
Invalid operations raise `OperationError`.

## Generated Tables

Every arithmetic operation is non-destructive to the source upload table. Each
operation:

1. Clones the source table into a new transient table.
2. Adds a result column (e.g. `total_income`, `log_salary`, `sin_angle`).
3. Populates the result column using a SQL `UPDATE`.
4. Returns the sample DataFrame directly; table metadata remains internal.

## Backend Behavior

Arithmetic supports DuckDB, PostgreSQL, and ClickHouse adapters:

- Identifiers are sanitized and quoted before SQL is generated.
- Numeric-looking text columns are auto-cast via backend-specific functions
  (`TRY_CAST` on DuckDB, regex + cast on PostgreSQL, `toFloat64OrNull` on
  ClickHouse).
- `ROUND` with decimal digits casts to `NUMERIC` on PostgreSQL.
- `MOD`/`POWER`/`LN`/`LOG10`/`TRUNC` use backend-specific function names.
- `percentage_change` and `normalize_range` use `1.0 *` to force floating-point
  division, avoiding integer truncation.
- ClickHouse uses `ALTER TABLE ... UPDATE` instead of standard `UPDATE`.

## Errors

Arithmetic methods raise `OperationError` for backend or validation failures:

- Division by zero produces `NULL` in the result column (division uses
  `NULLIF(denominator, 0)`).
- Negative or zero inputs to `log`/`log10` produce `NULL`.
- Negative inputs to `sqrt` produce `NULL`.
- Values outside `[-1, 1]` for `asin`/`acos` produce `NULL`.
- Unsupported backends raise `OperationError`.

## API Reference

::: memframe.wrappers.analytix.arithmetic.ArithmeticWrapper
    options:
      show_root_heading: true
      show_root_full_path: true
      members:
        - aadd
        - add
        - asubtract
        - subtract
        - asub
        - sub
        - amul
        - mul
        - adiv
        - div
        - amod
        - mod
        - apow
        - pow
        - aabs
        - abs
        - anegate
        - negate
        - around
        - round
        - aceil
        - ceil
        - afloor
        - floor
        - atruncate
        - truncate
        - aexp
        - exp
        - alog
        - log
        - alog10
        - log10
        - asqrt
        - sqrt
        - sin
        - cos
        - tan
        - aasin
        - asin
        - aacos
        - acos
        - aatan
        - atan
        - aatan2
        - atan2
        - aweighted_sum
        - weighted_sum
        - apercentage_change
        - percentage_change
        - anormalize_range
        - normalize_range
