# Arithmetic

> **Example data:** copy the setup cell from [Example Data](example-data.md) into Colab once, then run the examples below. Examples use that `ctx`; local frames are shown inline.

Source: `src/memframe/wrappers/analytix/arithmetic.py`

`ArithmeticWrapper` is the public arithmetic interface exposed through a
`ContextManager`. It provides pandas-like methods for binary arithmetic (add,
subtract, multiply, divide, modulo, power), unary transforms (absolute, negate,
round, ceil, floor, truncate), exp/log/root operations, trigonometric functions,
and complex operations (weighted sum, percentage change, normalize range).

Users normally call arithmetic methods directly on a dataset context returned by
an upload operation:

```python
# ctx is defined in the Example Data setup
result = ctx.add("salary", "bonus", "total_income")
```

The same methods are also available from `ctx.arithmetic`.

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
# ctx is defined in the Example Data setup

sample = ctx.add("salary", "bonus", "total_income")
```

```python
# ctx is defined in the Example Data setup

normalized_sample = await ctx.anormalize_range("salary", "normalized_salary")
```

## Binary Operations

### `add`, `subtract`, `sub`, `mul`, `div`

These methods combine two columns or a column with a scalar.

```python
result = ctx.add("salary", "bonus", "total_income")
result = ctx.mul("salary", 2, "double_salary")
result = ctx.div("salary", "age", "salary_per_age")
```

```python
result = await ctx.asubtract("sales", "bonus", "net_sales")
result = await ctx.amul("sales", "age", "sales_x_age")
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
result = ctx.mod("salary", 1000, "salary_mod")
```

```python
result = await ctx.amod("age", "8", "age_mod")
```

### `pow`

Raises the first operand to the power of the second. Uses `POWER()` on
PostgreSQL and DuckDB, `pow()` on ClickHouse.

```python
result = ctx.pow("age", 2, "age_squared")
```

```python
result = await ctx.apow("sales", "3", "sales_cubed")
```

## Unary Operations

### `abs`, `negate`

```python
result = ctx.abs("score", "abs_score")
result = ctx.negate("score", "neg_score")
```

```python
result = await ctx.aabs("score", "abs_score")
```

### `round`, `ceil`, `floor`, `truncate`

`round` rounds to the given number of decimal places. `ceil` and `floor` each
return the nearest integer. `truncate` truncates toward zero to the given
number of digits.

```python
result = ctx.round("score", 1, "rounded_score")
result = ctx.ceil("score", "ceil_score")
result = ctx.floor("score", "floor_score")
result = ctx.truncate("score", 0, "truncated_score")
```

```python
result = await ctx.around("sales", 0, "rounded_sales")
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
result = ctx.exp("age", "exp_age")
result = ctx.log("salary", "log_salary")
result = ctx.log10("salary", "log10_salary")
result = ctx.sqrt("age", "sqrt_age")
```

```python
result = await ctx.aexp("age", "exp_age")
result = await ctx.alog10("salary", "log10_salary")
```

Natural logarithm uses `LN()` on PostgreSQL and DuckDB, `log()` on ClickHouse.
Base-10 log uses `LOG()` (one-arg) on PostgreSQL, `LOG10()` on DuckDB,
`log10()` on ClickHouse.
Negative or zero inputs produce `NULL`.

## Trigonometric Functions

### `sin`, `cos`, `tan`

Standard trigonometric functions operating on radian values.

The main example table has no `[-1, 1]` column, so these examples use a small
local frame:

| `angle` | `ratio` | `x` | `y` |
| --- | --- | --- | --- |
| `0.0` | `-1.0` | `1.0` | `1.0` |
| `0.5` | `-0.5` | `2.0` | `1.5` |
| `1.0` | `0.0` | `3.0` | `2.0` |
| `1.5` | `0.5` | `4.0` | `2.5` |
| `2.0` | `1.0` | `5.0` | `3.0` |

```python
trig = pd.DataFrame({
    "angle": [0.0, 0.5, 1.0, 1.5, 2.0],
    "ratio": [-1.0, -0.5, 0.0, 0.5, 1.0],
    "x": [1.0, 2.0, 3.0, 4.0, 5.0],
    "y": [1.0, 1.5, 2.0, 2.5, 3.0],
})
trig_ctx = mf.DataFrame(trig, filename="trig")

result = trig_ctx.sin("angle", "sin_angle")
result = trig_ctx.cos("angle", "cos_angle")
result = trig_ctx.tan("angle", "tan_angle")
```

### `asin`, `acos`, `atan`

Inverse trigonometric functions.

```python
result = trig_ctx.asin("ratio", "angle_rad")
result = trig_ctx.acos("ratio", "angle_rad")
result = trig_ctx.atan("ratio", "angle_rad")
```

### `atan2`

Two-argument arctangent of `col1 / col2`.

```python
result = trig_ctx.atan2("y", "x", "angle")
```

```python
result = await trig_ctx.aatan2("y", "x", "theta")
```

## Complex Operations

### `weighted_sum`

Computes `(col1 * weight1 + col2 * weight2) / (weight1 + weight2)`.

```python
result = ctx.weighted_sum("salary", "bonus", 0.7, 0.3, "weighted")
```

```python
result = await ctx.aweighted_sum("salary", "bonus", 0.5, 0.5, "weighted_avg")
```

### `percentage_change`

Computes `((new - old) / |old|) * 100`.

```python
result = ctx.percentage_change("salary", "bonus", "pct_change")
```

```python
result = await ctx.apercentage_change("salary", "bonus", "pct_change")
```

### `normalize_range`

Applies min-max normalization: `(value - min) / (max - min)`.

```python
result = ctx.normalize_range("salary", "normalized_salary")
```

```python
result = await ctx.anormalize_range("age", "scaled_age")
```

## Return Values and Errors

Public arithmetic methods return the resulting DataFrame directly. Generated
table names and expression metadata remain internal to cache and AI layers.
Invalid operations raise `OperationError`.

## Generated Tables

Every arithmetic operation is non-destructive to the source upload table. Each
operation:

1. Clones the source table into a new cache table.
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
