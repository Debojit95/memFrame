# GroupBy Window

> **Example data:** copy the setup cell from [Example Data](../example-data.md) into Colab once, then run the examples below. Examples use that `ctx`; local frames are shown inline.

Source: `src/memframe/wrappers/analytix/groupby_window.py`

`GroupByWindowStatsWrapper` is the public partitioned-window interface
exposed through a `ContextManager`. It provides
pandas-`groupby().rolling()`-like analytics: rolling fixed-size windows
and expanding unbounded windows computed **per group** (`PARTITION BY`
the group columns), in-engine across DuckDB, PostgreSQL, and ClickHouse.
Each call adds the generated column(s) to the source upload table and returns
a preview of the involved and generated columns; the deep cache stores a ref
to the source, so a cached replay returns the same columns.

Users normally call grouped window methods through the unified `groupby`
builder on a dataset context returned by an upload operation. There are
two equivalent entry points:

- a direct call — `ctx.groupby("region").arolling(...)` — with the
  column, window, function, and group columns passed as arguments
  (async; sync via `rolling(...)` on the stats wrapper);
- a pandas-style fluent builder — `ctx.groupby("region").rolling(3,
  order_by="date").mean("sales")`.

```python
# ctx is defined in the Example Data setup

result = ctx.groupby("region").rolling(3, order_by="date").mean("sales")
result = ctx.groupby("region").expanding(order_by="date").sum("sales")
```

```python
# ctx is defined in the Example Data setup

result = await ctx.groupby("region").arolling(
    column="sales", window=3, func="mean",
    group_cols="region", order_by="date",
)
```

!!! note
    A flat `ctx.rolling(...)` resolves to the ungrouped
    `WindowWrapper.rolling(column, window, func, ...)` (it is registered
    first and takes no group columns). The grouped form above is only
    reachable through the `groupby(...)` builder or
    `GroupByWindowStatsWrapper` directly — the same arrangement as
    grouped `event_rate` on the Stats page.

The lower-level files are implementation details:

- `src/memframe/core/analytix/groupby_window/` builds and executes
  backend-specific SQL in `GroupbyWindowOps` and its DuckDB / PostgreSQL /
  ClickHouse subclasses. Grouped EWM reuses the ungrouped blockwise EWM
  helpers in `core/analytix/window/base.py`.
- `src/memframe/core/orchestrator/analytix/groupby_window.py` resolves
  the active dataset context, detects the column dtype, maps requested
  functions to dtype-appropriate engine methods, chains multi-function
  requests onto one working table, and applies `@record_call`.
- `src/memframe/wrappers/analytix/groupby_window.py` exposes the
  synchronous and asynchronous builder methods plus the direct API.
- `src/memframe/wrappers/analytix/groupby.py` is the unified `GroupBy`
  facade that routes `ctx.groupby(*columns)` calls to the stats,
  cumulative, and window builders.

## Public API

### Direct

| Synchronous | Asynchronous | Purpose |
| --- | --- | --- |
| `rolling(column, window, func, group_cols, order_by=None, q=0.5, min_periods=None, center=False, closed=None, win_type=None, func_kwargs=None)` | `await arolling(...)` | Grouped rolling window; `func` is a string or list |
| `expanding(column, func, group_cols, order_by=None, q=0.5, min_periods=1, func_kwargs=None)` | `await aexpanding(...)` | Grouped expanding window |
| `ewm(column, group_cols, order_by=None, com/span/halflife/alpha, ...)` | `await aewm(...)` | Grouped EWM (see note below) |

`group_cols` accepts a single column name or a list of names.

!!! note
    On a context, `ctx.arolling(...)` resolves to the **ungrouped** window
    method (it takes no `group_cols`). Grouped async calls go through the
    builder: `await ctx.groupby("region").rolling(3, order_by="date").amean("sales")`.

### Builder

`groupby(*columns)` returns the unified `GroupBy`; `.rolling(window,
order_by, min_periods=None, center=False, closed=None, win_type=None)` /
`.expanding(min_periods, order_by)` / `.ewm(...)` return
fluent builders. Each terminal method has a synchronous and asynchronous
form (`mean` / `amean`, …):

| Rolling / Expanding terminals | Purpose |
| --- | --- |
| `sum(column)` / `mean(column)` | Rolling/expanding sum / mean per group |
| `min(column)` / `max(column)` | Minimum / maximum |
| `count(column)` | Non-null count |
| `std(column, ddof=1)` / `var(column, ddof=1)` / `sem(column, ddof=1)` | Sample stddev / variance / standard error |
| `quantile(column, q=0.5, interpolation='linear')` / `median(column)` | Quantile / median |
| `rank(column, method='average', ascending=True, pct=False)` | Rank within the window |
| `skew(column)` / `kurt(column)` | Skewness / excess kurtosis |
| `nunique(column)` | Distinct count |
| `first(column)` / `last(column)` | First / last value in the frame |
| `agg(column, funcs)` | Multi-aggregation in one call |

!!! note
    Grouped EWM (`ewm`) uses the same blockwise SQL engine as ungrouped EWM,
    partitioned by the group columns. See the EWM section below.

## Usage Overview

```python
# ctx is defined in the Example Data setup

sample = ctx.groupby("region").rolling(3, order_by="date").mean("sales")
sample = ctx.groupby("region").expanding(order_by="date").sum("sales")
sample = ctx.groupby("region").rolling(3, order_by="date").agg("sales", ["sum", "mean"])
```

```python
# ctx is defined in the Example Data setup

sample = await ctx.groupby("region").arolling(
    column="sales", window=3, func="mean",
    group_cols="region", order_by="date",
)
sample = await ctx.groupby("region").aexpanding(
    column="sales", func="sum", group_cols="region", order_by="date",
)
```

`order_by` is optional — omit it to use the natural `_mf_idx` order inside each group:

```python
sample = ctx.groupby("region").rolling(3).mean("sales")
sample = ctx.groupby("region").expanding().sum("sales")
```

## Common Parameters

See [Rolling Reference](../window.md#rolling-reference) for the full window-form,
offset-alias, `center`/`closed`, and grouped-rolling matrix.

| Parameter | Type | Description |
| --- | --- | --- |
| `column` | `str` | Value column the window runs over. |
| `group_cols` | `str` or `list[str]` | Column(s) partitioning the window. At least one is required. |
| `window` | `int`, `timedelta`, or duration `str` | Rolling window: rows (`window-1 PRECEDING AND CURRENT ROW`) or a fixed duration (`"3D"`, `"36h"`, `timedelta(days=3)`) with a datetime `order_by`. `BaseIndexer` is not supported. |
| `func` | `str` or `list[str]` | Aggregation(s): `sum`, `mean`/`avg`, `min`, `max`, `count`, `std`/`stddev`/`stddev_samp`, `var`/`variance`/`var_samp`, `quantile`, `median`, `sem`, `rank`, `skew`, `kurt`, `nunique`, `first`/`first_value`, `last`/`last_value`. |
| `order_by` | `str`, `list[str]`, or `None` | Row ordering inside each group (the equivalent of the `on` parameter). `None` (default) uses the immutable upload order (`_mf_idx`). Time windows require one datetime column. |
| `q` | `float` | Quantile level for `quantile`/`median` (default `0.5`). |
| `func_kwargs` | `dict` or `None` | Per-function keyword arguments (`ddof`, rank `method`/`ascending`/`pct`, quantile `interpolation`/`q`). |
| `center` | `bool` | Rolling only — centre the frame on the current row (default `False`). |
| `closed` | `str` or `None` | Rolling only — `right` (default), `left`, `both`, `neither`. |
| `win_type` | `str` or `None` | Rolling only — scipy window weights (`boxcar`, `triang`, `bartlett`, `parzen`, `bohman`, `blackman`, `blackmanharris`, `barthann`, `nuttall`, `hann`, `hamming`, `cosine`) for weighted `sum`/`mean`. See [`scipy.signal.windows`](https://docs.scipy.org/doc/scipy/reference/signal.windows.html). |
| `min_periods` | `int` or `None` | Minimum observations before a value is produced. Rolling default `None` → `window` for row windows and `1` for time windows; expanding default `1`. Must satisfy `0 <= min_periods`; row windows also require `<= window` (time windows have no upper bound). |
| `step` | — | Not exposed by memFrame; passing it raises `TypeError`. |
| `method` | — | Not exposed by memFrame; passing it raises `TypeError`. |
| `new_table` | `str` or `None` | Kept for compatibility; the result is written to the source and `new_table` resolves to the source table. |
| `map_feature` | `bool` | Deprecated/ignored. Generated columns are always written back to the source; the envelope carries `mapped_table` / `mapped_columns`. `new_table` is the source table (the ref target). |

With no `order_by`, each group is ordered by the immutable upload index
(`_mf_idx`) and the window runs in that natural order. With an explicit
`order_by`, rows are grouped first, then ordered inside each group by
`order_by..., _mf_idx`; the additional `_mf_idx` key makes ties deterministic.

### How grouped windows run internally

```sql
-- no order_by
AVG("sales") OVER (PARTITION BY "region"
                   ORDER BY "_mf_idx"
                   ROWS BETWEEN 2 PRECEDING AND CURRENT ROW)

-- order_by="date"
AVG("sales") OVER (PARTITION BY "region"
                   ORDER BY "month", "_mf_idx"
                   ROWS BETWEEN 2 PRECEDING AND CURRENT ROW)
```

- **Partition first, order second.** `group_cols` splits the table into
  independent groups; only then does the window walk each group's rows in
  `order_by` order. A group's values never depend on rows from another group,
  and ordering inside one group never interleaves another group's rows.
- **No `order_by` is not "unordered".** The order key is always `_mf_idx`,
  the immutable index assigned at upload. It survives sorts and merges, so a
  no-order grouped window is deterministic and equivalent to pandas'
  `groupby(...).rolling(...)` on the frame in its upload row order.
- **`order_by` never reorders the output.** Each generated value is joined
  back to its source row by `_mf_idx`, so the result is returned in upload
  order even when the window itself walked the rows by `month`. `order_by`
  only decides which rows fall into each frame (and, with `center`/`closed`,
  where the frame edges sit). Sort the result yourself for display.
- **Ties** (`first`/`last`/`rank`, identical timestamps) are broken by
  `_mf_idx`, i.e. by upload order.

Result columns are named
`<column>_rolling_<stat>_w<window>_by_<groups>` (rolling) or
`<column>_expanding_<stat>_by_<groups>` (expanding). `<window>` is the row
count, or the duration tag for time windows (`w259200s`); `<stat>` is the
function label (`mean`, `q0.5`, `median`, `skew`, …). Duplicate requests get
a numeric suffix (`_2`, …).

## Multicolumn Group-By

Pass several group columns to partition by their combination — the
window restarts at every distinct key tuple:

```python
result = ctx.groupby("region", "date").rolling(3, order_by="date").sum("sales")
# column: sales_rolling_sum_w3_by_region_month
```

```python
result = await ctx.groupby("region", "date").aexpanding(
    column="sales", func="mean",
    group_cols=["region", "month"], order_by="date",
)
```

Direct calls accept the same shapes — a single name or a list:

```python
result = ctx.arolling(
    column="sales", window=3, func=["sum", "mean"],
    group_cols=["region", "month"], order_by="date",
)
```

Multi-aggregation (`func` as a list, or builder `.agg(column, funcs)`)
chains every requested function into **one** cache table, each
reading the previous call's output:

```python
result = ctx.groupby("region").rolling(3, order_by="date").agg(
    "sales", ["sum", "mean", "std"]
)
# columns: sales_rolling_sum_w3_by_region,
#          sales_rolling_mean_w3_by_region,
#          sales_rolling_std_w3_by_region
```

## Rolling

Each rolling call aggregates a frame partitioned by the group columns and
ordered by `<order>`. The default row frame is
`ROWS BETWEEN window-1 PRECEDING AND CURRENT ROW`; `center`/`closed` shift or
open the edges, and time windows use `(t - window, t]` (or
`(t - window/2, t + window/2]` when centred) instead of row offsets. The
result column(s) are merged back into the source.

`min_periods` defaults to `window`, so the first `window - 1`
rows of each group are `NULL`. Pass `min_periods=1` to aggregate the partial
window instead:

```python
result = ctx.groupby("region").rolling(3, order_by="date").mean("sales")
result = ctx.groupby("region").rolling(3, order_by="date", min_periods=1).mean("sales")
result = ctx.groupby("region").rolling(7, order_by="date").quantile("sales", q=0.9)
```

`center=True` shifts the frame forward `(window - 1) // 2` rows and `closed`
selects the included edges (`right` default, `left`, `both`, `neither`); both
apply per group. `closed="neither"` needs `window >= 2`.

`std`/`var`/`sem` take `ddof` (default `1`, matching pandas), and `rank` takes
`method` (`average` default, `min`, `max`, `first`, `dense`), `ascending`, and
`pct`:

```python
ctx.groupby("region").rolling(7, order_by="date").std("sales", ddof=0)
ctx.groupby("region").rolling(7, order_by="date").rank("sales", method="dense")
ctx.groupby("region").rolling(7, order_by="date").quantile(
    "sales", q=0.9, interpolation="higher",
)
```

`win_type` applies scipy window weights per group for weighted `sum`/`mean`:

```python
ctx.groupby("region").rolling(7, order_by="date", win_type="triang").mean("sales")
```

Passing a duration (`window="3D"` / `timedelta`) windows by time per group
instead of by row count; a datetime `order_by` is required and `min_periods`
defaults to `1`. `closed` (`right`/`left`/`both`/`neither`), `center`, and
every aggregation (including the specials, `skew`/`kurt`, and
`first`/`last`) work per group too:

```python
ctx.groupby("region").rolling("3D", order_by="date").mean("sales")
ctx.groupby("region").rolling("3D", order_by="date").quantile("sales", q=0.9)
ctx.groupby("region").rolling("2D", order_by="date", closed="both").sum("sales")
ctx.groupby("region").rolling("2D", order_by="date", center=True).sum("sales")
```

`win_type` remains unsupported for time windows.

`func` also accepts backend spellings directly (`AVG`, `STDDEV_SAMP`,
`first_value`, …). Unknown names and functions unsupported for the detected
dtype are skipped (`skipped_funcs`, `is_partial True`); a call only fails when
no requested function can run.

## Expanding

Expanding mirrors rolling with an unbounded frame (first row of each
group through the current row) plus `min_periods`:

```python
result = ctx.groupby("region").expanding(order_by="date").sum("sales")
result = ctx.groupby("region").expanding(order_by="date", min_periods=3).mean("sales")
```

## EWM

Grouped EWM runs the same blockwise SQL engine as the ungrouped `ewm`,
partitioned by the group columns:

```python
result = ctx.groupby("region").ewm(span=3, order_by="date").mean("sales")
result = ctx.groupby("region").ewm(
    span=3, order_by="date", adjust=False,
).agg("sales", ["mean", "var"])
```

- `order_by=None` uses natural upload order (`_mf_idx`) **inside each group**.
- Explicit `order_by` is applied per group, with `_mf_idx` as a deterministic
  tie-breaker.
- Supported functions: `mean`, `sum`, `std`, `var`; options: `com`, `span`,
  `halflife`, `alpha`, `adjust`, `ignore_na`, `min_periods`.
- Values follow the unified ungrouped contract:
  - `adjust=False` `sum` equals the EWMA state (same values as `mean`);
  - `adjust=False` `var/std` need two valid points, so the first row is NULL;
  - `adjust=True` + `ignore_na=False` decays state across null rows.
- Result columns: `<column>_ewm_<func>_by_<groups>`.

## Specials: quantile, median, sem, rank, nunique, skew/kurt

These share the builder signatures above:

- `quantile` / `median`: `linear` uses the native window on DuckDB and a
  correlated `PERCENTILE_CONT … WITHIN GROUP` (PostgreSQL) /
  `quantile(q)(…)` (ClickHouse) elsewhere. Non-linear interpolations index the
  sorted frame (`array_agg` + subscript on PostgreSQL/DuckDB,
  `arraySort`/`arrayElement` on ClickHouse). `median` is the linear `0.5`
  quantile.
- `sem`: sample standard deviation over `sqrt(count - ddof)`
  (`stddevSamp` on ClickHouse).
- `rank`: correlated `COUNT(*)` subqueries on all backends; supports
  `method` / `ascending` / `pct`.
- `nunique`: DuckDB windowed `COUNT(DISTINCT …)`; PostgreSQL and ClickHouse
  use a correlated distinct count (`countDistinct` on ClickHouse).
- `skew` / `kurt`: closed-form moment subqueries on all backends.
- `first` / `last`: `FIRST_VALUE`/`LAST_VALUE` for row windows; time windows
  read the earliest/latest row from the narrow base and keep the column type.
- Datetime `mean`/`median`/`mode` aggregate the epoch value and convert back.

With a time window (`window="3D"`) all of these run over the epoch frame
`(t - window, t]` (plus `closed`/`center` rules) instead of row offsets.

The orchestrator detects the value column dtype (numeric / datetime /
categorical) and maps each requested function to the dtype-appropriate
engine method; datetime columns support `min`/`max`/`mean`/`median`/`mode`/
`count`/`nunique`/`rank`/`first`/`last`. `min`/`max`/`first`/`last` use the
raw values; `mean`/`median`/`mode` aggregate epoch seconds and convert back.

## Source Write-Back

Every grouped window call writes its generated column(s) back to the source
table in place: PostgreSQL/DuckDB add the columns and backfill them with one
`UPDATE` keyed by `_mf_idx`; ClickHouse builds a narrow feature table and
swaps it in with `EXCHANGE TABLES`. The response carries a preview DataFrame
and `@record_call` stores a ref to the source.

```python
result = ctx.groupby("region").rolling(3, order_by="date").agg(
    "sales", ["mean"]
)
# source now has: region, month, sales, sales_rolling_mean_w3_by_region
# result (preview) has: sales, region, month, sales_rolling_mean_w3_by_region
```

Rules:

- A repeated identical call reuses the same generated column(s); the source
  keeps exactly one copy. `deep_cache=False` recomputes but writes no ref row.
- The `map_feature` parameter is retained for compatibility but ignored:
  write-back is always on.
- If the column name already exists from anything else, the call fails and
  the source is left unchanged — drop or rename it first.
- Write-back keeps the original table name: DuckDB/PostgreSQL backfill the
  source in place; ClickHouse atomically exchanges the feature table in.

## Return Values and Errors

Calling the builder (or the wrapper class) directly returns the raw
operation envelope; single-function calls also carry `new_column`:

```python
{
    "is_error": False,
    "message": "Rolling mean on 'sales' grouped by ['region'] (window=2)",
    "result": <pandas.DataFrame>,
    "new_table": "ab12CD",                     # source table (ref target)
    "new_columns": ["sales_rolling_mean_w2_by_region"],
    "new_column": "sales_rolling_mean_w2_by_region",   # single-function calls only
    "window": 2,
    "group_cols": ["region"],
    # multi-function calls also include:
    # "successful_funcs", "skipped_funcs", "failed_funcs", "is_partial", "dtype"
}
```

On a `ContextManager`, public methods return the resulting **DataFrame**
directly; invalid operations surface as `OperationError` when unwrapped.
An unknown function, a function unsupported for the detected dtype, or a
missing column fails the call (`message == ""` on the raw envelope).

## Generated Tables

Every grouped window operation writes its generated column(s) to the source
upload table; the deep cache stores a ref to the source:

- **PostgreSQL / DuckDB:** grouped rolling/expanding compute a narrow
  `(_mf_idx, <windows>)` feature query and backfill the source with one
  `UPDATE` keyed by `_mf_idx`. The source is never cloned or swapped.
  Grouped time-window `sum`/`mean`/`min`/`max`/`count`/`std`/`var` use native
  `RANGE` plus a current-timestamp prefix, preserving row-order tie handling.
  Specials (quantile/median, sem, rank, nunique, skew/kurt, first/last,
  datetime) use the same backfill over a narrow ordered base; specials without
  a portable native form retain the correlated fallback.
- **ClickHouse:** grouped rolling/expanding build a narrow feature table,
  join it to the source by `_mf_idx`, and atomically swap it in with
  `EXCHANGE TABLES`; the temp feature/wide tables are dropped.

Operations apply `@record_call`, so repeat calls are recorded in the
cache registry.

## Backend Behavior

Grouped windows support DuckDB, PostgreSQL, and ClickHouse:

- Identifiers are sanitized and quoted before SQL is generated.
- `group_cols` and `order_by` may each be a single column, a list, or
  (for `order_by`) omitted for natural `_mf_idx` order inside each group.
- Backend dialect differences (sample stddev/variance names,
  quantile/nunique/rank execution shapes, datetime epoch handling) are
  handled per backend.
- The per-backend SQL is locked by
  `tests/unit/test_groupby_window_sql_fingerprint.py` (17 scenarios × 3
  backends).

## Errors

Grouped window methods raise `OperationError` (once unwrapped) for
validation or backend failures:

- No group-by column, no aggregation function, or an invalid function entry.
- Unknown rolling function, or a function unsupported for the detected dtype.
- Missing or renamed `column` / `order_by` / group column (raised out of
  dtype detection).
- Missing `backend` / `data_id` at the ops layer, or an unsupported backend.

## Large datasets

For 10M+ rows:

- Use a file-backed DuckDB (`db_path="memframe.duckdb"`), not `:memory:`.
- Pass `temp_directory` and `memory_limit` so DuckDB can spill instead of
  exhausting Colab RAM:

```python
mf = MemFrame(
    connection_type="local",
    connection_params={
        "db_path": "memframe.duckdb",
        "temp_directory": "/content/duckdb_tmp",
        "memory_limit": "8GB",
    },
    preview_rows=1000,
)
```

- `preview_rows` caps the returned DataFrame (default `10,000`). The generated
  column still covers every source row; the source (and cache ref) holds the
  full result.
- Group-by cumulative and grouped-window operations update the source in
  place and cache a source ref, so no wide result copy is materialized.
- Benchmark the cold path with:

```bash
uv run python scripts/bench_groupby_memory.py --op cumsum --rows 5000000 --preview-rows 10
```

## API Reference

::: memframe.wrappers.analytix.groupby_window.GroupByWindowStatsWrapper
    options:
      show_root_heading: true
      show_root_full_path: true
      members:
        - arolling
        - rolling
        - aexpanding
        - expanding
        - aewm
        - ewm
        - groupby

::: memframe.wrappers.analytix.groupby_window.GroupByWindowWrapper
    options:
      show_root_heading: true
      show_root_full_path: true
      members:
        - rolling
        - expanding
        - ewm

::: memframe.wrappers.analytix.groupby_window.GroupByRollingWrapper
    options:
      show_root_heading: true
      show_root_full_path: true

::: memframe.wrappers.analytix.groupby_window.GroupByExpandingWrapper
    options:
      show_root_heading: true
      show_root_full_path: true

::: memframe.wrappers.analytix.groupby_window.GroupByEWMWrapper
    options:
      show_root_heading: true
      show_root_full_path: true
