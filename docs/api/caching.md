# Caching

Source: `src/memframe/cache/cache_manager.py`

memFrame records analytical method calls in a per-backend **cache registry**
and replays them when the same call is repeated with the same arguments and
the source data is unchanged. Results fall into two kinds:

| Kind | What it is | Stored as |
| --- | --- | --- |
| `ref` | A feature/projection that lives in a table (row-preserving generated columns, selected columns, feature-mapped group results) | Pointer: `ref_schema`, `ref_table`, `involved_cols`, `generated_cols`, `result_cols` |
| `actual` | A standalone result that has no table (aggregate-only group-by, scalar, dict, list, correlation matrix, describe report) | Payload in `memframe_cache_payloads` |

A call is one kind or the other; the registry row carries `cache_kind`.

## The `record_call` decorator

`record_call` is a `CacheManager` instance (`from memframe.cache import
record_call, CacheManager`). Orchestrator methods that return reusable results
are decorated with it:

```python
@record_call(deep_cache=True)   # record and replay
@record_call(deep_cache=False)  # record only (no replay)
@record_call                    # decorator default, then MemFrame setting
```

Resolution order:

1. Decorator argument, e.g. `@record_call(deep_cache=True)`.
2. `mf.connect(deep_cache=...)` / `MemFrame(deep_cache=...)`.
3. `False`.

`deep_cache=False` disables replay for that call. It does not change the
operation's data semantics.

## Ref results — no second table

For an Out_2 operation (arithmetic, cleaning, datetime, transform, cumulative,
window, grouped cumulative/window, comparison, `select_dtypes`,
`datetime_diff`), the generated columns are written into the active source
table. The cache row stores only a pointer:

```text
cache_kind = 'ref'
ref_schema = 'memframe_upload'
ref_table  = '<source table>'
involved_cols = '["salary"]'
generated_cols = '["cleaned_salary_mean_filled"]'
result_cols = '["salary","cleaned_salary_mean_filled"]'
```

A hit selects `result_cols` from `ref_table`. No cache copy is created and no
working table is relocated. Row-aligned window/grouped ops use
`publish_row_aligned`, which writes the wide result's feature columns into
the source (in place on PostgreSQL, swap on DuckDB/ClickHouse) and then
records the same source ref.

`owns_table` is `false` for source refs. Cache clear/delete never drops a
source table.

Filtering and sorting also use refs, with a small selection/permutation table
in `memframe_cache`:

```text
order_kind  = 'filter' | 'sort'
order_table = '<base>__op_<n>'
```

The order table holds only `(position, _mf_idx)`. Replay joins the source to
it and orders by `position`, so no filtered or sorted copy of the data is
stored. `filter(..., create_flag=True)` additionally writes the flag column
into the source and bumps the epoch.

### Group-by operations

The cache kind follows whether the operation writes to the source:

- **Aggregate only** (`map_feature=False`): the group table is the final
  result. It is cached as `actual` and the temporary group table is dropped.
- **Feature mapped** (`map_feature=True`): the feature columns are added to
  the source. The cache stores a source `ref` with the generated columns; the
  intermediate group table is dropped.
- **`groupby().fillna()`**: always writes `<col>_filled` into the source, so
  it is always a source `ref`.

A registered dataset base table (`<data_id>__base`) is never relocated into
the cache schema, even for unannotated operations.

## Actual results — payload table

Results that are not tied to a table are serialized into
`memframe_cache_payloads`:

```text
data_id, opidx, format, payload, created_at
```

- DataFrame → `format='parquet'` (the index is preserved; correlation matrices
  keep their row/column labels).
- dict / list / scalar → `format='json'` with a typed envelope that preserves
  datetimes, dates, timedeltas, decimals, bytes, non-string dict keys, and NaN.

A hit deserializes the payload and returns the original type. `describe`,
`info`, `null_analysis`, `corr`, `cov`, and all scalar/dict/list stats use
this path.

## Validity

Each row stores:

- `base_fingerprint` — row count + column set of the source table.
- `epoch` — the dataset's `cache_epoch` at store time.
- `input_refs` — optional cross-table dependencies.

Every executed mutating operation bumps `memframe_csv_registry.cache_epoch`.
A lookup is a hit only when the fingerprint matches and the stored epoch is
the current epoch. Actual results use the same rules.

## Registry

The registry lives in the backend `memframe_csv_registry` schema
(DuckDB/PostgreSQL) or database (ClickHouse):

| Backend | Registry table | Payload table | Marker table | Cache schema/database |
| --- | --- | --- | --- | --- |
| DuckDB | `memframe_csv_registry.memframe_cache_registry` | `memframe_csv_registry.memframe_cache_payloads` | `memframe_csv_registry.memframe_cache_markers` | `memframe_cache` |
| PostgreSQL | `memframe_csv_registry.memframe_cache_registry` | `memframe_csv_registry.memframe_cache_payloads` | `memframe_csv_registry.memframe_cache_markers` | `memframe_cache` |
| ClickHouse | `memframe_csv_registry.memframe_cache_registry` | `memframe_csv_registry.memframe_cache_payloads` | `memframe_csv_registry.memframe_cache_markers` | `memframe_cache` |

The fixed schemas/databases are `memframe_csv_registry`, `memframe_cache`, and
`memframe_upload`. ClickHouse payloads are stored base64-encoded in a `String`.

### Internal markers

`window_map`, `groupby_map`, `groupby_cum_map`, and `groupby_win_map` rows
protect reruns from dropping columns they did not create. They are not cache
results, so they live in `memframe_cache_markers`:

```text
data_id, operation_type, class_name, method_name,
source_schema, source_table, sig, created_at
```

The cache registry therefore contains only `operation_type = 'method_call'`
rows. Legacy marker rows found in an older registry are moved to the marker
table on connect.

## Clearing

`aclear_cache(data_id)`:

1. Drops tables the cache owns (`owns_table = true`, legacy cache copies).
2. Never drops a `ref_table` that is the user's source table.
3. Deletes the dataset's actual payload rows and internal markers.
4. Deletes its registry rows.

`adelete_table(data_id)` performs the same cache cleanup and then removes the
dataset itself.

Internal operation markers (`window_map`, `groupby_map`, `groupby_cum_map`,
`groupby_win_map`) are never treated as cache-owned tables, so clearing cache
cannot drop `<data_id>__base` or its public view. Failed operations are not
recorded in the cache registry.

Result tables created by an operation (`new_table=...`, `owns_table = true`)
are kept in `memframe_cache`, never in `memframe_upload`.

## Debugging hit/miss

The cache logs through the `memFrame.cache` logger:

```text
[cache] MISS ArithmeticWrapper.mul data_id=HW4dev — no usable registry row
[cache] STORE ArithmeticWrapper.mul data_id=HW4dev → memframe_upload.HW4dev (ref)
[cache] HIT  StatsWrapper.mean data_id=HW4dev → actual payload (0.002s)
[cache] STALE StatsWrapper.mean data_id=HW4dev — base changed; dropping entry
```

`result_metadata` on the returned envelope reports `from_cache` and
`cache_kind` (`"ref"` or `"actual"`).
