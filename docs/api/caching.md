# Caching

Source: `src/cache/cache_manager.py`

memFrame caches analytical method calls in a per-backend **transient registry**
so repeat work is either recorded (cheap signature logging) or fully reused
(deep cache reloads a saved result table without re-executing the method).

## The `record_call` decorator

`record_call` is a `CacheManager` instance (`from memframe.cache import
record_call, CacheManager`). Every analytics orchestrator method (`selection`,
`inspection`, `cleaning`, `stats`, `arithmetic`) is decorated with it. The
decorator wraps the method, records the call into the `transient_registry`
table, and — for deep cache — persists the returned DataFrame as a transient
table.

```python
@record_call                    # signature-only logging (default)
@record_call()                  # same as above
@record_call(deep_cache=False)  # explicit signature-only
@record_call(deep_cache=True)   # persist result table + reload on repeat call
```

## How caching works

### Resolution order

The effective cache mode for a call is decided in this order:

1. The decorator argument, e.g. `@record_call(deep_cache=True)`.
2. `MemFrame(deep_cache=...)` when the decorator does not specify one.
3. `False` (signature-only) as the default.

`MemFrame(deep_cache=False)` is the master switch: it overrides decorator
requests and disables deep caching for the whole session.

### Level 1 — signature-only (default)

Every call records its identity into `transient_registry`:

- `data_id` — the dataset the call ran on
- `opidx` — monotonically increasing operation index per dataset
- `class_name` / `method_name` — the decorated method
- `args` / `kwargs` — canonical JSON-serialized call arguments (keys sorted,
  numpy/pandas scalars coerced), so argument order in kwargs does not affect
  cache-key equality
- `generated_table_name`, `schema`, `is_deep_cache`

On a repeat call with identical arguments, the method **re-executes**. Any table
the method created is dropped — signature-only mode never persists results.

### Level 2 — deep cache

When deep caching is active, a successful result (a dict with `result` holding a
DataFrame) is written to a **typed** transient table (column types are inferred
from the DataFrame's Arrow schema, so cached results keep their dtypes):

```
{transient_schema}.{data_id}_{opidx}
```

On a repeat call with identical arguments, the registry lookup finds the saved
table and the decorator returns a cache-hit response **without executing the
method**:

```python
{
    "is_error": False,
    "message": "Cache hit for SelectionOrchestrator.where; reused generated table '...'",
    "generated_cols": [...],
    "result": <DataFrame from saved table>,
    "new_table": "<saved table name>",
    "result_metadata": {
        "from_cache": True,
        "saved_table": "<saved table name>",
        "row_count": N,
        "column_count": M,
        "strict_args_kwargs_match": True,
    },
}
```

### Table relocation

Methods that generate a table (e.g. a cleaned dataset) normally produce it in
the upload schema. With deep caching, that table is moved into
`transient_schema` so it can be reloaded on later calls and cleaned up with
`aclear_cache`. The relocation mechanism is backend-specific:

- DuckDB — `CREATE TABLE transient AS SELECT * FROM upload...` then `DROP`.
- PostgreSQL — `ALTER TABLE upload... SET SCHEMA transient`.
- ClickHouse — `RENAME TABLE upload... TO transient...`.

### Cache invalidation

`aclear_cache(data_id)` drops every transient table recorded for a dataset and
clears its registry rows. The original uploaded table is left untouched.

## Backend registry

The transient registry lives in the backend registry schema:

| Backend | Registry table | Transient schema |
| --- | --- | --- |
| DuckDB | `registry.transient_registry` | `transient` |
| PostgreSQL | `registry.transient_registry` | `transient` |
| ClickHouse | `registry.transient_registry` | `transient` |

With a `schema_prefix`, registry and transient schemas are namespaced
accordingly (see [Connector](connector.md)).

## Cache-key and registry index

Lookups filter `transient_registry` by `data_id`, `class_name`, `method_name`,
`args`, `kwargs`. DuckDB and PostgreSQL backends install an index on
`(data_id, class_name, method_name)` so repeated lookups do not scan the whole
registry.

## Debugging hit/miss

The cache logs every lookup through the `memFrame.cache` logger:

```
[cache] MISS ArithmeticWrapper.mul data_id=HW4dev — no matching registry row
[cache] STORE ArithmeticWrapper.mul data_id=HW4dev → transient.HW4dev__op_... (deep)
[cache] HIT  ArithmeticWrapper.mul data_id=HW4dev → HW4dev__op_... (3 rows, lookup+reload 0.013s)
```

- `MISS ... no matching registry row` — deep lookup found nothing (different
  args, or deep caching off for this call). If this happens on a repeat call
  that just logged `STORE ... (deep)`, the registry filter is the problem:
  ClickHouse stores `is_deep_cache` as `Bool`, which stringifies to
  `'true'`/`'false'`, so the lookup must compare it as `CAST(is_deep_cache AS
  UInt8) = 1`, not `= '1'`.
- `MISS ... reload failed for <table>` — registry row existed but the saved
  table could not be reloaded (warning level; the underlying error is logged).
- `HIT ...` — a saved table was reloaded; the timing includes the registry
  lookup plus the table fetch.
- `STORE ... (deep)` — a result was persisted to a transient table.
- `STORE ... (signature-only)` — L1 mode recorded the call only.

The `result_metadata.from_cache` flag on the returned dict also reports hit vs
miss programmatically.
