# Immutable Dataset Index

Source: `src/memframe/core/indexing.py`

Every memFrame-owned dataset carries an internal positional column:

```text
_mf_idx
```

It is a frozen row identity used by caches, ordering, alignment and
update joins. It is **not** a user-facing column and it is **not** a
database index.

## Rules

- Generated once at ingestion (or dataset creation).
- Never updated, renamed, dropped, or used as a write target.
- Row identity never changes.
- Derived columns may be added; values may be updated.
- Filtering/sorting may return subsets/permutations that retain the source
  `_mf_idx`.
- New datasets (merge, reshape, `return_ctx`) get a fresh `0..n-1`
  identity space.
- `_mf_idx` is hidden from public DataFrames and metadata.

## Dtype

Chosen from the row count at creation:

| Rows | Logical dtype | DuckDB | PostgreSQL | ClickHouse |
| --- | --- | --- | --- | --- |
| ≤ 255 | `uint8` | `UTINYINT` | `SMALLINT` | `UInt8` |
| ≤ 65,535 | `uint16` | `USMALLINT` | `INTEGER` | `UInt16` |
| ≤ 4,294,967,295 | `uint32` | `UINTEGER` | `BIGINT` | `UInt32` |
| larger | `uint64` | `UBIGINT` | `NUMERIC(20,0)` | `UInt64` |

Metadata is stored in `memframe_csv_registry`:

```text
mf_idx_dtype, mf_idx_max, mf_idx_source_id, mf_idx_ready
```

## Hidden from results

memFrame’s public read paths, metadata, statistics, masks and cache hits strip
`_mf_idx`. User operations cannot reference it:

- upload columns / `dtypes`
- `rename`, `drop`, `insert`, `update`, `map`
- arithmetic / cumulative `target_col`
- `sort_values(by=...)`
- filter/groupby predicates

Uploads create:

```text
memframe_upload.<data_id>__base   physical table (includes _mf_idx)
memframe_upload.<data_id>        public view (visible columns only)
```

`memframe_csv_registry.table_name` points at the public view;
`base_table`/`base_schema` point at the physical table for internal ops.
A raw `SELECT * FROM memframe_upload.<data_id>` therefore never returns
`_mf_idx`. Querying the `__base` table directly still exposes it; that is the
internal source used by memFrame.

When an operation adds generated columns, the public view is re-created so
those columns remain visible through raw SQL as well.

On PostgreSQL, generated columns are written into the base with
`ALTER TABLE ... ADD COLUMN` + `UPDATE ... FROM` keyed on `_mf_idx`. The base
relation is never dropped, so the public view stays valid throughout.

## Derived datasets

Merge, reshape, and `sort_values(..., return_ctx=True)` create a new
dataset rather than modifying the source. Each derived dataset gets:

- a fresh `0..n-1` identity space, regardless of the inputs' indices;
- its own physical `<data_id>__base` table;
- a public `<data_id>` view with only user-visible columns.

The registry's `mf_source_id` records the dataset the derived one came from.
Joining datasets never carries the sources' `_mf_idx` into the result; the
join selects only visible columns.

## SyncDB / external tables

`register_tables()` does not touch external tables. `set_active(data_id)` adds
and populates `_mf_idx` if missing, updates metadata, and is idempotent when
the column already exists.

## Sorting

`sort_values` leaves the source untouched. With deep cache enabled it stores a
write-once sorted snapshot in `memframe_cache` and records a ref with
`owns_table=true` and no order table; a cache hit scans the snapshot directly.
Without deep cache it stores a `(position, <idxdtype>)` permutation and joins
the source on replay:

```sql
(position <posdtype>, mf_idx <idxdtype>)
```

The permutation ref records `order_schema`, `order_table`,
`order_position_dtype`, `order_kind='sort'` and `order_limit`.

`sort_values(..., return_ctx=True)` instead materializes the sorted
result as a new upload dataset with a fresh `_mf_idx` and returns its
`ContextManager`. The original dataset is left untouched. The new dataset is
registered in `memframe_csv_registry`; this path bypasses the cache, so each
call creates a fresh context.

## Ordering

Operations that need the original row order use:

```sql
ORDER BY _mf_idx
```

Physical row order (`rowid`, `ctid`, storage order) is never trusted. Update
joins in window/cumulative/grouped-cumulative operations also key on
`_mf_idx`, so they survive DuckDB rewrites, PostgreSQL `VACUUM FULL`/`CLUSTER`
and ClickHouse merges.
