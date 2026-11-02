# Database Indexes

memFrame manages **real database indexes**. There is no pandas-style logical
index and no index metadata in `memframe_csv_registry`: the backend catalog is
the source of truth. Indexes are created, listed, and dropped with ordinary
DDL on the active dataset's table.

```python
ctx.create_index(columns=["month"], name="idx_month")
ctx.list_indexes()
ctx.drop_index("idx_month")
```

Every method has an asynchronous twin: `acreate_index`, `adrop_index`,
`alist_indexes`. Index operations are **not cached** and do not create
cache tables. Dropping a dataset/table drops its indexes with it; synced
external tables keep their indexes.

## Public API

| Synchronous | Asynchronous | Purpose |
| --- | --- | --- |
| `create_index(columns=None, name=None, unique=False, if_not_exists=True, ...)` | `await acreate_index(...)` | Create a backend-native index |
| `drop_index(name, if_exists=True, concurrently=False)` | `await adrop_index(...)` | Drop an index |
| `list_indexes()` | `await alist_indexes()` | Read the backend catalog |
| `set_order_by(columns, primary_key=None, dry_run=True, ...)` | `await aset_order_by(...)` | ClickHouse-only physical ORDER BY rebuild |

Backend-specific options are documented below. Unsupported options for a
backend return an `OperationError` rather than being ignored.

## DuckDB

DuckDB has two index types:

- **Min-max (zonemap)** indexes are created automatically for all general
  columns. You do not need to create them.
- **ART** indexes speed up point and highly selective (`< 0.1%`) lookups.
  They are persisted on disk, but must fit in memory during creation. They
  slow down loads and updates.

`create_index` creates an ART index. DuckDB has no `CONCURRENTLY`, partial
indexes, covering `INCLUDE`, or index methods.

```python
ctx.create_index(columns=["customer_id"], name="idx_customer")
ctx.create_index(columns=["email"], name="idx_email", unique=True)
ctx.create_index(columns=["tenant_id", "created_at"], name="idx_tenant_time")
ctx.create_index(columns=["email"])          # generated name: <table>_email_idx
```

- `UNIQUE` is enforced by DuckDB for existing and future rows.
- Index names are schema-scoped. `CREATE INDEX` cannot schema-qualify the name;
  the index is created in the table's schema. `DROP INDEX` is schema-qualified.
- `IF NOT EXISTS` still attempts the build before checking existence, so
  memFrame checks the catalog first.

## PostgreSQL

PostgreSQL supports real secondary indexes: B-tree, hash, GIN, GiST, BRIN,
composite keys, uniqueness, partial predicates, and covering columns.

```python
ctx.create_index(columns=["tenant_id", "created_at"], name="idx_tenant_time")
ctx.create_index(columns=["email"], name="idx_email", unique=True)
ctx.create_index(
    columns=["customer_id"], name="idx_active_customer",
    where="status = 'active'",          # partial index
    include=["name"],                   # covering index
)
ctx.create_index(
    columns=["created_at"], name="idx_created_brin",
    method="brin",
)
ctx.create_index(
    columns=["customer_id"], name="idx_customer_concurrent",
    concurrently=True,                   # avoids blocking writes
)
```

- **Column order matters.** Put equality columns first, then range/sort
  columns. An index on `(a, b, c)` serves `a`, `a,b`, and `a,b,c`; it does not
  serve `b` alone on pre-PG18.
- `method` accepts `btree` (default), `hash`, `gin`, `gist`, `brin`.
- `where` is a raw SQL predicate. It must match the query predicate for the
  planner to use the index.
- `include` adds non-key columns for index-only scans.
- `CONCURRENTLY` cannot run inside a transaction. memFrame does not wrap index
  operations in a transaction. A cancelled/failed concurrent build can leave an
  INVALID index; `list_indexes()` reports `valid`.
- Index names are schema-scoped and limited to 63 bytes. Generated names are
  truncated with a hash suffix.
- Constraint-backed indexes (PRIMARY KEY/UNIQUE) are listed but cannot be
  dropped with `DROP INDEX`; drop the constraint instead.
- Avoid duplicate indexes and over-indexing; monitor usage and invalid indexes.

## ClickHouse

ClickHouse has **no conventional secondary B-tree index**. The sparse primary
index is the table's physical `ORDER BY`, and **`ORDER BY` is immutable after
table creation**. A new `ORDER BY` requires rebuilding the table.

### Projection indexes (default)

On ClickHouse, `create_index` creates a **projection index** by default:

```python
ctx.create_index(columns=["user_id"], name="proj_user")
ctx.create_index(columns=["tenant_id", "event_date"], name="proj_tenant_time")
ctx.create_index(expression="lower(name)", name="proj_name_lower")
```

The generated DDL is:

```sql
ALTER TABLE events
ADD PROJECTION IF NOT EXISTS proj_user
(SELECT _part_offset ORDER BY (`user_id`));

ALTER TABLE events MATERIALIZE PROJECTION proj_user
SETTINGS mutations_sync = 1;
```

- `_part_offset` stores a **permutation**, not a full copy of the table's
  columns. This is the no-full-data-duplication option.
- One composite projection is created per call:
  `ORDER BY (col1, col2)`. It helps **prefix** predicates (`col1`,
  `col1 + col2`); create separate projections for independent filters.
- ClickHouse uses projection indexes for **part-level pruning** (as of
  CH 25.6/26.x), not granule-level skipping. They are useful when the table
  has many parts; data-skipping indexes remain better for granule-level
  selective filters.
- Projection indexes require **ClickHouse >= 25.5**. Older servers return a
  clear error; pass an explicit `index_type` (below) to use a skipping index
  instead.
- `MATERIALIZE` is a mutation and can be expensive on large tables; pass
  `materialize=False` to defer it. New parts are built automatically.
- `list_indexes()` reports projection indexes with
  `source="projection_index"` and includes their sorting key and definition.

### Data-skipping indexes (explicit)

Pass an explicit `index_type` to create a data-skipping index instead:

```python
ctx.create_index(
    columns=["user_id"], name="idx_user",
    index_type="bloom_filter", granularity=4,
)
ctx.create_index(
    columns=["amount"], name="idx_amount",
    index_type="minmax",
)
ctx.create_index(
    expression="lower(name)", name="idx_name_token",
    index_type="tokenbf_v1(10240, 3, 0)",
)
```

Supported types: `minmax`, `set(N)`, `bloom_filter`, `ngrambf_v1(...)`,
`tokenbf_v1(...)`, `text`. Only local `*MergeTree` engines are supported.
`unique=True`, `method`, `where`, `include`, and `concurrently` are rejected.

`ADD INDEX` is metadata-only for existing parts. With `materialize=True`
(default), memFrame runs `MATERIALIZE INDEX ... SETTINGS mutations_sync = 1`,
which is a mutation and can be expensive on large tables. New parts are indexed
automatically.

`list_indexes()` returns skipping indexes with `source="skipping"`, plus a
non-droppable `order_by` entry with the table's `engine`, `sorting_key`,
`primary_key`, and `partition_key`. `drop_index` auto-detects projection vs
skipping index by name.

Use skipping indexes **after** choosing native types, the `ORDER BY` key, and
materialized views. Verify with:

```sql
EXPLAIN indexes = 1
SELECT * FROM events WHERE user_id = 12345;
```

For projection indexes, inspect `EXPLAIN projections = 1` or
`system.query_log.projections`.

### Projection index vs normal projection vs skipping index

| Mechanism | Data copy | Pruning |
| --- | --- | --- |
| Projection index (`SELECT _part_offset ORDER BY ...`) | Permutation/offset only | Part-level |
| Normal projection (`SELECT * ORDER BY ...`) | Full selected columns | Alternate sort order |
| Data-skipping index (`INDEX ... TYPE bloom_filter/minmax/...`) | Metadata only | Granule-level |

memFrame only generates the `_part_offset` form; it never creates a normal
projection that duplicates full rows.

### Changing ORDER BY (`set_order_by`)

`set_order_by` is a ClickHouse-only **physical rebuild**:

1. read `system.tables.create_table_query`;
2. rewrite the top-level `ORDER BY`/`PRIMARY KEY` (preserving engine,
   partitioning, TTL, settings, skip indexes, projections, and columns);
3. create a temp table, `INSERT SELECT` all rows, verify row count;
4. atomically `RENAME` temp to the table and drop/keep the old table.

```python
# inspect first (default: no writes)
plan = ctx.set_order_by(
    columns=["tenant_id", "event_date"],
    primary_key=["tenant_id"],   # must be a prefix of columns
)
print(plan["ddl"], plan["rows"], plan["bytes_on_disk"])

# run in a maintenance window
result = ctx.set_order_by(
    columns=["tenant_id", "event_date"],
    primary_key=["tenant_id"],
    dry_run=False,
    keep_backup=True,
)
```

- `dry_run=True` is the default; pass `dry_run=False` to execute.
- `verify="count"` (default) checks the copied row count; `verify=None` skips
  it.
- `keep_backup=True` leaves `<table>__old_<timestamp>`; otherwise it is dropped
  after the swap.
- Requires **~2× table space** and takes time proportional to table size.
- **External writes during the copy can be lost.** memFrame serializes its own
  operations on the dataset, but ClickHouse cannot lock a table against
  external writers; use a maintenance window and back up first.
- Replicated, Distributed, and non-MergeTree engines are rejected.
- Nullable `ORDER BY` columns are supported by adding the table setting
  `allow_nullable_key = 1` (memFrame uploads columns as `Nullable`).
- Choose the key from real query filters, low-cardinality columns first, and
  keep it to 4–5 columns. `ORDER BY` is immutable; planning it at table
  creation is preferable to rebuilding later.

## What was removed

The old metadata-only logical index is gone:

- no `index_cols` column in `memframe_csv_registry`;
- no `set_index`/`reset_index`/`get_index`/`reindex`/`reindex_like`;
- no `ctx.index` property.

Use pandas on a fetched frame for row-label alignment, or the index API above
for database indexing.
