# Sync Existing Tables (SyncDB)

Source: `src/memframe/db_manager/ops.py`

`MemFrame` can register tables that **already exist** in your connected
DuckDB, PostgreSQL, or ClickHouse database into `memframe_csv_registry`, so they behave
exactly like uploaded datasets — activatable, queryable, and deletable through
the normal dataset APIs, with no re-ingestion.

## Public API

| Synchronous | Asynchronous | Input |
| --- | --- | --- |
| `register_tables()` | `await aregister_tables()` | — |

### Sync all existing tables

```python
registered = mf.register_tables()
# {'sales': [{'data_id': 'a1b2c3', 'table_name': 'orders', 'row_count': 100}, ...]}
```

```python
registered = await mf.aregister_tables()
```

## Behavior

- Enumerates every schema → table via the backend's `list_user_tables`,
  **skipping system schemas** (e.g. `pg_catalog`, ClickHouse `system` /
  `INFORMATION_SCHEMA`, and memFrame's own `memframe_upload` / `memframe_transient` / `memframe_csv_registry`).
- Skips tables that are already registered or empty.
- Assigns a fresh 6-char `data_id` per table and returns only what was
  registered **this call**.
- **Idempotent:** a second call returns `{}`.
- Registered tables are marked `is_external=True`. Deleting one removes only
  the registry entry — **your real table is never dropped**.
- After registering + `set_active(data_id)`, use the normal dataset context
  (`memFrame()`) exactly as with uploads.
