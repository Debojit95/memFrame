# Dataset Operations

Source: `src/wrappers/ops.py`

`OpsWrapper` is the public dataset-operation interface exposed through
`MemFrame`. It manages uploaded datasets, tracks the active dataset, and reads
or records operation history. Each public method has an asynchronous
`a`-prefixed form and a synchronous form.

The lower-level implementations in `src/db_manager/ops.py` remain internal.

## Public API

| Synchronous | Asynchronous | Purpose |
| --- | --- | --- |
| `list_tables()` | `await alist_tables()` | List successful uploads |
| `set_active(data_id)` | `await aset_active(data_id)` | Set the active dataset |
| `get_active_table()` | `await aget_active_table()` | Read the active dataset ID |
| `delete_table(data_id=None, filename=None)` | `await adelete_table(data_id=None, filename=None)` | Delete an uploaded dataset |
| `list_operations(data_id=None)` | `await alist_operations(data_id=None)` | List operation history |
| `retrieve_operation(data_id, opidx)` | `await aretrieve_operation(data_id, opidx)` | Resolve an operation table name |

## Dataset Management

### List Uploaded Datasets

```python
tables = mf.list_tables()
```

```python
tables = await mf.alist_tables()
```

The result is ordered from newest to oldest:

```python
[
    {"data_id": "a1b2c3", "filename": "customers.csv"},
    {"data_id": "d4e5f6", "filename": "orders.parquet"},
]
```

Only registry entries whose upload completed successfully are returned.

### Select the Active Dataset

```python
mf.set_active("a1b2c3")
active_id = mf.get_active_table()
```

```python
await mf.aset_active("a1b2c3")
active_id = await mf.aget_active_table()
```

Setting an active dataset first verifies that its upload table exists. Methods
that accept an optional `data_id` use the active dataset when no ID is given.

### Delete a Dataset

Delete by `data_id`:

```python
mf.delete_table(data_id="a1b2c3")
```

Delete by original filename:

```python
mf.delete_table(filename="customers.csv")
```

Deletion drops the dataset's generated transient tables, drops its upload
table, removes its registry records, and clears the active dataset when
necessary.

!!! warning
    Dataset deletion is permanent.

## Operation History

List the recorded operations for a dataset:

```python
operations = mf.list_operations("a1b2c3")
```

Each record contains `opidx`, `operation_type`, `table_name`, and `created_at`.
When no `data_id` is supplied, `list_operations` uses the active dataset.

Retrieve the generated table name for one operation:

```python
table_name = mf.retrieve_operation("a1b2c3", opidx=2)
```

Operation recording is handled internally by memFrame when library operations
create generated tables.

## Errors

- Operations that require a database raise `RuntimeError` when not connected.
- Selecting an unknown `data_id` raises `ValueError`.
- Deletion requires either `data_id` or `filename`.
- Listing operations without a supplied or active dataset raises `ValueError`.

## API Reference

<div class="compact-api" markdown="1">

::: src.wrappers.ops.OpsWrapper
    options:
      show_root_heading: true
      show_root_full_path: true
      members:
        - alist_tables
        - list_tables
        - aset_active
        - set_active
        - aget_active_table
        - get_active_table
        - adelete_table
        - delete_table
        - alist_operations
        - list_operations
        - aretrieve_operation
        - retrieve_operation

</div>
