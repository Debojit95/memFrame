# Upload Manager

Source: `src/wrappers/upload.py`

`UploadWrapper` is the public upload interface exposed through `MemFrame`. It
ingests CSV files, Parquet files, and pandas DataFrames into the connected
DuckDB or PostgreSQL backend. Successful uploads are recorded in
`registry.csv_registry` and return a `ContextManager` bound to the new dataset.

Users normally call these methods on a connected `MemFrame` instance. The
lower-level uploader methods in `src/core/ingestion/upload_manager.py` are
implementation details used by the wrapper.

## Public API

Every upload format has synchronous and asynchronous forms:

| Synchronous | Asynchronous | Input |
| --- | --- | --- |
| `upload_csv(path)` | `await aupload_csv(path)` | CSV file |
| `upload_parquet(path)` | `await aupload_parquet(path)` | Parquet file |
| `upload_df(df, filename=None)` | `await aupload_df(df, filename=None)` | pandas DataFrame |

All six methods return a `ContextManager`.

### Upload a CSV

```python
dataset = mf.upload_csv("data/customers.csv")
```

```python
dataset = await mf.aupload_csv("data/customers.csv")
```

CSV ingestion:

1. Detects the source encoding.
2. Cleans and deduplicates column names.
3. Samples values to infer column types.
4. Loads values into a text staging table.
5. Safely casts each column into the final `upload` table.
6. Drops the staging table and records the upload.

Values that cannot be cast to an inferred type become `NULL` instead of
aborting the entire CSV upload.

### Upload Parquet

```python
dataset = mf.upload_parquet("data/events.parquet")
```

Parquet files are read with PyArrow. The upload manager normalizes column names,
infers types from a sample, creates the final table, and inserts the complete
Arrow table.

### Upload a pandas DataFrame

```python
import pandas as pd

frame = pd.DataFrame(
    {
        "customer_id": [1, 2],
        "segment": ["retail", "enterprise"],
    }
)

dataset = mf.upload_df(frame, filename="customers.csv")
```

The DataFrame path writes a temporary CSV, passes it through CSV ingestion, and
then updates the registry with the requested filename. The temporary file is
removed whether the upload succeeds or fails.

## Column Names

Column names are normalized before table creation:

- Empty names become `column_<index>`.
- Leading and trailing whitespace and quote characters are removed.
- Characters other than letters, numbers, and `_` become `_`.
- Names beginning with a number receive a leading `_`.
- Duplicate names receive numeric suffixes such as `_1` and `_2`.

## Backend Behavior

For DuckDB, CSV data is first read with PyArrow and registered with the active
DuckDB connection. If PyArrow cannot read the source, ingestion falls back to
Python's CSV reader.

For PostgreSQL, CSV data is loaded with `COPY`. Rows with an unexpected number
of fields use a padding/truncation fallback before they are copied.

## Errors

- Uploading before connecting raises `RuntimeError`.
- A missing CSV or Parquet path raises `FileNotFoundError`.
- `upload_df` raises `TypeError` for non-DataFrame values.
- A DataFrame with no columns raises `ValueError`.

## API Reference

::: src.wrappers.upload.UploadWrapper
    options:
      show_root_heading: true
      show_root_full_path: true
      members:
        - aupload_csv
        - upload_csv
        - aupload_parquet
        - upload_parquet
        - aupload_df
        - upload_df
