# Example Data

Most examples in the `memFrame` API docs use the same `example` dataset.
It runs entirely in DuckDB in memory, so you can paste the setup into Colab
(or any Python shell) and run the rest of the page. A few pages define a small
local frame for a specialized shape (window ordering, `to_numeric` text,
merges, indexes, or trigonometric domains); those pages show the local frame
inline.

```python
# In Colab: !pip install -q memframe
import pandas as pd
from memframe import MemFrame

mf = MemFrame(
    connection_type="local",
    connection_params={"db_path": ":memory:"},
)
mf.connect()

rows = [
    [1, "2024-01-01", "2024-01-01 09:15", "2024-01", "North", "A", 10.0, 50000, 5000, 88.5, 25, "ok"],
    [2, "2024-01-02", "2024-01-02 10:30", "2024-01", "South", "B", 20.0, 60000, 6000, 92.0, 32, None],
    [3, "2024-01-03", "2024-01-03 11:45", "2024-01", "North", "A", 30.0, 55000, 5500, None, 41, "ok"],
    [4, "2024-02-01", "2024-02-01 14:00", "2024-02", "South", "C", 40.0, 65000, 6500, 60.0, 29, "ok"],
    [5, "2024-02-02", "2024-02-02 15:15", "2024-02", "North", "B", 50.0, 52000, 5200, 95.0, 36, None],
]
frame = pd.DataFrame(rows, columns=[
    "id", "date", "event_time", "month", "region", "category",
    "sales", "salary", "bonus", "score", "age", "notes",
])
frame["date"] = pd.to_datetime(frame["date"])
frame["event_time"] = pd.to_datetime(frame["event_time"])

ctx = mf.DataFrame(
    frame,
    filename="example",
    dtypes={"month": "VARCHAR", "notes": "VARCHAR"},
)

ctx.dtypes
ctx.head()
```

`ctx` is the public `ContextManager` for the uploaded table. Every computation
in the docs is called on `ctx`; the lower-level wrapper classes are
implementation details.

## Schema

| Column | pandas dtype | memFrame / DuckDB type | Example values | Used by |
| --- | --- | --- | --- | --- |
| `id` | `int64` | `BIGINT` | `1 … 5` | selection, sorting, joins |
| `date` | `datetime64[ns]` | `TIMESTAMP` | `2024-01-01 …` | datetime, `order_by` |
| `event_time` | `datetime64[ns]` | `TIMESTAMP` | `2024-01-01 09:15 …` | datetime |
| `month` | `object` | `VARCHAR` | `2024-01`, `2024-02` | groupby, aggregation |
| `region` | `object` | `VARCHAR` | `North`, `South` | groupby, filter |
| `category` | `object` | `VARCHAR` | `A`, `B`, `C` | groupby, transform |
| `sales` | `float64` | `DOUBLE` | `10.0 … 50.0` | window, stats, cumulative |
| `salary` | `int64` | `BIGINT` | `50000 … 65000` | arithmetic |
| `bonus` | `int64` | `BIGINT` | `5000 … 6500` | arithmetic |
| `score` | `float64` | `DOUBLE` | `88.5`, `92.0`, `NULL` | cleaning, stats |
| `age` | `int64` | `BIGINT` | `25 … 41` | stats, filter |
| `notes` | `object` | `VARCHAR` | `ok`, `NULL` | cleaning |

`score` and `notes` deliberately contain `NULL`s so cleaning examples have
something to fill or drop.

## Actual table

```text
{'id': 'BIGINT', 'date': 'TIMESTAMP', 'event_time': 'TIMESTAMP',
 'month': 'VARCHAR', 'region': 'VARCHAR', 'category': 'VARCHAR',
 'sales': 'DOUBLE', 'salary': 'BIGINT', 'bonus': 'BIGINT',
 'score': 'DOUBLE', 'age': 'BIGINT', 'notes': 'VARCHAR'}
```

```text
 id       date          event_time   month region category  sales  salary  bonus  score  age notes
  1 2024-01-01 2024-01-01 09:15:00 2024-01  North        A   10.0   50000   5000   88.5   25    ok
  2 2024-01-02 2024-01-02 10:30:00 2024-01  South        B   20.0   60000   6000   92.0   32  None
  3 2024-01-03 2024-01-03 11:45:00 2024-01  North        A   30.0   55000   5500    NaN   41    ok
  4 2024-02-01 2024-02-01 14:00:00 2024-02  South        C   40.0   65000   6500   60.0   29    ok
  5 2024-02-02 2024-02-02 15:15:00 2024-02  North        B   50.0   52000   5200   95.0   36  None
```

## Specialized frames

Some pages need a different shape. Those pages define their own small frame
inline and show its schema:

- **Clean** — `raw_ctx` with `amount_raw` for `to_numeric`.
- **Arithmetic** — `trig_ctx` with `angle`, `ratio`, `x`, `y` for the
  trigonometric domain.
- **Datetime** — `epoch_ctx` and `date_ctx` for epoch and string parsing.
- **Reshape** — `tags_ctx` with a list-like `tags` column for `explode`.
- **Merge** — local `left` + `right` contexts for join examples.
- **Index** — `customers_ctx` with `customer_id`, `email`, `status`,
  `created_at`, and `tenant_id`.
