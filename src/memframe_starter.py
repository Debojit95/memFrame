# @title memFrame — pandas-style DataFrames on DuckDB / Postgres / ClickHouse, plus an AI agent
# A self-contained, runnable tour of the library. Upload to Colab and Run all.

# %% [markdown]
# # memFrame
#
# **memFrame brings a pandas-like DataFrame API to real databases** — DuckDB, PostgreSQL, and ClickHouse — with an async-first engine and an optional AI agent you can chat with.
#
# What you get:
# - A familiar DataFrame surface (`head`, `loc`, `fillna`, `mean`, `groupby`-style transforms…) that runs as **SQL** under the hood.
# - **Plotting** (bar, line, pie, scatter, 3D, polar) straight from your table.
# - A **natural-language agent** that decomposes a request into real operations on your data.
# - One consistent API across three backends; switch from local DuckDB to Postgres/ClickHouse by changing one line.
#
# This notebook is fully self-contained: it generates a synthetic sales dataset, so no downloads or Kaggle credentials are required.



import nest_asyncio
import os
import pandas as pd
import numpy as np

nest_asyncio.apply()
from memframe import MemFrame


# %% [markdown]
# ## 1. Connect to a backend
#
# `MemFrame` owns the connection. Here we use DuckDB stored in a local file — swap `connection_type` to `"postgres"` or `"clickhouse"` (and the matching `connection_params`) to target a different engine with the exact same code.

# %%
mf = MemFrame(
    connection_type="local",
    connection_params={"db_path": "memframe_demo.duckdb"},
)
mf.connect()


# %% [markdown]
# ## 2. Create a demo dataset
#
# A small, realistic sales table: a year of daily rows with a region, product, price, units sold, revenue, returns and a rating — with a few missing values sprinkled in so the cleaning ops have something to do.
#
# Everything below is plain pandas; memFrame ingests it unchanged.

# %%
def make_demo_data(rows: int = 365, seed: int = 42) -> pd.DataFrame:
    rng = np.random.default_rng(seed)
    dates = pd.date_range("2023-01-01", periods=rows, freq="D")
    region = rng.choice(["North", "South", "East", "West"], rows)
    product = rng.choice(["Widget", "Gadget", "Gizmo"], rows)
    price = rng.uniform(10, 100, rows).round(2)
    units = rng.poisson(50, rows).astype(float)
    units[rng.random(rows) < 0.08] = np.nan          # inject missing values
    revenue = (units * price * rng.uniform(0.9, 1.1, rows)).round(2)
    revenue[rng.random(rows) < 0.05] = np.nan
    returns = rng.integers(0, 5, rows)
    rating = rng.uniform(1, 5, rows).round(1)
    return pd.DataFrame(
        {
            "date": dates,
            "region": region,
            "product": product,
            "price": price,
            "units_sold": units,
            "revenue": revenue,
            "returns": returns,
            "rating": rating,
        }
    )


demo = make_demo_data()
demo.head()


# %% [markdown]
# ## 3. Upload → a live, queryable context
#
# `upload_df` returns a **`ContextManager`** (`ds`) — a live, database-backed handle to the table. Every operation from here on executes as SQL against DuckDB.

# %%
ds = mf.upload_df(demo, filename="sales_2023")
ds.head(5)


# %% [markdown]
# ## 4. Inspection
#
# pandas-style table introspection. (`shape()`, `dtypes()`, `info()`, `describe()`, `null_analysis()`…)

# %%
ds.shape()
ds.dtypes()
ds.info()
ds.describe()
ds.null_analysis()
ds.head(5, columns=["date", "region", "units_sold", "revenue"])
ds.tail(3)
ds.sample(5, random_state=1)


# %% [markdown]
# ## 5. Selection
#
# Filtering and column picking. Operations are **non-destructive** — they return a new view, leaving the original table intact. (`where` and `select_dtypes` are shown here; label/position indexing via `loc` / `iloc` / `at` / `iat` is also available.)

# %%
ds.where("region = 'West'")            # filter rows (SQL WHERE) -> new table
ds.select_dtypes(include="numeric")    # keep only numeric columns
ds.where("units_sold > 40")            # another filtered view


# %% [markdown]
# ## 6. Cleaning
#
# Fill missing values, drop outliers, remap categories, and more. Each call returns the resulting table's data so you can inspect it immediately.

# %%
ds.to_numeric(column="units_sold")
filled = ds.fillna(column="units_sold", method="mean")
filled.head()
ds.dropna().head()
ds.clip(column="rating", lower=1, upper=5)
ds.map_values(column="region", mapping={"North": "N", "South": "S", "East": "E", "West": "W"})
ds.drop_outliers(column="revenue", z_thresh=3.0)


# %% [markdown]
# ## 7. Statistics
#
# Numeric, categorical, and datetime stats — with automatic dtype routing (e.g. `mean` works on numbers, `mode`/proportions on categories, `datetime_diff` on dates).

# %%
print("mean revenue   :", ds.mean(column="revenue"))
print("median units   :", ds.median(column="units_sold"))
print("max units      :", ds.max(column="units_sold"))
print("mode region    :", ds.mode(column="region"))
print("skew revenue   :", ds.skew(column="revenue"))
ds.value_counts(column="region", top_n=5)
ds.corr(columns=["units_sold", "revenue", "price"])   # correlation matrix
ds.outliers_zscore(column="revenue")
ds.datetime_diff(column="date")
ds.time_unit_counts(column="date", unit="month")


# %% [markdown]
# ## 8. Arithmetic
#
# Column/column and column/scalar math, all written back as new columns.

# %%
ds.add(col1="units_sold", col2="returns", target_col="units_plus_returns")
ds.sub(col1="revenue", col2="returns", target_col="net_revenue")
ds.percentage_change(old_col="units_sold", new_col="revenue", target_col="pct_chg")
ds.round(column="revenue", digits=0, target_col="revenue_rounded")
ds.sqrt(column="units_sold", target_col="units_sqrt")
ds.normalize_range(column="revenue", target_col="revenue_norm")


# %% [markdown]
# ## 9. A second, aggregated table (for clean charts)
#
# memFrame has no standalone "group-by → table" op, so we pre-aggregate with pandas and upload a second dataset. This also shows off the **dataset registry**: you can list and switch between multiple uploaded tables.

# %%
region_totals = (
    demo.dropna(subset=["units_sold"])
    .groupby("region", as_index=False)
    .agg(total_units=("units_sold", "sum"), total_revenue=("revenue", "sum"))
)
ds_r = mf.upload_df(region_totals, filename="region_summary")
ds_r.head()

mf.list_tables()
# Switch the active dataset by its data_id (filenames are just labels).
mf.set_active(ds_r._data_id)


# %% [markdown]
# ## 10. Plotting
#
# Charts are built directly from the active table via Plotly. Each call returns a Plotly `Figure`; in Colab the figure renders as the cell output.

# %%
ds.line(x="date", y="revenue", title="Revenue over time")

ds.scatter(x="price", y="revenue", color="region", title="Price vs Revenue")

ds_r.bar(x="region", y="total_units", color="region", title="Total units by region")

ds_r.pie(names="region", values="total_units", title="Share of units by region")

ds.scatter_3d(x="price", y="revenue", z="units_sold", title="3D: price / revenue / units")

ds_r.bar_polar(theta="region", r="total_units", title="Polar: units by region")


# %% [markdown]
# ## 11. Async-first under the hood
#
# Every sync method (e.g. `head`) is just `@async_to_sync` sugar over its `a*` async twin. You can call the async API directly.

# %%
preview = await ds.ahead(5)
preview


# %% [markdown]
# ## 12. The AI agent
#
# Enable the agent with any supported provider (OpenAI, Anthropic, Google, Ollama). It decomposes a natural-language request into the same operations you just ran — and renders charts inline.
#
# Set your key via the `OPENAI_API_KEY` environment variable (Colab Secrets work great), or paste it where the placeholder is.

# %%
import os

api_key = os.getenv("OPENAI_API_KEY", "PASTE_KEY_HERE")
mf.enable_agent(
    provider="openai",
    model="gpt-4o-mini",                       # use a model you have access to
    api_key=api_key,
)

if api_key != "PASTE_KEY_HERE":
    result = ds.chat(
        "Fill missing revenue with the median, then show a bar chart of total "
        "units sold per region and subtract returns from units sold into a new column."
    )
    print(result["answer"])     # any charts the agent builds render inline during the run
else:
    print("Set the OPENAI_API_KEY environment variable (or paste a key) to run the agent.")


# %% [markdown]
# ## Where to go next
#
# - Swap the backend: `connection_type="postgres"` / `"clickhouse"` with the right `connection_params` — the code above is unchanged.
# - Turn on **deep caching** (`MemFrame(..., deep_cache=True)`) for replayable result tables and a full operation lineage.
# - Read the docs at https://debojit95.github.io/memFrame/ and close the connection with `mf.close()`.
