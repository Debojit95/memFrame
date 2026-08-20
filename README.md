<p align="center">
  <img src="docs/assets/memframe-logo-full.png" alt="memFrame logo" width="720">
</p>

# memFrame

[![PyPI - Version](https://img.shields.io/pypi/v/memframe)](https://pypi.org/project/memframe/)
[![Python](https://img.shields.io/badge/python-3.10%2B-blue)](https://github.com/Debojit95/memFrame/blob/main/pyproject.toml)
[![CI](https://img.shields.io/github/actions/workflow/status/Debojit95/memFrame/ci.yml?label=ci)](https://github.com/Debojit95/memFrame/actions/workflows/ci.yml)
[![Tox](https://img.shields.io/github/actions/workflow/status/Debojit95/memFrame/tox.yml?label=tox)](https://github.com/Debojit95/memFrame/actions/workflows/tox.yml)
[![License - AGPL-3.0](https://img.shields.io/github/license/Debojit95/memFrame)](https://github.com/Debojit95/memFrame/blob/main/LICENSE)
[![PyPI - Downloads](https://img.shields.io/pypi/dm/memframe)](https://pypi.org/project/memframe/)
[![Open In Colab](https://colab.research.google.com/assets/colab-badge.svg)](https://colab.research.google.com/drive/1UtoPjzTmia4Cr6y2btDmV8mINgXaBPGP?usp=sharing)

> *memFrame brings a pandas-like DataFrame API to DuckDB, PostgreSQL, and ClickHouse — async-first, with an optional AI agent for natural-language data work.*

## Features

- Database-backed DataFrame API across DuckDB, PostgreSQL, and ClickHouse.
- Compiles every pandas-style call to backend-native SQL (DuckDB / PostgreSQL / ClickHouse) and runs it in-engine — your data never leaves the database.
- Async-first surface with sync equivalents for every operation.
- Upload from CSV, Parquet, or pandas DataFrame.
- Inspection, selection, cleaning, statistics, arithmetic, Plotly charts.
- Two-level cache: lineage audit + replayable result tables.
- Optional AI agent layer (`memframe_ai`) for chatting with your CSV.

## Installation

```bash
pip install memframe              # core
uv add memframe                   # alt: uv
pip install "memframe[ai]"        # + Pydantic AI agent layer
uv add "memframe[ai]"             # alt: uv
```

Local development from this repository:

```bash
git clone https://github.com/Debojit95/memFrame.git
cd memFrame
pip install -e ".[dev,ai]"
```

## Quick Start

```python
import asyncio
import pandas as pd

from memframe import MemFrame


async def main():
    mf = MemFrame(
        connection_type="local",
        connection_params={"db_path": "memFrame.duckdb"},
    )
    await mf.aconnect()

    customers = await mf.aupload_df(
        pd.DataFrame(
            {
                "id": [101, 102, 103],
                "name": ["Alice", "Bob", "Charlie"],
                "score": [95.5, 82.0, None],
                "active": [True, False, True],
            }
        ),
        filename="customers",
    )

    preview = await customers.ahead(n=5)
    print(preview["result"])

    await mf.aclose()


asyncio.run(main())
```

Each upload returns a dataset context; chain inspection, selection, cleaning,
arithmetic, statistics, and plotting on it. Sync methods drop the `a` prefix
(`head`, `iloc`, `fillna`, …).

## Architecture

Every pandas-style call compiles to backend-native SQL and runs in-engine — your
data never leaves the database. The call path is a four-layer stack (public API →
orchestrator → core SQL engine → backend adapter), with connection, ingestion,
and a two-level cache as cross-cutting subsystems, plus an optional
[`memframe_ai`](docs/architecture.md#optional-ai-layer-memframe_ai) agent layer on top.

See the full [Architecture](docs/architecture.md) page for the component diagram
and a per-layer breakdown.

## AI Agent

`memframe_ai` adds a Pydantic AI agent fleet on top of memFrame. After enabling
it on the `MemFrame` instance, any dataset context can run natural-language
queries that decompose into specialist tools and return typed response blocks.

```python
import asyncio
import pandas as pd

from memframe import MemFrame


async def main():
    mf = MemFrame(
        connection_type="local",
        connection_params={"db_path": "memFrame.duckdb"},
    )
    await mf.aconnect()

    await mf.aenable_agent(
        provider="openai",
        model="gpt-5.5",
        api_key="sk-...",
    )

    ds = await mf.aupload_df(
        pd.DataFrame(
            {
                "name": ["Alice", "Bob", "Charlie"],
                "score": [95.5, 82.0, None],
            }
        ),
        filename="customers",
    )

    result = await ds.achat("fill null scores with the mean")
    print(result["answer"])
    print(result["plots"])  # any charts the agent built (id, title, spec_preview)

    await mf.aclose()


asyncio.run(main())
```

The agent supports OpenAI, Anthropic, Google, and Ollama. Pick a provider when
you enable the agent:

```python
await mf.aenable_agent(api_key="sk-...", provider="anthropic", model="claude-...")
```

## Documentation

Full reference lives in [`docs/`](docs/):

- [Getting Started](docs/getting-started.md) — connect, upload, first query.
- [Connector & Connection](docs/api/connector.md) — DuckDB / Postgres / ClickHouse wiring.
- [Upload Manager](docs/api/upload-manager.md) — CSV / Parquet / DataFrame ingestion.
- [Dataset Operations](docs/api/database.md) — table and active-dataset management.
- [Inspection](docs/api/inspect.md) · [Selection](docs/api/selection.md) · [Cleaning](docs/api/cleaning.md)
- [Statistics](docs/api/stats.md) · [Arithmetic](docs/api/arithmetic.md)
- [Plotting](docs/api/bar.md) · [Caching](docs/api/caching.md)

Serve locally:

```bash
mkdocs serve
```