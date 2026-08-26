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
- Sync pre-existing DuckDB, PostgreSQL, or ClickHouse tables as datasets — no re-upload.
- Inspection, selection, cleaning, statistics, arithmetic, Plotly charts.
- Two-level cache: lineage audit + replayable result tables.
- Optional AI agent layer (`memframe_ai`) for chatting with your CSV.
- Optional **Logfire observability** for the AI agent layer — traces every agent run, LLM call, and tool call, plus host metrics. Opt-in via `logfire_enabled` (local console needs no token; bring-your-own-key for the cloud).

## Installation

```bash
pip install memframe                      # core
uv add memframe                          # alt: uv
pip install "memframe[ai]"               # + Pydantic AI agent layer
uv add "memframe[ai]"                    # alt: uv
pip install "memframe[logfire]"          # + AI agent layer AND Logfire observability
pip install "memframe[ai,logfire]"       # explicit equivalent of the above
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

Already have tables in your database? Register them as datasets without
re-uploading:

```python
registered = mf.register_tables()
# {'sales': [{'data_id': 'a1b2c3', 'table_name': 'orders', 'row_count': 100}, ...]}
```

Registered tables act like uploads — activate with `set_active(data_id)` and
query via `memFrame()`. Deleting one only removes memFrame's registry entry;
your real table is left untouched.

## Architecture

Every pandas-style call compiles to backend-native SQL and runs in-engine — your
data never leaves the database. The call path is a four-layer stack (public API →
orchestrator → core SQL engine → backend adapter), with connection, ingestion,
and a two-level cache as cross-cutting subsystems, plus an optional
[`memframe_ai`](docs/architecture.md#optional-ai-layer-memframe_ai) agent layer on top.

```mermaid
flowchart TD
    %% Global styling to match the original diagram
    classDef green fill:#d1e7dd,stroke:#198754,stroke-width:2px,color:#000000;
    classDef yellow fill:#fef9e7,stroke:#f1c40f,stroke-width:2px,color:#000000;
    classDef orange fill:#fdebd0,stroke:#e67e22,stroke-width:2px,color:#000000;
    classDef purple fill:#e8daef,stroke:#8e44ad,stroke-width:2px,color:#000000;
    classDef blue fill:#d6eaf8,stroke:#2980b9,stroke-width:2px,color:#000000;
    classDef red fill:#fadbd8,stroke:#c0392b,stroke-width:2px,color:#000000;
    classDef brown fill:#ede7f6,stroke:#5d4037,stroke-width:2px,color:#000000;

    %% Top-level node (bridging core and AI)
    OutputResponse["Output Response"]
    class OutputResponse green;

    %% Core Subgraph
    subgraph core ["core"]
        UserCode["User code: ds.head() / await ds.ahead()"]
        PublicAPI["Public API: MemFrame / ContextManager"]
        Analytics["Analytics/Processing/Transforms/Cleaning/Plotly Wrapper"]
        Getattr["_getattr_ * async/sync twins Wrapper"]
        Orchestrator["Orchestrator(type router for numerical, datetime)"]
        TwoLevelCache["Two-level Cache: L1/L2"]
        GroupBy["GroupBy/Window/Transform/Core"]
        GeneratePlot["Generate Plotly Plot"]
        CoreEngine["Core Analytics Engine"]
        CompileSQL["Compiles backend-native SQL"]
        DatabaseAdapter["DatabaseAdapter"]
        Results["Results collected"]
        Unwrap{"unwrap_response"}

        %% Core Connections
        UserCode --> PublicAPI
        PublicAPI --> Analytics
        PublicAPI -- Cache Store --> TwoLevelCache
        Analytics -- resolves --> Getattr
        Analytics -.-> GeneratePlot
        Getattr -- calls --> Orchestrator
        Orchestrator -- checks --> TwoLevelCache
        Orchestrator -- analytics ops --> GroupBy
        Orchestrator -- calls --> GeneratePlot
        GeneratePlot -- return plotly fig --> Orchestrator
        TwoLevelCache -- Cache Hit --> OutputResponse
        TwoLevelCache -- Cache Miss --> CoreEngine
        GroupBy -- Plot --> GeneratePlot
        GroupBy --> CoreEngine
        CoreEngine -- Compiles --> CompileSQL
        CompileSQL -- sends SQL --> DatabaseAdapter
        DatabaseAdapter -- executes SQL --> Results
        Results --> Unwrap
        Unwrap --> OutputResponse

        class UserCode green;
        class PublicAPI,Analytics,Getattr,Orchestrator,GroupBy,GeneratePlot,CoreEngine,CompileSQL,DatabaseAdapter,Results yellow;
        class TwoLevelCache purple;
        class Unwrap blue;
    end

    %% AI Subgraph
    subgraph ai ["ai(pydantic-ai-harness)"]
        ChatRouter["chat / enable_agent"]
        DashboardRouter["dashboard / dashboard"]
        Guardrail{"Guardrail Agent"}
        Planner["Planner Agent"]
        PlotAgents["Plot Agents"]
        AnalyticsAgents["Analytics Agent"]
        ModelGateway{{"ModelGateway (openai / anthropic / google / ollama)"}}
        Sandbox["Sandboxed Monty Runtime Execution"]
        ResultCollector["memFrame-ai result collector"]
        DashboardOut["Dashboard (dataviz)"]
        ChatOut["Chat Response"]

        %% AI Connections
        ChatRouter -- routes --> Guardrail
        DashboardRouter -- routes --> Guardrail
        Guardrail -- valid query --> Planner
        Planner -- Uses --> PlotAgents
        Planner -- Uses --> AnalyticsAgents
        PlotAgents -- Uses --> ModelGateway
        AnalyticsAgents -- Uses --> ModelGateway
        ModelGateway -- Code generation --> PlotAgents
        ModelGateway -- Code generation --> AnalyticsAgents
        PlotAgents -- Code generation --> Sandbox
        AnalyticsAgents -- Code generation --> Sandbox
        ModelGateway -- Calls --> Sandbox
        Sandbox -- Calls --> ResultCollector
        ResultCollector -- dashboard --> DashboardOut
        ResultCollector -- chat --> ChatOut

        class ChatRouter brown;
        class DashboardRouter,DashboardOut,ChatOut green;
        class Guardrail blue;
        class Planner,PlotAgents,AnalyticsAgents orange;
        class ModelGateway red;
        class Sandbox,ResultCollector yellow;
    end

    %% Cross-subgraph connection
    Unwrap -.-> ResultCollector
```

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
- [Sync Existing Tables](docs/api/syncdb.md) — register pre-existing DB tables as datasets.
- [Dataset Operations](docs/api/database.md) — table and active-dataset management.
- [Inspection](docs/api/inspect.md) · [Selection](docs/api/selection.md) · [Cleaning](docs/api/cleaning.md)
- [Statistics](docs/api/stats.md) · [Arithmetic](docs/api/arithmetic.md)
- [Plotting](docs/api/bar.md) · [Caching](docs/api/caching.md)

Serve locally:

```bash
mkdocs serve
```