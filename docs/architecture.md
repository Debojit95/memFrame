# Architecture

memFrame is a thin pandas-like API over a database engine. Every DataFrame-style
call is compiled to backend-native SQL and executed in-engine — your data never
leaves the database. The call path is a four-layer stack, with connection,
ingestion, and cache as cross-cutting subsystems, and an optional AI agent layer
on top.

```mermaid
flowchart TD
    U["User code<br/>ds.head() / await ds.ahead()"]

    subgraph PUBLIC["Public API · MemFrame / ContextManager"]
      CM["ContextManager.__getattr__<br/>resolves method on wrapper"]
      WR["Wrapper layer<br/>*Wrapper · async/sync twins"]
    end

    subgraph ORCH["Orchestration"]
      OR["*Orchestrator<br/>resolve table + schema"]
      CACHE[("Two-level Cache<br/>L1 signature log · L2 deep tables")]
    end

    subgraph CORE["Core Engine · GeneralTableOps"]
      SQL["Build backend-native SQL<br/>envelope ok() / fail()"]
      RESP["unwrap_response<br/>raise on error / return result"]
    end

    subgraph ADAPT["Adapter · DatabaseAdapter (ABC)"]
      D[("DuckDB")]
      P[("PostgreSQL")]
      C[("ClickHouse")]
    end

    subgraph SUBS["Cross-cutting"]
      CONN["ConnectorManager<br/>pool + backend + uploader"]
      ING["Ingestion<br/>CSV · Parquet · DF · DatatypeDetector"]
    end

    subgraph AI["Optional AI · memframe_ai"]
      CHAT["achat / enable_agent"]
      AGT["Planner + Analytics specialists"]
      TOOLS["Agent tools → core ops"]
      GW["ModelGateway<br/>OpenAI · Anthropic · Google · Ollama"]
    end

    U --> CM --> WR --> OR
    OR -. cache lookup .-> CACHE
    OR --> SQL --> ADAPT
    ADAPT --> D & P & C
    SQL --> RESP
    CONN -. owns .-> ADAPT
    ING -. feeds .-> CONN
    CHAT --> AGT --> TOOLS --> SQL
    GW -. models for .-> AGT
```

## Layers

- **Public API** — `MemFrame` and the per-dataset `ContextManager` it returns
  after upload. `ContextManager.__getattr__` lazily resolves any method onto a
  wrapper; each wrapper exposes an `async`/`sync` twin (`ahead`/`head`) and
  delegates via `super()`.
- **Orchestration** — the `*Orchestrator` resolves the active `data_id` to a
  `table_name, schema` from the `csv_registry`, lazily builds the core ops
  engine, and applies the `@record_call` two-level cache.
- **Core engine** — `GeneralTableOps` builds and runs the backend-native SQL,
  returning a structured `{is_error, result, ...}` envelope; `unwrap_response`
  raises on error or returns the raw result. Plotting mirrors this with
  `*PlotCore`.
- **Adapter** — `DatabaseAdapter` is the only place that knows placeholder style
  (`?` vs `$1`), identifier quoting, and metadata queries, per backend.

## Cross-cutting subsystems

- **Connection** — `ConnectorManager` owns the lifecycle: connection pool, the
  `DatabaseBackend` (creates `csv_registry` / `transient_registry`, runs schema
  migrations), and the uploader.
- **Ingestion** — upload strategies for CSV / Parquet / pandas DataFrame plus a
  `DatatypeDetector` (encoding, delimiter, type inference).
- **Cache** — `record_call` is a `CacheManager` decorator. L1 (`deep_cache=False`,
  default) records call signatures as a lineage/audit log only; L2
  (`deep_cache=True`) persists result DataFrames as typed transient tables and
  replays them on a hit.

## Optional AI layer (`memframe_ai`)

`MemFrame.aenable_agent` / `ContextManager.achat` route natural-language prompts
through `entrypoints` into a fleet of Pydantic-AI agents — a `PlannerAgent` and
`AnalyticsAgent` specialists (inspect, select, clean, stats, arithmetic, plots).
Each agent tool calls the **same core ops engine** the API uses. `ModelGateway`
maps a provider to a Pydantic-AI model (OpenAI, Anthropic, Google, Ollama), with
`FallbackModel` support.
