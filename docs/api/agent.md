# AI Agent

Source: `src/memframe_ai/`

`memframe_ai` adds an optional Pydantic AI agent fleet on top of memFrame.
After enabling the agent on a `MemFrame` instance, every dataset context
returned by that instance exposes `.achat(...)` and `.chat(...)` for
natural-language data work. The agent decomposes a prompt into a typed
sub-query plan, dispatches each sub-query to a specialist agent (selection,
cleaning, statistics, arithmetic, plotting, …), and returns typed response
blocks plus any generated plots.

## Install

```bash
pip install "memframe[ai]"
# or
uv add "memframe[ai]"
```

## Enable

Enable the agent once per `MemFrame` instance. Async:

```python
await mf.aenable_agent(
    provider="openai",
    model="gpt-5.5",
    api_key="sk-...",
)
```

Synchronous:

```python
mf.enable_agent(
    provider="openai",
    model="gpt-5.5",
    api_key="sk-...",
)
```

`provider` and `model` default to `"openai"` / `"gpt-5.5"` when omitted; pass
both explicitly when switching providers or pinning a model version.

### Supported providers

| Provider  | `provider=`    | Example model  |
| --------- | -------------- | -------------- |
| OpenAI    | `"openai"`     | `"gpt-5.5"`    |
| Anthropic | `"anthropic"`  | `"claude-..."` |
| Google    | `"google"`     | `"gemini-..."` |
| Ollama    | `"ollama"`     | `"llama3.2"`   |

```python
await mf.aenable_agent(
    provider="anthropic",
    model="claude-...",
    api_key="sk-ant-...",
)
```

## Chat

After enabling, every dataset context has `.achat()` and `.chat()`:

```python
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
print(result["answer"])   # one line per sub-query: `fn(args): ✓ (plot/table shown)` or `✗ <reason>`
print(result["table"])    # active table after the agent's work
print(result["plots"])    # any charts the agent built (id, title, spec_preview, full spec)
```

Synchronous:

```python
result = ds.chat("describe the score distribution")
```

## Dashboard

`adashboard()` (async) / `dashboard()` (sync) collapses the chat pipeline into a
one-shot visualization: it runs `.achat(sentence)` against the **active dataset**,
harvests the returned results and plots, then feeds them through the Dashboard
designer agent and renders a single full-screen Plotly-figure dashboard.

```python
await mf.aset_active("my_dataset")
await mf.aenable_agent(api_key="sk-...")

html = await mf.adashboard(
    "fillna C with mean then value counts of D and add B with the cleaned C",
)
# html is a complete dashboard page; in a notebook it renders inline,
# otherwise it is written to dashboard.html and opened in the browser.
```

- Returns the HTML string; pass `show=False` to skip the env-agnostic display
  (`smart_show` renders inline in notebooks/Colab/VSCode, opens the browser in a
  terminal).
- **Token safety**: only compact summaries (shape, columns, 2-row sample, capped
  Plotly spec) reach the dashboard LLM — never raw data values. The full
  DataFrames and figures stay local and are consumed solely by the renderer.
- **Type → widget contract**: a `DataFrame` always renders as a table, a pre-built
  `Figure` as a plot, and a `dict`/`number` as a metric (KPI). The whole dashboard
  is one responsive Plotly figure filling the viewport.

See [Dashboard Builder](dashboard.md) for the lower-level `DashboardManager` API
(build a design by hand, no agent required).

## How it works

0. **Guardrail (optional, on by default)**: before planning, a `GuardrailAgent`
   validates the prompt against the **active table's columns** (the domain
   context, rebuilt per query). It rejects cross-dataset confusion (e.g. asking
   about `ops2`'s columns while chatting on `ops1`) and off-topic requests (e.g.
   "who is the president of Kenya"). Disable via `AISettings(guardrails_enabled=False)`.
   A blocked query short-circuits gracefully: `chat()` returns a refusal answer,
   and `adashboard()`/`dashboard()` return a styled HTML page explaining why
   execution stopped (no exception is raised).
1. The prompt is sent to a Planner agent which produces a typed `SubQueryPlan`
   (a linked list of ordered sub-queries, each tagged with a specialist
   agent).
2. Independent sub-queries run in parallel; dependent sub-queries run
   sequentially with the session's domain context force-refreshed between
   steps so the next specialist sees any new columns produced by the prior
   step.
3. The session pins the active table for the duration of a chat so every
   specialist sees the same schema, even when transforms create transient
   tables.
4. Every tool call is routed through the public memFrame wrapper layer, so
   AI chat inherits the same two-level cache (audit lineage + replayable
   result tables) and the same backend abstraction as the interactive API.

Key source files:

- `src/memframe_ai/agents/analytics.py` — orchestrator + specialist fleet, packaging
- `src/memframe_ai/agents/planning.py` — Planner agent
- `src/memframe_ai/agents/dashboard.py` — Dashboard designer agent
- `src/memframe/dashboard/` — `DashboardManager`, renderer, design models
- `src/memframe_ai/sessions.py` — chat session state, pinned table, per-sub-query results
- `src/memframe_ai/gateway.py` — provider/model resolution
- `src/memframe_ai/observe.py` — model / tool call observability