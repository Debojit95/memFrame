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

## Guardrail

Source: `src/memframe_ai/agents/guardrail.py`

`GuardrailAgent` validates a user prompt against the **active table's columns
before** the planner runs. It is a single structured-output model call with no
tools — it never touches your data, it only reasons about whether the request is
a valid analytics task for the table you are currently chatting on.

### When it runs

The guardrail lives inside `AnalyticsAgent.achat()`, so **both** `chat()` and
`adashboard()` are gated by it. It receives `base_ctx` — the domain context
rebuilt on every query via `build_domain_context`, which lists the active
table's real columns and types. Because the context is rebuilt per query, a
request about dataset `ops2`'s columns while you are on `ops1` is caught (those
columns are absent from `ops1`'s context).

### Verdict

The agent returns a `GuardrailVerdict`:

| Field | Type | Meaning |
| --- | --- | --- |
| `is_valid` | `bool` | Whether the request may proceed to planning |
| `reason` | `str` | Short explanation (used in the graceful message) |
| `missing_terms` | `list[str]` | Request keywords absent from the active table's columns |

### What it blocks / allows

- **Blocked** — CROSS-DATASET (references another table/dataset or its columns
  that are absent from the active table), OFF-TOPIC (not a data task at all, e.g.
  `"who is the president of Kenya"`), or UNRELATED (entities with no plausible
  relation to the table).
- **Allowed** — paraphrases, aggregations, filtering, cleaning, and visualization
  over the table's present columns. The guardrail is deliberately lenient: only
  reject when there is clearly no relation to the active table's data.

### Configuration

`guardrails_enabled` lives on `AISettings` and defaults to `True`:

```python
from memframe_ai import AISettings

# disable the guardrail entirely
await mf.aenable_agent(
    provider="openai", model="gpt-5.5", api_key="sk-...",
    guardrails_enabled=False,
)
```

It is also **fail-open**: if the guardrail model call itself errors, the failure
is logged and the request proceeds to planning — the guardrail can never block a
legitimate query because of an infrastructure hiccup.

### Blocked queries are graceful (no exception)

A blocked query short-circuits without raising:

- `chat()` / `achat()` return a refusal answer dict —
  `guardrail_blocked: True`, `guardrail_reason`, and empty `results` / `plots` /
  `values`.
- `adashboard()` / `dashboard()` return a **styled HTML page** (built by
  `render_guardrail_blocked(reason)` in `src/memframe/dashboard/render.py`)
  explaining why execution stopped. With `show=True` it still renders inline in a
  notebook / opens in the browser.

```python
# active table "ops1" has columns a, b, c
await ds.achat("who is the president of Kenya")
# -> refused: off-topic, no exception raised

await ds.achat("average b grouped by c")
# -> valid, proceeds to planning

await ds.achat("sum the revenue column of ops2")
# -> blocked: 'revenue' / 'ops2' are not columns of ops1
```

## How it works

0. **Guardrail** gates the prompt first — see [Guardrail](#guardrail). A blocked
   query short-circuits gracefully (no exception): `chat()` returns a refusal
   answer, `adashboard()`/`dashboard()` return a styled HTML page.
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

See [Observability (Logfire)](observability.md) for full tracing setup, the
bring-your-own-key model, and what gets traced automatically.

## Key source files:

- `src/memframe_ai/agents/analytics.py` — orchestrator + specialist fleet, packaging
- `src/memframe_ai/agents/planning.py` — Planner agent
- `src/memframe_ai/agents/guardrail.py` — pre-plan query validation (GuardrailAgent)
- `src/memframe_ai/agents/dashboard.py` — Dashboard designer agent
- `src/memframe/dashboard/` — `DashboardManager`, renderer, design models
- `src/memframe_ai/sessions.py` — chat session state, pinned table, per-sub-query results
- `src/memframe_ai/gateway.py` — provider/model resolution
- `src/memframe_ai/observe.py` — model / tool call observability