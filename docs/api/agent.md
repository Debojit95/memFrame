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
print(result["answer"])   # natural-language summary
print(result["table"])    # active table after the agent's work
print(result["plots"])    # any charts the agent built
print(result["blocks"])   # per-tool response blocks
```

Synchronous:

```python
result = ds.chat("describe the score distribution")
```

## How it works

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

- `src/memframe_ai/agents/analytics.py` — orchestrator + specialist fleet
- `src/memframe_ai/agents/planning.py` — Planner agent
- `src/memframe_ai/sessions.py` — chat session state, pinned table
- `src/memframe_ai/gateway.py` — provider/model resolution
- `src/memframe_ai/format.py` — response block rendering
- `src/memframe_ai/observe.py` — model / tool call observability