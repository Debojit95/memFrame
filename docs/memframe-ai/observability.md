# Observability (Logfire)

memFrame can send the **entire** agent pipeline to [Logfire](https://logfire.pydantic.dev/)
— every agent run, LLM call, and tool call, plus a top-level trace per request and
spans around the guardrail, planner, and sub-query execution. This is opt-in and
fail-open: if the `logfire` extra isn't installed, the token is missing, or
configuration errors, logging just continues normally and your chat is never
blocked.

Install the extra:

```bash
pip install "memframe[logfire]"
# or
uv add "memframe[logfire]"
```

memFrame uses a **Bring-Your-Own-Key (BYOK)** model: you supply your own
Logfire token and telemetry goes to *your* project — nothing is hardcoded or
committed. Install the extra, then enable it when you turn the agent on (pass a
token, or set the `LOGFIRE_TOKEN` environment variable):

```bash
pip install "memframe[logfire]"
# or: uv add "memframe[logfire]"   (pulls logfire[system-metrics])
```

```python
await mf.aenable_agent(
    provider="openai",
    model="gpt-5.5",
    api_key="sk-...",
    logfire_enabled=True,
    logfire_token="<your-logfire-token>",   # or rely on LOGFIRE_TOKEN env
    logfire_environment="dev",
)
```

**Local dev without a token (optional, not committed):** after
`uv run logfire auth` (browser OAuth) you can bind this repo to a project with
`uv run logfire projects use --org <your-org> <your-project>`; then
`logfire.configure()` needs no token. Do not commit the resulting
`[tool.logfire]` binding — it pins the repo to a personal project. For CI /
headless runs, set `LOGFIRE_TOKEN` instead.

What gets traced automatically:

- `instrument_pydantic_ai()` traces every Pydantic-AI agent — the **guardrail**,
  the **planner**, each **specialist**, and the **dashboard designer** — with
  child spans for each LLM request/response and tool call.
- `instrument_logging()` (newer Logfire) forwards the standard `memframe.ai`
  log lines (model requests, tool calls, guardrail verdicts) into Logfire as
  logs.
- `instrument_system_metrics()` (via the `system-metrics` extra) populates the
  Logfire **Hosts** view (CPU / memory / disk) for the process.
- An `adashboard` / `achat` span wraps each user request, with nested
  `guardrail.verify`, `planner.plan`, and `execute_subqueries` spans — so the
  whole flow shows as one coherent trace in the Logfire UI.

Open <https://logfire.pydantic.dev/> to explore the traces.
