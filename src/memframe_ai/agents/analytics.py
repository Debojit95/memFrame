import functools
import time
from typing import Any, Optional

from pydantic_ai import Agent
from pydantic_ai.toolsets import FunctionToolset
from pydantic_ai.usage import UsageLimits
from pydantic_ai_harness import CodeMode

from memframe.utils.async_sync import async_to_sync

from memframe_ai.config import AISettings
from memframe_ai.format import classify_block, render_blocks
from memframe_ai.gateway import ModelGateway
from memframe_ai.observe import logger, make_hooks
from memframe_ai.tools import arithmetic, clean, context, inspect, plot, select, stats, upload

SPECIALISTS = {
    "context": ([context, upload], True),
    "inspect": ([inspect], True),
    "select": ([select], True),
    "clean": ([clean], True),
    "stats": ([stats], True),
    "arithmetic": ([arithmetic], True),
    "plot_bar": ("bar", False),
    "plot_line": ("line", False),
    "plot_pie": ("pie", False),
    "plot_scatter": ("scatter", False),
    "plot_scatter_3d": ("scatter_3d", False),
    "plot_bar_polar": ("bar_polar", False),
}

_DELEGATE_HELP = {
    "context": "list or switch datasets, upload a file, report the active table",
    "inspect": "preview rows, describe columns, check nulls",
    "select": "filter rows, pick columns, take slices",
    "clean": "drop missing/duplicate/outlier rows, fill nulls, remap values",
    "stats": "value counts, correlations, outlier statistics",
    "arithmetic": "add, subtract, multiply or divide columns/scalars into a new column",
    "plot_bar": "make a bar chart",
    "plot_line": "make a line chart",
    "plot_pie": "make a pie chart",
    "plot_scatter": "make a scatter chart",
    "plot_scatter_3d": "make a 3D scatter chart",
    "plot_bar_polar": "make a polar bar chart",
}

_PLOT_DOC = {
    "bar": "Build a bar chart from the active table. x: category column, y: value column.",
    "line": "Build a line chart from the active table. x: x-axis column, y: y-axis column.",
    "pie": "Build a pie chart from the active table. x: names column, y: values column.",
    "scatter": "Build a scatter chart from the active table. x, y: numeric columns.",
    "scatter_3d": "Build a 3D scatter chart from the active table. x, y, z: numeric columns.",
    "bar_polar": "Build a polar bar chart from the active table. x: theta column, y: r column.",
}

_CTX_SHORT = 120
_ORCHESTRATOR_MAX_REQUESTS = 15
_SPECIALIST_MAX_REQUESTS = 8


def _code_mode() -> CodeMode:
    return CodeMode(tools={"code_mode": True}, max_retries=3)


def _specialist_prompt(name: str) -> str:
    return (
        f"You are the '{name}' analytics specialist. The active dataset is ALREADY "
        "LOADED \u2014 never claim data is missing.\n"
        "To complete the task you MUST actually call the data functions. They are "
        "available inside the `run_code` sandbox as async functions, for example:\n"
        "    result = await fillna(column='C', mode='median')\n"
        "    result\n"
        "Always `await` them and put the final result on the last line. Do NOT answer "
        "from the provided context alone, and do NOT claim an operation succeeded "
        "unless you actually called the function and received its result. If a call "
        "fails, report the error, retry once, then stop.\n"
        "PRESERVE THE FULL DATASET: never drop, rename, or collapse any column not "
        "explicitly asked to be removed. A column operation adds or updates a column; "
        "everything else in the table stays intact. Always report the columns you "
        "actually used/observe. Do not invent placeholder operations (e.g. dividing "
        "by 1 to copy a column, or 'COALESCE(...) AS x') and never embed SQL "
        "expressions into column arguments."
    )


def _orchestrator_prompt(table: str, schema: str) -> str:
    return (
        "You orchestrate data-analytics agents over one active dataset. "
        f"The active dataset is ALREADY loaded: table '{table}' "
        f"in schema '{schema}'. Never claim no data is loaded \u2014 "
        "the table above is active and usable. Delegate to the specialist "
        "agents via the run_* tools (run_context, run_inspect, run_select, "
        "run_clean, run_stats, run_arithmetic, run_plot_*) and compose their "
        "answers. Never invent data: call the tools to get real values.\n"
        "Perform each requested operation EXACTLY ONCE via the run_* "
        "delegates. Do not re-run an operation you have already executed. "
        "To confirm the final state, make a single read-only inspection "
        "(run_select or run_inspect) at the end rather than repeating "
        "mutations.\n"
        "Do not drop or omit columns you were not asked to remove. When a "
        "request has multiple steps, order them so no step depends on a "
        "column a prior step may not carry."
    )


def _plot_tool(session, plot_type: str):
    base = {f.__name__: f for f in plot.tools(session)}["plot"]

    async def make_plot(
        x: str,
        y: str | None = None,
        z: str | None = None,
        color: str | None = None,
        title: str | None = None,
    ) -> dict:
        return await base(
            plot_type=plot_type, x=x, y=y, z=z, color=color, title=title
        )

    make_plot.__name__ = plot_type
    make_plot.__doc__ = _PLOT_DOC[plot_type]
    return make_plot


def _make_delegate(agent: Agent, name: str, help_text: str, get_context=None):
    async def delegate(instruction: str) -> str:
        t0 = time.perf_counter()
        ctx = (await get_context()) if get_context is not None else ""
        full = f"{ctx}\n\nTask:\n{instruction}" if ctx else instruction
        logger.info("delegate run_%s ctx_len=%d task='%s'", name, len(ctx), instruction[:_CTX_SHORT])
        try:
            result = await agent.run(
                full, usage_limits=UsageLimits(request_limit=_SPECIALIST_MAX_REQUESTS)
            )
            logger.info(
                "delegate run_%s done %.1fs usage=requests=%s",
                name, time.perf_counter() - t0,
                getattr(getattr(result, "usage", None), "requests", "?"),
            )
            return str(result.output)
        except Exception as exc:
            logger.warning(
                "delegate run_%s failed %.1fs %s: %s",
                name, time.perf_counter() - t0, type(exc).__name__, exc,
            )
            return f"{name} agent failed: {type(exc).__name__}: {exc}"

    delegate.__name__ = f"run_{name}"
    delegate.__doc__ = (
        f"Delegate to the {name} agent to {help_text}. Returns its answer as text."
    )
    return delegate


def _arg_summary(args: tuple, kwargs: dict) -> str:
    parts = [repr(a) for a in args] + [f"{k}={v!r}" for k, v in kwargs.items()]
    text = ", ".join(parts)
    return text if len(text) <= 120 else text[:117] + "..."


def _recorded(session, fn):
    """Wrap a specialist tool to record each execution as a response block."""

    @functools.wraps(fn)
    async def wrapped(*args, **kwargs):
        label = f"{fn.__name__}({_arg_summary(args, kwargs)})"
        t0 = time.perf_counter()
        try:
            result = await fn(*args, **kwargs)
        except Exception as exc:
            logger.warning("[tool] %s FAILED %.1fs %s: %s", label, time.perf_counter() - t0, type(exc).__name__, exc)
            session.record_block(
                {
                    "query": label,
                    "type": "error",
                    "message": f"{type(exc).__name__}: {exc}",
                }
            )
            raise
        ok = result.get("ok") if isinstance(result, dict) else None
        logger.info("[tool] %s ok=%s %.1fs", label, ok, time.perf_counter() - t0)
        session.record_block(classify_block(label, result))
        return result

    return wrapped


class AnalyticsAgent:
    """Specialist agents + delegating orchestrator; all built lazily per session."""

    def __init__(self, session, settings: AISettings):
        self._session = session
        self._settings = settings
        self._gateway = ModelGateway(settings)
        self._specialists: dict[str, Agent] = {}
        self._orchestrator: Optional[Agent] = None
        self._classifier = None

    def _classifier_agent(self):
        from memframe_ai.agents.intent import IntentClassifier

        if self._classifier is None:
            self._classifier = IntentClassifier(self._settings)
        return self._classifier

    def specialist_agents(self) -> dict[str, Agent]:
        if self._specialists:
            return self._specialists
        for name, (tools, use_code_mode) in SPECIALISTS.items():
            raw_fns = (
                [_plot_tool(self._session, tools)]
                if isinstance(tools, str)
                else [fn for module in tools for fn in module.tools(self._session)]
            )
            tool_fns = [_recorded(self._session, fn) for fn in raw_fns]
            toolset = FunctionToolset(tools=tool_fns).with_metadata(code_mode=True)
            self._specialists[name] = Agent(
                self._gateway.model(),
                name=name,
                system_prompt=_specialist_prompt(name),
                toolsets=[toolset],
                capabilities=[make_hooks(name), _code_mode()] if use_code_mode else [make_hooks(name)],
            )
        return self._specialists

    def orchestrator(self) -> Agent:
        if self._orchestrator is None:
            specialists = self.specialist_agents()
            delegates = [
                _make_delegate(
                    specialists[name],
                    name,
                    help_text,
                    get_context=self._session.domain_context,
                )
                for name, help_text in _DELEGATE_HELP.items()
            ]
            self._orchestrator = Agent(
                self._gateway.model(),
                name="orchestrator",
                system_prompt=_orchestrator_prompt(self._session.table, self._session.schema),
                toolsets=[FunctionToolset(tools=delegates).with_metadata(code_mode=True)],
                capabilities=[make_hooks("orchestrator"), _code_mode()],
            )
        return self._orchestrator

    async def achat(self, prompt: str) -> dict:
        t0 = time.perf_counter()
        await self._session.ensure()
        logger.info(
            "chat start session=%s table=%s.%s prompt='%s'",
            self._session.session_id, self._session.schema, self._session.table, prompt,
        )
        intent = await self._classifier_agent().classify(prompt)
        ctx = await self._session.domain_context()
        logger.info("chat ctx_len=%d intent=%s", len(ctx), _render_intent(intent))
        agent = self.orchestrator()
        # ponytail: per-session lock, serializes concurrent chats on one conversation
        async with self._session.lock:
            self._session.reset_blocks()
            self._session._pinned_ctx = ctx
            try:
                result = await agent.run(
                    f"{ctx}\n\n{_render_intent(intent)}",
                    usage_limits=UsageLimits(request_limit=_ORCHESTRATOR_MAX_REQUESTS),
                )
            except Exception as exc:
                logger.warning("chat orchestrator FAILED %.1fs %s: %s", time.perf_counter() - t0, type(exc).__name__, exc)
                return {
                    "session_id": self._session.session_id,
                    "answer": "",
                    "blocks": [],
                    "plots": [],
                    "table": self._session.table,
                    "schema": self._session.schema,
                    "error": f"{type(exc).__name__}: {exc}",
                }
            finally:
                self._session._pinned_ctx = None
        logger.info("chat done %.1fs", time.perf_counter() - t0)
        return self._package(result)

    def _package(self, result: Any) -> dict:
        blocks = list(self._session.blocks)
        output = result.output if isinstance(result.output, str) else str(result.output)
        return {
            "session_id": self._session.session_id,
            "answer": render_blocks(blocks) if blocks else output,
            "blocks": blocks,
            "plots": [
                {"id": pid, "title": p["title"], "spec": p["spec"]}
                for pid, p in self._session.plots.items()
            ],
            "table": self._session.table,
            "schema": self._session.schema,
            "error": None,
        }

    chat = async_to_sync(achat)


def _render_intent(intent) -> str:
    parts = [f"Primary task: {intent.primary_task}"]
    if intent.targets:
        parts.append("Target specialists: " + ", ".join(intent.targets))
    if intent.focus_columns:
        parts.append("Focus columns: " + ", ".join(intent.focus_columns))
    if intent.plot_type:
        parts.append(f"Plot type: {intent.plot_type}")
    parts.append(f"User goal: {intent.user_goal}")
    return "\n".join(parts)


def agent_for(session) -> AnalyticsAgent:
    """Get (or lazily build) the agent fleet bound to a session."""
    agent = getattr(session, "_agent", None)
    if agent is None:
        agent = AnalyticsAgent(session, session.settings)
        session._agent = agent
    return agent
