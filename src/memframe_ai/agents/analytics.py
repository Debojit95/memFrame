import functools
import time
import asyncio

from pydantic_ai import Agent
from pydantic_ai.toolsets import FunctionToolset
from pydantic_ai.usage import UsageLimits
from pydantic_ai_harness import CodeMode

from memframe.utils.async_sync import async_to_sync

from memframe_ai.agents.planning import SubQuery
from memframe_ai.agents.guardrail import GuardrailAgent, GuardrailVerdict
from memframe_ai.config import AISettings
from memframe_ai.gateway import ModelGateway
from memframe_ai.instrument import span as _lf_span, flush_logfire
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
_SPECIALIST_MAX_REQUESTS = 8


def _code_mode() -> CodeMode:
    return CodeMode(tools={"code_mode": True}, max_retries=3)


def _specialist_prompt(name: str) -> str:
    return (
        f"You are the '{name}' analytics specialist. The active dataset is ALREADY "
        "LOADED — never claim data is missing.\n"
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


def _arg_summary(args: tuple, kwargs: dict) -> str:
    parts = [repr(a) for a in args] + [f"{k}={v!r}" for k, v in kwargs.items()]
    text = ", ".join(parts)
    return text if len(text) <= 120 else text[:117] + "..."


def _recorded(session, fn):
    @functools.wraps(fn)
    async def wrapped(*args, **kwargs):
        label = f"{fn.__name__}({_arg_summary(args, kwargs)})"
        t0 = time.perf_counter()
        try:
            result = await fn(*args, **kwargs)
        except Exception as exc:
            logger.warning("[tool] %s FAILED %.1fs %s: %s", label, time.perf_counter() - t0, type(exc).__name__, exc)
            session.record_subquery_result(
                label, {"ok": False, "hint": f"{type(exc).__name__}: {exc}"}
            )
            raise
        ok = result.get("ok") if isinstance(result, dict) else None
        logger.info("[tool] %s ok=%s %.1fs", label, ok, time.perf_counter() - t0)
        session.record_subquery_result(
            label, result if isinstance(result, dict) else {"ok": True, "result": result}
        )
        return result

    return wrapped


def _fmt_subquery(label: str, payload: dict) -> str:
    """Render one sub-query result as a single human line for `answer`."""
    if not isinstance(payload, dict) or not payload.get("ok"):
        msg = payload.get("hint") or payload.get("message") if isinstance(payload, dict) else None
        return f"{label}: ✗ {msg or 'failed'}"
    if payload.get("plot_id") is not None or "spec" in payload:
        return f"{label}: ✓ (plot shown)"
    val = payload.get("result")
    if val is None:
        return f"{label}: ✓"
    if isinstance(val, list):
        return f"{label}: ✓ (table shown)"
    text = repr(val)
    if len(text) > 120:
        text = text[:117] + "..."
    return f"{label}: ✓ {text}"


class AnalyticsAgent:

    def __init__(self, session, settings: AISettings):
        self._session = session
        self._settings = settings
        self._gateway = ModelGateway(settings)
        self._specialists: dict[str, Agent] = {}
        self._planner = None
        self._guardrail = None

    def _planner_agent(self):
        from memframe_ai.agents.planning import PlannerAgent
        if self._planner is None:
            self._planner = PlannerAgent(self._settings)
        return self._planner

    def _guardrail_agent(self):
        if self._guardrail is None:
            self._guardrail = GuardrailAgent(self._settings)
        return self._guardrail

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

    async def achat(self, prompt: str) -> dict:
        t0 = time.perf_counter()
        with _lf_span("achat", prompt=prompt[:200], session_id=self._session.session_id):
            await self._session.ensure()
            self._session.reset_subquery_results()
            self._session.reset_results()
            logger.info(
                "chat start session=%s table=%s.%s prompt='%s'",
                self._session.session_id, self._session.schema, self._session.table, prompt,
            )

            # Capture the BASE context for this chat. Forced refresh: every achat
            # call rebuilds context so it reflects current table state, and the
            # result becomes this chat's frozen base_ctx.
            base_ctx = await self._session.domain_context(lightweight=False, force_refresh=True)

            # Guardrail: reject invalid / off-topic / cross-dataset queries before
            # spending a planner + specialist pass. Fail-open on guardrail errors.
            if self._settings.guardrails_enabled:
                try:
                    with _lf_span("guardrail.verify", prompt=prompt[:200]):
                        verdict = await self._guardrail_agent().verify(
                            prompt, base_ctx, self._session.table
                        )
                except Exception as exc:  # pragma: no cover - defensive
                    logger.warning("guardrail error (fail-open): %s", exc)
                else:
                    if not verdict.is_valid:
                        logger.info("guardrail blocked: %s", verdict.reason)
                        flush_logfire()
                        return self._refusal(prompt, verdict)

            # Planner: one structured-output model call
            try:
                with _lf_span("planner.plan", prompt=prompt[:200]):
                    subquery_head = await self._planner_agent().plan_with_dependencies(prompt, base_ctx)
            except Exception as exc:  # pragma: no cover — UnexpectedModelBehavior after retries
                logger.warning("planner failed after retries/fallback: %s", exc)
                subquery_head = None

            # Execute sub-queries. The session table stays pinned to the original
            # ctx table for the whole chat; dependent steps force-refresh the
            # domain context on that original table, which now carries any new
            # columns produced by earlier steps.
            if subquery_head:
                self._session.pin_table()
                try:
                    with _lf_span("execute_subqueries"):
                        await self._execute_subqueries(subquery_head, base_ctx)
                finally:
                    self._session.unpin_table()
            else:
                logger.info("No sub-queries produced — nothing to execute")

            self._render_results()
            logger.info("chat done %.1fs", time.perf_counter() - t0)
            flush_logfire()
            return self._package()

    def _render_results(self) -> None:
        """Render stashed DataFrame results inline, from the main kernel loop.

        Full-df results are captured during execution (inside CodeMode tool
        dispatch, where IPython display does not propagate to the notebook) and
        shown here, after the run, in their entirety. Plots already render
        inline during execution via the plot specialist's direct tool path.
        """
        from memframe.utils.plot_renderer import display_df

        for df in self._session.results:
            display_df(df)

    async def _execute_subqueries(self, head: SubQuery, base_ctx: str) -> None:
        """Walk the linked list.

        The session table is pinned to the original ctx table (set in achat), so
        tools never advance it to transient tables. Independent sub-queries run
        in parallel against the frozen base context. Dependent sub-queries run
        sequentially and force-refresh the domain context, which is computed on
        the same original table — now including any columns created by prior
        steps.
        """
        independent: list[SubQuery] = []
        dependent: list[SubQuery] = []
        cur = head
        while cur:
            if cur.prev_depends:
                dependent.append(cur)
            else:
                independent.append(cur)
            cur = cur.next

        if independent:
            logger.info("Executing %d independent sub-queries in parallel", len(independent))
            await asyncio.gather(
                *[self._execute_one(q, base_ctx) for q in independent]
            )

        for q in dependent:
            logger.info("Executing dependent sub-query: %s", q.query[:120])
            fresh_ctx = await self._session.domain_context(force_refresh=True)
            await self._execute_one(q, fresh_ctx)

    async def _execute_one(self, sq: SubQuery, base_ctx: str) -> None:
        """Run one sub-query on its specialist agent with the fixed base_ctx."""
        specialist = self.specialist_agents().get(sq.agent)
        if specialist is None:
            logger.warning("No specialist agent for '%s' — skipping", sq.agent)
            return
        try:
            await specialist.run(
                f"{base_ctx}\n\nTask:\n{sq.query}",
                usage_limits=UsageLimits(request_limit=_SPECIALIST_MAX_REQUESTS),
            )
        except Exception as exc:
            logger.warning("Sub-query failed (%s): %s", sq.agent, exc)

    def _refusal(self, prompt: str, verdict: GuardrailVerdict) -> dict:
        """Build a chat response for a guardrail-blocked query (no planner run)."""
        reason = verdict.reason or "Request did not pass the query guardrail."
        return {
            "session_id": self._session.session_id,
            "answer": (
                f"Execution stopped by the query guardrail: {reason}. "
                "Please rephrase your request so it refers to the active dataset's columns."
            ),
            "table": self._session.table,
            "schema": self._session.schema,
            "result": None,
            "results": [],
            "values": [],
            "plots": [],
            "guardrail_blocked": True,
            "guardrail_reason": reason,
            "error": None,
        }

    def _package(self) -> dict:
        sep = "\n............\n"
        answer = sep.join(
            _fmt_subquery(label, payload)
            for label, payload in self._session.subquery_results
        )
        results = list(self._session.results)
        # ponytail: scalar/dict/list sub-query results are stored (jsonable) in
        # subquery_results but dropped from the structured response above. Surface
        # them so consumers like adashboard() can render them. DataFrames are
        # excluded (already in `results` as full frames) and plots (in `plots`).
        values = [
            (label, payload.get("result"))
            for label, payload in self._session.subquery_results
            if payload.get("ok")
            and not payload.get("is_dataframe")
            and payload.get("plot_id") is None
            and "spec" not in payload
            and payload.get("result") is not None
        ]
        resp = {
            "session_id": self._session.session_id,
            "answer": answer,
            "table": self._session.table,
            "schema": self._session.schema,
            "result": results[-1] if results else None,
            "results": results,
            "values": values,
            "plots": [
                # ponytail: full spec shipped for re-render; spec_preview for lightweight clients
                {
                    "id": pid,
                    "title": p["title"],
                    "spec_preview": plot.plot_spec_preview(p["spec"]),
                    "spec": p["spec"],
                }
                for pid, p in self._session.plots.items()
            ],
            "error": None,
        }
        return resp

    chat = async_to_sync(achat)


def agent_for(session) -> AnalyticsAgent:
    # ponytail: rebuild the agent/model whenever the live AI settings change
    # (e.g. mf.enable_agent re-run with a different provider/model/api_key/base_url).
    # The Session caches settings at first achat, so compare against
    # memframe._ai_settings rather than session.settings; otherwise re-enabling
    # silently keeps the stale model/provider. Conversation history is preserved.
    live = session.memframe._ai_settings
    agent = getattr(session, "_agent", None)
    if agent is None or agent._settings is not live:
        agent = AnalyticsAgent(session, live)
        session._agent = agent
        session.settings = live
    return agent
