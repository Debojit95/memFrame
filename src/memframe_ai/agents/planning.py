"""Planning agent: decomposes a user prompt into ordered sub-queries.

Uses structured output (output_type) so the model produces the full plan in a
single model request — no tool-call round trips, no Planning capability.
"""

import time
from dataclasses import dataclass
from typing import Optional

from pydantic import BaseModel
from pydantic_ai import Agent


class SubQueryNode(BaseModel):
    """One sub-query step returned by the planner."""
    query: str
    prev_depends: bool = False
    agent: str = ""


class SubQueryPlan(BaseModel):
    """Structured output: the ordered list of sub-queries."""
    sub_queries: list[SubQueryNode]


@dataclass
class SubQuery:
    """Linked-list node used at execution time."""
    query: str
    prev_depends: bool = False
    agent: Optional[str] = None
    next: Optional["SubQuery"] = None


_AGENTS = (
    "clean — fill/drop nulls, remap values, deduplicate\n"
    "inspect — preview rows, describe columns, check nulls\n"
    "select — filter rows, pick columns, take slices\n"
    "stats — value counts, correlations, outlier statistics\n"
    "arithmetic — add, subtract, multiply or divide columns/scalars\n"
    "plot_bar, plot_line, plot_pie, plot_scatter, plot_scatter_3d, plot_bar_polar"
)

_PLANNER_SYSTEM = (
    "You decompose a user request into ordered sub-queries over an ALREADY-LOADED table. "
    "You are given a TABLE CONTEXT with column names and types. Use it — never guess.\n\n"
    "Rules:\n"
    "- Each sub-query maps to exactly one specialist agent (use the agent name directly).\n"
    "- For simple arithmetic/selection, produce ONE sub-query — skip inspection/stats.\n"
    "- If a sub-query needs the RESULT of a prior sub-query, set prev_depends=true.\n"
    "- Independent sub-queries can run in parallel (prev_depends=false).\n"
    "- PRESERVE ALL COLUMNS: never drop columns the user did not ask to remove.\n\n"
    f"Available agents:\n{_AGENTS}\n\n"
    "Return a SubQueryPlan with a list of SubQueryNode objects."
)


class PlannerAgent:
    """Produces a SubQueryPlan from a user prompt + table context in one model call."""

    def __init__(self, settings):
        self._settings = settings
        self._agent: Optional[Agent] = None

    def _build(self) -> Agent:
        if self._agent is None:
            from memframe_ai.gateway import ModelGateway
            self._agent = Agent(
                ModelGateway(self._settings).model(),
                name="planner",
                system_prompt=_PLANNER_SYSTEM,
                output_type=SubQueryPlan,
            )
        return self._agent

    async def plan_with_dependencies(self, prompt: str, context: str) -> Optional[SubQuery]:
        """One model request → structured SubQueryPlan → linked list."""
        t0 = time.perf_counter()
        agent = self._build()
        logger.info("planner prompt='%s'", prompt[:120])
        result = await agent.run(f"{context}\n\nUser request:\n{prompt}")
        plan: SubQueryPlan = result.output
        logger.info(
            "planner done %.1fs steps=%d input_tokens=%s output_tokens=%s",
            time.perf_counter() - t0,
            len(plan.sub_queries),
            getattr(getattr(result, "usage", None), "input_tokens", "?"),
            getattr(getattr(result, "usage", None), "output_tokens", "?"),
        )
        if not plan.sub_queries:
            return None
        head = self._to_linkedlist(plan.sub_queries)
        self._log_subquery_linkedlist(head)
        return head

    @staticmethod
    def _to_linkedlist(nodes: list[SubQueryNode]) -> SubQuery:
        head = SubQuery(query=nodes[0].query, prev_depends=False, agent=nodes[0].agent)
        cur = head
        for node in nodes[1:]:
            cur.next = SubQuery(query=node.query, prev_depends=node.prev_depends, agent=node.agent)
            cur = cur.next
        return head

    @staticmethod
    def _log_subquery_linkedlist(head: SubQuery) -> None:
        cur = head
        idx = 0
        while cur:
            logger.info(
                "SubQuery[%d]: agent='%s' prev_depends=%s query='%s'",
                idx, cur.agent, cur.prev_depends, cur.query[:120],
            )
            cur = cur.next
            idx += 1


# ── kept for backward compat if anything imports render_plan ────────────
def render_plan(items) -> str:
    return "(structured planner — render_plan unused)"


from memframe_ai.observe import logger
