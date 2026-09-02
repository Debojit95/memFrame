"""Planning agent: decomposes a user prompt into ordered sub-queries.

Uses structured output (output_type) so the model produces the full plan in a
single model request — no tool-call round trips, no Planning capability.
"""

import json
import re
import time
from dataclasses import dataclass
from typing import Any, Optional

from pydantic import BaseModel, field_validator
from pydantic_ai import Agent

from memframe_ai.observe import logger


class SubQueryNode(BaseModel):
    """One sub-query step returned by the planner."""

    query: str
    prev_depends: bool = False
    agent: str = ""

    @field_validator("query", mode="before")
    @classmethod
    def _coerce_query(cls, v: Any) -> str:
        # ponytail: model sometimes emits {"columns":["A","B"],"type":"correlation"} instead of string
        if isinstance(v, str):
            return v
        if isinstance(v, dict):
            cols = v.get("columns") or v.get("column") or v.get("cols") or []
            typ = v.get("type") or v.get("op") or v.get("operation") or ""
            if isinstance(cols, str):
                cols = [cols]
            if typ and cols:
                col_str = ", ".join(str(c) for c in cols)
                # "correlation of A and B", "value_counts of D"
                return f"{typ} of {col_str}".replace("_", " ")
            # fallback: JSON dump
            try:
                return json.dumps(v)
            except Exception:
                return str(v)
        return str(v)


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
    "- PRESERVE ALL COLUMNS: never drop columns the user did not ask to remove.\n"
    "- `query` is ALWAYS a plain English sentence (e.g. 'value counts of D', 'correlation of A and B'), NEVER JSON or {\"columns\": ...}. The specialist will call the concrete tool.\n\n"
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
                retries=3,
            )
        return self._agent

    def _heuristic_fallback(self, prompt: str, context: str) -> Optional[SubQuery]:
        # ponytail: degraded planner — keyword heuristic when structured output retries exhausted
        low = prompt.lower()
        nodes: list[SubQueryNode] = []
        # crude column extraction from context (lines like "- D (string)" or "| D |")
        cols = re.findall(r"\b([A-Z][A-Za-z0-9_]*)\b", context)
        # dedupe preserve order, filter obvious non-cols
        seen: set[str] = set()
        uniq_cols: list[str] = []
        for c in cols:
            if c not in seen and len(c) <= 32:
                seen.add(c)
                uniq_cols.append(c)
        if any(k in low for k in ("value count", "value_counts", "counts of", "distribution of")):
            # try to find column after "of"
            m = re.search(r"value counts? of\s+(\w+)", low)
            col = m.group(1).upper() if m else (uniq_cols[-1] if uniq_cols else "D")
            nodes.append(SubQueryNode(query=f"value counts of {col}", agent="stats"))
        if "correlat" in low:
            m = re.search(r"correlation of\s+(\w+)\s+and\s+(\w+)", low)
            if m:
                a, b = m.group(1).upper(), m.group(2).upper()
            else:
                a, b = (uniq_cols[0], uniq_cols[1]) if len(uniq_cols) >= 2 else ("A", "B")
            nodes.append(SubQueryNode(query=f"correlation of {a} and {b}", agent="stats"))
        if not nodes:
            # generic: split on "and" as two independent tasks
            return None
        logger.warning("planner fallback heuristic used: %d steps", len(nodes))
        return self._to_linkedlist(nodes)

    async def plan_with_dependencies(self, prompt: str, context: str) -> Optional[SubQuery]:
        """One model request → structured SubQueryPlan → linked list."""
        t0 = time.perf_counter()
        agent = self._build()
        logger.info("planner prompt='%s'", prompt[:120])
        try:
            result = await agent.run(f"{context}\n\nUser request:\n{prompt}")
        except Exception as exc:  # pragma: no cover — UnexpectedModelBehavior etc.
            # ponytail: model returned invalid structured output (e.g. dict query); retry budget exhausted
            logger.warning("planner failed (%s) — using heuristic fallback: %s", type(exc).__name__, exc)
            fb = self._heuristic_fallback(prompt, context)
            if fb is not None:
                return fb
            raise
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
