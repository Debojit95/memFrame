"""Dashboard designer agent: turns result summaries into a DashboardDesign.

Uses structured output (output_type) so the model produces the full design in
a single request — mirroring ``agents/planning.py`` (PlannerAgent). No tools,
no Planning capability.
"""

from typing import List, Optional

from pydantic_ai import Agent

from memframe_ai.config import AISettings
from memframe_ai.observe import logger

from memframe.dashboard.models import DashboardDesign


_DASHBOARD_SYSTEM = (
    "You are a senior data-visualization dashboard designer. "
    "You receive summaries of several already-computed query results and must "
    "design a compact, coherent dashboard layout. You only design — results are "
    "fixed.\n\n"
    "RULES:\n"
    "- Maximum TWO widgets per row. Give each widget a col_span (1-12) so no row "
    "exceeds 12 and no row holds more than 2 widgets. Use 6+6 for balanced pairs, "
    "or 8+4 / 10+2 when one widget deserves emphasis.\n"
    "- For a result that is a pre-built Plotly figure, set kind='plot', "
    "chart_type='keep_existing', and only set title / width / height / axis titles.\n"
    "- For a result that is a DataFrame with no figure, choose the best chart_type "
    "(bar/line/scatter/pie/histogram/box) and set x and y to real column names; "
    "or set kind='table' if it is wide or best shown raw.\n"
    "- For a result that is a dict or a single number, set kind='metric' and choose "
    "a prefix/suffix (e.g. '$', '%') and decimal_places.\n"
    "- Assign descriptive titles and axis labels derived from the query and column "
    "names (clean them: 'sales_amount' -> 'Sales Amount').\n"
    "- Sizing: wide time-series -> width>=800, height~400; simple bar/pie -> 500x400; "
    "metric cards -> height<=220. Keep width 300-1200, height 250-800.\n"
    "- Place KPI / metric cards and headline numbers at the top. Order widgets to "
    "tell a story.\n"
    "- Each widget's `result_index` MUST equal the 'Result N:' index of the summary "
    "it visualizes. Never invent indices beyond those provided; every widget maps "
    "to exactly one Result.\n"
    "Return the complete DashboardDesign."
)


def _build_prompt(summaries: List[str]) -> str:
    header = "Design a dashboard from the following result summaries:\n\n"
    body = "\n".join(f"Result {i}: {s}" for i, s in enumerate(summaries))
    return header + body


class DashboardAgent:
    """Produces a DashboardDesign from result summaries in one model call."""

    def __init__(self, settings: AISettings):
        self._settings = settings
        self._agent: Optional[Agent] = None

    def _build(self) -> Agent:
        if self._agent is None:
            from memframe_ai.gateway import ModelGateway

            self._agent = Agent(
                ModelGateway(self._settings).model(),
                name="dashboard_designer",
                system_prompt=_DASHBOARD_SYSTEM,
                output_type=DashboardDesign,
            )
        return self._agent

    async def design(self, summaries: List[str]) -> DashboardDesign:
        agent = self._build()
        logger.info("dashboard_designer prompts=%d", len(summaries))
        result = await agent.run(_build_prompt(summaries))
        plan: DashboardDesign = result.output
        logger.info("dashboard_designer done widgets=%d", len(plan.widgets))
        return plan
