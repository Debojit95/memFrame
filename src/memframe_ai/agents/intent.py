from typing import Optional

from pydantic import BaseModel, Field
from pydantic_ai import Agent

from memframe_ai.config import AISettings
from memframe_ai.gateway import ModelGateway
from memframe_ai.observe import logger, make_hooks

_PLOT_TYPES = {"bar", "line", "pie", "scatter", "scatter_3d", "bar_polar"}


class IntentResult(BaseModel):
    primary_task: str = Field(
        description="One of: inspect, select, clean, stats, plot, context, combined."
    )
    targets: list[str] = Field(
        default_factory=list,
        description="run_* specialist names to execute, e.g. run_inspect, run_plot_bar.",
    )
    focus_columns: list[str] = Field(
        default_factory=list, description="Columns the user asked about."
    )
    requires_plot: bool = Field(default=False, description="True if the user wants a chart.")
    plot_type: Optional[str] = Field(
        default=None, description="bar, line, pie, scatter, scatter_3d, or bar_polar."
    )
    user_goal: str = Field(
        description="Restate what the user wants, as a concrete instruction."
    )


class IntentClassifier:
    """Receives the raw user prompt and returns a structured routing intent."""

    def __init__(self, settings: AISettings):
        self._agent = Agent(
            ModelGateway(settings).model(),
            name="intent_classifier",
            system_prompt=(
                "You classify a raw user prompt about one ALREADY-LOADED analytics table "
                "into a structured intent for a routing orchestrator. Never claim data is "
                "missing — the active table is loaded and usable. Pick the specialist "
                "targets (run_context, run_inspect, run_select, run_clean, run_stats, "
                "run_arithmetic, run_plot_bar, run_plot_line, run_plot_pie, run_plot_scatter, "
                "run_plot_scatter_3d, run_plot_bar_polar) that fulfil the request, list "
                "any columns mentioned, and set requires_plot/plot_type when the user "
                "wants a chart."
            ),
            output_type=IntentResult,
            capabilities=[make_hooks("intent_classifier")],
        )

    async def classify(self, prompt: str) -> IntentResult:
        import time

        t0 = time.perf_counter()
        logger.info("intent_classifier prompt='%s'", prompt)
        result = await self._agent.run(prompt)
        logger.info(
            "intent_classifier done %.1fs intent=%s",
            time.perf_counter() - t0,
            result.output,
        )
        return result.output
