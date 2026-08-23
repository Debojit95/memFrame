"""Query guardrail agent: validates a user prompt against the active table

BEFORE the planner runs. One structured-output model call, no tools. Rejects
cross-dataset confusion (asking about another table's columns while on a
different active dataset) and off-topic requests, while staying lenient on
paraphrases/aggregations over the table's own columns.
"""

from typing import Optional

from pydantic import BaseModel, Field
from pydantic_ai import Agent

from memframe_ai.config import AISettings
from memframe_ai.observe import logger


class GuardrailVerdict(BaseModel):
    """Structured output: is the request valid for the active table?"""

    is_valid: bool
    reason: str = ""
    missing_terms: list[str] = Field(default_factory=list)


_GUARDRAIL_SYSTEM = (
    "You are a QUERY GUARDRAIL for a data-analytics assistant. The assistant "
    "operates on exactly ONE table at a time — the ACTIVE TABLE. You do NOT "
    "answer questions; you only decide whether a user request is a VALID "
    "analytics request for the ACTIVE TABLE.\n\n"
    "You are given the ACTIVE TABLE CONTEXT, which names the table and lists "
    "its real columns (with types). You also receive the user's request.\n\n"
    "A request is VALID when ALL of:\n"
    "- It is a data/analytics task (querying, filtering, aggregating, cleaning, "
    "computing on, or visualizing the table's data), AND\n"
    "- Its subject plausibly relates to the ACTIVE TABLE — it references "
    "columns/entities that exist in (or are clear synonyms of) the table's "
    "columns, or is a reasonable operation on the table as a whole (e.g. "
    "'show all rows', 'describe the table').\n\n"
    "A request is INVALID when ANY of:\n"
    "- CROSS-DATASET: it references a different table/dataset than the ACTIVE "
    "TABLE (e.g. mentions another dataset's name or columns absent from the "
    "ACTIVE TABLE's column list). Put the missing column/dataset names in "
    "missing_terms.\n"
    "- OFF-TOPIC: it is not a data task at all (general knowledge, chit-chat, "
    "or code unrelated to this table's data, e.g. 'who is the president of "
    "Kenya').\n"
    "- UNRELATED: it asks about entities clearly absent from the table with no "
    "plausible relation.\n\n"
    "Be LENIENT: paraphrases, aggregations, and analytic intent over present "
    "columns are valid even if the wording differs. Only reject when there is "
    "clearly no relation to the ACTIVE TABLE's data, or it is plainly "
    "off-topic.\n\n"
    "Return a GuardrailVerdict: is_valid (bool), reason (short explanation), "
    "missing_terms (list of request keywords absent from the ACTIVE TABLE's "
    "columns; empty if valid)."
)


class GuardrailAgent:
    """Produces a GuardrailVerdict from a user prompt + table context in one call."""

    def __init__(self, settings: AISettings):
        self._settings = settings
        self._agent: Optional[Agent] = None

    def _build(self) -> Agent:
        if self._agent is None:
            from memframe_ai.gateway import ModelGateway

            self._agent = Agent(
                ModelGateway(self._settings).model(),
                name="guardrail",
                system_prompt=_GUARDRAIL_SYSTEM,
                output_type=GuardrailVerdict,
            )
        return self._agent

    async def verify(self, prompt: str, context: str, table: str) -> GuardrailVerdict:
        agent = self._build()
        logger.info("guardrail prompt='%s' table=%s", prompt[:120], table)
        result = await agent.run(
            f"{context}\n\nACTIVE TABLE NAME: {table}\n\nUser request:\n{prompt}"
        )
        verdict: GuardrailVerdict = result.output
        logger.info("guardrail done is_valid=%s reason=%s", verdict.is_valid, verdict.reason)
        return verdict
