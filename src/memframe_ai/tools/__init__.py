from pydantic_ai.toolsets import FunctionToolset

from memframe_ai.tools import clean, context, inspect, plot, select, stats, upload


def build_toolset(session) -> FunctionToolset:
    """Build the sandboxed toolset for a session's agent."""
    tools = []
    for module in (context, upload, inspect, select, clean, stats, plot):
        tools.extend(module.tools(session))
    return FunctionToolset(tools=tools)
