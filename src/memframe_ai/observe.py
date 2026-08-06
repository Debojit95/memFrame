"""Observability for memframe_ai: pydantic-ai Hooks + memFrame logging.

Every agent (intent classifier, specialists, orchestrator) is created with a
``Hooks`` capability built by :func:`make_hooks` so each model request, tool
call, and completed run is logged through the standard ``memFrame`` logger.

The goal is to make a slow/looping chat query observable: which agent ran,
exactly what context was sent to the model, which tools were called, and how
many requests each run consumed.
"""

import logging
import time

logger = logging.getLogger("memFrame")

_CTX_PREVIEW = 400


def _latest_user_text(request_context) -> str:
    """Return the text of the most recent user message (skip tool/retry parts)."""
    for msg in reversed(request_context.messages):
        if getattr(msg, "kind", None) not in ("user", "request"):
            continue
        for part in getattr(msg, "parts", ()):
            if getattr(part, "part_kind", None) in ("user-prompt", "user-prompt-internal"):
                content = getattr(part, "content", "")
                if isinstance(content, str):
                    return content
    return ""


def _summarize_result(result) -> str:
    output = getattr(result, "output", None)
    return f"kind={type(output).__name__} len={len(str(output))}"


def _usage_summary(result) -> str:
    usage = getattr(result, "usage", None)
    if usage is None:
        return "requests=?"
    return (
        f"requests={getattr(usage, 'requests', '?')} "
        f"input_tokens={getattr(usage, 'input_tokens', '?')} "
        f"output_tokens={getattr(usage, 'output_tokens', '?')}"
    )


def make_hooks(agent_name: str) -> "Hooks":
    """Build a Hooks capability that logs model requests, tool calls and runs."""
    from pydantic_ai.capabilities.hooks import Hooks

    hooks = Hooks()
    _agent = agent_name
    _req_count = {"n": 0}

    def _log(fn):
        """Run the log callback; never let a logging bug break the agent."""
        try:
            fn()
        except Exception as exc:  # pragma: no cover
            logger.debug("[%s] observe logging error: %s: %s", _agent, type(exc).__name__, exc)

    @hooks.on.before_model_request
    async def _before_model_request(ctx, request_context):
        _req_count["n"] += 1
        ctx_text = _latest_user_text(request_context)
        preview = ctx_text[:_CTX_PREVIEW].replace("\n", " ")
        _log(lambda: logger.info(
            "[%s] model_request #%d messages=%d ctx_len=%d ctx='%s...'",
            _agent, _req_count["n"], len(request_context.messages),
            len(ctx_text), preview,
        ))
        return request_context

    @hooks.on.tool_execute
    async def _tool_execute(ctx, call, tool_def, args, handler):
        t0 = time.perf_counter()
        result = await handler(args)
        el = (time.perf_counter() - t0) * 1000
        summary = _summarize_result(result)
        _log(lambda: logger.info(
            "[%s] tool %s args=%r -> %s (%.1fms)", _agent, tool_def.name, args, summary, el,
        ))
        return result

    @hooks.on.tool_execute_error
    async def _tool_execute_error(ctx, call, tool_def, args, error):
        _log(lambda: logger.warning(
            "[%s] tool %s error: %s: %s", _agent, tool_def.name, type(error).__name__, error,
        ))
        return error

    @hooks.on.after_run
    async def _after_run(ctx, result):
        _log(lambda: logger.info(
            "[%s] after_run %s elapsed_requests=%s", _agent, _usage_summary(result), _req_count["n"],
        ))
        return result

    @hooks.on.run_error
    async def _run_error(ctx, error):
        _log(lambda: logger.warning(
            "[%s] run_error %s: %s", _agent, type(error).__name__, error,
        ))
        return error

    return hooks
