"""Opt-in Logfire observability for memframe_ai.

Logfire is an optional dependency (install via ``pip install "memframe[logfire]"``).
Everything here lazy-imports it so the core library and the unit suite work
without the extra installed. Enable by passing ``logfire_enabled=True`` to
``aenable_agent`` (plus a ``logfire_token`` or the ``LOGFIRE_TOKEN`` env var).

Once enabled, ``instrument_pydantic_ai()`` auto-traces every agent run, LLM call,
and tool call across all agents (guardrail, planner, specialists, dashboard
designer); ``instrument_logging()`` sends the standard ``memframe.ai`` log lines
to Logfire too. The ``span()`` helper wraps orchestration steps so the whole
pipeline appears as one coherent trace.
"""

import contextlib
import inspect
import logging
from typing import Any

logger = logging.getLogger("memframe.ai")

_configured = False


def configure_logfire(settings: Any) -> bool:
    """Configure and instrument Logfire (once). Returns True if active.

    No-op unless ``settings.logfire_enabled`` is True. Fail-open: any error
    (missing package, bad token, network) is logged and returns False so a
    logging hiccup can never break a chat.
    """
    global _configured
    if not getattr(settings, "logfire_enabled", False):
        return False
    try:
        import logfire
    except ImportError:
        logger.warning("logfire_enabled=True but the 'logfire' extra is not installed")
        return False
    if _configured:
        return True
    try:
        # Only pass kwargs the installed Logfire version accepts (e.g. 4.x has
        # no `project` arg); an unsupported kwarg would otherwise raise and
        # silently disable observability.
        params = set(inspect.signature(logfire.configure).parameters)
        cfg = {}
        if "token" in params:
            cfg["token"] = getattr(settings, "logfire_token", None) or None
        if "service_name" in params:
            cfg["service_name"] = getattr(settings, "logfire_service_name", "memframe-ai")
        if "environment" in params:
            cfg["environment"] = getattr(settings, "logfire_environment", None) or "production"
        if "project" in params:
            cfg["project"] = getattr(settings, "logfire_project", None) or None
        logfire.configure(**cfg)
        # Order matters: configure() must precede instrument_*().
        logfire.instrument_pydantic_ai()
        # instrument_logging() exists on newer Logfire; older versions capture
        # stdlib logs differently, so skip gracefully if absent.
        if hasattr(logfire, "instrument_logging"):
            logfire.instrument_logging()
        # Host metrics (CPU/mem/disk) for the Logfire "Hosts" view; requires the
        # system-metrics extra. Guarded so it's a no-op if unavailable.
        if hasattr(logfire, "instrument_system_metrics"):
            logfire.instrument_system_metrics()
    except Exception as exc:  # pragma: no cover - defensive
        logger.warning("logfire configure failed (disabled): %s", exc)
        return False
    _configured = True
    logger.info("logfire instrumentation active (service=%s)", settings.logfire_service_name)
    return True


def span(name: str, **attrs):
    """Return a Logfire span context manager, or a no-op if Logfire is inactive."""
    if not _configured:
        return contextlib.nullcontext()
    try:
        import logfire

        return logfire.span(name, **attrs)
    except Exception:
        return contextlib.nullcontext()


def flush_logfire():
    """Best-effort flush of buffered Logfire spans. No-op if inactive."""
    if not _configured:
        return
    try:
        import logfire

        logfire.force_flush()
    except Exception:
        pass
