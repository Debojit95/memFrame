"""Tests for opt-in Logfire instrumentation (skipped unless `logfire` is installed)."""

import contextlib

import pytest

logfire = pytest.importorskip("logfire")

from types import SimpleNamespace  # noqa: E402

import memframe_ai.instrument as instr  # noqa: E402


def _settings(**overrides):
    base = dict(
        logfire_enabled=False,
        logfire_token=None,
        logfire_project=None,
        logfire_service_name="memframe-ai",
        logfire_environment="test",
    )
    base.update(overrides)
    return SimpleNamespace(**base)


def test_configure_inactive_when_disabled(monkeypatch):
    monkeypatch.setattr(instr, "_configured", False)
    assert instr.configure_logfire(_settings(logfire_enabled=False)) is False
    # Span is a no-op context manager when Logfire is not active.
    assert type(instr.span("x")) is contextlib.nullcontext


def test_span_noop_when_inactive(monkeypatch):
    monkeypatch.setattr(instr, "_configured", False)
    with instr.span("anything", foo="bar"):
        pass  # must not raise


def test_configure_active_runs_instrumentation(monkeypatch):
    monkeypatch.setattr(instr, "_configured", False)
    monkeypatch.setenv("LOGFIRE_IGNORE_NO_CONFIG", "1")
    calls = {}
    monkeypatch.setattr(logfire, "configure", lambda **k: calls.setdefault("configure", True))
    monkeypatch.setattr(
        logfire, "instrument_pydantic_ai", lambda: calls.setdefault("ai", True)
    )
    if hasattr(logfire, "instrument_logging"):
        monkeypatch.setattr(
            logfire, "instrument_logging", lambda: calls.setdefault("logging", True)
        )

    assert instr.configure_logfire(_settings(logfire_enabled=True, logfire_token="dummy")) is True
    assert calls.get("configure") and calls.get("ai")

    # Now active: span returns a real Logfire span, not a no-op.
    sp = instr.span("x")
    assert type(sp) is not contextlib.nullcontext
    with sp:
        pass


def test_configure_idempotent(monkeypatch):
    monkeypatch.setattr(instr, "_configured", False)
    calls = {"n": 0}
    monkeypatch.setattr(logfire, "configure", lambda **k: calls.__setitem__("n", calls["n"] + 1))
    monkeypatch.setattr(logfire, "instrument_pydantic_ai", lambda: None)
    if hasattr(logfire, "instrument_logging"):
        monkeypatch.setattr(logfire, "instrument_logging", lambda: None)

    s = _settings(logfire_enabled=True, logfire_token="dummy")
    instr.configure_logfire(s)
    instr.configure_logfire(s)  # second call should be a no-op
    assert calls["n"] == 1
