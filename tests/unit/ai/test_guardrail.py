"""Unit tests for the query guardrail (no live LLM required).

The guardrail decision is exercised by monkeypatching GuardrailAgent/PlannerAgent
on AnalyticsAgent, so these run offline and fast.
"""

import asyncio
import types

import pytest

from memframe.db_manager.context import ContextManager
from memframe_ai.agents.analytics import AnalyticsAgent
from memframe_ai.agents.guardrail import GuardrailVerdict


class FakeSession:
    def __init__(self):
        self.session_id = "s1"
        self.table = "t1"
        self.schema = "public"
        self.subquery_results = []
        self.results = []
        self.plots = {}

    async def ensure(self):
        pass

    async def domain_context(self, lightweight=False, force_refresh=False):
        return "ACTIVE TABLE CONTEXT: t1 Columns: a [numeric], b [categorical]"

    def reset_subquery_results(self):
        self.subquery_results.clear()

    def reset_results(self):
        self.results.clear()


class FakeSettings:
    guardrails_enabled = True


class StubGuard:
    def __init__(self, verdict):
        self.verdict = verdict
        self.calls = 0

    async def verify(self, prompt, context, table):
        self.calls += 1
        return self.verdict


class StubPlanner:
    def __init__(self, calls):
        self.calls = calls

    async def plan_with_dependencies(self, prompt, context):
        self.calls["n"] += 1
        return None


def _make_agent(settings=None):
    agent = AnalyticsAgent(FakeSession(), settings or FakeSettings())
    return agent


def test_refusal_shape():
    agent = _make_agent()
    verdict = GuardrailVerdict(is_valid=False, reason="off", missing_terms=["x"])
    resp = agent._refusal("q", verdict)
    assert resp["guardrail_blocked"] is True
    assert resp["guardrail_reason"] == "off"
    assert resp["results"] == [] and resp["plots"] == [] and resp["values"] == []


def test_guardrail_blocks_and_skips_planner():
    agent = _make_agent()
    guard = StubGuard(GuardrailVerdict(is_valid=False, reason="off-topic"))
    agent._guardrail_agent = lambda: guard
    calls = {"n": 0}
    agent._planner_agent = lambda: StubPlanner(calls)

    resp = asyncio.run(agent.achat("who is the president of Kenya"))

    assert resp["guardrail_blocked"] is True
    assert "off-topic" in resp["answer"]
    assert guard.calls == 1
    assert calls["n"] == 0  # planner never ran


def test_guardrail_valid_proceeds_to_planner():
    agent = _make_agent()
    guard = StubGuard(GuardrailVerdict(is_valid=True, reason="ok"))
    agent._guardrail_agent = lambda: guard
    calls = {"n": 0}
    agent._planner_agent = lambda: StubPlanner(calls)

    resp = asyncio.run(agent.achat("show average a by b"))

    assert resp.get("guardrail_blocked") is None
    assert guard.calls == 1
    assert calls["n"] == 1  # planner ran


def test_guardrails_disabled_skips_guardrail():
    settings = FakeSettings()
    settings.guardrails_enabled = False
    agent = _make_agent(settings)
    guard = StubGuard(GuardrailVerdict(is_valid=True))
    agent._guardrail_agent = lambda: guard
    calls = {"n": 0}
    agent._planner_agent = lambda: StubPlanner(calls)

    asyncio.run(agent.achat("anything"))

    assert guard.calls == 0  # guardrail skipped
    assert calls["n"] == 1


def test_adashboard_returns_blocked_html(monkeypatch):
    mf = types.SimpleNamespace(
        _ai_settings=FakeSettings(), _active_id="x", _backend=None, _pool=None
    )
    ctx = ContextManager(mf, "d1")
    blocked = {
        "guardrail_blocked": True,
        "guardrail_reason": "off-topic request",
        "answer": "rejected",
    }

    async def fake_achat(sentence):
        return blocked

    monkeypatch.setattr(ContextManager, "achat", staticmethod(fake_achat))

    html = asyncio.run(ctx.adashboard("who is the president of Kenya"))

    assert isinstance(html, str)
    assert "<html" in html.lower()
    assert "off-topic request" in html
    assert "Execution was stopped" in html
