import logging

from pydantic_ai import Agent
from pydantic_ai.models.test import TestModel

from memframe_ai.observe import make_hooks


def test_hooks_log_model_requests(caplog):
    agent = Agent(TestModel(), name="t", capabilities=[make_hooks("test_agent")])
    with caplog.at_level(logging.INFO, logger="memFrame"):
        agent.run_sync("hello")
    assert "model_request" in caplog.text
    assert "test_agent" in caplog.text
    assert "ctx_len" in caplog.text


def test_hooks_log_after_run(caplog):
    agent = Agent(TestModel(), name="t", capabilities=[make_hooks("test_agent")])
    with caplog.at_level(logging.INFO, logger="memFrame"):
        agent.run_sync("hello")
    assert "after_run" in caplog.text
    assert "requests=" in caplog.text
