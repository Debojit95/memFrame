import asyncio

import pytest
from pydantic import ValidationError

from memframe_ai.config import AISettings


def test_entrypoints_are_real_methods():
    from memframe.db_manager.context import ContextManager
    from memframe.main import MemFrame

    assert callable(getattr(ContextManager, "chat", None))
    assert callable(getattr(ContextManager, "achat", None))
    assert callable(getattr(MemFrame, "enable_agent", None))
    assert callable(getattr(MemFrame, "aenable_agent", None))
    assert getattr(ContextManager, "achat") is getattr(MemFrame, "achat")


def test_chat_without_enable_agent_raises():
    from memframe.main import MemFrame

    m = MemFrame()
    ops = m._ops()
    with pytest.raises(RuntimeError, match="enable_agent"):
        ops.chat("hello")


def test_aenable_agent_stores_settings():
    from memframe.main import MemFrame

    m = MemFrame()
    settings = asyncio.run(m.aenable_agent(api_key="k"))
    assert isinstance(settings, AISettings)
    assert m._ai_settings is settings
    assert settings.api_key == "k"
    assert settings.provider == "openai"
    assert settings.model == "gpt-5-mini"


def test_aenable_agent_requires_api_key():
    from memframe.main import MemFrame

    m = MemFrame()
    with pytest.raises(TypeError):
        asyncio.run(m.aenable_agent())


def test_settings_requires_api_key():
    with pytest.raises(ValidationError):
        AISettings()
    assert AISettings(api_key="k").api_key == "k"
