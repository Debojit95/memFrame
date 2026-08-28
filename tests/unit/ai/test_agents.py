from memframe_ai.agents.analytics import agent_for
from memframe_ai.config import AISettings
from memframe_ai.sessions import Session


def _stub_memframe(settings):
    class _MF:
        _ai_settings = settings

    return _MF()


def _make_session(settings):
    mf = _stub_memframe(settings)
    ops = type("Ops", (), {"memframe": mf, "_data_id": None})()
    return Session(session_id="s1", ops=ops, memframe=mf, settings=settings)


def test_agent_for_rebuilds_on_settings_change():
    openai_settings = AISettings(api_key="k", provider="openai", model="gpt-4.1-mini")
    session = _make_session(openai_settings)
    a1 = agent_for(session)
    assert a1._settings.provider == "openai"

    # ponytail: re-enabling with a different provider/model must rebuild the
    # agent + model, otherwise the cached session keeps the stale provider.
    ollama_settings = AISettings(
        api_key="k", provider="ollama", model="qwen3", base_url="https://ollama.com/v1"
    )
    session.memframe._ai_settings = ollama_settings
    a2 = agent_for(session)
    assert a2 is not a1
    assert a2._settings.provider == "ollama"
    from pydantic_ai.models.ollama import OllamaModel

    assert isinstance(a2._gateway.model(), OllamaModel)


def test_agent_for_reuses_same_settings():
    settings = AISettings(api_key="k", provider="openai", model="gpt-4.1-mini")
    session = _make_session(settings)
    a1 = agent_for(session)
    a2 = agent_for(session)
    assert a1 is a2
