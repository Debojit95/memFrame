"""AISettings: logfire_* fields exist only when the logfire extra is installed."""

from memframe_ai.config import AISettings, _HAS_LOGFIRE


def test_logfire_fields_presence_matches_install():
    # ponytail: invariant for bug-1. When logfire isn't importable, AISettings
    # must not advertise logfire_* config; when it is, the fields must exist.
    has = "logfire_enabled" in AISettings.model_fields
    assert has == _HAS_LOGFIRE


def test_core_fields_always_present():
    s = AISettings(api_key="k")
    assert s.provider == "openai"
    assert s.guardrails_enabled is True
    assert s.max_output_rows == 20
