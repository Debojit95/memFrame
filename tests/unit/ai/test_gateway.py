from pydantic_ai.models.anthropic import AnthropicModel
from pydantic_ai.models.fallback import FallbackModel
from pydantic_ai.models.google import GoogleModel
from pydantic_ai.models.ollama import OllamaModel
from pydantic_ai.models.openai import OpenAIChatModel

from memframe_ai.config import AISettings
from memframe_ai.gateway import ModelGateway


def _settings(api_key: str = "k", **kw) -> AISettings:
    return AISettings(api_key=api_key, **kw)


def test_openai_gateway():
    g = ModelGateway(_settings())
    assert isinstance(g.model(), OpenAIChatModel)


def test_other_providers():
    assert isinstance(ModelGateway(_settings(provider="anthropic")).model(), AnthropicModel)
    assert isinstance(ModelGateway(_settings(provider="google")).model(), GoogleModel)
    assert isinstance(ModelGateway(_settings(provider="ollama")).model(), OllamaModel)


def test_ollama_cloud_forwards_api_key_and_base_url():
    # ponytail: Ollama Cloud needs a custom base_url AND api_key; the gateway must
    # forward both instead of dropping the key (which would make the cloud reject
    # the request with the default placeholder).
    g = ModelGateway(
        _settings(provider="ollama", base_url="https://api.ollama.com/v1", api_key="sk-cloud-123")
    )
    model = g.model()
    assert isinstance(model, OllamaModel)
    assert "api.ollama.com" in model.provider.base_url
    assert model.provider.client.api_key == "sk-cloud-123"


def test_ollama_local_defaults_to_localhost(monkeypatch):
    # ponytail: local Ollama still defaults to localhost when no base_url is given.
    # Isolate from any ambient OLLAMA_BASE_URL (e.g. loaded from .env.test).
    monkeypatch.delenv("OLLAMA_BASE_URL", raising=False)
    g = ModelGateway(_settings(provider="ollama"))
    model = g.model()
    assert isinstance(model, OllamaModel)
    assert "localhost:11434" in model.provider.base_url


def test_ollama_uses_OLLAMA_BASE_URL_env(monkeypatch):
    # ponytail: when base_url isn't passed, the gateway must honor the
    # OLLAMA_BASE_URL env var (matching OllamaProvider's own convention) instead
    # of silently defaulting to localhost.
    monkeypatch.delenv("OLLAMA_BASE_URL", raising=False)
    monkeypatch.setenv("OLLAMA_BASE_URL", "https://ollama.com/v1")
    g = ModelGateway(_settings(provider="ollama"))
    model = g.model()
    assert isinstance(model, OllamaModel)
    assert "ollama.com/v1" in model.provider.base_url


def test_ollama_explicit_base_url_wins_over_env(monkeypatch):
    # ponytail: an explicit base_url must take precedence over OLLAMA_BASE_URL.
    monkeypatch.setenv("OLLAMA_BASE_URL", "https://env.example/v1")
    g = ModelGateway(_settings(provider="ollama", base_url="https://explicit.example/v1"))
    model = g.model()
    assert isinstance(model, OllamaModel)
    assert "explicit.example" in model.provider.base_url


def test_unknown_provider_falls_back_to_string():
    g = ModelGateway(_settings(provider="groq", model="llama-3.3-70b"))
    assert g.model() == "groq:llama-3.3-70b"


def test_register_override():
    g = ModelGateway(_settings(provider="openai"))
    g.register("openai", lambda s: "custom")
    assert g.model() == "custom"


def test_base_url_passes_through():
    g = ModelGateway(_settings(base_url="http://localhost:8000/v1"))
    assert isinstance(g.model(), OpenAIChatModel)


def test_fallback_model():
    g = ModelGateway(_settings())
    fb = g.fallback("openai", "anthropic")
    assert isinstance(fb, FallbackModel)
