from pydantic_ai.models.anthropic import AnthropicModel
from pydantic_ai.models.fallback import FallbackModel
from pydantic_ai.models.google import GoogleModel
from pydantic_ai.models.ollama import OllamaModel
from pydantic_ai.models.openai import OpenAIChatModel

from memframe_ai.config import AISettings
from memframe_ai.gateway import ModelGateway


def _settings(**kw) -> AISettings:
    return AISettings(api_key="k", **kw)


def test_openai_gateway():
    g = ModelGateway(_settings())
    assert isinstance(g.model(), OpenAIChatModel)


def test_other_providers():
    assert isinstance(ModelGateway(_settings(provider="anthropic")).model(), AnthropicModel)
    assert isinstance(ModelGateway(_settings(provider="google")).model(), GoogleModel)
    assert isinstance(ModelGateway(_settings(provider="ollama")).model(), OllamaModel)


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
