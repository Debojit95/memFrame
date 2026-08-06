from typing import Callable, Dict

from memframe_ai.config import AISettings


def _openai(s: AISettings):
    from pydantic_ai.models.openai import OpenAIChatModel
    from pydantic_ai.providers.openai import OpenAIProvider

    return OpenAIChatModel(
        s.model, provider=OpenAIProvider(base_url=s.base_url, api_key=s.api_key)
    )


def _anthropic(s: AISettings):
    from pydantic_ai.models.anthropic import AnthropicModel
    from pydantic_ai.providers.anthropic import AnthropicProvider

    return AnthropicModel(s.model, provider=AnthropicProvider(api_key=s.api_key))


def _google(s: AISettings):
    from pydantic_ai.models.google import GoogleModel
    from pydantic_ai.providers.google import GoogleProvider

    return GoogleModel(s.model, provider=GoogleProvider(api_key=s.api_key))


def _ollama(s: AISettings):
    from pydantic_ai.models.ollama import OllamaModel
    from pydantic_ai.providers.ollama import OllamaProvider

    return OllamaModel(
        s.model, provider=OllamaProvider(base_url=s.base_url or "http://localhost:11434")
    )


def _string_model(s: AISettings):
    # ponytail: string models read API keys from env, we can't inject one.
    # Register a constructor for keyed access to any provider.
    return f"{s.provider}:{s.model}"


_PROVIDERS: Dict[str, Callable[[AISettings], object]] = {
    "openai": _openai,
    "anthropic": _anthropic,
    "google": _google,
    "ollama": _ollama,
}


class ModelGateway:
    """Resolves AISettings into a pydantic-ai Model for any provider."""

    def __init__(self, settings: AISettings):
        self._settings = settings
        self._registry = dict(_PROVIDERS)

    def register(self, name: str, constructor: Callable[[AISettings], object]) -> None:
        self._registry[name] = constructor

    def model(self):
        constructor = self._registry.get(self._settings.provider, _string_model)
        return constructor(self._settings)

    def fallback(self, *providers: str):
        from pydantic_ai.models.fallback import FallbackModel

        base = self._settings
        return FallbackModel(
            *[
                self._registry[p](base.model_copy(update={"provider": p}))
                for p in providers
            ]
        )
