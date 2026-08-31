from typing import Optional

import importlib.util
from pydantic import BaseModel, SecretStr

# ponytail: Logfire is an optional extra. Probe it without importing (avoids
# side effects / cost); the logfire_* settings below only exist when it's
# importable, so enable_agent() never advertises observability that isn't
# installed.
_HAS_LOGFIRE = importlib.util.find_spec("logfire") is not None


class AISettings(BaseModel):
    """Agent configuration, passed explicitly by the caller (no env / .env)."""

    provider: str = "openai"
    model: str = "gpt-5.5"
    api_key: SecretStr  # ponytail: SecretStr so repr()/model_dump() can't leak it
    base_url: Optional[str] = None
    max_output_rows: int = 20
    max_output_cols: int = 20
    guardrails_enabled: bool = True
    if _HAS_LOGFIRE:
        # ponytail: Logfire observability is opt-in; configure_logfire() reads
        # these via getattr(..., default), so their absence is harmless when the
        # extra isn't installed.
        logfire_enabled: bool = False
        logfire_token: Optional[SecretStr] = None
        logfire_project: Optional[str] = None
        logfire_service_name: str = "memframe-ai"
        logfire_environment: Optional[str] = None
