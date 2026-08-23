from typing import Optional

from pydantic import BaseModel


class AISettings(BaseModel):
    """Agent configuration, passed explicitly by the caller (no env / .env)."""

    provider: str = "openai"
    model: str = "gpt-5.5"
    api_key: str
    base_url: Optional[str] = None
    max_output_rows: int = 20
    max_output_cols: int = 20
    guardrails_enabled: bool = True
    # ponytail: Logfire observability is opt-in; configure_logfire() is a no-op
    # unless logfire_enabled is True (and the `logfire` extra is installed).
    logfire_enabled: bool = False
    logfire_token: Optional[str] = None
    logfire_project: Optional[str] = None
    logfire_service_name: str = "memframe-ai"
    logfire_environment: Optional[str] = None
