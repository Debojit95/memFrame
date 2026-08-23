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
