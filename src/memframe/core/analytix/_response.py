"""Shared response envelope for analytix operations."""

from __future__ import annotations

from typing import Any

from pydantic import BaseModel, ConfigDict, Field

from memframe.exceptions import OperationError


class OperationResponse(BaseModel):
    """Validated internal representation of an operation response."""

    model_config = ConfigDict(
        extra="allow",
        arbitrary_types_allowed=True,
    )

    is_error: bool
    message: str = ""
    error_message: str | None = None
    involved_cols: list[str] = Field(default_factory=list)
    generated_cols: list[str] = Field(default_factory=list)
    result: Any = None

    def to_payload(self) -> dict[str, Any]:
        """Return the existing dictionary API payload without dropping None."""
        return self.model_dump(exclude_none=False)


def is_operation_response(value: Any) -> bool:
    """Return whether a value is a canonical analytix response payload."""
    return isinstance(value, dict) and {
        "is_error",
        "error_message",
        "result",
    }.issubset(value)


def unwrap_response(value: Any) -> Any:
    """Return a public operation value or raise its operation error."""
    if not is_operation_response(value):
        return value
    if value["is_error"]:
        raise OperationError(
            value.get("error_message") or value.get("message") or "Operation failed"
        )
    if value.get("result") is None and value.get("iterator") is not None:
        return value["iterator"]
    return value["result"]


# Keep the argument order used by the existing analytix response builders.
def ok(
    message: str = "",
    involved_cols: list[str] | None = None,
    generated_cols: list[str] | None = None,
    result: Any = None,
    **extra: Any,
) -> dict[str, Any]:
    """Validate and build a successful response payload."""
    return OperationResponse(
        is_error=False,
        message=message,
        error_message=None,
        involved_cols=involved_cols or [],
        generated_cols=generated_cols or [],
        result=result,
        **extra,
    ).to_payload()


def fail(
    error_message: str,
    involved_cols: list[str] | None = None,
    generated_cols: list[str] | None = None,
    **extra: Any,
) -> dict[str, Any]:
    """Validate and build a failed response payload."""
    return OperationResponse(
        is_error=True,
        message="",
        error_message=error_message,
        involved_cols=involved_cols or [],
        generated_cols=generated_cols or [],
        result=None,
        **extra,
    ).to_payload()
