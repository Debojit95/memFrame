"""Allow an integration test module to be executed directly with Python."""

from __future__ import annotations

import sys
from pathlib import Path

_root = next(
    parent
    for parent in Path(__file__).resolve().parents
    if (parent / "pyproject.toml").exists()
)
if str(_root) not in sys.path:
    sys.path.insert(0, str(_root))

from tests._direct import direct  # noqa: E402

__all__ = ["direct"]
