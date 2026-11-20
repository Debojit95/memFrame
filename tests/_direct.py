"""Run a single test module directly under pytest."""

from __future__ import annotations

import sys

import pytest


def direct(module_file: str) -> None:
    """Run ``module_file`` with pytest, forwarding the CLI options."""
    raise SystemExit(pytest.main([str(module_file), *sys.argv[1:]]))
