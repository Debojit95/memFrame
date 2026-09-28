from __future__ import annotations

from typing import Any, Dict

from memframe.core.orchestrator.analytix.comparison import ComparisonOrchestrator
from memframe.utils.async_sync import async_to_sync


class ComparisonWrapper(ComparisonOrchestrator):
    """Wrapper around `ComparisonOrchestrator` with async/sync methods."""

    def __init__(self, *args, **kwargs):
        """Initialize the comparison wrapper with orchestrator arguments."""
        super().__init__(*args, **kwargs)

    async def acompare(self, *args, **kwargs) -> Dict[str, Any]:
        """Asynchronously evaluate a comparison expression or operation."""
        return await super().compare(*args, **kwargs)

    @async_to_sync
    async def compare(self, *args, **kwargs) -> Dict[str, Any]:
        """Synchronously evaluate a comparison expression or operation."""
        return await self.acompare(*args, **kwargs)

    def __call__(self, *args, **kwargs) -> Dict[str, Any]:
        """
        Allow direct call style:
            ops.compare("A >= B")
        """
        return self.compare(*args, **kwargs)


# Alias
CompareAccessor = ComparisonWrapper
