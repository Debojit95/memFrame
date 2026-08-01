"""Method-call caching for memFrame.

The ``record_call`` decorator (a ``CacheManager`` instance) implements the
two-level data cache used by the orchestrator layer: signature-only logging by
default, and persistent transient table storage when ``deep_cache`` is enabled.
"""

from .cache_manager import CacheManager, record_call

__all__ = ["CacheManager", "record_call"]
