from __future__ import annotations

from typing import Any


class GroupBy:
    """
    Unified GroupBy object that supports statistics (agg, mean, sum, ...),
    cumulative operations (cumsum, cummean, ...), and window operations
    (rolling, expanding, ewm).

    Created by ``ContextManager.groupby(*columns)``.
    """

    def __init__(self, stats_builder: Any, cum_builder: Any, window_builder: Any) -> None:
        self._stats = stats_builder
        self._cum = cum_builder
        self._window = window_builder
        self.group_cols = stats_builder.group_cols  # expose columns

    def __getattr__(self, name: str) -> Any:
        """
        Delegate attribute lookup:
          - 'rolling', 'expanding', 'ewm' → window builder
          - Names starting with 'cum' or 'acum' → cumulative builder
          - Everything else → stats builder
        """
        if name.startswith("_"):
            raise AttributeError(name)

        # Window methods – exact names, async variants not needed (they are sync)
        if name in ("rolling", "expanding", "ewm"):
            if hasattr(self._window, name):
                return getattr(self._window, name)

        # Cumulative methods – prefixed with cum / acum
        if name.startswith("cum") or name.startswith("acum"):
            if hasattr(self._cum, name):
                return getattr(self._cum, name)

        # Default: stats builder
        if hasattr(self._stats, name):
            return getattr(self._stats, name)

        # Fallback: try the other builders just in case
        for builder in (self._window, self._cum, self._stats):
            if hasattr(builder, name):
                return getattr(builder, name)

        raise AttributeError(
            f"'GroupBy' object has no attribute '{name}'"
        )

    def __repr__(self):
        return f"GroupBy(columns={self.group_cols})"