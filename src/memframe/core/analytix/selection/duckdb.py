"""
DuckDB selection operations.

Base DataSelectionOps already uses DuckDB-flavoured SQL as its defaults
(PRAGMA table_info, UNNEST(ARRAY[...]), date/datetime serialization), so this
subclass only exists for symmetry with the other backends and the factory.
"""

from memframe.core.analytix.selection.base import DataSelectionOps


class DuckDBSelectionOps(DataSelectionOps):
    pass
