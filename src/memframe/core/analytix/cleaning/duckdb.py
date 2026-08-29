"""
DuckDB cleaning operations.

The full set of cleaning operations lives in base.py and already branches on
the adapter type, so DuckDB needs no overrides here. ClickHouse/Postgres
subclasses follow the same pattern for symmetry and the factory dispatch.
"""

from memframe.core.analytix.cleaning.base import DataCleaningOps


class DuckDBCleaningOps(DataCleaningOps):
    pass
