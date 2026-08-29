"""
PostgreSQL cleaning operations.

See duckdb.py: the shared logic in base.py branches on the adapter type, so no
per-backend overrides are needed in this split.
"""

from memframe.core.analytix.cleaning.base import DataCleaningOps


class PostgresCleaningOps(DataCleaningOps):
    pass
