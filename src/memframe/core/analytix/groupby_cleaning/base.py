from memframe.core.analytix.cleaning.duckdb import DuckDBCleaningOps


class GroupByCleaningOps(DuckDBCleaningOps):
    """Shared base for group-by fillna ops (DuckDB-flavoured defaults)."""
