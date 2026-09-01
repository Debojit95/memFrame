from memframe.core.analytix.cleaning.base import DataCleaningOps


class DuckDBCleaningOps(DataCleaningOps):
    """DuckDB backend — inherits the DuckDB-flavoured defaults from base."""

    def _numeric_target_for(self, pg_type: str) -> str:
        return {
            "SMALLINT": "SMALLINT",
            "INTEGER": "INTEGER",
            "BIGINT": "BIGINT",
            "FLOAT": "DOUBLE",
        }.get(pg_type, "NUMERIC")
