from memframe.core.analytix.transform.base import TransformOps


class PostgresTransformOps(TransformOps):
    """PostgreSQL backend — inherits DuckDB-flavoured defaults from base.

    Overrides are added here only when PG SQL genuinely differs
    (e.g., PERCENTILE_CONT vs quantile, ::TEXT casts).
    Currently base already handles PG via isinstance branches,
    so this subclass is kept minimal and will receive wholesale
    overrides as needed.
    """
