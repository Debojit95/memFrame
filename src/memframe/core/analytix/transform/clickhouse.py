from memframe.core.analytix.transform.base import TransformOps


class ClickHouseTransformOps(TransformOps):
    """ClickHouse backend — inherits base, wholesale CTAS overrides live in base for now.

    Future: move ClickHouse CTAS branches here to keep base DuckDB-only.
    """
