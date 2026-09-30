from memframe.core.analytix.filter_II.base import DataFilteringOps


class ClickHouseFilteringOps(DataFilteringOps):
    """ClickHouse backend — MergeTree engine clause for CTAS."""

    def _engine_clause(self) -> str:
        # ClickHouse requires ENGINE + ORDER BY for MergeTree
        return "ENGINE = MergeTree() ORDER BY tuple()"
