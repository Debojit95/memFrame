from memframe.core.analytix.cumulative.base import CumulativeOps


class PostgresCumulativeOps(CumulativeOps):
    """PostgreSQL backend — row-addressed via ``ctid``; all else inherited."""

    @property
    def _row_id_col(self) -> str:
        return "ctid"
