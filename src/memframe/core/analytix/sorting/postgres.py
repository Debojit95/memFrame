from memframe.core.analytix.sorting.base import DataSortingOps


class PostgresSortingOps(DataSortingOps):
    """PostgreSQL backend — same NULLS FIRST/LAST + plain CREATE TABLE AS; inherits base."""
