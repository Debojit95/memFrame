from memframe.core.analytix.stats.base import DataStatsOps


class DuckDBDataStatsOps(DataStatsOps):
    """DuckDB backend — inherits the DuckDB-flavoured defaults from base."""

    async def numeric_multi_column_correlation(self, table: str, schema: str, columns, backend=None, data_id=None, new_table=None):
        agg = 'CORR'
        return await self._multi_column_assoc_matrix(table, schema, columns, agg, backend=backend, data_id=data_id, new_table=new_table)

    async def numeric_multi_column_covariance(self, table: str, schema: str, columns, backend=None, data_id=None, new_table=None):
        agg = 'COVAR_SAMP'
        return await self._multi_column_assoc_matrix(table, schema, columns, agg, backend=backend, data_id=data_id, new_table=new_table)
