from memframe.core.analytix.stats import DataStatsOps

from memframe_ai.tools._helpers import normalize

_NUMERIC = ("int", "float", "double", "decimal", "numeric", "bigint", "real", "smallint")


def tools(session):
    async def _ops():
        await session.ensure()
        return DataStatsOps(session.adapter)

    async def value_counts(column: str, top_n: int = 10) -> dict:
        """Return the top_n most frequent distinct values of a column and their counts.
        
        Simplified to avoid retries for numeric columns.
        """
        ops = await _ops()
        col_types = await session.adapter.get_column_types(session.table, session.schema)
        dtype = (col_types.get(column) or "").lower()
        if any(t in dtype for t in _NUMERIC):
            # For numeric columns, only return basic stats (count, mean, std)
            result = await ops.numeric_basic_stats(session.table, session.schema, column)
        else:
            result = await ops.categorical_value_counts(session.table, session.schema, column, top_n=top_n)
        return await normalize(result, session)

    async def corr(columns: list[str]) -> dict:
        """Return the pairwise correlation matrix of the given numeric columns."""
        ops = await _ops()
        result = await ops.numeric_multi_column_correlation(
            session.table, session.schema, columns
        )
        return await normalize(result, session)

    async def outliers_iqr(column: str) -> dict:
        """Return IQR-based outlier statistics (q1, q3, bounds, outlier count) for a numeric column."""
        ops = await _ops()
        result = await ops.numeric_outliers_iqr(session.table, session.schema, column)
        return await normalize(result, session)

    return [value_counts, corr, outliers_iqr]
