from memframe_ai.tools._helpers import normalize


def tools(session):
    w = session.wrappers.stats

    async def value_counts(column: str, top_n: int = 10) -> dict:
        """Return the top_n most frequent distinct values of a column and their counts.

        Numeric columns are auto-routed to numeric stats by the orchestrator.
        """
        return await normalize(await w.avalue_counts(column=column, top_n=top_n), session)

    async def corr(columns: list[str]) -> dict:
        """Return the pairwise correlation matrix of the given numeric columns."""
        return await normalize(await w.acorr(columns=columns), session)

    async def outliers_iqr(column: str) -> dict:
        """Return IQR-based outlier statistics (q1, q3, bounds, outlier count) for a numeric column."""
        return await normalize(await w.aoutliers_iqr(column=column), session)

    return [value_counts, corr, outliers_iqr]