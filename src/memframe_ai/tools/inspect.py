from memframe.core.analytix.inspection import GeneralTableOps

from memframe_ai.tools._helpers import normalize


def tools(session):
    async def _ops():
        await session.ensure()
        return GeneralTableOps(session.adapter)

    async def head(n: int = 10) -> dict:
        """Return the first n rows of the active table as a list of records."""
        ops = await _ops()
        return await normalize(
            await ops.dataframe_head(session.table, session.schema, n=n), session, advance=False
        )

    async def tail(n: int = 10) -> dict:
        """Return the last n rows of the active table as a list of records."""
        ops = await _ops()
        return await normalize(
            await ops.dataframe_tail(session.table, session.schema, n=n), session, advance=False
        )

    async def sample(n: int = 10) -> dict:
        """Return n randomly sampled rows of the active table."""
        ops = await _ops()
        return await normalize(
            await ops.dataframe_sample(session.table, session.schema, n=n), session, advance=False
        )

    async def describe(columns: list[str] | None = None) -> dict:
        """Return summary statistics (count, mean, std, min, quartiles, max) for numeric columns."""
        ops = await _ops()
        return await normalize(
            await ops.dataframe_describe(session.table, session.schema, columns=columns), session, advance=False
        )

    async def info() -> dict:
        """Return column names, dtypes, and null counts for the active table."""
        ops = await _ops()
        return await normalize(
            await ops.dataframe_info(session.table, session.schema), session, advance=False
        )

    async def null_analysis(columns: list[str] | None = None) -> dict:
        """Return missing-value counts and percentages for columns."""
        ops = await _ops()
        return await normalize(
            await ops.dataframe_null_analysis(session.table, session.schema, columns=columns),
            session,
            advance=False,
        )

    return [head, tail, sample, describe, info, null_analysis]
