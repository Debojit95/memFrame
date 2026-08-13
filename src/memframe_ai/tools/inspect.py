from memframe_ai.tools._helpers import normalize


def tools(session):
    w = session.wrappers.inspection

    async def head(n: int = 10) -> dict:
        """Return the first n rows of the active table as a list of records."""
        return await normalize(await w.ahead(n=n), session, advance=False)

    async def tail(n: int = 10) -> dict:
        """Return the last n rows of the active table as a list of records."""
        return await normalize(await w.atail(n=n), session, advance=False)

    async def sample(n: int = 10) -> dict:
        """Return n randomly sampled rows of the active table."""
        return await normalize(await w.asample(n=n), session, advance=False)

    async def describe(columns: list[str] | None = None) -> dict:
        """Return summary statistics (count, mean, std, min, quartiles, max) for numeric columns."""
        return await normalize(await w.adescribe(columns=columns), session, advance=False)

    async def info() -> dict:
        """Return column names, dtypes, and null counts for the active table."""
        return await normalize(await w.ainfo(), session, advance=False)

    async def null_analysis(columns: list[str] | None = None) -> dict:
        """Return missing-value counts and percentages for columns."""
        return await normalize(
            await w.anull_analysis(columns=columns), session, advance=False
        )

    return [head, tail, sample, describe, info, null_analysis]