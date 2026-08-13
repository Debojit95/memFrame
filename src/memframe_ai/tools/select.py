from memframe_ai.tools._helpers import normalize


def tools(session):
    w = session.wrappers.selection

    async def select_columns(columns: list[str]) -> dict:
        """Create a transient table containing only the named columns."""
        return await normalize(
            await w.aloc(row_selector=slice(None), columns=columns), session
        )

    async def where(cond: str) -> dict:
        """Filter rows by a SQL condition string (e.g. 'age > 30'), creating a transient table."""
        return await normalize(await w.awhere(cond=cond), session)

    async def take(indices: list[int]) -> dict:
        """Select rows by integer position (0-based), creating a transient table."""
        return await normalize(await w.atake(indices=indices, axis=0), session)

    return [select_columns, where, take]