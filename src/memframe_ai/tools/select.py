from memframe.core.analytix.selection import DataSelectionOps

from memframe_ai.tools._helpers import normalize


def tools(session):
    async def _ops():
        await session.ensure()
        return DataSelectionOps(session.adapter)

    async def select_columns(columns: list[str]) -> dict:
        """Create a transient table containing only the named columns."""
        ops = await _ops()
        col_types = await session.adapter.get_column_types(session.table, session.schema)
        all_cols = list(col_types.keys())
        indices = [all_cols.index(c) for c in columns if c in all_cols]
        if not indices:
            return {"ok": False, "hint": f"No valid columns in {columns}. Available: {all_cols}"}
        result = await ops.take(
            session.table, session.schema, indices, axis=1, **session.transform_kwargs()
        )
        return await normalize(result, session)

    async def where(cond: str) -> dict:
        """Filter rows by a SQL condition string (e.g. 'age > 30'), creating a transient table."""
        ops = await _ops()
        result = await ops.where(
            session.table, session.schema, cond=cond, **session.transform_kwargs()
        )
        return await normalize(result, session)

    async def take(indices: list[int]) -> dict:
        """Select rows by integer position (0-based), creating a transient table."""
        ops = await _ops()
        result = await ops.take(
            session.table, session.schema, indices, axis=0, **session.transform_kwargs()
        )
        return await normalize(result, session)

    return [select_columns, where, take]
