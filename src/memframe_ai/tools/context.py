def tools(session):
    async def list_tables() -> dict:
        """List uploaded datasets as [{'data_id': ..., 'filename': ...}], newest first."""
        rows = await session.memframe.alist_tables()
        return {"ok": True, "tables": rows}

    async def active_table() -> dict:
        """Return the currently active table name and its schema."""
        await session.ensure()
        return {"ok": True, "table": session.table, "schema": session.schema}

    async def use_table(data_id: str) -> dict:
        """Switch the active dataset to a data_id from list_tables."""
        await session.memframe.aset_active(data_id)
        session.data_id = data_id
        session.invalidate()
        return {"ok": True, "message": f"Active dataset set to {data_id}", "data_id": data_id}

    return [list_tables, active_table, use_table]
