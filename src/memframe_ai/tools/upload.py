def tools(session):
    async def upload_csv(file_path: str) -> dict:
        """Upload a CSV file from the host path and make it the active dataset."""
        cm = await session.memframe.aupload_csv(file_path)
        session.data_id = cm._data_id or session.memframe._active_id
        session.invalidate()
        return {"ok": True, "message": f"Uploaded CSV from {file_path}", "data_id": session.data_id}

    async def upload_parquet(file_path: str) -> dict:
        """Upload a Parquet file from the host path and make it the active dataset."""
        cm = await session.memframe.aupload_parquet(file_path)
        session.data_id = cm._data_id or session.memframe._active_id
        session.invalidate()
        return {"ok": True, "message": f"Uploaded Parquet from {file_path}", "data_id": session.data_id}

    return [upload_csv, upload_parquet]
