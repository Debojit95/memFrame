"""Shared result-frame fetch helper."""

from __future__ import annotations

import pandas as pd


async def fetch_df(db, sql: str, *args):
    """Fetch rows as a DataFrame, using columnar reads when available."""
    method = getattr(db, "fetch_df", None)
    if method is not None:
        return await method(sql, *args)
    rows = await db.fetch(sql, *args)
    return pd.DataFrame.from_records(rows)
