from memframe_ai.tools._helpers import normalize


def tools(session):
    w = session.wrappers.cleaning

    async def dropna(how: str = "any", thresh: int | None = None) -> dict:
        """Drop rows with missing values. how: 'any' (default) or 'all'; thresh: min non-null count."""
        return await normalize(await w.adropna(axis=0, how=how, thresh=thresh), session)

    async def drop_duplicates(subset: list[str] | None = None, keep: str = "first") -> dict:
        """Drop duplicate rows; subset restricts comparison to the given columns. keep: 'first'/'last'/False."""
        return await normalize(await w.adrop_duplicates(subset=subset, keep=keep), session)

    async def fillna(column: str, value=None, mode: str | None = None) -> dict:
        """Fill missing values in one column.

        Pass value=... for a constant fill; otherwise mode picks the strategy:
        'mean'/'median'/'mode'/'min'/'max'/'std'/'var' (numeric+datetime)
        or 'now' (datetime only). The orchestrator auto-detects dtype.
        """
        method = mode or ("constant" if value is not None else "mode")
        return await normalize(await w.afillna(column=column, value=value, method=method), session)

    async def drop_outliers(column: str, z_thresh: float = 3.0) -> dict:
        """Drop rows where the numeric column is a z-score outlier beyond z_thresh."""
        return await normalize(await w.adrop_outliers(column=column, z_thresh=z_thresh), session)

    async def clip(column: str, min_value=None, max_value=None) -> dict:
        """Enforce a numeric column to stay within [min_value, max_value]."""
        return await normalize(
            await w.aclip(column=column, lower=min_value, upper=max_value), session
        )

    async def to_numeric(column: str) -> dict:
        """Convert a text column to numeric values where possible."""
        return await normalize(await w.ato_numeric(column=column), session)

    async def map_values(column: str, mapping: dict) -> dict:
        """Remap categorical values in a column, e.g. {'old': 'new', 'a': 'b'}."""
        return await normalize(await w.amap_values(column=column, mapping=mapping), session)

    async def filter_valid(column: str, valid_values: list) -> dict:
        """Keep only rows where the column's value is in valid_values."""
        return await normalize(
            await w.afilter_valid(column=column, valid_values=valid_values), session
        )

    async def compress_rare(column: str, min_count: int = 10, other_label: str = "other") -> dict:
        """Replace rare categories (count < min_count) with other_label."""
        return await normalize(
            await w.acompress_rare(
                column=column, min_count=min_count, other_label=other_label
            ),
            session,
        )

    return [
        dropna,
        drop_duplicates,
        fillna,
        drop_outliers,
        clip,
        to_numeric,
        map_values,
        filter_valid,
        compress_rare,
    ]