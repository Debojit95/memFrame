from memframe.core.analytix.cleaning import DataCleaningOps

from memframe_ai.tools._helpers import normalize

_NUMERIC = ("int", "float", "double", "decimal", "numeric", "bigint", "real", "smallint")


def tools(session):
    async def _ops():
        await session.ensure()
        return DataCleaningOps(session.adapter)

    async def _column_dtype(column: str) -> str:
        col_types = await session.adapter.get_column_types(session.table, session.schema)
        return (col_types.get(column) or "").lower()

    async def dropna(how: str = "any", thresh: int | None = None) -> dict:
        """Drop rows with missing values. how: 'any' (default) or 'all'; thresh: min non-null count."""
        ops = await _ops()
        result = await ops.dataframe_dropna(
            session.table, session.schema, axis=0, how=how, thresh=thresh,
            **session.transform_kwargs(),
        )
        return await normalize(result, session)

    async def drop_duplicates(subset: list[str] | None = None, keep: str = "first") -> dict:
        """Drop duplicate rows; subset restricts comparison to the given columns. keep: 'first'/'last'/False."""
        ops = await _ops()
        result = await ops.dataframe_drop_duplicates(
            session.table, session.schema, subset=subset, keep=keep,
            **session.transform_kwargs(),
        )
        return await normalize(result, session)

    async def fillna(column: str, value=None, mode: str | None = None) -> dict:
        """Fill missing values in one column. Pass value for a constant, or mode: 'mean'/'median'/'mode'."""
        ops = await _ops()
        dtype = await _column_dtype(column)
        default_mode = "constant" if value is not None else "mode"
        mode = (mode or default_mode).upper()
        if any(t in dtype for t in _NUMERIC):
            result = await ops.numeric_fillna(
                session.table, session.schema, column, value=value, mode=mode,
                **session.transform_kwargs(),
            )
        elif "date" in dtype or "time" in dtype or "timestamp" in dtype:
            result = await ops.datetime_fillna(
                session.table, session.schema, column, value=value, mode=mode,
                **session.transform_kwargs(),
            )
        else:
            result = await ops.categorical_fillna(
                session.table, session.schema, column, value=value, mode=mode,
                **session.transform_kwargs(),
            )
        return await normalize(result, session)

    async def drop_outliers(column: str, z_thresh: float = 3.0) -> dict:
        """Drop rows where the numeric column is a z-score outlier beyond z_thresh."""
        ops = await _ops()
        result = await ops.numeric_drop_outliers_zscore(
            session.table, session.schema, column, z_thresh=z_thresh,
            **session.transform_kwargs(),
        )
        return await normalize(result, session)

    async def clip(column: str, min_value=None, max_value=None) -> dict:
        """Enforce a numeric column to stay within [min_value, max_value]."""
        ops = await _ops()
        result = await ops.numeric_enforce_range(
            session.table, session.schema, column, min_value=min_value, max_value=max_value,
            **session.transform_kwargs(),
        )
        return await normalize(result, session)

    async def to_numeric(column: str) -> dict:
        """Convert a text column to numeric values where possible."""
        ops = await _ops()
        result = await ops.numeric_convert_text(
            session.table, session.schema, column, **session.transform_kwargs()
        )
        return await normalize(result, session)

    async def map_values(column: str, mapping: dict) -> dict:
        """Remap categorical values in a column, e.g. {'old': 'new', 'a': 'b'}."""
        ops = await _ops()
        result = await ops.categorical_map_values(
            session.table, session.schema, column, mapping=mapping,
            **session.transform_kwargs(),
        )
        return await normalize(result, session)

    async def filter_valid(column: str, valid_values: list) -> dict:
        """Keep only rows where the column's value is in valid_values."""
        ops = await _ops()
        result = await ops.categorical_filter_invalid(
            session.table, session.schema, column, valid_values=valid_values,
            **session.transform_kwargs(),
        )
        return await normalize(result, session)

    async def compress_rare(column: str, min_count: int = 10, other_label: str = "other") -> dict:
        """Replace rare categories (count < min_count) with other_label."""
        ops = await _ops()
        result = await ops.categorical_compress_rare(
            session.table, session.schema, column, min_count=min_count, other_label=other_label,
            **session.transform_kwargs(),
        )
        return await normalize(result, session)

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
