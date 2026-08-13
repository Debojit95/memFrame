from memframe_ai.tools._helpers import normalize


def tools(session):
    w = session.wrappers.cleaning

    # ----- fill / drop -----
    async def fillna(column: str, value=None, mode: str | None = None) -> dict:
        """Fill missing values in `column`.

        Pass value=... for a constant fill; otherwise mode picks the strategy:
        'mean'/'median'/'mode'/'min'/'max'/'std'/'var' (numeric+datetime)
        or 'now' (datetime only). The orchestrator auto-detects dtype.
        """
        method = mode or ("constant" if value is not None else "mode")
        return await normalize(await w.afillna(column=column, value=value, method=method), session)

    async def groupby_fillna(column: str, group_cols: list[str], value=None, method: str = "mean", dtype: str | None = None) -> dict:
        """Fill missing values in `column` using per-group statistics.

        group_cols: columns to group by. method: 'mean'/'median'/'mode'/etc.
        dtype: 'numeric'/'categorical'/'datetime' (auto-detected if omitted).
        """
        return await normalize(
            await w.agroupby_fillna(
                column=column, group_cols=group_cols,
                value=value, method=method, dtype=dtype,
            ),
            session,
        )

    async def dropna(how: str = "any", thresh: int | None = None) -> dict:
        """Drop rows with missing values. how: 'any' (default) or 'all'; thresh: min non-null count."""
        return await normalize(await w.adropna(axis=0, how=how, thresh=thresh), session)

    async def drop_duplicates(subset: list[str] | None = None, keep: str = "first") -> dict:
        """Drop duplicate rows; subset restricts comparison to the given columns. keep: 'first'/'last'/False."""
        return await normalize(await w.adrop_duplicates(subset=subset, keep=keep), session)

    async def drop(columns: list[str] | None = None, axis: int = 0, index: list[int] | None = None) -> dict:
        """Drop rows (axis=0, by integer index) or columns (axis=1, by name) from the active table."""
        return await normalize(await w.adrop(columns=columns, axis=axis, index=index), session)

    async def drop_outliers(column: str, z_thresh: float = 3.0) -> dict:
        """Drop rows where the numeric column's z-score exceeds z_thresh."""
        return await normalize(await w.adrop_outliers(column=column, z_thresh=z_thresh), session)

    # ----- transform / coerce -----
    async def clip(column: str, min_value=None, max_value=None) -> dict:
        """Clip numeric `column` to [min_value, max_value]."""
        return await normalize(
            await w.aclip(column=column, lower=min_value, upper=max_value), session
        )

    async def clip_dates(column: str, min_dt: str | None = None, max_dt: str | None = None) -> dict:
        """Clip datetime `column` to optional ISO min_dt / max_dt bounds."""
        return await normalize(
            await w.aclip_dates(column=column, min_dt=min_dt, max_dt=max_dt), session
        )

    async def fix_dates(column: str) -> dict:
        """Parse and normalize date values in `column`."""
        return await normalize(await w.afix_dates(column=column), session)

    async def to_numeric(column: str) -> dict:
        """Convert text values in `column` to numeric where possible."""
        return await normalize(await w.ato_numeric(column=column), session)

    async def map_values(column: str, mapping: dict) -> dict:
        """Remap values in `column` via {old: new}."""
        return await normalize(await w.amap_values(column=column, mapping=mapping), session)

    async def filter_valid(column: str, valid_values: list) -> dict:
        """Keep rows where `column` ∈ valid_values."""
        return await normalize(
            await w.afilter_valid(column=column, valid_values=valid_values), session
        )

    async def compress_rare(column: str, min_count: int = 10, other_label: str = "other") -> dict:
        """Replace categories with count < min_count by `other_label`."""
        return await normalize(
            await w.acompress_rare(
                column=column, min_count=min_count, other_label=other_label
            ),
            session,
        )

    # ----- null masks -----
    async def isna() -> dict:
        """Return a boolean mask (per row) of null values across the active table."""
        return await normalize(await w.aisna(), session)

    async def notna() -> dict:
        """Return a boolean mask (per row) of non-null values across the active table."""
        return await normalize(await w.anotna(), session)

    # ----- data quality / reporting -----
    async def data_quality_missing_values(columns: list[str]) -> dict:
        """Per-column missing-value data-quality metrics for `columns`."""
        return await normalize(
            await w.adata_quality_missing_values(columns=columns), session
        )

    async def data_quality_completeness_score(columns: list[str]) -> dict:
        """Per-column completeness score (non-null fraction) for `columns`."""
        return await normalize(
            await w.adata_quality_completeness_score(columns=columns), session
        )

    async def comprehensive_numeric_summary(columns: list[str]) -> dict:
        """Full numeric summary across `columns` (count/mean/std/min/quartiles/max + skew/kurt)."""
        return await normalize(
            await w.acomprehensive_numeric_summary(columns=columns), session
        )

    async def statistical_profile_report(columns: list[str]) -> dict:
        """Statistical profile report for `columns` (numeric stats + dtype/null overview)."""
        return await normalize(
            await w.astatistical_profile_report(columns=columns), session
        )

    return [
        # fill / drop
        fillna, groupby_fillna, dropna, drop_duplicates, drop, drop_outliers,
        # transform / coerce
        clip, clip_dates, fix_dates, to_numeric, map_values, filter_valid, compress_rare,
        # null masks
        isna, notna,
        # data quality / reporting
        data_quality_missing_values, data_quality_completeness_score,
        comprehensive_numeric_summary, statistical_profile_report,
    ]