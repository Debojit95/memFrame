from memframe_ai.tools._helpers import normalize


def tools(session):
    w = session.wrappers.stats

    # ----- unified / dtype-routed -----
    async def count(column: str) -> dict:
        """Count of non-null values in `column`."""
        return await normalize(await w.acount(column=column), session)

    async def min(column: str) -> dict:
        """Minimum value of `column`."""
        return await normalize(await w.amin(column=column), session)

    async def max(column: str) -> dict:
        """Maximum value of `column`."""
        return await normalize(await w.amax(column=column), session)

    async def mode(column: str, top_n: int = 1) -> dict:
        """Most frequent value(s) of `column`. top_n=1 returns a single mode."""
        return await normalize(await w.amode(column=column, top_n=top_n), session)

    async def unique(column: str) -> dict:
        """List of distinct values in `column`."""
        return await normalize(await w.aunique(column=column), session)

    async def nunique(column: str) -> dict:
        """Number of distinct values in `column`."""
        return await normalize(await w.anunique(column=column), session)

    async def value_counts(column: str, top_n: int = 10) -> dict:
        """Top_n most frequent values of `column` and their counts.

        Numeric columns are auto-routed to numeric stats by the orchestrator.
        """
        return await normalize(await w.avalue_counts(column=column, top_n=top_n), session)

    async def mean(column: str) -> dict:
        """Mean (average) of `column`."""
        return await normalize(await w.amean(column=column), session)

    async def median(column: str) -> dict:
        """Median of `column`."""
        return await normalize(await w.amedian(column=column), session)

    # ----- numeric scalars -----
    async def sum(column: str) -> dict:
        """Sum of `column`."""
        return await normalize(await w.asum(column=column), session)

    async def std(column: str) -> dict:
        """Population standard deviation of `column`."""
        return await normalize(await w.astd(column=column), session)

    async def var(column: str) -> dict:
        """Population variance of `column`."""
        return await normalize(await w.avar(column=column), session)

    async def sem(column: str) -> dict:
        """Standard error of the mean of `column`."""
        return await normalize(await w.asem(column=column), session)

    async def mad(column: str) -> dict:
        """Mean absolute deviation of `column`."""
        return await normalize(await w.amad(column=column), session)

    async def iqr(column: str) -> dict:
        """Interquartile range (Q3 - Q1) of `column`."""
        return await normalize(await w.aiqr(column=column), session)

    async def range(column: str) -> dict:
        """Numeric range (max - min) of `column`."""
        return await normalize(await w.arange(column=column), session)

    async def skew(column: str) -> dict:
        """Skewness of `column` (Fisher-Pearson, matches pandas)."""
        return await normalize(await w.askew(column=column), session)

    async def kurtosis(column: str) -> dict:
        """Excess kurtosis of `column` (matches pandas Series.kurtosis)."""
        return await normalize(await w.akurtosis(column=column), session)

    async def entropy(column: str) -> dict:
        """Shannon entropy of `column`'s value distribution, in nats."""
        return await normalize(await w.aentropy(column=column), session)

    async def quantile(column: str, q: list[float] | None = None) -> dict:
        """Quantiles of `column`; default q=[0.25, 0.5, 0.75]."""
        return await normalize(await w.aquantile(column=column, q=q), session)

    async def autocorr(column: str, lag: int = 1) -> dict:
        """Autocorrelation of `column` at the given lag."""
        return await normalize(await w.aautocorr(column=column, lag=lag), session)

    async def coefficient_of_variation(column: str) -> dict:
        """Coefficient of variation (stddev / mean) of `column`."""
        return await normalize(await w.acoefficient_of_variation(column=column), session)

    async def outliers_iqr(column: str) -> dict:
        """IQR-rule outlier values for `column` (q1-1.5*IQR, q3+1.5*IQR)."""
        return await normalize(await w.aoutliers_iqr(column=column), session)

    async def outliers_zscore(column: str, threshold: float = 3.0) -> dict:
        """Z-score outlier values for `column` (|x-mean| > threshold*std)."""
        return await normalize(
            await w.aoutliers_zscore(column=column, threshold=threshold), session
        )

    # ----- multi-column (return result tables) -----
    async def corr(columns: list[str]) -> dict:
        """Pairwise correlation matrix over `columns`; persisted as a result table."""
        return await normalize(await w.acorr(columns=columns), session)

    async def cov(columns: list[str]) -> dict:
        """Pairwise covariance matrix over `columns`; persisted as a result table."""
        return await normalize(await w.acov(columns=columns), session)

    # ----- categorical -----
    async def proportions(column: str) -> dict:
        """Proportion of each category in `column`."""
        return await normalize(await w.aproportions(column=column), session)

    async def chi_square(column1: str, column2: str) -> dict:
        """Chi-square test of independence between `column1` and `column2`."""
        return await normalize(await w.achi_square(column1=column1, column2=column2), session)

    async def cramers_v(column1: str, column2: str) -> dict:
        """Cramér's V association score between `column1` and `column2`."""
        return await normalize(await w.acramers_v(column1=column1, column2=column2), session)

    async def theil_u(column1: str, column2: str) -> dict:
        """Theil's U (uncertainty coefficient) of `column1` given `column2`."""
        return await normalize(await w.atheil_u(column1=column1, column2=column2), session)

    async def mutual_information(column1: str, column2: str) -> dict:
        """Mutual information between `column1` and `column2`."""
        return await normalize(
            await w.amutual_information(column1=column1, column2=column2), session
        )

    # ----- datetime -----
    async def datetime_diff(column: str) -> dict:
        """Consecutive differences of a datetime `column` (in seconds)."""
        return await normalize(await w.adatetime_diff(column=column), session)

    async def time_delta_stats(column: str) -> dict:
        """Stats over datetime deltas in `column` (mean/median/std/min/max)."""
        return await normalize(await w.atime_delta_stats(column=column), session)

    async def event_rate(column: str, unit: str = "day") -> dict:
        """Event count per `unit` of `column` (unit: 'day'/'hour'/'minute'/etc.)."""
        return await normalize(await w.aevent_rate(column=column, unit=unit), session)

    async def time_unit_counts(column: str, unit: str = "day") -> dict:
        """Count of events per `unit` of `column`."""
        return await normalize(await w.atime_unit_counts(column=column, unit=unit), session)

    async def weekday_weekend_counts(column: str) -> dict:
        """Count of weekday vs weekend events from `column`."""
        return await normalize(await w.aweekday_weekend_counts(column=column), session)

    async def holiday_counts(column: str) -> dict:
        """Count of events on public holidays from `column`."""
        return await normalize(await w.aholiday_counts(column=column), session)

    return [
        # unified
        count, min, max, mode, unique, nunique, value_counts, mean, median,
        # numeric
        sum, std, var, sem, mad, iqr, range, skew, kurtosis, entropy,
        quantile, autocorr, coefficient_of_variation, outliers_iqr, outliers_zscore,
        # multi-col
        corr, cov,
        # categorical
        proportions, chi_square, cramers_v, theil_u, mutual_information,
        # datetime
        datetime_diff, time_delta_stats, event_rate, time_unit_counts,
        weekday_weekend_counts, holiday_counts,
    ]