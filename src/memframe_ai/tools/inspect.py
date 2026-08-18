from memframe_ai.tools._helpers import normalize


def tools(session):
    w = session.wrappers.inspection

    # ----- read-only row previews -----
    async def head(n: int = 10, columns: list[str] | None = None) -> dict:
        """First `n` rows of the active table (default 10)."""
        return await normalize(await w.ahead(n=n, columns=columns), session, advance=False)

    async def tail(n: int = 10, columns: list[str] | None = None) -> dict:
        """Last `n` rows of the active table (default 10)."""
        return await normalize(await w.atail(n=n, columns=columns), session, advance=False)

    async def sample(n: int = 10, columns: list[str] | None = None, random_state: int | None = None) -> dict:
        """`n` randomly sampled rows of the active table."""
        return await normalize(
            await w.asample(n=n, columns=columns, random_state=random_state),
            session, advance=False,
        )

    async def full_table(columns: list[str] | None = None, chunk_size: int | None = None) -> dict:
        """Return the full table as records (use `chunk_size` for large tables)."""
        return await normalize(
            await w.afull_table(columns=columns, chunk_size=chunk_size),
            session, advance=False,
        )

    # ----- summaries -----
    async def info() -> dict:
        """Column names, dtypes, and null counts for the active table."""
        return await normalize(await w.ainfo(), session, advance=False)

    async def describe(columns: list[str] | None = None) -> dict:
        """Summary statistics (count, mean, std, min, quartiles, max) for numeric columns."""
        return await normalize(await w.adescribe(columns=columns), session, advance=False)

    async def null_analysis(columns: list[str] | None = None) -> dict:
        """Missing-value counts and percentages for the given columns (or all)."""
        return await normalize(
            await w.anull_analysis(columns=columns), session, advance=False
        )

    # ----- structural property getters -----
    async def columns() -> dict:
        """Return column labels."""
        return await normalize(await w.acolumns(), session, advance=False)

    async def dtypes() -> dict:
        """Return column dtypes."""
        return await normalize(await w.adtypes(), session, advance=False)

    async def shape() -> dict:
        """(rows, columns) shape of the active table."""
        return await normalize(await w.ashape(), session, advance=False)

    async def values() -> dict:
        """Return the table values as a list of records."""
        return await normalize(await w.avalues(), session, advance=False)

    async def items() -> dict:
        """Iterate over (column_name, records) pairs."""
        return await normalize(await w.aitems(), session, advance=False)

    async def iterrows() -> dict:
        """Iterate over rows as (index, record) pairs."""
        return await normalize(await w.aiterrows(), session, advance=False)

    async def itertuples(index: bool = True) -> dict:
        """Iterate rows as named tuples; pass index=False to drop the index."""
        return await normalize(await w.aitertuples(index=index), session, advance=False)

    # ----- write ops (create a new transient table) -----
    async def astype(
        columns: list[str] | None = None,
        dtypes: list[str] | None = None,
        dtype_map: dict[str, str] | None = None,
    ) -> dict:
        """Cast `columns` to target dtypes; `dtype_map` is {col: 'INT'/'FLOAT'/...}."""
        return await normalize(
            await w.aastype(columns=columns, dtypes=dtypes, dtype_map=dtype_map),
            session,
        )

    async def insert(column: str, value) -> dict:
        """Insert or overwrite a column with a scalar `value`."""
        return await normalize(await w.ainsert(column=column, value=value), session)

    async def map(func: str, na_action: str | None = None, columns: list[str] | None = None, datetime_action: str = "skip") -> dict:
        """Apply a SQL `func` to values (e.g. 'UPPER', 'LOWER')."""
        return await normalize(
            await w.amap(
                func=func, na_action=na_action,
                columns=columns, datetime_action=datetime_action,
            ),
            session,
        )

    async def rename(columns: dict[str, str]) -> dict:
        """Rename columns using {old_name: new_name} mapping."""
        return await normalize(await w.arename(columns=columns), session)

    async def set_index(columns: list[str]) -> dict:
        """Set one or more `columns` as the table index."""
        return await normalize(await w.aset_index(columns=columns), session)

    async def update(
        on: str,
        other_table: str,
        other_schema: str = "upload",
        overwrite: bool = True,
        errors: str = "ignore",
    ) -> dict:
        """Join/update rows from `other_table` using key column `on`."""
        return await normalize(
            await w.aupdate(
                on=on, other_table=other_table, other_schema=other_schema,
                overwrite=overwrite, errors=errors,
            ),
            session,
        )

    async def resample(
        time_column: str,
        rule: str,
        agg: str = "COUNT",
        value_column: str | None = None,
        label: str = "left",
        closed: str = "left",
    ) -> dict:
        """Resample a time-series on `time_column` by `rule` (e.g. '1D') with aggregation `agg`."""
        return await normalize(
            await w.aresample(
                time_column=time_column, rule=rule, agg=agg,
                value_column=value_column, label=label, closed=closed,
            ),
            session,
        )

    return [
        # read-only row previews
        head, tail, sample, full_table,
        # summaries
        info, describe, null_analysis,
        # structural property getters
        columns, dtypes, shape, values, items, iterrows, itertuples,
        # write ops
        astype, insert, map, rename, set_index, update, resample,
    ]