from memframe_ai.tools._helpers import normalize


def tools(session):
    w = session.wrappers.selection

    # ----- label-based -----
    async def at(row_label, column_label: str, index_column: str | None = None) -> dict:
        """Scalar access by row label and column label (use `index_column` for non-default index)."""
        return await normalize(
            await w.aat(row_label=row_label, column_label=column_label, index_column=index_column),
            session,
        )

    async def loc(row_selector, columns: list[str] | None = None, index_column: str | None = None, chunk_size: int | None = None) -> dict:
        """Label-based row/column selection. Pass `row_selector` (e.g. slice) and optional `columns`."""
        return await normalize(
            await w.aloc(
                row_selector=row_selector, columns=columns,
                index_column=index_column, chunk_size=chunk_size,
            ),
            session,
        )

    # ----- integer-position -----
    async def iat(row_position: int, column_label: str, order_by) -> dict:
        """Scalar access by integer row position; specify `order_by` columns to define row order."""
        return await normalize(
            await w.aiat(row_position=row_position, column_label=column_label, order_by=order_by),
            session,
        )

    async def iloc(row_indexer=None, col_indexer=None, columns: list[str] | None = None) -> dict:
        """Integer-location row/column selection. Pass integer indices/slice via `row_indexer`/`col_indexer`."""
        return await normalize(
            await w.ailoc(
                row_indexer=row_indexer, col_indexer=col_indexer, columns=columns,
            ),
            session,
        )

    async def take(indices: list[int], axis: int = 0) -> dict:
        """Select rows (axis=0) or columns (axis=1) by integer indices."""
        return await normalize(await w.atake(indices=indices, axis=axis), session)

    # ----- column-level -----
    async def get(keys, default=None) -> dict:
        """Retrieve one or more columns by name; returns `default` if missing."""
        return await normalize(await w.aget(keys=keys, default=default), session)

    async def select_columns(columns: list[str]) -> dict:
        """Create a transient table containing only the named columns."""
        return await normalize(
            await w.aloc(row_selector=slice(None), columns=columns), session
        )

    async def select_dtypes(include: list[str] | str | None = None, exclude: list[str] | str | None = None, chunk_size: int | None = None) -> dict:
        """Select columns by included/excluded dtypes (e.g. 'int', 'float', 'datetime')."""
        return await normalize(
            await w.aselect_dtypes(include=include, exclude=exclude, chunk_size=chunk_size),
            session,
        )

    # ----- condition-based -----
    async def where(cond: str, other=None, chunk_size: int | None = None) -> dict:
        """Filter rows by SQL condition (e.g. 'age > 30'); create a transient table."""
        return await normalize(
            await w.awhere(cond=cond, other=other, chunk_size=chunk_size), session
        )

    async def asof(where, on: str, subset=None, chunk_size: int | None = None) -> dict:
        """As-of join: match rows to the latest reference value in `on` column.

        `where`: filter condition (str or list[str]); `subset`: optional extra columns to include.
        """
        return await normalize(
            await w.aasof(where=where, on=on, subset=subset, chunk_size=chunk_size),
            session,
        )

    return [
        at, loc, iat, iloc, take,
        get, select_columns, select_dtypes,
        where, asof,
    ]