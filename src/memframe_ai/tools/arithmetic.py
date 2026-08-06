from memframe.core.analytix.arithmetic import ArithmeticOps

from memframe_ai.tools._helpers import normalize


def tools(session):
    async def _ops():
        await session.ensure()
        return ArithmeticOps(session.adapter)

    async def _run(op, col1, col2, target_col):
        ops = await _ops()
        method = getattr(ops, op)
        kwargs = {}
        if target_col is not None:
            kwargs["target_col"] = target_col
        result = await method(
            session.table, session.schema, col1, col2,
            **session.transform_kwargs(), **kwargs,
        )
        return await normalize(result, session)

    async def add(col1, col2, target_col=None) -> dict:
        """Add two columns or a scalar to a column, writing the result to target_col.

        col1 and col2 can each be a column name or a numeric scalar. If target_col
        is omitted a column named like 'col1_add_col2' is created. The result
        becomes the new active table.
        """
        return await _run("add", col1, col2, target_col)

    async def subtract(col1, col2, target_col=None) -> dict:
        """Subtract col2 (column or scalar) from col1, writing the result to a new column.

        e.g. subtract('monthly_income', 'emi', target_col='net_income') computes
        monthly_income - emi per row into a new 'net_income' column. If target_col
        is omitted a column like 'col1_sub_col2' is created.
        """
        return await _run("subtract", col1, col2, target_col)

    async def multiply(col1, col2, target_col=None) -> dict:
        """Multiply col1 by col2 (columns or scalars) into a new column."""
        return await _run("multiply", col1, col2, target_col)

    async def divide(col1, col2, target_col=None) -> dict:
        """Divide col1 by col2 (columns or scalars) into a new column (divide-by-zero is NULL)."""
        return await _run("divide", col1, col2, target_col)

    return [add, subtract, multiply, divide]