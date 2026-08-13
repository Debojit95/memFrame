from memframe_ai.tools._helpers import normalize


def tools(session):
    w = session.wrappers.arithmetic

    async def _binary(method, col1, col2, target_col):
        kwargs = {"target_col": target_col} if target_col is not None else {}
        return await normalize(await method(col1, col2, **kwargs), session)

    async def _unary(method, column, target_col, **extra):
        kwargs = {**extra, "target_col": target_col} if target_col is not None else extra
        return await normalize(await method(column=column, **kwargs), session)

    # ----- binary ops -----
    async def add(col1, col2, target_col=None) -> dict:
        """Add `col1` + `col2` (columns or scalars) into a new column."""
        return await _binary(w.aadd, col1, col2, target_col)

    async def subtract(col1, col2, target_col=None) -> dict:
        """Subtract `col2` from `col1` (columns or scalars) into a new column."""
        return await _binary(w.asubtract, col1, col2, target_col)

    async def sub(col1, col2, target_col=None) -> dict:
        """Alias of subtract: `col1` − `col2` into a new column."""
        return await _binary(w.asub, col1, col2, target_col)

    async def mul(col1, col2, target_col=None) -> dict:
        """Multiply `col1` × `col2` (columns or scalars) into a new column."""
        return await _binary(w.amul, col1, col2, target_col)

    async def multiply(col1, col2, target_col=None) -> dict:
        """Alias of mul: `col1` × `col2` into a new column."""
        return await _binary(w.amul, col1, col2, target_col)

    async def div(col1, col2, target_col=None) -> dict:
        """Divide `col1` / `col2` (columns or scalars) into a new column (divide-by-zero = NULL)."""
        return await _binary(w.adiv, col1, col2, target_col)

    async def divide(col1, col2, target_col=None) -> dict:
        """Alias of div: `col1` / `col2` into a new column (divide-by-zero = NULL)."""
        return await _binary(w.adiv, col1, col2, target_col)

    async def mod(col1, col2, target_col=None) -> dict:
        """Modulo `col1` % `col2` (columns or scalars) into a new column."""
        return await _binary(w.amod, col1, col2, target_col)

    async def pow(col1, col2, target_col=None) -> dict:
        """Raise `col1` to the power `col2` (columns or scalars) into a new column."""
        return await _binary(w.apow, col1, col2, target_col)

    # ----- unary ops -----
    async def abs(column, target_col=None) -> dict:
        """Absolute value of `column` into a new column."""
        return await _unary(w.aabs, column, target_col)

    async def negate(column, target_col=None) -> dict:
        """Negate `column` into a new column."""
        return await _unary(w.anegate, column, target_col)

    async def round(column, digits=0, target_col=None) -> dict:
        """Round `column` to `digits` decimal places into a new column."""
        kwargs = {"digits": digits, "target_col": target_col} if target_col is not None else {"digits": digits}
        return await normalize(await w.around(column=column, **kwargs), session)

    async def ceil(column, target_col=None) -> dict:
        """Ceiling of `column` into a new column."""
        return await _unary(w.aceil, column, target_col)

    async def floor(column, target_col=None) -> dict:
        """Floor of `column` into a new column."""
        return await _unary(w.afloor, column, target_col)

    async def truncate(column, digits=0, target_col=None) -> dict:
        """Truncate `column` to `digits` decimal places into a new column."""
        kwargs = {"digits": digits, "target_col": target_col} if target_col is not None else {"digits": digits}
        return await normalize(await w.atruncate(column=column, **kwargs), session)

    # ----- exp / log / root -----
    async def exp(column, target_col=None) -> dict:
        """Exponential (e^x) of `column` into a new column."""
        return await _unary(w.aexp, column, target_col)

    async def log(column, target_col=None) -> dict:
        """Natural log (ln) of `column` into a new column."""
        return await _unary(w.alog, column, target_col)

    async def log10(column, target_col=None) -> dict:
        """Base-10 log of `column` into a new column."""
        return await _unary(w.alog10, column, target_col)

    async def sqrt(column, target_col=None) -> dict:
        """Square root of `column` into a new column."""
        return await _unary(w.asqrt, column, target_col)

    # ----- inverse trig -----
    async def asin(column, target_col=None) -> dict:
        """Inverse-sine (arcsin) of `column` into a new column."""
        return await _unary(w.aasin, column, target_col)

    async def acos(column, target_col=None) -> dict:
        """Inverse-cosine (arccos) of `column` into a new column."""
        return await _unary(w.aacos, column, target_col)

    async def atan(column, target_col=None) -> dict:
        """Inverse-tangent (arctan) of `column` into a new column."""
        return await _unary(w.aatan, column, target_col)

    async def atan2(col1, col2, target_col=None) -> dict:
        """Two-argument arctangent of `col1` / `col2` (columns or scalars) into a new column."""
        return await _binary(w.aatan2, col1, col2, target_col)

    # ----- complex -----
    async def weighted_sum(col1, col2, weight1=1, weight2=1, target_col=None) -> dict:
        """Compute weight1·col1 + weight2·col2 into a new column."""
        kwargs = {"weight1": weight1, "weight2": weight2}
        if target_col is not None:
            kwargs["target_col"] = target_col
        return await normalize(await w.aweighted_sum(col1, col2, **kwargs), session)

    async def percentage_change(old_col, new_col, target_col=None) -> dict:
        """Compute (new − old) / old between `old_col` and `new_col` into a new column."""
        kwargs = {"target_col": target_col} if target_col is not None else {}
        return await normalize(await w.apercentage_change(old_col, new_col, **kwargs), session)

    async def normalize_range(column, target_col=None) -> dict:
        """Normalize `column` to the configured numeric range into a new column."""
        return await _unary(w.anormalize_range, column, target_col)

    return [
        # binary
        add, subtract, sub, mul, multiply, div, divide, mod, pow,
        # unary
        abs, negate, round, ceil, floor, truncate,
        # exp / log / root
        exp, log, log10, sqrt,
        # inverse trig
        asin, acos, atan, atan2,
        # complex
        weighted_sum, percentage_change, normalize_range,
    ]