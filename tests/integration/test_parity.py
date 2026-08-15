"""Layer 4: cross-backend result parity.

Each operation is run against the configured backend(s) and compared to a
deterministic pandas reference. When multiple backends are configured, each
instance checks its backend against the same reference, so all backends must
agree transitively.
"""

import asyncio

import pytest

pytestmark = pytest.mark.integration


def _as_float_frame(df):
    """Drop non-numeric columns so dtype differences across backends don't mask values."""
    numeric = df.select_dtypes(include="number")
    return numeric.reset_index(drop=True)


class TestArithmeticParity:
    def test_add_parity(self, uploaded_ctx, sample_df, get_result_df):
        result = asyncio.run(uploaded_ctx.aadd("a", "b"))
        actual = _as_float_frame(get_result_df(result))
        expected = (sample_df["a"] + sample_df["b"]).to_frame().reset_index(drop=True)
        assert len(actual) == len(expected)
        assert actual.iloc[:, -1].tolist() == pytest.approx(expected.iloc[:, 0].tolist())

    def test_mul_parity(self, uploaded_ctx, sample_df, get_result_df):
        result = asyncio.run(uploaded_ctx.amul("a", "b"))
        actual = _as_float_frame(get_result_df(result))
        expected = (sample_df["a"] * sample_df["b"]).to_frame().reset_index(drop=True)
        assert len(actual) == len(expected)
        assert actual.iloc[:, -1].tolist() == pytest.approx(expected.iloc[:, 0].tolist())


class TestSelectionParity:
    def test_head_parity(self, uploaded_ctx, sample_df, get_result_df):
        result = asyncio.run(uploaded_ctx.ahead(2))
        actual = get_result_df(result)
        assert len(actual) == 2
        assert list(actual.columns) == list(sample_df.columns)

    def test_select_dtypes_numeric(self, uploaded_ctx, sample_df, get_result_df):
        result = asyncio.run(uploaded_ctx.aselect_dtypes(include="numeric"))
        actual = get_result_df(result)
        expected_cols = [c for c in sample_df.columns if sample_df[c].dtype.kind in "iuf"]
        assert sorted(actual.columns) == sorted(expected_cols)


class TestStatsParity:
    def test_sum_parity(self, uploaded_ctx, sample_df):
        result = asyncio.run(uploaded_ctx.asum("a"))
        assert result == pytest.approx(sample_df["a"].sum())
