import pandas as pd

from memframe.utils.plot_renderer import slice_for_display


def test_slice_for_display_default_is_full_frame():
    df = pd.DataFrame({"a": range(30), "b": range(30, 60), "c": range(60, 90)})
    out = slice_for_display(df)  # max_rows=0, max_cols=0 -> all rows, all cols
    assert len(out) == 30
    assert list(out.columns) == ["a", "b", "c"]


def test_slice_for_display_respects_caps():
    df = pd.DataFrame({"a": range(30), "b": range(30, 60), "c": range(60, 90)})
    out = slice_for_display(df, max_rows=5, max_cols=2)
    assert len(out) == 5
    assert list(out.columns) == ["a", "b"]


def test_display_df_is_noop_outside_notebook():
    """No kernel -> display_df must not raise (terminal / AI sandbox)."""
    from memframe.utils.plot_renderer import display_df

    df = pd.DataFrame({"a": range(30)})
    assert display_df(df) is None  # in_notebook() is False here
