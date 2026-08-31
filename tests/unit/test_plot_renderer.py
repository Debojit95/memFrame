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


def test_suppress_inline_display_toggles():
    from memframe.utils.plot_renderer import (
        inline_display_suppressed,
        suppress_inline_display,
    )

    assert inline_display_suppressed() is False
    with suppress_inline_display():
        assert inline_display_suppressed() is True
    assert inline_display_suppressed() is False


def test_smart_show_skipped_when_suppressed():
    # ponytail: during a dashboard build, the chat pipeline must not render
    # individual plots inline (in any environment).
    from memframe.utils.plot_renderer import smart_show, suppress_inline_display

    class _Fig:
        def show(self):
            raise AssertionError("fig.show() must not run when suppressed")

    with suppress_inline_display():
        smart_show(_Fig())  # must not raise


def test_display_df_skipped_when_suppressed(monkeypatch):
    # ponytail: result tables must not render inline during a dashboard build.
    from memframe.utils.plot_renderer import (
        display_df,
        suppress_inline_display,
    )

    monkeypatch.setattr(
        "memframe.utils.plot_renderer.in_notebook", lambda: True
    )
    df = pd.DataFrame({"a": range(30)})
    with suppress_inline_display():
        assert display_df(df) is None  # suppressed -> no-op, even in a notebook


def test_colab_env_selects_colab_renderer(monkeypatch):
    # ponytail: colab detection is env-var based ("COLAB_GPU"); when present
    # setup_plotly_renderer() must select the colab renderer.
    from memframe.utils.plot_renderer import setup_plotly_renderer

    monkeypatch.setattr("memframe.utils.plot_renderer.os.environ", {"COLAB_GPU": "1"})

    assert setup_plotly_renderer() == "colab"

