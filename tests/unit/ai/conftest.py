import pytest


@pytest.fixture(autouse=True)
def _suppress_inline_plot_display():
    """AI plot tools must not open Plotly's browser/HTTP renderer in tests."""
    from memframe.utils.plot_renderer import suppress_inline_display

    with suppress_inline_display():
        yield
