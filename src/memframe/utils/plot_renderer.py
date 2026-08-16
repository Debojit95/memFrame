import os
import webbrowser

import plotly.io as pio


def setup_plotly_renderer():
    """
    Universal Plotly renderer setup.

    Supports:
    - Google Colab
    - Jupyter
    - VSCode notebooks
    - Plain terminal / PowerShell / CMD
    - Fallback HTML rendering

    Returns:
        renderer_name (str)
    """

    # --------------------------------------------------
    # Google Colab
    # --------------------------------------------------
    if "COLAB_GPU" in os.environ:
        renderer = "colab"

    # --------------------------------------------------
    # VSCode notebook
    # --------------------------------------------------
    elif "VSCODE_PID" in os.environ:
        renderer = "vscode"

    # --------------------------------------------------
    # Jupyter notebook/lab
    # --------------------------------------------------
    elif "JPY_PARENT_PID" in os.environ:
        renderer = "notebook_connected"

    # --------------------------------------------------
    # Plain terminal / powershell / cmd
    # --------------------------------------------------
    else:
        renderer = "browser"

    pio.renderers.default = renderer

    print(f"[Plotly] Using renderer: {renderer}")

    return renderer


def smart_show(fig, filename="plot.html"):
    """
    Smart Plotly display function.

    Behavior:
    - Colab/Jupyter/VSCode -> inline render
    - Terminal/PowerShell/CMD -> browser
    - Fallback -> save HTML and open manually
    """

    setup_plotly_renderer()

    try:
        fig.show()

    except Exception as e:
        print(f"[Plotly] fig.show() failed: {e}")

        print("[Plotly] Falling back to HTML export...")

        abs_path = os.path.abspath(filename)
        abs_uri = f"file://{abs_path}"
        fig.write_html(abs_path)

    try:
        webbrowser.open(abs_uri,1)
        print(f"[Plotly] Opened: {abs_path}")
    except Exception as browser_error:
        print(f"[Plotly] Browser open failed: {browser_error}")
        print(f"[Plotly] HTML saved at: {abs_path}")


def in_notebook() -> bool:
    """True only when running inside a live IPython kernel (Colab/Jupyter/VSCode)."""
    try:
        ip = __import__("IPython").get_ipython  # raises if IPython missing
    except Exception:
        return False
    return ip() is not None


def slice_for_display(df, max_rows: int = 0, max_cols: int = 0):
    """Return a view of df respecting the row/col caps.

    max_rows<=0 / max_cols<=0 mean NO cap (all rows / all columns). Pure, so it
    is unit-testable without a live notebook kernel.
    """
    out = df
    if max_rows and max_rows > 0:
        out = out.head(max_rows)
    if max_cols and max_cols > 0:
        out = out.iloc[:, :max_cols]
    return out


def display_df(df, max_rows: int = 0, max_cols: int = 0):
    """Render a DataFrame inline with pandas/Colab's default truncation.

    Renders the standard pandas/Colab view (truncated to avoid multi-MB HTML
    on large frames). Pass max_rows>0 / max_cols>0 to override a cap; the
    un-capped case (default max_rows<=0 / max_cols<=0) lets pandas' own
    defaults decide. The full frame is still returned to the caller via the
    chat result dict; this only controls the inline rendering. No-op outside a
    live notebook kernel. Must be called from the main kernel context, not from
    inside CodeMode tool dispatch where rich display does not propagate.
    """
    if not in_notebook():
        return
    try:
        from IPython.display import display

        display(slice_for_display(df, max_rows, max_cols))
    except Exception:
        pass
