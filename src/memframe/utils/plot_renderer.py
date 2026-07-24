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

    renderer = setup_plotly_renderer()

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
