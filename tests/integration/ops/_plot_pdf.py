"""Shared Plotly/matplotlib → PDF helper for plot integration tests.

Plot wrappers return plotly Figures, which PdfPages cannot embed directly,
so plotly figures are rasterized to PNG first (requires ``kaleido`` from
the dev extras). Anything else falls back to the pre-existing matplotlib
path or a placeholder page, so reporting never fails the suite.
"""

import io

import matplotlib.pyplot as plt


def render_fig_to_pdf_page(
    pdf,
    title,
    method_call,
    fig,
    backend,
    status="PASSED",
    error_message="",
):
    # ponytail: ctx.*plot returns plotly Figures (no suptitle); rasterize to
    # PNG for the report, placeholder page if export is unavailable.
    if fig is not None and hasattr(fig, "to_image"):
        try:
            png = fig.to_image(format="png", scale=2)
            img = plt.imread(io.BytesIO(png))
            page = plt.figure(figsize=(16, 9))
            page.suptitle(
                f"{title}  [{backend}]  {status}\nCall: {method_call}",
                fontsize=12,
                fontweight="bold",
            )
            ax = page.add_subplot(111)
            ax.imshow(img)
            ax.axis("off")
            if error_message:
                page.text(
                    0.01, 0.02, f"Failure: {error_message}",
                    fontsize=9, color="crimson",
                )
            pdf.savefig(page, bbox_inches="tight")
            plt.close(page)
            return
        except Exception:
            pass
    if fig is not None and hasattr(fig, "suptitle"):
        fig.suptitle(
            f"{title}  [{backend}]  {status}\nCall: {method_call}",
            fontsize=12,
            fontweight="bold",
            y=1.02,
        )
        if error_message:
            fig.text(
                0.01, 0.98, f"Failure: {error_message}",
                fontsize=9, color="crimson", transform=fig.transFigure,
            )
        pdf.savefig(fig, bbox_inches="tight")
        plt.close(fig)
    else:
        page = plt.figure(figsize=(16, 4))
        page.suptitle(f"{title}  [{backend}]  {status}", fontsize=12, fontweight="bold")
        page.text(0.01, 0.9, f"Call: {method_call}", fontsize=10, family="monospace")
        if error_message:
            page.text(0.01, 0.85, f"Failure: {error_message}", fontsize=9, color="crimson")
        page.text(
            0.5, 0.5,
            "No figure generated" if fig is None else "Figure type not embeddable in PDF",
            ha="center", va="center", fontsize=14,
        )
        pdf.savefig(page)
        plt.close(page)
