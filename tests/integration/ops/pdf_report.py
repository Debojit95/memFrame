"""Shared PDF report renderer for the window integration tests."""

from __future__ import annotations

import textwrap

import matplotlib.pyplot as plt

CODE_WIDTH = 148


def _is_prose(text: str) -> bool:
    return "(" not in text


def _expression(text: str) -> str:
    if text.startswith(("dataset.", "await ", "mf.")):
        return text
    if text.startswith("."):
        return "dataset" + text
    if text.startswith("on("):
        return "dataset." + text
    if text.startswith(("arolling(", "aexpanding(", "aewm(", "aagg(")):
        return "await GroupByWindowStatsWrapper(ctx)." + text
    return "dataset." + text


def memframe_line(text: str) -> str:
    """Render a recorded call as a complete, runnable statement."""
    text = (text or "").strip()
    if not text:
        return "# (no call recorded)"
    if _is_prose(text):
        return "# " + text
    return "result = " + _expression(text)


def pandas_line(text: str) -> str:
    text = (text or "").strip()
    if not text:
        return "# (no pandas equivalent recorded)"
    if _is_prose(text):
        return "# " + text
    return "expected = " + text


def _wrap(line: str) -> list[str]:
    return textwrap.wrap(
        line,
        width=CODE_WIDTH,
        subsequent_indent="    ",
        break_long_words=False,
        break_on_hyphens=False,
    ) or [line]


def render_df_to_pdf_page(
    pdf,
    title,
    method_call,
    original_df,
    memframe_df,
    pandas_df,
    backend,
    status="PASSED",
    error_message="",
    pandas_call="",
):
    sections = [
        ("Original", original_df.head(10)),
        ("MemFrame Result", memframe_df.head(10)),
        ("Pandas Result", pandas_df.head(10)),
    ]
    fig_height = max(
        8, 2 + sum(max(2, len(df) + 2) for _, df in sections) * 0.4
    )
    fig, axes = plt.subplots(3, 1, figsize=(16, fig_height))
    fig.suptitle(
        f"{title}  [{backend}]  {status}",
        fontsize=12,
        fontweight="bold",
        y=0.985,
    )

    lines = ["# memframe"]
    lines.extend(_wrap(memframe_line(method_call)))
    lines.append("# pandas")
    lines.extend(_wrap(pandas_line(pandas_call)))
    if error_message:
        lines.append("# failure: " + error_message)

    y = 0.955
    for line in lines:
        fig.text(0.01, y, line, fontsize=9, family="monospace", va="top")
        y -= 0.0205
    top = max(0.55, y - 0.01)

    for ax, (label, df) in zip(axes, sections):
        ax.axis("off")
        ax.set_title(label, fontsize=10, loc="left")
        if df.empty:
            ax.text(0.01, 0.5, "(empty)", fontsize=9, transform=ax.transAxes)
            continue
        table = ax.table(
            cellText=df.values,
            colLabels=df.columns,
            cellLoc="center",
            loc="center",
        )
        table.auto_set_font_size(False)
        table.set_fontsize(8)
        table.scale(1.1, 1.2)

    plt.tight_layout(rect=[0, 0, 1, top])
    pdf.savefig(fig)
    plt.close(fig)
