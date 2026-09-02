import asyncio
import json
import uuid


_PLOT_WRAPPERS = (
    "bar",
    "line",
    "pie",
    "scatter",
    "scatter_3d",
    "bar_polar",
)


def plot_spec_preview(spec: dict) -> dict:
    """Small JSON-safe summary of a figure spec (never the full spec).

    The full spec can hold every data point (millions of tokens), so the model
    and the chat response only ever see this preview; the full spec lives in
    ``session.plots`` for client-side rendering.
    """
    data = spec.get("data") or []
    return {
        "traces": len(data),
        "trace_types": [d.get("type") for d in data],
        "points": sum(len(d.get("x") or []) for d in data),
        "title": (spec.get("layout") or {}).get("title", {}).get("text"),
    }


def _json_safe(value):
    """Recursively convert a value into plain JSON-serializable data.

    Converts numpy arrays/scalars and pandas scalars to native Python types
    so a spec can never leak a non-serializable object (PydanticSerializationError).
    """
    if isinstance(value, (str, int, float, bool)) or value is None:
        return value
    if isinstance(value, dict):
        return {k: _json_safe(v) for k, v in value.items()}
    if isinstance(value, (list, tuple)):
        return [_json_safe(v) for v in value]
    try:
        import numpy as np

        if isinstance(value, np.ndarray):
            return value.tolist()
        if isinstance(value, np.generic):
            return value.item()
    except ImportError:
        pass
    try:
        import pandas as pd

        if isinstance(value, (pd.Timestamp, pd.Timedelta)):
            return value.isoformat()
    except ImportError:
        pass
    try:
        json.dumps(value)
        return value
    except (TypeError, ValueError):
        return str(value)


def tools(session):
    async def plot(
        plot_type: str,
        x: str,
        y: str | None = None,
        z: str | None = None,
        color: str | None = None,
        title: str | None = None,
    ) -> dict:
        """Build a plot from the active table and store it as a figure.

        plot_type: 'bar', 'line', 'scatter', 'scatter_3d' (needs z), 'pie' (x=names, y=values),
        or 'bar_polar' (x=theta, y=r). Returns plot_id + a small spec preview.
        """
        await session.ensure()
        if plot_type not in _PLOT_WRAPPERS:
            return {
                "ok": False,
                "hint": f"Unknown plot_type {plot_type!r}; choose from {sorted(_PLOT_WRAPPERS)}",
            }

        wrapper_attr = f"plot_{plot_type}" if plot_type != "bar_polar" else "plot_bar_polar"
        wrapper = getattr(session.wrappers, wrapper_attr)
        method = getattr(wrapper, f"a{plot_type}")

        kwargs = {"title": title}
        if plot_type == "pie":
            kwargs["names"] = x
            if y is not None:
                kwargs["values"] = y
        elif plot_type == "bar_polar":
            kwargs["theta"] = x
            if y is not None:
                kwargs["r"] = y
        else:
            kwargs["x"] = x
            if y is not None:
                kwargs["y"] = y
            if z is not None:
                kwargs["z"] = z
        if color is not None:
            kwargs["color"] = color

        try:
            fig = await method(**kwargs)
        except Exception as exc:
            return {
                "ok": False,
                "hint": f"{plot_type} plot failed: {type(exc).__name__}: {exc}",
            }

        try:
            spec = json.loads(fig.to_json())
            spec = _json_safe(spec)
        except Exception as exc:
            return {
                "ok": False,
                "hint": f"plot serialization failed: {type(exc).__name__}: {exc}",
            }

        plot_id = uuid.uuid4().hex[:12]
        # ponytail: kaleido removed from core deps — PNG best-effort (install kaleido if needed); spec renders client-side anyway. to_thread + timeout so a hung Chromium can't freeze the loop.
        png = None
        try:
            png = await asyncio.wait_for(
                asyncio.to_thread(fig.to_image, format="png"), timeout=30.0
            )
        except Exception:
            png = None
        session.add_plot(plot_id, title or f"{plot_type} of {x}", spec, png)
        return {
            "ok": True,
            "message": f"Plot {plot_id} created",
            "plot_id": plot_id,
            "title": title or f"{plot_type} of {x}",
            "spec_preview": plot_spec_preview(spec),
        }

    return [plot]