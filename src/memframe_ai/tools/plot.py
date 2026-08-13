import json
import uuid

from memframe.utils.plot_renderer import smart_show


_PLOT_WRAPPERS = (
    "bar",
    "line",
    "pie",
    "scatter",
    "scatter_3d",
    "bar_polar",
)


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
        or 'bar_polar' (x=theta, y=r). Returns plot_id + figure spec.
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
            smart_show(fig)
        except Exception:
            pass

        try:
            spec = json.loads(fig.to_json())
            spec = _json_safe(spec)
        except Exception as exc:
            return {
                "ok": False,
                "hint": f"plot serialization failed: {type(exc).__name__}: {exc}",
            }

        plot_id = uuid.uuid4().hex[:12]
        # ponytail: PNG needs Chrome/kaleido; best-effort, spec renders client-side anyway
        png = None
        try:
            png = fig.to_image(format="png")
        except Exception:
            png = None
        session.add_plot(plot_id, title or f"{plot_type} of {x}", spec, png)
        return {
            "ok": True,
            "message": f"Plot {plot_id} created",
            "plot_id": plot_id,
            "title": title or f"{plot_type} of {x}",
            "spec": spec,
        }

    return [plot]