# Scatter 3D Plots

Source: `src/wrappers/plots/scatter_3d.py`

`Scatter3DWrapper` is the public 3D scatter plot interface exposed through a
`ContextManager`. It fetches the active backend table into a pandas DataFrame
for the requested columns, then delegates chart construction to
`plotly.express.scatter_3d`.

Use this page as a memFrame-specific entry point. For detailed 3D scatter plot
behavior, marker sizing, symbols, colors, axes, animations, and examples, see
the Plotly documentation:

<https://plotly.com/python/3d-scatter-plots/>

## Public API

| Synchronous | Asynchronous | Purpose |
| --- | --- | --- |
| `scatter3d(...)` | `await ascatter_3d(...)` | Build a Plotly 3D scatter plot from the active dataset context |

`dataset.scatter3d(...)` is callable directly because `dataset.scatter3d`
resolves to the 3D scatter plotting wrapper. The wrapper also exposes
`scatter_3d(...)` for callers that use the internal Plotly-style name.

## Usage Overview

```python
dataset = mf.upload_df(frame)

fig = dataset.scatter3d(x="score", y="salary", z="count_all")
fig.show()
```

```python
dataset = mf.upload_csv("data/sales.csv")

fig = dataset.scatter3d(
    x="revenue",
    y="profit",
    z="orders",
    color="region",
    size="discount",
    title="Revenue, profit, and orders",
)
fig.show()
```

```python
dataset = await mf.aupload_csv("data/sales.csv")

fig = await dataset.ascatter_3d(
    x="revenue",
    y="profit",
    z="orders",
    color="region",
)
fig.show()
```

## Common Parameters

Most parameters are passed through to `plotly.express.scatter_3d`.

| Parameter | Description |
| --- | --- |
| `x` | Column name or values used for the x-axis. |
| `y` | Column name or values used for the y-axis. |
| `z` | Column name or values used for the z-axis. |
| `color` | Column name or values used to map marker colors. |
| `symbol` | Column name or values used to map marker symbols. |
| `size` | Column name or values used to scale marker sizes. |
| `text` | Column name or values used as text labels. |
| `hover_name`, `hover_data`, `custom_data` | Columns or values included in hover labels or callbacks. |
| `error_x`, `error_x_minus`, `error_y`, `error_y_minus`, `error_z`, `error_z_minus` | Columns or values used for error bars. |
| `animation_frame`, `animation_group` | Columns or values used for animated charts. |
| `category_orders`, `labels` | Category ordering and display labels. |
| `size_max` | Maximum marker size. |
| `color_discrete_sequence`, `color_discrete_map`, `color_continuous_scale` | Color options passed to Plotly. |
| `range_color`, `color_continuous_midpoint` | Continuous color range options. |
| `symbol_sequence`, `symbol_map` | Symbol mapping options passed to Plotly. |
| `opacity` | Marker opacity. |
| `log_x`, `log_y`, `log_z` | Use logarithmic axes. |
| `range_x`, `range_y`, `range_z` | Axis ranges passed to Plotly. |
| `title`, `subtitle`, `template`, `width`, `height` | Display and layout options passed to Plotly. |
| `**kwargs` | Additional `plotly.express.scatter_3d` keyword arguments. |

Do not pass `data_frame`; memFrame derives it from the active dataset context.

## Return Value

`scatter3d`, `scatter_3d`, and `ascatter_3d` return the Plotly figure object
created by `plotly.express.scatter_3d`.

```python
fig = dataset.scatter3d(x="score", y="salary", z="count_all", color="category")
fig.update_layout(scene_zaxis_title="Count")
fig.show()
```

Because the return value is a Plotly figure, use Plotly figure methods such as
`update_layout`, `update_traces`, and `show` for final presentation changes.
