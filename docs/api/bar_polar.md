# Bar Polar Plots

Source: `src/wrappers/plots/bar_polar.py`

`BarPolarWrapper` is the public bar polar plotting interface exposed through a
`ContextManager`. It fetches the active backend table into a pandas DataFrame
for the requested columns, then delegates chart construction to
`plotly.express.bar_polar`.

Use this page as a memFrame-specific entry point. For detailed bar polar and
wind rose behavior, styling options, radial and angular axes, color scales, and
examples, see the Plotly documentation:

<https://plotly.com/python/wind-rose-charts/>

## Public API

| Synchronous | Asynchronous | Purpose |
| --- | --- | --- |
| `bar_polar(...)` | `await abar_polar(...)` | Build a Plotly bar polar chart from the active dataset context |

`dataset.bar_polar(...)` is also callable directly because
`dataset.bar_polar` resolves to the bar polar plotting wrapper.

## Usage Overview

```python
dataset = mf.upload_df(frame)

fig = dataset.bar_polar(theta="category")
fig.show()
```

```python
dataset = mf.upload_csv("data/sales.csv")

fig = dataset.bar_polar(
    theta="region",
    r="revenue",
    color="segment",
    barmode="group",
    title="Revenue by region",
)
fig.show()
```

```python
dataset = await mf.aupload_csv("data/sales.csv")

fig = await dataset.abar_polar(
    theta="region",
    r="revenue",
    color="segment",
)
fig.show()
```

## Common Parameters

Most parameters are passed through to `plotly.express.bar_polar`.

| Parameter | Description |
| --- | --- |
| `theta` | Column name or values used for angular positions. |
| `r` | Column name or values used for radial bar lengths. |
| `color` | Column name or values used to split bars by color. |
| `pattern_shape` | Column name or values used to split bars by pattern. |
| `hover_name`, `hover_data`, `custom_data` | Columns or values included in hover labels or callbacks. |
| `base` | Column name or values used as the radial base for bars. |
| `animation_frame`, `animation_group` | Columns or values used for animated charts. |
| `barnorm` | Plotly normalization mode for bar values. |
| `barmode` | Plotly bar mode, such as `relative`, `group`, `stack`, or `overlay`. Defaults to `relative`. |
| `direction` | Angular direction. Defaults to `clockwise`. |
| `start_angle` | Angle where the angular axis starts. Defaults to `90`. |
| `range_r`, `range_theta` | Radial or angular ranges passed to Plotly. |
| `log_r` | Use a logarithmic radial axis. |
| `title`, `subtitle`, `template`, `width`, `height` | Display and layout options passed to Plotly. |
| `**kwargs` | Additional `plotly.express.bar_polar` keyword arguments. |

Do not pass `data_frame`; memFrame derives it from the active dataset context.

## Return Value

`bar_polar` and `abar_polar` return the Plotly figure object created by
`plotly.express.bar_polar`.

```python
fig = dataset.bar_polar(theta="category", r="score")
fig.update_layout(polar_radialaxis_title="Score")
fig.show()
```

Because the return value is a Plotly figure, use Plotly figure methods such as
`update_layout`, `update_traces`, and `show` for final presentation changes.
