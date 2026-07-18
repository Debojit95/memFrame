# Line Charts

Source: `src/wrappers/plots/line.py`

`LineWrapper` is the public line chart plotting interface exposed through a
`ContextManager`. It fetches the active backend table into a pandas DataFrame
for the requested columns, then delegates chart construction to
`plotly.express.line`.

Use this page as a memFrame-specific entry point. For detailed line chart
behavior, markers, line styles, facets, animations, and examples, see the
Plotly documentation:

<https://plotly.com/python/line-charts/>

## Public API

| Synchronous | Asynchronous | Purpose |
| --- | --- | --- |
| `line(...)` | `await aline(...)` | Build a Plotly line chart from the active dataset context |

`dataset.line(...)` is also callable directly because `dataset.line` resolves
to the line plotting wrapper.

## Usage Overview

```python
dataset = mf.upload_df(frame)

fig = dataset.line(x="date", y="revenue")
fig.show()
```

```python
dataset = mf.upload_csv("data/sales.csv")

fig = dataset.line(
    x="month",
    y="revenue",
    color="region",
    line_dash="segment",
    title="Revenue over time",
)
fig.show()
```

```python
dataset = await mf.aupload_csv("data/sales.csv")

fig = await dataset.aline(
    x="month",
    y="revenue",
    color="region",
)
fig.show()
```

## Common Parameters

Most parameters are passed through to `plotly.express.line`.

| Parameter | Description |
| --- | --- |
| `x` | Column name or values used for the x-axis. |
| `y` | Column name or values used for the y-axis. |
| `line_group` | Column name or values used to group connected line segments. |
| `color` | Column name or values used to split lines by color. |
| `line_dash` | Column name or values used to split lines by dash style. |
| `symbol` | Column name or values used to map marker symbols. |
| `hover_name`, `hover_data`, `custom_data` | Columns or values included in hover labels or callbacks. |
| `text` | Column name or values used as text labels. |
| `facet_row`, `facet_col` | Column names used to split the chart into facets. |
| `error_x`, `error_x_minus`, `error_y`, `error_y_minus` | Columns or values used for error bars. |
| `animation_frame`, `animation_group` | Columns or values used for animated charts. |
| `**kwargs` | Additional `plotly.express.line` keyword arguments. |

Do not pass `data_frame`; memFrame derives it from the active dataset context.

## Return Value

`line` and `aline` return the Plotly figure object created by
`plotly.express.line`.

```python
fig = dataset.line(x="count_all", y="score", color="category")
fig.update_layout(xaxis_title="Count", yaxis_title="Score")
fig.show()
```

Because the return value is a Plotly figure, use Plotly figure methods such as
`update_layout`, `update_traces`, and `show` for final presentation changes.
