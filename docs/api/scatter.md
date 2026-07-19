# Scatter Plots

Source: `src/wrappers/plots/scatter.py`

`ScatterWrapper` is the public scatter plot interface exposed through a
`ContextManager`. It fetches the active backend table into a pandas DataFrame
for the requested columns, then delegates chart construction to
`plotly.express.scatter`.

Use this page as a memFrame-specific entry point. For detailed scatter plot
behavior, marker sizing, symbols, colors, facets, animations, and examples, see
the Plotly documentation:

<https://plotly.com/python/line-and-scatter/>

## Public API

| Synchronous | Asynchronous | Purpose |
| --- | --- | --- |
| `scatter(...)` | `await ascatter(...)` | Build a Plotly scatter plot from the active dataset context |

`dataset.scatter(...)` is also callable directly because `dataset.scatter`
resolves to the scatter plotting wrapper.

## Usage Overview

```python
dataset = mf.upload_df(frame)

fig = dataset.scatter(x="score", y="salary")
fig.show()
```

```python
dataset = mf.upload_csv("data/sales.csv")

fig = dataset.scatter(
    x="revenue",
    y="profit",
    color="region",
    size="orders",
    title="Revenue vs profit",
)
fig.show()
```

```python
dataset = await mf.aupload_csv("data/sales.csv")

fig = await dataset.ascatter(
    x="revenue",
    y="profit",
    color="region",
)
fig.show()
```

## Common Parameters

Most parameters are passed through to `plotly.express.scatter`.

| Parameter | Description |
| --- | --- |
| `x` | Column name or values used for the x-axis. |
| `y` | Column name or values used for the y-axis. |
| `color` | Column name or values used to map marker colors. |
| `symbol` | Column name or values used to map marker symbols. |
| `size` | Column name or values used to scale marker sizes. |
| `hover_name`, `hover_data`, `custom_data` | Columns or values included in hover labels or callbacks. |
| `text` | Column name or values used as text labels. |
| `facet_row`, `facet_col` | Column names used to split the chart into facets. |
| `error_x`, `error_x_minus`, `error_y`, `error_y_minus` | Columns or values used for error bars. |
| `animation_frame`, `animation_group` | Columns or values used for animated charts. |
| `**kwargs` | Additional `plotly.express.scatter` keyword arguments. |

Do not pass `data_frame`; memFrame derives it from the active dataset context.

## Return Value

`scatter` and `ascatter` return the Plotly figure object created by
`plotly.express.scatter`.

```python
fig = dataset.scatter(x="score", y="salary", color="category")
fig.update_layout(xaxis_title="Score", yaxis_title="Salary")
fig.show()
```

Because the return value is a Plotly figure, use Plotly figure methods such as
`update_layout`, `update_traces`, and `show` for final presentation changes.
