# Bar Plots

Source: `src/wrappers/plots/bar.py`

`BarWrapper` is the public bar plotting interface exposed through a
`ContextManager`. It fetches the active backend table into a pandas DataFrame
for the requested columns, then delegates chart construction to
`plotly.express.bar`.

Use this page as a memFrame-specific entry point. For detailed bar chart
behavior, styling options, grouping modes, faceting, text labels, and examples,
see the Plotly documentation:

<https://plotly.com/python/bar-charts/>

## Public API

| Synchronous | Asynchronous | Purpose |
| --- | --- | --- |
| `bar(...)` | `await abar(...)` | Build a Plotly bar chart from the active dataset context |

`dataset.bar(...)` is also callable directly because `dataset.bar` resolves to
the bar plotting wrapper.

## Usage Overview

```python
dataset = mf.upload_df(frame)

fig = dataset.bar(x="category")
fig.show()
```

```python
dataset = mf.upload_csv("data/sales.csv")

fig = dataset.bar(
    x="region",
    y="revenue",
    color="segment",
    barmode="group",
    title="Revenue by region",
)
fig.show()
```

```python
dataset = await mf.aupload_csv("data/sales.csv")

fig = await dataset.abar(
    x="region",
    y="revenue",
    color="segment",
)
fig.show()
```

## Common Parameters

Most parameters are passed through to `plotly.express.bar`.

| Parameter | Description |
| --- | --- |
| `x` | Column name or values used for the x-axis. |
| `y` | Column name or values used for the y-axis. If omitted, Plotly creates a count-style bar chart. |
| `color` | Column name or values used to split bars by color. |
| `pattern_shape` | Column name or values used to split bars by pattern. |
| `facet_row`, `facet_col` | Column names used to split the chart into facets. |
| `hover_data`, `custom_data`, `text` | Columns or values included in hover labels, callbacks, or bar text. |
| `orientation` | Bar orientation. Use Plotly's accepted values. |
| `barmode` | Plotly bar mode, such as `relative`, `group`, `stack`, or `overlay`. Defaults to `relative`. |
| `log_x`, `log_y` | Use logarithmic axes. |
| `range_x`, `range_y` | Axis ranges passed to Plotly. |
| `text_auto` | Enables automatic bar labels when supported by Plotly. |
| `title`, `subtitle`, `template`, `width`, `height` | Display and layout options passed to Plotly. |
| `**kwargs` | Additional `plotly.express.bar` keyword arguments. |

Do not pass `data_frame`; memFrame derives it from the active dataset context.

## Return Value

`bar` and `abar` return the Plotly figure object created by
`plotly.express.bar`.

```python
fig = dataset.bar(x="category", y="score")
fig.update_layout(xaxis_title="Category", yaxis_title="Score")
fig.show()
```

Because the return value is a Plotly figure, use Plotly figure methods such as
`update_layout`, `update_traces`, and `show` for final presentation changes.
