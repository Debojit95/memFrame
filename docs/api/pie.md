# Pie Charts

Source: `src/wrappers/plots/pie.py`

`PieWrapper` is the public pie chart plotting interface exposed through a
`ContextManager`. It fetches the active backend table into a pandas DataFrame
for the requested columns, then delegates chart construction to
`plotly.express.pie`.

Use this page as a memFrame-specific entry point. For detailed pie chart
behavior, donut charts, labels, colors, facets, and examples, see the Plotly
documentation:

<https://plotly.com/python/pie-charts/>

## Public API

| Synchronous | Asynchronous | Purpose |
| --- | --- | --- |
| `pie(...)` | `await apie(...)` | Build a Plotly pie chart from the active dataset context |

`dataset.pie(...)` is also callable directly because `dataset.pie` resolves to
the pie plotting wrapper.

## Usage Overview

```python
dataset = mf.upload_df(frame)

fig = dataset.pie(names="category")
fig.show()
```

```python
dataset = mf.upload_csv("data/sales.csv")

fig = dataset.pie(
    names="region",
    values="revenue",
    color="region",
    title="Revenue share by region",
)
fig.show()
```

```python
dataset = await mf.aupload_csv("data/sales.csv")

fig = await dataset.apie(
    names="region",
    values="revenue",
    hole=0.4,
)
fig.show()
```

## Common Parameters

Most parameters are passed through to `plotly.express.pie`.

| Parameter | Description |
| --- | --- |
| `names` | Column name or values used for slice labels. |
| `values` | Column name or values used for slice sizes. If omitted, Plotly counts rows by `names`. |
| `color` | Column name or values used to map slice colors. |
| `facet_row`, `facet_col` | Column names used to split the chart into facets. |
| `hover_name`, `hover_data`, `custom_data` | Columns or values included in hover labels or callbacks. |
| `category_orders` | Ordering rules for categorical values. |
| `labels` | Display labels for columns. |
| `color_discrete_sequence`, `color_discrete_map` | Discrete color options passed to Plotly. |
| `opacity` | Trace opacity. |
| `hole` | Creates a donut chart when set above `0`. |
| `title`, `subtitle`, `template`, `width`, `height` | Display and layout options passed to Plotly. |
| `**kwargs` | Additional `plotly.express.pie` keyword arguments. |

Do not pass `data_frame`; memFrame derives it from the active dataset context.

## Return Value

`pie` and `apie` return the Plotly figure object created by
`plotly.express.pie`.

```python
fig = dataset.pie(names="category", values="score")
fig.update_traces(textposition="inside", textinfo="percent+label")
fig.show()
```

Because the return value is a Plotly figure, use Plotly figure methods such as
`update_layout`, `update_traces`, and `show` for final presentation changes.
