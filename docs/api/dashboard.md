# Dashboard Builder

Source: `src/memframe/dashboard/`

Turn a batch of query results into a self-contained dashboard. You collect the
results memFrame already produced (DataFrames, Plotly figures, dicts/metrics,
scalars), an optional Pydantic-AI agent designs the layout, and the builder
renders a **zero-dependency HTML page** (Plotly figures inline, tables as HTML,
metrics as cards) — no Dash or web server required.

## Public API

`DashboardManager` collects results, designs a layout, and renders HTML:

| Method | Purpose |
| --- | --- |
| `add(title, result, description=None)` | append one result |
| `collect([(title, result), …])` | append many results |
| `summarize() -> list[str]` | compact per-result summaries for the agent |
| `await design(settings) -> DashboardDesign` | run the designer agent (needs the `ai` extra) |
| `render(design) -> str` | build the HTML dashboard |
| `save(design, filename="dashboard.html") -> str` | write the HTML to disk |

`settings` is a `memframe_ai.AISettings` (provider/model/api key). The agent is
imported lazily, so the core library stays free of the `pydantic-ai` extra
until you actually call `design()`.

## Example

```python
import pandas as pd
import plotly.express as px
from memframe.dashboard import DashboardManager
from memframe_ai import AISettings

df = pd.DataFrame({"region": ["A", "B", "C"], "sales": [10, 20, 30]})

dm = DashboardManager()
dm.add("Sales by region", px.bar(df, x="region", y="sales"))
dm.add("Raw table", df)
dm.add("Conversion", {"rate": 0.42})

settings = AISettings(provider="openai", model="gpt-4o", api_key="sk-...")
design = await dm.design(settings)
html = dm.render(design)          # or: dm.save(design, "dashboard.html")
```

## One-shot from a sentence

`MemFrame.adashboard(sentence)` collapses the whole flow into one call: it
reuses the existing planner + specialist pipeline (`.achat()`) to decompose a
natural-language sentence into sub-queries, executes them against the **active
dataset**, then feeds the results through the dashboard designer and renders a
self-contained HTML page. A sync `MemFrame.dashboard(sentence)` wrapper exists
for non-async contexts.

```python
await mf.aset_active("my_dataset")
await mf.aenable_agent(api_key="sk-...")

html = await mf.adashboard(
    "fillna C with mean then calculate the value counts of D and add B with the cleaned C",
)
# html is a complete dashboard page; in a notebook it also renders inline,
# otherwise it is written to dashboard.html and opened in the browser.
```

- **Async** (`adashboard`) and requires the agent enabled (`aenable_agent`). Raises a clear
  error otherwise, or if there is no active dataset.
- Set `show=False` to skip the env-agnostic display and just get the HTML back.
- The final display uses `memframe.utils.plot_renderer.smart_show`, which is
  env-agnostic: notebook/Colab/VSCode render inline, terminal opens the browser.

### Token safety

Raw result **values are never sent to an LLM**. The planner and specialists
receive only schema, aggregates, and a small capped table preview
(`build_domain_context`). The dashboard designer receives only the compact
summaries produced by `DashboardManager.summarize()` — shape, column names, a
2-row sample, and a capped Plotly spec preview. The full DataFrames and figures
stay local and are consumed solely by the renderer.

## Layout rules (enforced by the agent + renderer)

- **Maximum two widgets per row** — the renderer greedily packs widgets by
  `col_span` (1–12) and never places more than two in a row.
- **Auto-charting**: for a raw `DataFrame` with no figure, the agent picks a
  chart type (`bar`/`line`/`scatter`/`pie`/`histogram`/`box`) and the `x`/`y`
  columns; the renderer builds the figure with `plotly.express`. A pre-built
  `Figure` is kept as-is (`chart_type="keep_existing"`) with only its
  title/size/labels adjusted.
- **Metrics**: a `dict` or scalar becomes a KPI card with optional
  `prefix`/`suffix`/`decimal_places`.
- **Themes**: `global_theme="light"` (default) or `"dark"`, applied via a
  small inline stylesheet.

## Design model

`design()` returns a `DashboardDesign` (see `src/memframe/dashboard/models.py`):
a `dashboard_title`, `global_theme`, and a list of `WidgetDesign` items, each
referencing its source result by `result_index` and carrying a `FigureDesign`
or `MetricDesign`. The same model is the contract between the agent and the
renderer, so you can also build a `DashboardDesign` by hand and call
`render()` directly without the agent.
