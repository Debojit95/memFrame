# Dashboard Builder

Source: `src/memframe/dashboard/`

Turn a batch of query results into a self-contained dashboard. You collect the
results memFrame already produced (DataFrames, Plotly figures, dicts/metrics,
scalars), an optional Pydantic-AI agent designs the layout, and the builder
renders a **single full-screen Plotly figure** (the "canvas") composed of
subplots/domains — no Dash or web server required.

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
# In a notebook `adashboard` returns a native Plotly figure (renders inline via
# its mimebundle); in a terminal it returns the self-contained HTML string.
```

- **Async** (`adashboard`) and requires the agent enabled (`aenable_agent`). Raises a clear
  error otherwise, or if there is no active dataset.
- Set `show=False` to skip the env-agnostic display and just get the HTML back.
- The return value is env-aware: in a notebook (Jupyter/Colab/VSCode) it returns
  the native Plotly `Figure` so the cell renders it inline; in a terminal it
  returns the self-contained HTML string. The display step uses
  `memframe.utils.plot_renderer.smart_show` (notebook renders inline, terminal
  writes `dashboard.html` and opens the browser).

### Token safety

Raw result **values are never sent to an LLM**. The planner and specialists
receive only schema, aggregates, and a small capped table preview
(`build_domain_context`). The dashboard designer receives only the compact
summaries produced by `DashboardManager.summarize()` — shape, column names, a
2-row sample, and a capped Plotly spec preview. The full DataFrames and figures
stay local and are consumed solely by the renderer.

## Layout rules (enforced by the agent + renderer)

- **Type → widget (hard contract)**: a `DataFrame` always renders as a **table**,
  a pre-built Plotly `Figure` always renders as a **plot** (`keep_existing`), and
  a `dict`/`number` always renders as a **metric**. A DataFrame can never be
  charted — both the agent prompt and the renderer coerce `kind` from the actual
  result type, so a mislabel cannot produce a wrong widget.
- **Single-figure canvas**: the whole dashboard is ONE responsive Plotly figure.
  `DataFrame` → `go.Table` trace, `Figure` → its traces placed in a subplot cell,
  `dict`/number → `go.Indicator` (single value) or `go.Table` (multi-key dict).
  The figure fills the viewport (`100vw`/`100vh`, `config={"responsive": True}`).
- **Maximum two widgets per row** — the renderer greedily packs widgets into rows
  of one (full width) or two (paired) cells; each cell becomes a subplot.
- **Plots**: for a raw `DataFrame` the renderer builds a chart with
  `plotly.express` using the agent's `chart_type`/`x`/`y`; a pre-built `Figure`
  is kept as-is with only its title/size/labels adjusted.
- **Metrics**: a `dict`/`scalar` becomes a KPI (`go.Indicator`) with optional
  `prefix`/`suffix`/`decimal_places`; a multi-key dict becomes a small table.
- **Themes**: `global_theme="light"` (default) or `"dark"`, applied via the
  Plotly `plotly_white`/`plotly_dark` template.

## Design model

`design()` returns a `DashboardDesign` (see `src/memframe/dashboard/models.py`):
a `dashboard_title`, `global_theme`, and a list of `WidgetDesign` items, each
referencing its source result by `result_index` and carrying a `FigureDesign`
or `MetricDesign`. The same model is the contract between the agent and the
renderer, so you can also build a `DashboardDesign` by hand and call
`render()` directly without the agent.
