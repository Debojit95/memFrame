import inspect
import logging
from typing import Any, Optional
from functools import wraps

from memframe.core.ingestion.datatype_detector import Backend
from memframe.db_manager.adapters.base import DatabaseAdapter
from memframe.db_manager.adapters.postgresql import PostgresAdapter
from memframe.db_manager.adapters.duckdb import DuckDBAdapter
from memframe.db_manager.adapters.clickhouse import ClickHouseAdapter
from memframe.exceptions import (
    BackendNotSupported,
    ConnectionNotReady,
    DataNotFound,
)
from memframe.utils.async_sync import async_to_sync
from memframe.core.analytix._response import unwrap_response


logger = logging.getLogger("memFrame")


def _public_result(method):
    """Expose raw operation values while leaving AI wrappers unchanged."""
    @wraps(method)
    def wrapped(*args, **kwargs):
        value = method(*args, **kwargs)
        if inspect.isawaitable(value):
            async def resolve():
                return unwrap_response(await value)

            return resolve()
        return unwrap_response(value)

    return wrapped


class ContextManager:
    def __init__(self, memframe_instance, data_id: Optional[str] = None):
        self.memframe = memframe_instance
        self._data_id = data_id
        self._adapter: Optional[DatabaseAdapter] = None
        self._wrappers = None

    def _lazy_init_wrappers(self):
        if self._wrappers is not None:
            return
        from memframe.wrappers.analytix.selection import SelectionWrapper
        from memframe.wrappers.analytix.inspection import TableOpsWrapper
        from memframe.wrappers.analytix.cleaning import CleaningWrapper
        from memframe.wrappers.analytix.stats import StatsWrapper
        from memframe.wrappers.analytix.arithmetic import ArithmeticWrapper
        from memframe.wrappers.plots.bar import BarWrapper
        from memframe.wrappers.plots.bar_polar import BarPolarWrapper
        from memframe.wrappers.plots.pie import PieWrapper
        from memframe.wrappers.plots.line import LineWrapper
        from memframe.wrappers.plots.scatter import ScatterWrapper
        from memframe.wrappers.plots.scatter3d import Scatter3DWrapper

        self._wrappers = [
            SelectionWrapper(self),
            TableOpsWrapper(self),
            CleaningWrapper(self),
            StatsWrapper(self),
            ArithmeticWrapper(self),
            BarWrapper(self),
            BarPolarWrapper(self),
            PieWrapper(self),
            LineWrapper(self),
            ScatterWrapper(self),
            Scatter3DWrapper(self),
        ]

    def __getattr__(self, name: str) -> Any:
        self._lazy_init_wrappers()
        for w in self._wrappers:
            if hasattr(w, name):
                attribute = getattr(w, name)
                return _public_result(attribute) if callable(attribute) else attribute
        raise AttributeError(f"{self.__class__.__name__!r} object has no attribute {name!r}")

    def __dir__(self):
        self._lazy_init_wrappers()
        names = set(super().__dir__())
        for w in self._wrappers:
            names.update(w.__dir__())
        return sorted(names)

    async def _ensure_adapter(self):
        if self._adapter is not None:
            return
        backend = self.memframe._backend
        pool = getattr(self.memframe, "_pool", None)
        if backend is None or pool is None:
            raise ConnectionNotReady("Not connected. Call await connect() first.")
        if backend.backend == Backend.DUCKDB:
            self._adapter = DuckDBAdapter(pool)
        elif backend.backend == Backend.POSTGRES:
            self._adapter = PostgresAdapter(pool)
        elif backend.backend == Backend.CLICKHOUSE:
            self._adapter = ClickHouseAdapter(pool)
        else:
            raise BackendNotSupported("Unsupported backend")

    async def aclose(self):
        self._adapter = None

    # ── AI agent entrypoints (delegate to memframe_ai) ──────────

    async def achat(self, prompt: str, session_id: Optional[str] = None) -> dict:
        from memframe_ai.entrypoints import achat as _achat

        return await _achat(self, prompt, session_id)

    @async_to_sync
    async def chat(self, prompt: str, session_id: Optional[str] = None) -> dict:
        return await self.achat(prompt, session_id)

    async def aenable_agent(
        self,
        api_key: str,
        provider: Optional[str] = None,
        model: Optional[str] = None,
        **overrides,
    ) -> Any:
        from memframe_ai.config import AISettings

        # Store settings; the agent fleet is built lazily on first achat.
        kwargs = {"api_key": api_key}
        if provider is not None:
            kwargs["provider"] = provider
        if model is not None:
            kwargs["model"] = model
        kwargs.update(overrides)
        settings = AISettings(**kwargs)
        self.memframe._ai_settings = settings
        from memframe_ai.instrument import configure_logfire

        configure_logfire(settings)
        return settings

    @async_to_sync
    async def enable_agent(self, api_key: str, provider: Optional[str] = None, model: Optional[str] = None, **overrides):
        return await self.aenable_agent(api_key, provider, model, **overrides)

    async def adashboard(
        self,
        sentence: str,
        show: bool = True,
        filename: str = "dashboard.html",
    ) -> str:
        # ponytail: reuses the planner->specialist pipeline (.achat) for the
        # heavy lifting; only compact summaries (DashboardManager.summarize)
        # ever reach the dashboard LLM, so raw data values are never tokenized.
        from memframe_ai.entrypoints import _get_settings
        from memframe.dashboard import DashboardManager
        import json
        import plotly.io as pio

        settings = _get_settings(self.memframe)  # raises if agent not enabled
        if not (self._data_id or getattr(self, "_active_id", None)):
            raise RuntimeError(
                "No active dataset. Call aset_active(data_id) or run on a dataset context."
            )
        from memframe_ai.instrument import span as _lf_span, flush_logfire

        with _lf_span("adashboard", sentence=sentence[:200], show=show):
            resp = await self.achat(sentence)

            # ponytail: a guardrail-blocked query must not produce an empty dashboard;
            # return a graceful, themed page explaining why execution stopped instead.
            if resp.get("guardrail_blocked"):
                reason = (
                    resp.get("guardrail_reason")
                    or resp.get("answer")
                    or "Query blocked by the guardrail."
                )
                from memframe.dashboard.render import render_guardrail_blocked

                html = render_guardrail_blocked(reason)
                if show:
                    DashboardManager().show(html=html, filename=filename)
                return html

            # ponytail: plots already carry their figure; results may ALSO contain
            # figures (plot sub-queries record both), so skip figures there to avoid
            # duplicates. Only DataFrames/scalars from results become new widgets.
            dm = DashboardManager()
            seen: set = set()
            for p in resp.get("plots", []):
                fig = pio.from_json(json.dumps(p["spec"]))
                dm.add(p.get("title") or "Plot", fig)
            for r in resp.get("results", []) or []:
                if hasattr(r, "to_plotly_json") and hasattr(r, "to_html"):
                    continue  # figure already captured via resp["plots"]
                if hasattr(r, "shape") and hasattr(r, "columns"):
                    if id(r) in seen:
                        continue
                    seen.add(id(r))
                    dm.add(f"Result {r.shape[0]}x{r.shape[1]}", r)
                else:
                    dm.add("Result", r)
            # ponytail: scalar/dict/list sub-query results are surfaced in resp["values"]
            # (DataFrames/plots are handled above); harvest them so they render as metrics.
            for label, val in resp.get("values", []) or []:
                dm.add(label, val)

            design = await dm.design(settings)
            html = dm.render(design)
            if show:
                dm.show(html=html, filename=filename)
            flush_logfire()
            return html

    @async_to_sync
    async def dashboard(
        self,
        sentence: str,
        show: bool = True,
        filename: str = "dashboard.html",
    ) -> str:
        return await self.adashboard(sentence, show=show, filename=filename)

    async def _get_active_context(self):
        data_id = self._data_id or self.memframe._active_id
        if not data_id:
            raise DataNotFound("No active dataset and no explicit data_id provided.")
        backend = self.memframe._backend
        rows = await backend.fetch(
            f"SELECT table_name, schema FROM {backend.csv_registry_table} WHERE data_id = {backend.placeholder(1)}",
            data_id,
        )
        if not rows:
            raise DataNotFound(f"No registry entry for {data_id}")
        table_name = rows[0][0]
        schema = rows[0][1] or backend.upload_schema
        return table_name, schema
