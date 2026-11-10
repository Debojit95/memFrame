"""Integration test for MemFrame.dashboard() — the one-sentence auto-dashboard.

Runs the full planner -> specialist -> dashboard-agent pipeline against a live
LLM. Skipped unless OLLAMA_API_KEY is present and MEMFRAME_RUN_LIVE_TESTS=1,
so normal commit/release gates never depend on external model availability.
"""

import asyncio
import os

import pandas as pd
import pytest

from memframe.main import MemFrame

pytestmark = [
    pytest.mark.integration,
    pytest.mark.skipif(
        not os.getenv("OLLAMA_API_KEY")
        or os.getenv("MEMFRAME_RUN_LIVE_TESTS") != "1",
        reason="live agent run requires OLLAMA_API_KEY and MEMFRAME_RUN_LIVE_TESTS=1",
    ),
]


@pytest.fixture
def mf():
    m = MemFrame(connection_type="local")
    asyncio.run(m.aconnect())
    df = pd.DataFrame(
        {
            "A": list(range(1, 21)),
            "B": [x * 10 for x in range(1, 21)],
            "C": [None if i % 4 == 0 else float(i) for i in range(1, 21)],
            "D": (["x", "y", "z"] * 7)[:20],
        }
    )
    ctx = asyncio.run(m.aupload_df(df, filename="dash_conv"))
    asyncio.run(m.aset_active(ctx._data_id))
    yield m
    asyncio.run(m.aclose())


def test_dashboard_one_sentence(mf):
    try:
        asyncio.run(_run_dashboard_one_sentence(mf))
    except Exception as exc:  # noqa: BLE001 - external model availability varies
        message = str(exc)
        if (
            type(exc).__name__ in ("ModelHTTPError", "ModelAPIError")
            or "status_code: 402" in message
            or "usage credits" in message
        ):
            pytest.skip(f"Live model unavailable: {message}")
        raise


async def _run_dashboard_one_sentence(mf):
    await mf.aenable_agent(
        provider="ollama",
        # ponytail: free-tier Ollama Cloud model (subscription models 402);
        # override with OLLAMA_MODEL if gpt-oss:120b is ever retired
        model=os.getenv("OLLAMA_MODEL", "gpt-oss:120b-cloud"),
        api_key=os.getenv("OLLAMA_API_KEY"),
        base_url=os.getenv("OLLAMA_BASE_URL", "https://ollama.com/v1"),
    )
    html = await mf.adashboard(
        "calculate the value counts of D and show the correlation of A and B",
        show=False,
    )
    assert isinstance(html, str)
    assert "<html" in html.lower()
    assert len(html) > 1000
