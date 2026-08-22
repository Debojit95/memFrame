"""Integration test for MemFrame.dashboard() — the one-sentence auto-dashboard.

Runs the full planner -> specialist -> dashboard-agent pipeline against a live
LLM, so it is gated on an API key and skipped in CI/offline runs.
"""

import asyncio
import os

import pandas as pd
import pytest

from memframe.main import MemFrame

pytestmark = [
    pytest.mark.integration,
    pytest.mark.skipif(
        not os.getenv("OPENAI_API_KEY"),
        reason="live agent run requires OPENAI_API_KEY",
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
    asyncio.run(m.aset_active(ctx.data_id))
    yield m
    asyncio.run(m.aclose())


async def test_dashboard_one_sentence(mf):
    await mf.aenable_agent(
        api_key=os.getenv("OPENAI_API_KEY"),
        provider=os.getenv("MEMFRAME_TEST_PROVIDER", "openai"),
        model=os.getenv("MEMFRAME_TEST_MODEL", "gpt-5.5"),
    )
    html = await mf.adashboard(
        "calculate the value counts of D and show the correlation of A and B",
        show=False,
    )
    assert isinstance(html, str)
    assert "<html" in html.lower()
    assert len(html) > 1000
