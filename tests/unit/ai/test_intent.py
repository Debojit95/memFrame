from memframe_ai.agents import IntentClassifier, IntentResult
from memframe_ai.agents.analytics import _render_intent
from memframe_ai.config import AISettings


def test_intent_result_defaults():
    r = IntentResult(primary_task="inspect", user_goal="show first 2 rows")
    assert r.targets == []
    assert r.focus_columns == []
    assert r.requires_plot is False
    assert r.plot_type is None


def test_classifier_builds_without_model_call():
    c = IntentClassifier(AISettings(api_key="k"))
    assert c._agent.name == "intent_classifier"
    assert c._agent.output_type is IntentResult


def test_render_intent_includes_targets_and_goal():
    r = IntentResult(
        primary_task="plot",
        targets=["run_plot_bar"],
        focus_columns=["D"],
        plot_type="bar",
        user_goal="bar chart of column D",
    )
    out = _render_intent(r)
    assert "Primary task: plot" in out
    assert "run_plot_bar" in out
    assert "D" in out
    assert "bar" in out


def test_render_intent_omits_empty_fields():
    r = IntentResult(primary_task="inspect", user_goal="head")
    out = _render_intent(r)
    assert "Target specialists:" not in out
    assert "Focus columns:" not in out
