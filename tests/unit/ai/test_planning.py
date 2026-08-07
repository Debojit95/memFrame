from memframe_ai.agents import PlannerAgent
from memframe_ai.config import AISettings


def test_planner_builds_structured_output_agent():
    p = PlannerAgent(AISettings(api_key="k"))
    agent = p._build()
    assert agent.name == "planner"
    assert agent.output_type is not None
    assert "SubQuery" in agent.output_type.__name__


def test_to_linkedlist_links_dependent_queries():
    from memframe_ai.agents.planning import SubQueryNode, PlannerAgent

    nodes = [
        SubQueryNode(query="fill nulls of C", agent="clean"),
        SubQueryNode(query="add B to cleaned C", agent="arithmetic", prev_depends=True),
    ]
    head = PlannerAgent._to_linkedlist(nodes)
    assert head.query == "fill nulls of C"
    assert head.prev_depends is False
    assert head.agent == "clean"
    assert head.next is not None
    assert head.next.query == "add B to cleaned C"
    assert head.next.prev_depends is True
    assert head.next.agent == "arithmetic"
    assert head.next.next is None


def test_to_linked_list_single_node():
    from memframe_ai.agents.planning import SubQueryNode, PlannerAgent

    head = PlannerAgent._to_linkedlist([SubQueryNode(query="sub B from C", agent="arithmetic")])
    assert head.query == "sub B from C"
    assert head.agent == "arithmetic"
    assert head.next is None