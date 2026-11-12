"""Tool permissions: which agent may call which tool, for which role. Enforced in code and audited."""
import uuid

import pytest
from sqlalchemy import text

from app.agents import tools as T
from app.agents.tools import PermissionDenied, ToolContext, call_tool
from app.llm import MockLLM, NullLLM


def ctx(role="engineer", llm=None):
    return ToolContext(run_id=str(uuid.uuid4()), actor="tester", role=role, llm=llm or NullLLM())


def denied_audits(app_engine, run_id):
    with app_engine.connect() as c:
        return c.execute(text("SELECT detail->>'reason' FROM app.audit_events WHERE resource_id=:r AND action='agent.tool.denied'"),
                         {"r": run_id}).scalars().all()


def test_registry_shape_is_narrow():
    assert T.TOOLS["apply_change"].agents == {T.EXECUTOR}                       # only the post-approval executor
    assert T.TOOLS["create_approval"].agents == {T.ETL_RECO}
    assert T.TOOLS["sql_execute"].agents == {T.SQL_PLANNER} == T.TOOLS["sql_validate"].agents
    assert T.METADATA not in T.TOOLS["llm_complete"].agents                     # metadata discovery is deterministic
    assert all(s.agents and s.roles for s in T.TOOLS.values())
    for name in ("dq_latest_run", "dq_results", "dq_alerts", "run_details", "create_approval", "apply_change"):
        assert T.TOOLS[name].roles == T.PRIVILEGED                              # analysts never reach pipeline internals


@pytest.mark.parametrize("agent,tool,kwargs", [
    (T.METADATA, "sql_execute", {"sql": "SELECT 1"}), (T.METADATA, "llm_complete", {"system": "s", "user": "u"}),
    (T.METADATA, "apply_change", {"approval_id": 1}), (T.SQL_PLANNER, "apply_change", {"approval_id": 1}),
    (T.SQL_PLANNER, "create_approval", {"proposal": {}, "summary": "x", "pipeline_run_id": None}),
    (T.SQL_PLANNER, "dq_results", {"run_id": 1}), (T.SQL_PLANNER, "catalog_search", {"q": "x"}),
    (T.DATA_QUALITY, "sql_execute", {"sql": "SELECT 1"}), (T.DATA_QUALITY, "create_approval", {"proposal": {}, "summary": "x", "pipeline_run_id": None}),
    (T.DATA_QUALITY, "apply_change", {"approval_id": 1}), (T.ETL_RECO, "sql_execute", {"sql": "SELECT 1"}),
    (T.ETL_RECO, "apply_change", {"approval_id": 1}), (T.ETL_RECO, "dq_latest_run", {"pipeline": "sales"}),
    (T.EXECUTOR, "llm_complete", {"system": "s", "user": "u"}), (T.EXECUTOR, "sql_execute", {"sql": "SELECT 1"}),
    (T.ROUTER, "sql_execute", {"sql": "SELECT 1"}), ("rogue_agent", "catalog_search", {"q": "x"}),
])
def test_agent_cannot_use_tools_outside_its_allowlist(app_engine, agent, tool, kwargs):
    c = ctx("admin")
    with pytest.raises(PermissionDenied, match="not permitted"):
        call_tool(agent, tool, c, **kwargs)
    assert denied_audits(app_engine, c.run_id)                                  # the denial itself is audited


def test_unknown_tools_are_denied(app_engine):
    c = ctx("admin")
    with pytest.raises(PermissionDenied, match="unknown tool"):
        call_tool(T.SQL_PLANNER, "drop_database", c)
    assert denied_audits(app_engine, c.run_id) == ["unknown tool 'drop_database'"]


@pytest.mark.parametrize("tool,kwargs,agent", [
    ("dq_latest_run", {"pipeline": "sales"}, T.DATA_QUALITY), ("dq_results", {"run_id": 1}, T.DATA_QUALITY),
    ("run_details", {"run_id": 1}, T.ETL_RECO), ("create_approval", {"proposal": {}, "summary": "x", "pipeline_run_id": None}, T.ETL_RECO),
])
def test_analyst_role_cannot_use_privileged_tools_even_through_the_right_agent(app_engine, tool, kwargs, agent):
    c = ctx("analyst")
    with pytest.raises(PermissionDenied, match="role 'analyst'"):
        call_tool(agent, tool, c, **kwargs)
    assert any("role 'analyst'" in r for r in denied_audits(app_engine, c.run_id))


def test_unknown_role_gets_nothing():
    for tool, agent in (("catalog_search", T.METADATA), ("sql_validate", T.SQL_PLANNER)):
        with pytest.raises(PermissionDenied):
            call_tool(agent, tool, ctx("hacker"), **({"q": "x"} if tool == "catalog_search" else {"sql": "SELECT 1"}))


def test_allowed_calls_work_and_are_audited(app_engine):
    c = ctx("analyst")
    hits = call_tool(T.METADATA, "catalog_search", c, q="orders")
    assert hits and call_tool(T.SQL_PLANNER, "sql_validate", c, sql="SELECT channel FROM gold.daily_sales")["ok"]
    with app_engine.connect() as cn:
        rows = cn.execute(text("SELECT detail->>'tool', outcome FROM app.audit_events WHERE resource_id=:r AND action='agent.tool.call' ORDER BY id"),
                          {"r": c.run_id}).all()
    assert rows == [("catalog_search", "success"), ("sql_validate", "success")]


def test_gold_schema_respects_role_boundaries():
    analyst = {t["name"] for t in call_tool(T.METADATA, "gold_schema", ctx("analyst"))}
    engineer = {t["name"] for t in call_tool(T.METADATA, "gold_schema", ctx("engineer"))}
    assert "gold.customer_value" not in analyst and "gold.customer_value" in engineer
    assert analyst == {"gold.daily_sales", "gold.funnel_daily", "gold.delivery_sla"}
    cols = [c["name"] for t in call_tool(T.METADATA, "gold_schema", ctx("admin")) for c in t["columns"]]
    assert not any(c.startswith("_") for c in cols)                              # internal columns never shown to the planner


def test_apply_change_refuses_without_an_approved_approval(app_engine, make_run):
    name, rid = make_run()
    proposal = {"action": "set_max_reject_pct", "params": {"value": 5.0}, "requires_approval": True, "pipeline": name,
                "rationale": "r", "risk": "k"}
    c = ctx("engineer")
    approval_id = call_tool(T.ETL_RECO, "create_approval", c, proposal=proposal, summary="s", pipeline_run_id=rid)
    with pytest.raises(T.ApplyRefused, match="not approved"):
        call_tool(T.EXECUTOR, "apply_change", c, approval_id=approval_id)         # still pending
    with pytest.raises(T.ApplyRefused, match="not found"):
        call_tool(T.EXECUTOR, "apply_change", c, approval_id=987654321)
    with app_engine.connect() as cn:
        assert cn.execute(text("SELECT params FROM app.pipelines WHERE name=:n"), {"n": name}).scalar() == {}


def test_create_approval_refuses_invalid_or_advice_proposals(make_run):
    name, rid = make_run()
    for bad in ({"action": "drop_table", "params": {}, "requires_approval": True, "pipeline": name},
                {"action": "advice_only", "params": {}, "requires_approval": False, "pipeline": name},
                {"action": "set_max_reject_pct", "params": {"value": 99}, "requires_approval": True, "pipeline": name}):
        with pytest.raises(PermissionDenied):
            call_tool(T.ETL_RECO, "create_approval", ctx("engineer"), proposal=bad, summary="s", pipeline_run_id=rid)
