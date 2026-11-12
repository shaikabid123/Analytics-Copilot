"""The single LangGraph workflow:

  intake -> classify -> [metadata_agent | dq_agent] -> ... -> verify -> respond -> finalize

  sql:   metadata_agent -> sql_draft -> sql_validate -(ok)-> sql_execute -> verify
                                          |-(rejected, attempts left)-> sql_repair -> sql_validate   (bounded)
                                          |-(rejected, repairs spent)-> sql_template -> sql_validate (deterministic fallback)
  dq:    dq_agent -> verify
  etl:   dq_agent -> etl_agent -> [approval_gate (interrupt) -> apply_change] -> verify
"""
from __future__ import annotations

from langgraph.errors import GraphInterrupt
from langgraph.graph import END, START, StateGraph

from app import audit
from app.agents import runstore
from app.agents.nodes import MAX_SQL_ATTEMPTS, Nodes
from app.agents.state import CopilotState
from app.db import get_engine
from app.llm import LLMClient


def _merged(state, update) -> dict:
    out = {**state, **update}
    out["steps"] = list(state.get("steps", [])) + list(update.get("steps", []))
    out["degraded_reasons"] = list(state.get("degraded_reasons", [])) + list(update.get("degraded_reasons", []))
    return out


def tracked(name: str, fn):
    """Persist progress after every node; mark the run awaiting_approval on interrupt, failed on error."""
    def wrapper(state: CopilotState) -> dict:
        run_id = state["run_id"]
        runstore.mark(run_id, current_node=name)
        try:
            update = fn(state)
        except GraphInterrupt:
            runstore.mark(run_id, status="awaiting_approval", current_node=name)
            raise
        except Exception as exc:  # noqa: BLE001 - recorded, then surfaced
            runstore.mark(run_id, status="failed", current_node=name, error_message=f"{type(exc).__name__}: {exc}"[:1000])
            raise
        runstore.snapshot(run_id, _merged(state, update))
        runstore.mark(run_id, status="running")        # a resumed run is running again
        return update
    return wrapper


def finalize(state: CopilotState) -> dict:
    from sqlalchemy.orm import Session
    err = state.get("error")
    runstore.snapshot(state["run_id"], {**state})
    status = "failed" if err else "succeeded"
    runstore.mark(state["run_id"], status=status, current_node="finalize", error_message=err)
    with Session(get_engine()) as s:
        audit.record(s, actor=state.get("actor") or "unknown", action="agent.run.finish", resource_type="agent_run",
                     resource_id=state["run_id"], outcome="failure" if err else "success",
                     detail={"intent": state.get("intent"), "intent_source": state.get("intent_source"),
                             "degraded": state.get("degraded_reasons") or [], "verified": state.get("verified"),
                             "approval_id": state.get("approval_id"), "steps": state.get("steps", [])})
        s.commit()
    return {"steps": ["finalize"]}


# ------------------------------------------------------------------ routing (pure functions of state)
def after_intake(state):
    return "respond" if state.get("error") else "classify"


def after_classify(state):
    if state.get("denied"):
        return "respond"
    return {"metadata": "metadata_agent", "sql": "metadata_agent", "dq": "dq_agent", "etl": "dq_agent"}.get(state["intent"], "respond")


def after_metadata(state):
    return "sql_draft" if state["intent"] == "sql" else "verify"


def _next_sql_attempt(state):
    if state.get("sql_source") == "llm" and state.get("sql_attempts", 1) < MAX_SQL_ATTEMPTS:
        return "sql_repair"
    return "verify" if state.get("template_tried") else "sql_template"


def after_sql_validate(state):
    return "sql_execute" if state.get("sql_validated") else _next_sql_attempt(state)


def after_sql_execute(state):
    return "verify" if state.get("sql_result") and not state.get("guard_errors") else _next_sql_attempt(state)


def after_dq(state):
    if state.get("error"):
        return "respond"
    return "etl_agent" if state["intent"] == "etl" else "verify"


def after_etl(state):
    p = state.get("proposal")
    return "approval_gate" if state.get("approval_id") and p and p.get("requires_approval") else "verify"


def after_gate(state):
    return "apply_change" if state.get("approval_status") == "approved" else "verify"


def build_graph(llm: LLMClient, checkpointer):
    n = Nodes(llm)
    g = StateGraph(CopilotState)
    for name, fn in (("intake", n.intake), ("classify", n.classify), ("metadata_agent", n.metadata_agent),
                     ("sql_draft", n.sql_draft), ("sql_validate", n.sql_validate), ("sql_repair", n.sql_repair),
                     ("sql_template", n.sql_template), ("sql_execute", n.sql_execute), ("dq_agent", n.dq_agent),
                     ("etl_agent", n.etl_agent), ("approval_gate", n.approval_gate), ("apply_change", n.apply_change),
                     ("verify", n.verify), ("respond", n.respond)):
        g.add_node(name, tracked(name, fn))
    g.add_node("finalize", finalize)
    g.add_edge(START, "intake")
    g.add_conditional_edges("intake", after_intake, {"respond": "respond", "classify": "classify"})
    g.add_conditional_edges("classify", after_classify, {"respond": "respond", "metadata_agent": "metadata_agent", "dq_agent": "dq_agent"})
    g.add_conditional_edges("metadata_agent", after_metadata, {"sql_draft": "sql_draft", "verify": "verify"})
    g.add_edge("sql_draft", "sql_validate")
    g.add_conditional_edges("sql_validate", after_sql_validate, {"sql_execute": "sql_execute", "sql_repair": "sql_repair",
                                                                 "sql_template": "sql_template", "verify": "verify"})
    g.add_edge("sql_repair", "sql_validate")
    g.add_edge("sql_template", "sql_validate")
    g.add_conditional_edges("sql_execute", after_sql_execute, {"verify": "verify", "sql_repair": "sql_repair", "sql_template": "sql_template"})
    g.add_conditional_edges("dq_agent", after_dq, {"respond": "respond", "etl_agent": "etl_agent", "verify": "verify"})
    g.add_conditional_edges("etl_agent", after_etl, {"approval_gate": "approval_gate", "verify": "verify"})
    g.add_conditional_edges("approval_gate", after_gate, {"apply_change": "apply_change", "verify": "verify"})
    g.add_edge("apply_change", "verify")
    g.add_edge("verify", "respond")
    g.add_edge("respond", "finalize")
    g.add_edge("finalize", END)
    return g.compile(checkpointer=checkpointer)
