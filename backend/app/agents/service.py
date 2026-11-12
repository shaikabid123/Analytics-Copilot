"""Entry points used by CLI/API/tests: start a run, resume it after a human decision, read run state."""
from __future__ import annotations

import uuid
from contextlib import contextmanager

import psycopg
from langgraph.checkpoint.postgres import PostgresSaver
from langgraph.types import Command
from psycopg.rows import dict_row
from sqlalchemy import text
from sqlalchemy.engine import make_url
from sqlalchemy.orm import Session

from app import audit
from app.agents import runstore
from app.agents.nodes import clean_text
from app.agents.graph import build_graph
from app.agents.tools import PRIVILEGED, PermissionDenied
from app.config import get_settings
from app.db import get_engine
from app.llm import LLMClient, get_llm


class ApprovalError(Exception):
    pass


class ApprovalNotFound(ApprovalError):
    pass


class ApprovalConflict(ApprovalError):
    pass


class SelfApprovalError(ApprovalError):
    pass


@contextmanager
def open_checkpointer():
    """LangGraph Postgres checkpointer in the app schema, using the app_rw role (no extra privileges)."""
    u = make_url(get_settings().database_url)
    dsn = (f"host={u.host} port={u.port or 5432} dbname={u.database} user={u.username} password={u.password} "
           "options='-c search_path=app'")
    with psycopg.connect(dsn, autocommit=True, prepare_threshold=0, row_factory=dict_row) as conn:
        cp = PostgresSaver(conn)
        cp.setup()
        yield cp


def _outcome(graph, config, run_id: str, result: dict | None) -> dict:
    snap = graph.get_state(config)
    run = runstore.get_run(run_id)
    interrupted = bool(snap.next)
    values = snap.values or {}
    return {"run_id": run_id, "status": "awaiting_approval" if interrupted else run["status"],
            "answer": values.get("answer"), "intent": values.get("intent"), "approval_id": values.get("approval_id"),
            "state": values, "next": list(snap.next)}


def run_request(text_: str, actor: str, role: str, *, llm: LLMClient | None = None, pipeline: str | None = None,
                target_run_id: int | None = None) -> dict:
    run_id = str(uuid.uuid4())
    text_ = clean_text(text_)          # strips control characters (Postgres rejects NUL) before anything is stored
    actor = clean_text(actor)[:64]
    runstore.create_run(run_id, actor, text_)
    with Session(get_engine()) as s:
        audit.record(s, actor=actor or "unknown", action="agent.run.start", resource_type="agent_run", resource_id=run_id,
                     detail={"role": role, "request": text_[:200]})
        s.commit()
    config = {"configurable": {"thread_id": run_id}}
    init = {"run_id": run_id, "actor": actor, "role": role, "request_text": text_, "pipeline": pipeline,
            "target_run_id": target_run_id, "steps": [], "degraded_reasons": []}
    with open_checkpointer() as cp:
        graph = build_graph(llm or get_llm(), cp)
        result = graph.invoke(init, config)
        return _outcome(graph, config, run_id, result)


def resume_approval(approval_id: int, decision: str, actor: str, role: str, note: str = "",
                    *, llm: LLMClient | None = None) -> dict:
    """Human decision on a pending approval. Only engineer/admin may decide; the decision is recorded atomically
    in the database FIRST, then the paused graph is resumed from its Postgres checkpoint."""
    if role not in PRIVILEGED:
        with Session(get_engine()) as s:
            audit.record(s, actor=str(actor)[:64], action="approval.decide_denied", resource_type="approval",
                         resource_id=approval_id, outcome="denied", detail={"role": role})
            s.commit()
        raise PermissionDenied(f"role '{role}' may not approve or reject changes")
    if decision not in ("approved", "rejected"):
        raise ApprovalError("decision must be 'approved' or 'rejected'")
    with Session(get_engine()) as s:
        req = s.execute(text("SELECT requested_by_label, status FROM app.approvals WHERE id=:i"), {"i": approval_id}).first()
        if req is None:
            raise ApprovalNotFound("approval not found")
        if req[0] and req[0] == str(actor) and not get_settings().allow_self_approval:
            audit.record(s, actor=str(actor)[:64], action="approval.self_decision_denied", resource_type="approval",
                         resource_id=approval_id, outcome="denied", detail={"role": role})
            s.commit()
            raise SelfApprovalError("you cannot approve or reject a change that you requested")
    with Session(get_engine()) as s:
        row = s.execute(text(
            "UPDATE app.approvals SET status=:d, decided_by_label=:a, decided_at=now(), decision_note=:n "
            "WHERE id=:i AND status='pending' RETURNING agent_run_id"),
            {"d": decision, "a": str(actor)[:64], "n": note[:500] or None, "i": approval_id}).first()
        if row is None:
            raise ApprovalConflict("approval already decided")
        audit.record(s, actor=str(actor)[:64], action="approval.decided", resource_type="approval", resource_id=approval_id,
                     detail={"decision": decision, "role": role})
        s.commit()
    run_id = str(row[0])
    from app.integrations.slack import notify_safely
    notify_safely(f"Approval #{approval_id} {decision} by {actor}", resource=("approval", str(approval_id)))
    config = {"configurable": {"thread_id": run_id}}
    with open_checkpointer() as cp:
        graph = build_graph(llm or get_llm(), cp)
        result = graph.invoke(Command(resume={"decision": decision}), config)
        return _outcome(graph, config, run_id, result)


get_run = runstore.get_run
