"""Persistence of the user-visible run record (app.agent_runs). LangGraph's own checkpoints (app.checkpoints*)
hold the resumable graph state; this table holds what the UI and audits read."""
from __future__ import annotations

import uuid
from datetime import datetime, timezone

from sqlalchemy.orm import Session

from app.db import get_engine
from app.models import AgentRun

SNAPSHOT_KEYS = ("intent", "intent_source", "pipeline", "target_run_id", "steps", "degraded_reasons", "denied", "error",
                 "sql_source", "sql_attempts", "sql_validated", "guard_errors", "approval_id", "approval_status",
                 "applied", "verified", "verification_notes", "proposal", "diagnosis")


def create_run(run_id: str, user: str, text: str) -> None:
    with Session(get_engine()) as s:
        s.add(AgentRun(id=uuid.UUID(run_id), thread_id=run_id, request_text=text[:2000], status="running",
                       current_node="start", state={"actor": user}))
        s.commit()


def mark(run_id: str, **fields) -> None:
    with Session(get_engine()) as s:
        run = s.get(AgentRun, uuid.UUID(run_id))
        if run is None:
            return
        for k, v in fields.items():
            setattr(run, k, v)
        run.updated_at = datetime.now(timezone.utc)
        if fields.get("status") in ("succeeded", "failed"):
            run.finished_at = run.updated_at
        s.commit()


def snapshot(run_id: str, state: dict) -> None:
    snap = {k: state[k] for k in SNAPSHOT_KEYS if k in state}
    r = state.get("sql_result")
    if r:
        snap["sql_result"] = {"row_count": r["row_count"], "columns": r["columns"], "rows_preview": r["rows"][:20],
                              "truncated": r["truncated"], "sql": r["sql"], "estimated_cost": r["estimated_cost"],
                              "duration_ms": r["duration_ms"]}
    d = state.get("dq_report")
    if d:
        snap["dq_summary"] = {"run": d["run"], "totals": d["totals"]}
    with Session(get_engine()) as s:
        run = s.get(AgentRun, uuid.UUID(run_id))
        if run is not None:
            run.state = {**(run.state or {}), **snap}
            run.request_type = state.get("intent") or run.request_type
            s.commit()


def get_run(run_id: str) -> dict | None:
    with Session(get_engine()) as s:
        run = s.get(AgentRun, uuid.UUID(run_id))
        if run is None:
            return None
        return {"id": str(run.id), "thread_id": run.thread_id, "status": run.status, "current_node": run.current_node,
                "intent": run.request_type, "request": run.request_text, "state": run.state, "error": run.error_message,
                "created_at": run.created_at.isoformat(), "finished_at": run.finished_at.isoformat() if run.finished_at else None}
