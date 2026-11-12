"""Persisted human approval queue. Engineers and admins only; a requester cannot decide their own request."""
from typing import Literal

from fastapi import APIRouter, Depends, HTTPException, Query
from pydantic import BaseModel, ConfigDict, Field
from sqlalchemy import select
from sqlalchemy.orm import Session

from app.agents import service
from app.agents.tools import PermissionDenied
from app.db import get_session
from app.models import Approval
from app.security import Principal, require_roles

router = APIRouter(prefix="/approvals", tags=["approvals"], dependencies=[Depends(require_roles("engineer", "admin"))])
REVIEWER = require_roles("engineer", "admin")


class DecisionIn(BaseModel):
    model_config = ConfigDict(extra="forbid")
    decision: Literal["approved", "rejected"]
    note: str | None = Field(default=None, max_length=500)


def _out(a: Approval) -> dict:
    change = a.proposed_change or {}
    return {"id": a.id, "status": a.status, "type": a.request_type, "summary": a.summary, "requested_by": a.requested_by_label,
            "created_at": a.created_at.isoformat(), "decided_by": a.decided_by_label,
            "decided_at": a.decided_at.isoformat() if a.decided_at else None, "note": a.decision_note,
            "pipeline_run_id": a.pipeline_run_id, "proposal": change.get("proposal"), "applied_at": change.get("applied_at")}


@router.get("")
def list_approvals(status: Literal["pending", "approved", "rejected", "all"] = "pending",
                   limit: int = Query(50, ge=1, le=200), session: Session = Depends(get_session)):
    stmt = select(Approval).order_by(Approval.id.desc()).limit(limit)
    if status != "all":
        stmt = stmt.where(Approval.status == status)
    return [_out(a) for a in session.scalars(stmt)]


@router.get("/{approval_id}")
def get_approval(approval_id: int, session: Session = Depends(get_session)):
    a = session.get(Approval, approval_id)
    if a is None:
        raise HTTPException(404, "approval not found")
    return _out(a)


@router.post("/{approval_id}/decision")
def decide(approval_id: int, body: DecisionIn, user: Principal = Depends(REVIEWER)):
    try:
        out = service.resume_approval(approval_id, body.decision, user.username, user.role, body.note or "")
    except service.ApprovalNotFound:
        raise HTTPException(404, "approval not found") from None
    except service.SelfApprovalError as exc:
        raise HTTPException(403, str(exc)) from None
    except PermissionDenied as exc:
        raise HTTPException(403, str(exc)) from None
    except service.ApprovalConflict as exc:
        raise HTTPException(409, str(exc)) from None
    except service.ApprovalError as exc:
        raise HTTPException(422, str(exc)) from None
    return {"approval_id": approval_id, "run_id": out["run_id"], "run_status": out["status"], "answer": out["answer"]}
