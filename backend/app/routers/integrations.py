"""Integration operations (engineer/admin): deliver pending alerts, inspect delivery and call history."""
from typing import Literal

from fastapi import APIRouter, Depends, Query
from sqlalchemy import select
from sqlalchemy.orm import Session

from app import audit
from app.db import get_session
from app.integrations.dispatch import dispatch_pending_alerts
from app.models import DqAlert, IntegrationCall
from app.security import Principal, require_roles

router = APIRouter(prefix="/integrations", tags=["integrations"], dependencies=[Depends(require_roles("engineer", "admin"))])


@router.post("/dispatch")
def dispatch(user: Principal = Depends(require_roles("engineer", "admin")), session: Session = Depends(get_session)):
    summary = dispatch_pending_alerts()
    audit.record(session, actor=user.username, action="integrations.dispatch", resource_type="dq_alert",
                 outcome="success" if not summary["degraded"] else "failure", detail=summary)
    session.commit()
    return summary


@router.get("/alerts")
def alerts(status: Literal["pending", "sent", "failed", "all"] = "all", limit: int = Query(50, ge=1, le=200),
           session: Session = Depends(get_session)):
    stmt = select(DqAlert).order_by(DqAlert.id.desc()).limit(limit)
    if status != "all":
        stmt = stmt.where(DqAlert.status == status)
    return [{"id": a.id, "run_id": a.run_id, "severity": a.severity, "title": a.title, "status": a.status,
             "attempts": a.attempts, "last_error": a.last_error, "ticket_key": a.ticket_key,
             "sent_at": a.sent_at.isoformat() if a.sent_at else None} for a in session.scalars(stmt)]


@router.get("/calls")
def calls(limit: int = Query(50, ge=1, le=200), session: Session = Depends(get_session)):
    return [{"id": c.id, "integration": c.integration, "operation": c.operation, "target": c.target, "status": c.status,
             "attempts": c.attempts, "latency_ms": c.latency_ms, "http_status": c.http_status, "error": c.error,
             "resource": f"{c.resource_type}:{c.resource_id}" if c.resource_type else None, "at": c.created_at.isoformat()}
            for c in session.scalars(select(IntegrationCall).order_by(IntegrationCall.id.desc()).limit(limit))]
