"""Administration: users and audit logs. Admin role only."""
from fastapi import APIRouter, Depends, HTTPException, Query
from pydantic import BaseModel, ConfigDict, Field
from sqlalchemy import select
from sqlalchemy.orm import Session

from app import audit
from app.db import get_session
from app.models import AuditEvent, QueryAudit, Role, User
from app.security import Principal, require_roles
from app.services.users import UserError, create_user

router = APIRouter(tags=["admin"], dependencies=[Depends(require_roles("admin"))])


class UserIn(BaseModel):
    model_config = ConfigDict(extra="forbid")
    username: str = Field(min_length=3, max_length=32)
    password: str = Field(min_length=1, max_length=128)
    role: str = Field(max_length=16)
    email: str | None = Field(default=None, max_length=255)


class UserPatch(BaseModel):
    model_config = ConfigDict(extra="forbid")
    is_active: bool | None = None
    role: str | None = Field(default=None, max_length=16)


class UserOut(BaseModel):
    id: int
    username: str
    role: str
    is_active: bool
    locked: bool


def _out(u: User) -> UserOut:
    from datetime import datetime, timezone
    return UserOut(id=u.id, username=u.username, role=u.role.name, is_active=u.is_active,
                   locked=bool(u.locked_until and u.locked_until > datetime.now(timezone.utc)))


@router.get("/admin/users", response_model=list[UserOut])
def list_users(session: Session = Depends(get_session)):
    return [_out(u) for u in session.scalars(select(User).order_by(User.id))]


@router.post("/admin/users", response_model=UserOut, status_code=201)
def add_user(body: UserIn, admin: Principal = Depends(require_roles("admin")), session: Session = Depends(get_session)):
    try:
        return _out(create_user(session, body.username, body.password, body.role, actor=admin.username, email=body.email))
    except UserError as exc:
        raise HTTPException(422 if "exists" not in str(exc) else 409, str(exc)) from None


@router.patch("/admin/users/{username}", response_model=UserOut)
def patch_user(username: str, body: UserPatch, admin: Principal = Depends(require_roles("admin")),
               session: Session = Depends(get_session)):
    user = session.scalar(select(User).where(User.username == username))
    if user is None:
        raise HTTPException(404, "user not found")
    if user.username == admin.username and (body.is_active is False or (body.role and body.role != "admin")):
        raise HTTPException(409, "you cannot deactivate or demote yourself")
    changes = {}
    if body.is_active is not None:
        user.is_active = body.is_active
        changes["is_active"] = body.is_active
    if body.role is not None:
        role = session.scalar(select(Role).where(Role.name == body.role))
        if role is None:
            raise HTTPException(422, "unknown role")
        user.role_id, changes["role"] = role.id, body.role
    audit.record(session, actor=admin.username, action="user.update", resource_type="user", resource_id=user.id, detail=changes)
    session.commit()
    session.refresh(user)
    return _out(user)


@router.get("/audit/events")
def audit_events(limit: int = Query(50, ge=1, le=200), action: str | None = Query(None, max_length=64),
                 session: Session = Depends(get_session)):
    stmt = select(AuditEvent).order_by(AuditEvent.id.desc()).limit(limit)
    if action:
        stmt = stmt.where(AuditEvent.action == action)
    return [{"id": e.id, "at": e.occurred_at.isoformat(), "actor": e.actor_label, "action": e.action, "outcome": e.outcome,
             "resource": f"{e.resource_type}:{e.resource_id}", "detail": e.detail} for e in session.scalars(stmt)]


@router.get("/audit/queries")
def audit_queries(limit: int = Query(50, ge=1, le=200), session: Session = Depends(get_session)):
    rows = session.scalars(select(QueryAudit).order_by(QueryAudit.id.desc()).limit(limit))
    return [{"id": q.id, "at": q.created_at.isoformat(), "actor": q.actor, "role": q.role, "status": q.status, "source": q.source,
             "sql": q.generated_sql, "executed_sql": q.executed_sql, "reasons": q.block_reasons, "rows": q.row_count,
             "cost": q.estimated_cost} for q in rows]
